import discord
import logging
import yaml
import sqlite3
import asyncio
import requests
from discord.ext import commands, tasks
from discord import app_commands
from datetime import datetime, timezone
from discord.utils import utcnow

def audit_log(message: str):
    ts = utcnow().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open("audit.log", "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {message}\n")
    except Exception as e:
        logging.error(f"Failed to write to audit.log: {e}")

class InstagramMonitor(commands.Cog):
    """
    Polls Instagram's web_profile_info endpoint for a public user and
    posts an embed when a new post appears, pinging a role first.
    """
    def __init__(self, bot: commands.Bot):
        self.bot = bot

        # load config
        with open("config.yaml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        self.username             = cfg["instagram_username"]
        self.announce_channel_id  = cfg["instagram_announce_channel_id"]
        self.ping_role_id         = cfg.get("instagram_ping_role_id")
        self.poll_interval        = cfg.get("instagram_poll_interval", 300)

        # database setup
        self.db = sqlite3.connect("database.db", check_same_thread=False)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS instagram_last (
                username   TEXT PRIMARY KEY,
                shortcode  TEXT
            )
        """)
        self.db.commit()

        # load last seen shortcode
        row = self.db.execute(
            "SELECT shortcode FROM instagram_last WHERE username = ?",
            (self.username,)
        ).fetchone()
        self.last_shortcode = row[0] if row else None

        audit_log(f"InstagramMonitor initialised for @{self.username}")

        # start polling task
        self.check_posts.start()
        self.check_posts.change_interval(seconds=self.poll_interval)

    @commands.Cog.listener()
    async def on_ready(self):
        # log when cog is ready
        logging.info("\033[96mInstagramMonitor\033[0m cog synced successfully.")
        audit_log("InstagramMonitor cog synced successfully.")

    def cog_unload(self):
        self.check_posts.cancel()

    @tasks.loop(seconds=60.0)
    async def check_posts(self):
        try:
            url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={self.username}"
            headers = {
                "User-Agent": "Instagram 155.0.0.37.107 Android",
                "Accept": "*/*",
            }
            # fetch JSON
            resp = await asyncio.to_thread(
                requests.get,
                url,
                headers=headers,
                timeout=10
            )
            if not resp.ok:
                logging.error(f"Instagram returned HTTP {resp.status_code} for {self.username}")
                audit_log(f"Failed to fetch Instagram profile: HTTP {resp.status_code}")
                return

            try:
                data = resp.json()
            except ValueError as e:
                logging.error(f"Instagram JSON decode error for {self.username}: {e}")
                audit_log(f"JSON decode failed for {self.username}: {e}")
                return

            user = data.get("data", {}).get("user", {})
            edges = user.get("edge_owner_to_timeline_media", {}).get("edges", [])
            if not edges:
                logging.warning(f"No posts found in JSON for {self.username}")
                return

            node = edges[0]["node"]
            shortcode     = node.get("shortcode")
            caption_edge  = node.get("edge_media_to_caption", {}).get("edges", [])
            caption       = caption_edge[0]["node"]["text"] if caption_edge else ""
            image_url     = node.get("display_url")
            timestamp     = datetime.fromtimestamp(
                node.get("taken_at_timestamp", 0), tz=timezone.utc
            )

            # only new posts
            if shortcode == self.last_shortcode:
                return

            # update DB
            self.last_shortcode = shortcode
            self.db.execute(
                "INSERT OR REPLACE INTO instagram_last (username, shortcode) VALUES (?, ?)",
                (self.username, shortcode)
            )
            self.db.commit()

            channel = self.bot.get_channel(self.announce_channel_id)
            if channel is None:
                logging.error(f"Discord channel {self.announce_channel_id} not found")
                return

            # build embed
            embed = discord.Embed(
                title="📸 New Instagram Post",
                description=caption or "*No caption*",
                url=f"https://www.instagram.com/p/{shortcode}/",
                timestamp=timestamp,
                color=discord.Color.magenta()
            )
            if image_url:
                embed.set_image(url=image_url)
            embed.set_author(
                name=f"@{self.username}",
                url=f"https://www.instagram.com/{self.username}/"
            )

            # ping role then send embed in same message
            mention = f"<@&{self.ping_role_id}> " if self.ping_role_id else None
            await channel.send(
                content=mention,
                embed=embed
            )
            audit_log(f"Posted new Instagram post {shortcode} to channel {channel.id}")

        except Exception as e:
            logging.error(f"Error in InstagramMonitor.check_posts: {e}")
            audit_log(f"Error polling Instagram: {e}")

    @check_posts.before_loop
    async def before_check(self):
        await self.bot.wait_until_ready()

async def setup(bot: commands.Bot):
    await bot.add_cog(InstagramMonitor(bot))
