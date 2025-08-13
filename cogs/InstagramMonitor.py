import discord
import logging
import yaml
import sqlite3
import asyncio
import requests
import re
from typing import Optional, Tuple, Dict, Any
from discord.ext import commands, tasks
from datetime import datetime, timezone
from discord.utils import utcnow


IG_APP_ID = "936619743392459"  # Instagram Web App ID


def audit_log(message: str):
    ts = utcnow().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open("audit.log", "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {message}\n")
    except Exception as e:
        logging.error(f"Failed to write to audit.log: {e}")


class InstagramMonitor(commands.Cog):
    """
    Polls Instagram for a public user and posts an embed when a new post appears.
    Uses multiple fallbacks and backs off on 401/403 to avoid log spam.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot

        # Load config
        with open("config.yaml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

        self.username = cfg.get("instagram_username")
        self.announce_channel_id = cfg.get("instagram_announce_channel_id")
        self.ping_role_id = cfg.get("instagram_ping_role_id")
        self.poll_interval = int(cfg.get("instagram_poll_interval", 300))  # seconds
        # Optional cookie string or just sessionid=... to improve reliability
        # Example:
        # instagram_cookie: "sessionid=YOUR_SESSION_ID; csrftoken=XYZ; ds_user_id=12345"
        self.cookie_raw = cfg.get("instagram_cookie", "").strip()

        if not self.username or not self.announce_channel_id:
            raise RuntimeError(
                "Missing instagram_username or instagram_announce_channel_id in config.yaml"
            )

        # DB
        self.db = sqlite3.connect("database.db", check_same_thread=False)
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS instagram_last (
                username   TEXT PRIMARY KEY,
                shortcode  TEXT
            )
            """
        )
        self.db.commit()

        row = self.db.execute(
            "SELECT shortcode FROM instagram_last WHERE username = ?", (self.username,)
        ).fetchone()
        self.last_shortcode: Optional[str] = row[0] if row else None

        # HTTP session
        self.session = requests.Session()
        self.base_headers = {
            # Desktop UA is more stable today than the legacy Android UA
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Accept-Language": "en-GB,en;q=0.9",
            "Referer": f"https://www.instagram.com/{self.username}/",
            "X-IG-App-ID": IG_APP_ID,
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://www.instagram.com",
            "Connection": "keep-alive",
        }
        if self.cookie_raw:
            # Allow raw cookie header to be passed. Requests prefers dicts but Cookie header works fine.
            self.base_headers["Cookie"] = self.cookie_raw

        # Backoff state to avoid spamming logs on auth errors
        self._consecutive_auth_errors = 0
        self._max_backoff = 3600  # 60 minutes cap
        self._current_interval = max(30, self.poll_interval)

        audit_log(
            f"InstagramMonitor initialised for @{self.username} (interval={self._current_interval}s)"
        )
        self.check_posts.change_interval(seconds=self._current_interval)
        self.check_posts.start()

    @commands.Cog.listener()
    async def on_ready(self):
        logging.info("InstagramMonitor cog synced successfully.")
        audit_log("InstagramMonitor cog synced successfully.")

    def cog_unload(self):
        self.check_posts.cancel()
        try:
            self.session.close()
        except Exception:
            pass
        try:
            self.db.close()
        except Exception:
            pass

    # ---------------------------
    # Internal helpers
    # ---------------------------

    def _auth_backoff(self):
        """Increase loop interval on repeated 401/403s to reduce noise."""
        self._consecutive_auth_errors += 1
        # Exponential-ish backoff: 5m, 10m, 20m, 40m, 60m...
        next_interval = min(
            self._max_backoff,
            max(300, self.poll_interval) * (2 ** (self._consecutive_auth_errors - 1)),
        )
        if next_interval != self._current_interval:
            self._current_interval = int(next_interval)
            self.check_posts.change_interval(seconds=self._current_interval)
            audit_log(
                f"Instagram 401/403 backoff engaged. New interval: {self._current_interval}s (errors={self._consecutive_auth_errors})"
            )

    def _auth_recover(self):
        """Reset interval back to configured when a call succeeds."""
        if self._consecutive_auth_errors > 0:
            self._consecutive_auth_errors = 0
            if self._current_interval != self.poll_interval:
                self._current_interval = int(self.poll_interval)
                self.check_posts.change_interval(seconds=self._current_interval)
                audit_log(
                    f"Instagram backoff cleared. Interval reset to {self._current_interval}s"
                )

    def _get(
        self, url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 15
    ) -> requests.Response:
        merged = dict(self.base_headers)
        if headers:
            merged.update(headers)
        return self.session.get(
            url, headers=merged, timeout=timeout, allow_redirects=True
        )

    def _parse_latest_from_json(
        self, data: Dict[str, Any]
    ) -> Optional[Tuple[str, str, str, datetime]]:
        # New style: data -> user -> edge_owner_to_timeline_media -> edges[0].node
        user = (data or {}).get("data", {}).get("user", {})
        edges = user.get("edge_owner_to_timeline_media", {}).get("edges", [])
        if not edges:
            return None

        node = edges[0].get("node", {}) or {}
        shortcode = node.get("shortcode")
        if not shortcode:
            return None

        caption_edges = node.get("edge_media_to_caption", {}).get("edges", [])
        caption = (
            caption_edges[0]["node"].get("text", "")
            if caption_edges and "node" in caption_edges[0]
            else ""
        )
        image_url = node.get("display_url") or node.get("thumbnail_src") or ""
        ts_raw = node.get("taken_at_timestamp", 0) or 0
        timestamp = (
            datetime.fromtimestamp(int(ts_raw), tz=timezone.utc)
            if ts_raw
            else utcnow().replace(tzinfo=timezone.utc)
        )

        return shortcode, caption, image_url, timestamp

    def _parse_latest_from_html(
        self, html: str
    ) -> Optional[Tuple[str, str, str, datetime]]:
        """
        Very conservative HTML fallback. We only try to find the first post shortcode.
        Captions and images are optional in this path.
        """
        # Look for "shortcode":"XXXXXXXX"
        m = re.search(r'"shortcode"\s*:\s*"([A-Za-z0-9_-]{5,})"', html)
        if not m:
            return None
        shortcode = m.group(1)

        # Try to find a display_url near the first media object
        img = re.search(r'"display_url"\s*:\s*"([^"]+)"', html)
        image_url = img.group(1).encode("utf-8").decode("unicode_escape") if img else ""

        # Captions are harder to reliably extract without full JSON; keep empty
        caption = ""

        # No reliable timestamp in HTML without JSON; set to now as a safe default
        timestamp = utcnow().replace(tzinfo=timezone.utc)

        return shortcode, caption, image_url, timestamp

    def fetch_latest_post(self) -> Optional[Tuple[str, str, str, datetime]]:
        """
        Attempt 1: Web profile info endpoint
        Attempt 2: Alt path variant
        Attempt 3: HTML scrape
        Returns tuple (shortcode, caption, image_url, timestamp) or None
        Raises for network issues so caller can handle and back off.
        """
        # Try primary endpoint
        primary = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={self.username}"
        r1 = self._get(primary)
        if r1.status_code == 200:
            try:
                data = r1.json()
            except ValueError:
                data = {}
            result = self._parse_latest_from_json(data)
            if result:
                return result

        # Try legacy path that some proxies still support
        legacy = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={self.username}"
        r2 = self._get(legacy, headers={"User-Agent": self.base_headers["User-Agent"]})
        if r2.status_code == 200:
            try:
                data = r2.json()
            except ValueError:
                data = {}
            result = self._parse_latest_from_json(data)
            if result:
                return result

        # If either returned auth-related errors, signal caller to back off
        if r1.status_code in (401, 403) or r2.status_code in (401, 403):
            raise PermissionError(
                f"Instagram auth error: primary={r1.status_code}, legacy={r2.status_code}"
            )

        # Final fallback: HTML scrape of profile page
        html_url = f"https://www.instagram.com/{self.username}/"
        r3 = self._get(html_url)
        if r3.status_code == 200 and r3.text:
            result = self._parse_latest_from_html(r3.text)
            if result:
                return result

        # If we got here, we did not find anything
        return None

    # ---------------------------
    # Poll loop
    # ---------------------------

    @tasks.loop(seconds=60.0)
    async def check_posts(self):
        try:
            result = await asyncio.to_thread(self.fetch_latest_post)

            if result is None:
                # Not an error. Could be private account, no posts, or page changed.
                logging.debug(f"No latest post found for @{self.username} this cycle.")
                return

            shortcode, caption, image_url, timestamp = result

            # Same as before, nothing to do
            if self.last_shortcode and shortcode == self.last_shortcode:
                self._auth_recover()
                return

            # Update DB and memory
            self.last_shortcode = shortcode
            self.db.execute(
                "INSERT OR REPLACE INTO instagram_last (username, shortcode) VALUES (?, ?)",
                (self.username, shortcode),
            )
            self.db.commit()

            # Build and send embed
            channel = self.bot.get_channel(int(self.announce_channel_id))
            if channel is None:
                logging.error(f"Discord channel {self.announce_channel_id} not found")
                audit_log(
                    f"Discord channel {self.announce_channel_id} not found for Instagram post {shortcode}"
                )
                self._auth_recover()
                return

            embed = discord.Embed(
                title="📸 New Instagram Post",
                description=caption or "*No caption*",
                url=f"https://www.instagram.com/p/{shortcode}/",
                timestamp=timestamp,
                color=discord.Color.magenta(),
            )
            if image_url:
                embed.set_image(url=image_url)
            embed.set_author(
                name=f"@{self.username}",
                url=f"https://www.instagram.com/{self.username}/",
            )

            content = f"<@&{self.ping_role_id}>" if self.ping_role_id else None
            await channel.send(
                content=content,
                embed=embed,
                allowed_mentions=discord.AllowedMentions(roles=True),
            )

            audit_log(f"Posted new Instagram post {shortcode} to channel {channel.id}")
            self._auth_recover()

        except PermissionError as e:
            # 401/403 style issues
            logging.error(f"Instagram returned an auth error for {self.username}: {e}")
            audit_log(f"Instagram auth error for @{self.username}: {e}")
            self._auth_backoff()

        except requests.RequestException as e:
            logging.error(
                f"Network error contacting Instagram for {self.username}: {e}"
            )
            audit_log(f"Instagram network error for @{self.username}: {e}")

        except Exception as e:
            logging.error(f"Error in InstagramMonitor.check_posts: {e}")
            audit_log(f"Error polling Instagram: {e}")

    @check_posts.before_loop
    async def before_check(self):
        await self.bot.wait_until_ready()

    # Optional manual check slash command for debugging
    @commands.hybrid_command(
        name="instagram_check", description="Force-check Instagram now"
    )
    @commands.has_permissions(manage_guild=True)
    async def instagram_check(self, ctx: commands.Context):
        await ctx.defer(ephemeral=True) if hasattr(ctx, "defer") else None
        try:
            result = await asyncio.to_thread(self.fetch_latest_post)
            if result is None:
                await ctx.reply(
                    "No post found right now. Account private, empty, or endpoint blocked.",
                    ephemeral=True,
                )
                return
            shortcode, caption, image_url, timestamp = result
            await ctx.reply(
                f"Latest shortcode: `{shortcode}` at {timestamp.isoformat()}",
                ephemeral=True,
            )
        except PermissionError as e:
            await ctx.reply(
                f"Auth error: {e}. Consider adding a valid instagram_cookie in config.yaml.",
                ephemeral=True,
            )
        except Exception as e:
            await ctx.reply(f"Error checking Instagram: {e}", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(InstagramMonitor(bot))
