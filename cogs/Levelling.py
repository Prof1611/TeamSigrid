import re
import math
import random
import sqlite3
import asyncio
import discord
import logging
from typing import Optional, Tuple, Dict, Any, List, Literal
from discord import app_commands
from discord.ext import commands
from datetime import datetime


# ======================================================================================
# Utilities
# ======================================================================================


def audit_log(message: str):
    """Append a timestamped message to the audit log file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open("audit.log", "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {message}\n")
    except Exception as e:
        logging.error(f"Failed to write to audit.log: {e}")


def make_embed(title: str, description: str, color: discord.Color) -> discord.Embed:
    return discord.Embed(title=title, description=description, color=color)


# ======================================================================================
# Database layer
# ======================================================================================

DB_PATH = "levels.db"

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL;")
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# Create tables
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS guild_settings (
        guild_id INTEGER PRIMARY KEY,
        xp_min INTEGER NOT NULL DEFAULT 10,
        xp_max INTEGER NOT NULL DEFAULT 20,
        cooldown_seconds INTEGER NOT NULL DEFAULT 60,
        multiplier REAL NOT NULL DEFAULT 1.0,
        min_chars INTEGER NOT NULL DEFAULT 5,
        attachments_bonus INTEGER NOT NULL DEFAULT 5,
        mentions_bonus INTEGER NOT NULL DEFAULT 2,
        threads_multiplier REAL NOT NULL DEFAULT 1.0,
        ignore_bots INTEGER NOT NULL DEFAULT 1,
        curve_type TEXT NOT NULL DEFAULT 'quadratic', -- linear, quadratic, or exponential
        base_xp INTEGER NOT NULL DEFAULT 100,         -- used by all curves
        curve_a INTEGER NOT NULL DEFAULT 50,          -- used by linear and quadratic
        curve_b INTEGER NOT NULL DEFAULT 0,           -- reserved
        announce_level_up INTEGER NOT NULL DEFAULT 1,
        announce_channel_id INTEGER
    );
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS ignored_channels (
        guild_id INTEGER NOT NULL,
        channel_id INTEGER NOT NULL,
        PRIMARY KEY (guild_id, channel_id)
    );
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS blacklisted_roles (
        guild_id INTEGER NOT NULL,
        role_id INTEGER NOT NULL,
        PRIMARY KEY (guild_id, role_id)
    );
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS user_xp (
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        xp INTEGER NOT NULL DEFAULT 0,
        level INTEGER NOT NULL DEFAULT 0,
        last_message_ts INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (guild_id, user_id)
    );
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS role_rewards (
        guild_id INTEGER NOT NULL,
        level INTEGER NOT NULL,
        role_id INTEGER NOT NULL,
        PRIMARY KEY (guild_id, level)
    );
    """
)

conn.commit()

_db_lock = asyncio.Lock()


# ======================================================================================
# Core maths for level curves
# ======================================================================================


def xp_required_for_level(curve: str, base_xp: int, a: int, b: int, level: int) -> int:
    """
    Return the cumulative XP required to reach a given level.
    Level 0 requires 0 XP by definition.
    Curves:
      - linear: xp = (base_xp + a) * level
      - quadratic: xp = base_xp * level^2 + a * level + b
      - exponential: xp = base_xp * (1.15^level - 1)
    Values are rounded to integers.
    """
    level = max(0, level)
    if level == 0:
        return 0

    curve = (curve or "quadratic").lower()
    if curve == "linear":
        xp = (base_xp + a) * level
    elif curve == "exponential":
        xp = base_xp * (math.pow(1.15, level) - 1.0)
    else:
        # quadratic default
        xp = base_xp * (level**2) + a * level + b
    return int(round(max(0, xp)))


def level_from_total_xp(curve: str, base_xp: int, a: int, b: int, total_xp: int) -> int:
    """
    Given cumulative XP, return the highest level such that xp_required_for_level(level) <= total_xp.
    Uses a simple search with an upper bound that grows until it exceeds total_xp.
    """
    total_xp = max(0, total_xp)
    # Quick ramp to find an upper bound
    hi = 1
    while xp_required_for_level(curve, base_xp, a, b, hi) <= total_xp:
        hi *= 2
        if hi > 10000:
            break
    lo = 0
    # Binary search between lo and hi
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if xp_required_for_level(curve, base_xp, a, b, mid) <= total_xp:
            lo = mid
        else:
            hi = mid - 1
    return lo


def xp_between_levels(curve: str, base_xp: int, a: int, b: int, level: int) -> int:
    """Return the XP needed to advance from level to level+1."""
    needed = xp_required_for_level(
        curve, base_xp, a, b, level + 1
    ) - xp_required_for_level(curve, base_xp, a, b, level)
    return max(1, needed)


# ======================================================================================
# Helper functions for database access
# ======================================================================================

DEFAULT_SETTINGS = {
    "xp_min": 10,
    "xp_max": 20,
    "cooldown_seconds": 60,
    "multiplier": 1.0,
    "min_chars": 5,
    "attachments_bonus": 5,
    "mentions_bonus": 2,
    "threads_multiplier": 1.0,
    "ignore_bots": 1,
    "curve_type": "quadratic",
    "base_xp": 100,
    "curve_a": 50,
    "curve_b": 0,
    "announce_level_up": 1,
    "announce_channel_id": None,
}


async def get_settings(guild_id: int) -> Dict[str, Any]:
    async with _db_lock:
        row = cursor.execute(
            "SELECT * FROM guild_settings WHERE guild_id = ?",
            (guild_id,),
        ).fetchone()
        if not row:
            # Insert defaults if not present then fetch again
            cursor.execute(
                """
                INSERT INTO guild_settings (
                    guild_id, xp_min, xp_max, cooldown_seconds, multiplier, min_chars,
                    attachments_bonus, mentions_bonus, threads_multiplier, ignore_bots,
                    curve_type, base_xp, curve_a, curve_b, announce_level_up, announce_channel_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    guild_id,
                    DEFAULT_SETTINGS["xp_min"],
                    DEFAULT_SETTINGS["xp_max"],
                    DEFAULT_SETTINGS["cooldown_seconds"],
                    DEFAULT_SETTINGS["multiplier"],
                    DEFAULT_SETTINGS["min_chars"],
                    DEFAULT_SETTINGS["attachments_bonus"],
                    DEFAULT_SETTINGS["mentions_bonus"],
                    DEFAULT_SETTINGS["threads_multiplier"],
                    DEFAULT_SETTINGS["ignore_bots"],
                    DEFAULT_SETTINGS["curve_type"],
                    DEFAULT_SETTINGS["base_xp"],
                    DEFAULT_SETTINGS["curve_a"],
                    DEFAULT_SETTINGS["curve_b"],
                    DEFAULT_SETTINGS["announce_level_up"],
                    DEFAULT_SETTINGS["announce_channel_id"],
                ),
            )
            conn.commit()
            row = cursor.execute(
                "SELECT * FROM guild_settings WHERE guild_id = ?",
                (guild_id,),
            ).fetchone()
        return dict(row)


async def update_settings(guild_id: int, **fields):
    if not fields:
        return
    keys = []
    vals = []
    for k, v in fields.items():
        keys.append(f"{k} = ?")
        vals.append(v)
    vals.append(guild_id)
    async with _db_lock:
        cursor.execute(
            f"UPDATE guild_settings SET {', '.join(keys)} WHERE guild_id = ?",
            tuple(vals),
        )
        conn.commit()


async def get_user_record(guild_id: int, user_id: int) -> sqlite3.Row:
    async with _db_lock:
        row = cursor.execute(
            "SELECT * FROM user_xp WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        ).fetchone()
        if not row:
            cursor.execute(
                "INSERT INTO user_xp (guild_id, user_id, xp, level, last_message_ts) VALUES (?, ?, 0, 0, 0)",
                (guild_id, user_id),
            )
            conn.commit()
            row = cursor.execute(
                "SELECT * FROM user_xp WHERE guild_id = ? AND user_id = ?",
                (guild_id, user_id),
            ).fetchone()
        return row


async def add_xp_and_check_level_up(
    guild: discord.Guild,
    member: discord.Member,
    gained_xp: int,
) -> Tuple[int, int, List[Tuple[int, int]]]:
    """
    Increase XP for a user and check for level-ups.
    Returns tuple of (new_total_xp, new_level, awarded_roles_list)
    awarded_roles_list: list of (level, role_id) granted during this update.
    """
    settings = await get_settings(guild.id)
    curve = settings["curve_type"]
    base_xp = settings["base_xp"]
    a = settings["curve_a"]
    b = settings["curve_b"]

    async with _db_lock:
        row = cursor.execute(
            "SELECT xp, level FROM user_xp WHERE guild_id = ? AND user_id = ?",
            (guild.id, member.id),
        ).fetchone()
        if not row:
            cursor.execute(
                "INSERT INTO user_xp (guild_id, user_id, xp, level, last_message_ts) VALUES (?, ?, 0, 0, 0)",
                (guild.id, member.id),
            )
            conn.commit()
            current_xp, current_level = 0, 0
        else:
            current_xp, current_level = int(row["xp"]), int(row["level"])

        new_total = max(0, current_xp + int(gained_xp))
        new_level = level_from_total_xp(curve, base_xp, a, b, new_total)

        cursor.execute(
            "UPDATE user_xp SET xp = ?, level = ? WHERE guild_id = ? AND user_id = ?",
            (new_total, new_level, guild.id, member.id),
        )
        conn.commit()

    awarded: List[Tuple[int, int]] = []
    if new_level > current_level:
        # Award any role rewards at levels current_level+1..new_level
        async with _db_lock:
            rows = cursor.execute(
                "SELECT level, role_id FROM role_rewards WHERE guild_id = ? AND level BETWEEN ? AND ? ORDER BY level ASC",
                (guild.id, current_level + 1, new_level),
            ).fetchall()
        for rr in rows:
            level_at, role_id = int(rr["level"]), int(rr["role_id"])
            role = guild.get_role(role_id)
            if not role:
                continue
            try:
                if role not in member.roles:
                    await member.add_roles(
                        role, reason=f"Level reward at level {level_at}"
                    )
                    awarded.append((level_at, role_id))
            except discord.Forbidden:
                audit_log(
                    f"Missing permissions to assign role {role_id} in guild {guild.id}"
                )
            except discord.HTTPException as e:
                audit_log(
                    f"HTTP error assigning role {role_id} in guild {guild.id}: {e}"
                )

    return new_total, new_level, awarded


async def set_last_message_ts(guild_id: int, user_id: int, ts: int):
    async with _db_lock:
        cursor.execute(
            "UPDATE user_xp SET last_message_ts = ? WHERE guild_id = ? AND user_id = ?",
            (ts, guild_id, user_id),
        )
        conn.commit()


async def get_last_message_ts(guild_id: int, user_id: int) -> int:
    async with _db_lock:
        row = cursor.execute(
            "SELECT last_message_ts FROM user_xp WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        ).fetchone()
    return int(row["last_message_ts"]) if row else 0


async def is_channel_ignored(guild_id: int, channel_id: int) -> bool:
    async with _db_lock:
        row = cursor.execute(
            "SELECT 1 FROM ignored_channels WHERE guild_id = ? AND channel_id = ?",
            (guild_id, channel_id),
        ).fetchone()
    return row is not None


async def add_ignored_channel(guild_id: int, channel_id: int):
    async with _db_lock:
        cursor.execute(
            "INSERT OR IGNORE INTO ignored_channels (guild_id, channel_id) VALUES (?, ?)",
            (guild_id, channel_id),
        )
        conn.commit()


async def remove_ignored_channel(guild_id: int, channel_id: int):
    async with _db_lock:
        cursor.execute(
            "DELETE FROM ignored_channels WHERE guild_id = ? AND channel_id = ?",
            (guild_id, channel_id),
        )
        conn.commit()


async def list_ignored_channels(guild_id: int) -> List[int]:
    async with _db_lock:
        rows = cursor.execute(
            "SELECT channel_id FROM ignored_channels WHERE guild_id = ?",
            (guild_id,),
        ).fetchall()
    return [int(r["channel_id"]) for r in rows]


async def is_role_blacklisted(guild_id: int, role_id: int) -> bool:
    async with _db_lock:
        row = cursor.execute(
            "SELECT 1 FROM blacklisted_roles WHERE guild_id = ? AND role_id = ?",
            (guild_id, role_id),
        ).fetchone()
    return row is not None


async def add_blacklisted_role(guild_id: int, role_id: int):
    async with _db_lock:
        cursor.execute(
            "INSERT OR IGNORE INTO blacklisted_roles (guild_id, role_id) VALUES (?, ?)",
            (guild_id, role_id),
        )
        conn.commit()


async def remove_blacklisted_role(guild_id: int, role_id: int):
    async with _db_lock:
        cursor.execute(
            "DELETE FROM blacklisted_roles WHERE guild_id = ? AND role_id = ?",
            (guild_id, role_id),
        )
        conn.commit()


async def list_blacklisted_roles(guild_id: int) -> List[int]:
    async with _db_lock:
        rows = cursor.execute(
            "SELECT role_id FROM blacklisted_roles WHERE guild_id = ?",
            (guild_id,),
        ).fetchall()
    return [int(r["role_id"]) for r in rows]


async def set_role_reward(guild_id: int, level: int, role_id: int):
    async with _db_lock:
        cursor.execute(
            "INSERT OR REPLACE INTO role_rewards (guild_id, level, role_id) VALUES (?, ?, ?)",
            (guild_id, level, role_id),
        )
        conn.commit()


async def remove_role_reward(guild_id: int, level: int):
    async with _db_lock:
        cursor.execute(
            "DELETE FROM role_rewards WHERE guild_id = ? AND level = ?",
            (guild_id, level),
        )
        conn.commit()


async def list_role_rewards(guild_id: int) -> List[sqlite3.Row]:
    async with _db_lock:
        rows = cursor.execute(
            "SELECT level, role_id FROM role_rewards WHERE guild_id = ? ORDER BY level ASC",
            (guild_id,),
        ).fetchall()
    return rows


async def top_users(guild_id: int, limit: int, offset: int = 0) -> List[sqlite3.Row]:
    async with _db_lock:
        rows = cursor.execute(
            "SELECT user_id, xp, level FROM user_xp WHERE guild_id = ? ORDER BY xp DESC, user_id ASC LIMIT ? OFFSET ?",
            (guild_id, limit, offset),
        ).fetchall()
    return rows


async def user_rank(guild_id: int, user_id: int) -> int:
    async with _db_lock:
        # Count how many have strictly more XP
        row = cursor.execute(
            "SELECT COUNT(*) AS better FROM user_xp WHERE guild_id = ? AND xp > (SELECT xp FROM user_xp WHERE guild_id = ? AND user_id = ?)",
            (guild_id, guild_id, user_id),
        ).fetchone()
        if row is None:
            return 0
        return int(row["better"]) + 1


# ======================================================================================
# UI helpers
# ======================================================================================


def bool_emoji(value: bool) -> str:
    return "✅" if value else "❌"


def format_progress_bar(progress: int, goal: int, length: int = 18) -> str:
    goal = max(goal, 1)
    progress = max(0, progress)
    ratio = min(1.0, progress / goal)
    filled = int(round(ratio * length))
    if progress > 0 and filled == 0:
        filled = 1
    filled = max(0, min(length, filled))
    bar = "█" * filled + "░" * (length - filled)
    return bar


def create_profile_embed(
    target: discord.abc.User,
    level_val: int,
    rank: int,
    total_xp: int,
    progress: int,
    to_next: int,
) -> discord.Embed:
    color = discord.Color.blurple()
    if isinstance(target, discord.Member) and target.color.value:
        color = target.color

    rank_label = f"#{rank}" if rank > 0 else "Unranked"
    embed = discord.Embed(
        title=f"{target.display_name}'s Level Profile",
        color=color,
        timestamp=datetime.utcnow(),
    )
    embed.description = (
        f"{target.mention} is currently **Level {level_val}**\n"
        f"Server Rank: **{rank_label}**"
    )
    embed.set_thumbnail(url=target.display_avatar.url)

    embed.add_field(name="Total XP", value=f"{total_xp:,}", inline=True)

    if to_next > 0:
        remaining = max(0, to_next - progress)
        embed.add_field(name="Next Level In", value=f"{remaining:,} XP", inline=True)
    else:
        embed.add_field(name="Next Level In", value="Max level reached", inline=True)

    progress_goal = to_next if to_next > 0 else max(progress, 1)
    progress_bar = format_progress_bar(progress, progress_goal)
    if to_next > 0:
        percent = min(100.0, max(0.0, (progress / to_next) * 100))
        progress_text = f"{progress_bar}\n`{progress:,}/{to_next:,} XP` ({percent:.1f}%)"
    else:
        progress_text = f"{progress_bar}\n`{progress:,} XP` gained at this level"
    embed.add_field(name="Progress", value=progress_text, inline=False)

    embed.set_footer(text=f"User ID: {target.id}")
    return embed


def build_leaderboard_embed(
    guild: discord.Guild,
    rows: List[sqlite3.Row],
    start_index: int,
    page: int,
    per_page: int,
    viewer_id: Optional[int] = None,
) -> discord.Embed:
    color = discord.Color.blurple()
    me = getattr(guild, "me", None)
    if isinstance(me, discord.Member) and me.color.value:
        color = me.color

    embed = discord.Embed(
        title=f"Leaderboard • Page {page + 1}",
        color=color,
        timestamp=datetime.utcnow(),
    )

    if guild.icon:
        embed.set_author(name=guild.name, icon_url=guild.icon.url)
        embed.set_thumbnail(url=guild.icon.url)
    else:
        embed.set_author(name=guild.name)

    if not rows:
        embed.description = "No data to display yet. Start chatting to earn XP!"
    else:
        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        lines = []
        for idx, r in enumerate(rows):
            rank = start_index + idx + 1
            medal = medals.get(rank)
            rank_label = medal or f"#{rank}"
            member = guild.get_member(int(r["user_id"]))
            mention = member.mention if member else f"<@{int(r['user_id'])}>"
            level_val = int(r["level"])
            xp = int(r["xp"])
            entry = f"{rank_label} {mention} • Level **{level_val}** • {xp:,} XP"
            if viewer_id and viewer_id == int(r["user_id"]):
                entry = f"__{entry}__"
            lines.append(entry)

        embed.description = "\n".join(lines)
        embed.add_field(
            name="Showing",
            value=f"Ranks {start_index + 1} – {start_index + len(rows)}",
            inline=True,
        )
        embed.add_field(
            name="Entries",
            value=f"{len(rows)}/{per_page}",
            inline=True,
        )

    if viewer_id:
        viewer = guild.get_member(viewer_id)
        footer_text = (
            f"Requested by {viewer.display_name}"
            if viewer
            else f"Requested by ID {viewer_id}"
        )
        embed.set_footer(text=f"{footer_text} • Use the buttons below to navigate.")
    else:
        embed.set_footer(text="Use the buttons below to navigate.")

    return embed


class LeaderboardView(discord.ui.View):
    def __init__(
        self,
        cog: "LevelSystem",
        guild: discord.Guild,
        per_page: int = 10,
        viewer_id: Optional[int] = None,
    ):
        super().__init__(timeout=60)
        self.cog = cog
        self.guild = guild
        self.page = 0
        self.per_page = per_page
        self.viewer_id = viewer_id

    async def _render(self, interaction: discord.Interaction):
        start = self.page * self.per_page
        rows = await top_users(self.guild.id, self.per_page, start)
        embed = build_leaderboard_embed(
            self.guild,
            rows,
            start,
            self.page,
            self.per_page,
            viewer_id=self.viewer_id,
        )

        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary)
    async def previous(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        if self.page > 0:
            self.page -= 1
        await self._render(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.primary)
    async def next(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page += 1
        await self._render(interaction)

    async def on_timeout(self):
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True


# ======================================================================================
# The Cog
# ======================================================================================


class LevelSystem(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        audit_log("LevelSystem cog initialised.")

    @commands.Cog.listener()
    async def on_ready(self):
        logging.info("\033[96mLevelSystem\033[0m cog synced successfully.")
        audit_log("LevelSystem cog synced successfully.")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """
        Earn XP on message. Applies cooldown, channel ignore, role blacklist, and content length rules.
        """
        try:
            if not message.guild:
                return
            if message.author.bot:
                settings = await get_settings(message.guild.id)
                if int(settings["ignore_bots"]) == 1:
                    return

            settings = await get_settings(message.guild.id)

            # Ignore channel if configured
            if await is_channel_ignored(message.guild.id, message.channel.id):
                return

            # Ignore members with any blacklisted role
            if isinstance(message.author, discord.Member):
                member: discord.Member = message.author
                bl_roles = set(await list_blacklisted_roles(message.guild.id))
                if any(r.id in bl_roles for r in member.roles):
                    return

            # Enforce minimum characters
            content_len = len(message.content.strip())
            if content_len < int(settings["min_chars"]):
                return

            # Cooldown
            now_ts = int(datetime.utcnow().timestamp())
            last_ts = await get_last_message_ts(message.guild.id, message.author.id)
            cooldown = int(settings["cooldown_seconds"])
            if now_ts - last_ts < cooldown:
                return

            # Compute XP gain
            xp_min = int(settings["xp_min"])
            xp_max = int(settings["xp_max"])
            base_gain = random.randint(xp_min, xp_max)

            # Bonus for attachments and mentions
            attach_bonus = int(settings["attachments_bonus"]) * len(message.attachments)
            mentions_bonus = int(settings["mentions_bonus"]) * len(message.mentions)

            gain = base_gain + attach_bonus + mentions_bonus

            # Threads multiplier
            thread_mult = float(settings["threads_multiplier"])
            if isinstance(message.channel, discord.Thread):
                gain = int(round(gain * thread_mult))

            # Global multiplier
            gain = int(round(gain * float(settings["multiplier"])))
            gain = max(0, gain)

            # Update last message timestamp
            await set_last_message_ts(message.guild.id, message.author.id, now_ts)

            if gain <= 0:
                return

            # Apply XP and handle level up
            new_total, new_level, awarded = await add_xp_and_check_level_up(
                message.guild, message.author, gain
            )

            if awarded or new_level > 0:
                # Announce if enabled
                if int(settings["announce_level_up"]) == 1:
                    channel_id = settings["announce_channel_id"]
                    announce_channel: Optional[discord.abc.Messageable] = None
                    if channel_id:
                        ch = message.guild.get_channel(int(channel_id))
                        if isinstance(ch, (discord.TextChannel, discord.Thread)):
                            announce_channel = ch
                    if announce_channel is None:
                        announce_channel = message.channel

                    try:
                        current_level = new_level
                        need_to_next = xp_between_levels(
                            settings["curve_type"],
                            int(settings["base_xp"]),
                            int(settings["curve_a"]),
                            int(settings["curve_b"]),
                            current_level,
                        )
                        prev_req = xp_required_for_level(
                            settings["curve_type"],
                            int(settings["base_xp"]),
                            int(settings["curve_a"]),
                            int(settings["curve_b"]),
                            current_level,
                        )
                        progress = new_total - prev_req
                        embed = discord.Embed(
                            title="Level Up",
                            description=(
                                f"{message.author.mention} reached **Level {current_level}**!\n"
                                f"Progress to next level: `{progress}/{need_to_next}` XP"
                            ),
                            color=discord.Color.gold(),
                        )
                        if awarded:
                            role_lines = []
                            for lvl, rid in awarded:
                                role = message.guild.get_role(rid)
                                if role:
                                    role_lines.append(
                                        f"Level {lvl} reward: {role.mention}"
                                    )
                            if role_lines:
                                embed.add_field(
                                    name="Rewards",
                                    value="\n".join(role_lines),
                                    inline=False,
                                )
                        await announce_channel.send(embed=embed)
                    except discord.Forbidden:
                        audit_log(
                            f"Cannot announce level up in #{message.channel.id} for guild {message.guild.id}"
                        )
                    except Exception as e:
                        logging.warning(f"Level up announce failed: {e}")
                        audit_log(f"Level up announce failed: {e}")

            audit_log(
                f"Gained {gain} XP for {message.author} in {message.guild.name} ({message.guild.id}). Total now {new_total}."
            )

        except Exception as e:
            logging.error(f"on_message error: {e}")
            audit_log(f"on_message error: {e}")

    # ==============================================================================
    # Public commands
    # ==============================================================================

    group = app_commands.Group(name="level", description="Levelling system commands")

    @group.command(
        name="profile", description="Show your level profile or another member's."
    )
    @app_commands.describe(member="Member to view, leave empty for yourself")
    async def profile(
        self, interaction: discord.Interaction, member: Optional[discord.Member] = None
    ):
        try:
            if not interaction.guild:
                return await interaction.response.send_message(
                    embed=discord.Embed(
                        description="This can only be used in a server."
                    ),
                    ephemeral=True,
                )
            target = member or interaction.user
            record = await get_user_record(interaction.guild.id, target.id)
            settings = await get_settings(interaction.guild.id)

            total = int(record["xp"])
            level_val = int(record["level"])
            rank = await user_rank(interaction.guild.id, target.id)

            to_next = xp_between_levels(
                settings["curve_type"],
                int(settings["base_xp"]),
                int(settings["curve_a"]),
                int(settings["curve_b"]),
                level_val,
            )
            prev_req = xp_required_for_level(
                settings["curve_type"],
                int(settings["base_xp"]),
                int(settings["curve_a"]),
                int(settings["curve_b"]),
                level_val,
            )
            progress = total - prev_req
            progress = max(0, progress)

            embed = create_profile_embed(
                target,
                level_val,
                rank,
                total,
                progress,
                to_next,
            )
            await interaction.response.send_message(embed=embed, ephemeral=False)
            audit_log(
                f"{interaction.user} checked profile for {target} in {interaction.guild.name}"
            )
        except Exception as e:
            logging.error(f"/level profile failed: {e}")
            try:
                await interaction.response.send_message(
                    embed=discord.Embed(description="Failed to fetch profile."),
                    ephemeral=True,
                )
            except discord.InteractionResponded:
                await interaction.followup.send(
                    embed=discord.Embed(description="Failed to fetch profile."),
                    ephemeral=True,
                )

    @group.command(name="leaderboard", description="Show the server leaderboard.")
    async def leaderboard(self, interaction: discord.Interaction):
        try:
            if not interaction.guild:
                return await interaction.response.send_message(
                    embed=discord.Embed(
                        description="This can only be used in a server."
                    ),
                    ephemeral=True,
                )
            per_page = 10
            view = LeaderboardView(
                self,
                interaction.guild,
                per_page=per_page,
                viewer_id=interaction.user.id,
            )
            # Initial render
            rows = await top_users(interaction.guild.id, per_page, 0)
            embed = build_leaderboard_embed(
                interaction.guild,
                rows,
                0,
                0,
                per_page,
                viewer_id=interaction.user.id,
            )
            await interaction.response.send_message(embed=embed, view=view)
        except Exception as e:
            logging.error(f"/level leaderboard failed: {e}")
            try:
                await interaction.response.send_message(
                    embed=discord.Embed(description="Failed to fetch leaderboard."),
                    ephemeral=True,
                )
            except discord.InteractionResponded:
                await interaction.followup.send(
                    embed=discord.Embed(description="Failed to fetch leaderboard."),
                    ephemeral=True,
                )

    # ==============================================================================
    # Admin and config commands
    # ==============================================================================

    config = app_commands.Group(
        name="levelconfig", description="Configure server levelling"
    )

    # ----- helpers -----
    @staticmethod
    def _clamp_int(value: int, min_v: int, max_v: int) -> int:
        return max(min_v, min(max_v, value))

    @staticmethod
    def _curve_name(value: str) -> str:
        v = (value or "quadratic").strip().lower()
        return v if v in {"linear", "quadratic", "exponential"} else "quadratic"

    # ----- overview -----
    @config.command(name="show", description="Show current levelling settings.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_show(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message(
                embed=discord.Embed(description="Server only."),
                ephemeral=True,
            )
        s = await get_settings(interaction.guild.id)
        ignored = await list_ignored_channels(interaction.guild.id)
        bl_roles = await list_blacklisted_roles(interaction.guild.id)
        rewards = await list_role_rewards(interaction.guild.id)

        ch_mentions = [f"<#{cid}>" for cid in ignored] or ["None"]
        role_mentions = []
        for rid in bl_roles:
            role = interaction.guild.get_role(rid)
            role_mentions.append(role.mention if role else f"`{rid}`")
        role_mentions = role_mentions or ["None"]

        # Build reward lines safely
        reward_lines: List[str] = []
        for r in rewards:
            level = int(r["level"])
            role_id = int(r["role_id"])
            role = interaction.guild.get_role(role_id)
            if role:
                reward_lines.append(f"Level {level} → {role.mention}")
            else:
                reward_lines.append(f"Level {level} → `{role_id}`")
        reward_lines = reward_lines or ["None"]

        announce_ch = None
        if s["announce_channel_id"]:
            c = interaction.guild.get_channel(int(s["announce_channel_id"]))
            announce_ch = c.mention if c else f"`{s['announce_channel_id']}`"
        else:
            announce_ch = "Same channel as level up"

        desc = (
            f"XP range: **{s['xp_min']} - {s['xp_max']}**\n"
            f"Cooldown: **{s['cooldown_seconds']} s**\n"
            f"Multiplier: **{s['multiplier']}** | Threads multiplier: **{s['threads_multiplier']}**\n"
            f"Min chars: **{s['min_chars']}** | Attachments bonus: **{s['attachments_bonus']}** | Mentions bonus: **{s['mentions_bonus']}**\n"
            f"Ignore bots: {bool_emoji(bool(s['ignore_bots']))}\n"
            f"Curve: **{s['curve_type']}** | base_xp: **{s['base_xp']}** | a: **{s['curve_a']}** | b: **{s['curve_b']}**\n"
            f"Announce level up: {bool_emoji(bool(s['announce_level_up']))} | Channel: {announce_ch}\n\n"
            f"Ignored channels: {', '.join(ch_mentions)}\n"
            f"Blacklisted roles: {', '.join(role_mentions)}\n"
            f"Role rewards:\n" + "\n".join(reward_lines)
        )
        embed = make_embed("Levelling Settings", desc, discord.Color.green())
        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ----- core numeric settings -----
    @config.command(name="setrange", description="Set random XP range per message.")
    @app_commands.describe(
        xp_min="Minimum XP per message", xp_max="Maximum XP per message"
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_setrange(
        self,
        interaction: discord.Interaction,
        xp_min: app_commands.Range[int, 0, 100000],
        xp_max: app_commands.Range[int, 1, 100000],
    ):
        if xp_max < xp_min:
            return await interaction.response.send_message(
                embed=discord.Embed(
                    description="xp_max must be greater than or equal to xp_min."
                ),
                ephemeral=True,
            )
        await update_settings(
            interaction.guild.id, xp_min=int(xp_min), xp_max=int(xp_max)
        )
        await interaction.response.send_message(
            embed=discord.Embed(
                description=f"XP range set to {xp_min}-{xp_max}."
            ),
            ephemeral=True,
        )

    @config.command(
        name="setcooldown", description="Set seconds between XP awards per user."
    )
    @app_commands.describe(seconds="Cooldown in seconds")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_setcooldown(
        self,
        interaction: discord.Interaction,
        seconds: app_commands.Range[int, 0, 86400],
    ):
        await update_settings(interaction.guild.id, cooldown_seconds=int(seconds))
        await interaction.response.send_message(
            embed=discord.Embed(
                description=f"Cooldown set to {seconds} seconds."
            ),
            ephemeral=True,
        )

    @config.command(name="setmultiplier", description="Set global XP multiplier.")
    @app_commands.describe(multiplier="Global multiplier. 1.0 for default.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_setmultiplier(
        self,
        interaction: discord.Interaction,
        multiplier: app_commands.Range[float, 0.0, 100.0],
    ):
        await update_settings(interaction.guild.id, multiplier=float(multiplier))
        await interaction.response.send_message(
            embed=discord.Embed(
                description=f"Global multiplier set to {multiplier}."
            ),
            ephemeral=True,
        )

    @config.command(
        name="setminchars", description="Set minimum characters required to earn XP."
    )
    @app_commands.describe(min_chars="Minimum characters")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_setminchars(
        self,
        interaction: discord.Interaction,
        min_chars: app_commands.Range[int, 0, 4000],
    ):
        await update_settings(interaction.guild.id, min_chars=int(min_chars))
        await interaction.response.send_message(
            embed=discord.Embed(
                description=f"Minimum characters set to {min_chars}."
            ),
            ephemeral=True,
        )

    @config.command(
        name="setbonuses",
        description="Set bonus XP values for attachments and mentions.",
    )
    @app_commands.describe(
        attachments_bonus="XP per attachment", mentions_bonus="XP per mention"
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_setbonuses(
        self,
        interaction: discord.Interaction,
        attachments_bonus: app_commands.Range[int, 0, 100000],
        mentions_bonus: app_commands.Range[int, 0, 100000],
    ):
        await update_settings(
            interaction.guild.id,
            attachments_bonus=int(attachments_bonus),
            mentions_bonus=int(mentions_bonus),
        )
        await interaction.response.send_message(
            embed=discord.Embed(
                description=(
                    f"Bonuses updated. Attachments: {attachments_bonus}, Mentions: {mentions_bonus}."
                )
            ),
            ephemeral=True,
        )

    @config.command(
        name="setthreadsmult", description="Set XP multiplier for messages in threads."
    )
    @app_commands.describe(threads_multiplier="Multiplier for thread channels")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_setthreadsmult(
        self,
        interaction: discord.Interaction,
        threads_multiplier: app_commands.Range[float, 0.0, 100.0],
    ):
        await update_settings(
            interaction.guild.id, threads_multiplier=float(threads_multiplier)
        )
        await interaction.response.send_message(
            embed=discord.Embed(
                description=f"Threads multiplier set to {threads_multiplier}."
            ),
            ephemeral=True,
        )

    @config.command(name="ignorebots", description="Toggle ignoring bot messages.")
    @app_commands.describe(enabled="True to ignore bot messages")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_ignorebots(self, interaction: discord.Interaction, enabled: bool):
        await update_settings(interaction.guild.id, ignore_bots=1 if enabled else 0)
        await interaction.response.send_message(
            embed=discord.Embed(
                description=f"Ignore bots set to {enabled}."
            ),
            ephemeral=True,
        )

    # ----- curve settings -----
    @config.command(
        name="setcurve", description="Set the XP curve type and parameters."
    )
    @app_commands.describe(
        curve_type="linear, quadratic, or exponential",
        base_xp="Base XP factor",
        curve_a="Curve parameter a",
        curve_b="Curve parameter b",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_setcurve(
        self,
        interaction: discord.Interaction,
        curve_type: str,
        base_xp: app_commands.Range[int, 1, 1000000],
        curve_a: int = 50,
        curve_b: int = 0,
    ):
        ct = self._curve_name(curve_type)
        await update_settings(
            interaction.guild.id,
            curve_type=ct,
            base_xp=int(base_xp),
            curve_a=int(curve_a),
            curve_b=int(curve_b),
        )
        await interaction.response.send_message(
            embed=discord.Embed(
                description=(
                    f"Curve set to {ct}. base_xp={base_xp}, a={curve_a}, b={curve_b}."
                )
            ),
            ephemeral=True,
        )

    # ----- announcements -----
    @config.command(
        name="announce", description="Enable or disable level up announcements."
    )
    @app_commands.describe(enabled="True to announce level ups")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_announce(self, interaction: discord.Interaction, enabled: bool):
        await update_settings(
            interaction.guild.id, announce_level_up=1 if enabled else 0
        )
        await interaction.response.send_message(
            embed=discord.Embed(
                description=f"Level up announcements set to {enabled}."
            ),
            ephemeral=True,
        )

    @config.command(
        name="announcechannel",
        description="Set a channel for level up announcements, or clear to use the current channel.",
    )
    @app_commands.describe(
        channel="Channel to post announcements in, leave empty to clear"
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_announcechannel(
        self,
        interaction: discord.Interaction,
        channel: Optional[discord.TextChannel] = None,
    ):
        ch_id = channel.id if channel else None
        await update_settings(interaction.guild.id, announce_channel_id=ch_id)
        if channel:
            await interaction.response.send_message(
                embed=discord.Embed(
                    description=f"Announcements will be posted in {channel.mention}."
                ),
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                embed=discord.Embed(
                    description=(
                        "Announcement channel cleared. Will post in the same channel as the level up event."
                    )
                ),
                ephemeral=True,
            )

    # ----- ignored channels -----
    @config.command(
        name="ignore",
        description="Manage channels that will not award XP.",
    )
    @app_commands.describe(
        action="Choose whether to add, remove, or list ignored channels.",
        channel="Channel to add or remove (required for add/remove).",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_ignore(
        self,
        interaction: discord.Interaction,
        action: Literal["add", "remove", "list"],
        channel: Optional[discord.abc.GuildChannel] = None,
    ):
        if action in {"add", "remove"}:
            if channel is None:
                return await interaction.response.send_message(
                    embed=discord.Embed(
                        description=(
                            "You must specify a channel when using the add or remove actions."
                        )
                    ),
                    ephemeral=True,
                )
            if action == "add":
                await add_ignored_channel(interaction.guild.id, channel.id)
                await interaction.response.send_message(
                    embed=discord.Embed(
                        description=f"Added {channel.mention} to ignored channels."
                    ),
                    ephemeral=True,
                )
            else:
                await remove_ignored_channel(interaction.guild.id, channel.id)
                await interaction.response.send_message(
                    embed=discord.Embed(
                        description=(
                            f"Removed {channel.mention} from ignored channels."
                        )
                    ),
                    ephemeral=True,
                )
            return

        ch_ids = await list_ignored_channels(interaction.guild.id)
        if not ch_ids:
            await interaction.response.send_message(
                embed=discord.Embed(description="No ignored channels."),
                ephemeral=True,
            )
            return

        mentions = []
        for cid in ch_ids:
            ch = interaction.guild.get_channel(cid)
            mentions.append(ch.mention if ch else f"`{cid}`")

        await interaction.response.send_message(
            embed=discord.Embed(
                description="Ignored channels:\n" + ", ".join(mentions)
            ),
            ephemeral=True,
        )

    # ----- blacklist roles -----
    @config.command(
        name="blacklistadd", description="Add a role that will not earn XP."
    )
    @app_commands.describe(role="Role to blacklist")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_blacklistadd(
        self, interaction: discord.Interaction, role: discord.Role
    ):
        await add_blacklisted_role(interaction.guild.id, role.id)
        await interaction.response.send_message(
            embed=discord.Embed(
                description=f"Added {role.mention} to blacklisted roles."
            ),
            ephemeral=True,
        )

    @config.command(
        name="blacklistremove", description="Remove a role from the blacklist."
    )
    @app_commands.describe(role="Role to remove from blacklist")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_blacklistremove(
        self, interaction: discord.Interaction, role: discord.Role
    ):
        await remove_blacklisted_role(interaction.guild.id, role.id)
        await interaction.response.send_message(
            embed=discord.Embed(
                description=f"Removed {role.mention} from blacklisted roles."
            ),
            ephemeral=True,
        )

    @config.command(name="blacklistlist", description="List blacklisted roles.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_blacklistlist(self, interaction: discord.Interaction):
        roles = await list_blacklisted_roles(interaction.guild.id)
        if not roles:
            return await interaction.response.send_message(
                embed=discord.Embed(description="No blacklisted roles."),
                ephemeral=True,
            )
        mentions = []
        for rid in roles:
            r = interaction.guild.get_role(rid)
            mentions.append(r.mention if r else f"`{rid}`")
        await interaction.response.send_message(
            embed=discord.Embed(
                description="Blacklisted roles:\n" + ", ".join(mentions)
            ),
            ephemeral=True,
        )

    # ----- role rewards -----
    @config.command(
        name="rewardset", description="Set a role reward at a specific level."
    )
    @app_commands.describe(
        level="Level at which to award the role", role="Role to award"
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_rewardset(
        self,
        interaction: discord.Interaction,
        level: app_commands.Range[int, 1, 100000],
        role: discord.Role,
    ):
        await interaction.response.defer(ephemeral=True)
        try:
            await set_role_reward(interaction.guild.id, int(level), role.id)
            await interaction.followup.send(
                embed=discord.Embed(
                    description=f"Set reward for level {level} to {role.mention}."
                ),
                ephemeral=True,
            )
        except Exception as e:
            logging.error(f"rewardset failed: {e}")
            await interaction.followup.send(
                embed=discord.Embed(description="Failed to set role reward."),
                ephemeral=True,
            )

    @config.command(
        name="rewardremove", description="Remove a role reward at a specific level."
    )
    @app_commands.describe(level="Level to remove the reward from")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_rewardremove(
        self,
        interaction: discord.Interaction,
        level: app_commands.Range[int, 1, 100000],
    ):
        await interaction.response.defer(ephemeral=True)
        try:
            await remove_role_reward(interaction.guild.id, int(level))
            await interaction.followup.send(
                embed=discord.Embed(
                    description=f"Removed reward for level {level}."
                ),
                ephemeral=True,
            )
        except Exception as e:
            logging.error(f"rewardremove failed: {e}")
            await interaction.followup.send(
                embed=discord.Embed(description="Failed to remove role reward."),
                ephemeral=True,
            )

    @config.command(name="rewardlist", description="List all role rewards.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_rewardlist(self, interaction: discord.Interaction):
        rows = await list_role_rewards(interaction.guild.id)
        if not rows:
            return await interaction.response.send_message(
                embed=discord.Embed(description="No role rewards set."),
                ephemeral=True,
            )
        lines: List[str] = []
        for r in rows:
            lvl = int(r["level"])
            role_id = int(r["role_id"])
            role = interaction.guild.get_role(role_id)
            if role:
                lines.append(f"Level {lvl} → {role.mention}")
            else:
                lines.append(f"Level {lvl} → `{role_id}`")

        await interaction.response.send_message(
            embed=discord.Embed(
                description="Role rewards:\n" + "\n".join(lines)
            ),
            ephemeral=True,
        )

    # ----- user management -----
    @config.command(name="addxp", description="Add XP to a user.")
    @app_commands.describe(member="Member to modify", amount="Amount of XP to add")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_addxp(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        amount: app_commands.Range[int, -1000000, 1000000],
    ):
        new_total, new_level, awarded = await add_xp_and_check_level_up(
            interaction.guild, member, int(amount)
        )
        msg = f"Gave {amount} XP to {member.mention}. Total XP now {new_total}, level {new_level}."
        if awarded:
            names = []
            for lvl, rid in awarded:
                role = interaction.guild.get_role(rid)
                if role:
                    names.append(f"Level {lvl}: {role.mention}")
            if names:
                msg += "\nRewards granted:\n" + "\n".join(names)
        await interaction.response.send_message(
            embed=discord.Embed(description=msg),
            ephemeral=True,
        )

    @config.command(
        name="setlevel",
        description="Set a user's level directly. XP will be set to the minimum for that level.",
    )
    @app_commands.describe(member="Member to modify", level_value="Level to set")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_setlevel(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        level_value: app_commands.Range[int, 0, 100000],
    ):
        s = await get_settings(interaction.guild.id)
        # Set XP to exact requirement for that level
        req = xp_required_for_level(
            s["curve_type"],
            int(s["base_xp"]),
            int(s["curve_a"]),
            int(s["curve_b"]),
            int(level_value),
        )
        async with _db_lock:
            cursor.execute(
                "INSERT INTO user_xp(guild_id, user_id, xp, level, last_message_ts) VALUES (?, ?, ?, ?, COALESCE((SELECT last_message_ts FROM user_xp WHERE guild_id=? AND user_id=?), 0)) "
                "ON CONFLICT(guild_id, user_id) DO UPDATE SET xp=excluded.xp, level=excluded.level",
                (
                    interaction.guild.id,
                    member.id,
                    int(req),
                    int(level_value),
                    interaction.guild.id,
                    member.id,
                ),
            )
            conn.commit()
        await interaction.response.send_message(
            embed=discord.Embed(
                description=f"Set {member.mention} to level {level_value} with XP {req}."
            ),
            ephemeral=True,
        )

    @config.command(name="resetuser", description="Reset a user's XP and level.")
    @app_commands.describe(member="Member to reset")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_resetuser(
        self, interaction: discord.Interaction, member: discord.Member
    ):
        async with _db_lock:
            cursor.execute(
                "UPDATE user_xp SET xp = 0, level = 0 WHERE guild_id = ? AND user_id = ?",
                (interaction.guild.id, member.id),
            )
            conn.commit()
        await interaction.response.send_message(
            embed=discord.Embed(
                description=f"Reset {member.mention}'s XP and level."
            ),
            ephemeral=True,
        )

    @config.command(
        name="wipeguild", description="Wipe all levelling data for this server."
    )
    @app_commands.describe(confirm="You must set this to True to proceed")
    @app_commands.checks.has_permissions(administrator=True)
    async def cfg_wipeguild(
        self, interaction: discord.Interaction, confirm: bool = False
    ):
        if not confirm:
            return await interaction.response.send_message(
                embed=discord.Embed(
                    description="You must confirm by setting confirm=True."
                ),
                ephemeral=True,
            )
        gid = interaction.guild.id
        async with _db_lock:
            cursor.execute("DELETE FROM user_xp WHERE guild_id = ?", (gid,))
            cursor.execute("DELETE FROM ignored_channels WHERE guild_id = ?", (gid,))
            cursor.execute("DELETE FROM blacklisted_roles WHERE guild_id = ?", (gid,))
            cursor.execute("DELETE FROM role_rewards WHERE guild_id = ?", (gid,))
            cursor.execute("DELETE FROM guild_settings WHERE guild_id = ?", (gid,))
            conn.commit()
        await interaction.response.send_message(
            embed=discord.Embed(
                description=
                "All levelling data wiped for this server. Defaults will be recreated on next use."
            ),
            ephemeral=True,
        )

    # ----- simulate preview numbers -----
    @config.command(
        name="simulate",
        description="Simulate XP required for a range of levels with current curve.",
    )
    @app_commands.describe(levels="Show requirements up to this level, inclusive")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_simulate(
        self, interaction: discord.Interaction, levels: app_commands.Range[int, 1, 200]
    ):
        s = await get_settings(interaction.guild.id)
        lines = []
        for lvl in range(0, int(levels) + 1):
            req = xp_required_for_level(
                s["curve_type"],
                int(s["base_xp"]),
                int(s["curve_a"]),
                int(s["curve_b"]),
                lvl,
            )
            if lvl == 0:
                lines.append(f"Level {lvl}: {req} XP (start)")
            else:
                step = xp_between_levels(
                    s["curve_type"],
                    int(s["base_xp"]),
                    int(s["curve_a"]),
                    int(s["curve_b"]),
                    lvl - 1,
                )
                lines.append(f"Level {lvl}: {req} XP total (+{step} from {lvl-1})")
        chunks = []
        chunk = []
        total_len = 0
        for line in lines:
            if total_len + len(line) + 1 > 1900:
                chunks.append("\n".join(chunk))
                chunk = []
                total_len = 0
            chunk.append(line)
            total_len += len(line) + 1
        if chunk:
            chunks.append("\n".join(chunk))

        await interaction.response.defer(ephemeral=True)
        for idx, part in enumerate(chunks):
            if idx == 0 and not interaction.response.is_done():
                try:
                    await interaction.response.send_message(
                        embed=discord.Embed(
                            description=f"Curve: **{s['curve_type']}**\n```text\n{part}\n```"
                        ),
                        ephemeral=True,
                    )
                except discord.InteractionResponded:
                    await interaction.followup.send(
                        embed=discord.Embed(
                            description=f"Curve: **{s['curve_type']}**\n```text\n{part}\n```"
                        ),
                        ephemeral=True,
                    )
            else:
                await interaction.followup.send(
                    embed=discord.Embed(description=f"```text\n{part}\n```"),
                    ephemeral=True,
                )

    # ----- recalculate levels after changing curve -----
    @config.command(
        name="recalc",
        description="Recalculate stored levels for all users based on the current curve.",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def cfg_recalc(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        s = await get_settings(interaction.guild.id)
        curve = s["curve_type"]
        base_xp = int(s["base_xp"])
        a = int(s["curve_a"])
        b = int(s["curve_b"])

        gid = interaction.guild.id
        changed = 0
        awarded_roles_total = 0

        # Pass 1: recalc for members we can address as discord.Member, which will also apply role rewards.
        async with _db_lock:
            rows = cursor.execute(
                "SELECT user_id, xp, level FROM user_xp WHERE guild_id = ?",
                (gid,),
            ).fetchall()

        member_ids = {m.id for m in interaction.guild.members}
        member_rows = [r for r in rows if int(r["user_id"]) in member_ids]
        other_rows = [r for r in rows if int(r["user_id"]) not in member_ids]

        # Members present in the guild: use the existing pipeline to update level and award roles.
        for r in member_rows:
            uid = int(r["user_id"])
            member = interaction.guild.get_member(uid)
            if member is None:
                continue
            # Add 0 XP to force recompute based on current curve and preserve XP
            new_total, new_level, awarded = await add_xp_and_check_level_up(
                interaction.guild, member, 0
            )
            if new_level != int(r["level"]):
                changed += 1
            awarded_roles_total += len(awarded)

        # Non-members or uncached users: recompute level numerically and write it back (no role awards).
        async with _db_lock:
            for r in other_rows:
                uid = int(r["user_id"])
                total = int(r["xp"])
                old_level = int(r["level"])
                new_level = level_from_total_xp(curve, base_xp, a, b, total)
                if new_level != old_level:
                    cursor.execute(
                        "UPDATE user_xp SET level = ? WHERE guild_id = ? AND user_id = ?",
                        (new_level, gid, uid),
                    )
                    changed += 1
            conn.commit()

        await interaction.followup.send(
            embed=discord.Embed(
                description=(
                    f"Recalculated levels using curve **{curve}** (base_xp={base_xp}, a={a}, b={b}).\n"
                    f"Updated members: **{changed}**. Role rewards granted: **{awarded_roles_total}**."
                )
            ),
            ephemeral=True,
        )

# ======================================================================================
# Context menu (must be defined at module level, not inside a class)
# ======================================================================================


@app_commands.context_menu(name="View Level Profile")
async def view_level_profile(interaction: discord.Interaction, member: discord.Member):
    try:
        if not interaction.guild:
            return await interaction.response.send_message(
                embed=discord.Embed(description="This can only be used in a server."),
                ephemeral=True,
            )
        record = await get_user_record(interaction.guild.id, member.id)
        settings = await get_settings(interaction.guild.id)
        total = int(record["xp"])
        level_val = int(record["level"])
        rank = await user_rank(interaction.guild.id, member.id)

        to_next = xp_between_levels(
            settings["curve_type"],
            int(settings["base_xp"]),
            int(settings["curve_a"]),
            int(settings["curve_b"]),
            level_val,
        )
        prev_req = xp_required_for_level(
            settings["curve_type"],
            int(settings["base_xp"]),
            int(settings["curve_a"]),
            int(settings["curve_b"]),
            level_val,
        )
        progress = max(0, total - prev_req)

        embed = create_profile_embed(
            member,
            level_val,
            rank,
            total,
            progress,
            to_next,
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
    except Exception as e:
        logging.error(f"context profile failed: {e}")
        try:
            await interaction.response.send_message(
                embed=discord.Embed(description="Failed to fetch profile."),
                ephemeral=True,
            )
        except discord.InteractionResponded:
            await interaction.followup.send(
                embed=discord.Embed(description="Failed to fetch profile."),
                ephemeral=True,
            )


# Store a reference so we can remove it on teardown if needed
_VIEW_PROFILE_CM = view_level_profile


async def setup(bot: commands.Bot):
    await bot.add_cog(LevelSystem(bot))
    # Register the context menu at the tree level
    bot.tree.add_command(_VIEW_PROFILE_CM)


async def teardown(bot: commands.Bot):
    # Cleanly remove the context menu if the extension is unloaded
    bot.tree.remove_command(_VIEW_PROFILE_CM.name, type=discord.AppCommandType.user)
