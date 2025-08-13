import discord
import logging
import yaml
import re
from discord.ext import commands, tasks
import datetime
import asyncio
from typing import Optional, Dict, Any, Set
from collections import defaultdict


def audit_log(message: str):
    """Append a timestamped message to the audit log file."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open("audit.log", "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {message}\n")
    except Exception as e:
        logging.error(f"Failed to write to audit.log: {e}")


class MemberStats(commands.Cog):
    """
    Maintains a locked voice channel that shows the current server member count.
    Creation and behaviour are driven only by config.yaml keys.
    The script never writes to config.yaml and never asks for runtime input.
    """

    CONFIG_PATH = "config.yaml"

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config: Dict[str, Any] = self._load_config()

        # Settings pulled from config.yaml with sensible defaults
        self.stats_category_name: str = self.config.get(
            "stats_category_name", "📊 Server Stats"
        )
        self.name_template: str = self.config.get(
            "member_count_name_format", "Members: {count}"
        )
        self.fallback_name_template: str = self.config.get(
            "member_count_fallback_name_format", "members-{count}"
        )
        self.include_bots: bool = self.config.get("member_count_include_bots", True)
        self.auto_repair: bool = self.config.get("member_count_auto_repair", True)
        self.periodic_refresh_minutes: int = int(
            self.config.get("member_count_refresh_minutes", 60)
        )
        # New: coalesce rapid renames to avoid 429s
        self.min_rename_seconds: int = int(
            self.config.get("member_count_min_rename_seconds", 300)
        )

        # In-memory runtime cache only. Never persisted.
        # Maps guild_id -> channel_id for quick access within the current process.
        self._channel_cache: Dict[int, int] = {}

        # Tracks guilds configured during this runtime.
        self._configured_guilds: Set[int] = set()

        # Concurrency guards per guild
        self._guild_locks: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)

        # Per-guild rename coalescing state
        self._last_rename_at: Dict[int, float] = {}          # monotonic timestamps
        self._pending_counts: Dict[int, int] = {}
        self._rename_tasks: Dict[int, asyncio.Task] = {}

        # Guilds where the primary template was rejected by Discovery filters
        self._discovery_blocked_guilds: Set[int] = set()

        # Precompile name-matching regexes used to rediscover the channel at startup
        self._name_regex = self._compile_name_regex(self.name_template)
        self._fallback_name_regex = self._compile_name_regex(self.fallback_name_template)

    # ---------------------------
    # Lifecycle
    # ---------------------------

    @commands.Cog.listener()
    async def on_ready(self):
        logging.info("MemberStats cog synced successfully.")
        audit_log("MemberStats cog synced successfully.")

        if not getattr(self.bot, "intents", None) or not self.bot.intents.members:
            msg = (
                "Warning: Server Members Intent is disabled. Live member updates may be delayed. "
                "Enable it in the Developer Portal and pass intents when creating the bot client."
            )
            logging.warning(msg)
            audit_log(msg)

        # Verify or adopt existing channels based on config-driven discovery.
        for guild in self.bot.guilds:
            try:
                await self._verify_or_adopt_for_guild(guild)
            except Exception as e:
                logging.error(
                    f"[Startup verify] Guild '{guild.name}' ({guild.id}) error: {e}",
                    exc_info=True,
                )
                audit_log(
                    f"[Startup verify] Error in guild '{guild.name}' ({guild.id}): {e}"
                )

        # Start periodic refresh after verification
        if not self.periodic_refresh.is_running():
            self.periodic_refresh.change_interval(
                minutes=max(5, self.periodic_refresh_minutes)
            )
            self.periodic_refresh.start()

    # ---------------------------
    # Commands
    # ---------------------------

    @commands.hybrid_command(
        name="setupmembercount",
        aliases=["membercount", "setup_member_count"],
        description="Create or adopt a locked voice channel that shows the current member count, based on config.yaml.",
    )
    @commands.guild_only()
    @commands.has_permissions(manage_channels=True)
    @commands.bot_has_permissions(manage_channels=True)
    async def setup_member_count(self, ctx: commands.Context):
        """
        Create the stats category and locked voice channel for this server based on config.yaml.
        No runtime inputs are accepted. Never writes to config.
        """
        guild = ctx.guild
        assert guild is not None

        try:
            async with self._guild_lock(guild.id):
                # If a channel already exists that matches either template, adopt and enforce settings
                channel = await self._get_or_discover_channel(guild)
                if channel is None:
                    # Fresh creation
                    channel = await self._create_member_count_channel_fresh(
                        guild=guild,
                        category_name=self.stats_category_name,
                        name_template=self.name_template,
                        include_bots=self.include_bots,
                        reason=f"Requested by {ctx.author} via /setupmembercount",
                    )
                else:
                    # Already exists, just move, lock, and queue rename if needed
                    await self._place_and_lock_and_rename(
                        channel=channel,
                        category_name=self.stats_category_name,
                        include_bots=self.include_bots,
                        reason=f"Verify via /setupmembercount by {ctx.author}",
                    )

                # Mark this guild as configured in-memory
                self._configured_guilds.add(guild.id)

            await ctx.reply(
                f"Member count channel is set to **#{channel.name}** in **{channel.category.name if channel.category else 'no category'}**.",
                mention_author=False,
            )

        except discord.Forbidden:
            await ctx.reply(
                "I do not have permission to manage channels here.",
                mention_author=False,
            )
        except discord.HTTPException as e:
            logging.error(
                f"Error while creating or updating the channel: {e}",
                exc_info=True,
            )
            await ctx.reply(
                "I could not set up the channel. Please try again later.",
                mention_author=False,
            )
        except Exception as e:
            logging.error(f"Unexpected error in setup_member_count: {e}", exc_info=True)
            audit_log(
                f"Unexpected error in setup_member_count for guild '{guild.name}' ({guild.id}): {e}"
            )
            await ctx.reply(
                "Something went wrong while setting up the member count. Please try again later.",
                mention_author=False,
            )

    @setup_member_count.error
    async def setup_member_count_error(
        self, ctx: commands.Context, error: commands.CommandError
    ):
        if isinstance(error, commands.MissingPermissions):
            await ctx.reply(
                "You need the Manage Channels permission to run this.",
                mention_author=False,
            )
        elif isinstance(error, commands.BotMissingPermissions):
            await ctx.reply(
                "I need the Manage Channels permission to do that.",
                mention_author=False,
            )
        elif isinstance(error, commands.NoPrivateMessage):
            await ctx.reply(
                "This command must be used in a server.", mention_author=False
            )
        else:
            logging.error(f"setup_member_count_error: {error}", exc_info=True)
            await ctx.reply(
                "I could not run that command right now. Please try again later.",
                mention_author=False,
            )

    @commands.hybrid_command(
        name="refreshmembercount",
        aliases=["refresh_member_count"],
        description="Manually refresh the member count channel name.",
    )
    @commands.guild_only()
    async def refresh_member_count(self, ctx: commands.Context):
        guild = ctx.guild
        if guild is None:
            await ctx.reply(
                "This command must be used in a server.", mention_author=False
            )
            return

        try:
            async with self._guild_lock(guild.id):
                channel = await self._get_or_discover_channel(guild)
                if channel is None:
                    await ctx.reply(
                        "No member count channel detected. Run /setupmembercount.",
                        mention_author=False,
                    )
                    return
                updated = await self._queue_rename(
                    guild, reason=f"Manual refresh by {ctx.author}"
                )
            if updated:
                await ctx.reply(
                    "Queued a refresh of the member count channel name.",
                    mention_author=False,
                )
            else:
                await ctx.reply(
                    "Could not find a suitable channel. Try /setupmembercount.",
                    mention_author=False,
                )
        except discord.Forbidden:
            await ctx.reply(
                "I do not have permission to manage channels here.",
                mention_author=False,
            )
        except discord.HTTPException as e:
            logging.error(
                f"Error while updating the channel name: {e}", exc_info=True
            )
            await ctx.reply(
                "I ran into a problem while updating the channel. Please try again later.",
                mention_author=False,
            )
        except Exception as e:
            logging.error(
                f"Unexpected error in refresh_member_count: {e}", exc_info=True
            )
            audit_log(
                f"Unexpected error in refresh_member_count for guild '{guild.name}' ({guild.id}): {e}"
            )
            await ctx.reply(
                "Something went wrong while refreshing the member count. Please try again later.",
                mention_author=False,
            )

    @commands.hybrid_command(
        name="removemembercount",
        aliases=["remove_member_count", "deletemembercount"],
        description="Remove the member count voice channel. Does not modify config.",
    )
    @commands.guild_only()
    @commands.has_permissions(manage_channels=True)
    @commands.bot_has_permissions(manage_channels=True)
    async def remove_member_count(self, ctx: commands.Context):
        guild = ctx.guild
        if guild is None:
            await ctx.reply(
                "This command must be used in a server.", mention_author=False
            )
            return

        async with self._guild_lock(guild.id):
            ch = await self._get_or_discover_channel(guild)
            if ch is None:
                await ctx.reply(
                    "No member count channel was found to remove.",
                    mention_author=False,
                )
                return

            try:
                await ch.delete(
                    reason=f"Requested by {ctx.author} via /removemembercount"
                )
            except discord.Forbidden:
                await ctx.reply(
                    "I do not have permission to delete that channel.",
                    mention_author=False,
                )
                return
            except discord.HTTPException as e:
                logging.error(
                    f"Error while deleting member count channel: {e}",
                    exc_info=True,
                )
                await ctx.reply(
                    "I ran into a problem while deleting that channel. Please try again later.",
                    mention_author=False,
                )
                return

            # Clear runtime cache and configured flag
            self._channel_cache.pop(guild.id, None)
            self._configured_guilds.discard(guild.id)

        await ctx.reply("Member count channel removed.", mention_author=False)
        audit_log(
            f"Removed member count channel in guild '{guild.name}' ({guild.id}) by {ctx.author} ({ctx.author.id})."
        )

    # ---------------------------
    # Event listeners
    # ---------------------------

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        # Queue a rename rather than hammering the API
        try:
            async with self._guild_lock(member.guild.id):
                channel = await self._get_or_discover_channel(member.guild)
                if channel is None:
                    return
                await self._queue_rename(
                    member.guild, reason=f"Member joined: {member} ({member.id})"
                )
        except Exception as e:
            logging.error(
                f"on_member_join update error in guild '{member.guild.name}' ({member.guild.id}): {e}",
                exc_info=True,
            )
            audit_log(
                f"Error updating count on join in guild '{member.guild.name}' ({member.guild.id}): {e}"
            )

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        try:
            async with self._guild_lock(member.guild.id):
                channel = await self._get_or_discover_channel(member.guild)
                if channel is None:
                    return
                await self._queue_rename(
                    member.guild, reason=f"Member left: {member} ({member.id})"
                )
        except Exception as e:
            logging.error(
                f"on_member_remove update error in guild '{member.guild.name}' ({member.guild.id}): {e}",
                exc_info=True,
            )
            audit_log(
                f"Error updating count on leave in guild '{member.guild.name}' ({member.guild.id}): {e}"
            )

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        """If the member count channel is deleted, optionally auto-repair based on runtime state."""
        try:
            guild = channel.guild
            cached_id = self._channel_cache.get(guild.id)
            if cached_id and channel.id == cached_id:
                audit_log(
                    f"Configured member count channel deleted in guild '{guild.name}' ({guild.id})."
                )
                logging.warning("Member count channel was deleted.")
                # Clear runtime cache
                self._channel_cache.pop(guild.id, None)
                # Auto-repair only if enabled and we had previously configured this guild in this runtime
                if self.auto_repair and guild.id in self._configured_guilds:
                    async with self._guild_lock(guild.id):
                        await self._ensure_member_count_channel(
                            guild=guild,
                            category_name=self.stats_category_name,
                            name_template=self.name_template,
                            include_bots=self.include_bots,
                            reason="Auto-repair after deletion",
                        )
        except Exception as e:
            logging.error(f"on_guild_channel_delete error: {e}", exc_info=True)

    # ---------------------------
    # Background task
    # ---------------------------

    @tasks.loop(minutes=60.0)
    async def periodic_refresh(self):
        """Periodic safety refresh. Skips guilds with no discoverable channel."""
        for guild in list(self.bot.guilds):
            try:
                channel = await self._get_or_discover_channel(guild)
                if channel is None:
                    continue
                async with self._guild_lock(guild.id):
                    await self._queue_rename(guild, reason="Periodic refresh")
            except Exception as e:
                logging.error(
                    f"[Periodic refresh] Guild '{guild.name}' ({guild.id}) error: {e}",
                    exc_info=True,
                )

    @periodic_refresh.before_loop
    async def before_periodic_refresh(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(10)

    # ---------------------------
    # Internal helpers
    # ---------------------------

    def _load_config(self) -> Dict[str, Any]:
        """Load YAML config. Never writes or creates the file."""
        try:
            with open(self.CONFIG_PATH, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
                if not isinstance(cfg, dict):
                    logging.warning(
                        "Config did not parse to a dict. Using empty defaults."
                    )
                    return {}
                return cfg
        except FileNotFoundError:
            logging.error("config.yaml not found. Proceeding with defaults in memory.")
            audit_log("config.yaml not found. Proceeding with defaults in memory.")
            return {}
        except Exception as e:
            logging.error(f"Error loading config.yaml: {e}", exc_info=True)
            audit_log(f"Error loading config.yaml: {e}")
            return {}

    def _compile_name_regex(self, template: str) -> re.Pattern:
        r"""
        Build a regex that matches a channel name derived from the template.
        Example: 'Members: {count}' -> r'^Members:\s*\d+$'
        """
        # Escape everything, then replace the escaped {count} with a digit capture
        escaped = re.escape(template)
        escaped = escaped.replace(re.escape("{count}"), r"(\d+)")
        pattern = r"^" + escaped + r"$"
        return re.compile(pattern)

    def _format_name_for_guild(self, guild_id: int, count: int) -> str:
        """
        Choose template per guild. If Discovery blocked earlier, use fallback.
        Ensure final name fits Discord's 100-char limit.
        """
        template = (
            self.fallback_name_template
            if guild_id in self._discovery_blocked_guilds
            else self.name_template
        )
        name = template.format(count=count)
        if len(name) > 100:
            suffix = f" {count}"
            name = (name[: 100 - len(suffix)]).rstrip() + suffix
        return name

    def _current_member_count(
        self, guild: discord.Guild, include_bots: Optional[bool] = None
    ) -> int:
        """Get the current member count with an option to exclude bots."""
        include_bots = self.include_bots if include_bots is None else include_bots
        if include_bots:
            return guild.member_count
        if not self.bot.intents.members:
            logging.warning(
                f"[{guild.name}] Members intent disabled. Counting non-bots may be inaccurate."
            )
            return sum(1 for m in guild.members if not getattr(m, "bot", False))
        return sum(1 for m in guild.members if not m.bot)

    async def _get_or_discover_channel(
        self, guild: discord.Guild
    ) -> Optional[discord.VoiceChannel]:
        """
        Return the member count channel if known or discoverable by name pattern.
        Uses runtime cache first, then searches by template-derived regexes.
        """
        # Cached
        ch_id = self._channel_cache.get(guild.id)
        if ch_id:
            ch = guild.get_channel(ch_id)
            if isinstance(ch, discord.VoiceChannel):
                return ch
            # If cached id no longer resolves, drop it
            self._channel_cache.pop(guild.id, None)

        # Discover by name pattern (primary then fallback)
        channel = self._find_member_count_channel(guild)
        if channel:
            self._channel_cache[guild.id] = channel.id
            self._configured_guilds.add(guild.id)
            return channel
        return None

    def _find_member_count_channel(
        self, guild: discord.Guild
    ) -> Optional[discord.VoiceChannel]:
        """
        Search for a voice channel whose name matches the template pattern.
        Preference is given to channels under the configured stats category.
        Recognises both primary and fallback templates.
        """
        matches: list[discord.VoiceChannel] = []

        # First gather all voice channels that match either name pattern
        for ch in guild.voice_channels:
            if self._name_regex.match(ch.name) or self._fallback_name_regex.match(
                ch.name
            ):
                matches.append(ch)

        if not matches:
            return None

        # Prefer channel within the stats category name, if that category exists
        target_category = None
        for cat in guild.categories:
            if cat.name.casefold() == self.stats_category_name.casefold():
                target_category = cat
                break

        if target_category:
            in_cat = [c for c in matches if c.category_id == target_category.id]
            if in_cat:
                return in_cat[0]

        # Fallback to first match
        return matches[0]

    async def _find_or_create_stats_category(
        self, guild: discord.Guild, name: str
    ) -> discord.CategoryChannel:
        """Find a category by name (case-insensitive) or create it."""
        for cat in guild.categories:
            if cat.name.casefold() == name.casefold():
                return cat

        try:
            category = await guild.create_category(
                name=name, reason="Create stats category"
            )
            audit_log(
                f"Created stats category '{name}' in guild '{guild.name}' ({guild.id})."
            )
            logging.info(f"[{guild.name}] Created stats category '{name}'.")
            return category
        except discord.Forbidden as e:
            logging.error(f"Forbidden creating category in '{guild.name}': {e}")
            audit_log(f"Forbidden creating category in '{guild.name}' ({guild.id}).")
            raise
        except discord.HTTPException as e:
            logging.error(f"HTTPException creating category in '{guild.name}': {e}")
            audit_log(
                f"HTTPException creating category in '{guild.name}' ({guild.id}). Error: {e}"
            )
            raise

    async def _create_member_count_channel_fresh(
        self,
        guild: discord.Guild,
        category_name: str,
        name_template: str,
        include_bots: bool,
        reason: str,
    ) -> discord.VoiceChannel:
        """Create the voice channel for the first time. Does not write to config."""
        # Temporarily apply template and include_bots for this call
        original_template = self.name_template
        original_include = self.include_bots
        self.name_template = name_template
        self.include_bots = include_bots
        self._name_regex = self._compile_name_regex(self.name_template)

        try:
            category = await self._find_or_create_stats_category(guild, category_name)
            everyone = guild.default_role
            overwrites = {
                everyone: discord.PermissionOverwrite(
                    view_channel=True, connect=False, speak=False, stream=False
                )
            }
            count = self._current_member_count(guild, include_bots)
            desired_name = self._format_name_for_guild(guild.id, count)
            channel = await guild.create_voice_channel(
                name=desired_name,
                category=category,
                overwrites=overwrites,
                reason=reason,
            )

            # Cache in memory for this runtime
            self._channel_cache[guild.id] = channel.id
            self._configured_guilds.add(guild.id)

            logging.info(
                f"[{guild.name}] Created member count channel #{channel.name} ({channel.id})."
            )
            audit_log(
                f"Created member count channel #{channel.name} ({channel.id}) in guild '{guild.name}' ({guild.id})."
            )
            return channel
        finally:
            self.name_template = original_template
            self.include_bots = original_include
            self._name_regex = self._compile_name_regex(self.name_template)

    async def _place_and_lock_and_rename(
        self,
        channel: discord.VoiceChannel,
        category_name: str,
        include_bots: bool,
        reason: str,
    ):
        """Move to category if needed, lock overwrites, then queue a rename if needed."""
        category = await self._find_or_create_stats_category(
            channel.guild, category_name
        )
        if channel.category_id != category.id:
            try:
                await channel.edit(category=category, reason="Move to stats category")
                audit_log(
                    f"Moved member count channel to stats category in guild '{channel.guild.name}' ({channel.guild.id})."
                )
                logging.info(
                    f"[{channel.guild.name}] Moved member count channel to '{category.name}'."
                )
            except discord.HTTPException as e:
                logging.warning(
                    f"Failed moving channel category in '{channel.guild.name}': {e}"
                )

        everyone = channel.guild.default_role
        current_overwrites = channel.overwrites_for(everyone)
        should_update = (
            current_overwrites.view_channel is not True
            or current_overwrites.connect is not False
            or current_overwrites.speak is not False
            or current_overwrites.stream is not False
        )
        if should_update:
            try:
                await channel.set_permissions(
                    target=everyone,
                    view_channel=True,
                    connect=False,
                    speak=False,
                    stream=False,
                    reason="Lock member count voice channel",
                )
            except discord.HTTPException as e:
                logging.warning(
                    f"Failed updating overwrites in '{channel.guild.name}': {e}"
                )

        # Queue a rename rather than doing it immediately
        await self._queue_rename(channel.guild, reason)

    async def _ensure_member_count_channel(
        self,
        guild: discord.Guild,
        category_name: str,
        name_template: str,
        include_bots: bool,
        reason: str,
    ) -> discord.VoiceChannel:
        """
        Ensure the channel exists if we previously knew about it this runtime.
        Will create a replacement only when the guild is marked configured already.
        """
        if guild.id not in self._configured_guilds:
            raise RuntimeError("Attempted to ensure channel for an unconfigured guild.")

        # Temporarily apply template and include_bots for this call
        original_template = self.name_template
        original_include = self.include_bots
        self.name_template = name_template
        self.include_bots = include_bots
        self._name_regex = self._compile_name_regex(self.name_template)

        try:
            channel = await self._get_or_discover_channel(guild)
            if channel is None:
                # Create a replacement
                category = await self._find_or_create_stats_category(
                    guild, category_name
                )
                everyone = guild.default_role
                overwrites = {
                    everyone: discord.PermissionOverwrite(
                        view_channel=True, connect=False, speak=False, stream=False
                    )
                }
                count = self._current_member_count(guild, include_bots)
                desired_name = self._format_name_for_guild(guild.id, count)
                channel = await guild.create_voice_channel(
                    name=desired_name,
                    category=category,
                    overwrites=overwrites,
                    reason=reason,
                )

                # Cache new id and keep configured flag
                self._channel_cache[guild.id] = channel.id

                logging.info(
                    f"[{guild.name}] Recreated member count channel #{channel.name} ({channel.id})."
                )
                audit_log(
                    f"Recreated member count channel #{channel.name} ({channel.id}) in guild '{guild.name}' ({guild.id})."
                )
            else:
                # Already exists, just place, lock, and queue rename
                await self._place_and_lock_and_rename(
                    channel=channel,
                    category_name=category_name,
                    include_bots=include_bots,
                    reason=reason,
                )

            return channel
        finally:
            self.name_template = original_template
            self.include_bots = original_include
            self._name_regex = self._compile_name_regex(self.name_template)

    async def _queue_rename(self, guild: discord.Guild, reason: str) -> bool:
        """
        Queue a rename respecting per-guild min interval. Returns False if no channel.
        """
        channel = await self._get_or_discover_channel(guild)
        if channel is None:
            return False

        count = self._current_member_count(guild, self.include_bots)
        self._pending_counts[guild.id] = count

        # Spawn or wake the worker for this guild
        if guild.id not in self._rename_tasks or self._rename_tasks[guild.id].done():
            self._rename_tasks[guild.id] = asyncio.create_task(
                self._rename_worker(guild, reason), name=f"memberstats_rename_{guild.id}"
            )
        return True

    async def _rename_worker(self, guild: discord.Guild, reason: str):
        """
        Coalescing worker: ensures at least min_rename_seconds between edits.
        It applies the latest pending count when the window allows.
        """
        gid = guild.id
        # Loop while there is a pending value that we have not applied
        while True:
            desired = self._pending_counts.get(gid)
            if desired is None:
                return  # nothing more to do

            # Respect min interval
            now = asyncio.get_running_loop().time()
            last = self._last_rename_at.get(gid, 0.0)
            wait_for = (last + float(self.min_rename_seconds)) - now
            if wait_for > 0:
                await asyncio.sleep(wait_for)

            # Recheck desired just before editing
            desired = self._pending_counts.get(gid)
            if desired is None:
                return

            try:
                async with self._guild_lock(gid):
                    channel = await self._get_or_discover_channel(guild)
                    if channel is None:
                        # Nothing to do; clear pending and exit
                        self._pending_counts.pop(gid, None)
                        return
                    await self._apply_rename(channel, desired, reason)
                    self._last_rename_at[gid] = asyncio.get_running_loop().time()
                    # Clear one applied value but keep looping in case a new one arrives
                    self._pending_counts.pop(gid, None)
            except Exception as e:
                logging.error(
                    f"Rename worker error in guild '{guild.name}' ({gid}): {e}",
                    exc_info=True,
                )
                # Back off slightly to avoid hot loops on persistent errors
                await asyncio.sleep(10)

    async def _apply_rename(self, channel: discord.VoiceChannel, count: int, reason: str):
        """Attempt to rename. If Discovery blocks, switch to fallback template for this guild and retry once."""
        desired = self._format_name_for_guild(channel.guild.id, count)
        if channel.name == desired:
            return
        try:
            await channel.edit(name=desired, reason=reason)
            logging.info(
                f"[{channel.guild.name}] Renamed member count channel to '{desired}'."
            )
            audit_log(
                f"Renamed member count channel to '{desired}' in guild '{channel.guild.name}' ({channel.guild.id})."
            )
        except discord.HTTPException as e:
            # 50035 Invalid Form Body with Discovery filter complaint
            text = str(e)
            if e.code == 50035 and "Contains words not allowed for servers in Server Discovery" in text:
                gid = channel.guild.id
                if gid not in self._discovery_blocked_guilds:
                    self._discovery_blocked_guilds.add(gid)
                    safe_name = self._format_name_for_guild(gid, count)  # uses fallback now
                    try:
                        await channel.edit(name=safe_name, reason=reason + " (discovery-safe fallback)")
                        logging.info(
                            f"[{channel.guild.name}] Discovery-safe rename applied: '{safe_name}'."
                        )
                        audit_log(
                            f"Discovery-safe rename applied to '{safe_name}' in guild '{channel.guild.name}' ({channel.guild.id})."
                        )
                        return
                    except Exception as inner:
                        logging.error(
                            f"Failed applying discovery-safe rename in '{channel.guild.name}': {inner}",
                            exc_info=True,
                        )
                # If already blocked or retry failed, re-raise original
            raise
        except discord.Forbidden:
            logging.error(
                f"Forbidden renaming channel #{channel.name} in '{channel.guild.name}'."
            )
            audit_log(
                f"Forbidden renaming member count channel in guild '{channel.guild.name}' ({channel.guild.id})."
            )

    async def _verify_or_adopt_for_guild(self, guild: discord.Guild):
        """
        At startup, adopt an existing channel that matches the template.
        Never create new channels at startup for unknown guilds.
        """
        async with self._guild_lock(guild.id):
            channel = self._find_member_count_channel(guild)
            if channel is None:
                return

            # Adopt it
            self._channel_cache[guild.id] = channel.id
            self._configured_guilds.add(guild.id)

            # If the adopted name matches the fallback regex, mark guild as discovery-blocked
            if self._fallback_name_regex.match(channel.name):
                self._discovery_blocked_guilds.add(guild.id)

            # Ensure overwrites and queue name verification
            everyone = guild.default_role
            current_overwrites = channel.overwrites_for(everyone)
            if not (
                current_overwrites.view_channel is True
                and current_overwrites.connect is False
                and current_overwrites.speak is False
                and current_overwrites.stream is False
            ):
                try:
                    await channel.set_permissions(
                        target=everyone,
                        view_channel=True,
                        connect=False,
                        speak=False,
                        stream=False,
                        reason="Verify lock at startup",
                    )
                except Exception as e:
                    logging.warning(
                        f"Failed to enforce overwrites at startup in '{guild.name}': {e}"
                    )

            await self._queue_rename(guild, reason="Startup verify")

    # ---------------------------
    # Concurrency helper
    # ---------------------------

    def _guild_lock(self, guild_id: int) -> asyncio.Lock:
        return self._guild_locks[guild_id]

    # ---------------------------
    # Cog teardown
    # ---------------------------

    def cog_unload(self):
        if self.periodic_refresh.is_running():
            self.periodic_refresh.cancel()
        # Cancel any running rename workers
        for task in list(self._rename_tasks.values()):
            try:
                task.cancel()
            except Exception:
                pass


async def setup(bot: commands.Bot):
    await bot.add_cog(MemberStats(bot))
