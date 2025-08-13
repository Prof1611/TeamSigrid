import discord
import logging
import yaml
from discord import app_commands
from discord.ext import commands
import asyncio
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import requests  # For HTTP requests to the Live page
import unicodedata
import string
from bs4 import BeautifulSoup  # For HTML parsing
from typing import List, Tuple, Optional


def audit_log(message: str):
    """Append a timestamped message to the audit log file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open("audit.log", "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {message}\n")
    except Exception as e:
        logging.error(f"Failed to write to audit.log: {e}")


def normalize_string(s: str) -> str:
    """
    Normalize a string by removing diacritics, punctuation, extra whitespace,
    and converting to lowercase.
    """
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("utf-8")
    s = s.translate(str.maketrans("", "", string.punctuation))
    return " ".join(s.split()).lower()


class Scrape(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # Load the config file with UTF-8 encoding.
        with open("config.yaml", "r", encoding="utf-8") as config_file:
            self.config = yaml.safe_load(config_file) or {}
        # The URL containing the <section id="live"> structure.
        self.LIVE_PAGE_URL = "https://www.thisissigrid.com/"
        audit_log("Scrape cog initialised and configuration loaded successfully.")

    @commands.Cog.listener()
    async def on_ready(self):
        logging.info(f"\033[96mScrape\033[0m cog synced successfully.")
        audit_log("Scrape cog synced successfully.")

    @app_commands.command(
        name="scrape",
        description="Checks the band's website for new shows and updates #live-shows and server events.",
    )
    async def scrape(self, interaction: discord.Interaction):
        # This command must run in a guild, not in DMs.
        if interaction.guild is None:
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="Unavailable in DMs",
                    description="Please run `/scrape` inside a server.",
                    color=discord.Color.red(),
                ),
                ephemeral=True,
            )
            audit_log(
                f"{getattr(interaction.user, 'name', 'Unknown')} attempted /scrape in DMs. Blocked."
            )
            return

        await interaction.response.defer()
        user_name = getattr(interaction.user, "name", "Unknown")
        user_id = getattr(interaction.user, "id", "Unknown")
        guild_name = getattr(interaction.guild, "name", "Unknown Guild")
        guild_id = getattr(interaction.guild, "id", "Unknown")

        audit_log(
            f"{user_name} (ID: {user_id}) invoked /scrape command in guild '{guild_name}' (ID: {guild_id})."
        )
        try:
            audit_log("Starting scraping process via /scrape command.")
            # Run the scraper asynchronously in a separate thread.
            new_entries = await asyncio.to_thread(self.run_scraper)
            audit_log(
                f"{user_name} (ID: {user_id}) retrieved {len(new_entries)} new entries from the website."
            )
            # Create forum threads and get count.
            threads_created = await self.check_forum_threads(
                interaction.guild, interaction, new_entries
            )
            # Create scheduled events and get count.
            events_created = await self.check_server_events(
                interaction.guild, interaction, new_entries
            )
            # Send a combined summary.
            await self.send_combined_summary(
                interaction, threads_created, events_created
            )
            logging.info(
                f"Full scrape and creation process done: {threads_created} threads, {events_created} events created."
            )
            audit_log("Scrape process completed successfully.")
        except Exception as e:
            logging.error(f"An error occurred in the scrape command: {e}")
            audit_log(
                f"{user_name} (ID: {user_id}) encountered an error in /scrape command: {e}"
            )
            error_embed = discord.Embed(
                title="Error",
                description="Something went wrong while checking the website. Please try again later.",
                color=discord.Color.red(),
            )
            try:
                await interaction.followup.send(embed=error_embed)
            except Exception:
                # Fallback if followup isn't available for some reason
                await interaction.channel.send(embed=error_embed)

    # ---------------------------
    # HTML scraping for #live
    # ---------------------------

    def run_scraper(self) -> List[Tuple[str, str, str, Optional[str]]]:
        """
        Scrape Sigrid's homepage for the <section id="live"> structure and extract
        each <div class="live-date"> block. Return a list of tuples:
            [(formatted_date, venue, location, tickets_url), ...]
        where:
          formatted_date = "16 August 2025"
          venue = "MS Dockville 2025"
          location = "Hamburg, DE"  or "大阪市" if country absent
          tickets_url = "https://..." or None
        """
        logging.info("Running scraper by parsing HTML from Sigrid's live section...")
        audit_log(
            "Starting scraper: Requesting event data from homepage #live section."
        )
        new_entries: List[Tuple[str, str, str, Optional[str]]] = []

        try:
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/126.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "en-GB,en;q=0.9",
            }
            response = requests.get(self.LIVE_PAGE_URL, timeout=15, headers=headers)
            response.raise_for_status()
            html = response.text

            # Parse the HTML with BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")

            # Find the <section id="live"> and inside it the <div id="live-dates">
            live_section = soup.find("section", id="live")
            if not live_section:
                logging.error("Could not find <section id='live'> on the page.")
                audit_log("Error: <section id='live'> not found on homepage.")
                return []

            live_dates = live_section.find("div", id="live-dates")
            if not live_dates:
                logging.error("Could not find <div id='live-dates'> within #live.")
                audit_log("Error: <div id='live-dates'> not found in #live section.")
                return []

            # Each <div class="live-date"> is one show
            items = live_dates.find_all("div", class_="live-date")
            logging.info(f"Found {len(items)} <div class='live-date'> elements.")
            audit_log(f"Found {len(items)} shows in #live section.")

            for div in items:
                try:
                    # Extract fields
                    date_tag = div.find("p", class_="date")
                    venue_tag = div.find("p", class_="venue")
                    location_tag = div.find("p", class_="location")
                    ticket_tag = div.find("a", class_="tickets")

                    raw_date = date_tag.get_text(strip=True) if date_tag else ""
                    venue = venue_tag.get_text(strip=True) if venue_tag else ""
                    raw_location = (
                        location_tag.get_text(strip=True) if location_tag else ""
                    )
                    tickets_url = ticket_tag.get("href") if ticket_tag else None

                    formatted_date = self._parse_live_date(raw_date)
                    location = self._clean_location(raw_location)

                    new_entries.append((formatted_date, venue, location, tickets_url))
                    logging.debug(
                        f"Parsed entry: ({formatted_date}, {venue}, {location}, {tickets_url})"
                    )
                    audit_log(
                        f"Processed show: {formatted_date} @ {venue}, {location} | Tickets: {tickets_url or 'None'}"
                    )

                except Exception as inner_e:
                    logging.error(f"Error parsing one <div.live-date>: {inner_e}")
                    audit_log(f"Error parsing <div class='live-date'>: {inner_e}")
                    continue

        except Exception as e:
            logging.error(f"Error fetching homepage live section: {e}")
            audit_log(f"Error fetching HTML from {self.LIVE_PAGE_URL}: {e}")

        return new_entries

    @staticmethod
    def _strip_ordinal(day_text: str) -> str:
        """Remove English ordinal suffixes from a day string like '16th' -> '16'."""
        return (
            day_text.replace("st", "")
            .replace("nd", "")
            .replace("rd", "")
            .replace("th", "")
        )

    def _parse_live_date(self, raw: str) -> str:
        """
        Convert raw date like '16th Aug 2025' into '16 August 2025'.
        Falls back to the raw string if parsing fails.
        """
        try:
            parts = raw.split()
            if len(parts) == 3:
                day = self._strip_ordinal(parts[0])
                month_abbr = parts[1]
                year = parts[2]
                dt = datetime.strptime(f"{day} {month_abbr} {year}", "%d %b %Y")
                return dt.strftime("%d %B %Y")
            return raw
        except Exception as e:
            logging.error(f"Error parsing live date '{raw}': {e}")
            audit_log(f"Error parsing live date '{raw}': {e}")
            return raw

    @staticmethod
    def _clean_location(raw: str) -> str:
        """
        Clean location text. Removes trailing commas and excess whitespace.
        For cases like '大阪市, ' returns '大阪市'.
        """
        if not raw:
            return ""
        cleaned = raw.strip().rstrip(",").strip()
        return cleaned

    # ---------------------------
    # Timezone handling
    # ---------------------------

    def _get_london_tz(self):
        """
        Try to return an IANA Europe/London timezone.
        On Windows without tzdata this may fail, so fall back to UTC with a warning.
        """
        try:
            return ZoneInfo("Europe/London")
        except Exception as e:
            audit_log(
                f"Timezone 'Europe/London' unavailable, falling back to UTC. Install 'tzdata' to fix. Underlying error: {e}"
            )
            logging.warning(
                "Timezone 'Europe/London' unavailable. Falling back to UTC."
            )
            return timezone.utc

    # ---------------------------
    # Legacy format methods kept for compatibility
    # ---------------------------

    def format_date(self, date_str):
        # Original method for page-based dates remains unchanged.
        if "-" in date_str:
            start_date_str, end_date_str = map(str.strip, date_str.split("-"))
            start_date = datetime.strptime(start_date_str, "%b %d, %Y").strftime(
                "%d %B %Y"
            )
            end_date = datetime.strptime(end_date_str, "%b %d, %Y").strftime("%d %B %Y")
            return f"{start_date} - {end_date}"
        else:
            return datetime.strptime(date_str, "%b %d, %Y").strftime("%d %B %Y")

    def parse_event_dates(self, formatted_date: str):
        """
        Parse the formatted date string (e.g. "01 January 2025" or "01 January 2025 - 02 January 2025")
        into start and end timezone-aware datetime objects.

        - If it's a single date, set the event from 7:00 PM to 11:00 PM.
        - If it's a range, set the start time to 8:00 AM on the first day and the end time to 11:00 PM on the last day.

        Uses Europe/London if available, otherwise UTC.
        """
        try:
            tz = self._get_london_tz()
            if "-" in formatted_date:
                start_date_str, end_date_str = map(str.strip, formatted_date.split("-"))
                dt_start = datetime.strptime(start_date_str, "%d %B %Y")
                dt_end = datetime.strptime(end_date_str, "%d %B %Y")
                start_dt = datetime(
                    dt_start.year, dt_start.month, dt_start.day, 8, 0, 0, tzinfo=tz
                )
                end_dt = datetime(
                    dt_end.year, dt_end.month, dt_end.day, 23, 0, 0, tzinfo=tz
                )
            else:
                dt = datetime.strptime(formatted_date, "%d %B %Y")
                start_dt = datetime(dt.year, dt.month, dt.day, 19, 0, 0, tzinfo=tz)
                end_dt = datetime(dt.year, dt.month, dt.day, 23, 0, 0, tzinfo=tz)
            logging.debug(
                f"Parsed event dates from '{formatted_date}' -> start: {start_dt}, end: {end_dt}"
            )
            audit_log(f"Successfully parsed event dates for '{formatted_date}'.")
            return start_dt, end_dt
        except Exception as e:
            logging.error(f"Error parsing event dates from '{formatted_date}': {e}")
            audit_log(f"Error parsing event dates from '{formatted_date}': {e}")
            now = datetime.now(timezone.utc)
            return now, now + timedelta(hours=4)

    # ---------------------------
    # Discord thread and event creation
    # ---------------------------

    async def check_forum_threads(self, guild, interaction, new_entries):
        audit_log("Starting check for forum threads for new entries.")
        liveshows_channel_id = self.config.get("liveshows_channel_id")
        if not liveshows_channel_id:
            logging.error("Missing 'liveshows_channel_id' in config.")
            await interaction.followup.send(
                embed=discord.Embed(
                    title="Error",
                    description="Missing 'liveshows_channel_id' in config.yaml.",
                    color=discord.Color.red(),
                )
            )
            return 0

        liveshows_channel = guild.get_channel(liveshows_channel_id)
        if liveshows_channel is None:
            logging.error(f"Channel with ID {liveshows_channel_id} not found.")
            error_embed = discord.Embed(
                title="Error",
                description="Threads channel was not found. Please double-check the config.",
                color=discord.Color.red(),
            )
            await interaction.followup.send(embed=error_embed)
            audit_log(
                f"{getattr(interaction.user, 'name', 'Unknown')} (ID: {getattr(interaction.user, 'id', 'Unknown')}): "
                f"Failed to update threads because channel with ID {liveshows_channel_id} was not found in guild '{guild.name}' (ID: {guild.id})."
            )
            return 0

        new_threads_created = 0
        for entry in new_entries:
            # entry = (event_date, venue, location, tickets_url)
            event_date, venue, location, tickets_url = entry
            thread_title = event_date.title()
            norm_title = normalize_string(thread_title)
            norm_location = normalize_string(location)
            logging.debug(
                f"Checking thread: original title='{thread_title}', normalized='{norm_title}', location normalized='{norm_location}'"
            )
            exists = await self.thread_exists(
                liveshows_channel, norm_title, norm_location
            )
            logging.info(
                f"Does thread '{thread_title}' with location '{location}' exist in channel '{getattr(liveshows_channel, 'name', 'unknown')}'? {exists}"
            )
            if exists:
                audit_log(
                    f"Skipping thread creation for '{thread_title}' as it already exists."
                )
            if not exists:
                try:
                    content_base = (
                        f"Sigrid at {venue.title()}, {location.title()}"
                        if venue or location
                        else "Sigrid live"
                    )
                    content = (
                        f"{content_base}\nTickets: {tickets_url}"
                        if tickets_url
                        else content_base
                    )
                    logging.info(f"Creating thread for: {thread_title}")
                    await liveshows_channel.create_thread(
                        name=thread_title,
                        content=content,
                        auto_archive_duration=60,
                    )
                    new_threads_created += 1
                    logging.info(f"Successfully created thread: {thread_title}")
                    audit_log(
                        f"{getattr(interaction.user, 'name', 'Unknown')} (ID: {getattr(interaction.user, 'id', 'Unknown')}) "
                        f"created thread '{thread_title}' in channel #{getattr(liveshows_channel, 'name', 'unknown')} (ID: {getattr(liveshows_channel, 'id', 'unknown')}) "
                        f"in guild '{guild.name}' (ID: {guild.id})."
                    )
                    await asyncio.sleep(2)
                except discord.Forbidden:
                    logging.error(
                        f"Permission denied when trying to create thread '{thread_title}'"
                    )
                    error_embed = discord.Embed(
                        title="Error",
                        description=f"Permission denied when trying to create thread '{thread_title}'.",
                        color=discord.Color.red(),
                    )
                    await interaction.followup.send(embed=error_embed)
                    audit_log(
                        f"{getattr(interaction.user, 'name', 'Unknown')} (ID: {getattr(interaction.user, 'id', 'Unknown')}) "
                        f"encountered permission error creating thread '{thread_title}' in channel #{getattr(liveshows_channel, 'name', 'unknown')} (ID: {getattr(liveshows_channel, 'id', 'unknown')})."
                    )
                except discord.HTTPException as e:
                    logging.error(f"Failed to create thread '{thread_title}': {e}")
                    error_embed = discord.Embed(
                        title="Error",
                        description=f"I couldn't create the thread '{thread_title}'. Please try again later.",
                        color=discord.Color.red(),
                    )
                    await interaction.followup.send(embed=error_embed)
                    audit_log(
                        f"{getattr(interaction.user, 'name', 'Unknown')} (ID: {getattr(interaction.user, 'id', 'Unknown')}) "
                        f"failed to create thread '{thread_title}' in channel #{getattr(liveshows_channel, 'name', 'unknown')} (ID: {getattr(liveshows_channel, 'id', 'Unknown')}) "
                        f"due to HTTP error: {e}"
                    )
        audit_log(
            f"Forum threads check complete. New threads created: {new_threads_created}."
        )
        return new_threads_created

    async def thread_exists(self, channel, thread_title, location):
        """Check if a thread exists with the given title and if its starter message contains the location."""
        norm_title = normalize_string(thread_title)
        norm_location = normalize_string(location)
        logging.debug(
            f"Checking existence for thread with normalized title '{norm_title}' and location '{norm_location}'"
        )
        try:
            threads = channel.threads
            logging.debug(
                f"Channel '{getattr(channel, 'name', 'unknown')}' has {len(threads)} active threads."
            )
        except Exception as e:
            logging.error(f"Error accessing channel threads: {e}")
            threads = []
        for thread in threads:
            thread_norm = normalize_string(thread.name)
            logging.debug(
                f"Comparing with thread: original name='{thread.name}', normalized='{thread_norm}'"
            )
            if thread_norm == norm_title:
                try:
                    starter_message = await thread.fetch_message(thread.id)
                    message_norm = normalize_string(starter_message.content)
                    logging.debug(
                        f"Starter message for thread '{thread.name}' normalized to: '{message_norm}'"
                    )
                    if norm_location and norm_location in message_norm:
                        logging.debug(
                            f"Found matching location '{norm_location}' in message for thread '{thread.name}'."
                        )
                        audit_log(
                            f"Thread '{thread.name}' exists with matching location '{location}'."
                        )
                        return True
                except Exception as e:
                    logging.error(
                        f"Error fetching starter message for thread '{thread.name}': {e}"
                    )
                    audit_log(
                        f"Assuming thread '{thread.name}' exists due to error fetching its message."
                    )
                    return True
        # Fallback: check scheduled events for matching thread title
        try:
            scheduled_events = await channel.guild.fetch_scheduled_events()
            logging.debug(
                f"Fetched {len(scheduled_events)} scheduled events for guild '{channel.guild.name}'."
            )
            for event in scheduled_events:
                normalized_event_name = normalize_string(event.name)
                logging.debug(
                    f"Comparing with scheduled event: original name='{event.name}', normalized='{normalized_event_name}'"
                )
                # Use startswith to allow for extra details in scheduled event names
                if normalized_event_name.startswith(norm_title):
                    logging.debug(
                        f"Match found in scheduled events: '{normalized_event_name}' starts with '{norm_title}'"
                    )
                    audit_log(
                        f"Scheduled event '{event.name}' exists with similar title to '{thread_title}'."
                    )
                    return True
        except Exception as e:
            logging.error(f"Error fetching scheduled events: {e}")
            audit_log(
                f"Error fetching scheduled events while checking thread existence: {e}"
            )
        return False

    async def check_server_events(self, guild, interaction, new_entries):
        audit_log("Starting check for scheduled events for new entries.")
        new_events_created = 0
        try:
            with open("event-image.jpg", "rb") as img_file:
                event_image = img_file.read()
        except Exception as e:
            logging.error(f"Failed to load event image: {e}")
            audit_log(f"Failed to load event image: {e}")
            event_image = None

        scheduled_events = await guild.fetch_scheduled_events()
        logging.debug(
            f"Guild '{guild.name}' has {len(scheduled_events)} scheduled events."
        )
        for entry in new_entries:
            # entry = (event_date, venue, location, tickets_url)
            event_date, venue, location, tickets_url = entry
            event_name = (
                f"{event_date.title()} - {venue.title() if venue else ''}".strip(" -")
            )
            norm_event_name = normalize_string(event_name)
            logging.debug(f"Normalized scheduled event name: '{norm_event_name}'")
            exists = any(
                normalize_string(e.name) == norm_event_name for e in scheduled_events
            )
            logging.info(
                f"Does scheduled event '{event_name}' exist in guild '{guild.name}'? {exists}"
            )
            if exists:
                audit_log(
                    f"Skipping creation of scheduled event '{event_name}' as it already exists."
                )
            if not exists:
                start_time, end_time = self.parse_event_dates(event_date)
                description_lines = []
                if venue or location:
                    description_lines.append(
                        f"Sigrid at {venue.title() if venue else ''}{', ' if venue and location else ''}{location.title() if location else ''}".strip(
                            ", "
                        )
                    )
                if tickets_url:
                    description_lines.append(f"Tickets: {tickets_url}")
                description = (
                    "\n".join(description_lines) if description_lines else "Sigrid live"
                )

                try:
                    await guild.create_scheduled_event(
                        name=event_name,
                        description=description,
                        start_time=start_time,
                        end_time=end_time,
                        location=f"{venue.title() if venue else ''}{', ' if venue and location else ''}{location.title() if location else ''}".strip(
                            ", "
                        ),
                        entity_type=discord.EntityType.external,
                        image=event_image,
                        privacy_level=discord.PrivacyLevel.guild_only,
                    )
                    new_events_created += 1
                    logging.info(f"Successfully created scheduled event: {event_name}")
                    audit_log(
                        f"{getattr(interaction.user, 'name', 'Unknown')} (ID: {getattr(interaction.user, 'id', 'Unknown')}) "
                        f"created scheduled event '{event_name}' in guild '{guild.name}' (ID: {guild.id})."
                    )
                    await asyncio.sleep(2)
                except discord.Forbidden:
                    logging.error(
                        f"Permission denied when trying to create scheduled event '{event_name}'"
                    )
                    error_embed = discord.Embed(
                        title="Error",
                        description=f"Permission denied when trying to create scheduled event '{event_name}'.",
                        color=discord.Color.red(),
                    )
                    await interaction.followup.send(embed=error_embed)
                    audit_log(
                        f"{getattr(interaction.user, 'name', 'Unknown')} (ID: {getattr(interaction.user, 'id', 'Unknown')}) "
                        f"encountered permission error creating scheduled event '{event_name}' in guild '{guild.name}' (ID: {guild.id})."
                    )
                except discord.HTTPException as e:
                    logging.error(
                        f"Failed to create scheduled event '{event_name}': {e}"
                    )
                    error_embed = discord.Embed(
                        title="Error",
                        description=f"I couldn't create the event '{event_name}'. Please try again later.",
                        color=discord.Color.red(),
                    )
                    await interaction.followup.send(embed=error_embed)
                    audit_log(
                        f"{getattr(interaction.user, 'name', 'Unknown')} (ID: {getattr(interaction.user, 'id', 'Unknown')}) "
                        f"failed to create scheduled event '{event_name}' in guild '{guild.name}' (ID: {guild.id}) due to HTTP error: {e}"
                    )
        audit_log(
            f"Scheduled events check complete. New events created: {new_events_created}."
        )
        return new_events_created

    async def send_combined_summary(
        self, interaction, threads_created: int, events_created: int
    ):
        if threads_created == 0 and events_created == 0:
            description = "All up to date. No new threads or scheduled events created."
        else:
            description = (
                f"**Forum Threads:** {threads_created} new thread{'s' if threads_created != 1 else ''} created.\n"
                f"**Scheduled Events:** {events_created} new scheduled event{'s' if events_created != 1 else ''} created."
            )
        embed = discord.Embed(
            title="Scrape Completed",
            description=description,
            color=(
                discord.Color.green()
                if (threads_created or events_created)
                else discord.Color.blurple()
            ),
        )
        logging.debug(f"Sending summary embed with description: {description}")
        await interaction.followup.send(embed=embed)
        audit_log("Combined summary sent to user with details: " + description)

    async def setup_audit(self, interaction):
        audit_log(
            f"{getattr(interaction.user, 'name', 'Unknown')} (ID: {getattr(interaction.user, 'id', 'Unknown')}) "
            f"initiated a scrape command in guild '{getattr(interaction.guild, 'name', 'Unknown Guild')}' "
            f"(ID: {getattr(interaction.guild, 'id', 'Unknown')})."
        )


async def setup(bot):
    await bot.add_cog(Scrape(bot))
