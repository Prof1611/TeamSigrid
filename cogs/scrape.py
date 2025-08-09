import discord
import logging
import yaml
from discord import app_commands
from discord.ext import commands
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests  # For HTTP requests to the Live page
import unicodedata
import string
from bs4 import BeautifulSoup  # For HTML parsing


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
            self.config = yaml.safe_load(config_file)
        # The URL of Sigrid's tour page (HTML-based).
        self.LIVE_PAGE_URL = "https://www.thisissigrid.com/tour/"
        audit_log("Scrape cog initialised and configuration loaded successfully.")

    @commands.Cog.listener()
    async def on_ready(self):
        logging.info(f"\033[96mScrape\033[0m cog synced successfully.")
        audit_log("Scrape cog synced successfully.")

    @discord.app_commands.command(
        name="scrape",
        description="Checks the band's website for new shows and updates #live-shows and server events.",
    )
    async def scrape(self, interaction: discord.Interaction):
        await interaction.response.defer()
        audit_log(
            f"{interaction.user.name} (ID: {interaction.user.id}) invoked /scrape command in guild '{interaction.guild.name}' (ID: {interaction.guild.id})."
        )
        try:
            audit_log("Starting scraping process via /scrape command.")
            # Run the scraper asynchronously in a separate thread.
            new_entries = await asyncio.to_thread(self.run_scraper)
            audit_log(
                f"{interaction.user.name} (ID: {interaction.user.id}) retrieved {len(new_entries)} new entries from the website."
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
                f"{interaction.user.name} (ID: {interaction.user.id}) encountered an error in /scrape command: {e}"
            )
            error_embed = discord.Embed(
                title="Error",
                description=f"An error occurred during scraping:\n`{e}`",
                color=discord.Color.red(),
            )
            await interaction.followup.send(embed=error_embed)

    def run_scraper(self):
        """
        Scrape Sigrid's tour page by fetching its HTML and extracting
        each <li class="date-item">. Return a list of tuples:
            [(formatted_date, venue, location), ...]
        """
        logging.info("Running scraper by parsing HTML from Sigrid's tour page...")
        audit_log("Starting scraper: Requesting event data from Sigrid tour HTML.")
        new_entries = []

        try:
            response = requests.get(self.LIVE_PAGE_URL, timeout=10)
            response.raise_for_status()
            html = response.text

            # Parse the HTML with BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")

            # Find the <ul class="liveContainer"> … </ul> block
            live_container = soup.find("ul", class_="liveContainer")
            if not live_container:
                logging.error("Could not find <ul class='liveContainer'> in HTML.")
                audit_log("Error: <ul class='liveContainer'> not found on /live/ page.")
                return []

            # Each <li class="date-item"> is one show
            items = live_container.find_all("li", class_="date-item")
            logging.info(f"Found {len(items)} <li class='date-item'> elements.")
            audit_log(f"Found {len(items)} shows in HTML.")

            for li in items:
                try:
                    # Extract date from <div class="googleDate">
                    date_tag = li.find("div", class_="googleDate")
                    raw_date = date_tag.get_text(strip=True) if date_tag else ""
                    formatted_date = self._parse_raw_date(raw_date)

                    # Extract venue from <div class="s_venue">
                    venue_tag = li.find("div", class_="s_venue")
                    venue_name = venue_tag.get_text(strip=True) if venue_tag else ""

                    # Extract location from addressLocality and addressCountry,
                    # but strip any trailing commas first
                    locality_tag = li.find("span", class_="addressLocality")
                    country_tag = li.find("span", class_="addressCountry")

                    if locality_tag:
                        # .get_text(strip=True) might give "Nürnberg," (with a trailing comma),
                        # so rstrip(",") removes any trailing comma if present.
                        locality = locality_tag.get_text(strip=True).rstrip(",")
                    else:
                        locality = ""

                    if country_tag:
                        country = country_tag.get_text(strip=True).rstrip(",")
                    else:
                        country = ""

                    if locality and country:
                        location = f"{locality}, {country}"
                    else:
                        # If one is missing, just use whichever is non-empty
                        location = locality or country

                    new_entries.append((formatted_date, venue_name, location))
                    logging.debug(
                        f"Parsed entry: ({formatted_date}, {venue_name}, {location})"
                    )
                    audit_log(
                        f"Processed show: {formatted_date} @ {venue_name}, {location}"
                    )

                except Exception as inner_e:
                    logging.error(f"Error parsing one <li>: {inner_e}")
                    audit_log(f"Error parsing <li class='date-item'>: {inner_e}")
                    continue

        except Exception as e:
            logging.error(f"Error fetching /live/ page: {e}")
            audit_log(f"Error fetching HTML from {self.LIVE_PAGE_URL}: {e}")

        return new_entries

    def _parse_raw_date(self, raw: str) -> str:
        """
        Convert raw date string like "25-Jun-06" into "06 June 2025".
        The site's format is “YY-Mon-DD” where “25” is the year “2025”.
        """
        try:
            parts = raw.split("-")
            if len(parts) == 3:
                year_two_digit = parts[0]  # e.g. "25"
                month_abbr = parts[1]  # e.g. "Jun"
                day = parts[2]  # e.g. "06"

                year_full = int("20" + year_two_digit)  # → 2025
                dt = datetime.strptime(f"{year_full} {month_abbr} {day}", "%Y %b %d")
                return dt.strftime("%d %B %Y")  # → "06 June 2025"
            else:
                return raw
        except Exception as e:
            logging.error(f"Error parsing raw date '{raw}': {e}")
            audit_log(f"Error parsing raw date '{raw}': {e}")
            return raw

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
        """
        try:
            tz = ZoneInfo("Europe/London")
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
            now = datetime.now(ZoneInfo("Europe/London"))
            return now, now + timedelta(hours=4)

    async def check_forum_threads(self, guild, interaction, new_entries):
        audit_log("Starting check for forum threads for new entries.")
        liveshows_channel_id = self.config["liveshows_channel_id"]
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
                f"{interaction.user.name} (ID: {interaction.user.id}): Failed to update threads because channel with ID {liveshows_channel_id} was not found in guild '{guild.name}' (ID: {guild.id})."
            )
            return 0

        new_threads_created = 0
        for entry in new_entries:
            event_date, venue, location = entry
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
                f"Does thread '{thread_title}' with location '{location}' exist in channel '{liveshows_channel.name}'? {exists}"
            )
            if exists:
                audit_log(
                    f"Skipping thread creation for '{thread_title}' as it already exists."
                )
            if not exists:
                try:
                    content = f"Sigrid at {venue.title()}, {location.title()}"
                    logging.info(f"Creating thread for: {thread_title}")
                    await liveshows_channel.create_thread(
                        name=thread_title,
                        content=content,
                        auto_archive_duration=60,
                    )
                    new_threads_created += 1
                    logging.info(f"Successfully created thread: {thread_title}")
                    audit_log(
                        f"{interaction.user.name} (ID: {interaction.user.id}) created thread '{thread_title}' in channel #{liveshows_channel.name} (ID: {liveshows_channel.id}) in guild '{guild.name}' (ID: {guild.id})."
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
                        f"{interaction.user.name} (ID: {interaction.user.id}) encountered permission error creating thread '{thread_title}' in channel #{liveshows_channel.name} (ID: {liveshows_channel.id})."
                    )
                except discord.HTTPException as e:
                    logging.error(f"Failed to create thread '{thread_title}': {e}")
                    error_embed = discord.Embed(
                        title="Error",
                        description=f"Failed to create thread '{thread_title}': `{e}`",
                        color=discord.Color.red(),
                    )
                    await interaction.followup.send(embed=error_embed)
                    audit_log(
                        f"{interaction.user.name} (ID: {interaction.user.id}) failed to create thread '{thread_title}' in channel #{liveshows_channel.name} (ID: {liveshows_channel.id}) due to HTTP error: {e}"
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
                f"Channel '{channel.name}' has {len(threads)} active threads."
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
            event_date, venue, location = entry
            event_name = f"{event_date.title()} - {venue.title() if venue else ''}"
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
                try:
                    await guild.create_scheduled_event(
                        name=event_name,
                        description=f"Sigrid at {venue.title() if venue else ''}, {location.title() if location else ''}",
                        start_time=start_time,
                        end_time=end_time,
                        location=f"{venue.title() if venue else ''}, {location.title() if location else ''}",
                        entity_type=discord.EntityType.external,
                        image=event_image,
                        privacy_level=discord.PrivacyLevel.guild_only,
                    )
                    new_events_created += 1
                    logging.info(f"Successfully created scheduled event: {event_name}")
                    audit_log(
                        f"{interaction.user.name} (ID: {interaction.user.id}) created scheduled event '{event_name}' in guild '{guild.name}' (ID: {guild.id})."
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
                        f"{interaction.user.name} (ID: {interaction.user.id}) encountered permission error creating scheduled event '{event_name}' in guild '{guild.name}' (ID: {guild.id})."
                    )
                except discord.HTTPException as e:
                    logging.error(
                        f"Failed to create scheduled event '{event_name}': {e}"
                    )
                    error_embed = discord.Embed(
                        title="Error",
                        description=f"Failed to create scheduled event '{event_name}': `{e}`",
                        color=discord.Color.red(),
                    )
                    await interaction.followup.send(embed=error_embed)
                    audit_log(
                        f"{interaction.user.name} (ID: {interaction.user.id}) failed to create scheduled event '{event_name}' in guild '{guild.name}' (ID: {guild.id}) due to HTTP error: {e}"
                    )
        audit_log(
            f"Scheduled events check complete. New events created: {new_events_created}."
        )
        return new_events_created

    async def send_combined_summary(
        self, interaction, threads_created: int, events_created: int
    ):
        if threads_created == 0 and events_created == 0:
            description = "All up to date! No new threads or scheduled events created."
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
            f"{interaction.user.name} (ID: {interaction.user.id}) initiated a scrape command in guild '{interaction.guild.name}' (ID: {interaction.guild.id})."
        )


async def setup(bot):
    await bot.add_cog(Scrape(bot))
