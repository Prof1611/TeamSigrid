import discord
import logging
import yaml
import sqlite3
from discord.ext import commands
import datetime

def audit_log(message: str):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open("audit.log", "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {message}\n")
    except Exception as e:
        logging.error(f"Failed to write to audit.log: {e}")

class AutoRole(commands.Cog):
    MAX_ROLE_COUNT = 1000
    ROLE_ID = 1380205700306829332

    def __init__(self, bot: commands.Bot):
        self.bot = bot

        # Load config.yaml
        with open("config.yaml", "r", encoding="utf-8") as config_file:
            self.config = yaml.safe_load(config_file)
        self.welcome_channel_id = self.config.get("welcome_channel_id")

        # Open/create the SQLite DB and ensure table exists
        try:
            self.db = sqlite3.connect("database.db", check_same_thread=False)
            self.db.row_factory = sqlite3.Row
            self.create_table()
        except Exception as e:
            logging.critical(f"Failed to open/create database: {e}")
            audit_log(f"CRITICAL: Could not open/create database: {e}")

    def create_table(self):
        try:
            with self.db:
                self.db.execute("""
                    CREATE TABLE IF NOT EXISTS autorole_counter (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        roles_given INTEGER NOT NULL
                    )
                """)
                if self.db.execute("SELECT COUNT(*) FROM autorole_counter").fetchone()[0] == 0:
                    self.db.execute("INSERT INTO autorole_counter (id, roles_given) VALUES (1, 0)")
        except Exception as e:
            logging.critical(f"Database table creation failed: {e}")
            audit_log(f"CRITICAL: Database table creation failed: {e}")

    def get_roles_given(self) -> int:
        try:
            result = self.db.execute("SELECT roles_given FROM autorole_counter WHERE id = 1").fetchone()
            if result is None:
                logging.error("autorole_counter missing row; resetting count to 0.")
                audit_log("autorole_counter missing row; resetting count to 0.")
                with self.db:
                    self.db.execute("INSERT OR REPLACE INTO autorole_counter (id, roles_given) VALUES (1, 0)")
                return 0
            return result["roles_given"]
        except Exception as e:
            logging.critical(f"Failed to read autorole_counter: {e}")
            audit_log(f"CRITICAL: Failed to read autorole_counter: {e}")
            return 0

    def increment_roles_given(self):
        try:
            with self.db:
                self.db.execute("UPDATE autorole_counter SET roles_given = roles_given + 1 WHERE id = 1")
        except Exception as e:
            logging.critical(f"Failed to increment autorole_counter: {e}")
            audit_log(f"CRITICAL: Failed to increment autorole_counter: {e}")

    @commands.Cog.listener()
    async def on_ready(self):
        logging.info("\033[96mAutoRole\033[0m cog synced successfully.")
        audit_log("AutoRole cog synced successfully.")

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        # Ignore bots
        if member.bot:
            logging.info(f"Skipped role assignment for bot member: {member.name} (ID: {member.id})")
            audit_log(f"Skipped role assignment for bot member: {member.name} (ID: {member.id})")
            return

        roles_given = self.get_roles_given()
        if roles_given >= self.MAX_ROLE_COUNT:
            logging.info("AutoRole cap reached. No role assigned.")
            audit_log("Role assignment skipped: cap of 1000 members reached.")
            return

        if not member.guild:
            logging.error(f"Member {member.name} (ID: {member.id}) has no guild context.")
            audit_log(f"Member {member.name} (ID: {member.id}) has no guild context. Skipped.")
            return

        role = member.guild.get_role(self.ROLE_ID)
        if not role:
            logging.error(f"Role ID {self.ROLE_ID} not found in guild {member.guild.name}.")
            audit_log(f"Role ID {self.ROLE_ID} not found in guild {member.guild.name} (ID: {member.guild.id}).")
            return

        # Extra check for role hierarchy (bot's top role > target role)
        bot_member = member.guild.get_member(self.bot.user.id)
        if bot_member and role.position >= bot_member.top_role.position:
            msg = (f"Cannot assign role '{role.name}' (ID: {role.id}) to '{member.name}': "
                   "role is higher or equal to bot's highest role.")
            logging.error(msg)
            audit_log(msg)
            return

        try:
            await member.add_roles(role, reason="Auto-assigned role on join")
            self.increment_roles_given()
            logging.info(f"Assigned role '{role.name}' to '{member.name}'.")
            audit_log(f"Assigned role '{role.name}' (ID: {role.id}) to '{member.name}' in guild '{member.guild.name}'.")

            # --- Post embed in welcome channel if within first 1000 ---
            if self.welcome_channel_id:
                channel = member.guild.get_channel(self.welcome_channel_id)
                if channel:
                    roles_given_now = self.get_roles_given()
                    if roles_given_now <= self.MAX_ROLE_COUNT:
                        embed = discord.Embed(
                            title="🔥 You Made It!",
                            description=(
                                f"You're member **{roles_given_now} of {self.MAX_ROLE_COUNT}** to join the chaos.\n\n"
                                "One of our first 1000 voices, thank you for being part of the noise. Let's make some magic together. 🖤"
                            ),
                            colour=discord.Colour.green(),
                            timestamp=datetime.datetime.utcnow()
                        )
                        embed.set_author(
                            name=member.display_name,
                            icon_url=member.avatar.url if member.avatar else None
                        )
                        await channel.send(
                            content=f"Welcome, {member.mention}!",
                            embed=embed
                        )

        except discord.Forbidden:
            logging.error(f"Permission error assigning role to '{member.name}'.")
            audit_log(f"Permission error: Failed to assign role to '{member.name}'.")
        except Exception as e:
            logging.error(f"Unexpected error assigning role to '{member.name}': {e}")
            audit_log(f"Unexpected error: Failed to assign role to '{member.name}': {e}")

async def setup(bot: commands.Bot):
    await bot.add_cog(AutoRole(bot))
