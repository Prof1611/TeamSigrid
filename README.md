# SigridHQ Discord Bot

**SigridHQ** is a customised Discord bot developed for the Official Sigrid Community. It focuses on enhancing member engagement, streamlining server utilities, and providing robust audit logging—all in a modular, easy-to-configure package.

## 🚀 Features

- **🎉 Auto-Role & Milestone Recognition**  
  Automatically assigns a “first 1000” role to new members and posts a themed embed in your welcome channel to commemorate milestone joins.

- **👋 Welcome Messages**  
  Sends a custom welcome embed (with image attachment) to guide newcomers to your server’s essential channels.

- **📌 Sticky Messages**  
  `/setsticky` lets you pin a message to the bottom of any channel (plain text or coloured embed), and `/removesticky` removes it. It automatically keeps that message “sticky” as new chat comes in—and even re-posts it if someone manually deletes it.

- **🎨 Custom Embed Tool**  
  `/sendembed` walks you through picking a colour (including custom hex), then entering title & description via modals, before sending your embed to any channel you choose.

- **📅 Show Scraper**
  `/scrape` fetches Sigrid’s official tour page, creates forum threads for new dates, and spins up scheduled events in Discord with all the right details and images.

- **📸 Instagram Monitor**
  Checks Sigrid's public Instagram profile every _n_ seconds and, when a brand-new post appears, pings a specified role and posts an embed (with image & caption) in a chosen channel.

- **🕒 Uptime**  
  `/uptime` shows how long the bot’s been online.

- **📖 Dynamic Help**  
  `/help` lists every slash command or shows detailed usage for a specific command.

- **📋 Audit Logging**  
  All commands, errors and automated actions are timestamped and appended to `audit.log` so you can trace exactly what the bot did, when and why.

---

## 🧾 Slash Commands

| Command          | Description                                                      |
| ---------------- | ---------------------------------------------------------------- |
| `/help [command]`| List all commands—or get detailed usage for one.                |
| `/uptime`        | Show how long SigridHQ has been running.                        |
| `/setsticky`     | Set (or update) a sticky message in this channel.               |
| `/removesticky`  | Remove the sticky message from this channel.                    |
| `/sendembed`     | Send a custom embed (choose colour, title & description).       |
| `/scrape`        | Manually trigger the live-show scraper & event/thread updater.  |

> **Automated features (no slash command):**  
> - Instagram Monitor (polls & posts on new Insta posts)
> - Welcome Messages (automatically sent when a new member joins the server)
> - Autorole (automatically gives the first 1000 members a special role)

---

## ⚙️ Configuration

All settings live in `config.yaml` (UTF-8). Here are the keys you’ll want to fill out:

```yaml
# ==========================
# SigridHQ Configuration - Sigrid Discord Server
# ==========================

# --------------------------
# Status Messages
# --------------------------
# Status messages to rotate through for the bot's presence.

statuses:
  - "Strangers 🤝"
  - "Don't Kill My Vibe ✋"
  - "Sucker Punch 🥊"
  - "Plot Twist 📚"
  - "Mirror 🪞"
  - "Don't Feel Like Crying 😢"
  - "High Five 🙌"
  - "Burning Bridges 🔥"
  - "It Gets Dark 🌑"
  - "Bad Life 💔"
  - "Head on Fire 🔥🧠"
  - "Sight of You 👀"
  - "Dynamite 💣"
  - "Basic ⚪"
  - "Home to You 🏠"
  - "Maybe It's a Good Thing ✅"
  - "Raw 🎤"
  - "Level Up ⬆️"
  - "Schedules 🗓️"
  - "Focus 🎯"
  - "Dance for Me 💃"
  - "In Vain 🌀"
  - "A Driver Saved My Night 🚗"
  - "Ring 💍"

# --------------------------
# Channel and Role IDs
# --------------------------
# IDs of channels and roles used by the bot for various functions.

liveshows_channel_id: 1380157321547874414              # Forum channel for live shows threads.
welcome_channel_id: 1380157415097634868        # Channel where welcome messages are posted.
new_member_channel_id: 1380157745851793408        # Channel where new members are directed.
dm_forward_channel_id: 1380176986512822324    # Channel for forwarding direct messages.
instagram_announce_channel_id: 1380157930233397359  # Discord channel ID for announcements.
instagram_ping_role_id: 1380175070101770321 # The role to ping when there's a new post.

# --------------------------
# Feature Toggles
# --------------------------
# Enable or disable specific features. Set to 'true' to enable the feature.
welcome_enabled: true     # Set to true to enable welcome messages.

# --------------------------
# Instagram Settings
# --------------------------
instagram_username: thisissigrid
instagram_poll_interval: 300   # seconds between checks.

```

---

## 🗃 Data Storage

- **SQLite** (`database.db`) stores:  
  - Sticky messages (`sticky_messages` table)  
  - Instagram last-seen post (`instagram_last` table)  
  - Autorole counter (`autorole_counter` table)

- **Audit Log** (`audit.log`) is a plain-text, timestamped record of every key action.

---

## 📄 Licence

This project is released under the **GPL-3.0 Licence**. See [LICENCE](LICENCE) for full details.

---

## 🛠 Maintained by

- **GitHub:** [Prof1611](https://github.com/Prof1611)  
- **Discord:** Tygafire  
