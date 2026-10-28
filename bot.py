import os
import sys
import re
import time
import asyncio
import subprocess
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Tuple

# -------------------------------------------------------------
# 1. Automatic Dependency Check & Installation
# -------------------------------------------------------------
try:
    import discord
    from discord import app_commands
    from discord.ext import commands, tasks
except ImportError:
    print("[Setup] Installing discord.py...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "discord.py"])
    import discord
    from discord import app_commands
    from discord.ext import commands, tasks

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# -------------------------------------------------------------
# 2. SQLite Database with Persistent Storage & Auto-Migration
# -------------------------------------------------------------
DATA_DIR = os.getenv("DATA_DIR", "/app/data" if os.path.exists("/app") else ".")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "bot_database.db")

START_TIME = time.time()
MSK_TZ = timezone(timedelta(hours=3))  # Moscow Time (UTC+3)


def get_db():
    return sqlite3.connect(DB_PATH)


def init_db():
    """Initialize all database tables and migrate schema synchronously on startup."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER PRIMARY KEY,
                ideas_channel_id INTEGER,
                showcase_channel_id INTEGER,
                star_threshold INTEGER DEFAULT 12,
                booster_role_id INTEGER,
                autorole_id INTEGER
            )
            """
        )
        try:
            cursor.execute("ALTER TABLE guild_settings ADD COLUMN autorole_id INTEGER")
        except sqlite3.OperationalError:
            pass

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS starboard_entries (
                orig_message_id INTEGER PRIMARY KEY,
                showcase_message_id INTEGER UNIQUE,
                guild_id INTEGER,
                channel_id INTEGER,
                stars_count INTEGER DEFAULT 0
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_posts (
                message_id INTEGER PRIMARY KEY,
                channel_id INTEGER,
                guild_id INTEGER,
                style TEXT DEFAULT 'announcement',
                title TEXT,
                content TEXT,
                image_url TEXT,
                footer_text TEXT,
                button_label TEXT,
                button_url TEXT,
                author_id INTEGER,
                updated_at INTEGER
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS scheduled_events (
                message_id INTEGER PRIMARY KEY,
                channel_id INTEGER,
                guild_id INTEGER,
                role_id INTEGER,
                title TEXT,
                subtitle TEXT,
                description TEXT,
                image_url TEXT,
                start_timestamp INTEGER,
                remind_minutes INTEGER DEFAULT 10,
                reminded INTEGER DEFAULT 0,
                started INTEGER DEFAULT 0,
                followers_count INTEGER DEFAULT 0,
                author_id INTEGER
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_showcase_msg ON starboard_entries(showcase_message_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_active ON scheduled_events(started, reminded)"
        )
        conn.commit()
    print(f"[Database] Initialized successfully at: {DB_PATH}")


init_db()


# --- Guild Settings Helpers ---
def get_guild_settings(guild_id: int) -> Dict[str, Any]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT ideas_channel_id, showcase_channel_id, star_threshold, booster_role_id, autorole_id
            FROM guild_settings WHERE guild_id = ?
            """,
            (guild_id,),
        )
        row = cursor.fetchone()
        if row:
            return {
                "ideas_channel_id": row[0],
                "showcase_channel_id": row[1],
                "star_threshold": row[2] if row[2] is not None else 12,
                "booster_role_id": row[3],
                "autorole_id": row[4],
            }
        return {
            "ideas_channel_id": None,
            "showcase_channel_id": None,
            "star_threshold": 12,
            "booster_role_id": None,
            "autorole_id": None,
        }


def update_guild_settings(guild_id: int, **kwargs):
    current = get_guild_settings(guild_id)
    current.update(kwargs)
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO guild_settings (
                guild_id, ideas_channel_id, showcase_channel_id, star_threshold, booster_role_id, autorole_id
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
                ideas_channel_id = excluded.ideas_channel_id,
                showcase_channel_id = excluded.showcase_channel_id,
                star_threshold = excluded.star_threshold,
                booster_role_id = excluded.booster_role_id,
                autorole_id = excluded.autorole_id
            """,
            (
                guild_id,
                current["ideas_channel_id"],
                current["showcase_channel_id"],
                current["star_threshold"],
                current["booster_role_id"],
                current["autorole_id"],
            ),
        )
        conn.commit()


# --- Starboard Helpers ---
def get_entry_by_orig(orig_message_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT showcase_message_id, channel_id, stars_count FROM starboard_entries WHERE orig_message_id = ?",
            (orig_message_id,),
        )
        return cursor.fetchone()


def get_entry_by_showcase(showcase_message_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT orig_message_id, channel_id, stars_count FROM starboard_entries WHERE showcase_message_id = ?",
            (showcase_message_id,),
        )
        return cursor.fetchone()


def save_starboard_entry(
    orig_message_id: int,
    showcase_message_id: int,
    guild_id: int,
    channel_id: int,
    stars_count: int,
):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO starboard_entries
            (orig_message_id, showcase_message_id, guild_id, channel_id, stars_count)
            VALUES (?, ?, ?, ?, ?)
            """,
            (orig_message_id, showcase_message_id, guild_id, channel_id, stars_count),
        )
        conn.commit()


def get_total_ideas_count(guild_id: int) -> int:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM starboard_entries WHERE guild_id = ?", (guild_id,))
        row = cursor.fetchone()
        return row[0] if row else 0


def get_total_posts_count(guild_id: int) -> int:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM bot_posts WHERE guild_id = ?", (guild_id,))
        row = cursor.fetchone()
        return row[0] if row else 0


# --- Custom Bot Posts Helpers ---
def save_bot_post(
    message_id: int,
    channel_id: int,
    guild_id: int,
    style: str,
    title: str,
    content: str,
    image_url: str,
    footer_text: str,
    button_label: str,
    button_url: str,
    author_id: int,
):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO bot_posts (
                message_id, channel_id, guild_id, style, title, content,
                image_url, footer_text, button_label, button_url, author_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                channel_id,
                guild_id,
                style,
                title,
                content,
                image_url,
                footer_text,
                button_label,
                button_url,
                author_id,
                int(time.time()),
            ),
        )
        conn.commit()


def get_bot_post(message_id: int) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT message_id, channel_id, guild_id, style, title, content,
                   image_url, footer_text, button_label, button_url, author_id
            FROM bot_posts WHERE message_id = ?
            """,
            (message_id,),
        )
        row = cursor.fetchone()
        if row:
            return {
                "message_id": row[0],
                "channel_id": row[1],
                "guild_id": row[2],
                "style": row[3] or "announcement",
                "title": row[4] or "",
                "content": row[5] or "",
                "image_url": row[6] or "",
                "footer_text": row[7] or "",
                "button_label": row[8] or "",
                "button_url": row[9] or "",
                "author_id": row[10],
            }
        return None


# --- Scheduled Events Helpers ---
def save_scheduled_event(
    message_id: int,
    channel_id: int,
    guild_id: int,
    role_id: int,
    title: str,
    subtitle: str,
    description: str,
    image_url: str,
    start_timestamp: int,
    remind_minutes: int = 10,
    reminded: int = 0,
    started: int = 0,
    followers_count: int = 0,
    author_id: int = 0,
):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO scheduled_events (
                message_id, channel_id, guild_id, role_id, title, subtitle,
                description, image_url, start_timestamp, remind_minutes,
                reminded, started, followers_count, author_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                channel_id,
                guild_id,
                role_id,
                title,
                subtitle,
                description,
                image_url,
                start_timestamp,
                remind_minutes,
                reminded,
                started,
                followers_count,
                author_id,
            ),
        )
        conn.commit()


def get_scheduled_event(message_id: int) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT message_id, channel_id, guild_id, role_id, title, subtitle,
                   description, image_url, start_timestamp, remind_minutes,
                   reminded, started, followers_count, author_id
            FROM scheduled_events WHERE message_id = ?
            """,
            (message_id,),
        )
        row = cursor.fetchone()
        if row:
            return {
                "message_id": row[0],
                "channel_id": row[1],
                "guild_id": row[2],
                "role_id": row[3],
                "title": row[4] or "",
                "subtitle": row[5] or "",
                "description": row[6] or "",
                "image_url": row[7] or "",
                "start_timestamp": row[8],
                "remind_minutes": row[9] or 10,
                "reminded": row[10],
                "started": row[11],
                "followers_count": row[12] or 0,
                "author_id": row[13],
            }
        return None


def get_pending_events():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT message_id, channel_id, guild_id, role_id, title, subtitle,
                   description, image_url, start_timestamp, remind_minutes,
                   reminded, started, followers_count, author_id
            FROM scheduled_events WHERE started = 0
            """
        )
        rows = cursor.fetchall()
        result = []
        for row in rows:
            result.append(
                {
                    "message_id": row[0],
                    "channel_id": row[1],
                    "guild_id": row[2],
                    "role_id": row[3],
                    "title": row[4] or "",
                    "subtitle": row[5] or "",
                    "description": row[6] or "",
                    "image_url": row[7] or "",
                    "start_timestamp": row[8],
                    "remind_minutes": row[9] or 10,
                    "reminded": row[10],
                    "started": row[11],
                    "followers_count": row[12] or 0,
                    "author_id": row[13],
                }
            )
        return result


# -------------------------------------------------------------
# 3. Discord Bot Setup
# -------------------------------------------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.reactions = True

bot = commands.Bot(
    command_prefix="!",
    intents=intents,
    activity=discord.Activity(type=discord.ActivityType.watching, name="for top ideas ⭐"),
    status=discord.Status.online,
)


# -------------------------------------------------------------
# 4. Event Scheduler Builders, Date Parser & Persistent View
# -------------------------------------------------------------
def parse_msk_datetime(raw: str) -> Optional[int]:
    """Parses a Moscow Time (UTC+3) date string into a Unix timestamp."""
    cleaned = raw.strip()
    formats = [
        "%d.%m.%Y %H:%M",
        "%Y-%m-%d %H:%M",
        "%d/%m/%Y %H:%M",
        "%d.%m.%y %H:%M",
        "%d-%m-%Y %H:%M",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(cleaned, fmt).replace(tzinfo=MSK_TZ)
            return int(dt.timestamp())
        except ValueError:
            continue
    return None


def format_msk_datetime(ts: int) -> str:
    """Formats a Unix timestamp back into DD.MM.YYYY HH:MM (MSK) for editing."""
    dt = datetime.fromtimestamp(ts, tz=MSK_TZ)
    return dt.strftime("%d.%m.%Y %H:%M")


def build_event_embed(
    guild: discord.Guild,
    title: str,
    subtitle: str,
    description: str,
    start_ts: int,
    role_id: int,
    image_url: str = "",
    remind_minutes: int = 10,
    started: int = 0,
    followers_count: int = 0,
) -> discord.Embed:
    """Builds the rich interactive Event Card with local timezone & countdown."""
    now = int(time.time())
    if started or now >= start_ts:
        color = 0x2ECC71
        status_badge = "🔴 EVENT IS LIVE NOW!"
    elif now >= start_ts - (remind_minutes * 60):
        color = 0xE67E22
        status_badge = f"⚡ STARTING SOON (< {remind_minutes} MIN)"
    else:
        color = 0xEB459E
        status_badge = "📅 UPCOMING SERVER EVENT"

    full_desc = ""
    if subtitle.strip():
        full_desc += f"### {subtitle.strip()}\n\n"
    full_desc += f"{description.strip()}\n\n"

    embed = discord.Embed(
        title=title.strip(),
        description=full_desc,
        color=color,
        timestamp=datetime.fromtimestamp(start_ts, tz=timezone.utc),
    )

    icon_url = guild.icon.url if guild and guild.icon else None
    embed.set_author(name=status_badge, icon_url=icon_url)

    # Discord automatically converts <t:ts:F> to each user's local timezone
    # and <t:ts:R> into a live ticking countdown!
    embed.add_field(
        name="🕒 Start Time (Your Local Time)",
        value=f"<t:{start_ts}:F>",
        inline=True,
    )
    embed.add_field(
        name="⏳ Countdown",
        value="🔴 **LIVE NOW!**" if started else f"<t:{start_ts}:R>",
        inline=True,
    )
    embed.add_field(
        name="🔔 Notification Role",
        value=f"<@&{role_id}> • **{followers_count}** following\n*(Ping {remind_minutes}m before start)*",
        inline=False,
    )

    if image_url and image_url.startswith(("http://", "https://")):
        embed.set_image(url=image_url.strip())

    embed.set_footer(text=f"{guild.name if guild else 'Event System'} • Click 'Follow' below to get notified")
    return embed


class EventFollowView(discord.ui.View):
    """Persistent view allowing users to toggle the event role with 1 click."""

    def __init__(self, followers_count: int = 0, started: bool = False):
        super().__init__(timeout=None)
        btn = discord.ui.Button(
            label=f"Follow ({followers_count})" if not started else f"Event Started ({followers_count})",
            style=discord.ButtonStyle.success if not started else discord.ButtonStyle.secondary,
            emoji="🔔",
            custom_id="persistent:event_follow_btn",
            disabled=started,
        )
        btn.callback = self.toggle_follow
        self.add_item(btn)

    async def toggle_follow(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        ev = get_scheduled_event(interaction.message.id)
        if not ev:
            return await interaction.response.send_message(
                "❌ This event is no longer registered in the database.", ephemeral=True
            )

        role = interaction.guild.get_role(ev["role_id"])
        if not role:
            return await interaction.response.send_message(
                "⚠️ The notification role for this event no longer exists.", ephemeral=True
            )

        if interaction.guild.me.top_role <= role:
            return await interaction.response.send_message(
                f"⚠️ My highest role must be placed **above** {role.mention} in Server Settings ➔ Roles!",
                ephemeral=True,
            )

        member = interaction.user
        try:
            if role in member.roles:
                await member.remove_roles(role, reason=f"Unfollowed event: {ev['title']}")
                action_msg = f"🔕 You unfollowed **{ev['title']}**. Role {role.mention} removed."
            else:
                await member.add_roles(role, reason=f"Followed event: {ev['title']}")
                action_msg = (
                    f"🔔 You are now following **{ev['title']}**! "
                    f"You were given {role.mention} and will be notified **{ev['remind_minutes']} minutes** before the start."
                )
        except discord.Forbidden:
            return await interaction.response.send_message(
                "❌ I don't have permission to manage that role.", ephemeral=True
            )

        # Count current human members with this role
        followers_count = sum(1 for m in role.members if not m.bot)
        ev["followers_count"] = followers_count
        save_scheduled_event(**ev)

        new_embed = build_event_embed(
            guild=interaction.guild,
            title=ev["title"],
            subtitle=ev["subtitle"],
            description=ev["description"],
            start_ts=ev["start_timestamp"],
            role_id=ev["role_id"],
            image_url=ev["image_url"],
            remind_minutes=ev["remind_minutes"],
            started=ev["started"],
            followers_count=followers_count,
        )
        new_view = EventFollowView(followers_count=followers_count, started=bool(ev["started"]))
        await interaction.response.edit_message(embed=new_embed, view=new_view)
        await interaction.followup.send(action_msg, ephemeral=True)


class EventComposerModal(discord.ui.Modal):
    """Modal for creating or editing a Scheduled Event."""

    title_input = discord.ui.TextInput(
        label="Event Title",
        placeholder="e.g., Mega Boss Raid & Giveaway",
        max_length=120,
        required=True,
    )
    subtitle_input = discord.ui.TextInput(
        label="Subtitle (Optional)",
        placeholder="e.g., Special Weekend Tournament • 2x Rewards",
        max_length=150,
        required=False,
    )
    datetime_input = discord.ui.TextInput(
        label="Start Date & Time in MSK (DD.MM.YYYY HH:MM)",
        placeholder="e.g., 28.10.2026 19:30 (Moscow Time UTC+3)",
        max_length=30,
        required=True,
    )
    description_input = discord.ui.TextInput(
        label="Event Description",
        style=discord.TextStyle.paragraph,
        placeholder="Write event details, rules, and prizes here...",
        max_length=2500,
        required=True,
    )
    image_input = discord.ui.TextInput(
        label="Banner Image URL (Leave blank if uploaded)",
        placeholder="https://... (or upload from gallery via command)",
        max_length=500,
        required=False,
    )

    def __init__(
        self,
        target_channel: discord.TextChannel,
        role: discord.Role,
        remind_minutes: int = 10,
        uploaded_image_url: str = "",
        existing_event: Optional[Dict[str, Any]] = None,
        target_message: Optional[discord.Message] = None,
    ):
        modal_title = "✏️ Edit Scheduled Event" if existing_event else "📅 Create Scheduled Event"
        super().__init__(title=modal_title)
        self.target_channel = target_channel
        self.role = role
        self.remind_minutes = remind_minutes
        self.uploaded_image_url = uploaded_image_url
        self.existing_event = existing_event
        self.target_message = target_message

        if existing_event:
            self.title_input.default = existing_event["title"]
            self.subtitle_input.default = existing_event["subtitle"]
            self.datetime_input.default = format_msk_datetime(existing_event["start_timestamp"])
            self.description_input.default = existing_event["description"]
            self.image_input.default = uploaded_image_url or existing_event["image_url"]
        elif uploaded_image_url:
            self.image_input.default = uploaded_image_url

    async def on_submit(self, interaction: discord.Interaction):
        start_ts = parse_msk_datetime(self.datetime_input.value)
        if not start_ts:
            return await interaction.response.send_message(
                "❌ **Invalid Date/Time format!**\n"
                "Please enter Moscow Time using format `DD.MM.YYYY HH:MM` (for example: `28.10.2026 19:30`).",
                ephemeral=True,
            )

        final_image = self.uploaded_image_url or self.image_input.value.strip()
        followers_count = sum(1 for m in self.role.members if not m.bot)
        now = int(time.time())
        is_started = 1 if now >= start_ts else 0
        is_reminded = 1 if now >= start_ts - (self.remind_minutes * 60) else 0

        embed = build_event_embed(
            guild=interaction.guild,
            title=self.title_input.value,
            subtitle=self.subtitle_input.value,
            description=self.description_input.value,
            start_ts=start_ts,
            role_id=self.role.id,
            image_url=final_image,
            remind_minutes=self.remind_minutes,
            started=is_started,
            followers_count=followers_count,
        )
        view = EventFollowView(followers_count=followers_count, started=bool(is_started))

        try:
            if self.target_message and self.existing_event:
                await self.target_message.edit(embed=embed, view=view)
                save_scheduled_event(
                    message_id=self.target_message.id,
                    channel_id=self.target_channel.id,
                    guild_id=interaction.guild.id,
                    role_id=self.role.id,
                    title=self.title_input.value,
                    subtitle=self.subtitle_input.value,
                    description=self.description_input.value,
                    image_url=final_image,
                    start_timestamp=start_ts,
                    remind_minutes=self.remind_minutes,
                    reminded=is_reminded,
                    started=is_started,
                    followers_count=followers_count,
                    author_id=interaction.user.id,
                )
                await interaction.response.send_message(
                    f"✅ **Event updated!** [Jump to Event]({self.target_message.jump_url})",
                    ephemeral=True,
                )
            else:
                sent = await self.target_channel.send(embed=embed, view=view)
                save_scheduled_event(
                    message_id=sent.id,
                    channel_id=self.target_channel.id,
                    guild_id=interaction.guild.id,
                    role_id=self.role.id,
                    title=self.title_input.value,
                    subtitle=self.subtitle_input.value,
                    description=self.description_input.value,
                    image_url=final_image,
                    start_timestamp=start_ts,
                    remind_minutes=self.remind_minutes,
                    reminded=is_reminded,
                    started=is_started,
                    followers_count=followers_count,
                    author_id=interaction.user.id,
                )
                await interaction.response.send_message(
                    f"🎉 **Event Published in {self.target_channel.mention}!**\n"
                    f"• **Start Time (Local):** <t:{start_ts}:F> (<t:{start_ts}:R>)\n"
                    f"• **Follow Role:** {self.role.mention} (Ping **{self.remind_minutes} min** before start)\n"
                    f"• **Message ID:** `{sent.id}`\n"
                    f"💡 *Tip: Use `/event_image` anytime to upload a new photo from your gallery, or right-click the message ➔ `Apps` ➔ `📅 Edit Event`.*",
                    ephemeral=True,
                )
        except discord.Forbidden:
            await interaction.response.send_message(
                f"❌ Missing permissions to send or edit messages in {self.target_channel.mention}.",
                ephemeral=True,
            )


# -------------------------------------------------------------
# 5. Post Studio Visual Styles & Builders
# -------------------------------------------------------------
POST_STYLES: Dict[str, Dict[str, Any]] = {
    "announcement": {
        "name": "📢 Official Announcement",
        "color": 0x5865F2,
        "badge": "📢 OFFICIAL ANNOUNCEMENT",
        "use_timestamp": True,
        "use_server_icon": True,
    },
    "update": {
        "name": "🚀 Update / Patch Notes",
        "color": 0x2ECC71,
        "badge": "🚀 UPDATE & PATCH NOTES",
        "use_timestamp": True,
        "use_server_icon": True,
    },
    "event": {
        "name": "🎉 Event / Giveaway",
        "color": 0xEB459E,
        "badge": "🎉 SPECIAL COMMUNITY EVENT",
        "use_timestamp": True,
        "use_server_icon": True,
    },
    "guide": {
        "name": "📘 Server Guide / Info",
        "color": 0x3498DB,
        "badge": "📘 INFORMATION & GUIDE",
        "use_timestamp": False,
        "use_server_icon": True,
    },
    "alert": {
        "name": "⚠️ Urgent Alert",
        "color": 0xED4245,
        "badge": "⚠️ IMPORTANT NOTICE",
        "use_timestamp": True,
        "use_server_icon": False,
    },
    "minimal": {
        "name": "🖤 Minimalist Dark",
        "color": 0x2B2D31,
        "badge": None,
        "use_timestamp": False,
        "use_server_icon": False,
    },
}


def parse_button_input(raw: str) -> Tuple[str, str]:
    if not raw or not raw.strip():
        return "", ""
    raw = raw.strip()
    if "|" in raw:
        parts = raw.split("|", 1)
        label = parts[0].strip()[:80] or "Open Link"
        url = parts[1].strip()
    else:
        label = "Open Link"
        url = raw
    if url.startswith(("http://", "https://")):
        return label, url
    return "", ""


def build_styled_post(
    guild: discord.Guild,
    style_key: str,
    title: str,
    content: str,
    image_url: str = "",
    footer_text: str = "",
    button_label: str = "",
    button_url: str = "",
) -> Tuple[discord.Embed, Optional[discord.ui.View]]:
    style = POST_STYLES.get(style_key, POST_STYLES["announcement"])
    formatted_desc = content
    if style_key == "alert" and content and not content.startswith(">"):
        formatted_desc = f">>> {content}"

    embed = discord.Embed(
        title=title.strip() if title.strip() else None,
        description=formatted_desc,
        color=style["color"],
        timestamp=datetime.now(timezone.utc) if style["use_timestamp"] else None,
    )

    if style["badge"]:
        icon_url = guild.icon.url if (style["use_server_icon"] and guild and guild.icon) else None
        embed.set_author(name=style["badge"], icon_url=icon_url)

    if image_url and image_url.strip().startswith(("http://", "https://")):
        embed.set_image(url=image_url.strip())

    if footer_text and footer_text.strip():
        embed.set_footer(text=footer_text.strip())
    elif style_key != "minimal" and guild:
        embed.set_footer(text=guild.name)

    view = None
    if button_url and button_url.startswith(("http://", "https://")):
        view = discord.ui.View()
        view.add_item(
            discord.ui.Button(
                label=button_label or "Learn More",
                style=discord.ButtonStyle.link,
                url=button_url,
                emoji="🔗",
            )
        )
    return embed, view


class PostComposerModal(discord.ui.Modal):
    post_title = discord.ui.TextInput(
        label="Headline / Title (Optional)",
        placeholder="e.g., Update v2.4 is Live!",
        max_length=256,
        required=False,
    )
    post_content = discord.ui.TextInput(
        label="Message Body (Supports Markdown)",
        style=discord.TextStyle.paragraph,
        placeholder="Write your announcement here...",
        max_length=3800,
        required=True,
    )
    post_image = discord.ui.TextInput(
        label="Banner Image URL (Optional)",
        placeholder="https://example.com/banner.png",
        max_length=500,
        required=False,
    )
    post_footer = discord.ui.TextInput(
        label="Footer Text (Optional)",
        placeholder="Leave empty for default server name",
        max_length=120,
        required=False,
    )
    post_button = discord.ui.TextInput(
        label="Link Button: Label | https://url (Optional)",
        placeholder="e.g., Play Now | https://roblox.com/...",
        max_length=300,
        required=False,
    )

    def __init__(
        self,
        style_key: str,
        target_channel: discord.TextChannel,
        ping_role: Optional[discord.Role] = None,
        existing_message: Optional[discord.Message] = None,
        initial_data: Optional[Dict[str, Any]] = None,
    ):
        modal_title = "✏️ Edit Bot Message" if existing_message else f"✨ New: {POST_STYLES[style_key]['name'][:28]}"
        super().__init__(title=modal_title)
        self.style_key = style_key
        self.target_channel = target_channel
        self.ping_role = ping_role
        self.existing_message = existing_message

        if initial_data:
            self.post_title.default = initial_data.get("title", "")
            self.post_content.default = initial_data.get("content", "")
            self.post_image.default = initial_data.get("image_url", "")
            self.post_footer.default = initial_data.get("footer_text", "")
            btn_lbl = initial_data.get("button_label", "")
            btn_url = initial_data.get("button_url", "")
            if btn_url:
                self.post_button.default = f"{btn_lbl} | {btn_url}" if btn_lbl else btn_url

    async def on_submit(self, interaction: discord.Interaction):
        title_val = self.post_title.value.strip()
        content_val = self.post_content.value.strip()
        image_val = self.post_image.value.strip()
        footer_val = self.post_footer.value.strip()
        btn_label, btn_url = parse_button_input(self.post_button.value)

        embed, view = build_styled_post(
            guild=interaction.guild,
            style_key=self.style_key,
            title=title_val,
            content=content_val,
            image_url=image_val,
            footer_text=footer_val,
            button_label=btn_label,
            button_url=btn_url,
        )

        try:
            if self.existing_message:
                await self.existing_message.edit(embed=embed, view=view)
                save_bot_post(
                    message_id=self.existing_message.id,
                    channel_id=self.existing_message.channel.id,
                    guild_id=interaction.guild.id,
                    style=self.style_key,
                    title=title_val,
                    content=content_val,
                    image_url=image_val,
                    footer_text=footer_val,
                    button_label=btn_label,
                    button_url=btn_url,
                    author_id=interaction.user.id,
                )
                await interaction.response.send_message(
                    f"✅ **Message updated!** [Jump to Message]({self.existing_message.jump_url})",
                    ephemeral=True,
                )
            else:
                ping_content = self.ping_role.mention if self.ping_role else None
                sent_msg = await self.target_channel.send(
                    content=ping_content,
                    embed=embed,
                    view=view,
                    allowed_mentions=discord.AllowedMentions(roles=True),
                )
                save_bot_post(
                    message_id=sent_msg.id,
                    channel_id=sent_msg.channel.id,
                    guild_id=interaction.guild.id,
                    style=self.style_key,
                    title=title_val,
                    content=content_val,
                    image_url=image_val,
                    footer_text=footer_val,
                    button_label=btn_label,
                    button_url=btn_url,
                    author_id=interaction.user.id,
                )
                await interaction.response.send_message(
                    f"✅ **Published in {self.target_channel.mention}!** (`{sent_msg.id}`)",
                    ephemeral=True,
                )
        except discord.Forbidden:
            await interaction.response.send_message(
                f"❌ Missing permissions in {self.target_channel.mention}.", ephemeral=True
            )


def extract_post_data_from_message(message: discord.Message) -> Dict[str, Any]:
    record = get_bot_post(message.id)
    if record:
        return record

    data = {
        "style": "announcement",
        "title": "",
        "content": message.content or "",
        "image_url": "",
        "footer_text": "",
        "button_label": "",
        "button_url": "",
    }
    if message.embeds:
        emb = message.embeds[0]
        data["title"] = emb.title or ""
        desc = emb.description or ""
        if desc.startswith(">>> "):
            desc = desc[4:]
        data["content"] = desc
        if emb.image and emb.image.url:
            data["image_url"] = emb.image.url
        if emb.footer and emb.footer.text:
            data["footer_text"] = emb.footer.text
        for k, v in POST_STYLES.items():
            if emb.color and emb.color.value == v["color"]:
                data["style"] = k
                break
    return data


# -------------------------------------------------------------
# 6. Starboard Visual Builders & Unique Voter Deduplication
# -------------------------------------------------------------
def get_tier_visuals(stars: int) -> Tuple[int, str]:
    if stars >= 50:
        return 0x9B59B6, "🟣 LEGENDARY IDEA"
    elif stars >= 25:
        return 0xE67E22, "🔥 TRENDING IDEA"
    elif stars >= 15:
        return 0xF1C40F, "✨ COMMUNITY CHOICE"
    return 0x2B2D31, "⭐ FEATURED IDEA"


def build_showcase_embed(message: discord.Message, stars: int) -> discord.Embed:
    color, badge = get_tier_visuals(stars)
    content = message.content.strip() if message.content else ""
    description = f">>> {content}" if content else "*[Media Attachment]*"

    embed = discord.Embed(description=description, color=color, timestamp=message.created_at)
    embed.set_author(
        name=f"{message.author.display_name}  •  {badge}",
        icon_url=message.author.display_avatar.url,
        url=message.jump_url,
    )
    if message.attachments:
        first = message.attachments[0]
        if first.content_type and first.content_type.startswith("image/"):
            embed.set_image(url=first.url)
    embed.set_footer(text=f"⭐ {stars}  •  #{message.channel.name}")
    return embed


def build_showcase_view(message: discord.Message, stars: int) -> discord.ui.View:
    view = discord.ui.View()
    view.add_item(
        discord.ui.Button(
            label="Original Post",
            style=discord.ButtonStyle.link,
            url=message.jump_url,
            emoji="🔗",
        )
    )
    view.add_item(
        discord.ui.Button(
            label=f"{stars}",
            style=discord.ButtonStyle.secondary,
            disabled=True,
            emoji="⭐",
        )
    )
    return view


async def get_unique_star_count(
    orig_msg: Optional[discord.Message],
    showcase_msg: Optional[discord.Message] = None,
) -> int:
    unique_users = set()
    for msg in (orig_msg, showcase_msg):
        if not msg:
            continue
        for reaction in msg.reactions:
            if str(reaction.emoji) == "⭐":
                try:
                    async for user in reaction.users():
                        if not user.bot:
                            unique_users.add(user.id)
                except Exception:
                    pass
    return len(unique_users)


# -------------------------------------------------------------
# 7. Interactive Admin Control Panel (/panel)
# -------------------------------------------------------------
def build_panel_embed(guild: discord.Guild) -> discord.Embed:
    settings = get_guild_settings(guild.id)
    ideas_ch = f"<#{settings['ideas_channel_id']}>" if settings["ideas_channel_id"] else "`Not Set`"
    showcase_ch = f"<#{settings['showcase_channel_id']}>" if settings["showcase_channel_id"] else "`Not Set`"
    booster_role = f"<@&{settings['booster_role_id']}>" if settings["booster_role_id"] else "`Not Set`"
    autorole = f"<@&{settings['autorole_id']}>" if settings["autorole_id"] else "`Disabled`"

    embed = discord.Embed(
        title="⚙️ Server Control Center",
        description="Configure channels, star threshold, booster protection, and member auto-role below.",
        color=0x5865F2,
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="💡 Ideas Channel", value=ideas_ch, inline=True)
    embed.add_field(name="🏆 Showcase Channel", value=showcase_ch, inline=True)
    embed.add_field(name="⭐ Star Threshold", value=f"`{settings['star_threshold']} ⭐`", inline=True)
    embed.add_field(name="🚀 Monitored Booster Role", value=booster_role, inline=True)
    embed.add_field(name="👋 Auto-Role (On Join)", value=autorole, inline=True)
    embed.add_field(
        name="📊 Server Activity",
        value=f"`{get_total_ideas_count(guild.id)}` ideas • `{get_total_posts_count(guild.id)}` posts",
        inline=True,
    )
    embed.set_footer(text="Use /event to schedule events • /post for styled announcements")
    return embed


class ThresholdModal(discord.ui.Modal, title="Set Star Threshold"):
    stars = discord.ui.TextInput(
        label="Required unique stars for Showcase",
        placeholder="e.g., 12",
        min_length=1,
        max_length=4,
    )

    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            val = int(self.stars.value.strip())
            if val < 1:
                raise ValueError
        except ValueError:
            return await interaction.response.send_message("❌ Enter a valid positive number.", ephemeral=True)

        update_guild_settings(self.guild_id, star_threshold=val)
        await interaction.response.edit_message(
            embed=build_panel_embed(interaction.guild), view=ControlPanelView(self.guild_id)
        )


class ControlPanelView(discord.ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=None)
        self.guild_id = guild_id

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="💡 Select Ideas Submission Channel...",
        row=0,
    )
    async def select_ideas(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        update_guild_settings(self.guild_id, ideas_channel_id=select.values[0].id)
        await interaction.response.edit_message(embed=build_panel_embed(interaction.guild), view=self)

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="🏆 Select Showcase Channel...",
        row=1,
    )
    async def select_showcase(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        update_guild_settings(self.guild_id, showcase_channel_id=select.values[0].id)
        await interaction.response.edit_message(embed=build_panel_embed(interaction.guild), view=self)

    @discord.ui.select(
        cls=discord.ui.RoleSelect,
        placeholder="🚀 Select Monitored Booster Role...",
        row=2,
    )
    async def select_booster_role(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        update_guild_settings(self.guild_id, booster_role_id=select.values[0].id)
        await interaction.response.edit_message(embed=build_panel_embed(interaction.guild), view=self)

    @discord.ui.select(
        cls=discord.ui.RoleSelect,
        placeholder="👋 Select Auto-Role for New Members (On Join)...",
        row=3,
    )
    async def select_autorole(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        update_guild_settings(self.guild_id, autorole_id=select.values[0].id)
        await interaction.response.edit_message(embed=build_panel_embed(interaction.guild), view=self)

    @discord.ui.button(label="Star Threshold", style=discord.ButtonStyle.primary, emoji="⭐", row=4)
    async def change_threshold(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ThresholdModal(self.guild_id))

    @discord.ui.button(label="Clear Auto-Role", style=discord.ButtonStyle.secondary, emoji="🚫", row=4)
    async def clear_autorole(self, interaction: discord.Interaction, button: discord.ui.Button):
        update_guild_settings(self.guild_id, autorole_id=None)
        await interaction.response.edit_message(embed=build_panel_embed(interaction.guild), view=self)


# -------------------------------------------------------------
# 8. Background Task: Event Reminders & Live Start Trigger
# -------------------------------------------------------------
@tasks.loop(seconds=20)
async def check_scheduled_events():
    now = int(time.time())
    pending = get_pending_events()

    for ev in pending:
        guild = bot.get_guild(ev["guild_id"])
        if not guild:
            continue
        channel = guild.get_channel(ev["channel_id"])
        if not channel:
            continue

        role = guild.get_role(ev["role_id"])
        role_mention = role.mention if role else ""
        followers_count = sum(1 for m in role.members if not m.bot) if role else ev["followers_count"]

        remind_ts = ev["start_timestamp"] - (ev["remind_minutes"] * 60)

        # 1) Send 10-minute (or custom) pre-start reminder
        if not ev["reminded"] and now >= remind_ts and now < ev["start_timestamp"]:
            ev["reminded"] = 1
            ev["followers_count"] = followers_count
            save_scheduled_event(**ev)

            jump_url = f"https://discord.com/channels/{ev['guild_id']}/{ev['channel_id']}/{ev['message_id']}"
            remind_embed = discord.Embed(
                title=f"⏰ Starting Soon: {ev['title']}",
                description=(
                    f"Get ready! **{ev['title']}** begins <t:{ev['start_timestamp']}:R>!\n"
                    f"🕒 **Start Time:** <t:{ev['start_timestamp']}:t>"
                ),
                color=0xE67E22,
            )
            view = discord.ui.View()
            view.add_item(discord.ui.Button(label="View Event Card", style=discord.ButtonStyle.link, url=jump_url, emoji="📅"))
            try:
                await channel.send(
                    content=f"🔔 {role_mention} — **Event starts in {ev['remind_minutes']} minutes!**",
                    embed=remind_embed,
                    view=view,
                    allowed_mentions=discord.AllowedMentions(roles=True),
                )
            except Exception as e:
                print(f"[Event Reminder Error] {e}")

        # 2) Trigger Event Start when countdown hits 0
        if not ev["started"] and now >= ev["start_timestamp"]:
            ev["started"] = 1
            ev["reminded"] = 1
            ev["followers_count"] = followers_count
            save_scheduled_event(**ev)

            try:
                msg = await channel.fetch_message(ev["message_id"])
                updated_embed = build_event_embed(
                    guild=guild,
                    title=ev["title"],
                    subtitle=ev["subtitle"],
                    description=ev["description"],
                    start_ts=ev["start_timestamp"],
                    role_id=ev["role_id"],
                    image_url=ev["image_url"],
                    remind_minutes=ev["remind_minutes"],
                    started=1,
                    followers_count=followers_count,
                )
                await msg.edit(embed=updated_embed, view=EventFollowView(followers_count=followers_count, started=True))
            except Exception:
                pass

            try:
                start_embed = discord.Embed(
                    title=f"🔴 LIVE NOW: {ev['title']}",
                    description=f"**{ev['title']}** has officially started! Join in now!",
                    color=0x2ECC71,
                )
                await channel.send(
                    content=f"🚀 {role_mention} — **{ev['title']} is LIVE NOW!**",
                    embed=start_embed,
                    allowed_mentions=discord.AllowedMentions(roles=True),
                )
            except Exception as e:
                print(f"[Event Start Error] {e}")


@check_scheduled_events.before_loop
async def before_check_events():
    await bot.wait_until_ready()


# -------------------------------------------------------------
# 9. Bot Gateway Events
# -------------------------------------------------------------
@bot.event
async def on_ready():
    bot.add_view(EventFollowView())
    if not check_scheduled_events.is_running():
        check_scheduled_events.start()

    print(f"[Bot Online] Logged in as {bot.user} (ID: {bot.user.id})")
    try:
        synced = await bot.tree.sync()
        print(f"[Slash Commands] Globally synced {len(synced)} commands.")
    except Exception as e:
        print(f"[Sync Error] {e}")


@bot.event
async def on_member_join(member: discord.Member):
    if member.bot:
        return
    settings = get_guild_settings(member.guild.id)
    autorole_id = settings.get("autorole_id")
    if not autorole_id:
        return
    role = member.guild.get_role(autorole_id)
    if role and member.guild.me.top_role > role:
        try:
            await member.add_roles(role, reason="Auto-Role on Join")
        except Exception as e:
            print(f"[Auto-Role Error] {e}")


@bot.event
async def on_message(message: discord.Message):
    if not message.guild or message.author.bot:
        return
    settings = get_guild_settings(message.guild.id)
    if settings["ideas_channel_id"] and message.channel.id == settings["ideas_channel_id"]:
        try:
            await message.add_reaction("⭐")
        except Exception:
            pass
    await bot.process_commands(message)


async def process_star_reaction(payload: discord.RawReactionActionEvent, is_add: bool):
    if str(payload.emoji) != "⭐" or not payload.guild_id:
        return
    if bot.user and payload.user_id == bot.user.id:
        return

    settings = get_guild_settings(payload.guild_id)
    ideas_ch_id = settings["ideas_channel_id"]
    showcase_ch_id = settings["showcase_channel_id"]
    threshold = settings["star_threshold"]

    if not ideas_ch_id or not showcase_ch_id:
        return

    showcase_channel = bot.get_channel(showcase_ch_id)
    ideas_channel = bot.get_channel(ideas_ch_id)
    if not showcase_channel or not ideas_channel:
        return

    if payload.channel_id == ideas_ch_id:
        entry = get_entry_by_orig(payload.message_id)
        try:
            orig_msg = await ideas_channel.fetch_message(payload.message_id)
        except (discord.NotFound, discord.Forbidden):
            return

        if entry:
            showcase_msg_id = entry[0]
            if is_add:
                user = bot.get_user(payload.user_id)
                if not user:
                    try:
                        user = await bot.fetch_user(payload.user_id)
                    except Exception:
                        user = None
                if user:
                    try:
                        await orig_msg.remove_reaction("⭐", user)
                    except Exception:
                        pass

            showcase_msg = None
            try:
                showcase_msg = await showcase_channel.fetch_message(showcase_msg_id)
            except discord.NotFound:
                pass

            total_stars = await get_unique_star_count(orig_msg, showcase_msg)
            embed = build_showcase_embed(orig_msg, total_stars)
            view = build_showcase_view(orig_msg, total_stars)

            if showcase_msg:
                await showcase_msg.edit(embed=embed, view=view)
                save_starboard_entry(orig_msg.id, showcase_msg.id, payload.guild_id, orig_msg.channel.id, total_stars)
            else:
                new_showcase = await showcase_channel.send(embed=embed, view=view)
                try:
                    await new_showcase.add_reaction("⭐")
                except Exception:
                    pass
                save_starboard_entry(orig_msg.id, new_showcase.id, payload.guild_id, orig_msg.channel.id, total_stars)
        else:
            total_stars = await get_unique_star_count(orig_msg, None)
            if total_stars >= threshold:
                embed = build_showcase_embed(orig_msg, total_stars)
                view = build_showcase_view(orig_msg, total_stars)
                new_showcase = await showcase_channel.send(embed=embed, view=view)
                try:
                    await new_showcase.add_reaction("⭐")
                except Exception:
                    pass
                save_starboard_entry(orig_msg.id, new_showcase.id, payload.guild_id, orig_msg.channel.id, total_stars)

    elif payload.channel_id == showcase_ch_id:
        entry = get_entry_by_showcase(payload.message_id)
        if not entry:
            return
        orig_msg_id = entry[0]
        try:
            showcase_msg = await showcase_channel.fetch_message(payload.message_id)
        except (discord.NotFound, discord.Forbidden):
            return
        try:
            orig_msg = await ideas_channel.fetch_message(orig_msg_id)
        except (discord.NotFound, discord.Forbidden):
            orig_msg = None

        total_stars = await get_unique_star_count(orig_msg, showcase_msg)
        if orig_msg:
            embed = build_showcase_embed(orig_msg, total_stars)
            view = build_showcase_view(orig_msg, total_stars)
            await showcase_msg.edit(embed=embed, view=view)
            save_starboard_entry(orig_msg.id, showcase_msg.id, payload.guild_id, orig_msg.channel.id, total_stars)


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    await process_star_reaction(payload, is_add=True)


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    await process_star_reaction(payload, is_add=False)


@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    settings = get_guild_settings(after.guild.id)
    booster_role_id = settings["booster_role_id"]
    if not booster_role_id:
        return
    role = after.guild.get_role(booster_role_id)
    if not role or role not in after.roles:
        return
    if after.premium_since is None:
        try:
            await after.remove_roles(role, reason="Server boost expired")
        except Exception as e:
            print(f"[Booster Role Error] {e}")


# -------------------------------------------------------------
# 10. Slash Commands & Right-Click Context Menus
# -------------------------------------------------------------
@bot.tree.command(name="event", description="Create an interactive scheduled event with Follow button & auto-ping")
@app_commands.describe(
    channel="Channel where the event card will be published",
    role="Existing role given to followers & pinged before the event starts",
    image="Upload an image directly from your gallery (optional)",
    remind_before="Minutes before start to ping the role (Default: 10)",
)
@app_commands.default_permissions(administrator=True)
async def event_command(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    role: discord.Role,
    image: Optional[discord.Attachment] = None,
    remind_before: app_commands.Range[int, 1, 120] = 10,
):
    if interaction.guild.me.top_role <= role:
        return await interaction.response.send_message(
            f"❌ My highest role ({interaction.guild.me.top_role.mention}) must be placed **above** {role.mention} in **Server Settings ➔ Roles** so I can give it to followers!",
            ephemeral=True,
        )

    uploaded_url = ""
    if image:
        if not (image.content_type and image.content_type.startswith("image/")):
            return await interaction.response.send_message(
                "❌ Please upload a valid image file (PNG, JPG, GIF, WEBP).", ephemeral=True
            )
        uploaded_url = image.url

    await interaction.response.send_modal(
        EventComposerModal(
            target_channel=channel,
            role=role,
            remind_minutes=remind_before,
            uploaded_image_url=uploaded_url,
        )
    )


@bot.tree.command(name="event_image", description="Upload a new photo from your gallery to replace an event's banner")
@app_commands.describe(
    message_id_or_link="Message ID or Discord Link of the event card",
    image="Upload the new banner photo from your device gallery",
)
@app_commands.default_permissions(administrator=True)
async def event_image_command(
    interaction: discord.Interaction,
    message_id_or_link: str,
    image: discord.Attachment,
):
    if not (image.content_type and image.content_type.startswith("image/")):
        return await interaction.response.send_message(
            "❌ Please upload a valid image file from your gallery.", ephemeral=True
        )

    ids = re.findall(r"\d{15,22}", message_id_or_link)
    if not ids:
        return await interaction.response.send_message("❌ Invalid Message ID or link.", ephemeral=True)

    msg_id = int(ids[-1])
    ev = get_scheduled_event(msg_id)
    if not ev:
        return await interaction.response.send_message(
            "❌ Could not find a scheduled event with that Message ID.", ephemeral=True
        )

    channel = interaction.guild.get_channel(ev["channel_id"])
    if not channel:
        return await interaction.response.send_message("❌ Event channel not found.", ephemeral=True)

    try:
        msg = await channel.fetch_message(msg_id)
    except Exception:
        return await interaction.response.send_message("❌ Could not fetch the event message.", ephemeral=True)

    ev["image_url"] = image.url
    save_scheduled_event(**ev)

    new_embed = build_event_embed(
        guild=interaction.guild,
        title=ev["title"],
        subtitle=ev["subtitle"],
        description=ev["description"],
        start_ts=ev["start_timestamp"],
        role_id=ev["role_id"],
        image_url=image.url,
        remind_minutes=ev["remind_minutes"],
        started=ev["started"],
        followers_count=ev["followers_count"],
    )
    await msg.edit(embed=new_embed)
    await interaction.response.send_message(
        f"✅ **Event photo updated!** [Jump to Event]({msg.jump_url})", ephemeral=True
    )


@bot.tree.context_menu(name="📅 Edit Event")
@app_commands.default_permissions(administrator=True)
async def edit_event_context(interaction: discord.Interaction, message: discord.Message):
    ev = get_scheduled_event(message.id)
    if not ev:
        return await interaction.response.send_message(
            "❌ This message is not a scheduled event created via `/event`.", ephemeral=True
        )
    role = interaction.guild.get_role(ev["role_id"])
    if not role:
        return await interaction.response.send_message("❌ Event role no longer exists.", ephemeral=True)

    await interaction.response.send_modal(
        EventComposerModal(
            target_channel=message.channel,
            role=role,
            remind_minutes=ev["remind_minutes"],
            existing_event=ev,
            target_message=message,
        )
    )


@bot.tree.command(name="panel", description="Open the interactive server configuration & control center")
@app_commands.default_permissions(administrator=True)
async def panel_command(interaction: discord.Interaction):
    await interaction.response.send_message(
        embed=build_panel_embed(interaction.guild),
        view=ControlPanelView(interaction.guild.id),
        ephemeral=True,
    )


@bot.tree.command(name="post", description="Create a custom styled message from the bot")
@app_commands.describe(
    style="Choose a visual style preset",
    channel="Target channel (defaults to current)",
    ping_role="Optional role to mention",
)
@app_commands.choices(
    style=[
        app_commands.Choice(name="📢 Official Announcement (Blurple)", value="announcement"),
        app_commands.Choice(name="🚀 Update / Patch Notes (Emerald)", value="update"),
        app_commands.Choice(name="🎉 Event / Giveaway (Neon Pink)", value="event"),
        app_commands.Choice(name="📘 Server Guide / Info (Sky Blue)", value="guide"),
        app_commands.Choice(name="⚠️ Urgent Alert (Crimson Red)", value="alert"),
        app_commands.Choice(name="🖤 Minimalist Dark (Frameless)", value="minimal"),
    ]
)
@app_commands.default_permissions(administrator=True)
async def post_command(
    interaction: discord.Interaction,
    style: app_commands.Choice[str],
    channel: Optional[discord.TextChannel] = None,
    ping_role: Optional[discord.Role] = None,
):
    target_ch = channel or interaction.channel
    await interaction.response.send_modal(
        PostComposerModal(style_key=style.value, target_channel=target_ch, ping_role=ping_role)
    )


@bot.tree.context_menu(name="✏️ Edit Bot Post")
@app_commands.default_permissions(administrator=True)
async def edit_post_context(interaction: discord.Interaction, message: discord.Message):
    if message.author.id != bot.user.id:
        return await interaction.response.send_message("❌ I can only edit messages sent by me.", ephemeral=True)
    data = extract_post_data_from_message(message)
    await interaction.response.send_modal(
        PostComposerModal(
            style_key=data["style"],
            target_channel=message.channel,
            existing_message=message,
            initial_data=data,
        )
    )


@bot.tree.command(name="stats", description="View bot latency, uptime, and server statistics")
async def stats_command(interaction: discord.Interaction):
    uptime_sec = int(time.time() - START_TIME)
    hours, rem = divmod(uptime_sec, 3600)
    minutes, seconds = divmod(rem, 60)
    latency_ms = round(bot.latency * 1000)

    embed = discord.Embed(title="📊 System Diagnostics", color=0x2B2D31)
    embed.add_field(name="🏓 Latency", value=f"`{latency_ms} ms`", inline=True)
    embed.add_field(name="⏱️ Uptime", value=f"`{hours}h {minutes}m {seconds}s`", inline=True)
    embed.add_field(name="🏆 Featured Ideas", value=f"`{get_total_ideas_count(interaction.guild.id)}`", inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.command(name="sync")
@commands.has_permissions(administrator=True)
async def sync_guild(ctx: commands.Context):
    bot.tree.copy_global_to(guild=ctx.guild)
    synced = await bot.tree.sync(guild=ctx.guild)
    await ctx.send(f"⚡ Instantly synced **{len(synced)}** commands & menus to this server!")


if __name__ == "__main__":
    TOKEN = os.getenv("DISCORD_TOKEN")
    if not TOKEN:
        print("[CRITICAL ERROR] DISCORD_TOKEN environment variable is missing!")
        sys.exit(1)
    bot.run(TOKEN)
