import os
import sys
import re
import time
import asyncio
import subprocess
import sqlite3
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple

# -------------------------------------------------------------
# 1. Automatic Dependency Check & Installation
# -------------------------------------------------------------
try:
    import discord
    from discord import app_commands
    from discord.ext import commands
except ImportError:
    print("[Setup] Installing discord.py...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "discord.py"])
    import discord
    from discord import app_commands
    from discord.ext import commands

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# -------------------------------------------------------------
# 2. SQLite Database with Persistent Storage & Auto-Migration
# -------------------------------------------------------------
DATA_DIR = os.getenv("DATA_DIR", ".")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "bot_database.db")

START_TIME = time.time()


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
        # Safe migration for existing databases that don't have autorole_id yet
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
            "CREATE INDEX IF NOT EXISTS idx_showcase_msg ON starboard_entries(showcase_message_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_bot_posts_guild ON bot_posts(guild_id)"
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
            """
            SELECT showcase_message_id, channel_id, stars_count 
            FROM starboard_entries WHERE orig_message_id = ?
            """,
            (orig_message_id,),
        )
        return cursor.fetchone()


def get_entry_by_showcase(showcase_message_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT orig_message_id, channel_id, stars_count 
            FROM starboard_entries WHERE showcase_message_id = ?
            """,
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


def delete_bot_post_record(message_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM bot_posts WHERE message_id = ?", (message_id,))
        conn.commit()


# -------------------------------------------------------------
# 3. Discord Bot Initialization
# -------------------------------------------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.reactions = True

bot = commands.Bot(
    command_prefix="!",
    intents=intents,
    activity=discord.Activity(
        type=discord.ActivityType.watching,
        name="for top ideas ⭐"
    ),
    status=discord.Status.online,
)


# -------------------------------------------------------------
# 4. Visual Presets & Post Builder Engine
# -------------------------------------------------------------
POST_STYLES: Dict[str, Dict[str, Any]] = {
    "announcement": {
        "name": "📢 Official Announcement",
        "color": 0x5865F2,  # Discord Blurple
        "badge": "📢 OFFICIAL ANNOUNCEMENT",
        "emoji": "📢",
        "show_author": True,
        "show_timestamp": True,
        "quote_body": False,
    },
    "update": {
        "name": "🚀 Update / Patch Notes",
        "color": 0x10B981,  # Emerald Green
        "badge": "🚀 UPDATE & CHANGELOG",
        "emoji": "🚀",
        "show_author": True,
        "show_timestamp": True,
        "quote_body": False,
    },
    "event": {
        "name": "🎉 Event / Giveaway",
        "color": 0xEC4899,  # Neon Pink
        "badge": "🎉 COMMUNITY EVENT",
        "emoji": "🎉",
        "show_author": True,
        "show_timestamp": True,
        "quote_body": False,
    },
    "guide": {
        "name": "📘 Guide / Information",
        "color": 0x0EA5E9,  # Sky Blue
        "badge": "📘 SERVER GUIDE & INFO",
        "emoji": "📘",
        "show_author": True,
        "show_timestamp": False,
        "quote_body": False,
    },
    "alert": {
        "name": "⚠️ Urgent Alert",
        "color": 0xEF4444,  # Crimson Red
        "badge": "⚠️ IMPORTANT NOTICE",
        "emoji": "⚠️",
        "show_author": True,
        "show_timestamp": True,
        "quote_body": True,
    },
    "minimal": {
        "name": "🖤 Minimalist Dark",
        "color": 0x2B2D31,  # Sleek Dark Background Blend
        "badge": "",
        "emoji": "🖤",
        "show_author": False,
        "show_timestamp": False,
        "quote_body": False,
    },
}


def is_valid_url(url: str) -> bool:
    """Validate HTTP/HTTPS URLs to prevent Discord API errors."""
    if not url:
        return False
    url = url.strip()
    return bool(re.match(r"^https?://[^\s/$.?#].[^\s]*$", url, re.IGNORECASE))


def parse_button_input(raw_input: str) -> Tuple[str, str]:
    """
    Parse 'Label | https://url' or just 'https://url' into (label, url).
    Returns ('', '') if empty or invalid.
    """
    if not raw_input or not raw_input.strip():
        return "", ""
    raw = raw_input.strip()
    if "|" in raw:
        parts = raw.split("|", 1)
        label = parts[0].strip()[:80] or "Open Link"
        url = parts[1].strip()
    else:
        label = "Open Link"
        url = raw

    if is_valid_url(url):
        return label, url
    return "", ""


def extract_post_data_from_message(message: discord.Message) -> Dict[str, Any]:
    """
    Retrieve post metadata from SQLite or reconstruct it from the Discord message embed
    so any bot message can be edited seamlessly.
    """
    saved = get_bot_post(message.id)
    if saved:
        return saved

    style = "announcement"
    title = ""
    content = message.content or ""
    image_url = ""
    footer_text = ""
    button_label = ""
    button_url = ""

    if message.embeds:
        emb = message.embeds[0]
        title = emb.title or ""
        content = emb.description or ""
        if content.startswith(">>> "):
            content = content[4:]
        if emb.image and emb.image.url:
            image_url = emb.image.url
        if emb.footer and emb.footer.text:
            footer_text = emb.footer.text

        if emb.color:
            for key, preset in POST_STYLES.items():
                if preset["color"] == emb.color.value:
                    style = key
                    break

    if message.components:
        for row in message.components:
            for child in getattr(row, "children", []):
                if getattr(child, "url", None):
                    button_label = getattr(child, "label", None) or "Open Link"
                    button_url = child.url
                    break

    return {
        "message_id": message.id,
        "channel_id": message.channel.id,
        "guild_id": message.guild.id if message.guild else 0,
        "style": style,
        "title": title,
        "content": content,
        "image_url": image_url,
        "footer_text": footer_text,
        "button_label": button_label,
        "button_url": button_url,
        "author_id": 0,
    }


def build_styled_post(
    guild: Optional[discord.Guild],
    style_key: str,
    title: str,
    content: str,
    image_url: str = "",
    footer_text: str = "",
    button_label: str = "",
    button_url: str = "",
) -> Tuple[discord.Embed, Optional[discord.ui.View]]:
    """Construct the styled Discord Embed and optional Link Button View."""
    preset = POST_STYLES.get(style_key, POST_STYLES["announcement"])

    formatted_body = content.strip()
    if preset.get("quote_body") and formatted_body and not formatted_body.startswith(">>>"):
        formatted_body = f">>> {formatted_body}"

    embed = discord.Embed(
        title=title.strip() if title and title.strip() else None,
        description=formatted_body if formatted_body else "\u200b",
        color=preset["color"],
        timestamp=datetime.now(timezone.utc) if preset.get("show_timestamp") else None,
    )

    if preset.get("show_author") and preset.get("badge"):
        icon_url = guild.icon.url if (guild and guild.icon) else (bot.user.display_avatar.url if bot.user else None)
        embed.set_author(name=preset["badge"], icon_url=icon_url)

    if image_url and is_valid_url(image_url):
        embed.set_image(url=image_url.strip())

    if footer_text and footer_text.strip():
        icon_url = guild.icon.url if (guild and guild.icon) else None
        embed.set_footer(text=footer_text.strip(), icon_url=icon_url)
    elif preset.get("show_timestamp") and guild:
        embed.set_footer(text=guild.name, icon_url=guild.icon.url if guild.icon else None)

    view = None
    if button_url and is_valid_url(button_url):
        view = discord.ui.View(timeout=None)
        view.add_item(
            discord.ui.Button(
                label=(button_label.strip() if button_label else "Open Link")[:80],
                style=discord.ButtonStyle.link,
                url=button_url.strip(),
                emoji="🔗",
            )
        )

    return embed, view


# -------------------------------------------------------------
# 5. Post Composer & Editor Modals and Views
# -------------------------------------------------------------
class PostModal(discord.ui.Modal):
    def __init__(
        self,
        *,
        mode: str = "create",
        target_channel: Optional[discord.TextChannel] = None,
        target_message: Optional[discord.Message] = None,
        style_key: str = "announcement",
        mention_role: Optional[discord.Role] = None,
        initial_data: Optional[Dict[str, Any]] = None,
    ):
        preset = POST_STYLES.get(style_key, POST_STYLES["announcement"])
        modal_title = "Edit Bot Post" if mode == "edit" else f"Create: {preset['name'][:28]}"
        super().__init__(title=modal_title)

        self.mode = mode
        self.target_channel = target_channel
        self.target_message = target_message
        self.style_key = style_key
        self.mention_role = mention_role

        data = initial_data or {}
        default_btn = ""
        if data.get("button_url"):
            lbl = data.get("button_label") or "Open Link"
            default_btn = f"{lbl} | {data['button_url']}"

        self.post_title = discord.ui.TextInput(
            label="Post Title (Optional)",
            placeholder="e.g. Season 4 Update is Live!",
            default=data.get("title", "")[:256],
            required=False,
            max_length=256,
        )
        self.post_content = discord.ui.TextInput(
            label="Message Content (Supports Markdown)",
            placeholder="Write your announcement, patch notes, or message here...",
            style=discord.TextStyle.paragraph,
            default=data.get("content", "")[:3900],
            required=True,
            max_length=3900,
        )
        self.post_image = discord.ui.TextInput(
            label="Banner / Image URL (Optional)",
            placeholder="https://example.com/banner.png",
            default=data.get("image_url", "")[:500],
            required=False,
            max_length=500,
        )
        self.post_footer = discord.ui.TextInput(
            label="Footer Text (Optional)",
            placeholder="e.g. CMTD Administration Team",
            default=data.get("footer_text", "")[:200],
            required=False,
            max_length=200,
        )
        self.post_button = discord.ui.TextInput(
            label="Link Button: Label | https://url (Optional)",
            placeholder="Read More | https://discord.gg/...",
            default=default_btn[:300],
            required=False,
            max_length=300,
        )

        self.add_item(self.post_title)
        self.add_item(self.post_content)
        self.add_item(self.post_image)
        self.add_item(self.post_footer)
        self.add_item(self.post_button)

    async def on_submit(self, interaction: discord.Interaction):
        title_val = self.post_title.value.strip()
        content_val = self.post_content.value.strip()
        image_val = self.post_image.value.strip()
        footer_val = self.post_footer.value.strip()
        btn_label, btn_url = parse_button_input(self.post_button.value)

        if image_val and not is_valid_url(image_val):
            return await interaction.response.send_message(
                "❌ **Invalid Image URL!** Please make sure the image link starts with `https://` or `http://`.",
                ephemeral=True,
            )

        if self.post_button.value.strip() and not btn_url:
            return await interaction.response.send_message(
                "❌ **Invalid Button URL!** Use the format: `Button Name | https://example.com`",
                ephemeral=True,
            )

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

        if self.mode == "create" and self.target_channel:
            mention_text = self.mention_role.mention if self.mention_role else None
            allowed_mentions = discord.AllowedMentions(roles=True, everyone=True)
            try:
                sent_msg = await self.target_channel.send(
                    content=mention_text,
                    embed=embed,
                    view=view,
                    allowed_mentions=allowed_mentions,
                )
            except discord.Forbidden:
                return await interaction.response.send_message(
                    f"❌ I don't have permission to send messages or embeds in {self.target_channel.mention}.",
                    ephemeral=True,
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

            confirm_embed = build_post_controller_embed(sent_msg, self.style_key)
            await interaction.response.send_message(
                embed=confirm_embed,
                view=PostQuickControlView(sent_msg, self.style_key),
                ephemeral=True,
            )

        elif self.mode == "edit" and self.target_message:
            try:
                await self.target_message.edit(embed=embed, view=view)
            except discord.DiscordException as e:
                return await interaction.response.send_message(
                    f"❌ Failed to edit message: `{e}`",
                    ephemeral=True,
                )

            save_bot_post(
                message_id=self.target_message.id,
                channel_id=self.target_message.channel.id,
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

            confirm_embed = build_post_controller_embed(self.target_message, self.style_key, updated=True)
            await interaction.response.send_message(
                embed=confirm_embed,
                view=PostQuickControlView(self.target_message, self.style_key),
                ephemeral=True,
            )


def build_post_controller_embed(message: discord.Message, style_key: str, updated: bool = False) -> discord.Embed:
    preset = POST_STYLES.get(style_key, POST_STYLES["announcement"])
    status_title = "✅ Post Updated Successfully" if updated else "✨ Post Published Successfully"
    embed = discord.Embed(
        title=status_title,
        description=(
            f"Your message is live in {message.channel.mention}!\n"
            f"Use the controls below to edit text, switch visual styles on the fly, or right-click the message anytime (**Apps ➔ ✏️ Edit Bot Post**)."
        ),
        color=preset["color"],
    )
    embed.add_field(name="🎨 Active Style", value=f"**{preset['name']}**", inline=True)
    embed.add_field(name="🆔 Message ID", value=f"`{message.id}`", inline=True)
    embed.add_field(name="🔗 Direct Link", value=f"[Jump to Message]({message.jump_url})", inline=True)
    return embed


class PostQuickControlView(discord.ui.View):
    """Interactive ephemeral control bar to edit or restyle a post immediately."""

    def __init__(self, target_message: discord.Message, current_style: str):
        super().__init__(timeout=900)
        self.target_message = target_message
        self.current_style = current_style

        options = [
            discord.SelectOption(
                label=preset["name"].split(" ", 1)[1],
                value=key,
                emoji=preset["emoji"],
                default=(key == current_style),
                description=f"Switch post appearance to {key.capitalize()} style",
            )
            for key, preset in POST_STYLES.items()
        ]
        self.style_select.options = options

    @discord.ui.select(
        placeholder="🎨 Switch visual style instantly...",
        min_values=1,
        max_values=1,
        row=0,
    )
    async def style_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        new_style = select.values[0]
        self.current_style = new_style

        data = extract_post_data_from_message(self.target_message)
        data["style"] = new_style

        embed, view = build_styled_post(
            guild=interaction.guild,
            style_key=new_style,
            title=data["title"],
            content=data["content"],
            image_url=data["image_url"],
            footer_text=data["footer_text"],
            button_label=data["button_label"],
            button_url=data["button_url"],
        )

        try:
            await self.target_message.edit(embed=embed, view=view)
        except discord.NotFound:
            return await interaction.response.send_message("❌ Target message no longer exists.", ephemeral=True)

        save_bot_post(
            message_id=self.target_message.id,
            channel_id=self.target_message.channel.id,
            guild_id=interaction.guild.id,
            style=new_style,
            title=data["title"],
            content=data["content"],
            image_url=data["image_url"],
            footer_text=data["footer_text"],
            button_label=data["button_label"],
            button_url=data["button_url"],
            author_id=interaction.user.id,
        )

        for opt in self.style_select.options:
            opt.default = (opt.value == new_style)

        confirm_embed = build_post_controller_embed(self.target_message, new_style, updated=True)
        await interaction.response.edit_message(embed=confirm_embed, view=self)

    @discord.ui.button(label="Edit Text & Links", style=discord.ButtonStyle.primary, emoji="✏️", row=1)
    async def edit_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = extract_post_data_from_message(self.target_message)
        await interaction.response.send_modal(
            PostModal(
                mode="edit",
                target_message=self.target_message,
                style_key=data["style"],
                initial_data=data,
            )
        )

    @discord.ui.button(label="Delete Post", style=discord.ButtonStyle.danger, emoji="🗑️", row=1)
    async def delete_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.target_message.delete()
            delete_bot_post_record(self.target_message.id)
        except discord.NotFound:
            pass
        await interaction.response.edit_message(
            content="🗑️ **Post has been deleted.**",
            embed=None,
            view=None,
        )


# -------------------------------------------------------------
# 6. Sleek & Compact Ideas Showcase Cards (Starboard)
# -------------------------------------------------------------
def get_tier_info(stars: int) -> Tuple[int, str]:
    """Dynamic border color and subtle rank badge based on vote count."""
    if stars >= 50:
        return 0xA855F7, "🟣 Legendary Idea"
    elif stars >= 25:
        return 0xF97316, "🔥 Trending Idea"
    elif stars >= 18:
        return 0xFACC15, "✨ Community Choice"
    return 0x38BDF8, "⭐ Featured Idea"


def build_showcase_embed(message: discord.Message, total_stars: int) -> discord.Embed:
    """Compact, modern embed layout for the Showcase channel."""
    color, tier_label = get_tier_info(total_stars)

    content_text = message.content.strip() if message.content else "*[Media Attachment]*"

    embed = discord.Embed(
        description=content_text,
        color=color,
        timestamp=message.created_at,
    )

    embed.set_author(
        name=f"{message.author.display_name}",
        icon_url=message.author.display_avatar.url,
        url=message.jump_url,
    )

    if message.attachments:
        for att in message.attachments:
            if att.content_type and att.content_type.startswith("image/"):
                embed.set_image(url=att.url)
                break

    embed.set_footer(text=f"{tier_label} • ⭐ {total_stars} • #{message.channel.name}")
    return embed


def build_jump_view(message: discord.Message, total_stars: int) -> discord.ui.View:
    """Compact action row with jump button and live star counter badge."""
    view = discord.ui.View(timeout=None)
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
            label=f"{total_stars}",
            style=discord.ButtonStyle.secondary,
            disabled=True,
            emoji="⭐",
        )
    )
    return view


# -------------------------------------------------------------
# 7. Mass Role Assignment Helper
# -------------------------------------------------------------
async def execute_mass_role(interaction: discord.Interaction, role: discord.Role):
    """Assigns a role to all human members on the server with a live progress bar."""
    guild = interaction.guild
    if not guild:
        return

    if guild.me.top_role <= role:
        return await interaction.response.send_message(
            f"❌ **Role Hierarchy Error!**\n"
            f"My highest role ({guild.me.top_role.mention}) must be dragged **above** {role.mention} in **Server Settings ➔ Roles**.",
            ephemeral=True,
        )

    if role.managed:
        return await interaction.response.send_message(
            f"❌ {role.mention} is managed by an external integration/bot and cannot be assigned manually.",
            ephemeral=True,
        )

    members_to_update = [m for m in guild.members if not m.bot and role not in m.roles]
    total = len(members_to_update)

    if total == 0:
        return await interaction.response.send_message(
            f"✅ All human members on the server already have the {role.mention} role!",
            ephemeral=True,
        )

    embed = discord.Embed(
        title="⏳ Mass Role Assignment in Progress...",
        description=(
            f"Assigning {role.mention} to **{total}** members.\n"
            f"`[░░░░░░░░░░] 0%` (`0/{total}`)"
        ),
        color=0x5865F2,
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

    added = 0
    failed = 0

    for idx, member in enumerate(members_to_update, start=1):
        try:
            await member.add_roles(role, reason=f"Mass role assigned by {interaction.user}")
            added += 1
        except discord.DiscordException:
            failed += 1

        if idx % 10 == 0 or idx == total:
            pct = int((idx / total) * 100)
            filled = int(pct / 10)
            bar = "█" * filled + "░" * (10 - filled)
            progress_embed = discord.Embed(
                title="⏳ Mass Role Assignment in Progress..." if idx < total else "🎉 Mass Role Assignment Complete!",
                description=(
                    f"Target Role: {role.mention}\n"
                    f"`[{bar}] {pct}%` (`{idx}/{total}`)\n\n"
                    f"✅ **Assigned:** `{added}`  •  ⚠️ **Failed:** `{failed}`"
                ),
                color=0x5865F2 if idx < total else 0x10B981,
                timestamp=datetime.now(timezone.utc),
            )
            try:
                await interaction.edit_original_response(embed=progress_embed)
            except discord.DiscordException:
                pass

        await asyncio.sleep(0.6)


# -------------------------------------------------------------
# 8. Interactive Control Panel (/panel) with Live Refresh
# -------------------------------------------------------------
def build_panel_embed(guild: discord.Guild) -> discord.Embed:
    settings = get_guild_settings(guild.id)
    ideas_ch = f"<#{settings['ideas_channel_id']}>" if settings["ideas_channel_id"] else "🔴 *Not configured*"
    showcase_ch = f"<#{settings['showcase_channel_id']}>" if settings["showcase_channel_id"] else "🔴 *Not configured*"
    booster_role = f"<@&{settings['booster_role_id']}>" if settings["booster_role_id"] else "🔴 *Not configured*"
    autorole = f"<@&{settings['autorole_id']}>" if settings["autorole_id"] else "🔴 *Not configured*"

    ready_status = (
        "🟢 **System Active**"
        if (settings["ideas_channel_id"] and settings["showcase_channel_id"])
        else "🟡 **Setup Required**"
    )

    embed = discord.Embed(
        title="⚙️ CMTD Bot — Interactive Control Center",
        description=(
            f"Status: {ready_status}\n"
            "Select channels, roles, or adjust the star threshold using the controls below. Changes apply immediately."
        ),
        color=0x5865F2,
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="💡 Ideas Channel", value=ideas_ch, inline=True)
    embed.add_field(name="🏆 Showcase Channel", value=showcase_ch, inline=True)
    embed.add_field(name="⭐ Star Threshold", value=f"**`{settings['star_threshold']} ⭐`**", inline=True)
    embed.add_field(name="💎 Monitored Booster Role", value=booster_role, inline=True)
    embed.add_field(name="👋 Auto-Role on Join", value=autorole, inline=True)
    embed.add_field(
        name="📢 Post Studio & Mass Role",
        value="Use `/post` to publish styled messages, `/massrole` to give any role to everyone, or click the buttons below.",
        inline=False,
    )
    embed.set_footer(text=f"{guild.name} • Live Configuration", icon_url=guild.icon.url if guild.icon else None)
    return embed


class ThresholdModal(discord.ui.Modal, title="Set Star Threshold"):
    stars = discord.ui.TextInput(
        label="Minimum stars required for showcase",
        placeholder="e.g. 12",
        default="12",
        min_length=1,
        max_length=3,
    )

    def __init__(self, guild_id: int, panel_view: "ControlPanelView"):
        super().__init__()
        self.guild_id = guild_id
        self.panel_view = panel_view
        current = get_guild_settings(guild_id)["star_threshold"]
        self.stars.default = str(current)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            val = int(self.stars.value.strip())
            if val < 1:
                raise ValueError
        except ValueError:
            return await interaction.response.send_message(
                "❌ Please enter a valid positive number (1 or higher).", ephemeral=True
            )

        update_guild_settings(self.guild_id, star_threshold=val)
        await interaction.response.edit_message(
            embed=build_panel_embed(interaction.guild),
            view=self.panel_view,
        )


class ControlPanelView(discord.ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=900)
        self.guild_id = guild_id

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="💡 Select Ideas Submission Channel...",
        row=0,
    )
    async def select_ideas(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        ch = select.values[0]
        update_guild_settings(self.guild_id, ideas_channel_id=ch.id)
        await interaction.response.edit_message(embed=build_panel_embed(interaction.guild), view=self)

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="🏆 Select Showcase Channel...",
        row=1,
    )
    async def select_showcase(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        ch = select.values[0]
        update_guild_settings(self.guild_id, showcase_channel_id=ch.id)
        await interaction.response.edit_message(embed=build_panel_embed(interaction.guild), view=self)

    @discord.ui.select(
        cls=discord.ui.RoleSelect,
        placeholder="💎 Select Monitored Booster Role...",
        row=2,
    )
    async def select_booster_role(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role = select.values[0]
        update_guild_settings(self.guild_id, booster_role_id=role.id)
        await interaction.response.edit_message(embed=build_panel_embed(interaction.guild), view=self)

    @discord.ui.select(
        cls=discord.ui.RoleSelect,
        placeholder="👋 Select Auto-Role for New Members (On Join)...",
        row=3,
    )
    async def select_autorole(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role = select.values[0]
        update_guild_settings(self.guild_id, autorole_id=role.id)
        await interaction.response.edit_message(embed=build_panel_embed(interaction.guild), view=self)

    @discord.ui.button(label="Star Threshold", style=discord.ButtonStyle.primary, emoji="⭐", row=4)
    async def change_threshold(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ThresholdModal(self.guild_id, self))

    @discord.ui.button(label="Give Auto-Role to All", style=discord.ButtonStyle.secondary, emoji="👥", row=4)
    async def give_autorole_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings = get_guild_settings(self.guild_id)
        role_id = settings.get("autorole_id")
        if not role_id:
            return await interaction.response.send_message(
                "❌ Please select an **Auto-Role for New Members** in the dropdown above first, or use `/massrole`!",
                ephemeral=True,
            )
        role = interaction.guild.get_role(role_id)
        if not role:
            return await interaction.response.send_message(
                "❌ The configured Auto-Role no longer exists on this server.",
                ephemeral=True,
            )
        await execute_mass_role(interaction, role)

    @discord.ui.button(label="Create New Post", style=discord.ButtonStyle.success, emoji="📢", row=4)
    async def quick_post(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not isinstance(interaction.channel, discord.TextChannel):
            return await interaction.response.send_message(
                "❌ Please use this inside a standard text channel.", ephemeral=True
            )
        await interaction.response.send_modal(
            PostModal(
                mode="create",
                target_channel=interaction.channel,
                style_key="announcement",
            )
        )


# -------------------------------------------------------------
# 9. Reaction Handling & Two-Way Star Sync
# -------------------------------------------------------------
@bot.event
async def on_message(message: discord.Message):
    if not message.guild or message.author.bot:
        return

    settings = get_guild_settings(message.guild.id)
    if settings["ideas_channel_id"] and message.channel.id == settings["ideas_channel_id"]:
        try:
            await message.add_reaction("⭐")
        except discord.DiscordException:
            pass

    await bot.process_commands(message)


async def get_unique_star_count(
    orig_msg: Optional[discord.Message],
    showcase_msg: Optional[discord.Message],
) -> int:
    """Calculates distinct human voters across both messages to prevent double-voting."""
    unique_user_ids = set()
    for msg in (orig_msg, showcase_msg):
        if not msg:
            continue
        for react in msg.reactions:
            if str(react.emoji) == "⭐":
                async for user in react.users():
                    if not user.bot:
                        unique_user_ids.add(user.id)
    return len(unique_user_ids)


async def handle_star_sync(payload: discord.RawReactionActionEvent):
    """Synchronizes reactions between the ideas channel and the showcase channel."""
    if bot.user and payload.user_id == bot.user.id:
        return
    if str(payload.emoji) != "⭐" or not payload.guild_id:
        return

    settings = get_guild_settings(payload.guild_id)
    ideas_ch_id = settings["ideas_channel_id"]
    showcase_ch_id = settings["showcase_channel_id"]
    threshold = settings["star_threshold"]

    if not ideas_ch_id or not showcase_ch_id:
        return

    ideas_channel = bot.get_channel(ideas_ch_id)
    showcase_channel = bot.get_channel(showcase_ch_id)
    if not ideas_channel or not showcase_channel:
        return

    orig_msg = None
    showcase_msg = None

    # Case 1: Reaction in the original ideas channel
    if payload.channel_id == ideas_ch_id:
        try:
            orig_msg = await ideas_channel.fetch_message(payload.message_id)
        except (discord.NotFound, discord.Forbidden):
            return

        entry = get_entry_by_orig(payload.message_id)
        if entry:
            # Once featured in showcase, remove new stars placed on the original post
            if payload.event_type == "REACTION_ADD":
                try:
                    guild = bot.get_guild(payload.guild_id)
                    member = payload.member or (guild.get_member(payload.user_id) if guild else None)
                    if member and not member.bot:
                        await orig_msg.remove_reaction(payload.emoji, member)
                except discord.DiscordException:
                    pass

            try:
                showcase_msg = await showcase_channel.fetch_message(entry[0])
            except (discord.NotFound, discord.Forbidden):
                showcase_msg = None

    # Case 2: Reaction directly on the showcase card
    elif payload.channel_id == showcase_ch_id:
        entry = get_entry_by_showcase(payload.message_id)
        if not entry:
            return

        try:
            showcase_msg = await showcase_channel.fetch_message(payload.message_id)
            orig_msg = await ideas_channel.fetch_message(entry[0])
        except (discord.NotFound, discord.Forbidden):
            return
    else:
        return

    total_stars = await get_unique_star_count(orig_msg, showcase_msg)

    # First publication once threshold is met
    if total_stars >= threshold and not showcase_msg and orig_msg:
        embed = build_showcase_embed(orig_msg, total_stars)
        view = build_jump_view(orig_msg, total_stars)
        new_showcase = await showcase_channel.send(embed=embed, view=view)
        try:
            await new_showcase.add_reaction("⭐")
        except discord.DiscordException:
            pass
        save_starboard_entry(orig_msg.id, new_showcase.id, payload.guild_id, ideas_ch_id, total_stars)

    # Live update of an existing showcase card
    elif showcase_msg and orig_msg:
        embed = build_showcase_embed(orig_msg, total_stars)
        view = build_jump_view(orig_msg, total_stars)
        try:
            await showcase_msg.edit(embed=embed, view=view)
        except (discord.NotFound, discord.Forbidden):
            pass
        save_starboard_entry(orig_msg.id, showcase_msg.id, payload.guild_id, ideas_ch_id, total_stars)


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    await handle_star_sync(payload)


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    await handle_star_sync(payload)


# -------------------------------------------------------------
# 10. Auto-Role on Join & Booster Role Revocation System
# -------------------------------------------------------------
@bot.event
async def on_member_join(member: discord.Member):
    """Automatically assigns the configured Auto-Role to new human members."""
    if member.bot:
        return

    settings = get_guild_settings(member.guild.id)
    autorole_id = settings.get("autorole_id")
    if not autorole_id:
        return

    role = member.guild.get_role(autorole_id)
    if not role:
        return

    try:
        await member.add_roles(role, reason="Auto-Role assigned on server join")
        print(f"[Auto-Role] Assigned '{role.name}' to new member {member.display_name}")
    except discord.Forbidden:
        print(f"[Hierarchy Warning] Cannot assign Auto-Role '{role.name}' — move bot role higher in Server Settings!")
    except Exception as e:
        print(f"[Auto-Role Error] {e}")


@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    """Automatically revokes the monitored booster role when a member's boost expires."""
    settings = get_guild_settings(after.guild.id)
    booster_role_id = settings["booster_role_id"]
    if not booster_role_id:
        return

    role = after.guild.get_role(booster_role_id)
    if not role or role not in after.roles:
        return

    if after.premium_since is None:
        try:
            await after.remove_roles(role, reason="Server boost expired (auto-removal)")
            print(f"[Boosters] Revoked role '{role.name}' from {after.display_name}")
        except discord.Forbidden:
            print(f"[Hierarchy Warning] Bot role is below '{role.name}' in server settings!")
        except Exception as e:
            print(f"[Booster Revocation Error] {e}")


# -------------------------------------------------------------
# 11. Slash Commands & Context Menus
# -------------------------------------------------------------
@bot.tree.command(name="panel", description="Open the interactive bot control center")
@app_commands.default_permissions(administrator=True)
async def panel_command(interaction: discord.Interaction):
    await interaction.response.send_message(
        embed=build_panel_embed(interaction.guild),
        view=ControlPanelView(interaction.guild.id),
        ephemeral=True,
    )


@bot.tree.command(name="massrole", description="Give a specific role to all current human members on the server")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(role="Select the role to assign to all human members")
async def massrole_command(interaction: discord.Interaction, role: discord.Role):
    await execute_mass_role(interaction, role)


@bot.tree.command(name="post", description="Create and publish a styled bot message")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    style="Choose a visual preset for your post",
    channel="Target channel (defaults to current channel)",
    mention_role="Optional role to ping above the embed",
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
async def post_command(
    interaction: discord.Interaction,
    style: app_commands.Choice[str],
    channel: Optional[discord.TextChannel] = None,
    mention_role: Optional[discord.Role] = None,
):
    target_ch = channel or interaction.channel
    if not isinstance(target_ch, discord.TextChannel):
        return await interaction.response.send_message(
            "❌ Please select a valid text channel.", ephemeral=True
        )

    await interaction.response.send_modal(
        PostModal(
            mode="create",
            target_channel=target_ch,
            style_key=style.value,
            mention_role=mention_role,
        )
    )


@bot.tree.command(name="edit", description="Edit an existing bot message by ID or message link")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    message_id_or_link="Paste the Message ID or full Discord Message Link",
    new_style="Optionally switch the visual style while editing",
)
@app_commands.choices(
    new_style=[
        app_commands.Choice(name="📢 Official Announcement (Blurple)", value="announcement"),
        app_commands.Choice(name="🚀 Update / Patch Notes (Emerald)", value="update"),
        app_commands.Choice(name="🎉 Event / Giveaway (Neon Pink)", value="event"),
        app_commands.Choice(name="📘 Server Guide / Info (Sky Blue)", value="guide"),
        app_commands.Choice(name="⚠️ Urgent Alert (Crimson Red)", value="alert"),
        app_commands.Choice(name="🖤 Minimalist Dark (Frameless)", value="minimal"),
    ]
)
async def edit_command(
    interaction: discord.Interaction,
    message_id_or_link: str,
    new_style: Optional[app_commands.Choice[str]] = None,
):
    raw = message_id_or_link.strip()
    target_msg: Optional[discord.Message] = None

    link_match = re.search(r"channels/\d+/(\d+)/(\d+)", raw)
    if link_match:
        ch_id = int(link_match.group(1))
        msg_id = int(link_match.group(2))
        ch = interaction.guild.get_channel(ch_id)
        if isinstance(ch, discord.TextChannel):
            try:
                target_msg = await ch.fetch_message(msg_id)
            except discord.DiscordException:
                pass
    elif raw.isdigit():
        msg_id = int(raw)
        saved = get_bot_post(msg_id)
        if saved and saved["channel_id"]:
            ch = interaction.guild.get_channel(saved["channel_id"])
            if isinstance(ch, discord.TextChannel):
                try:
                    target_msg = await ch.fetch_message(msg_id)
                except discord.DiscordException:
                    pass
        if not target_msg and isinstance(interaction.channel, discord.TextChannel):
            try:
                target_msg = await interaction.channel.fetch_message(msg_id)
            except discord.DiscordException:
                pass

    if not target_msg:
        return await interaction.response.send_message(
            "❌ **Message not found!** Paste the full message link (Right-Click ➔ Copy Message Link) or run `/edit` in the same channel as the message ID.",
            ephemeral=True,
        )

    if not bot.user or target_msg.author.id != bot.user.id:
        return await interaction.response.send_message(
            "❌ I can only edit messages that were sent by me!",
            ephemeral=True,
        )

    data = extract_post_data_from_message(target_msg)
    chosen_style = new_style.value if new_style else data["style"]

    await interaction.response.send_modal(
        PostModal(
            mode="edit",
            target_message=target_msg,
            style_key=chosen_style,
            initial_data=data,
        )
    )


# --- Right-Click Context Menus (Apps -> ...) ---
@bot.tree.context_menu(name="✏️ Edit Bot Post")
@app_commands.default_permissions(administrator=True)
async def context_edit_post(interaction: discord.Interaction, message: discord.Message):
    if not bot.user or message.author.id != bot.user.id:
        return await interaction.response.send_message(
            "❌ You can only edit messages sent by this bot.",
            ephemeral=True,
        )

    data = extract_post_data_from_message(message)
    await interaction.response.send_modal(
        PostModal(
            mode="edit",
            target_message=message,
            style_key=data["style"],
            initial_data=data,
        )
    )


@bot.tree.context_menu(name="🎨 Change Post Style")
@app_commands.default_permissions(administrator=True)
async def context_style_post(interaction: discord.Interaction, message: discord.Message):
    if not bot.user or message.author.id != bot.user.id:
        return await interaction.response.send_message(
            "❌ You can only restyle messages sent by this bot.",
            ephemeral=True,
        )

    data = extract_post_data_from_message(message)
    embed = build_post_controller_embed(message, data["style"], updated=False)
    await interaction.response.send_message(
        embed=embed,
        view=PostQuickControlView(message, data["style"]),
        ephemeral=True,
    )


@bot.tree.command(name="stats", description="Display bot performance and server statistics")
async def stats_command(interaction: discord.Interaction):
    uptime_sec = int(time.time() - START_TIME)
    hours, remainder = divmod(uptime_sec, 3600)
    minutes, seconds = divmod(remainder, 60)
    uptime_str = f"{hours}h {minutes}m {seconds}s"

    latency_ms = round(bot.latency * 1000)
    ideas_count = get_total_ideas_count(interaction.guild.id)
    posts_count = get_total_posts_count(interaction.guild.id)

    embed = discord.Embed(
        title="📊 CMTD Bot — Live Telemetry",
        color=0x38BDF8,
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="⚡ Latency", value=f"`{latency_ms} ms`", inline=True)
    embed.add_field(name="⏳ Uptime", value=f"`{uptime_str}`", inline=True)
    embed.add_field(name="🏆 Featured Ideas", value=f"**`{ideas_count}`**", inline=True)
    embed.add_field(name="📢 Published Posts", value=f"**`{posts_count}`**", inline=True)
    embed.set_footer(text=f"Requested by {interaction.user.display_name}")

    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.command(name="sync")
@commands.has_permissions(administrator=True)
async def sync_guild(ctx: commands.Context):
    """Force instant slash & context menu command synchronization for this server."""
    bot.tree.copy_global_to(guild=ctx.guild)
    synced = await bot.tree.sync(guild=ctx.guild)
    await ctx.send(f"⚡ Instantly synced **{len(synced)}** command(s) and context menus for this server!")


@bot.event
async def on_ready():
    print("=" * 60)
    print(f"[Ready] Logged in as {bot.user} (ID: {bot.user.id})")
    try:
        synced = await bot.tree.sync()
        print(f"[Commands] Synced globally: {len(synced)} commands/menus")
    except Exception as e:
        print(f"[Sync Error] {e}")
    print("=" * 60)


# -------------------------------------------------------------
# 12. Startup Entrypoint
# -------------------------------------------------------------
if __name__ == "__main__":
    TOKEN = os.getenv("DISCORD_TOKEN")
    if not TOKEN:
        print("[CRITICAL ERROR] DISCORD_TOKEN is missing from environment variables!")
        sys.exit(1)

    try:
        bot.run(TOKEN)
    except discord.errors.PrivilegedIntentsRequired:
        print("\n" + "=" * 60)
        print("[INTENTS ERROR] Enable Privileged Gateway Intents in Discord Developer Portal:")
        print("1. Open https://discord.com/developers/applications")
        print("2. Select your Bot -> 'Bot' tab")
        print("3. Enable 'Server Members Intent' and 'Message Content Intent'")
        print("4. Click 'Save Changes'")
        print("=" * 60 + "\n")
    except Exception as e:
        print(f"[Startup Error] {e}")
