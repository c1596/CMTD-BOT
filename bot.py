import os
import re
import sys
import time
import asyncio
import sqlite3
import subprocess
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

try:
    from zoneinfo import ZoneInfo
    MSK_TZ = ZoneInfo("Europe/Moscow")
except Exception:
    MSK_TZ = timezone(timedelta(hours=3))

# -------------------------------------------------------------
# 1. Dependency Check & Environment Setup
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

START_TIME = time.time()

# -------------------------------------------------------------
# 2. High-Performance SQLite Database (WAL Mode + RAM Cache)
# -------------------------------------------------------------
DATA_DIR = os.getenv("DATA_DIR", "/app/data" if os.path.exists("/app") else ".")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "bot_database.db")

SETTINGS_CACHE: Dict[int, Dict[str, Any]] = {}
WFL_EMOJI_CACHE: Dict[int, List[discord.Emoji]] = {}
STARBOARD_LOCKS: Dict[int, asyncio.Lock] = {}
POLL_UPDATE_TASKS: Dict[int, asyncio.Task] = {}

WFL_EMOJI_NAMES = ("w1_win", "w1_fair", "w1_lose")
POLL_NUM_EMOJIS = ("1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣")


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA cache_size=-16000;")
    return conn


def init_db():
    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER PRIMARY KEY,
                ideas_channel_id INTEGER,
                showcase_channel_id INTEGER,
                star_threshold INTEGER DEFAULT 12,
                booster_role_id INTEGER,
                autorole_id INTEGER,
                wfl_channel_id INTEGER
            )
            """
        )
        c.execute("PRAGMA table_info(guild_settings)")
        cols = {row[1] for row in c.fetchall()}
        for col_name in ("booster_role_id", "autorole_id", "wfl_channel_id"):
            if col_name not in cols:
                c.execute(f"ALTER TABLE guild_settings ADD COLUMN {col_name} INTEGER")

        c.execute(
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
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS starboard_votes (
                orig_message_id INTEGER,
                user_id INTEGER,
                PRIMARY KEY (orig_message_id, user_id)
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS custom_posts (
                message_id INTEGER PRIMARY KEY,
                channel_id INTEGER,
                guild_id INTEGER,
                style TEXT,
                title TEXT,
                description TEXT,
                image_url TEXT,
                footer_text TEXT,
                button_label TEXT,
                button_url TEXT
            )
            """
        )
        c.execute(
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
                start_ts INTEGER,
                remind_minutes INTEGER DEFAULT 10,
                reminded INTEGER DEFAULT 0,
                started INTEGER DEFAULT 0
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS event_followers (
                message_id INTEGER,
                user_id INTEGER,
                PRIMARY KEY (message_id, user_id)
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS polls (
                message_id INTEGER PRIMARY KEY,
                channel_id INTEGER,
                guild_id INTEGER,
                creator_id INTEGER,
                question TEXT,
                description TEXT,
                options_packed TEXT,
                image_url TEXT,
                allow_multiple INTEGER DEFAULT 0,
                ends_at INTEGER DEFAULT 0,
                is_closed INTEGER DEFAULT 0
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS poll_votes (
                message_id INTEGER,
                user_id INTEGER,
                option_index INTEGER,
                PRIMARY KEY (message_id, user_id, option_index)
            )
            """
        )
        c.execute("CREATE INDEX IF NOT EXISTS idx_starboard_showcase ON starboard_entries(showcase_message_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_polls_active ON polls(is_closed, ends_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_events_active ON scheduled_events(started, start_ts)")
        conn.commit()
    print(f"[Database] Ready (WAL Mode) at: {DB_PATH}")


init_db()


def get_guild_settings(guild_id: int) -> Dict[str, Any]:
    if guild_id in SETTINGS_CACHE:
        return SETTINGS_CACHE[guild_id]
    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT ideas_channel_id, showcase_channel_id, star_threshold,
                   booster_role_id, autorole_id, wfl_channel_id
            FROM guild_settings WHERE guild_id = ?
            """,
            (guild_id,),
        )
        row = c.fetchone()
        data = {
            "ideas_channel_id": row[0] if row else None,
            "showcase_channel_id": row[1] if row else None,
            "star_threshold": row[2] if (row and row[2] is not None) else 12,
            "booster_role_id": row[3] if row else None,
            "autorole_id": row[4] if row else None,
            "wfl_channel_id": row[5] if row else None,
        }
        SETTINGS_CACHE[guild_id] = data
        return data


def update_guild_settings(guild_id: int, **kwargs):
    current = dict(get_guild_settings(guild_id))
    current.update(kwargs)
    SETTINGS_CACHE[guild_id] = current
    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO guild_settings (
                guild_id, ideas_channel_id, showcase_channel_id,
                star_threshold, booster_role_id, autorole_id, wfl_channel_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
                ideas_channel_id = excluded.ideas_channel_id,
                showcase_channel_id = excluded.showcase_channel_id,
                star_threshold = excluded.star_threshold,
                booster_role_id = excluded.booster_role_id,
                autorole_id = excluded.autorole_id,
                wfl_channel_id = excluded.wfl_channel_id
            """,
            (
                guild_id,
                current["ideas_channel_id"],
                current["showcase_channel_id"],
                current["star_threshold"],
                current["booster_role_id"],
                current["autorole_id"],
                current["wfl_channel_id"],
            ),
        )
        conn.commit()


def get_wfl_emojis(guild: discord.Guild) -> List[discord.Emoji]:
    if guild.id in WFL_EMOJI_CACHE:
        return WFL_EMOJI_CACHE[guild.id]
    guild_map = {e.name.lower(): e for e in guild.emojis}
    bot_map = {e.name.lower(): e for e in bot.emojis}
    found = []
    for name in WFL_EMOJI_NAMES:
        em = guild_map.get(name) or bot_map.get(name)
        if em:
            found.append(em)
    WFL_EMOJI_CACHE[guild.id] = found
    return found


def parse_message_reference(raw: str, default_channel_id: int) -> Tuple[Optional[int], Optional[int]]:
    raw = raw.strip()
    link_match = re.search(r"channels/(?:\d+|@me)/(\d+)/(\d+)", raw)
    if link_match:
        return int(link_match.group(1)), int(link_match.group(2))
    if raw.isdigit():
        return default_channel_id, int(raw)
    return None, None


# -------------------------------------------------------------
# 3. Discord Bot Initialization
# -------------------------------------------------------------
class CommunityBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.reactions = True
        super().__init__(
            command_prefix="!",
            intents=intents,
            activity=discord.Activity(type=discord.ActivityType.watching, name="80k+ community ⭐"),
            status=discord.Status.online,
        )

    async def setup_hook(self):
        self.add_view(PollView(option_count=5, closed=False))
        self.add_view(EventFollowView(count=0, disabled=False))
        if not background_scheduler.is_running():
            background_scheduler.start()


bot = CommunityBot()


# -------------------------------------------------------------
# 4. Post Studio Presets & Builders
# -------------------------------------------------------------
POST_STYLES: Dict[str, Dict[str, Any]] = {
    "announcement": {
        "name": "📢 Announcement",
        "color": 0xF1C40F,
        "badge": "📢 OFFICIAL ANNOUNCEMENT",
    },
    "update": {
        "name": "🚀 Update / Patch Notes",
        "color": 0x2ECC71,
        "badge": "🚀 UPDATE & PATCH NOTES",
    },
    "event": {
        "name": "🎉 Event / Giveaway",
        "color": 0xEB459E,
        "badge": "🎉 SPECIAL EVENT",
    },
    "guide": {
        "name": "📘 Guide / Info",
        "color": 0x3498DB,
        "badge": "📘 INFORMATION & GUIDE",
    },
    "alert": {
        "name": "⚠️ Urgent Alert",
        "color": 0xE74C3C,
        "badge": "⚠️ IMPORTANT NOTICE",
    },
    "minimal": {
        "name": "🖤 Minimalist Dark",
        "color": 0x2B2D31,
        "badge": None,
    },
}


def build_post_embed(
    guild: Optional[discord.Guild],
    style_key: str,
    title: str,
    description: str,
    image_url: str = "",
    footer_text: str = "",
) -> discord.Embed:
    style = POST_STYLES.get(style_key, POST_STYLES["announcement"])
    embed = discord.Embed(
        title=title.strip() if title.strip() else None,
        description=description.strip(),
        color=style["color"],
        timestamp=datetime.now(timezone.utc),
    )
    if style["badge"] and guild:
        embed.set_author(
            name=style["badge"],
            icon_url=guild.icon.url if guild.icon else None,
        )
    elif style["badge"]:
        embed.set_author(name=style["badge"])

    if image_url and image_url.strip().startswith(("http://", "https://")):
        embed.set_image(url=image_url.strip())

    if footer_text and footer_text.strip():
        embed.set_footer(
            text=footer_text.strip(),
            icon_url=guild.icon.url if (guild and guild.icon) else None,
        )
    elif guild:
        embed.set_footer(text=guild.name, icon_url=guild.icon.url if guild.icon else None)

    return embed


def build_link_view(button_label: str, button_url: str) -> Optional[discord.ui.View]:
    if button_label and button_url and button_url.strip().startswith(("http://", "https://")):
        view = discord.ui.View()
        view.add_item(
            discord.ui.Button(
                style=discord.ButtonStyle.link,
                label=button_label.strip()[:80],
                url=button_url.strip(),
            )
        )
        return view
    return None


def save_custom_post(
    message_id: int,
    channel_id: int,
    guild_id: int,
    style: str,
    title: str,
    description: str,
    image_url: str,
    footer_text: str,
    button_label: str,
    button_url: str,
):
    with get_db() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO custom_posts
            (message_id, channel_id, guild_id, style, title, description,
             image_url, footer_text, button_label, button_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                channel_id,
                guild_id,
                style,
                title,
                description,
                image_url,
                footer_text,
                button_label,
                button_url,
            ),
        )
        conn.commit()


def load_post_data_from_message(message: discord.Message) -> Dict[str, Any]:
    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT style, title, description, image_url, footer_text, button_label, button_url
            FROM custom_posts WHERE message_id = ?
            """,
            (message.id,),
        )
        row = c.fetchone()
    if row:
        return {
            "style": row[0] or "announcement",
            "title": row[1] or "",
            "description": row[2] or "",
            "image_url": row[3] or "",
            "footer_text": row[4] or "",
            "button_label": row[5] or "",
            "button_url": row[6] or "",
        }
    emb = message.embeds[0] if message.embeds else None
    btn_label, btn_url = "", ""
    for row_comp in message.components:
        for child in getattr(row_comp, "children", []):
            if getattr(child, "url", None):
                btn_label = getattr(child, "label", "") or ""
                btn_url = child.url
                break
    return {
        "style": "announcement",
        "title": (emb.title or "") if emb else "",
        "description": (emb.description or message.content or "") if emb else (message.content or ""),
        "image_url": (emb.image.url if (emb and emb.image) else ""),
        "footer_text": (emb.footer.text if (emb and emb.footer) else ""),
        "button_label": btn_label,
        "button_url": btn_url,
    }


class PostComposerModal(discord.ui.Modal):
    def __init__(
        self,
        style_key: str,
        target_channel: discord.TextChannel,
        ping_role: Optional[discord.Role] = None,
        edit_message: Optional[discord.Message] = None,
        defaults: Optional[Dict[str, str]] = None,
    ):
        modal_title = "Edit Bot Post" if edit_message else f"Create Post ({POST_STYLES[style_key]['name'][:22]})"
        super().__init__(title=modal_title[:45])
        self.style_key = style_key
        self.target_channel = target_channel
        self.ping_role = ping_role
        self.edit_message = edit_message
        d = defaults or {}

        self.title_input = discord.ui.TextInput(
            label="Post Title (Optional)",
            style=discord.TextStyle.short,
            required=False,
            max_length=150,
            default=d.get("title", ""),
            placeholder="e.g. Season 4 Update is Live!",
        )
        self.desc_input = discord.ui.TextInput(
            label="Message Content",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=3500,
            default=d.get("description", ""),
            placeholder="Write your announcement using Discord Markdown...",
        )
        self.image_input = discord.ui.TextInput(
            label="Banner Image URL (Optional)",
            style=discord.TextStyle.short,
            required=False,
            max_length=500,
            default=d.get("image_url", ""),
            placeholder="https://...",
        )
        self.footer_input = discord.ui.TextInput(
            label="Footer Text (Optional)",
            style=discord.TextStyle.short,
            required=False,
            max_length=100,
            default=d.get("footer_text", ""),
            placeholder="Leave empty for server name",
        )
        default_btn = (
            f"{d['button_label']} | {d['button_url']}"
            if d.get("button_label") and d.get("button_url")
            else ""
        )
        self.button_input = discord.ui.TextInput(
            label="Link Button: Label | URL",
            style=discord.TextStyle.short,
            required=False,
            max_length=300,
            default=default_btn,
            placeholder="Play Now | https://roblox.com/...",
        )

        self.add_item(self.title_input)
        self.add_item(self.desc_input)
        self.add_item(self.image_input)
        self.add_item(self.footer_input)
        self.add_item(self.button_input)

    async def on_submit(self, interaction: discord.Interaction):
        btn_label, btn_url = "", ""
        raw_btn = self.button_input.value.strip()
        if "|" in raw_btn:
            parts = raw_btn.split("|", 1)
            btn_label, btn_url = parts[0].strip(), parts[1].strip()

        embed = build_post_embed(
            guild=interaction.guild,
            style_key=self.style_key,
            title=self.title_input.value,
            description=self.desc_input.value,
            image_url=self.image_input.value,
            footer_text=self.footer_input.value,
        )
        view = build_link_view(btn_label, btn_url)

        if self.edit_message:
            try:
                await self.edit_message.edit(embed=embed, view=view)
                save_custom_post(
                    self.edit_message.id,
                    self.edit_message.channel.id,
                    interaction.guild.id,
                    self.style_key,
                    self.title_input.value,
                    self.desc_input.value,
                    self.image_input.value,
                    self.footer_input.value,
                    btn_label,
                    btn_url,
                )
                await interaction.response.send_message(
                    f"✅ Post updated! [Jump to Message]({self.edit_message.jump_url})",
                    ephemeral=True,
                )
            except Exception as e:
                await interaction.response.send_message(f"❌ Failed to edit post: `{e}`", ephemeral=True)
            return

        content = self.ping_role.mention if self.ping_role else None
        allowed = discord.AllowedMentions(roles=True) if self.ping_role else discord.AllowedMentions.none()
        try:
            sent = await self.target_channel.send(content=content, embed=embed, view=view, allowed_mentions=allowed)
            save_custom_post(
                sent.id,
                self.target_channel.id,
                interaction.guild.id,
                self.style_key,
                self.title_input.value,
                self.desc_input.value,
                self.image_input.value,
                self.footer_input.value,
                btn_label,
                btn_url,
            )
            await interaction.response.send_message(
                f"✅ Published in {self.target_channel.mention}! [Jump to Post]({sent.jump_url})",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ Could not send post: `{e}`", ephemeral=True)


class StyleSwitcherView(discord.ui.View):
    def __init__(self, target_message: discord.Message, post_data: Dict[str, Any]):
        super().__init__(timeout=120)
        self.target_message = target_message
        self.post_data = post_data

        options = [
            discord.SelectOption(
                label=info["name"],
                value=key,
                default=(key == post_data.get("style")),
            )
            for key, info in POST_STYLES.items()
        ]
        select = discord.ui.Select(placeholder="🎨 Choose a new visual preset...", options=options)
        select.callback = self.on_style_selected
        self.add_item(select)

    async def on_style_selected(self, interaction: discord.Interaction):
        new_style = interaction.data["values"][0]
        self.post_data["style"] = new_style
        embed = build_post_embed(
            guild=interaction.guild,
            style_key=new_style,
            title=self.post_data["title"],
            description=self.post_data["description"],
            image_url=self.post_data["image_url"],
            footer_text=self.post_data["footer_text"],
        )
        view = build_link_view(self.post_data["button_label"], self.post_data["button_url"])
        await self.target_message.edit(embed=embed, view=view)
        save_custom_post(
            self.target_message.id,
            self.target_message.channel.id,
            interaction.guild.id,
            new_style,
            self.post_data["title"],
            self.post_data["description"],
            self.post_data["image_url"],
            self.post_data["footer_text"],
            self.post_data["button_label"],
            self.post_data["button_url"],
        )
        await interaction.response.edit_message(
            content=f"✅ Style changed to **{POST_STYLES[new_style]['name']}**!",
            view=None,
        )

    @discord.ui.button(label="Pin / Unpin", style=discord.ButtonStyle.secondary, emoji="📌", row=1)
    async def pin_post_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if self.target_message.pinned:
                await self.target_message.unpin()
                await interaction.response.send_message("📌 Post unpinned.", ephemeral=True)
            else:
                await self.target_message.pin()
                await interaction.response.send_message("📌 Post pinned!", ephemeral=True)
        except discord.Forbidden:
            await interaction.response.send_message("❌ Missing `Manage Messages` permission to pin.", ephemeral=True)

    @discord.ui.button(label="Delete Post", style=discord.ButtonStyle.danger, emoji="🗑️", row=1)
    async def delete_post_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.target_message.delete()
        with get_db() as conn:
            conn.execute("DELETE FROM custom_posts WHERE message_id = ?", (self.target_message.id,))
            conn.commit()
        await interaction.response.edit_message(content="🗑️ Post deleted.", view=None)


# -------------------------------------------------------------
# 5. Live Interactive Polls System (/poll)
# -------------------------------------------------------------
def pack_options(options: List[str]) -> str:
    return "\n".join(opt.replace("\n", " ").strip() for opt in options if opt.strip())


def unpack_options(packed: str) -> List[str]:
    return [line.strip() for line in packed.split("\n") if line.strip()]


def build_poll_embed(
    guild: Optional[discord.Guild],
    question: str,
    description: str,
    options: List[str],
    vote_counts: List[int],
    allow_multiple: bool,
    ends_at: int,
    is_closed: bool,
    image_url: str = "",
) -> discord.Embed:
    total_votes = sum(vote_counts)
    max_votes = max(vote_counts) if vote_counts else 0

    lines: List[str] = []
    if description.strip():
        lines.append(f"*{description.strip()}*\n")

    for idx, opt_text in enumerate(options):
        count = vote_counts[idx] if idx < len(vote_counts) else 0
        pct = (count / total_votes * 100.0) if total_votes > 0 else 0.0
        filled = round((count / total_votes) * 12) if total_votes > 0 else 0
        bar = "█" * filled + "░" * (12 - filled)
        winner_badge = " 🏆" if (is_closed and max_votes > 0 and count == max_votes) else ""
        lines.append(
            f"{POLL_NUM_EMOJIS[idx]} **{opt_text}**{winner_badge}\n"
            f"`{bar}` **{pct:.1f}%** (`{count}`)"
        )

    meta_parts = [f"🗳️ **{total_votes}** vote{'s' if total_votes != 1 else ''}"]
    meta_parts.append("Multiple Choice" if allow_multiple else "Single Choice")
    if is_closed:
        meta_parts.append("🔒 **Closed**")
    elif ends_at > 0:
        meta_parts.append(f"⏳ Ends <t:{ends_at}:R>")

    lines.append(f"\n{' • '.join(meta_parts)}")

    embed = discord.Embed(
        title=f"📊 {question.strip()}",
        description="\n".join(lines),
        color=0x95A5A6 if is_closed else 0x5865F2,
        timestamp=datetime.now(timezone.utc),
    )
    if image_url and image_url.startswith(("http://", "https://")):
        embed.set_image(url=image_url)
    if guild:
        embed.set_footer(
            text=f"{guild.name} • Community Poll",
            icon_url=guild.icon.url if guild.icon else None,
        )
    return embed


class PollView(discord.ui.View):
    def __init__(self, option_count: int = 5, closed: bool = False):
        super().__init__(timeout=None)
        for idx in range(min(max(option_count, 2), 5)):
            btn = discord.ui.Button(
                style=discord.ButtonStyle.secondary,
                emoji=POLL_NUM_EMOJIS[idx],
                custom_id=f"poll_vote:{idx}",
                disabled=closed,
                row=0,
            )
            btn.callback = self._make_vote_callback(idx)
            self.add_item(btn)

        close_btn = discord.ui.Button(
            style=discord.ButtonStyle.danger,
            label="End",
            emoji="🔒",
            custom_id="poll_close",
            disabled=closed,
            row=1,
        )
        close_btn.callback = self._close_callback
        self.add_item(close_btn)

    def _make_vote_callback(self, option_idx: int):
        async def callback(interaction: discord.Interaction):
            await handle_poll_vote(interaction, option_idx)
        return callback

    async def _close_callback(self, interaction: discord.Interaction):
        await handle_poll_close(interaction)


def get_poll_state(message_id: int) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT channel_id, guild_id, creator_id, question, description,
                   options_packed, image_url, allow_multiple, ends_at, is_closed
            FROM polls WHERE message_id = ?
            """,
            (message_id,),
        )
        row = c.fetchone()
        if not row:
            return None
        options = unpack_options(row[5])
        c.execute(
            "SELECT option_index, COUNT(*) FROM poll_votes WHERE message_id = ? GROUP BY option_index",
            (message_id,),
        )
        counts_map = dict(c.fetchall())
        counts = [counts_map.get(i, 0) for i in range(len(options))]
        return {
            "message_id": message_id,
            "channel_id": row[0],
            "guild_id": row[1],
            "creator_id": row[2],
            "question": row[3],
            "description": row[4] or "",
            "options": options,
            "image_url": row[6] or "",
            "allow_multiple": bool(row[7]),
            "ends_at": row[8] or 0,
            "is_closed": bool(row[9]),
            "counts": counts,
        }


async def refresh_poll_message(guild: Optional[discord.Guild], message: discord.Message, state: Dict[str, Any]):
    embed = build_poll_embed(
        guild=guild,
        question=state["question"],
        description=state["description"],
        options=state["options"],
        vote_counts=state["counts"],
        allow_multiple=state["allow_multiple"],
        ends_at=state["ends_at"],
        is_closed=state["is_closed"],
        image_url=state["image_url"],
    )
    view = PollView(option_count=len(state["options"]), closed=state["is_closed"])
    await message.edit(embed=embed, view=view)


def schedule_debounced_poll_refresh(guild: Optional[discord.Guild], message: discord.Message):
    msg_id = message.id
    existing = POLL_UPDATE_TASKS.get(msg_id)
    if existing and not existing.done():
        return

    async def _worker():
        await asyncio.sleep(1.5)
        try:
            state = get_poll_state(msg_id)
            if state:
                await refresh_poll_message(guild, message, state)
        except Exception:
            pass
        finally:
            POLL_UPDATE_TASKS.pop(msg_id, None)

    POLL_UPDATE_TASKS[msg_id] = asyncio.create_task(_worker())


async def handle_poll_vote(interaction: discord.Interaction, option_idx: int):
    if not interaction.message:
        return
    msg_id = interaction.message.id
    user_id = interaction.user.id

    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT options_packed, allow_multiple, is_closed FROM polls WHERE message_id = ?", (msg_id,))
        row = c.fetchone()
        if not row:
            return await interaction.response.send_message("❌ Poll data not found.", ephemeral=True)
        options = unpack_options(row[0])
        allow_multiple = bool(row[1])
        is_closed = bool(row[2])

        if is_closed:
            return await interaction.response.send_message("🔒 This poll has already ended.", ephemeral=True)
        if option_idx >= len(options):
            return await interaction.response.send_message("❌ Invalid option.", ephemeral=True)

        c.execute(
            "SELECT 1 FROM poll_votes WHERE message_id = ? AND user_id = ? AND option_index = ?",
            (msg_id, user_id, option_idx),
        )
        already_voted_this = c.fetchone() is not None

        if allow_multiple:
            if already_voted_this:
                c.execute(
                    "DELETE FROM poll_votes WHERE message_id = ? AND user_id = ? AND option_index = ?",
                    (msg_id, user_id, option_idx),
                )
                feedback = f"➖ Removed vote for **{options[option_idx]}**."
            else:
                c.execute(
                    "INSERT INTO poll_votes (message_id, user_id, option_index) VALUES (?, ?, ?)",
                    (msg_id, user_id, option_idx),
                )
                feedback = f"✅ Voted for **{options[option_idx]}**!"
        else:
            c.execute("DELETE FROM poll_votes WHERE message_id = ? AND user_id = ?", (msg_id, user_id))
            if already_voted_this:
                feedback = f"➖ Removed your vote from **{options[option_idx]}**."
            else:
                c.execute(
                    "INSERT INTO poll_votes (message_id, user_id, option_index) VALUES (?, ?, ?)",
                    (msg_id, user_id, option_idx),
                )
                feedback = f"✅ Vote recorded: **{options[option_idx]}**!"
        conn.commit()

    await interaction.response.send_message(feedback, ephemeral=True)
    schedule_debounced_poll_refresh(interaction.guild, interaction.message)


async def handle_poll_close(interaction: discord.Interaction):
    if not interaction.message or not interaction.guild:
        return
    msg_id = interaction.message.id
    state = get_poll_state(msg_id)
    if not state:
        return await interaction.response.send_message("❌ Poll not found.", ephemeral=True)

    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    is_admin = bool(member and member.guild_permissions.manage_messages)
    if not is_admin and interaction.user.id != state["creator_id"]:
        return await interaction.response.send_message(
            "❌ Only the poll creator or moderators can close this poll.",
            ephemeral=True,
        )

    with get_db() as conn:
        conn.execute("UPDATE polls SET is_closed = 1 WHERE message_id = ?", (msg_id,))
        conn.commit()

    state["is_closed"] = True
    await interaction.response.defer(ephemeral=True)
    await refresh_poll_message(interaction.guild, interaction.message, state)
    await interaction.followup.send("🔒 Poll closed and final results locked!", ephemeral=True)


class PollCreatorModal(discord.ui.Modal, title="Create Community Poll"):
    def __init__(
        self,
        target_channel: discord.TextChannel,
        duration_minutes: int,
        allow_multiple: bool,
        image_url: str = "",
    ):
        super().__init__()
        self.target_channel = target_channel
        self.duration_minutes = duration_minutes
        self.allow_multiple = allow_multiple
        self.image_url = image_url

        self.question_input = discord.ui.TextInput(
            label="Poll Question",
            style=discord.TextStyle.short,
            required=True,
            max_length=200,
            placeholder="e.g. Which unit should get buffed next?",
        )
        self.options_input = discord.ui.TextInput(
            label="Options (1 per line, 2-5 total)",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=400,
            placeholder="Option 1\nOption 2\nOption 3",
        )
        self.desc_input = discord.ui.TextInput(
            label="Extra Details (Optional)",
            style=discord.TextStyle.paragraph,
            required=False,
            max_length=600,
            placeholder="Optional context shown above the choices...",
        )
        self.add_item(self.question_input)
        self.add_item(self.options_input)
        self.add_item(self.desc_input)

    async def on_submit(self, interaction: discord.Interaction):
        options = unpack_options(self.options_input.value)[:5]
        if len(options) < 2:
            return await interaction.response.send_message(
                "❌ Please enter at least **2 options** (each on a new line).",
                ephemeral=True,
            )

        ends_at = int(time.time()) + (self.duration_minutes * 60) if self.duration_minutes > 0 else 0
        counts = [0] * len(options)

        embed = build_poll_embed(
            guild=interaction.guild,
            question=self.question_input.value,
            description=self.desc_input.value,
            options=options,
            vote_counts=counts,
            allow_multiple=self.allow_multiple,
            ends_at=ends_at,
            is_closed=False,
            image_url=self.image_url,
        )
        view = PollView(option_count=len(options), closed=False)
        sent = await self.target_channel.send(embed=embed, view=view)

        with get_db() as conn:
            conn.execute(
                """
                INSERT INTO polls (
                    message_id, channel_id, guild_id, creator_id, question,
                    description, options_packed, image_url, allow_multiple, ends_at, is_closed
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    sent.id,
                    self.target_channel.id,
                    interaction.guild.id,
                    interaction.user.id,
                    self.question_input.value.strip(),
                    self.desc_input.value.strip(),
                    pack_options(options),
                    self.image_url,
                    1 if self.allow_multiple else 0,
                    ends_at,
                ),
            )
            conn.commit()

        await interaction.response.send_message(
            f"✅ Poll launched in {self.target_channel.mention}! [Jump to Poll]({sent.jump_url})",
            ephemeral=True,
        )


# -------------------------------------------------------------
# 6. Event Scheduling System (/event, /event_image, Follow)
# -------------------------------------------------------------
def parse_msk_datetime(raw: str) -> Optional[int]:
    raw = raw.strip()
    now_msk = datetime.now(MSK_TZ)
    for fmt in ("%d.%m.%Y %H:%M", "%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=MSK_TZ)
            return int(dt.timestamp())
        except ValueError:
            pass
    for fmt in ("%d.%m %H:%M", "%d/%m %H:%M"):
        try:
            parsed = datetime.strptime(raw, fmt)
            dt = parsed.replace(year=now_msk.year, tzinfo=MSK_TZ)
            return int(dt.timestamp())
        except ValueError:
            pass
    try:
        parsed = datetime.strptime(raw, "%H:%M")
        dt = now_msk.replace(hour=parsed.hour, minute=parsed.minute, second=0, microsecond=0)
        if dt <= now_msk:
            dt += timedelta(days=1)
        return int(dt.timestamp())
    except ValueError:
        return None


def build_event_embed(
    guild: Optional[discord.Guild],
    title: str,
    subtitle: str,
    description: str,
    start_ts: int,
    followers_count: int,
    image_url: str = "",
    started: bool = False,
) -> discord.Embed:
    header_parts = []
    if subtitle and subtitle.strip():
        header_parts.append(f"-# {subtitle.strip()}")
    if description and description.strip():
        header_parts.append(description.strip())

    status_line = (
        f"🔴 **EVENT IS LIVE NOW!** • Started <t:{start_ts}:R>"
        if started
        else f"🗓️ **Starts:** <t:{start_ts}:F> (<t:{start_ts}:R>)"
    )
    header_parts.append(f"\n{status_line}")

    embed = discord.Embed(
        title=f"📅 {title.strip()}",
        description="\n".join(header_parts),
        color=0x2ECC71 if started else 0xEB459E,
    )
    if image_url and image_url.startswith(("http://", "https://")):
        embed.set_image(url=image_url)
    embed.set_footer(
        text=f"🔔 {followers_count} Following • Local time shown automatically",
        icon_url=guild.icon.url if (guild and guild.icon) else None,
    )
    return embed


class EventFollowView(discord.ui.View):
    def __init__(self, count: int = 0, disabled: bool = False):
        super().__init__(timeout=None)
        btn = discord.ui.Button(
            style=discord.ButtonStyle.primary,
            label=f"Follow ({count})",
            emoji="🔔",
            custom_id="event_follow",
            disabled=disabled,
        )
        btn.callback = self.on_follow_click
        self.add_item(btn)

    async def on_follow_click(self, interaction: discord.Interaction):
        if not interaction.message or not interaction.guild or not isinstance(interaction.user, discord.Member):
            return
        msg_id = interaction.message.id
        with get_db() as conn:
            c = conn.cursor()
            c.execute(
                """
                SELECT role_id, title, subtitle, description, image_url, start_ts, remind_minutes, started
                FROM scheduled_events WHERE message_id = ?
                """,
                (msg_id,),
            )
            ev = c.fetchone()
            if not ev:
                return await interaction.response.send_message("❌ Event data not found.", ephemeral=True)

            role_id, title, subtitle, desc, img_url, start_ts, remind_min, started = ev
            c.execute(
                "SELECT 1 FROM event_followers WHERE message_id = ? AND user_id = ?",
                (msg_id, interaction.user.id),
            )
            is_following = c.fetchone() is not None

            role = interaction.guild.get_role(role_id) if role_id else None
            if is_following:
                c.execute(
                    "DELETE FROM event_followers WHERE message_id = ? AND user_id = ?",
                    (msg_id, interaction.user.id),
                )
                if role and role in interaction.user.roles:
                    try:
                        await interaction.user.remove_roles(role, reason="Unfollowed scheduled event")
                    except Exception:
                        pass
                reply_text = f"🔕 You unfollowed **{title}**."
            else:
                c.execute(
                    "INSERT INTO event_followers (message_id, user_id) VALUES (?, ?)",
                    (msg_id, interaction.user.id),
                )
                if role and role not in interaction.user.roles:
                    try:
                        await interaction.user.add_roles(role, reason="Followed scheduled event")
                    except Exception:
                        pass
                reply_text = (
                    f"🔔 You are now following **{title}**! "
                    f"You will be pinged **{remind_min} min** before start."
                )

            c.execute("SELECT COUNT(*) FROM event_followers WHERE message_id = ?", (msg_id,))
            new_count = c.fetchone()[0]
            conn.commit()

        embed = build_event_embed(
            guild=interaction.guild,
            title=title,
            subtitle=subtitle or "",
            description=desc or "",
            start_ts=start_ts,
            followers_count=new_count,
            image_url=img_url or "",
            started=bool(started),
        )
        view = EventFollowView(count=new_count, disabled=bool(started))
        await interaction.response.send_message(reply_text, ephemeral=True)
        try:
            await interaction.message.edit(embed=embed, view=view)
        except Exception:
            pass


class EventComposerModal(discord.ui.Modal):
    def __init__(
        self,
        target_channel: discord.TextChannel,
        role: discord.Role,
        remind_before: int,
        image_url: str = "",
        edit_message: Optional[discord.Message] = None,
        defaults: Optional[Dict[str, str]] = None,
    ):
        super().__init__(title="Edit Event" if edit_message else "Schedule Event")
        self.target_channel = target_channel
        self.role = role
        self.remind_before = remind_before
        self.image_url = image_url
        self.edit_message = edit_message
        d = defaults or {}

        default_dt = d.get(
            "start_str",
            (datetime.now(MSK_TZ) + timedelta(hours=1)).strftime("%d.%m.%Y %H:%M"),
        )
        self.title_input = discord.ui.TextInput(
            label="Event Title",
            style=discord.TextStyle.short,
            required=True,
            max_length=120,
            default=d.get("title", ""),
        )
        self.subtitle_input = discord.ui.TextInput(
            label="Subtitle (Optional)",
            style=discord.TextStyle.short,
            required=False,
            max_length=150,
            default=d.get("subtitle", ""),
        )
        self.time_input = discord.ui.TextInput(
            label="Start Time MSK (DD.MM.YYYY HH:MM)",
            style=discord.TextStyle.short,
            required=True,
            max_length=25,
            default=default_dt,
        )
        self.desc_input = discord.ui.TextInput(
            label="Event Description",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=2500,
            default=d.get("description", ""),
        )
        self.add_item(self.title_input)
        self.add_item(self.subtitle_input)
        self.add_item(self.time_input)
        self.add_item(self.desc_input)

    async def on_submit(self, interaction: discord.Interaction):
        start_ts = parse_msk_datetime(self.time_input.value)
        if not start_ts:
            return await interaction.response.send_message(
                "❌ Invalid date format. Use `DD.MM.YYYY HH:MM` (Moscow Time, e.g. `15.11.2026 19:30`).",
                ephemeral=True,
            )

        if self.edit_message:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM event_followers WHERE message_id = ?", (self.edit_message.id,))
                f_count = c.fetchone()[0]
                c.execute(
                    """
                    UPDATE scheduled_events
                    SET title = ?, subtitle = ?, description = ?, start_ts = ?, reminded = 0, started = 0
                    WHERE message_id = ?
                    """,
                    (
                        self.title_input.value.strip(),
                        self.subtitle_input.value.strip(),
                        self.desc_input.value.strip(),
                        start_ts,
                        self.edit_message.id,
                    ),
                )
                conn.commit()
            embed = build_event_embed(
                guild=interaction.guild,
                title=self.title_input.value,
                subtitle=self.subtitle_input.value,
                description=self.desc_input.value,
                start_ts=start_ts,
                followers_count=f_count,
                image_url=self.image_url,
                started=False,
            )
            await self.edit_message.edit(embed=embed, view=EventFollowView(count=f_count, disabled=False))
            return await interaction.response.send_message("✅ Event updated!", ephemeral=True)

        embed = build_event_embed(
            guild=interaction.guild,
            title=self.title_input.value,
            subtitle=self.subtitle_input.value,
            description=self.desc_input.value,
            start_ts=start_ts,
            followers_count=0,
            image_url=self.image_url,
            started=False,
        )
        sent = await self.target_channel.send(embed=embed, view=EventFollowView(count=0, disabled=False))
        with get_db() as conn:
            conn.execute(
                """
                INSERT INTO scheduled_events (
                    message_id, channel_id, guild_id, role_id, title,
                    subtitle, description, image_url, start_ts, remind_minutes, reminded, started
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0)
                """,
                (
                    sent.id,
                    self.target_channel.id,
                    interaction.guild.id,
                    self.role.id,
                    self.title_input.value.strip(),
                    self.subtitle_input.value.strip(),
                    self.desc_input.value.strip(),
                    self.image_url,
                    start_ts,
                    self.remind_before,
                ),
            )
            conn.commit()

        await interaction.response.send_message(
            f"✅ Event scheduled for <t:{start_ts}:F>! [Jump to Event]({sent.jump_url})",
            ephemeral=True,
        )


# -------------------------------------------------------------
# 7. Starboard / Ideas Showcase System (O(1) Deduplication)
# -------------------------------------------------------------
def build_showcase_embed(message: discord.Message, stars: int) -> discord.Embed:
    if stars >= 50:
        color, badge = 0x9B59B6, "🟣 LEGENDARY IDEA"
    elif stars >= 25:
        color, badge = 0xE67E22, "🔥 TRENDING HIT"
    else:
        color, badge = 0xF1C40F, "🌟 COMMUNITY CHOICE"

    content = message.content.strip()
    formatted = f">>> {content}" if content else "*[Media Submission]*"

    embed = discord.Embed(
        description=f"**{badge}**\n{formatted}",
        color=color,
        timestamp=message.created_at,
    )
    embed.set_author(
        name=message.author.display_name,
        url=message.jump_url,
        icon_url=message.author.display_avatar.url,
    )
    for att in message.attachments:
        if att.content_type and att.content_type.startswith("image/"):
            embed.set_image(url=att.url)
            break

    embed.set_footer(text=f"⭐ {stars} • #{message.channel.name}")
    return embed


def build_showcase_view(jump_url: str, stars: int) -> discord.ui.View:
    view = discord.ui.View()
    view.add_item(
        discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label=f"⭐ {stars}",
            disabled=True,
        )
    )
    view.add_item(
        discord.ui.Button(
            style=discord.ButtonStyle.link,
            label="Original Post",
            emoji="🔗",
            url=jump_url,
        )
    )
    return view


async def process_starboard_reaction(payload: discord.RawReactionActionEvent, is_add: bool):
    if not payload.guild_id or str(payload.emoji) != "⭐":
        return
    if bot.user and payload.user_id == bot.user.id:
        return

    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return
    settings = get_guild_settings(guild.id)
    ideas_ch_id = settings["ideas_channel_id"]
    showcase_ch_id = settings["showcase_channel_id"]
    threshold = settings["star_threshold"]

    if not ideas_ch_id or not showcase_ch_id:
        return
    if payload.channel_id not in (ideas_ch_id, showcase_ch_id):
        return

    lock = STARBOARD_LOCKS.setdefault(payload.guild_id, asyncio.Lock())
    async with lock:
        with get_db() as conn:
            c = conn.cursor()
            orig_id: Optional[int] = None
            showcase_msg_id: Optional[int] = None

            if payload.channel_id == ideas_ch_id:
                orig_id = payload.message_id
                c.execute(
                    "SELECT showcase_message_id FROM starboard_entries WHERE orig_message_id = ?",
                    (orig_id,),
                )
                row = c.fetchone()
                showcase_msg_id = row[0] if row else None

                if showcase_msg_id and is_add:
                    try:
                        ch = guild.get_channel(ideas_ch_id)
                        if isinstance(ch, discord.TextChannel):
                            orig_msg = await ch.fetch_message(orig_id)
                            member = guild.get_member(payload.user_id) or discord.Object(id=payload.user_id)
                            await orig_msg.remove_reaction("⭐", member)
                    except Exception:
                        pass
                    return
            else:
                c.execute(
                    "SELECT orig_message_id FROM starboard_entries WHERE showcase_message_id = ?",
                    (payload.message_id,),
                )
                row = c.fetchone()
                if not row:
                    return
                orig_id = row[0]
                showcase_msg_id = payload.message_id

            if is_add:
                c.execute(
                    "INSERT OR IGNORE INTO starboard_votes (orig_message_id, user_id) VALUES (?, ?)",
                    (orig_id, payload.user_id),
                )
            else:
                c.execute(
                    "DELETE FROM starboard_votes WHERE orig_message_id = ? AND user_id = ?",
                    (orig_id, payload.user_id),
                )

            c.execute("SELECT COUNT(*) FROM starboard_votes WHERE orig_message_id = ?", (orig_id,))
            stars_count = c.fetchone()[0]

            if showcase_msg_id:
                c.execute(
                    "UPDATE starboard_entries SET stars_count = ? WHERE orig_message_id = ?",
                    (stars_count, orig_id),
                )
            conn.commit()

        ideas_ch = guild.get_channel(ideas_ch_id)
        showcase_ch = guild.get_channel(showcase_ch_id)
        if not isinstance(ideas_ch, discord.TextChannel) or not isinstance(showcase_ch, discord.TextChannel):
            return

        try:
            orig_message = await ideas_ch.fetch_message(orig_id)
        except Exception:
            return

        embed = build_showcase_embed(orig_message, stars_count)
        view = build_showcase_view(orig_message.jump_url, stars_count)

        if showcase_msg_id:
            try:
                sc_msg = await showcase_ch.fetch_message(showcase_msg_id)
                await sc_msg.edit(embed=embed, view=view)
            except discord.NotFound:
                showcase_msg_id = None

        if not showcase_msg_id and stars_count >= threshold:
            sent = await showcase_ch.send(embed=embed, view=view)
            try:
                await sent.add_reaction("⭐")
            except Exception:
                pass
            with get_db() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO starboard_entries
                    (orig_message_id, showcase_message_id, guild_id, channel_id, stars_count)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (orig_id, sent.id, guild.id, ideas_ch_id, stars_count),
                )
                conn.commit()


# -------------------------------------------------------------
# 8. Admin Control Panel (/panel)
# -------------------------------------------------------------
class ThresholdModal(discord.ui.Modal, title="Set Starboard Threshold"):
    threshold_input = discord.ui.TextInput(
        label="Stars Required (1 - 500)",
        style=discord.TextStyle.short,
        required=True,
        max_length=3,
        placeholder="e.g. 12",
    )

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild:
            return
        val = self.threshold_input.value.strip()
        if not val.isdigit() or not (1 <= int(val) <= 500):
            return await interaction.response.send_message("❌ Enter a number between 1 and 500.", ephemeral=True)
        update_guild_settings(interaction.guild.id, star_threshold=int(val))
        await interaction.response.edit_message(embed=build_panel_embed(interaction.guild), view=ControlPanelView())


def build_panel_embed(guild: discord.Guild) -> discord.Embed:
    s = get_guild_settings(guild.id)
    ideas = f"<#{s['ideas_channel_id']}>" if s["ideas_channel_id"] else "`Not Set`"
    showcase = f"<#{s['showcase_channel_id']}>" if s["showcase_channel_id"] else "`Not Set`"
    wfl = f"<#{s['wfl_channel_id']}>" if s["wfl_channel_id"] else "`Not Set`"
    booster = f"<@&{s['booster_role_id']}>" if s["booster_role_id"] else "`Not Set`"
    autorole = f"<@&{s['autorole_id']}>" if s["autorole_id"] else "`Not Set`"

    detected_wfl = get_wfl_emojis(guild)
    wfl_status = " ".join(str(e) for e in detected_wfl) if detected_wfl else "`Missing (w1_win, w1_fair, w1_lose)`"

    embed = discord.Embed(
        title="⚙️ Server Control Panel",
        description="Configure Starboard, W/F/L auto-reactions, Booster Guard, and Auto-Role below.",
        color=0x5865F2,
    )
    embed.add_field(name="💡 Ideas Channel", value=ideas, inline=True)
    embed.add_field(name="🌟 Showcase Channel", value=showcase, inline=True)
    embed.add_field(name="⭐ Star Threshold", value=f"**{s['star_threshold']}** stars", inline=True)
    embed.add_field(name="⚖️ W/F/L Channel", value=wfl, inline=True)
    embed.add_field(name="🎯 W/F/L Emojis", value=wfl_status, inline=True)
    embed.add_field(name="🚀 Booster Role", value=booster, inline=True)
    embed.add_field(name="👋 Join Auto-Role", value=autorole, inline=True)
    embed.set_footer(text=f"{guild.name} • {guild.member_count:,} members")
    return embed


class ControlPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="💡 Select Ideas Channel...",
        row=0,
    )
    async def select_ideas(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        update_guild_settings(interaction.guild.id, ideas_channel_id=select.values[0].id)
        await interaction.response.edit_message(embed=build_panel_embed(interaction.guild), view=self)

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="🌟 Select Showcase Channel...",
        row=1,
    )
    async def select_showcase(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        update_guild_settings(interaction.guild.id, showcase_channel_id=select.values[0].id)
        await interaction.response.edit_message(embed=build_panel_embed(interaction.guild), view=self)

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="⚖️ Select W/F/L Auto-Reaction Channel...",
        row=2,
    )
    async def select_wfl(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        WFL_EMOJI_CACHE.pop(interaction.guild.id, None)
        update_guild_settings(interaction.guild.id, wfl_channel_id=select.values[0].id)
        await interaction.response.edit_message(embed=build_panel_embed(interaction.guild), view=self)

    @discord.ui.select(
        cls=discord.ui.RoleSelect,
        placeholder="👋 Select Join Auto-Role (Optional)...",
        row=3,
    )
    async def select_autorole(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        update_guild_settings(interaction.guild.id, autorole_id=select.values[0].id)
        await interaction.response.edit_message(embed=build_panel_embed(interaction.guild), view=self)

    @discord.ui.button(label="Set Star Threshold", style=discord.ButtonStyle.secondary, emoji="⭐", row=4)
    async def set_threshold_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ThresholdModal())

    @discord.ui.button(label="Create Post", style=discord.ButtonStyle.primary, emoji="📢", row=4)
    async def quick_post_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_modal(
                PostComposerModal(style_key="announcement", target_channel=interaction.channel)
            )

    @discord.ui.button(label="Clear W/F/L & Auto-Role", style=discord.ButtonStyle.danger, emoji="🧹", row=4)
    async def clear_optional_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        update_guild_settings(interaction.guild.id, wfl_channel_id=None, autorole_id=None)
        await interaction.response.edit_message(embed=build_panel_embed(interaction.guild), view=self)


# -------------------------------------------------------------
# 9. Slash Commands & Context Menus
# -------------------------------------------------------------
@bot.tree.command(name="panel", description="Open the server configuration panel")
@app_commands.default_permissions(administrator=True)
async def panel_cmd(interaction: discord.Interaction):
    if not interaction.guild:
        return
    await interaction.response.send_message(
        embed=build_panel_embed(interaction.guild),
        view=ControlPanelView(),
        ephemeral=True,
    )


@bot.tree.command(name="booster_role", description="Set the exclusive role guarded for active Server Boosters")
@app_commands.describe(role="Role to remove automatically when a member stops boosting")
@app_commands.default_permissions(administrator=True)
async def booster_role_cmd(interaction: discord.Interaction, role: Optional[discord.Role] = None):
    if not interaction.guild:
        return
    update_guild_settings(interaction.guild.id, booster_role_id=role.id if role else None)
    msg = f"🚀 Booster Guard set to {role.mention}." if role else "🧹 Booster Guard disabled."
    await interaction.response.send_message(msg, ephemeral=True)


@bot.tree.command(name="post", description="Publish a styled embed post from the bot")
@app_commands.describe(
    style="Choose a visual style preset",
    channel="Target channel (defaults to current channel)",
    ping_role="Optional role to mention",
    image="Optional image from your gallery",
)
@app_commands.choices(
    style=[
        app_commands.Choice(name="📢 Announcement", value="announcement"),
        app_commands.Choice(name="🚀 Update / Patch Notes", value="update"),
        app_commands.Choice(name="🎉 Event / Giveaway", value="event"),
        app_commands.Choice(name="📘 Guide / Info", value="guide"),
        app_commands.Choice(name="⚠️ Urgent Alert", value="alert"),
        app_commands.Choice(name="🖤 Minimalist Dark", value="minimal"),
    ]
)
@app_commands.default_permissions(manage_messages=True)
async def post_cmd(
    interaction: discord.Interaction,
    style: str = "announcement",
    channel: Optional[discord.TextChannel] = None,
    ping_role: Optional[discord.Role] = None,
    image: Optional[discord.Attachment] = None,
):
    target = channel or interaction.channel
    if not isinstance(target, discord.TextChannel):
        return await interaction.response.send_message("❌ Please select a valid text channel.", ephemeral=True)
    defaults = {"image_url": image.url} if image else {}
    await interaction.response.send_modal(
        PostComposerModal(style_key=style, target_channel=target, ping_role=ping_role, defaults=defaults)
    )


@bot.tree.command(name="edit", description="Edit an existing bot post by Message ID or Link")
@app_commands.describe(message="Message ID or full Discord message link")
@app_commands.default_permissions(manage_messages=True)
async def edit_cmd(interaction: discord.Interaction, message: str):
    if not interaction.guild or not interaction.channel:
        return
    ch_id, msg_id = parse_message_reference(message, interaction.channel.id)
    if not ch_id or not msg_id:
        return await interaction.response.send_message("❌ Invalid Message ID or Link.", ephemeral=True)

    target_ch = interaction.guild.get_channel(ch_id)
    if not isinstance(target_ch, discord.TextChannel):
        return await interaction.response.send_message("❌ Channel not found.", ephemeral=True)

    try:
        target_msg = await target_ch.fetch_message(msg_id)
    except Exception:
        return await interaction.response.send_message("❌ Message not found in that channel.", ephemeral=True)

    if not bot.user or target_msg.author.id != bot.user.id:
        return await interaction.response.send_message("❌ I can only edit my own messages.", ephemeral=True)

    data = load_post_data_from_message(target_msg)
    await interaction.response.send_modal(
        PostComposerModal(
            style_key=data["style"],
            target_channel=target_ch,
            edit_message=target_msg,
            defaults=data,
        )
    )


@bot.tree.command(name="post_image", description="Replace the banner image on a bot post using an uploaded file")
@app_commands.describe(message="Message ID or full Discord message link", image="New image from your gallery")
@app_commands.default_permissions(manage_messages=True)
async def post_image_cmd(interaction: discord.Interaction, message: str, image: discord.Attachment):
    if not interaction.guild or not interaction.channel:
        return
    ch_id, msg_id = parse_message_reference(message, interaction.channel.id)
    if not ch_id or not msg_id:
        return await interaction.response.send_message("❌ Invalid Message ID or Link.", ephemeral=True)

    target_ch = interaction.guild.get_channel(ch_id)
    if not isinstance(target_ch, discord.TextChannel):
        return await interaction.response.send_message("❌ Channel not found.", ephemeral=True)

    try:
        target_msg = await target_ch.fetch_message(msg_id)
    except Exception:
        return await interaction.response.send_message("❌ Message not found.", ephemeral=True)

    if not bot.user or target_msg.author.id != bot.user.id:
        return await interaction.response.send_message("❌ I can only edit my own messages.", ephemeral=True)

    data = load_post_data_from_message(target_msg)
    data["image_url"] = image.url
    embed = build_post_embed(
        guild=interaction.guild,
        style_key=data["style"],
        title=data["title"],
        description=data["description"],
        image_url=image.url,
        footer_text=data["footer_text"],
    )
    view = build_link_view(data["button_label"], data["button_url"])
    await target_msg.edit(embed=embed, view=view)
    save_custom_post(
        target_msg.id,
        target_ch.id,
        interaction.guild.id,
        data["style"],
        data["title"],
        data["description"],
        image.url,
        data["footer_text"],
        data["button_label"],
        data["button_url"],
    )
    await interaction.response.send_message(f"✅ Post image updated! [Jump to Post]({target_msg.jump_url})", ephemeral=True)


@bot.tree.command(name="poll", description="Create an interactive community poll with live progress bars")
@app_commands.describe(
    channel="Channel to post the poll in (defaults to current)",
    duration_minutes="Auto-close after N minutes (0 = manual close only)",
    allow_multiple="Allow members to vote for multiple options",
    image="Optional banner image for the poll",
)
@app_commands.default_permissions(manage_messages=True)
async def poll_cmd(
    interaction: discord.Interaction,
    channel: Optional[discord.TextChannel] = None,
    duration_minutes: app_commands.Range[int, 0, 43200] = 0,
    allow_multiple: bool = False,
    image: Optional[discord.Attachment] = None,
):
    target = channel or interaction.channel
    if not isinstance(target, discord.TextChannel):
        return await interaction.response.send_message("❌ Please select a valid text channel.", ephemeral=True)
    await interaction.response.send_modal(
        PollCreatorModal(
            target_channel=target,
            duration_minutes=duration_minutes,
            allow_multiple=allow_multiple,
            image_url=image.url if image else "",
        )
    )


@bot.tree.command(name="event", description="Schedule an event with Moscow time input & local time display")
@app_commands.describe(
    channel="Channel where the event card will be published",
    role="Role assigned on Follow & pinged before start",
    image="Optional event banner image from gallery",
    remind_before="Minutes before start to ping the role (default: 10)",
)
@app_commands.default_permissions(manage_events=True)
async def event_cmd(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    role: discord.Role,
    image: Optional[discord.Attachment] = None,
    remind_before: app_commands.Range[int, 1, 120] = 10,
):
    await interaction.response.send_modal(
        EventComposerModal(
            target_channel=channel,
            role=role,
            remind_before=remind_before,
            image_url=image.url if image else "",
        )
    )


@bot.tree.command(name="event_image", description="Update the banner image of an existing event")
@app_commands.describe(message="Event Message ID or Link", image="New event image from gallery")
@app_commands.default_permissions(manage_events=True)
async def event_image_cmd(interaction: discord.Interaction, message: str, image: discord.Attachment):
    if not interaction.guild or not interaction.channel:
        return
    ch_id, msg_id = parse_message_reference(message, interaction.channel.id)
    if not ch_id or not msg_id:
        return await interaction.response.send_message("❌ Invalid Message ID or Link.", ephemeral=True)

    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT title, subtitle, description, start_ts, started FROM scheduled_events WHERE message_id = ?",
            (msg_id,),
        )
        ev = c.fetchone()
        if not ev:
            return await interaction.response.send_message("❌ Event not found in database.", ephemeral=True)
        c.execute("SELECT COUNT(*) FROM event_followers WHERE message_id = ?", (msg_id,))
        f_count = c.fetchone()[0]
        c.execute("UPDATE scheduled_events SET image_url = ? WHERE message_id = ?", (image.url, msg_id))
        conn.commit()

    ch = interaction.guild.get_channel(ch_id)
    if isinstance(ch, discord.TextChannel):
        msg_obj = await ch.fetch_message(msg_id)
        embed = build_event_embed(
            guild=interaction.guild,
            title=ev[0],
            subtitle=ev[1] or "",
            description=ev[2] or "",
            start_ts=ev[3],
            followers_count=f_count,
            image_url=image.url,
            started=bool(ev[4]),
        )
        await msg_obj.edit(embed=embed)
    await interaction.response.send_message("✅ Event banner updated!", ephemeral=True)


@bot.tree.command(name="stats", description="View bot performance, cache health, and server metrics")
async def stats_cmd(interaction: discord.Interaction):
    uptime_sec = int(time.time() - START_TIME)
    hours, rem = divmod(uptime_sec, 3600)
    minutes, seconds = divmod(rem, 60)

    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM starboard_entries")
        total_ideas = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM polls WHERE is_closed = 0")
        active_polls = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM scheduled_events WHERE started = 0")
        active_events = c.fetchone()[0]

    embed = discord.Embed(title="📈 Bot Performance & Metrics", color=0x2ECC71)
    embed.add_field(name="🏓 Latency", value=f"`{round(bot.latency * 1000)} ms`", inline=True)
    embed.add_field(name="⏱️ Uptime", value=f"`{hours}h {minutes}m {seconds}s`", inline=True)
    embed.add_field(name="🌟 Showcased Ideas", value=f"`{total_ideas}`", inline=True)
    embed.add_field(name="📊 Active Polls", value=f"`{active_polls}`", inline=True)
    embed.add_field(name="📅 Upcoming Events", value=f"`{active_events}`", inline=True)
    embed.add_field(name="⚡ Cache", value="`WAL + RAM Active`", inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)


# --- Right-Click Context Menus ---
@bot.tree.context_menu(name="✏️ Edit Bot Post")
@app_commands.default_permissions(manage_messages=True)
async def ctx_edit_post(interaction: discord.Interaction, message: discord.Message):
    if not bot.user or message.author.id != bot.user.id:
        return await interaction.response.send_message("❌ Select a message sent by this bot.", ephemeral=True)
    if not isinstance(message.channel, discord.TextChannel):
        return
    data = load_post_data_from_message(message)
    await interaction.response.send_modal(
        PostComposerModal(
            style_key=data["style"],
            target_channel=message.channel,
            edit_message=message,
            defaults=data,
        )
    )


@bot.tree.context_menu(name="🎨 Change Post Style")
@app_commands.default_permissions(manage_messages=True)
async def ctx_style_post(interaction: discord.Interaction, message: discord.Message):
    if not bot.user or message.author.id != bot.user.id:
        return await interaction.response.send_message("❌ Select a message sent by this bot.", ephemeral=True)
    data = load_post_data_from_message(message)
    await interaction.response.send_message(
        "🎨 Choose a new style, pin, or delete this post:",
        view=StyleSwitcherView(message, data),
        ephemeral=True,
    )


@bot.tree.context_menu(name="📅 Edit Event")
@app_commands.default_permissions(manage_events=True)
async def ctx_edit_event(interaction: discord.Interaction, message: discord.Message):
    if not interaction.guild or not isinstance(message.channel, discord.TextChannel):
        return
    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT role_id, title, subtitle, description, image_url, start_ts, remind_minutes FROM scheduled_events WHERE message_id = ?",
            (message.id,),
        )
        ev = c.fetchone()
    if not ev:
        return await interaction.response.send_message("❌ This message is not a scheduled event.", ephemeral=True)

    role = interaction.guild.get_role(ev[0]) or interaction.guild.default_role
    start_str = datetime.fromtimestamp(ev[5], tz=MSK_TZ).strftime("%d.%m.%Y %H:%M")
    defaults = {
        "title": ev[1] or "",
        "subtitle": ev[2] or "",
        "description": ev[3] or "",
        "start_str": start_str,
    }
    await interaction.response.send_modal(
        EventComposerModal(
            target_channel=message.channel,
            role=role,
            remind_before=ev[6] or 10,
            image_url=ev[4] or "",
            edit_message=message,
            defaults=defaults,
        )
    )


# -------------------------------------------------------------
# 10. Event Listeners & Background Scheduler
# -------------------------------------------------------------
@bot.event
async def on_ready():
    print(f"[Online] Logged in as {bot.user} (ID: {bot.user.id}) | Guilds: {len(bot.guilds)}")


@bot.event
async def on_guild_emojis_update(guild: discord.Guild, before, after):
    WFL_EMOJI_CACHE.pop(guild.id, None)


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    if message.guild:
        settings = get_guild_settings(message.guild.id)

        if settings["ideas_channel_id"] and message.channel.id == settings["ideas_channel_id"]:
            try:
                await message.add_reaction("⭐")
            except Exception:
                pass

        if settings["wfl_channel_id"] and message.channel.id == settings["wfl_channel_id"]:
            for emoji in get_wfl_emojis(message.guild):
                try:
                    await message.add_reaction(emoji)
                except Exception:
                    pass

    await bot.process_commands(message)


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    await process_starboard_reaction(payload, is_add=True)


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    await process_starboard_reaction(payload, is_add=False)


@bot.event
async def on_member_join(member: discord.Member):
    if member.bot:
        return
    settings = get_guild_settings(member.guild.id)
    autorole_id = settings["autorole_id"]
    if autorole_id:
        role = member.guild.get_role(autorole_id)
        if role and member.guild.me.top_role > role:
            try:
                await member.add_roles(role, reason="Join Auto-Role")
            except Exception:
                pass


@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    if before.premium_since is not None and after.premium_since is None:
        settings = get_guild_settings(after.guild.id)
        booster_role_id = settings["booster_role_id"]
        if booster_role_id:
            role = after.guild.get_role(booster_role_id)
            if role and role in after.roles:
                try:
                    await after.remove_roles(role, reason="Server Boost expired")
                except Exception:
                    pass


@tasks.loop(seconds=20)
async def background_scheduler():
    now_ts = int(time.time())

    # 1. Check expired polls
    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT message_id, channel_id, guild_id FROM polls WHERE is_closed = 0 AND ends_at > 0 AND ends_at <= ?",
            (now_ts,),
        )
        expired_polls = c.fetchall()
        for msg_id, ch_id, guild_id in expired_polls:
            c.execute("UPDATE polls SET is_closed = 1 WHERE message_id = ?", (msg_id,))
        conn.commit()

    for msg_id, ch_id, guild_id in expired_polls:
        guild = bot.get_guild(guild_id)
        ch = guild.get_channel(ch_id) if guild else None
        if isinstance(ch, discord.TextChannel):
            try:
                msg = await ch.fetch_message(msg_id)
                state = get_poll_state(msg_id)
                if state:
                    await refresh_poll_message(guild, msg, state)
            except Exception:
                pass

    # 2. Check scheduled events reminders & start
    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT message_id, channel_id, guild_id, role_id, title, subtitle,
                   description, image_url, start_ts, remind_minutes, reminded, started
            FROM scheduled_events WHERE started = 0
            """
        )
        events = c.fetchall()

        for ev in events:
            msg_id, ch_id, guild_id, role_id, title, subtitle, desc, img_url, start_ts, rem_min, reminded, started = ev
            guild = bot.get_guild(guild_id)
            ch = guild.get_channel(ch_id) if guild else None

            if not reminded and now_ts >= (start_ts - rem_min * 60) and now_ts < start_ts:
                c.execute("UPDATE scheduled_events SET reminded = 1 WHERE message_id = ?", (msg_id,))
                conn.commit()
                if isinstance(ch, discord.TextChannel):
                    role_mention = f"<@&{role_id}>" if role_id else ""
                    try:
                        await ch.send(
                            f"⏰ {role_mention} **{title}** starts <t:{start_ts}:R>!",
                            allowed_mentions=discord.AllowedMentions(roles=True),
                        )
                    except Exception:
                        pass

            if now_ts >= start_ts:
                c.execute("UPDATE scheduled_events SET started = 1, reminded = 1 WHERE message_id = ?", (msg_id,))
                c.execute("SELECT COUNT(*) FROM event_followers WHERE message_id = ?", (msg_id,))
                f_count = c.fetchone()[0]
                conn.commit()
                if isinstance(ch, discord.TextChannel):
                    try:
                        msg_obj = await ch.fetch_message(msg_id)
                        embed = build_event_embed(
                            guild=guild,
                            title=title,
                            subtitle=subtitle or "",
                            description=desc or "",
                            start_ts=start_ts,
                            followers_count=f_count,
                            image_url=img_url or "",
                            started=True,
                        )
                        await msg_obj.edit(embed=embed, view=EventFollowView(count=f_count, disabled=True))
                    except Exception:
                        pass


@background_scheduler.before_loop
async def before_scheduler():
    await bot.wait_until_ready()


@bot.command(name="sync")
@commands.has_permissions(administrator=True)
async def sync_commands(ctx: commands.Context):
    msg = await ctx.send("⏳ Cleaning duplicate guild overrides & syncing slash commands...")
    if ctx.guild:
        bot.tree.clear_commands(guild=ctx.guild)
        await bot.tree.sync(guild=ctx.guild)
    synced = await bot.tree.sync()
    await msg.edit(content=f"✅ Successfully synced **{len(synced)}** global commands (duplicates cleared)!")


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    text = f"⚠️ Error: `{error}`"
    if interaction.response.is_done():
        await interaction.followup.send(text, ephemeral=True)
    else:
        await interaction.response.send_message(text, ephemeral=True)


# -------------------------------------------------------------
# 11. Entrypoint
# -------------------------------------------------------------
if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN") or os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("Missing DISCORD_TOKEN environment variable!")
    bot.run(token)
