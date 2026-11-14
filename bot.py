import os
import re
import sys
import html as html_lib
import time
import asyncio
import sqlite3
import subprocess
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple, Set

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
    import aiohttp
except ImportError:
    aiohttp = None

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
WFL_EMOJI_CACHE: Dict[int, List[Any]] = {}
APP_EMOJIS_CACHE: List[Any] = []
STARBOARD_LOCKS: Dict[int, asyncio.Lock] = {}
POLL_UPDATE_TASKS: Dict[int, asyncio.Task] = {}
VALUE_VOTE_UPDATE_TASKS: Dict[int, asyncio.Task] = {}
LOCKDOWN_IN_PROGRESS: Set[int] = set()

# Value List RAM Cache (O(1) lookup for 80k+ members)
VALUE_ITEMS_CACHE: Dict[str, Dict[str, Any]] = {}
VALUE_ALIAS_MAP: Dict[str, str] = {}
LAST_VALUE_WEB_SYNC: float = 0.0
DEFAULT_VALUE_LIST_URL = "https://cameraman-td.vaultedvaluesx.com/value-list"

# Temporary Voice RAM State
TEMP_VOICE_CACHE: Dict[int, Dict[str, Any]] = {}
VOICE_CREATE_COOLDOWNS: Dict[int, float] = {}
VOICE_WARNING_SECONDS = 240     # 4 minutes: send Keep Alive warning
VOICE_INACTIVITY_SECONDS = 300  # 5 minutes: delete inactive room

WFL_EMOJI_NAMES = ("w1_win", "w1_fair", "w1_lose")
POLL_NUM_EMOJIS = ("1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣")
YOUTUBE_ACTIVITY_ID = 880218394199220334  # Discord Watch Together Activity


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
                wfl_channel_id INTEGER,
                jtc_channel_id INTEGER,
                antiscam_enabled INTEGER DEFAULT 0,
                antiscam_log_channel_id INTEGER,
                antiscam_timeout_mins INTEGER DEFAULT 60,
                value_channel_id INTEGER,
                value_ping_role_id INTEGER,
                value_list_url TEXT
            )
            """
        )
        c.execute("PRAGMA table_info(guild_settings)")
        cols = {row[1] for row in c.fetchall()}
        extra_cols = {
            "booster_role_id": "INTEGER",
            "autorole_id": "INTEGER",
            "wfl_channel_id": "INTEGER",
            "jtc_channel_id": "INTEGER",
            "antiscam_enabled": "INTEGER DEFAULT 0",
            "antiscam_log_channel_id": "INTEGER",
            "antiscam_timeout_mins": "INTEGER DEFAULT 60",
            "value_channel_id": "INTEGER",
            "value_ping_role_id": "INTEGER",
            "value_list_url": "TEXT",
        }
        for col_name, col_type in extra_cols.items():
            if col_name not in cols:
                c.execute(f"ALTER TABLE guild_settings ADD COLUMN {col_name} {col_type}")

        # Value List Items & Changelog Votes
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS value_items (
                item_key TEXT PRIMARY KEY,
                name TEXT,
                aliases TEXT,
                category TEXT,
                value_text TEXT,
                prev_value_text TEXT,
                demand_text TEXT,
                stability_text TEXT,
                sign_value_text TEXT,
                image_url TEXT,
                updated_at INTEGER
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS value_post_votes (
                message_id INTEGER,
                user_id INTEGER,
                vote INTEGER,
                PRIMARY KEY (message_id, user_id)
            )
            """
        )

        # Temporary Voice Rooms & Personal Presets (Voice 2.0)
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS temp_voices (
                channel_id INTEGER PRIMARY KEY,
                guild_id INTEGER,
                owner_id INTEGER,
                control_message_id INTEGER DEFAULT 0,
                created_at INTEGER,
                last_active_at INTEGER,
                last_renamed_at INTEGER DEFAULT 0,
                is_locked INTEGER DEFAULT 0,
                is_hidden INTEGER DEFAULT 0,
                warned_inactive INTEGER DEFAULT 0
            )
            """
        )
        c.execute("PRAGMA table_info(temp_voices)")
        tv_cols = {row[1] for row in c.fetchall()}
        if "warned_inactive" not in tv_cols:
            c.execute("ALTER TABLE temp_voices ADD COLUMN warned_inactive INTEGER DEFAULT 0")

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS user_voice_presets (
                user_id INTEGER PRIMARY KEY,
                custom_name TEXT,
                user_limit INTEGER DEFAULT 0,
                default_locked INTEGER DEFAULT 0,
                default_hidden INTEGER DEFAULT 0
            )
            """
        )

        # Lockdown Snapshots & Whitelist
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS lockdown_snapshots (
                guild_id INTEGER,
                channel_id INTEGER,
                prev_send_messages INTEGER,
                prev_send_in_threads INTEGER,
                prev_create_threads INTEGER,
                PRIMARY KEY (guild_id, channel_id)
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS lockdown_whitelist (
                guild_id INTEGER,
                target_id INTEGER,
                target_type TEXT,
                PRIMARY KEY (guild_id, target_id)
            )
            """
        )

        # Starboard / Showcase
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

        # Custom Posts
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

        # Scheduled Events
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
        c.execute("PRAGMA table_info(scheduled_events)")
        ev_cols = {row[1] for row in c.fetchall()}
        ev_migrations = {
            "channel_id": "INTEGER",
            "guild_id": "INTEGER",
            "role_id": "INTEGER",
            "title": "TEXT",
            "subtitle": "TEXT",
            "description": "TEXT",
            "image_url": "TEXT",
            "start_ts": "INTEGER DEFAULT 0",
            "remind_minutes": "INTEGER DEFAULT 10",
            "reminded": "INTEGER DEFAULT 0",
            "started": "INTEGER DEFAULT 0",
        }
        for col_name, col_def in ev_migrations.items():
            if col_name not in ev_cols:
                c.execute(f"ALTER TABLE scheduled_events ADD COLUMN {col_name} {col_def}")
        for legacy_ts_col in ("starts_at", "start_time", "event_time", "timestamp"):
            if legacy_ts_col in ev_cols:
                c.execute(
                    f"UPDATE scheduled_events SET start_ts = COALESCE({legacy_ts_col}, 0) "
                    "WHERE start_ts IS NULL OR start_ts = 0"
                )
                break
        if "remind_before" in ev_cols:
            c.execute(
                "UPDATE scheduled_events SET remind_minutes = COALESCE(remind_before, 10) "
                "WHERE remind_minutes IS NULL"
            )
        if "is_started" in ev_cols:
            c.execute(
                "UPDATE scheduled_events SET started = COALESCE(is_started, 0) "
                "WHERE started IS NULL"
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

        # Polls
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
                   booster_role_id, autorole_id, wfl_channel_id, jtc_channel_id,
                   antiscam_enabled, antiscam_log_channel_id, antiscam_timeout_mins,
                   value_channel_id, value_ping_role_id, value_list_url
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
            "jtc_channel_id": row[6] if row else None,
            "antiscam_enabled": bool(row[7]) if row else False,
            "antiscam_log_channel_id": row[8] if row else None,
            "antiscam_timeout_mins": row[9] if (row and row[9] is not None) else 60,
            "value_channel_id": row[10] if row else None,
            "value_ping_role_id": row[11] if row else None,
            "value_list_url": (row[12] if (row and row[12]) else DEFAULT_VALUE_LIST_URL),
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
                star_threshold, booster_role_id, autorole_id, wfl_channel_id, jtc_channel_id,
                antiscam_enabled, antiscam_log_channel_id, antiscam_timeout_mins,
                value_channel_id, value_ping_role_id, value_list_url
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
                ideas_channel_id = excluded.ideas_channel_id,
                showcase_channel_id = excluded.showcase_channel_id,
                star_threshold = excluded.star_threshold,
                booster_role_id = excluded.booster_role_id,
                autorole_id = excluded.autorole_id,
                wfl_channel_id = excluded.wfl_channel_id,
                jtc_channel_id = excluded.jtc_channel_id,
                antiscam_enabled = excluded.antiscam_enabled,
                antiscam_log_channel_id = excluded.antiscam_log_channel_id,
                antiscam_timeout_mins = excluded.antiscam_timeout_mins,
                value_channel_id = excluded.value_channel_id,
                value_ping_role_id = excluded.value_ping_role_id,
                value_list_url = excluded.value_list_url
            """,
            (
                guild_id,
                current["ideas_channel_id"],
                current["showcase_channel_id"],
                current["star_threshold"],
                current["booster_role_id"],
                current["autorole_id"],
                current["wfl_channel_id"],
                current["jtc_channel_id"],
                1 if current["antiscam_enabled"] else 0,
                current["antiscam_log_channel_id"],
                current["antiscam_timeout_mins"],
                current["value_channel_id"],
                current["value_ping_role_id"],
                current["value_list_url"],
            ),
        )
        conn.commit()


def parse_message_reference(raw: str, default_channel_id: int) -> Tuple[Optional[int], Optional[int]]:
    raw = raw.strip()
    link_match = re.search(r"channels/(?:\d+|@me)/(\d+)/(\d+)", raw)
    if link_match:
        return int(link_match.group(1)), int(link_match.group(2))
    if raw.isdigit():
        return default_channel_id, int(raw)
    return None, None


# -------------------------------------------------------------
# 3. Value List System (Auto-Sync + /value + /value_post)
# -------------------------------------------------------------
KNOWN_UNIT_ALIASES: Dict[str, List[str]] = {
    "upgraded titan tv man": ["uttv", "upgraded titan tv", "ttv"],
    "upgraded titan cameraman": ["utc", "upgraded titan cam", "tcm"],
    "upgraded titan speakerman": ["uts", "upgraded titan speaker", "tsm"],
    "engineer cameraman": ["eng", "engineer cam", "engineer"],
    "hacked cameraman": ["hacked cam", "hacked"],
    "executioner tv man": ["exectioner tv man", "exec", "executioner"],
    "reinforced titan cameraman": ["rtc", "reinforced tcm"],
    "reinforced titan speakerman": ["rts", "reinforced tsm"],
    "large scientist cameraman": ["lsc", "large scientist"],
    "large laser cameraman": ["llc", "large laser"],
    "camera attack helicopter": ["cah", "attack heli"],
    "cinemaman titan": ["cinemaman", "cinema"],
    "pharaoh cameraman": ["pharaoh"],
    "chaos cameraman": ["chaos"],
    "spider tv": ["spider"],
    "devsigned engineer": ["devsigned eng", "signed engineer"],
    "devsigned pumpkin farm": ["devsigned pumpkin", "signed pumpkin"],
    "devsigned scientist": ["signed scientist"],
}

INITIAL_VALUE_SEED: List[Tuple[str, str, str, str, str]] = [
    ("Pharaoh Cameraman", "Godly", "23,000", "8.5/10", "Fluctuating"),
    ("Spider Tv", "Exclusive", "14,000 – 15,000", "9.5/10", "Unstable"),
    ("Engineer Cameraman", "Exclusive", "6,350", "8/10", "Fluctuating"),
    ("Chaos Cameraman", "Mythic", "5,000", "9.5/10", "Heavily Overpaid"),
    ("Upgraded Titan TV Man", "Godly", "4,650", "8/10", "Slowly Dropping | Underpaid"),
    ("Upgraded Titan Cameraman", "Godly", "2,800", "7/10", "Underpaid | Stable"),
    ("x2 Candy", "Gamepasses", "2,350", "8/10", "Overpaid"),
    ("2x Money", "Gamepasses", "2,200", "9/10", "Overpaid"),
    ("Hacked Cameraman", "Exclusive", "2,000", "9/10", "Fluctuating"),
    ("Premium Pass", "Gamepasses", "1,900", "6/10", "Overpaid"),
    ("2x Lucky", "Gamepasses", "1,800", "7/10", "Stable"),
    ("Executioner TV Man", "Exclusive", "1,700", "8/10", "Stable"),
    ("Reinforced Titan Cameraman", "Exclusive", "1,100", "4/10", "Dropping"),
    ("2X Speed", "Gamepasses", "1,050", "7/10", "Stable"),
    ("Vampire Camera Man", "Exclusive", "750", "1/10", "Dropping"),
    ("Reinforced Titan Speakerman", "Exclusive", "700", "5/10", "Slowly Dropping"),
    ("Prototype Duo", "Exclusive", "500", "4/10", "Fluctuating | Underpaid"),
    ("Scientist Crate", "Crates", "425", "7/10", "Stable"),
    ("Headless TV Man", "Exclusive", "400", "1/10", "Stable | Underpaid"),
    ("Jetpack Cameraman", "Exclusive", "350", "3/10", "Underpaid | Slowly Dropping"),
    ("Large Scientist Cameraman", "Exclusive", "350", "5/10", "Underpaid"),
    ("Camera Attack Helicopter", "Mythic", "300", "2/10", "Slowly Dropping"),
    ("CinemaMan Titan", "Mythic", "300", "5.5/10", "Unstable"),
    ("Large Laser Cameraman", "Mythic", "240", "4/10", "Dropping"),
    ("Sand Coffin Crate", "Crates", "235", "2/10", "Fluctuating"),
    ("TV Woman", "Mythic", "235", "2/10", "Stable"),
    ("Large Prototype Titan", "Mythic", "220", "3.5/10", "Unstable"),
    ("Upgraded Titan Speakerman", "Mythic", "160", "2.9/10", "Stable"),
    ("Glitch Plungerman", "Mythic", "150", "4.5/10", "Slowly Dropping"),
    ("Videotaper Titan", "Mythic", "150", "3/10", "Slowly Dropping"),
    ("Scientist Cameraman", "Exclusive", "115", "6/10", "Overpaid"),
    ("Spider Crate", "Crates", "70", "4.5/10", "Stable"),
    ("Dark Speakerman", "Legendary", "35", "2/10", "Stable"),
    ("Speaker Helicopter", "Legendary", "35", "1/10", "Stable"),
    ("Camera Repair Drone", "Legendary", "25", "4/10", "Stable"),
    ("Camera Spider", "Exclusive", "15", "2/10", "Stable"),
    ("Halloween Crate", "Crates", "15", "3.5/10", "Stable"),
    ("Pumpkin Farmer", "Epic", "10", "3/10", "Stable"),
    ("Woman Crate", "Crates", "7", "1.8/10", "Stable"),
    ("SpeakerWoman", "Legendary", "3", "1.5/10", "Unstable"),
    ("Dragon Cameraman", "Legendary", "2", "1/10", "Slowly Dropping"),
    ("Laser Cameraman Car", "Legendary", "2", "1/10", "Stable"),
    ("Ninja Cameraman", "Legendary", "2", "1.8/10", "Stable"),
    ("Titan Cameraman", "Legendary", "2", "1.5/10", "Stable"),
    ("Titan TV Man", "Legendary", "2", "1.5/10", "Stable"),
    ("Titan Speakerman", "Legendary", "1", "1/10", "Stable"),
    ("DevSigned Engineer", "Signed", "o/c", "9/10", "Unstable"),
    ("DevSigned Pumpkin Farm", "Signed", "o/c", "8/10", "Stable"),
    ("DevSigned Scientist", "Signed", "o/c", "7/10", "Unstable"),
]


def normalize_unit_key(name: str) -> str:
    return re.sub(r"\s+", " ", name.strip().lower())


def build_aliases_for_unit(name: str, existing_aliases: str = "") -> str:
    key = normalize_unit_key(name)
    aliases: Set[str] = {key}
    for raw_a in existing_aliases.split(","):
        cleaned = normalize_unit_key(raw_a)
        if cleaned:
            aliases.add(cleaned)
    for known_key, extra_list in KNOWN_UNIT_ALIASES.items():
        if key == known_key or key in extra_list:
            aliases.add(known_key)
            for item in extra_list:
                aliases.add(normalize_unit_key(item))
    words = [w for w in re.split(r"[^a-z0-9]+", key) if w]
    if len(words) >= 3:
        acronym = "".join(w[0] for w in words)
        if len(acronym) >= 3:
            aliases.add(acronym)
    return ",".join(sorted(aliases))


def reload_value_cache():
    VALUE_ITEMS_CACHE.clear()
    VALUE_ALIAS_MAP.clear()
    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT item_key, name, aliases, category, value_text, prev_value_text,
                   demand_text, stability_text, sign_value_text, image_url, updated_at
            FROM value_items
            """
        )
        for row in c.fetchall():
            item = {
                "item_key": row[0],
                "name": row[1],
                "aliases": row[2] or row[0],
                "category": row[3] or "Unit",
                "value_text": row[4] or "N/A",
                "prev_value_text": row[5] or "",
                "demand_text": row[6] or "N/A",
                "stability_text": row[7] or "Stable",
                "sign_value_text": row[8] or "",
                "image_url": row[9] or "",
                "updated_at": row[10] or int(time.time()),
            }
            VALUE_ITEMS_CACHE[row[0]] = item
            VALUE_ALIAS_MAP[row[0]] = row[0]
            for alias in item["aliases"].split(","):
                a_clean = normalize_unit_key(alias)
                if a_clean:
                    VALUE_ALIAS_MAP[a_clean] = row[0]


def seed_initial_values_if_empty():
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM value_items")
        if c.fetchone()[0] == 0:
            now_ts = int(time.time())
            rows = []
            for name, cat, val, dem, stab in INITIAL_VALUE_SEED:
                key = normalize_unit_key(name)
                aliases = build_aliases_for_unit(name)
                rows.append((key, name, aliases, cat, val, "", dem, stab, "", "", now_ts))
            c.executemany(
                """
                INSERT OR IGNORE INTO value_items (
                    item_key, name, aliases, category, value_text, prev_value_text,
                    demand_text, stability_text, sign_value_text, image_url, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
    reload_value_cache()


seed_initial_values_if_empty()


def find_value_item(query: str) -> Optional[Dict[str, Any]]:
    q = normalize_unit_key(query)
    if not q:
        return None
    if q in VALUE_ALIAS_MAP:
        return VALUE_ITEMS_CACHE.get(VALUE_ALIAS_MAP[q])
    for alias, key in VALUE_ALIAS_MAP.items():
        if alias.startswith(q):
            return VALUE_ITEMS_CACHE.get(key)
    for alias, key in VALUE_ALIAS_MAP.items():
        if q in alias:
            return VALUE_ITEMS_CACHE.get(key)
    return None


def upsert_value_item(
    name: str,
    category: Optional[str] = None,
    value_text: Optional[str] = None,
    prev_value_text: Optional[str] = None,
    demand_text: Optional[str] = None,
    stability_text: Optional[str] = None,
    sign_value_text: Optional[str] = None,
    image_url: Optional[str] = None,
) -> Dict[str, Any]:
    existing = find_value_item(name)
    now_ts = int(time.time())
    if existing:
        item_key = existing["item_key"]
        final_name = existing["name"]
        final_aliases = build_aliases_for_unit(final_name, existing.get("aliases", ""))
        final_cat = category if category else existing["category"]
        final_val = value_text if value_text is not None else existing["value_text"]
        final_prev = prev_value_text if prev_value_text is not None else existing["prev_value_text"]
        final_dem = demand_text if demand_text is not None else existing["demand_text"]
        final_stab = stability_text if stability_text is not None else existing["stability_text"]
        final_sign = sign_value_text if sign_value_text is not None else existing["sign_value_text"]
        final_img = image_url if image_url else existing["image_url"]
    else:
        item_key = normalize_unit_key(name)
        final_name = name.strip()
        final_aliases = build_aliases_for_unit(final_name)
        final_cat = category or "Unit"
        final_val = value_text or "N/A"
        final_prev = prev_value_text or ""
        final_dem = demand_text or "N/A"
        final_stab = stability_text or "Stable"
        final_sign = sign_value_text or ""
        final_img = image_url or ""

    with get_db() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO value_items (
                item_key, name, aliases, category, value_text, prev_value_text,
                demand_text, stability_text, sign_value_text, image_url, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item_key,
                final_name,
                final_aliases,
                final_cat,
                final_val,
                final_prev,
                final_dem,
                final_stab,
                final_sign,
                final_img,
                now_ts,
            ),
        )
        conn.commit()

    item_dict = {
        "item_key": item_key,
        "name": final_name,
        "aliases": final_aliases,
        "category": final_cat,
        "value_text": final_val,
        "prev_value_text": final_prev,
        "demand_text": final_dem,
        "stability_text": final_stab,
        "sign_value_text": final_sign,
        "image_url": final_img,
        "updated_at": now_ts,
    }
    VALUE_ITEMS_CACHE[item_key] = item_dict
    for a in final_aliases.split(","):
        if a.strip():
            VALUE_ALIAS_MAP[a.strip()] = item_key
    return item_dict


async def sync_value_list_from_website(url: str = DEFAULT_VALUE_LIST_URL) -> int:
    global LAST_VALUE_WEB_SYNC
    if not aiohttp:
        return 0

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; CMTDBot/8.5; +https://discord.com)",
        "Accept": "text/html,application/xhtml+xml",
    }
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return 0
                raw_html = await resp.text()
    except Exception:
        return 0

    img_map: Dict[str, str] = {}
    for m in re.finditer(r'<img[^>]+alt="([^"]+?)\s+artwork"[^>]+src="([^"]+)"', raw_html, re.IGNORECASE):
        u_name = normalize_unit_key(html_lib.unescape(m.group(1)))
        src = html_lib.unescape(m.group(2))
        if src.startswith("/"):
            src = "https://cameraman-td.vaultedvaluesx.com" + src
        img_map[u_name] = src
    for m in re.finditer(r'<img[^>]+src="([^"]+)"[^>]+alt="([^"]+?)\s+artwork"', raw_html, re.IGNORECASE):
        src = html_lib.unescape(m.group(1))
        u_name = normalize_unit_key(html_lib.unescape(m.group(2)))
        if src.startswith("/"):
            src = "https://cameraman-td.vaultedvaluesx.com" + src
        img_map.setdefault(u_name, src)

    text_only = re.sub(r"<script[^>]*>.*?</script>", "\n", raw_html, flags=re.DOTALL | re.IGNORECASE)
    text_only = re.sub(r"<style[^>]*>.*?</style>", "\n", text_only, flags=re.DOTALL | re.IGNORECASE)
    text_only = re.sub(r"<[^>]+>", "\n", text_only)
    lines = [html_lib.unescape(ln).strip() for ln in text_only.splitlines() if html_lib.unescape(ln).strip()]

    categories_set = {
        "Godly", "Exclusive", "Mythic", "Legendary", "Epic", "Rare",
        "Uncommon", "Common", "Gamepasses", "Crates", "Signed",
    }
    updated_count = 0
    i = 0
    while i < len(lines):
        if lines[i] == "Value" and i + 1 < len(lines):
            val_str = lines[i + 1]
            cand_name = ""
            cand_cat = None
            for back in range(1, 4):
                if i - back < 0:
                    break
                prev_ln = lines[i - back]
                if prev_ln in categories_set:
                    cand_cat = prev_ln
                elif prev_ln not in ("Filter", "Quick scan", "Full cards") and not prev_ln.endswith("artwork"):
                    cand_name = prev_ln
                    break

            dem_str = None
            stab_str = None
            for fwd in range(2, 7):
                if i + fwd >= len(lines):
                    break
                if lines[i + fwd] == "Demand" and i + fwd + 1 < len(lines):
                    dem_str = lines[i + fwd + 1]
                elif lines[i + fwd] == "Stability" and i + fwd + 1 < len(lines):
                    stab_str = lines[i + fwd + 1]
                elif lines[i + fwd] == "Value":
                    break

            if cand_name and len(cand_name) <= 60:
                existing = find_value_item(cand_name)
                prev_val = None
                if existing and existing["value_text"] != val_str:
                    prev_val = existing["value_text"]
                img_url = img_map.get(normalize_unit_key(cand_name))
                upsert_value_item(
                    name=cand_name,
                    category=cand_cat,
                    value_text=val_str,
                    prev_value_text=prev_val,
                    demand_text=dem_str,
                    stability_text=stab_str,
                    image_url=img_url,
                )
                updated_count += 1
        i += 1

    if updated_count > 0:
        LAST_VALUE_WEB_SYNC = time.time()
    return updated_count


def parse_numeric_value(raw: str) -> Optional[float]:
    nums = re.findall(r"\d+(?:,\d+)*(?:\.\d+)?", raw)
    if not nums:
        return None
    parsed = [float(n.replace(",", "")) for n in nums]
    return sum(parsed) / len(parsed)


def parse_value_changelog_text(raw_text: str) -> List[Dict[str, Any]]:
    blocks = re.split(r"\n\s*\n", raw_text.strip())
    parsed_changes: List[Dict[str, Any]] = []

    for block in blocks:
        lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
        if not lines:
            continue

        unit_raw = lines[0].lstrip("-•* ").strip()
        if ":" in unit_raw and unit_raw.lower().startswith(("value", "status", "stability", "demand", "sign")):
            continue

        existing = find_value_item(unit_raw)
        display_name = existing["name"] if existing else unit_raw

        entry: Dict[str, Any] = {
            "raw_name": unit_raw,
            "display_name": display_name,
            "trend": "neutral",
            "lines": [],
        }

        new_val: Optional[str] = None
        prev_val: Optional[str] = None
        new_dem: Optional[str] = None
        new_stab: Optional[str] = None
        new_sign: Optional[str] = None

        for attr_line in lines[1:]:
            if ":" not in attr_line:
                entry["lines"].append(attr_line)
                continue
            k_raw, v_raw = attr_line.split(":", 1)
            k = k_raw.strip().lower()
            v = v_raw.strip()

            arrow_parts = re.split(r"\s*(?:->|➔|→|=>)\s*", v, maxsplit=1)
            has_arrow = len(arrow_parts) == 2

            if k.startswith("val"):
                if has_arrow:
                    prev_val, new_val = arrow_parts[0].strip(), arrow_parts[1].strip()
                    n_old = parse_numeric_value(prev_val)
                    n_new = parse_numeric_value(new_val)
                    if n_old is not None and n_new is not None:
                        diff = n_new - n_old
                        if diff > 0:
                            entry["trend"] = "up"
                            diff_str = f" (+{int(diff):,})" if diff.is_integer() else f" (+{diff:,.1f})"
                        elif diff < 0:
                            entry["trend"] = "down"
                            diff_str = f" ({int(diff):,})" if diff.is_integer() else f" ({diff:,.1f})"
                        else:
                            diff_str = ""
                    else:
                        diff_str = ""
                    entry["lines"].append(f"**Value:** `{prev_val}` ➔ **`{new_val}`**{diff_str}")
                else:
                    new_val = v
                    entry["lines"].append(f"**Value:** **`{new_val}`**")

            elif k.startswith("dem"):
                if has_arrow:
                    old_d, new_dem = arrow_parts[0].strip(), arrow_parts[1].strip()
                    if "/10" not in new_dem and new_dem.replace(".", "", 1).isdigit():
                        new_dem_db = f"{new_dem}/10"
                    else:
                        new_dem_db = new_dem
                    new_dem = new_dem_db
                    entry["lines"].append(f"**Demand:** `{old_d}` ➔ **`{new_dem}`**")
                else:
                    new_dem = f"{v}/10" if ("/10" not in v and v.replace(".", "", 1).isdigit()) else v
                    entry["lines"].append(f"**Demand:** **`{new_dem}`**")

            elif k.startswith(("stat", "stab")):
                if has_arrow:
                    old_s, new_stab = arrow_parts[0].strip(), arrow_parts[1].strip()
                    entry["lines"].append(f"**Status:** `{old_s}` ➔ **`{new_stab}`**")
                else:
                    new_stab = v
                    entry["lines"].append(f"**Status:** **`{new_stab}`**")

            elif k.startswith("sign"):
                if has_arrow:
                    old_sg, new_sign = arrow_parts[0].strip(), arrow_parts[1].strip()
                    entry["lines"].append(f"**Sign Value:** `{old_sg}` ➔ **`{new_sign}`**")
                else:
                    new_sign = v
                    entry["lines"].append(f"**Sign Value:** **`{new_sign}`**")
            else:
                entry["lines"].append(f"**{k_raw.strip()}:** {v}")

        upsert_value_item(
            name=display_name,
            value_text=new_val,
            prev_value_text=prev_val,
            demand_text=new_dem,
            stability_text=new_stab,
            sign_value_text=new_sign,
        )
        parsed_changes.append(entry)

    return parsed_changes


def build_unit_value_embed(guild: Optional[discord.Guild], item: Dict[str, Any], site_url: str) -> discord.Embed:
    stab_lower = item["stability_text"].lower()
    if "overpaid" in stab_lower:
        color = 0x2ECC71
    elif "dropping" in stab_lower or "underpaid" in stab_lower:
        color = 0xE74C3C
    elif "fluctuating" in stab_lower or "unstable" in stab_lower:
        color = 0xF39C12
    else:
        color = 0x3498DB

    val_display = f"**`{item['value_text']}`**"
    if item.get("prev_value_text") and item["prev_value_text"] != item["value_text"]:
        val_display += f"\n*(Previously: `{item['prev_value_text']}`)*"

    embed = discord.Embed(
        title=f"💎 {item['name']}",
        color=color,
        timestamp=datetime.fromtimestamp(item["updated_at"], tz=timezone.utc),
    )
    embed.add_field(name="💰 Value", value=val_display, inline=True)
    embed.add_field(name="🔥 Demand", value=f"**`{item['demand_text']}`**", inline=True)
    embed.add_field(name="🏷️ Category", value=f"`{item['category']}`", inline=True)
    embed.add_field(name="📊 Stability / Status", value=f"**{item['stability_text']}**", inline=True)

    if item.get("sign_value_text"):
        embed.add_field(name="✍️ Sign Value", value=f"**`{item['sign_value_text']}`**", inline=True)

    if item.get("image_url") and item["image_url"].startswith(("http://", "https://")):
        embed.set_thumbnail(url=item["image_url"])

    embed.set_footer(
        text=f"{guild.name if guild else 'CMTD'} • Official Value List",
        icon_url=guild.icon.url if (guild and guild.icon) else None,
    )
    return embed


def get_value_post_vote_counts(message_id: int) -> Tuple[int, int]:
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM value_post_votes WHERE message_id = ? AND vote = 1", (message_id,))
        up = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM value_post_votes WHERE message_id = ? AND vote = -1", (message_id,))
        down = c.fetchone()[0]
    return up, down


class ValueChangelogVoteView(discord.ui.View):
    def __init__(self, up_count: int = 0, down_count: int = 0):
        super().__init__(timeout=None)
        total = up_count + down_count
        up_pct = round((up_count / total) * 100) if total > 0 else 0
        down_pct = round((down_count / total) * 100) if total > 0 else 0

        up_btn = discord.ui.Button(
            style=discord.ButtonStyle.success,
            label=f"W Changes ({up_count} • {up_pct}%)",
            emoji="👍",
            custom_id="val_vote:up",
        )
        down_btn = discord.ui.Button(
            style=discord.ButtonStyle.danger,
            label=f"L Changes ({down_count} • {down_pct}%)",
            emoji="👎",
            custom_id="val_vote:down",
        )
        up_btn.callback = self.on_vote_up
        down_btn.callback = self.on_vote_down
        self.add_item(up_btn)
        self.add_item(down_btn)

    async def _handle_vote(self, interaction: discord.Interaction, vote_val: int):
        if not interaction.message:
            return
        msg_id = interaction.message.id
        user_id = interaction.user.id

        with get_db() as conn:
            c = conn.cursor()
            c.execute(
                "SELECT vote FROM value_post_votes WHERE message_id = ? AND user_id = ?",
                (msg_id, user_id),
            )
            row = c.fetchone()
            if row and row[0] == vote_val:
                c.execute(
                    "DELETE FROM value_post_votes WHERE message_id = ? AND user_id = ?",
                    (msg_id, user_id),
                )
                feedback = "➖ Your feedback vote was removed."
            else:
                c.execute(
                    "INSERT OR REPLACE INTO value_post_votes (message_id, user_id, vote) VALUES (?, ?, ?)",
                    (msg_id, user_id, vote_val),
                )
                feedback = "👍 Voted **W Changes**!" if vote_val == 1 else "👎 Voted **L Changes**!"
            conn.commit()

        await interaction.response.send_message(feedback, ephemeral=True)
        schedule_debounced_value_vote_refresh(interaction.message)

    async def on_vote_up(self, interaction: discord.Interaction):
        await self._handle_vote(interaction, 1)

    async def on_vote_down(self, interaction: discord.Interaction):
        await self._handle_vote(interaction, -1)


def schedule_debounced_value_vote_refresh(message: discord.Message):
    msg_id = message.id
    existing = VALUE_VOTE_UPDATE_TASKS.get(msg_id)
    if existing and not existing.done():
        return

    async def _worker():
        await asyncio.sleep(1.5)
        try:
            up, down = get_value_post_vote_counts(msg_id)
            await message.edit(view=ValueChangelogVoteView(up_count=up, down_count=down))
        except Exception:
            pass
        finally:
            VALUE_VOTE_UPDATE_TASKS.pop(msg_id, None)

    VALUE_VOTE_UPDATE_TASKS[msg_id] = asyncio.create_task(_worker())


class QuickValueLookupModal(discord.ui.Modal, title="Check Unit Value"):
    unit_input = discord.ui.TextInput(
        label="Unit Name or Shortcut (e.g. UTTV, Pharaoh)",
        style=discord.TextStyle.short,
        required=True,
        max_length=60,
        placeholder="Type unit name or shortcut like UTTV, UTC, Engineer...",
    )

    async def on_submit(self, interaction: discord.Interaction):
        item = find_value_item(self.unit_input.value)
        if not item:
            return await interaction.response.send_message(
                f"❌ Unit `{self.unit_input.value}` was not found in the value list. Try another name or check the full website!",
                ephemeral=True,
            )
        site_url = get_guild_settings(interaction.guild.id)["value_list_url"] if interaction.guild else DEFAULT_VALUE_LIST_URL
        embed = build_unit_value_embed(interaction.guild, item, site_url)
        view = build_link_view("Open Full Value List", site_url)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


class ValueHubView(discord.ui.View):
    def __init__(self, site_url: str = DEFAULT_VALUE_LIST_URL):
        super().__init__(timeout=None)
        self.add_item(
            discord.ui.Button(
                style=discord.ButtonStyle.link,
                label="Open Official Value List",
                emoji="🔗",
                url=site_url,
            )
        )
        check_btn = discord.ui.Button(
            style=discord.ButtonStyle.primary,
            label="Check Unit Value",
            emoji="🔍",
            custom_id="val_hub:check_unit",
        )
        check_btn.callback = self.on_check_click
        self.add_item(check_btn)

    async def on_check_click(self, interaction: discord.Interaction):
        await interaction.response.send_modal(QuickValueLookupModal())


class ValueChangelogModal(discord.ui.Modal, title="Publish Official Value Changes"):
    header_input = discord.ui.TextInput(
        label="Changelog Title (Optional)",
        style=discord.TextStyle.short,
        required=False,
        max_length=120,
        default="📊 Official Value List Changes",
    )
    changes_input = discord.ui.TextInput(
        label="Changes (Paste your usual unit blocks)",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=3500,
        placeholder="UTTV\nValue: 4650 -> 4000\nStatus: Underpaid\n\nUTC\nValue: 2800 -> 3000",
    )
    ping_confirm_input = discord.ui.TextInput(
        label="Ping Value Changes Role? (yes / no)",
        style=discord.TextStyle.short,
        required=True,
        max_length=5,
        default="no",
        placeholder="Type yes to ping or no to post silently",
    )
    note_input = discord.ui.TextInput(
        label="Extra Note (Optional)",
        style=discord.TextStyle.short,
        required=False,
        max_length=300,
        placeholder="e.g. Adjusted based on recent trade demand.",
    )

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild:
            return
        settings = get_guild_settings(interaction.guild.id)
        target_ch_id = settings.get("value_channel_id")
        target_ch = interaction.guild.get_channel(target_ch_id) if target_ch_id else interaction.channel
        if not isinstance(target_ch, discord.TextChannel):
            return await interaction.response.send_message(
                "❌ Value Changelog channel not found. Configure it in `/panel` ➔ `💎 Value List`.",
                ephemeral=True,
            )

        parsed = parse_value_changelog_text(self.changes_input.value)
        if not parsed:
            return await interaction.response.send_message(
                "❌ Could not parse any unit changes. Make sure each unit block starts with the unit name.",
                ephemeral=True,
            )

        sections: List[str] = []
        for entry in parsed:
            icon = "📈" if entry["trend"] == "up" else ("📉" if entry["trend"] == "down" else "⚖️")
            details = "\n".join(f"> {ln}" for ln in entry["lines"]) if entry["lines"] else "> *Updated*"
            sections.append(f"{icon} **{entry['display_name']}**\n{details}")

        desc = "\n\n".join(sections)
        if self.note_input.value.strip():
            desc += f"\n\n📝 *{self.note_input.value.strip()}*"

        embed = discord.Embed(
            title=self.header_input.value.strip() or "📊 Official Value List Changes",
            description=desc[:4000],
            color=0xF1C40F,
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_author(
            name=f"Updated by {interaction.user.display_name}",
            icon_url=interaction.user.display_avatar.url,
        )
        embed.set_footer(
            text=f"{interaction.guild.name} • Use /value to check any unit",
            icon_url=interaction.guild.icon.url if interaction.guild.icon else None,
        )

        should_ping = self.ping_confirm_input.value.strip().lower() in ("yes", "y", "да", "1", "true")
        ping_role_id = settings.get("value_ping_role_id")
        ping_role = interaction.guild.get_role(ping_role_id) if ping_role_id else None

        content = ping_role.mention if (should_ping and ping_role) else None
        allowed = discord.AllowedMentions(roles=True) if content else discord.AllowedMentions.none()

        sent = await target_ch.send(
            content=content,
            embed=embed,
            view=ValueChangelogVoteView(up_count=0, down_count=0),
            allowed_mentions=allowed,
        )
        ping_note = f" (pinged {ping_role.mention})" if content else " (posted without ping)"
        await interaction.response.send_message(
            f"✅ Published **{len(parsed)}** unit update(s) in {target_ch.mention}{ping_note} and synced the `/value` database! [Jump to Post]({sent.jump_url})",
            ephemeral=True,
        )


# -------------------------------------------------------------
# 4. Reliable W/F/L Emoji Resolver & Reaction Queue
# -------------------------------------------------------------
async def resolve_wfl_emojis(guild: discord.Guild, force_refresh: bool = False) -> List[Any]:
    global APP_EMOJIS_CACHE
    if not force_refresh and guild.id in WFL_EMOJI_CACHE and len(WFL_EMOJI_CACHE[guild.id]) == 3:
        return WFL_EMOJI_CACHE[guild.id]

    guild_map = {e.name.lower(): e for e in guild.emojis}
    bot_map = {e.name.lower(): e for e in bot.emojis}
    app_map = {e.name.lower(): e for e in APP_EMOJIS_CACHE}

    found = []
    for name in WFL_EMOJI_NAMES:
        em = guild_map.get(name) or bot_map.get(name) or app_map.get(name)
        if em:
            found.append(em)

    if len(found) < 3:
        try:
            fetched_guild_emojis = await guild.fetch_emojis()
            for e in fetched_guild_emojis:
                guild_map[e.name.lower()] = e
        except Exception:
            pass

        if len(guild_map) < 3 or any(n not in guild_map and n not in bot_map for n in WFL_EMOJI_NAMES):
            try:
                APP_EMOJIS_CACHE = await bot.fetch_application_emojis()
                app_map = {e.name.lower(): e for e in APP_EMOJIS_CACHE}
            except Exception:
                pass

        found = []
        for name in WFL_EMOJI_NAMES:
            em = guild_map.get(name) or bot_map.get(name) or app_map.get(name)
            if em:
                found.append(em)

    if len(found) == 3:
        WFL_EMOJI_CACHE[guild.id] = found
    return found


async def apply_wfl_reactions(message: discord.Message):
    if not message.guild:
        return
    emojis = await resolve_wfl_emojis(message.guild)
    if not emojis:
        return

    for emoji in emojis:
        for attempt in range(3):
            try:
                await message.add_reaction(emoji)
                await asyncio.sleep(0.25)
                break
            except discord.NotFound:
                return
            except discord.Forbidden:
                return
            except discord.HTTPException:
                await asyncio.sleep(0.6 * (attempt + 1))


# -------------------------------------------------------------
# 5. Smart Anti-Scam & Phishing Shield (Configurable in /panel)
# -------------------------------------------------------------
URL_EXTRACT_RE = re.compile(r"https?://([a-zA-Z0-9.-]+)", re.IGNORECASE)

SAFE_DOMAINS = {
    "roblox.com",
    "www.roblox.com",
    "web.roblox.com",
    "devforum.roblox.com",
    "create.roblox.com",
    "corp.roblox.com",
    "discord.com",
    "www.discord.com",
    "discord.gg",
    "discordapp.com",
    "cdn.discordapp.com",
    "media.discordapp.net",
    "youtube.com",
    "www.youtube.com",
    "youtu.be",
    "twitter.com",
    "x.com",
    "tenor.com",
    "giphy.com",
    "imgur.com",
    "i.imgur.com",
    "cameraman-td.vaultedvaluesx.com",
    "vaultedvaluesx.com",
}

PHISHING_DOMAIN_PATTERNS = (
    re.compile(r"(^|\.)rob[il1]ox", re.IGNORECASE),
    re.compile(r"(^|\.)r[o0]b[il1][o0]x", re.IGNORECASE),
    re.compile(r"roblox\.com\.[a-z]{2,}", re.IGNORECASE),
    re.compile(r"roblox-[a-z0-9-]+\.", re.IGNORECASE),
    re.compile(r"(^|\.)d[il1]sc[o0]rd(?!app\.com$|a\.com$|\.com$|\.gg$|\.media$|\.gift$)", re.IGNORECASE),
    re.compile(r"discord-?nitro", re.IGNORECASE),
    re.compile(r"steamcommun[il1]ty", re.IGNORECASE),
)


def detect_phishing_domain(content: str) -> Optional[str]:
    if "http://" not in content.lower() and "https://" not in content.lower():
        return None
    for match in URL_EXTRACT_RE.finditer(content):
        domain = match.group(1).lower().rstrip(".")
        if domain in SAFE_DOMAINS:
            continue
        for pat in PHISHING_DOMAIN_PATTERNS:
            if pat.search(domain):
                return domain
    return None


async def handle_antiscam_check(message: discord.Message, settings: Dict[str, Any]) -> bool:
    if not settings.get("antiscam_enabled"):
        return False
    if not message.guild or not isinstance(message.author, discord.Member):
        return False
    if message.author.guild_permissions.manage_messages or message.author.guild_permissions.administrator:
        return False

    bad_domain = detect_phishing_domain(message.content)
    if not bad_domain:
        return False

    try:
        await message.delete()
    except Exception:
        pass

    timeout_mins = settings.get("antiscam_timeout_mins", 60)
    action_note = "Message deleted."
    if timeout_mins and timeout_mins > 0:
        try:
            await message.author.timeout(
                timedelta(minutes=timeout_mins),
                reason=f"Anti-Scam Shield: phishing domain ({bad_domain})",
            )
            action_note = f"Message deleted + **{timeout_mins}m timeout** applied."
        except Exception:
            action_note = "Message deleted (missing permission to timeout member)."

    log_ch_id = settings.get("antiscam_log_channel_id")
    if log_ch_id:
        log_ch = message.guild.get_channel(log_ch_id)
        if isinstance(log_ch, discord.TextChannel):
            embed = discord.Embed(
                title="🛡️ Anti-Scam Shield Triggered",
                color=0xE74C3C,
                timestamp=datetime.now(timezone.utc),
            )
            embed.add_field(
                name="👤 Offender",
                value=f"{message.author.mention} (`{message.author.id}`)",
                inline=True,
            )
            embed.add_field(name="📍 Channel", value=message.channel.mention, inline=True)
            embed.add_field(name="🚨 Flagged Domain", value=f"`{bad_domain}`", inline=True)
            embed.add_field(name="⚡ Action Taken", value=action_note, inline=False)
            safe_preview = discord.utils.escape_markdown(message.content[:500])
            embed.add_field(name="📝 Blocked Content", value=f"```{safe_preview}```", inline=False)
            try:
                await log_ch.send(embed=embed)
            except Exception:
                pass
    return True


# -------------------------------------------------------------
# 6. Bot Client Initialization & Persistent Views Setup
# -------------------------------------------------------------
class CommunityBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.guilds = True
        intents.voice_states = True
        intents.emojis_and_stickers = True
        super().__init__(
            command_prefix="!",
            intents=intents,
            help_command=None,
            max_messages=2500,
        )

    async def setup_hook(self):
        self.add_view(ValueChangelogVoteView())
        self.add_view(ValueHubView())
        self.add_view(VoiceKeepAliveView())
        self.add_view(VoiceControlView())
        self.add_view(PollView())
        self.add_view(EventFollowView())
        if not background_scheduler.is_running():
            background_scheduler.start()


bot = CommunityBot()


# -------------------------------------------------------------
# 7. Server Lockdown System (/closeall, /openall, /lockdown_ignore)
# -------------------------------------------------------------
def is_lockdown_active(guild_id: int) -> bool:
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT 1 FROM lockdown_snapshots WHERE guild_id = ? LIMIT 1", (guild_id,))
        return c.fetchone() is not None


def get_lockdown_whitelist(guild_id: int) -> Dict[int, str]:
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT target_id, target_type FROM lockdown_whitelist WHERE guild_id = ?", (guild_id,))
        return dict(c.fetchall())


def toggle_lockdown_whitelist(guild_id: int, target_id: int, target_type: str) -> bool:
    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT 1 FROM lockdown_whitelist WHERE guild_id = ? AND target_id = ?",
            (guild_id, target_id),
        )
        exists = c.fetchone() is not None
        if exists:
            c.execute(
                "DELETE FROM lockdown_whitelist WHERE guild_id = ? AND target_id = ?",
                (guild_id, target_id),
            )
            conn.commit()
            return False
        else:
            c.execute(
                "INSERT INTO lockdown_whitelist (guild_id, target_id, target_type) VALUES (?, ?, ?)",
                (guild_id, target_id, target_type),
            )
            conn.commit()
            return True


def encode_perm_bool(val: Optional[bool]) -> int:
    if val is True:
        return 1
    if val is False:
        return 0
    return -1


def decode_perm_bool(code: int) -> Optional[bool]:
    if code == 1:
        return True
    if code == 0:
        return False
    return None


@bot.tree.command(name="closeall", description="Safely lock all open public channels while keeping read-only channels untouched")
@app_commands.describe(
    reason="Reason shown in locked channels (optional)",
    announce="Send a lock notice message into each locked channel (default: True)",
)
@app_commands.default_permissions(administrator=True)
async def closeall_cmd(
    interaction: discord.Interaction,
    reason: str = "Temporary server lockdown by administration.",
    announce: bool = True,
):
    if not interaction.guild:
        return
    guild = interaction.guild

    if guild.id in LOCKDOWN_IN_PROGRESS:
        return await interaction.response.send_message(
            "⏳ A lockdown operation is already running on this server. Please wait.",
            ephemeral=True,
        )
    if is_lockdown_active(guild.id):
        return await interaction.response.send_message(
            "⚠️ Server is **already locked down**! Run `/openall` first before using `/closeall` again so your clean channel snapshot is preserved.",
            ephemeral=True,
        )

    LOCKDOWN_IN_PROGRESS.add(guild.id)
    await interaction.response.send_message("🔒 Scanning channel permissions & starting safe lockdown...", ephemeral=True)

    try:
        whitelist = get_lockdown_whitelist(guild.id)
        everyone = guild.default_role

        channels_to_lock: List[discord.TextChannel] = []
        skipped_readonly = 0
        skipped_whitelisted = 0

        for ch in guild.text_channels:
            if ch.id in whitelist or (ch.category_id and ch.category_id in whitelist):
                skipped_whitelisted += 1
                continue

            effective_perms = ch.permissions_for(everyone)
            if not effective_perms.send_messages:
                skipped_readonly += 1
                continue

            channels_to_lock.append(ch)

        if not channels_to_lock:
            return await interaction.edit_original_response(
                content=(
                    f"ℹ️ No open public channels needed locking.\n"
                    f"• Read-only skipped (e.g. announcements/logs): `{skipped_readonly}`\n"
                    f"• Whitelisted skipped: `{skipped_whitelisted}`"
                )
            )

        snapshot_rows = []
        locked_count = 0
        failed_count = 0

        notice_embed = discord.Embed(
            title="🔒 Channel Temporarily Locked",
            description=f"**Reason:** {reason.strip()}",
            color=0xE74C3C,
            timestamp=datetime.now(timezone.utc),
        )
        notice_embed.set_footer(text=guild.name, icon_url=guild.icon.url if guild.icon else None)

        for idx, ch in enumerate(channels_to_lock, start=1):
            ow = ch.overwrites_for(everyone)
            snapshot_rows.append(
                (
                    guild.id,
                    ch.id,
                    encode_perm_bool(ow.send_messages),
                    encode_perm_bool(ow.send_messages_in_threads),
                    encode_perm_bool(ow.create_public_threads),
                )
            )
            ow.send_messages = False
            ow.send_messages_in_threads = False
            ow.create_public_threads = False

            try:
                await ch.set_permissions(everyone, overwrite=ow, reason=f"Lockdown by {interaction.user}: {reason[:80]}")
                locked_count += 1
                if announce:
                    try:
                        await ch.send(embed=notice_embed)
                    except Exception:
                        pass
            except Exception:
                failed_count += 1

            await asyncio.sleep(0.35)
            if idx % 8 == 0 and idx < len(channels_to_lock):
                try:
                    await interaction.edit_original_response(
                        content=f"🔒 Locking channels... (**{idx}/{len(channels_to_lock)}**)"
                    )
                except Exception:
                    pass

        with get_db() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO lockdown_snapshots
                (guild_id, channel_id, prev_send_messages, prev_send_in_threads, prev_create_threads)
                VALUES (?, ?, ?, ?, ?)
                """,
                snapshot_rows,
            )
            conn.commit()

        summary = discord.Embed(
            title="🔒 Server Lockdown Complete",
            color=0xE74C3C,
            timestamp=datetime.now(timezone.utc),
        )
        summary.add_field(name="🔒 Channels Locked", value=f"`{locked_count}`", inline=True)
        summary.add_field(name="🛡️ Read-Only Preserved", value=f"`{skipped_readonly}`", inline=True)
        summary.add_field(name="📋 Whitelisted Skipped", value=f"`{skipped_whitelisted}`", inline=True)
        if failed_count:
            summary.add_field(name="⚠️ Missing Perms", value=f"`{failed_count}`", inline=True)
        summary.add_field(name="📝 Reason", value=reason.strip(), inline=False)
        summary.set_footer(text="Run /openall when ready to restore exact permissions.")
        await interaction.edit_original_response(content=None, embed=summary)
    finally:
        LOCKDOWN_IN_PROGRESS.discard(guild.id)


@bot.tree.command(name="openall", description="Restore all channels locked by /closeall without opening announcements/logs")
@app_commands.describe(announce="Send an unlock notice into each restored channel (default: True)")
@app_commands.default_permissions(administrator=True)
async def openall_cmd(interaction: discord.Interaction, announce: bool = True):
    if not interaction.guild:
        return
    guild = interaction.guild

    if guild.id in LOCKDOWN_IN_PROGRESS:
        return await interaction.response.send_message(
            "⏳ A lockdown operation is currently in progress. Please wait.",
            ephemeral=True,
        )

    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT channel_id, prev_send_messages, prev_send_in_threads, prev_create_threads
            FROM lockdown_snapshots WHERE guild_id = ?
            """,
            (guild.id,),
        )
        rows = c.fetchall()

    if not rows:
        return await interaction.response.send_message(
            "ℹ️ No active lockdown snapshot found for this server.",
            ephemeral=True,
        )

    LOCKDOWN_IN_PROGRESS.add(guild.id)
    await interaction.response.send_message(
        f"🔓 Restoring permissions for **{len(rows)}** channels from snapshot...",
        ephemeral=True,
    )

    try:
        everyone = guild.default_role
        restored_count = 0
        missing_count = 0

        unlock_embed = discord.Embed(
            title="🔓 Channel Unlocked",
            description="Chat permissions have been restored by administration.",
            color=0x2ECC71,
            timestamp=datetime.now(timezone.utc),
        )
        unlock_embed.set_footer(text=guild.name, icon_url=guild.icon.url if guild.icon else None)

        for idx, (ch_id, prev_send, prev_threads, prev_create) in enumerate(rows, start=1):
            ch = guild.get_channel(ch_id)
            if not isinstance(ch, discord.TextChannel):
                missing_count += 1
                continue

            ow = ch.overwrites_for(everyone)
            ow.send_messages = decode_perm_bool(prev_send)
            ow.send_messages_in_threads = decode_perm_bool(prev_threads)
            ow.create_public_threads = decode_perm_bool(prev_create)

            try:
                if ow.is_empty():
                    await ch.set_permissions(everyone, overwrite=None, reason=f"Lockdown lifted by {interaction.user}")
                else:
                    await ch.set_permissions(everyone, overwrite=ow, reason=f"Lockdown lifted by {interaction.user}")
                restored_count += 1
                if announce:
                    try:
                        await ch.send(embed=unlock_embed)
                    except Exception:
                        pass
            except Exception:
                missing_count += 1

            await asyncio.sleep(0.35)
            if idx % 8 == 0 and idx < len(rows):
                try:
                    await interaction.edit_original_response(
                        content=f"🔓 Restoring channels... (**{idx}/{len(rows)}**)"
                    )
                except Exception:
                    pass

        with get_db() as conn:
            conn.execute("DELETE FROM lockdown_snapshots WHERE guild_id = ?", (guild.id,))
            conn.commit()

        summary = discord.Embed(
            title="🔓 Server Lockdown Lifted",
            description="All locked channels have been restored to their exact pre-lockdown permissions. Read-only channels remained untouched.",
            color=0x2ECC71,
            timestamp=datetime.now(timezone.utc),
        )
        summary.add_field(name="🔓 Channels Restored", value=f"`{restored_count}`", inline=True)
        if missing_count:
            summary.add_field(name="⚠️ Skipped / Deleted", value=f"`{missing_count}`", inline=True)
        await interaction.edit_original_response(content=None, embed=summary)
    finally:
        LOCKDOWN_IN_PROGRESS.discard(guild.id)


@bot.tree.command(name="lockdown_ignore", description="Add or remove a channel/category from /closeall lockdown")
@app_commands.describe(
    channel="Text channel to ignore/unignore during /closeall",
    category="Entire category to ignore/unignore during /closeall",
)
@app_commands.default_permissions(administrator=True)
async def lockdown_ignore_cmd(
    interaction: discord.Interaction,
    channel: Optional[discord.TextChannel] = None,
    category: Optional[discord.CategoryChannel] = None,
):
    if not interaction.guild:
        return
    guild = interaction.guild
    actions = []

    if channel:
        added = toggle_lockdown_whitelist(guild.id, channel.id, "channel")
        actions.append(f"{'✅ Added' if added else '🗑️ Removed'} channel {channel.mention}")
    if category:
        added = toggle_lockdown_whitelist(guild.id, category.id, "category")
        actions.append(f"{'✅ Added' if added else '🗑️ Removed'} category **{category.name}**")

    wl = get_lockdown_whitelist(guild.id)
    items_desc = []
    for tid, ttype in wl.items():
        if ttype == "category":
            cat = guild.get_channel(tid)
            items_desc.append(f"📁 **{cat.name if cat else tid}** (Category)")
        else:
            items_desc.append(f"💬 <#{tid}>")

    embed = discord.Embed(
        title="📋 Lockdown Whitelist (Ignored by /closeall)",
        description="\n".join(items_desc) if items_desc else "*No channels or categories whitelisted yet.*",
        color=0x5865F2,
    )
    header = "\n".join(actions) if actions else "Pass a `channel` or `category` parameter to toggle it in this list."
    await interaction.response.send_message(content=header, embed=embed, ephemeral=True)


# -------------------------------------------------------------
# 8. Private Temporary Voice Channels 2.0
# -------------------------------------------------------------
def get_user_voice_preset(user_id: int) -> Dict[str, Any]:
    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT custom_name, user_limit, default_locked, default_hidden FROM user_voice_presets WHERE user_id = ?",
            (user_id,),
        )
        row = c.fetchone()
    if not row:
        return {
            "custom_name": None,
            "user_limit": 0,
            "default_locked": False,
            "default_hidden": False,
        }
    return {
        "custom_name": row[0],
        "user_limit": row[1] or 0,
        "default_locked": bool(row[2]),
        "default_hidden": bool(row[3]),
    }


def save_user_voice_preset(user_id: int, **kwargs):
    current = get_user_voice_preset(user_id)
    current.update(kwargs)
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO user_voice_presets (user_id, custom_name, user_limit, default_locked, default_hidden)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                custom_name = excluded.custom_name,
                user_limit = excluded.user_limit,
                default_locked = excluded.default_locked,
                default_hidden = excluded.default_hidden
            """,
            (
                user_id,
                current["custom_name"],
                current["user_limit"],
                1 if current["default_locked"] else 0,
                1 if current["default_hidden"] else 0,
            ),
        )
        conn.commit()


def reset_user_voice_preset(user_id: int):
    with get_db() as conn:
        conn.execute("DELETE FROM user_voice_presets WHERE user_id = ?", (user_id,))
        conn.commit()


def load_temp_voices_from_db():
    TEMP_VOICE_CACHE.clear()
    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT channel_id, guild_id, owner_id, control_message_id,
                   created_at, last_active_at, last_renamed_at, is_locked, is_hidden, warned_inactive
            FROM temp_voices
            """
        )
        for row in c.fetchall():
            TEMP_VOICE_CACHE[row[0]] = {
                "channel_id": row[0],
                "guild_id": row[1],
                "owner_id": row[2],
                "control_message_id": row[3] or 0,
                "created_at": row[4] or int(time.time()),
                "last_active_at": row[5] or int(time.time()),
                "last_renamed_at": row[6] or 0,
                "is_locked": bool(row[7]),
                "is_hidden": bool(row[8]),
                "warned_inactive": bool(row[9]),
            }


load_temp_voices_from_db()


def save_temp_voice(room: Dict[str, Any]):
    TEMP_VOICE_CACHE[room["channel_id"]] = room
    with get_db() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO temp_voices (
                channel_id, guild_id, owner_id, control_message_id,
                created_at, last_active_at, last_renamed_at, is_locked, is_hidden, warned_inactive
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                room["channel_id"],
                room["guild_id"],
                room["owner_id"],
                room.get("control_message_id", 0),
                room["created_at"],
                room["last_active_at"],
                room.get("last_renamed_at", 0),
                1 if room.get("is_locked") else 0,
                1 if room.get("is_hidden") else 0,
                1 if room.get("warned_inactive") else 0,
            ),
        )
        conn.commit()


def touch_temp_voice_activity(channel_id: int):
    room = TEMP_VOICE_CACHE.get(channel_id)
    if not room:
        return
    now_ts = int(time.time())
    was_warned = room.get("warned_inactive", False)
    should_persist = was_warned or ((now_ts - room.get("last_active_at", 0)) >= 10)
    room["last_active_at"] = now_ts
    room["warned_inactive"] = False
    if should_persist:
        with get_db() as conn:
            conn.execute(
                "UPDATE temp_voices SET last_active_at = ?, warned_inactive = 0 WHERE channel_id = ?",
                (now_ts, channel_id),
            )
            conn.commit()


def remove_temp_voice_record(channel_id: int):
    TEMP_VOICE_CACHE.pop(channel_id, None)
    with get_db() as conn:
        conn.execute("DELETE FROM temp_voices WHERE channel_id = ?", (channel_id,))
        conn.commit()


async def delete_temp_voice_channel(channel: discord.VoiceChannel, reason: str):
    remove_temp_voice_record(channel.id)
    try:
        await channel.delete(reason=reason)
    except Exception:
        pass


async def get_or_create_watch_together_invite(vc: discord.VoiceChannel, user: discord.abc.User) -> str:
    try:
        for inv in await vc.invites():
            if inv.target_application and inv.target_application.id == YOUTUBE_ACTIVITY_ID:
                return inv.url
    except Exception:
        pass

    try:
        invite = await vc.create_invite(
            max_age=3600,
            unique=False,
            target_type=discord.InviteTarget.embedded_application,
            target_application_id=YOUTUBE_ACTIVITY_ID,
            reason=f"Watch Together launched by {user}",
        )
        return invite.url
    except discord.HTTPException as e:
        if e.code != 30016:
            raise

    deleted_any = False
    try:
        guild_invites = await vc.guild.invites()
        bot_invites = [
            inv for inv in guild_invites
            if bot.user and inv.inviter and inv.inviter.id == bot.user.id
        ]
        bot_invites.sort(key=lambda inv: inv.created_at or datetime.now(timezone.utc))
        for old_inv in bot_invites[:15]:
            try:
                await old_inv.delete(reason="Pruning stale bot activity invites (30016 limit)")
                deleted_any = True
                await asyncio.sleep(0.2)
            except Exception:
                pass
    except Exception:
        pass

    if deleted_any:
        invite = await vc.create_invite(
            max_age=3600,
            unique=False,
            target_type=discord.InviteTarget.embedded_application,
            target_application_id=YOUTUBE_ACTIVITY_ID,
            reason=f"Watch Together launched by {user}",
        )
        return invite.url

    raise RuntimeError("SERVER_INVITE_CAP_REACHED")


def build_voice_control_embed(channel: discord.VoiceChannel, room: Dict[str, Any]) -> discord.Embed:
    status_badges = []
    status_badges.append("🔒 **Locked**" if room.get("is_locked") else "🔓 **Open**")
    status_badges.append("🙈 **Hidden**" if room.get("is_hidden") else "👁️ **Visible**")
    limit_str = f"`{channel.user_limit}` slots" if channel.user_limit > 0 else "`Unlimited`"
    status_badges.append(f"👥 {limit_str}")

    owner_member = channel.guild.get_member(room["owner_id"])
    if owner_member and owner_member.premium_since is not None:
        status_badges.append(f"🚀 **{channel.bitrate // 1000}kbps HD**")

    embed = discord.Embed(
        title="🎛️ Private Voice Room Controls 2.0",
        description=(
            f"**Room Owner:** <@{room['owner_id']}>\n"
            f"**Status:** {' • '.join(status_badges)}\n\n"
            "• Your room name, limit & lock settings are **auto-saved** for your next session.\n"
            "• Use **Watch Together** to queue YouTube/music right inside this room.\n"
            "-# ⏱️ Auto-deletes when empty or after **5 minutes** of inactivity (with a 1-min warning)."
        ),
        color=0xE74C3C if room.get("is_locked") else 0x2ECC71,
    )
    embed.set_footer(text=f"Room ID: {channel.id}")
    return embed


async def refresh_voice_control_panel(channel: discord.VoiceChannel, room: Dict[str, Any]):
    msg_id = room.get("control_message_id", 0)
    if not msg_id:
        return
    try:
        msg = await channel.fetch_message(msg_id)
        await msg.edit(embed=build_voice_control_embed(channel, room), view=VoiceControlView())
    except Exception:
        pass


class VoiceKeepAliveView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Keep Room Alive",
        style=discord.ButtonStyle.success,
        emoji="⚡",
        custom_id="vc_ctrl:keepalive",
    )
    async def keep_alive_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not isinstance(interaction.channel, discord.VoiceChannel):
            return
        room = TEMP_VOICE_CACHE.get(interaction.channel.id)
        if not room:
            return await interaction.response.send_message("❌ Room already closed.", ephemeral=True)
        touch_temp_voice_activity(interaction.channel.id)
        await interaction.response.edit_message(
            content=f"⚡ Activity confirmed by {interaction.user.mention}! Room timer reset.",
            view=None,
        )


class VoiceTransferView(discord.ui.View):
    def __init__(self, vc: discord.VoiceChannel, room: Dict[str, Any]):
        super().__init__(timeout=60)
        self.vc = vc
        self.room = room

    @discord.ui.select(
        cls=discord.ui.UserSelect,
        placeholder="👑 Select the new room owner...",
        min_values=1,
        max_values=1,
    )
    async def select_new_owner(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        target = select.values[0]
        if target.bot or not isinstance(target, discord.Member) or target not in self.vc.members:
            return await interaction.response.send_message(
                "❌ The new owner must be a human member currently inside this voice channel.",
                ephemeral=True,
            )
        if target.id == self.room["owner_id"]:
            return await interaction.response.send_message("👑 You already own this room!", ephemeral=True)

        self.room["owner_id"] = target.id
        self.room["last_active_at"] = int(time.time())
        self.room["warned_inactive"] = False
        save_temp_voice(self.room)
        await self.vc.set_permissions(target, connect=True, view_channel=True, manage_channels=True)
        await refresh_voice_control_panel(self.vc, self.room)
        await interaction.response.edit_message(content=f"👑 Ownership transferred to {target.mention}!", view=None)


class VoiceRenameModal(discord.ui.Modal, title="Rename Voice Room"):
    name_input = discord.ui.TextInput(
        label="New Room Name (Auto-saved to your preset)",
        style=discord.TextStyle.short,
        required=True,
        min_length=2,
        max_length=40,
        placeholder="e.g. Ranked Squad | Need 2",
    )

    def __init__(self, channel: discord.VoiceChannel, room: Dict[str, Any]):
        super().__init__()
        self.channel = channel
        self.room = room
        self.name_input.default = channel.name

    async def on_submit(self, interaction: discord.Interaction):
        now_ts = int(time.time())
        new_name = self.name_input.value.strip()
        save_user_voice_preset(interaction.user.id, custom_name=new_name)

        last_ren = self.room.get("last_renamed_at", 0)
        if now_ts - last_ren < 300:
            ready_ts = last_ren + 300
            return await interaction.response.send_message(
                f"💾 Name saved to your personal preset for future rooms! Discord limits live renames — you can rename this active channel again <t:{ready_ts}:R>.",
                ephemeral=True,
            )
        try:
            await self.channel.edit(name=new_name, reason=f"Renamed by room owner {interaction.user}")
            self.room["last_renamed_at"] = now_ts
            self.room["last_active_at"] = now_ts
            self.room["warned_inactive"] = False
            save_temp_voice(self.room)
            await interaction.response.send_message(
                f"✅ Room renamed to **{new_name}** and saved to your personal preset!",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ Could not rename room: `{e}`", ephemeral=True)


class VoiceLimitModal(discord.ui.Modal, title="Set Room User Limit"):
    limit_input = discord.ui.TextInput(
        label="Max Members (0 = Unlimited, 1 - 99)",
        style=discord.TextStyle.short,
        required=True,
        max_length=2,
        placeholder="e.g. 4",
    )

    def __init__(self, channel: discord.VoiceChannel, room: Dict[str, Any]):
        super().__init__()
        self.channel = channel
        self.room = room
        self.limit_input.default = str(channel.user_limit)

    async def on_submit(self, interaction: discord.Interaction):
        val = self.limit_input.value.strip()
        if not val.isdigit() or not (0 <= int(val) <= 99):
            return await interaction.response.send_message("❌ Enter a number from `0` to `99`.", ephemeral=True)
        limit = int(val)
        await self.channel.edit(user_limit=limit, reason=f"Limit updated by {interaction.user}")
        save_user_voice_preset(interaction.user.id, user_limit=limit)
        self.room["last_active_at"] = int(time.time())
        self.room["warned_inactive"] = False
        save_temp_voice(self.room)
        await refresh_voice_control_panel(self.channel, self.room)
        msg = "👥 User limit removed (`Unlimited`) & saved." if limit == 0 else f"👥 User limit set to **{limit}** & saved."
        await interaction.response.send_message(msg, ephemeral=True)


class VoiceControlView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    def _get_room_and_verify_owner(
        self, interaction: discord.Interaction
    ) -> Tuple[Optional[discord.VoiceChannel], Optional[Dict[str, Any]], Optional[str]]:
        if not interaction.guild or not isinstance(interaction.channel, discord.VoiceChannel):
            return None, None, "❌ This control panel only works inside a temporary voice channel."
        room = TEMP_VOICE_CACHE.get(interaction.channel.id)
        if not room:
            return None, None, "❌ Room data not found."
        if interaction.user.id != room["owner_id"]:
            return None, None, f"❌ Only the room owner (<@{room['owner_id']}>) can change room settings."
        return interaction.channel, room, None

    @discord.ui.button(label="Lock / Unlock", style=discord.ButtonStyle.secondary, emoji="🔒", custom_id="vc_ctrl:lock", row=0)
    async def lock_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        vc, room, err = self._get_room_and_verify_owner(interaction)
        if err or not vc or not room:
            return await interaction.response.send_message(err, ephemeral=True)

        new_locked = not room.get("is_locked", False)
        everyone = vc.guild.default_role
        ow = vc.overwrites_for(everyone)
        ow.connect = False if new_locked else None
        await vc.set_permissions(everyone, overwrite=ow, reason="Room lock toggled")

        owner_member = vc.guild.get_member(room["owner_id"])
        if owner_member:
            await vc.set_permissions(owner_member, connect=True, view_channel=True, manage_channels=True)

        save_user_voice_preset(interaction.user.id, default_locked=new_locked)
        room["is_locked"] = new_locked
        room["last_active_at"] = int(time.time())
        room["warned_inactive"] = False
        save_temp_voice(room)
        await refresh_voice_control_panel(vc, room)
        await interaction.response.send_message(
            "🔒 Room **locked** (saved to preset)!" if new_locked else "🔓 Room **unlocked** (saved to preset)!",
            ephemeral=True,
        )

    @discord.ui.button(label="Hide / Show", style=discord.ButtonStyle.secondary, emoji="👁️", custom_id="vc_ctrl:hide", row=0)
    async def hide_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        vc, room, err = self._get_room_and_verify_owner(interaction)
        if err or not vc or not room:
            return await interaction.response.send_message(err, ephemeral=True)

        new_hidden = not room.get("is_hidden", False)
        everyone = vc.guild.default_role
        ow = vc.overwrites_for(everyone)
        ow.view_channel = False if new_hidden else None
        await vc.set_permissions(everyone, overwrite=ow, reason="Room visibility toggled")

        owner_member = vc.guild.get_member(room["owner_id"])
        if owner_member:
            await vc.set_permissions(owner_member, connect=True, view_channel=True, manage_channels=True)

        save_user_voice_preset(interaction.user.id, default_hidden=new_hidden)
        room["is_hidden"] = new_hidden
        room["last_active_at"] = int(time.time())
        room["warned_inactive"] = False
        save_temp_voice(room)
        await refresh_voice_control_panel(vc, room)
        await interaction.response.send_message(
            "🙈 Room is now **hidden** (saved to preset)." if new_hidden else "👁️ Room is now **visible** (saved to preset).",
            ephemeral=True,
        )

    @discord.ui.button(label="Rename", style=discord.ButtonStyle.secondary, emoji="✏️", custom_id="vc_ctrl:rename", row=0)
    async def rename_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        vc, room, err = self._get_room_and_verify_owner(interaction)
        if err or not vc or not room:
            return await interaction.response.send_message(err, ephemeral=True)
        await interaction.response.send_modal(VoiceRenameModal(vc, room))

    @discord.ui.button(label="Limit", style=discord.ButtonStyle.secondary, emoji="👥", custom_id="vc_ctrl:limit", row=0)
    async def limit_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        vc, room, err = self._get_room_and_verify_owner(interaction)
        if err or not vc or not room:
            return await interaction.response.send_message(err, ephemeral=True)
        await interaction.response.send_modal(VoiceLimitModal(vc, room))

    @discord.ui.button(label="Claim", style=discord.ButtonStyle.primary, emoji="👑", custom_id="vc_ctrl:claim", row=0)
    async def claim_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.channel, discord.VoiceChannel):
            return
        vc = interaction.channel
        room = TEMP_VOICE_CACHE.get(vc.id)
        if not room:
            return await interaction.response.send_message("❌ Room data not found.", ephemeral=True)

        if interaction.user.id == room["owner_id"]:
            return await interaction.response.send_message("👑 You already own this room!", ephemeral=True)

        if not isinstance(interaction.user, discord.Member) or interaction.user not in vc.members:
            return await interaction.response.send_message(
                "❌ You must be connected to this voice channel to claim it.",
                ephemeral=True,
            )

        current_owner = vc.guild.get_member(room["owner_id"])
        if current_owner and current_owner in vc.members:
            return await interaction.response.send_message(
                f"❌ Current owner {current_owner.mention} is still in the voice channel!",
                ephemeral=True,
            )

        room["owner_id"] = interaction.user.id
        room["last_active_at"] = int(time.time())
        room["warned_inactive"] = False
        save_temp_voice(room)
        await vc.set_permissions(interaction.user, connect=True, view_channel=True, manage_channels=True)
        await refresh_voice_control_panel(vc, room)
        await interaction.response.send_message(f"👑 {interaction.user.mention} claimed ownership of this voice room!")

    @discord.ui.select(
        placeholder="⚡ Quick Mode: Duo / Trio / Squad / Raid / Open...",
        options=[
            discord.SelectOption(label="👤 Duo (2 Slots)", value="2", description="Set user limit to 2"),
            discord.SelectOption(label="🛡️ Trio (3 Slots)", value="3", description="Set user limit to 3"),
            discord.SelectOption(label="⚔️ Squad (4 Slots)", value="4", description="Set user limit to 4"),
            discord.SelectOption(label="🔥 Raid Party (6 Slots)", value="6", description="Set user limit to 6"),
            discord.SelectOption(label="🌐 Open Lobby (Unlimited)", value="0", description="Remove user limit"),
        ],
        custom_id="vc_ctrl:quickmode",
        row=1,
    )
    async def quick_mode_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        vc, room, err = self._get_room_and_verify_owner(interaction)
        if err or not vc or not room:
            return await interaction.response.send_message(err, ephemeral=True)

        limit = int(select.values[0])
        await vc.edit(user_limit=limit, reason=f"Quick mode selected by {interaction.user}")
        save_user_voice_preset(interaction.user.id, user_limit=limit)
        room["last_active_at"] = int(time.time())
        room["warned_inactive"] = False
        save_temp_voice(room)
        await refresh_voice_control_panel(vc, room)
        label = "Unlimited" if limit == 0 else f"{limit} slots"
        await interaction.response.send_message(f"⚡ Quick Mode applied: **{label}** (saved to preset)!", ephemeral=True)

    @discord.ui.select(
        cls=discord.ui.UserSelect,
        placeholder="✅ Allow / Trust Users (can join even if locked)...",
        min_values=1,
        max_values=5,
        custom_id="vc_ctrl:allow",
        row=2,
    )
    async def allow_users_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        vc, room, err = self._get_room_and_verify_owner(interaction)
        if err or not vc or not room:
            return await interaction.response.send_message(err, ephemeral=True)

        allowed_mentions = []
        for target in select.values:
            if target.bot:
                continue
            await vc.set_permissions(target, connect=True, view_channel=True, reason="Trusted by room owner")
            allowed_mentions.append(target.mention)

        room["last_active_at"] = int(time.time())
        room["warned_inactive"] = False
        save_temp_voice(room)
        if not allowed_mentions:
            return await interaction.response.send_message("⚠️ Please select human members.", ephemeral=True)
        await interaction.response.send_message(f"✅ Granted access to: {', '.join(allowed_mentions)}", ephemeral=True)

    @discord.ui.select(
        cls=discord.ui.UserSelect,
        placeholder="🚫 Kick & Block Users from connecting...",
        min_values=1,
        max_values=5,
        custom_id="vc_ctrl:block",
        row=3,
    )
    async def block_users_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        vc, room, err = self._get_room_and_verify_owner(interaction)
        if err or not vc or not room:
            return await interaction.response.send_message(err, ephemeral=True)

        blocked_mentions = []
        for target in select.values:
            if target.bot or target.id == room["owner_id"]:
                continue
            await vc.set_permissions(target, connect=False, reason="Blocked by room owner")
            if isinstance(target, discord.Member) and target in vc.members:
                try:
                    await target.move_to(None, reason="Kicked by room owner")
                except Exception:
                    pass
            blocked_mentions.append(target.mention)

        room["last_active_at"] = int(time.time())
        room["warned_inactive"] = False
        save_temp_voice(room)
        if not blocked_mentions:
            return await interaction.response.send_message("⚠️ Cannot block yourself or bots.", ephemeral=True)
        await interaction.response.send_message(f"🚫 Kicked & blocked: {', '.join(blocked_mentions)}", ephemeral=True)

    @discord.ui.button(label="Watch / Music", style=discord.ButtonStyle.success, emoji="🎬", custom_id="vc_ctrl:activity", row=4)
    async def watch_together_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not isinstance(interaction.channel, discord.VoiceChannel):
            return
        vc = interaction.channel
        touch_temp_voice_activity(vc.id)
        await interaction.response.defer(ephemeral=True)
        try:
            invite_url = await get_or_create_watch_together_invite(vc, interaction.user)
            await interaction.followup.send(
                f"🎬 **YouTube / Music Watch Together** is ready for this room!\n"
                f"Click here to launch the shared player: {invite_url}",
                ephemeral=True,
            )
        except RuntimeError as e:
            if str(e) == "SERVER_INVITE_CAP_REACHED":
                await interaction.followup.send(
                    "⚠️ This server has reached Discord's **1,000 active invites** limit.\n"
                    "You can still start **Watch Together (YouTube)** right now by clicking the **🚀 Rocket (Activities)** icon at the bottom of your voice channel controls!",
                    ephemeral=True,
                )
            else:
                await interaction.followup.send(f"❌ Could not launch Watch Together: `{e}`", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(
                f"❌ Could not launch Watch Together (`Create Invite` or `Use Activities` permission needed): `{e}`",
                ephemeral=True,
            )

    @discord.ui.button(label="Transfer", style=discord.ButtonStyle.secondary, emoji="👑", custom_id="vc_ctrl:transfer", row=4)
    async def transfer_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        vc, room, err = self._get_room_and_verify_owner(interaction)
        if err or not vc or not room:
            return await interaction.response.send_message(err, ephemeral=True)
        await interaction.response.send_message(
            "👑 Choose a member inside your room to transfer ownership:",
            view=VoiceTransferView(vc, room),
            ephemeral=True,
        )

    @discord.ui.button(label="Reset Preset", style=discord.ButtonStyle.secondary, emoji="🔄", custom_id="vc_ctrl:resetpreset", row=4)
    async def reset_preset_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        reset_user_voice_preset(interaction.user.id)
        await interaction.response.send_message(
            "🔄 Your personal room preset has been reset to default!",
            ephemeral=True,
        )

    @discord.ui.button(label="Delete Room", style=discord.ButtonStyle.danger, emoji="🗑️", custom_id="vc_ctrl:delete", row=4)
    async def delete_room_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        vc, room, err = self._get_room_and_verify_owner(interaction)
        if err or not vc or not room:
            return await interaction.response.send_message(err, ephemeral=True)
        await interaction.response.send_message("🗑️ Closing voice room...", ephemeral=True)
        await delete_temp_voice_channel(vc, reason=f"Closed by owner {interaction.user}")


async def handle_join_to_create(member: discord.Member, trigger_channel: discord.VoiceChannel):
    guild = member.guild
    now = time.time()

    for ch_id, room in list(TEMP_VOICE_CACHE.items()):
        if room["guild_id"] == guild.id and room["owner_id"] == member.id:
            existing_vc = guild.get_channel(ch_id)
            if isinstance(existing_vc, discord.VoiceChannel):
                try:
                    await member.move_to(existing_vc, reason="Moved to existing temporary voice room")
                    touch_temp_voice_activity(existing_vc.id)
                except Exception:
                    pass
                return
            else:
                remove_temp_voice_record(ch_id)

    last_created = VOICE_CREATE_COOLDOWNS.get(member.id, 0.0)
    if now - last_created < 15.0:
        return
    VOICE_CREATE_COOLDOWNS[member.id] = now

    preset = get_user_voice_preset(member.id)
    default_locked = preset["default_locked"]
    default_hidden = preset["default_hidden"]
    user_limit = preset["user_limit"]
    room_name = (preset["custom_name"] or f"🔊 {member.display_name}'s Room")[:95]

    everyone_ow = discord.PermissionOverwrite()
    if default_locked:
        everyone_ow.connect = False
    if default_hidden:
        everyone_ow.view_channel = False

    overwrites = {
        guild.default_role: everyone_ow,
        guild.me: discord.PermissionOverwrite(
            view_channel=True,
            connect=True,
            manage_channels=True,
            move_members=True,
            send_messages=True,
        ),
        member: discord.PermissionOverwrite(
            view_channel=True,
            connect=True,
            manage_channels=True,
        ),
    }

    bitrate = min(guild.bitrate_limit, 256000) if member.premium_since is not None else 64000

    try:
        new_vc = await guild.create_voice_channel(
            name=room_name,
            category=trigger_channel.category,
            user_limit=user_limit,
            bitrate=bitrate,
            overwrites=overwrites,
            reason=f"Join-to-Create room for {member}",
        )
    except Exception as e:
        print(f"[TempVoice] Failed to create voice channel: {e}")
        return

    try:
        await member.move_to(new_vc, reason="Moved to newly created voice room")
    except Exception:
        try:
            await new_vc.delete(reason="User disconnected before move")
        except Exception:
            pass
        return

    now_ts = int(time.time())
    room_data = {
        "channel_id": new_vc.id,
        "guild_id": guild.id,
        "owner_id": member.id,
        "control_message_id": 0,
        "created_at": now_ts,
        "last_active_at": now_ts,
        "last_renamed_at": 0,
        "is_locked": default_locked,
        "is_hidden": default_hidden,
        "warned_inactive": False,
    }
    save_temp_voice(room_data)

    try:
        ctrl_msg = await new_vc.send(
            content=f"👋 Welcome {member.mention}!",
            embed=build_voice_control_embed(new_vc, room_data),
            view=VoiceControlView(),
        )
        room_data["control_message_id"] = ctrl_msg.id
        save_temp_voice(room_data)
    except Exception:
        pass


# -------------------------------------------------------------
# 9. Post Studio Presets & Builders
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
# 10. Live Interactive Polls System (/poll)
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
# 11. Event Scheduling System (/event, /event_image, Follow)
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
# 12. Starboard / Ideas Showcase System (O(1) Deduplication)
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
# 13. Admin Control Panel (/panel) & Sub-Panels
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
        await interaction.response.edit_message(
            embed=await build_panel_embed(interaction.guild),
            view=ControlPanelView(),
        )


class ValueUrlModal(discord.ui.Modal, title="Set Value List Website URL"):
    url_input = discord.ui.TextInput(
        label="Official Value List URL",
        style=discord.TextStyle.short,
        required=True,
        max_length=300,
        placeholder="https://cameraman-td.vaultedvaluesx.com/value-list",
    )

    def __init__(self, current_url: str):
        super().__init__()
        self.url_input.default = current_url

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild:
            return
        url = self.url_input.value.strip()
        if not url.startswith(("http://", "https://")):
            return await interaction.response.send_message("❌ Enter a valid `https://` URL.", ephemeral=True)
        update_guild_settings(interaction.guild.id, value_list_url=url)
        await interaction.response.send_message(f"✅ Value list URL updated to: {url}", ephemeral=True)


class ValueSetupView(discord.ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=180)
        self.guild_id = guild_id

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="📊 Select Value Changes Channel...",
        row=0,
    )
    async def select_value_channel(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        chosen = select.values[0]
        update_guild_settings(self.guild_id, value_channel_id=chosen.id)
        await interaction.response.edit_message(
            content=f"✅ Value Changelog channel set to <#{chosen.id}>.",
            view=self,
        )

    @discord.ui.select(
        cls=discord.ui.RoleSelect,
        placeholder="🏷️ Select Value Changes Ping Role...",
        row=1,
    )
    async def select_value_role(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        chosen = select.values[0]
        update_guild_settings(self.guild_id, value_ping_role_id=chosen.id)
        await interaction.response.edit_message(
            content=f"✅ Value Changes ping role set to <@&{chosen.id}>.",
            view=self,
        )

    @discord.ui.button(label="Set Site URL", style=discord.ButtonStyle.secondary, emoji="🔗", row=2)
    async def set_url_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        cur_url = get_guild_settings(self.guild_id)["value_list_url"]
        await interaction.response.send_modal(ValueUrlModal(cur_url))

    @discord.ui.button(label="Sync Website Now", style=discord.ButtonStyle.secondary, emoji="🔄", row=2)
    async def sync_web_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        site_url = get_guild_settings(self.guild_id)["value_list_url"]
        count = await sync_value_list_from_website(site_url)
        await interaction.followup.send(
            f"🔄 Synced **{count}** items from `{site_url}`! Total units in RAM cache: **{len(VALUE_ITEMS_CACHE)}**.",
            ephemeral=True,
        )

    @discord.ui.button(label="Post Value Hub", style=discord.ButtonStyle.primary, emoji="📌", row=2)
    async def post_hub_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.channel, discord.TextChannel):
            return
        site_url = get_guild_settings(self.guild_id)["value_list_url"]
        embed = discord.Embed(
            title="💎 Official CMTD Value List",
            description=(
                "Check live unit values, demand, and stability right here in Discord or open the full website!\n\n"
                "• Click **`🔍 Check Unit Value`** below to look up any unit privately.\n"
                "• Or type `/value` in chat with instant autocomplete."
            ),
            color=0x3498DB,
        )
        embed.set_footer(
            text=f"{interaction.guild.name} • {len(VALUE_ITEMS_CACHE)} Tracked Units",
            icon_url=interaction.guild.icon.url if interaction.guild.icon else None,
        )
        sent = await interaction.channel.send(embed=embed, view=ValueHubView(site_url))
        await interaction.response.send_message(
            f"📌 Value List Hub posted! [Jump to Message]({sent.jump_url})",
            ephemeral=True,
        )


class TempVoiceSetupView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=180)

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.voice],
        placeholder="🔊 Select Join-to-Create Trigger Voice Channel...",
        row=0,
    )
    async def select_jtc_channel(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        if not interaction.guild:
            return
        chosen = select.values[0]
        update_guild_settings(interaction.guild.id, jtc_channel_id=chosen.id)
        await interaction.response.edit_message(
            content=f"✅ Join-to-Create trigger set to <#{chosen.id}>! Rooms auto-save presets and delete after 5m of inactivity.",
            view=self,
        )

    @discord.ui.button(label="Disable Temp Voice", style=discord.ButtonStyle.danger, emoji="🧹", row=1)
    async def disable_jtc_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild:
            return
        update_guild_settings(interaction.guild.id, jtc_channel_id=None)
        await interaction.response.edit_message(
            content="🧹 Join-to-Create Private Voice Channels disabled.",
            view=None,
        )


class AntiScamSetupView(discord.ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=180)
        self.guild_id = guild_id

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="📋 Select Security Log Channel (Optional)...",
        row=0,
    )
    async def select_log_channel(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        chosen = select.values[0]
        update_guild_settings(self.guild_id, antiscam_log_channel_id=chosen.id)
        await interaction.response.edit_message(
            content=f"📋 Anti-Scam security log channel set to <#{chosen.id}>.",
            view=self,
        )

    @discord.ui.select(
        placeholder="⏱️ Select Auto-Timeout Duration for Scammers...",
        options=[
            discord.SelectOption(label="Delete Message Only (No Timeout)", value="0"),
            discord.SelectOption(label="1 Hour Timeout + Delete", value="60"),
            discord.SelectOption(label="24 Hours Timeout + Delete", value="1440"),
        ],
        row=1,
    )
    async def select_timeout(self, interaction: discord.Interaction, select: discord.ui.Select):
        mins = int(select.values[0])
        update_guild_settings(self.guild_id, antiscam_timeout_mins=mins)
        label = "Delete only" if mins == 0 else f"{mins} minutes timeout"
        await interaction.response.edit_message(
            content=f"⏱️ Anti-Scam punishment updated to: **{label}**.",
            view=self,
        )

    @discord.ui.button(label="Enable Shield", style=discord.ButtonStyle.success, emoji="🛡️", row=2)
    async def enable_shield_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        update_guild_settings(self.guild_id, antiscam_enabled=True)
        await interaction.response.edit_message(
            content="🛡️ **Anti-Scam & Phishing Shield ENABLED!** Fake Roblox/Discord scam links will be deleted automatically.",
            view=self,
        )

    @discord.ui.button(label="Disable Shield", style=discord.ButtonStyle.danger, emoji="🛑", row=2)
    async def disable_shield_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        update_guild_settings(self.guild_id, antiscam_enabled=False)
        await interaction.response.edit_message(
            content="🛑 **Anti-Scam & Phishing Shield DISABLED.**",
            view=self,
        )


class LockdownSetupView(discord.ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=180)
        self.guild_id = guild_id

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text, discord.ChannelType.category],
        placeholder="📋 Toggle Channel or Category in Lockdown Whitelist...",
        row=0,
    )
    async def toggle_ignore_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        chosen = select.values[0]
        is_cat = chosen.type == discord.ChannelType.category
        added = toggle_lockdown_whitelist(self.guild_id, chosen.id, "category" if is_cat else "channel")
        status = "Added to" if added else "Removed from"
        await interaction.response.edit_message(
            content=f"📋 **{status}** lockdown whitelist: <#{chosen.id}> (`{'Category' if is_cat else 'Channel'}`).",
            view=self,
        )


async def build_panel_embed(guild: discord.Guild) -> discord.Embed:
    s = get_guild_settings(guild.id)
    ideas = f"<#{s['ideas_channel_id']}>" if s["ideas_channel_id"] else "`Not Set`"
    showcase = f"<#{s['showcase_channel_id']}>" if s["showcase_channel_id"] else "`Not Set`"
    wfl = f"<#{s['wfl_channel_id']}>" if s["wfl_channel_id"] else "`Not Set`"
    jtc = f"<#{s['jtc_channel_id']}>" if s["jtc_channel_id"] else "`Not Set`"
    val_ch = f"<#{s['value_channel_id']}>" if s["value_channel_id"] else "`Not Set`"
    val_role = f"<@&{s['value_ping_role_id']}>" if s["value_ping_role_id"] else "`Not Set`"
    booster = f"<@&{s['booster_role_id']}>" if s["booster_role_id"] else "`Not Set`"
    autorole = f"<@&{s['autorole_id']}>" if s["autorole_id"] else "`Not Set`"

    detected_wfl = await resolve_wfl_emojis(guild)
    if len(detected_wfl) == 3:
        wfl_status = " ".join(str(e) for e in detected_wfl)
    else:
        found_names = {e.name.lower() for e in detected_wfl}
        missing = [n for n in WFL_EMOJI_NAMES if n not in found_names]
        wfl_status = f"`Missing: {', '.join(missing)}`"

    shield_status = "🟢 **Active**" if s["antiscam_enabled"] else "⚪ `Disabled`"
    if s["antiscam_enabled"] and s["antiscam_log_channel_id"]:
        shield_status += f" (<#{s['antiscam_log_channel_id']}>)"

    lock_status = "🔴 **LOCKED DOWN**" if is_lockdown_active(guild.id) else "🟢 Normal"

    embed = discord.Embed(
        title="⚙️ Server Control Panel",
        description="Configure Starboard, W/F/L reactions, Value System, Voice 2.0, Anti-Scam & Lockdown below.",
        color=0x5865F2,
    )
    embed.add_field(name="💡 Ideas Channel", value=ideas, inline=True)
    embed.add_field(name="🌟 Showcase Channel", value=showcase, inline=True)
    embed.add_field(name="⭐ Star Threshold", value=f"**{s['star_threshold']}** stars", inline=True)
    embed.add_field(name="⚖️ W/F/L Channel", value=wfl, inline=True)
    embed.add_field(name="🎯 W/F/L Emojis", value=wfl_status, inline=True)
    embed.add_field(name="🔊 Voice 2.0 Trigger", value=jtc, inline=True)
    embed.add_field(name="💎 Value Channel", value=val_ch, inline=True)
    embed.add_field(name="🏷️ Value Ping Role", value=val_role, inline=True)
    embed.add_field(name="📦 Tracked Units", value=f"`{len(VALUE_ITEMS_CACHE)}` units", inline=True)
    embed.add_field(name="🛡️ Anti-Scam Shield", value=shield_status, inline=True)
    embed.add_field(name="🔒 Server Lockdown", value=lock_status, inline=True)
    embed.add_field(name="🚀 Booster / Auto-Role", value=f"{booster} / {autorole}", inline=True)
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
        await interaction.response.edit_message(embed=await build_panel_embed(interaction.guild), view=self)

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="🌟 Select Showcase Channel...",
        row=1,
    )
    async def select_showcase(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        update_guild_settings(interaction.guild.id, showcase_channel_id=select.values[0].id)
        await interaction.response.edit_message(embed=await build_panel_embed(interaction.guild), view=self)

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="⚖️ Select W/F/L Auto-Reaction Channel...",
        row=2,
    )
    async def select_wfl(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        WFL_EMOJI_CACHE.pop(interaction.guild.id, None)
        update_guild_settings(interaction.guild.id, wfl_channel_id=select.values[0].id)
        await interaction.response.edit_message(embed=await build_panel_embed(interaction.guild), view=self)

    @discord.ui.select(
        cls=discord.ui.RoleSelect,
        placeholder="👋 Select Join Auto-Role (Optional)...",
        row=3,
    )
    async def select_autorole(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        update_guild_settings(interaction.guild.id, autorole_id=select.values[0].id)
        await interaction.response.edit_message(embed=await build_panel_embed(interaction.guild), view=self)

    @discord.ui.button(label="Value List", style=discord.ButtonStyle.primary, emoji="💎", row=4)
    async def setup_value_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild:
            return
        s = get_guild_settings(interaction.guild.id)
        await interaction.response.send_message(
            f"💎 **Value List & Changelog System**\n"
            f"• **Website:** {s['value_list_url']}\n"
            f"• **Tracked Units:** `{len(VALUE_ITEMS_CACHE)}`\n"
            "Configure your changelog channel, ping role, or post the pinned Value Hub below:",
            view=ValueSetupView(interaction.guild.id),
            ephemeral=True,
        )

    @discord.ui.button(label="Voice 2.0", style=discord.ButtonStyle.secondary, emoji="🔊", row=4)
    async def setup_voice_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "🔊 **Private Voice Rooms 2.0 Setup**\n"
            "Select a trigger voice channel below. Rooms auto-save personal presets, support Quick Modes & Watch Together, and auto-delete after 5m of inactivity:",
            view=TempVoiceSetupView(),
            ephemeral=True,
        )

    @discord.ui.button(label="Anti-Scam", style=discord.ButtonStyle.secondary, emoji="🛡️", row=4)
    async def setup_antiscam_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild:
            return
        s = get_guild_settings(interaction.guild.id)
        state_txt = "ENABLED" if s["antiscam_enabled"] else "DISABLED"
        await interaction.response.send_message(
            f"🛡️ **Anti-Scam & Phishing Shield** (Currently: **{state_txt}**)\n"
            "Blocks fake Roblox/Discord scam links in `<0.1ms` without affecting staff or official links:",
            view=AntiScamSetupView(interaction.guild.id),
            ephemeral=True,
        )

    @discord.ui.button(label="Lockdown", style=discord.ButtonStyle.secondary, emoji="🔒", row=4)
    async def setup_lockdown_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild:
            return
        await interaction.response.send_message(
            "🔒 **Lockdown Whitelist Configuration**\n"
            "Use `/closeall` to safely lock open public channels (read-only channels like `#announcements` are automatically preserved) and `/openall` to restore them.\n"
            "Select any extra open channels or categories below that `/closeall` should **never** touch:",
            view=LockdownSetupView(interaction.guild.id),
            ephemeral=True,
        )

    @discord.ui.button(label="Stars / Post", style=discord.ButtonStyle.secondary, emoji="⭐", row=4)
    async def set_threshold_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ThresholdModal())


# -------------------------------------------------------------
# 14. Slash Commands & Context Menus
# -------------------------------------------------------------
async def unit_name_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    q = normalize_unit_key(current)
    matches: List[Dict[str, Any]] = []
    seen_keys: Set[str] = set()

    for item in VALUE_ITEMS_CACHE.values():
        if not q or q in item["item_key"] or q in item["aliases"]:
            if item["item_key"] not in seen_keys:
                seen_keys.add(item["item_key"])
                matches.append(item)
        if len(matches) >= 25:
            break

    return [
        app_commands.Choice(
            name=f"{m['name']} — {m['value_text']} ({m['category']})"[:100],
            value=m["name"][:100],
        )
        for m in matches
    ]


@bot.tree.command(name="value", description="Check the current value, demand & stability of any unit")
@app_commands.describe(
    unit="Unit name or shortcut (e.g. UTTV, UTC, Pharaoh Cameraman)",
    public="Show the card publicly in chat (default: False / only visible to you)",
)
@app_commands.autocomplete(unit=unit_name_autocomplete)
async def value_cmd(interaction: discord.Interaction, unit: str, public: bool = False):
    item = find_value_item(unit)
    if not item:
        return await interaction.response.send_message(
            f"❌ Unit `{unit}` not found in the value list.",
            ephemeral=True,
        )
    site_url = get_guild_settings(interaction.guild.id)["value_list_url"] if interaction.guild else DEFAULT_VALUE_LIST_URL
    embed = build_unit_value_embed(interaction.guild, item, site_url)
    view = build_link_view("Open Full Value List", site_url)
    await interaction.response.send_message(embed=embed, view=view, ephemeral=not public)


@bot.tree.command(name="value_post", description="Publish formatted value list changes & auto-update the bot's value database")
@app_commands.default_permissions(manage_messages=True)
async def value_post_cmd(interaction: discord.Interaction):
    await interaction.response.send_modal(ValueChangelogModal())


@bot.tree.command(name="panel", description="Open the server configuration panel")
@app_commands.default_permissions(administrator=True)
async def panel_cmd(interaction: discord.Interaction):
    if not interaction.guild:
        return
    await interaction.response.send_message(
        embed=await build_panel_embed(interaction.guild),
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
    embed.add_field(name="💎 Value Units", value=f"`{len(VALUE_ITEMS_CACHE)}`", inline=True)
    embed.add_field(name="🌟 Showcased Ideas", value=f"`{total_ideas}`", inline=True)
    embed.add_field(name="📊 Active Polls", value=f"`{active_polls}`", inline=True)
    embed.add_field(name="🔊 Active Temp Voices", value=f"`{len(TEMP_VOICE_CACHE)}`", inline=True)
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
# 15. Event Listeners & Background Scheduler
# -------------------------------------------------------------
@bot.event
async def on_ready():
    print(f"[Online] Logged in as {bot.user} (ID: {bot.user.id}) | Guilds: {len(bot.guilds)}")
    for guild in bot.guilds:
        s = get_guild_settings(guild.id)
        if s.get("wfl_channel_id"):
            await resolve_wfl_emojis(guild)

    synced_units = await sync_value_list_from_website(DEFAULT_VALUE_LIST_URL)
    if synced_units:
        print(f"[ValueList] Synced {synced_units} items from website (Total cached: {len(VALUE_ITEMS_CACHE)})")

    now_ts = int(time.time())
    for ch_id, room in list(TEMP_VOICE_CACHE.items()):
        guild = bot.get_guild(room["guild_id"])
        ch = guild.get_channel(ch_id) if guild else None
        if not isinstance(ch, discord.VoiceChannel):
            remove_temp_voice_record(ch_id)
            continue
        humans = [m for m in ch.members if not m.bot]
        if not humans or (now_ts - room["last_active_at"] >= VOICE_INACTIVITY_SECONDS):
            await delete_temp_voice_channel(ch, reason="Startup cleanup of empty/inactive temp voice")


@bot.event
async def on_guild_emojis_update(guild: discord.Guild, before, after):
    WFL_EMOJI_CACHE.pop(guild.id, None)


@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot:
        return

    guild = member.guild
    settings = get_guild_settings(guild.id)
    jtc_id = settings.get("jtc_channel_id")

    if before.channel and before.channel.id in TEMP_VOICE_CACHE:
        humans_left = [m for m in before.channel.members if not m.bot]
        if len(humans_left) == 0:
            await delete_temp_voice_channel(before.channel, reason="All members left temporary voice channel")
        else:
            touch_temp_voice_activity(before.channel.id)

    if after.channel and after.channel.id in TEMP_VOICE_CACHE:
        touch_temp_voice_activity(after.channel.id)

    if jtc_id and after.channel and after.channel.id == jtc_id:
        if not before.channel or before.channel.id != jtc_id:
            await handle_join_to_create(member, after.channel)


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    if message.guild:
        settings = get_guild_settings(message.guild.id)

        if await handle_antiscam_check(message, settings):
            return

        if message.channel.id in TEMP_VOICE_CACHE:
            touch_temp_voice_activity(message.channel.id)

        if settings["ideas_channel_id"] and message.channel.id == settings["ideas_channel_id"]:
            try:
                await message.add_reaction("⭐")
            except Exception:
                pass

        if settings["wfl_channel_id"] and message.channel.id == settings["wfl_channel_id"]:
            asyncio.create_task(apply_wfl_reactions(message))

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

    # 1. Periodic 15-minute website refresh for Value List
    if time.time() - LAST_VALUE_WEB_SYNC >= 900:
        asyncio.create_task(sync_value_list_from_website(DEFAULT_VALUE_LIST_URL))

    # 2. Check temporary voice rooms: empty, 4-min warning, or 5-min inactivity deletion
    for ch_id, room in list(TEMP_VOICE_CACHE.items()):
        guild = bot.get_guild(room["guild_id"])
        ch = guild.get_channel(ch_id) if guild else None
        if not isinstance(ch, discord.VoiceChannel):
            remove_temp_voice_record(ch_id)
            continue

        humans = [m for m in ch.members if not m.bot]
        idle_sec = now_ts - room["last_active_at"]

        if len(humans) == 0:
            await delete_temp_voice_channel(ch, reason="Temporary voice channel empty")
        elif idle_sec >= VOICE_INACTIVITY_SECONDS:
            await delete_temp_voice_channel(ch, reason="Temporary voice channel inactive for 5 minutes")
        elif idle_sec >= VOICE_WARNING_SECONDS and not room.get("warned_inactive"):
            room["warned_inactive"] = True
            save_temp_voice(room)
            try:
                await ch.send(
                    content=f"⏳ <@{room['owner_id']}> This room has been quiet for 4 minutes and will auto-close in **1 minute** unless there is activity or you click below:",
                    view=VoiceKeepAliveView(),
                )
            except Exception:
                pass

    # 3. Check expired polls
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

    # 4. Check scheduled events reminders & start
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
            start_ts = start_ts or 0
            rem_min = rem_min or 10
            if start_ts <= 0:
                continue
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


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.CommandNotFound):
        return
    raise error


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    text = f"⚠️ Error: `{error}`"
    if interaction.response.is_done():
        await interaction.followup.send(text, ephemeral=True)
    else:
        await interaction.response.send_message(text, ephemeral=True)


# -------------------------------------------------------------
# 16. Entrypoint
# -------------------------------------------------------------
if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN") or os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("Missing DISCORD_TOKEN environment variable!")
    bot.run(token)
