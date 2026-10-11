import os
import sys
import time
import subprocess
import sqlite3
from typing import Optional

# -------------------------------------------------------------
# 1. Automatic dependency check and installation
# -------------------------------------------------------------
try:
    import discord
    from discord import app_commands
    from discord.ext import commands
except ImportError:
    print("[Setup] The discord.py library was not found. Installing...")
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
# 2. SQLite Database with persistent storage support
# -------------------------------------------------------------
DATA_DIR = os.getenv("DATA_DIR", ".")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "bot_database.db")

START_TIME = time.time()


def get_db():
    return sqlite3.connect(DB_PATH)


def init_db():
    """Synchronous database table initialization prior to logging in."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER PRIMARY KEY,
                ideas_channel_id INTEGER,
                showcase_channel_id INTEGER,
                star_threshold INTEGER DEFAULT 12,
                booster_role_id INTEGER
            )
            """
        )
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
            "CREATE INDEX IF NOT EXISTS idx_showcase_msg ON starboard_entries(showcase_message_id)"
        )
        conn.commit()
    print(f"[Database] Initialization completed successfully: {DB_PATH}")


init_db()


def get_guild_settings(guild_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT ideas_channel_id, showcase_channel_id, star_threshold, booster_role_id
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
            }
        return {
            "ideas_channel_id": None,
            "showcase_channel_id": None,
            "star_threshold": 12,
            "booster_role_id": None,
        }


def update_guild_settings(guild_id: int, **kwargs):
    current = get_guild_settings(guild_id)
    current.update(kwargs)
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO guild_settings (guild_id, ideas_channel_id, showcase_channel_id, star_threshold, booster_role_id)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
                ideas_channel_id = excluded.ideas_channel_id,
                showcase_channel_id = excluded.showcase_channel_id,
                star_threshold = excluded.star_threshold,
                booster_role_id = excluded.booster_role_id
            """,
            (
                guild_id,
                current["ideas_channel_id"],
                current["showcase_channel_id"],
                current["star_threshold"],
                current["booster_role_id"],
            ),
        )
        conn.commit()


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


def save_starboard_entry(orig_message_id: int, showcase_message_id: int, guild_id: int, channel_id: int, stars_count: int):
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
    status=discord.Status.online
)


# -------------------------------------------------------------
# 4. Showcase Cards & Visual Styling (Embed & UI)
# -------------------------------------------------------------
def get_tier_info(stars: int):
    """Returns dynamic color scheme, tier title, and icon based on stars."""
    if stars >= 50:
        return 0x9333EA, "🟣 LEGENDARY IDEA", "💫"
    elif stars >= 25:
        return 0xF97316, "🔥 TRENDING HIT", "⚡"
    elif stars >= 18:
        return 0xEAB308, "🌟 COMMUNITY CHOICE", "✨"
    else:
        return 0x38BDF8, "⭐ APPROVED IDEA", "⭐"


def build_showcase_embed(message: discord.Message, total_stars: int) -> discord.Embed:
    color, tier_title, tier_icon = get_tier_info(total_stars)

    embed = discord.Embed(
        title=f"{tier_icon} {tier_title}",
        color=color,
        timestamp=message.created_at
    )

    # Clean markdown blockquote for original idea content
    content = message.content.strip() if message.content else "*[Media attachment without text]*"
    embed.description = f">>> {content}" if len(content) < 1800 else content

    # Author header
    embed.set_author(
        name=f"{message.author.display_name} (@{message.author.name})",
        icon_url=message.author.display_avatar.url
    )

    # Metadata badges
    created_ts = int(message.created_at.timestamp())
    embed.add_field(
        name="📊 Support",
        value=f"**{total_stars}** ⭐ (total)",
        inline=True
    )
    embed.add_field(
        name="📍 Channel",
        value=message.channel.mention,
        inline=True
    )
    embed.add_field(
        name="📅 Submitted",
        value=f"<t:{created_ts}:R>",
        inline=True
    )

    # Attached images support
    if message.attachments:
        for att in message.attachments:
            if att.content_type and att.content_type.startswith("image/"):
                embed.set_image(url=att.url)
                break

    embed.set_footer(
        text=f"CMTD Ideas • Idea ID: {message.id}",
        icon_url=bot.user.display_avatar.url if bot.user else None
    )
    return embed


def build_jump_view(message: discord.Message, total_stars: int) -> discord.ui.View:
    view = discord.ui.View(timeout=None)
    view.add_item(
        discord.ui.Button(
            label="Jump to Original",
            style=discord.ButtonStyle.link,
            url=message.jump_url,
            emoji="🔗",
        )
    )
    view.add_item(
        discord.ui.Button(
            label=f"{total_stars} ⭐",
            style=discord.ButtonStyle.secondary,
            disabled=True,
            custom_id="counter_badge"
        )
    )
    return view


# -------------------------------------------------------------
# 5. Interactive Control Panel (/panel)
# -------------------------------------------------------------
class ThresholdModal(discord.ui.Modal, title="Star Threshold"):
    stars = discord.ui.TextInput(
        label="Minimum stars required for showcase",
        placeholder="e.g. 12",
        default="12",
        min_length=1,
        max_length=3,
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
            return await interaction.response.send_message(
                "❌ Please enter a positive integer greater than zero.", ephemeral=True
            )

        update_guild_settings(self.guild_id, star_threshold=val)
        await interaction.response.send_message(
            f"✅ Showcase star threshold updated to: **{val} ⭐**", ephemeral=True
        )


class ControlPanelView(discord.ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=None)
        self.guild_id = guild_id

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="💡 Select ideas submission channel...",
        row=0,
    )
    async def select_ideas(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        ch = select.values[0]
        update_guild_settings(self.guild_id, ideas_channel_id=ch.id)
        await interaction.response.send_message(
            f"✅ Ideas channel set to: {ch.mention}", ephemeral=True
        )

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="🏆 Select showcase channel for top ideas...",
        row=1,
    )
    async def select_showcase(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        ch = select.values[0]
        update_guild_settings(self.guild_id, showcase_channel_id=ch.id)
        await interaction.response.send_message(
            f"✅ Showcase channel set to: {ch.mention}", ephemeral=True
        )

    @discord.ui.select(
        cls=discord.ui.RoleSelect,
        placeholder="💎 Select booster role for auto-revocation...",
        row=2,
    )
    async def select_role(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role = select.values[0]
        update_guild_settings(self.guild_id, booster_role_id=role.id)
        await interaction.response.send_message(
            f"✅ Monitored booster role set to: {role.mention}", ephemeral=True
        )

    @discord.ui.button(label="Change Star Threshold", style=discord.ButtonStyle.primary, emoji="⭐", row=3)
    async def change_threshold(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ThresholdModal(self.guild_id))


# -------------------------------------------------------------
# 6. Reaction Handling and Two-Way Sync
# -------------------------------------------------------------
@bot.event
async def on_message(message: discord.Message):
    if not message.guild or message.author.bot:
        return

    settings = get_guild_settings(message.guild.id)
    # Automatically add a ⭐ reaction to any new post in the ideas channel
    if settings["ideas_channel_id"] and message.channel.id == settings["ideas_channel_id"]:
        try:
            await message.add_reaction("⭐")
        except discord.DiscordException:
            pass

    await bot.process_commands(message)


def count_message_stars(msg: discord.Message) -> int:
    """Counts ⭐ reactions present on a message."""
    for react in msg.reactions:
        if str(react.emoji) == "⭐":
            return react.count
    return 0


async def handle_star_sync(payload: discord.RawReactionActionEvent):
    """Synchronizes reactions between the ideas channel and the showcase channel."""
    # Ignore the bot's own reactions
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
    orig_msg_id = None
    showcase_msg_id = None

    # Case 1: Reaction added/removed inside the ideas channel
    if payload.channel_id == ideas_ch_id:
        orig_msg_id = payload.message_id
        try:
            orig_msg = await ideas_channel.fetch_message(orig_msg_id)
        except (discord.NotFound, discord.Forbidden):
            return

        entry = get_entry_by_orig(orig_msg_id)
        if entry:
            showcase_msg_id = entry[0]
            try:
                showcase_msg = await showcase_channel.fetch_message(showcase_msg_id)
            except (discord.NotFound, discord.Forbidden):
                showcase_msg = None

    # Case 2: Reaction added/removed directly on the showcase card
    elif payload.channel_id == showcase_ch_id:
        showcase_msg_id = payload.message_id
        entry = get_entry_by_showcase(showcase_msg_id)
        if not entry:
            return
        orig_msg_id = entry[0]

        try:
            showcase_msg = await showcase_channel.fetch_message(showcase_msg_id)
            orig_msg = await ideas_channel.fetch_message(orig_msg_id)
        except (discord.NotFound, discord.Forbidden):
            return
    else:
        return

    # Total reactions across both locations
    orig_stars = count_message_stars(orig_msg) if orig_msg else 0
    # The bot adds 1 initial reaction to the showcase card, so subtract it for accurate user count
    showcase_stars = max(0, count_message_stars(showcase_msg) - 1) if showcase_msg else 0
    total_stars = orig_stars + showcase_stars

    # First-time publication when threshold is reached
    if total_stars >= threshold and not showcase_msg:
        embed = build_showcase_embed(orig_msg, total_stars)
        view = build_jump_view(orig_msg, total_stars)
        new_showcase = await showcase_channel.send(embed=embed, view=view)
        # Pre-seed the showcase card with a star reaction so members can vote directly
        try:
            await new_showcase.add_reaction("⭐")
        except discord.DiscordException:
            pass
        save_starboard_entry(orig_msg.id, new_showcase.id, payload.guild_id, ideas_ch_id, total_stars)

    # Update existing card
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
# 7. Booster Role Revocation System
# -------------------------------------------------------------
@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    settings = get_guild_settings(after.guild.id)
    booster_role_id = settings["booster_role_id"]
    if not booster_role_id:
        return

    role = after.guild.get_role(booster_role_id)
    if not role or role not in after.roles:
        return

    # If member has no remaining active boosts
    if after.premium_since is None:
        try:
            await after.remove_roles(role, reason="CMTD: Server boost expired (auto-removal)")
            print(f"[Boosters] Revoked role '{role.name}' from {after.display_name}")
        except discord.Forbidden:
            print(f"[Error] The bot's role is below '{role.name}' in the server hierarchy!")
        except Exception as e:
            print(f"[Booster Revocation Error] {e}")


# -------------------------------------------------------------
# 8. Slash Commands & Administration
# -------------------------------------------------------------
@bot.tree.command(name="panel", description="Open the interactive bot control panel")
@app_commands.default_permissions(administrator=True)
async def panel_command(interaction: discord.Interaction):
    settings = get_guild_settings(interaction.guild.id)
    ideas_ch = f"<#{settings['ideas_channel_id']}>" if settings["ideas_channel_id"] else "🔴 *(not set)*"
    showcase_ch = f"<#{settings['showcase_channel_id']}>" if settings["showcase_channel_id"] else "🔴 *(not set)*"
    role_info = f"<@&{settings['booster_role_id']}>" if settings["booster_role_id"] else "🔴 *(not set)*"

    embed = discord.Embed(
        title="⚙️ CMTD Ideas Control Center",
        description="Select server channels and parameters using the dropdown menus below:",
        color=0x5865F2,
    )
    embed.add_field(name="💡 Ideas Channel", value=ideas_ch, inline=True)
    embed.add_field(name="🏆 Showcase Channel", value=showcase_ch, inline=True)
    embed.add_field(name="⭐ Star Threshold", value=f"**{settings['star_threshold']}**", inline=True)
    embed.add_field(name="💎 Monitored Booster Role", value=role_info, inline=False)
    embed.set_footer(text="CMTD Ideas • Changes take effect immediately")

    await interaction.response.send_message(
        embed=embed, view=ControlPanelView(interaction.guild.id), ephemeral=True
    )


@bot.tree.command(name="stats", description="Display bot performance and statistics")
async def stats_command(interaction: discord.Interaction):
    """Summary overview command for administrators and community members."""
    uptime_sec = int(time.time() - START_TIME)
    hours, remainder = divmod(uptime_sec, 3600)
    minutes, seconds = divmod(remainder, 60)
    uptime_str = f"{hours}h {minutes}m {seconds}s"

    latency_ms = round(bot.latency * 1000)
    ideas_count = get_total_ideas_count(interaction.guild.id)

    embed = discord.Embed(
        title="📊 CMTD Ideas Stats",
        color=0x38BDF8
    )
    embed.add_field(name="⚡ Latency (Ping)", value=f"`{latency_ms} ms`", inline=True)
    embed.add_field(name="⏳ Uptime", value=f"`{uptime_str}`", inline=True)
    embed.add_field(name="🏆 Showcase Ideas", value=f"**{ideas_count}**", inline=True)
    embed.set_footer(text="Bot is online and synchronizing reactions in real-time.")

    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.command(name="sync")
@commands.has_permissions(administrator=True)
async def sync_guild(ctx: commands.Context):
    """Force instant slash command synchronization for this server."""
    bot.tree.copy_global_to(guild=ctx.guild)
    synced = await bot.tree.sync(guild=ctx.guild)
    await ctx.send(f"⚡ Instantly synced {len(synced)} command(s) for this server!")


@bot.event
async def on_ready():
    print(f"[Bot Online] Logged in as {bot.user} (ID: {bot.user.id})")
    try:
        synced = await bot.tree.sync()
        print(f"[Commands] Synced slash commands globally: {len(synced)}")
    except Exception as e:
        print(f"[Sync Error] {e}")


# -------------------------------------------------------------
# 9. Startup Entrypoint
# -------------------------------------------------------------
if __name__ == "__main__":
    TOKEN = os.getenv("DISCORD_TOKEN")
    if not TOKEN:
        print("[CRITICAL ERROR] DISCORD_TOKEN environment variable is not defined!")
        sys.exit(1)

    try:
        bot.run(TOKEN)
    except Exception as e:
        print(f"[Startup Error] {e}")
