import os
import sys
import subprocess
import asyncio
import sqlite3
from typing import Optional

# -------------------------------------------------------------
# 1. Автоматическая проверка и установка зависимостей
# -------------------------------------------------------------
try:
    import discord
    from discord import app_commands
    from discord.ext import commands
except ImportError:
    print("[Установка] Установка discord.py...")
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
# 2. Настройка путей и базы данных SQLite
# -------------------------------------------------------------
DATA_DIR = os.getenv("DATA_DIR", ".")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "bot_database.db")


def get_db():
    return sqlite3.connect(DB_PATH)


def init_db():
    """Создаёт необходимые таблицы до подключения шлюза."""
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
                showcase_message_id INTEGER,
                guild_id INTEGER,
                channel_id INTEGER,
                stars_count INTEGER DEFAULT 0
            )
            """
        )
        conn.commit()
    print(f"[База данных] Таблицы успешно проверены/созданы в: {DB_PATH}")


# Инициализация сразу при старте скрипта
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


def get_starboard_entry(orig_message_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT showcase_message_id, stars_count FROM starboard_entries WHERE orig_message_id = ?",
            (orig_message_id,),
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


# -------------------------------------------------------------
# 3. Инициализация Discord Bot
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
# 4. Визуальные карточки витрины (Showcase Embed)
# -------------------------------------------------------------
def build_showcase_embed(message: discord.Message, stars: int) -> discord.Embed:
    if stars >= 25:
        color = 0x9B59B6
        rank_badge = "🟣 МЕГА-ИДЕЯ"
    elif stars >= 18:
        color = 0xE67E22
        rank_badge = "🔥 ВЫБОР СООБЩЕСТВА"
    else:
        color = 0xF1C40F
        rank_badge = "⭐ ТОПОВАЯ ИДЕЯ"

    embed = discord.Embed(
        title=f"{rank_badge} | {stars} Звёзд",
        description=message.content or "*[Вложение без текста]*",
        color=color,
        timestamp=message.created_at,
    )
    embed.set_author(
        name=f"{message.author.display_name} (@{message.author.name})",
        icon_url=message.author.display_avatar.url,
    )

    if message.attachments:
        first = message.attachments[0]
        if first.content_type and first.content_type.startswith("image/"):
            embed.set_image(url=first.url)

    embed.set_footer(text=f"ID автора: {message.author.id} • Канал: #{message.channel.name}")
    return embed


def build_jump_view(message: discord.Message, stars: int) -> discord.ui.View:
    view = discord.ui.View()
    view.add_item(
        discord.ui.Button(
            label="Перейти к оригиналу",
            style=discord.ButtonStyle.link,
            url=message.jump_url,
            emoji="🔗",
        )
    )
    view.add_item(
        discord.ui.Button(
            label=f"{stars} звёзд",
            style=discord.ButtonStyle.secondary,
            disabled=True,
            emoji="⭐",
        )
    )
    return view


# -------------------------------------------------------------
# 5. Интерактивная Панель Управления (/panel)
# -------------------------------------------------------------
class ThresholdModal(discord.ui.Modal, title="Настройка порога звёзд"):
    stars = discord.ui.TextInput(
        label="Количество звёзд для витрины",
        placeholder="Например: 12",
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
                "❌ Пожалуйста, введи корректное целое число больше нуля.", ephemeral=True
            )

        update_guild_settings(self.guild_id, star_threshold=val)
        await interaction.response.send_message(
            f"✅ Порог для витрины установлен на: **{val} ⭐**", ephemeral=True
        )


class ControlPanelView(discord.ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=None)
        self.guild_id = guild_id

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="💡 Выбери канал для сбора предложений/идей",
        row=0,
    )
    async def select_ideas(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        ch = select.values[0]
        update_guild_settings(self.guild_id, ideas_channel_id=ch.id)
        await interaction.response.send_message(
            f"✅ Канал предложений установлен: {ch.mention}", ephemeral=True
        )

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="🏆 Выбери канал-витрину для лучших идей",
        row=1,
    )
    async def select_showcase(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        ch = select.values[0]
        update_guild_settings(self.guild_id, showcase_channel_id=ch.id)
        await interaction.response.send_message(
            f"✅ Канал витрины установлен: {ch.mention}", ephemeral=True
        )

    @discord.ui.select(
        cls=discord.ui.RoleSelect,
        placeholder="🚀 Выбери контролируемую роль бустера",
        row=2,
    )
    async def select_role(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role = select.values[0]
        update_guild_settings(self.guild_id, booster_role_id=role.id)
        await interaction.response.send_message(
            f"✅ Контролируемая роль бустера: {role.mention}", ephemeral=True
        )

    @discord.ui.button(label="Изменить порог звёзд", style=discord.ButtonStyle.primary, emoji="⭐", row=3)
    async def change_threshold(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ThresholdModal(self.guild_id))


# -------------------------------------------------------------
# 6. События Discord (Events)
# -------------------------------------------------------------
@bot.event
async def on_ready():
    print(f"[Бот запущен] Авторизован как {bot.user} (ID: {bot.user.id})")
    try:
        synced = await bot.tree.sync()
        print(f"[Команды] Синхронизировано слэш-команд: {len(synced)}")
    except Exception as e:
        print(f"[Ошибка синхронизации] {e}")


@bot.event
async def on_message(message: discord.Message):
    if not message.guild or message.author.bot:
        return

    settings = get_guild_settings(message.guild.id)
    if settings["ideas_channel_id"] and message.channel.id == settings["ideas_channel_id"]:
        try:
            await message.add_reaction("⭐")
        except discord.Forbidden:
            print(f"[Предупреждение] Нет прав на реакцию ⭐ в #{message.channel.name}")
        except Exception as e:
            print(f"[Ошибка реакции] {e}")

    await bot.process_commands(message)


async def handle_star_update(payload: discord.RawReactionActionEvent):
    if str(payload.emoji) != "⭐" or not payload.guild_id:
        return

    settings = get_guild_settings(payload.guild_id)
    ideas_channel_id = settings["ideas_channel_id"]
    showcase_channel_id = settings["showcase_channel_id"]
    threshold = settings["star_threshold"]

    if not ideas_channel_id or not showcase_channel_id:
        return
    if payload.channel_id != ideas_channel_id:
        return

    channel = bot.get_channel(payload.channel_id)
    if not channel:
        return

    try:
        orig_msg = await channel.fetch_message(payload.message_id)
    except (discord.NotFound, discord.Forbidden):
        return

    star_count = 0
    for reaction in orig_msg.reactions:
        if str(reaction.emoji) == "⭐":
            star_count = reaction.count
            break

    entry = get_starboard_entry(orig_msg.id)
    showcase_channel = bot.get_channel(showcase_channel_id)
    if not showcase_channel:
        return

    if star_count >= threshold:
        embed = build_showcase_embed(orig_msg, star_count)
        view = build_jump_view(orig_msg, star_count)

        if entry:
            showcase_msg_id = entry[0]
            try:
                target_msg = await showcase_channel.fetch_message(showcase_msg_id)
                await target_msg.edit(embed=embed, view=view)
                save_starboard_entry(orig_msg.id, showcase_msg_id, payload.guild_id, payload.channel_id, star_count)
            except discord.NotFound:
                new_msg = await showcase_channel.send(embed=embed, view=view)
                save_starboard_entry(orig_msg.id, new_msg.id, payload.guild_id, payload.channel_id, star_count)
        else:
            new_msg = await showcase_channel.send(embed=embed, view=view)
            save_starboard_entry(orig_msg.id, new_msg.id, payload.guild_id, payload.channel_id, star_count)


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    await handle_star_update(payload)


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    await handle_star_update(payload)


@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    settings = get_guild_settings(after.guild.id)
    booster_role_id = settings["booster_role_id"]
    if not booster_role_id:
        return

    role = after.guild.get_role(booster_role_id)
    if not role or role not in after.roles:
        return

    # Если участник больше не бустит сервер
    if after.premium_since is None:
        try:
            await after.remove_roles(role, reason="Снятие эксклюзивной роли: буст завершён")
            print(f"[Бустеры] Снята роль {role.name} с пользователя {after.display_name}")
        except discord.Forbidden:
            print(f"[Ошибка] Роль бота ниже роли {role.name}! Перемести роль бота выше в настройках сервера.")
        except Exception as e:
            print(f"[Ошибка снятия роли] {e}")


# -------------------------------------------------------------
# 7. Слэш-команды администрирования
# -------------------------------------------------------------
@bot.tree.command(name="panel", description="Открыть интерактивную панель управления ботом")
@app_commands.default_permissions(administrator=True)
async def panel_command(interaction: discord.Interaction):
    settings = get_guild_settings(interaction.guild.id)
    ideas_ch = f"<#{settings['ideas_channel_id']}>" if settings["ideas_channel_id"] else "*(не задан)*"
    showcase_ch = f"<#{settings['showcase_channel_id']}>" if settings["showcase_channel_id"] else "*(не задан)*"
    role_info = f"<@&{settings['booster_role_id']}>" if settings["booster_role_id"] else "*(не задана)*"

    embed = discord.Embed(
        title="⚙️ Панель управления ботом",
        description="Настрой каналы, порог звёзд и роли с помощью меню ниже:",
        color=0x5865F2,
    )
    embed.add_field(name="💡 Канал предложений", value=ideas_ch, inline=True)
    embed.add_field(name="🏆 Канал витрины", value=showcase_ch, inline=True)
    embed.add_field(name="⭐ Порог звёзд", value=f"**{settings['star_threshold']}**", inline=True)
    embed.add_field(name="🚀 Контроль бустеров", value=role_info, inline=False)
    embed.set_footer(text="Изменения применяются сразу без перезагрузки.")

    await interaction.response.send_message(
        embed=embed, view=ControlPanelView(interaction.guild.id), ephemeral=True
    )


# -------------------------------------------------------------
# 8. Запуск программы
# -------------------------------------------------------------
if __name__ == "__main__":
    TOKEN = os.getenv("DISCORD_TOKEN")
    if not TOKEN:
        print("[КРИТИЧЕСКАЯ ОШИБКА] Переменная DISCORD_TOKEN не найдена в окружении!")
        sys.exit(1)

    try:
        bot.run(TOKEN)
    except discord.errors.PrivilegedIntentsRequired:
        print("\n" + "=" * 60)
        print("[ОШИБКА INTENTS] Включи Gateway Intents на портале Discord:")
        print("1. Зайди на https://discord.com/developers/applications")
        print("2. Выбери бота -> Вкладка 'Bot'")
        print("3. Включи Server Members Intent и Message Content Intent")
        print("4. Нажми 'Save Changes'")
        print("=" * 60 + "\n")
    except Exception as e:
        print(f"[Ошибка старта] {e}")
