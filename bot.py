import os
import sys
import subprocess
import datetime
import sqlite3

# Автоматическая проверка и доустановка зависимостей
def ensure_package(package_name: str, import_name: str = None):
    module_to_check = import_name or package_name
    try:
        __import__(module_to_check)
    except ImportError:
        print(f"[Установка] Библиотека '{package_name}' не найдена. Устанавливаем...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
            print(f"[Установка] Библиотека '{package_name}' успешно установлена!")
        except Exception as err:
            print(f"[Ошибка] Сбой установки {package_name}: {err}")
            sys.exit(1)

ensure_package("discord.py", "discord")

import discord
from discord import app_commands
from discord.ext import commands

# Токен бота (поддерживает запуск как из переменной окружения, так и прямое значение)
TOKEN = os.getenv("DISCORD_TOKEN") or "MTU1NzY3NDgxOTUwMjM0NjMxMQ.GBQdOE.COam3N1jaIKBgvDK9soDvrVwt_tsDaYdPPKgsY"

# Настройка интентов шлюза
intents = discord.Intents.default()
intents.members = True          # Отслеживание бустов участников
intents.message_content = True  # Чтение текста для идей
intents.reactions = True        # Отслеживание звёзд

bot = commands.Bot(command_prefix="!", intents=intents)

# База данных SQLite (с поддержкой постоянного хранилища хостинга DATA_DIR)
DATA_DIR = os.getenv("DATA_DIR", ".")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "bot_database.db")

def init_db():
    """Инициализация базы данных и создание таблиц."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER PRIMARY KEY,
                ideas_channel_id INTEGER,
                showcase_channel_id INTEGER,
                star_threshold INTEGER DEFAULT 12,
                booster_role_id INTEGER
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS starboard_posts (
                original_message_id INTEGER PRIMARY KEY,
                showcase_message_id INTEGER,
                channel_id INTEGER,
                guild_id INTEGER,
                stars_count INTEGER DEFAULT 0
            )
        """)
        conn.commit()

def get_guild_settings(guild_id: int):
    """Получение параметров сервера."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ideas_channel_id, showcase_channel_id, star_threshold, booster_role_id
            FROM guild_settings WHERE guild_id = ?
        """, (guild_id,))
        row = cursor.fetchone()
        if row:
            return {
                "ideas_channel_id": row[0],
                "showcase_channel_id": row[1],
                "star_threshold": row[2] if row[2] is not None else 12,
                "booster_role_id": row[3]
            }
        return {
            "ideas_channel_id": None,
            "showcase_channel_id": None,
            "star_threshold": 12,
            "booster_role_id": None
        }

def update_guild_setting(guild_id: int, key: str, value):
    """Обновление одного параметра конфигурации."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO guild_settings (guild_id) VALUES (?)", (guild_id,))
        cursor.execute(f"UPDATE guild_settings SET {key} = ? WHERE guild_id = ?", (value, guild_id))
        conn.commit()

def get_starboard_entry_by_orig(orig_id: int):
    """Поиск записи витрины по оригинальному сообщению."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT showcase_message_id, channel_id, stars_count
            FROM starboard_posts WHERE original_message_id = ?
        """, (orig_id,))
        return cursor.fetchone()

def get_starboard_entry_by_showcase(showcase_id: int):
    """Поиск оригинального сообщения по сообщению в витрине."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT original_message_id, channel_id, guild_id, stars_count
            FROM starboard_posts WHERE showcase_message_id = ?
        """, (showcase_id,))
        return cursor.fetchone()

def save_starboard_entry(orig_id: int, showcase_id: int, channel_id: int, guild_id: int, stars: int):
    """Сохранение новой опубликованной идеи."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO starboard_posts 
            (original_message_id, showcase_message_id, channel_id, guild_id, stars_count)
            VALUES (?, ?, ?, ?, ?)
        """, (orig_id, showcase_id, channel_id, guild_id, stars))
        conn.commit()

def get_tier_design(stars: int):
    """Определение палитры и статуса в зависимости от числа звёзд."""
    if stars >= 50:
        return 0xA855F7, "🟣 ЛЕГЕНДАРНАЯ ИДЕЯ", "💫"
    elif stars >= 25:
        return 0xFF4500, "🔥 ПОПУЛЯРНАЯ ИДЕЯ", "⚡"
    elif stars >= 18:
        return 0xF59E0B, "🌟 ИЗБРАННАЯ ИДЕЯ", "✨"
    else:
        return 0xEAB308, "⭐ ОДОБРЕННАЯ ИДЕЯ", "⭐"

def create_showcase_embed(message: discord.Message, stars_count: int) -> discord.Embed:
    """Генерация стильного премиального эмбеда идеи."""
    color, tier_title, tier_icon = get_tier_design(stars_count)
    
    embed = discord.Embed(
        title=f"{tier_icon} {tier_title}",
        color=color,
        timestamp=message.created_at
    )
    
    # Текст сообщения
    text_content = message.content.strip() if message.content else "*[Автор прикрепил медиа без текста]*"
    embed.description = f"```quote\n{text_content}\n```" if len(text_content) < 1800 else text_content

    # Автор идеи
    embed.set_author(
        name=f"{message.author.display_name} (@{message.author.name})",
        icon_url=message.author.display_avatar.url
    )

    # Статистика и метаданные
    embed.add_field(
        name="📊 Поддержка комьюнити",
        value=f"**{stars_count}** {tier_icon} (в канале {message.channel.mention})",
        inline=True
    )
    embed.add_field(
        name="👤 Автор",
        value=message.author.mention,
        inline=True
    )

    # Обработка вложений
    image_attached = False
    other_files = []

    if message.attachments:
        for attachment in message.attachments:
            ext = attachment.filename.lower()
            if not image_attached and any(ext.endswith(img_ext) for img_ext in [".png", ".jpg", ".jpeg", ".gif", ".webp"]):
                embed.set_image(url=attachment.url)
                image_attached = True
            else:
                other_files.append(f"📎 [{attachment.filename}]({attachment.url})")

    if other_files:
        embed.add_field(
            name="📁 Прикреплённые файлы",
            value="\n".join(other_files[:4]),
            inline=False
        )

    embed.set_footer(
        text=f"CMTD • ID идеи: {message.id}",
        icon_url=bot.user.display_avatar.url if bot.user else None
    )
    return embed

class ShowcaseView(discord.ui.View):
    """Интерактивные кнопки под карточкой идеи."""
    def __init__(self, jump_url: str, stars: int):
        super().__init__(timeout=None)
        # Кнопка перехода к оригиналу
        self.add_item(discord.ui.Button(
            label="Перейти к оригиналу",
            style=discord.ButtonStyle.link,
            url=jump_url,
            emoji="🔗"
        ))
        # Индикатор звёзд
        self.add_item(discord.ui.Button(
            label=f"{stars} звёзд",
            style=discord.ButtonStyle.secondary,
            disabled=True,
            emoji="⭐"
        ))

class ThresholdModal(discord.ui.Modal, title="Порог звёзд"):
    threshold_input = discord.ui.TextInput(
        label="Минимальное число звёзд",
        placeholder="Например: 12",
        default="12",
        min_length=1,
        max_length=3
    )

    def __init__(self, guild_id: int, on_updated_callback):
        super().__init__()
        self.guild_id = guild_id
        self.on_updated_callback = on_updated_callback

    async def on_submit(self, interaction: discord.Interaction):
        try:
            val = int(self.threshold_input.value)
            if val < 1:
                raise ValueError
        except ValueError:
            await interaction.response.send_message("❌ Введи положительное целое число (минимум 1).", ephemeral=True)
            return

        update_guild_setting(self.guild_id, "star_threshold", val)
        await self.on_updated_callback(interaction)

class ConfigPanelView(discord.ui.View):
    """Интерактивная панель управления настройками сервера."""
    def __init__(self, guild_id: int):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.refresh_components()

    def refresh_components(self):
        self.clear_items()
        
        # Селектор канала идей
        ideas_select = discord.ui.ChannelSelect(
            placeholder="💡 Выбери канал для приёма идей...",
            channel_types=[discord.ChannelType.text],
            min_values=1,
            max_values=1,
            custom_id="ideas_select",
            row=0
        )
        ideas_select.callback = self.on_ideas_selected
        self.add_item(ideas_select)

        # Селектор канала витрины
        showcase_select = discord.ui.ChannelSelect(
            placeholder="🏆 Выбери канал-витрину для лучших...",
            channel_types=[discord.ChannelType.text],
            min_values=1,
            max_values=1,
            custom_id="showcase_select",
            row=1
        )
        showcase_select.callback = self.on_showcase_selected
        self.add_item(showcase_select)

        # Селектор роли бустера
        role_select = discord.ui.RoleSelect(
            placeholder="💎 Выбери роль бустера (для авто-снятия)...",
            min_values=1,
            max_values=1,
            custom_id="role_select",
            row=2
        )
        role_select.callback = self.on_role_selected
        self.add_item(role_select)

        # Кнопка изменения порога
        thresh_btn = discord.ui.Button(
            label="Изменить порог звёзд",
            style=discord.ButtonStyle.primary,
            emoji="⭐",
            row=3
        )
        thresh_btn.callback = self.on_threshold_click
        self.add_item(thresh_btn)

        # Кнопка сброса/обновления
        refresh_btn = discord.ui.Button(
            label="Обновить статус",
            style=discord.ButtonStyle.secondary,
            emoji="🔄",
            row=3
        )
        refresh_btn.callback = self.on_refresh_click
        self.add_item(refresh_btn)

    async def on_ideas_selected(self, interaction: discord.Interaction):
        selected_ch = interaction.data["values"][0]
        update_guild_setting(self.guild_id, "ideas_channel_id", int(selected_ch))
        await self.update_panel_response(interaction, f"✅ Канал приёма идей изменён на <#{selected_ch}>")

    async def on_showcase_selected(self, interaction: discord.Interaction):
        selected_ch = interaction.data["values"][0]
        update_guild_setting(self.guild_id, "showcase_channel_id", int(selected_ch))
        await self.update_panel_response(interaction, f"✅ Канал-витрина изменён на <#{selected_ch}>")

    async def on_role_selected(self, interaction: discord.Interaction):
        selected_role = interaction.data["values"][0]
        update_guild_setting(self.guild_id, "booster_role_id", int(selected_role))
        await self.update_panel_response(interaction, f"✅ Роль бустера изменена на <@&{selected_role}>")

    async def on_threshold_click(self, interaction: discord.Interaction):
        await interaction.response.send_modal(
            ThresholdModal(self.guild_id, lambda inter: self.update_panel_response(inter, "✅ Порог звёзд успешно обновлён!"))
        )

    async def on_refresh_click(self, interaction: discord.Interaction):
        await self.update_panel_response(interaction, "🔄 Панель синхронизирована с базой данных.")

    async def update_panel_response(self, interaction: discord.Interaction, notice: str):
        embed = render_settings_embed(self.guild_id, notice)
        if interaction.response.is_done():
            await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=self)
        else:
            await interaction.response.edit_message(embed=embed, view=self)

def render_settings_embed(guild_id: int, notice: str = None) -> discord.Embed:
    """Генерация красивого меню настроек сервера."""
    settings = get_guild_settings(guild_id)
    
    ideas = f"<#{settings['ideas_channel_id']}>" if settings["ideas_channel_id"] else "🔴 *Не настроен*"
    showcase = f"<#{settings['showcase_channel_id']}>" if settings["showcase_channel_id"] else "🔴 *Не настроен*"
    role = f"<@&{settings['booster_role_id']}>" if settings["booster_role_id"] else "🔴 *Не выбрана*"
    threshold = settings["star_threshold"]

    embed = discord.Embed(
        title="⚙️ Панель управления CMTD Bot",
        description=(
            "Настрой бота в 1 клик с помощью селекторов и кнопок ниже.\n"
            "Все изменения сохраняются в базу данных автоматически."
        ),
        color=0x5865F2,
        timestamp=datetime.datetime.now(datetime.timezone.utc)
    )

    embed.add_field(name="💡 Канал приёма идей", value=ideas, inline=True)
    embed.add_field(name="🏆 Канал-витрина", value=showcase, inline=True)
    embed.add_field(name="⭐ Порог попадания", value=f"**{threshold}** звёзд", inline=True)
    embed.add_field(name="💎 Контролируемая роль бустера", value=role, inline=False)

    if notice:
        embed.add_field(name="Статус последнего действия", value=f"*{notice}*", inline=False)

    embed.set_footer(text="CMTD Control Center • Доступно только Администраторам")
    return embed

@bot.event
async def on_message(message: discord.Message):
    """Автоматическая постановка начальной звёздочки на идею."""
    if message.author.bot or not message.guild:
        return

    settings = get_guild_settings(message.guild.id)
    if settings["ideas_channel_id"] and message.channel.id == settings["ideas_channel_id"]:
        try:
            await message.add_reaction("⭐")
        except discord.DiscordException as err:
            print(f"[Starboard] Не удалось добавить авто-реакцию: {err}")

    await bot.process_commands(message)

async def process_star_sync(payload: discord.RawReactionActionEvent):
    """Двусторонняя синхронизация звёзд между каналом идей и витриной."""
    if str(payload.emoji) != "⭐" or not payload.guild_id:
        return

    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return

    settings = get_guild_settings(guild.id)
    ideas_id = settings["ideas_channel_id"]
    showcase_id = settings["showcase_channel_id"]
    threshold = settings["star_threshold"]

    if not ideas_id or not showcase_id:
        return

    showcase_channel = guild.get_channel(showcase_id)
    ideas_channel = guild.get_channel(ideas_id)
    if not showcase_channel or not ideas_channel:
        return

    # Случай А: Реакция поставлена/убрана в канале идей
    if payload.channel_id == ideas_id:
        try:
            orig_msg = await ideas_channel.fetch_message(payload.message_id)
        except (discord.NotFound, discord.Forbidden):
            return

        star_react = discord.utils.get(orig_msg.reactions, emoji="⭐")
        stars_count = star_react.count if star_react else 0

        post = get_starboard_entry_by_orig(orig_msg.id)

        # Публикуем впервые при достижении порога
        if stars_count >= threshold and not post:
            embed = create_showcase_embed(orig_msg, stars_count)
            view = ShowcaseView(orig_msg.jump_url, stars_count)
            showcase_msg = await showcase_channel.send(embed=embed, view=view)
            await showcase_msg.add_reaction("⭐")
            save_starboard_entry(orig_msg.id, showcase_msg.id, ideas_channel.id, guild.id, stars_count)

        # Уже в витрине -> обновляем счётчик и дизайн
        elif post:
            showcase_msg_id = post[0]
            try:
                showcase_msg = await showcase_channel.fetch_message(showcase_msg_id)
                new_embed = create_showcase_embed(orig_msg, stars_count)
                new_view = ShowcaseView(orig_msg.jump_url, stars_count)
                await showcase_msg.edit(embed=new_embed, view=new_view)
                save_starboard_entry(orig_msg.id, showcase_msg_id, ideas_channel.id, guild.id, stars_count)
            except (discord.NotFound, discord.Forbidden):
                pass

    # Случай Б: Реакция поставлена/убрана на карточке в витрине
    elif payload.channel_id == showcase_id:
        match = get_starboard_entry_by_showcase(payload.message_id)
        if not match:
            return

        orig_id, _, _, _ = match
        try:
            orig_msg = await ideas_channel.fetch_message(orig_id)
            showcase_msg = await showcase_channel.fetch_message(payload.message_id)

            orig_react = discord.utils.get(orig_msg.reactions, emoji="⭐")
            showcase_react = discord.utils.get(showcase_msg.reactions, emoji="⭐")

            orig_stars = orig_react.count if orig_react else 0
            # -1 вычитается, так как одну стартовую звёздочку ставит сам бот
            showcase_stars = max(0, (showcase_react.count - 1)) if showcase_react else 0
            total_stars = orig_stars + showcase_stars

            new_embed = create_showcase_embed(orig_msg, total_stars)
            new_view = ShowcaseView(orig_msg.jump_url, total_stars)
            await showcase_msg.edit(embed=new_embed, view=new_view)
            save_starboard_entry(orig_msg.id, showcase_msg.id, ideas_channel.id, guild.id, total_stars)
        except (discord.NotFound, discord.Forbidden):
            pass

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    await process_star_sync(payload)

@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    await process_star_sync(payload)

@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    """Снятие роли, если у участника истекли абсолютно все бусты сервера."""
    if before.premium_since is not None and after.premium_since is None:
        settings = get_guild_settings(after.guild.id)
        role_id = settings["booster_role_id"]
        if not role_id:
            return

        role = after.guild.get_role(role_id)
        if role and role in after.roles:
            try:
                await after.remove_roles(role, reason="CMTD: Все бусты сервера истекли (авто-снятие)")
                print(f"[Бустеры] ✅ Роль '{role.name}' успешно снята у участника {after} ({after.id}).")
            except discord.Forbidden:
                print(f"[Ошибка] ❌ Недостаточно прав! Роль бота должна быть ВЫШЕ роли '{role.name}' в настройках сервера!")
            except Exception as e:
                print(f"[Ошибка] Не удалось снять роль: {e}")

@bot.tree.command(name="panel", description="Открыть интерактивную панель управления ботом")
@app_commands.default_permissions(administrator=True)
async def panel_command(interaction: discord.Interaction):
    """Интерактивная панель управления с селекторами."""
    embed = render_settings_embed(interaction.guild_id)
    view = ConfigPanelView(interaction.guild_id)
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

@bot.tree.command(name="setup_ideas", description="Быстрая настройка каналов идей и витрины")
@app_commands.default_permissions(administrator=True)
async def setup_ideas(
    interaction: discord.Interaction,
    ideas_channel: discord.TextChannel,
    showcase_channel: discord.TextChannel,
    threshold: int = 12
):
    update_guild_setting(interaction.guild_id, "ideas_channel_id", ideas_channel.id)
    update_guild_setting(interaction.guild_id, "showcase_channel_id", showcase_channel.id)
    update_guild_setting(interaction.guild_id, "star_threshold", threshold)

    embed = discord.Embed(
        title="✅ Конфигурация идей сохранена",
        color=0x22C55E,
        description=(
            f"• **Приём идей:** {ideas_channel.mention}\n"
            f"• **Витрина лучших:** {showcase_channel.mention}\n"
            f"• **Порог для публикации:** **{threshold}** ⭐"
        )
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="setup_booster", description="Указать роль бустера для автоматического снятия")
@app_commands.default_permissions(administrator=True)
async def setup_booster(interaction: discord.Interaction, role: discord.Role):
    update_guild_setting(interaction.guild_id, "booster_role_id", role.id)
    embed = discord.Embed(
        title="💎 Роль бустера установлена",
        color=0xEC4899,
        description=(
            f"Выбранная роль: {role.mention}\n\n"
            f"*Бот снимет эту роль автоматически, когда у человека закончатся все бусты сервера.*"
        )
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="config", description="Показать текущую сводку настроек")
@app_commands.default_permissions(administrator=True)
async def config(interaction: discord.Interaction):
    embed = render_settings_embed(interaction.guild_id)
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.event
async def on_ready():
    init_db()
    try:
        synced = await bot.tree.sync()
        sync_text = f"Синхронизировано команд: {len(synced)}"
    except Exception as e:
        sync_text = f"Ошибка синхронизации: {e}"

    activity = discord.Activity(type=discord.ActivityType.watching, name="за крутыми идеями ⭐")
    await bot.change_presence(status=discord.Status.online, activity=activity)

    banner = f"""
    ╔═══════════════════════════════════════════════════════════════╗
    ║                 ⭐ CMTD BOT v2.0 В СЕТИ!                     ║
    ║                                                               ║
    ║  Авторизован: {str(bot.user):<47} ║
    ║  {sync_text:<60} ║
    ║  База данных: bot_database.db [OK]                            ║
    ║  Интерфейс:   Используй /panel на сервере                     ║
    ╚═══════════════════════════════════════════════════════════════╝
    """
    print(banner)

if __name__ == "__main__":
    try:
        bot.run(TOKEN)
    except discord.errors.PrivilegedIntentsRequired:
        print("\n" + "=" * 65)
        print("❌ [КРИТИЧЕСКАЯ НАСТРОЙКА DISCORD] НЕ ВКЛЮЧЕНЫ INTENTS!")
        print("=" * 65)
        print("1. Зайди на https://discord.com/developers/applications")
        print("2. Выбери своего бота -> вкладка 'Bot' слева.")
        print("3. Прокрути страницу вниз до 'Privileged Gateway Intents'.")
        print("4. Включи
