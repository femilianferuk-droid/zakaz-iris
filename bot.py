import asyncio
import logging
import os
from typing import Optional

import asyncpg
from aiogram import Bot, Dispatcher, Router, F
from aiogram.enums import ParseMode, ChatMemberStatus
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
)
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
db_pool: Optional[asyncpg.Pool] = None

# ─── Premium эмодзи (только для кнопок) ──────────────────────────────────────

def pe(emoji_id: str, fallback: str = "•") -> str:
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'

E_SETTINGS = pe("5870982283724328568", "⚙")
E_PROFILE  = pe("5870994129244131212", "👤")
E_PEOPLE   = pe("5870772616305839506", "👥")
E_FOLDER   = pe("5870528606328852614", "📁")
E_SMILE    = pe("5870764288364252592", "🙂")
E_CHART    = pe("5870921681735781843", "📊")
E_HOME     = pe("5873147866364514353", "🏘")
E_CHECK    = pe("5870633910337015697", "✅")
E_CROSS    = pe("5870657884844462243", "❌")
E_PENCIL   = pe("5870676941614354370", "🖋")
E_TRASH    = pe("5870875489362513438", "🗑")
E_INFO     = pe("6028435952299413210", "ℹ")
E_BOT      = pe("6030400221232501136", "🤖")
E_WARN     = pe("5984993945214517554", "⚠")
E_TAG      = pe("5886285355279193209", "🏷")
E_BACK     = "◁"
E_HELP     = pe("6028435952299413210", "ℹ")

# ─── FSM ─────────────────────────────────────────────────────────────────────

class CreateTeam(StatesGroup):
    name = State()

class AddGroup(StatesGroup):
    username = State()

class AddModerator(StatesGroup):
    username = State()

# ─── Database ─────────────────────────────────────────────────────────────────

SCHEMA = """
DROP TABLE IF EXISTS punishments CASCADE;
DROP TABLE IF EXISTS team_groups CASCADE;
DROP TABLE IF EXISTS moderators CASCADE;
DROP TABLE IF EXISTS teams CASCADE;
DROP TABLE IF EXISTS users CASCADE;

CREATE TABLE IF NOT EXISTS users (
    user_id    BIGINT PRIMARY KEY,
    username   TEXT,
    full_name  TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS teams (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    owner_id   BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS moderators (
    id       SERIAL PRIMARY KEY,
    team_id  INT    NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    username TEXT   NOT NULL,
    user_id  BIGINT,
    UNIQUE(team_id, username)
);

CREATE TABLE IF NOT EXISTS team_groups (
    id       SERIAL PRIMARY KEY,
    team_id  INT    NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    chat_id  BIGINT NOT NULL,
    username TEXT,
    title    TEXT,
    UNIQUE(team_id, chat_id)
);

CREATE TABLE IF NOT EXISTS punishments (
    id               SERIAL PRIMARY KEY,
    team_id          INT    NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    chat_id          BIGINT NOT NULL,
    target_user_id   BIGINT,
    target_username  TEXT,
    mod_user_id      BIGINT NOT NULL,
    type             TEXT   NOT NULL CHECK (type IN ('log','warn','ban')),
    comment          TEXT,
    active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);
"""

async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    async with db_pool.acquire() as conn:
        await conn.execute(SCHEMA)

async def upsert_user(user_id: int, username: Optional[str], full_name: str):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO users(user_id, username, full_name)
               VALUES($1,$2,$3)
               ON CONFLICT(user_id) DO UPDATE SET username=$2, full_name=$3""",
            user_id, username, full_name
        )
        if username:
            await conn.execute(
                "UPDATE moderators SET user_id=$1 WHERE LOWER(username)=LOWER($2) AND user_id IS NULL",
                user_id, username
            )

# ─── Helpers ─────────────────────────────────────────────────────────────────

async def get_user_teams(user_id: int, username: Optional[str]) -> list:
    async with db_pool.acquire() as conn:
        if username:
            return await conn.fetch(
                """SELECT DISTINCT t.* FROM teams t
                   LEFT JOIN moderators m ON m.team_id = t.id
                   WHERE t.owner_id=$1
                      OR m.user_id=$1
                      OR LOWER(m.username)=LOWER($2)
                   ORDER BY t.created_at""",
                user_id, username
            )
        return await conn.fetch(
            """SELECT DISTINCT t.* FROM teams t
               LEFT JOIN moderators m ON m.team_id = t.id
               WHERE t.owner_id=$1 OR m.user_id=$1
               ORDER BY t.created_at""",
            user_id
        )

async def is_team_owner(team_id: int, user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT owner_id FROM teams WHERE id=$1", team_id)
        return bool(row and row["owner_id"] == user_id)

async def is_team_member(team_id: int, user_id: int, username: Optional[str] = None) -> bool:
    async with db_pool.acquire() as conn:
        if username:
            row = await conn.fetchrow(
                """SELECT 1 FROM teams WHERE id=$1 AND owner_id=$2
                   UNION
                   SELECT 1 FROM moderators WHERE team_id=$1 AND (user_id=$2 OR LOWER(username)=LOWER($3))""",
                team_id, user_id, username
            )
        else:
            row = await conn.fetchrow(
                """SELECT 1 FROM teams WHERE id=$1 AND owner_id=$2
                   UNION
                   SELECT 1 FROM moderators WHERE team_id=$1 AND user_id=$2""",
                team_id, user_id
            )
        return bool(row)

async def get_team_groups(team_id: int) -> list:
    async with db_pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM team_groups WHERE team_id=$1", team_id)

async def get_team_mods(team_id: int) -> list:
    async with db_pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM moderators WHERE team_id=$1", team_id)

async def count_active(team_id: int, chat_id: int, target_user_id: int, ptype: str) -> int:
    async with db_pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT COUNT(*) FROM punishments WHERE team_id=$1 AND chat_id=$2 AND target_user_id=$3 AND type=$4 AND active=TRUE",
            team_id, chat_id, target_user_id, ptype
        )

async def find_team_for_chat(chat_id: int, mod_user_id: int, mod_username: Optional[str] = None) -> Optional[dict]:
    async with db_pool.acquire() as conn:
        if mod_username:
            row = await conn.fetchrow(
                """SELECT tg.team_id, tg.id AS group_id
                   FROM team_groups tg
                   WHERE tg.chat_id=$1
                     AND (
                       EXISTS(SELECT 1 FROM teams WHERE id=tg.team_id AND owner_id=$2)
                       OR EXISTS(SELECT 1 FROM moderators WHERE team_id=tg.team_id
                                 AND (user_id=$2 OR LOWER(username)=LOWER($3)))
                     )""",
                chat_id, mod_user_id, mod_username
            )
        else:
            row = await conn.fetchrow(
                """SELECT tg.team_id, tg.id AS group_id
                   FROM team_groups tg
                   WHERE tg.chat_id=$1
                     AND (
                       EXISTS(SELECT 1 FROM teams WHERE id=tg.team_id AND owner_id=$2)
                       OR EXISTS(SELECT 1 FROM moderators WHERE team_id=tg.team_id AND user_id=$2)
                     )""",
                chat_id, mod_user_id
            )
        return dict(row) if row else None

async def check_bot_permissions(chat_id: int) -> tuple[bool, str]:
    try:
        me = await bot.get_me()
        member = await bot.get_chat_member(chat_id, me.id)
        if member.status != ChatMemberStatus.ADMINISTRATOR:
            return False, "❌ Бот должен быть администратором группы."
        if not getattr(member, "can_restrict_members", False):
            return False, "❌ Боту нужно право ограничивать участников."
        if not getattr(member, "can_delete_messages", False):
            return False, "❌ Боту нужно право удалять сообщения."
        return True, ""
    except Exception as e:
        return False, f"❌ Не удалось проверить права: {e}"

# ─── Keyboards ───────────────────────────────────────────────────────────────

def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [
            KeyboardButton(text="Создать команду", icon_custom_emoji_id="5870772616305839506"),
            KeyboardButton(text="Мои команды",     icon_custom_emoji_id="5870528606328852614"),
        ],
        [
            KeyboardButton(text="Профиль", icon_custom_emoji_id="5870994129244131212"),
            KeyboardButton(text="Помощь",  icon_custom_emoji_id="6028435952299413210"),
        ],
    ], resize_keyboard=True)

def back_kb(callback: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"{E_BACK} Назад", callback_data=callback)
    ]])

def teams_kb(teams: list) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(
        text=t["name"], callback_data=f"team:{t['id']}",
        icon_custom_emoji_id="5870528606328852614"
    )] for t in teams]
    rows.append([InlineKeyboardButton(text=f"{E_BACK} Назад", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def team_menu_kb(team_id: int, is_owner: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="Группы",     callback_data=f"groups:{team_id}", icon_custom_emoji_id="5870772616305839506")],
        [InlineKeyboardButton(text="Модераторы", callback_data=f"mods:{team_id}",   icon_custom_emoji_id="5891207662678317861")],
    ]
    if is_owner:
        rows.append([InlineKeyboardButton(text="Удалить команду", callback_data=f"del_team:{team_id}", icon_custom_emoji_id="5870657884844462243")])
    rows.append([InlineKeyboardButton(text=f"{E_BACK} Назад", callback_data="my_teams")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def groups_kb(team_id: int, groups: list) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(
        text=g["title"] or g["username"] or str(g["chat_id"]),
        callback_data=f"group_info:{team_id}:{g['id']}",
        icon_custom_emoji_id="5870772616305839506"
    )] for g in groups]
    if len(groups) < 10:
        rows.append([InlineKeyboardButton(text="Добавить группу", callback_data=f"add_group:{team_id}", icon_custom_emoji_id="5870633910337015697")])
    rows.append([InlineKeyboardButton(text=f"{E_BACK} Назад", callback_data=f"team:{team_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def group_info_kb(team_id: int, group_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Удалить группу", callback_data=f"del_group:{team_id}:{group_id}", icon_custom_emoji_id="5870875489362513438")],
        [InlineKeyboardButton(text=f"{E_BACK} Назад", callback_data=f"groups:{team_id}")],
    ])

def mods_kb(team_id: int, mods: list, is_owner: bool) -> InlineKeyboardMarkup:
    rows = []
    for m in mods:
        label = f"@{m['username']}" + (" ⏳" if m["user_id"] is None else "")
        rows.append([InlineKeyboardButton(
            text=label, callback_data=f"mod_info:{team_id}:{m['id']}",
            icon_custom_emoji_id="5870994129244131212"
        )])
    if is_owner:
        rows.append([InlineKeyboardButton(text="Добавить модератора", callback_data=f"add_mod:{team_id}", icon_custom_emoji_id="5891207662678317861")])
    rows.append([InlineKeyboardButton(text=f"{E_BACK} Назад", callback_data=f"team:{team_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def mod_info_kb(team_id: int, mod_row_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Удалить модератора", callback_data=f"del_mod:{team_id}:{mod_row_id}", icon_custom_emoji_id="5893192487324880883")],
        [InlineKeyboardButton(text=f"{E_BACK} Назад", callback_data=f"mods:{team_id}")],
    ])

# ─── /start ──────────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(msg: Message):
    await upsert_user(msg.from_user.id, msg.from_user.username, msg.from_user.full_name)
    await msg.answer(
        f'{E_SMILE} Привет, <b>{msg.from_user.first_name}</b>!\n\n'
        f'{E_BOT} Я бот для управления модерацией в группах.\n\n'
        f'{E_INFO} Создай команду, добавь группы и модераторов, '
        f'затем используй команды прямо в чате.\n\n'
        f'Нажми <b>«Помощь»</b> чтобы узнать все команды.',
        reply_markup=main_menu_kb()
    )

# ─── Помощь ──────────────────────────────────────────────────────────────────

@router.message(F.text == "Помощь")
async def help_handler(msg: Message):
    text = (
        "ℹ <b>Команды модерации</b>\n"
        "Работают в группах, привязанных к команде.\n\n"

        "📁 <b>/log</b> — выдать лог (3 лога = варн)\n"
        "<code>/log @user спам</code>\n"
        "<code>/log спам</code> (ответом)\n\n"

        "⚠ <b>/warn</b> — выдать варн (3 варна = бан)\n"
        "<code>/warn @user мат</code>\n"
        "<code>/warn мат</code> (ответом)\n\n"

        "🔒 <b>/ban</b> — заблокировать (сообщения удаляются)\n"
        "<code>/ban @user причина</code>\n"
        "<code>/ban причина</code> (ответом)\n\n"

        "🔓 <b>/unlog</b> — снять лог\n"
        "<code>/unlog @user</code>\n\n"

        "🔓 <b>/unwarn</b> — снять варн\n"
        "<code>/unwarn @user</code>\n\n"

        "🔓 <b>/unban</b> — разбанить\n"
        "<code>/unban @user</code>\n\n"

        "📊 <b>/stats</b> — статистика\n"
        "<code>/stats @user</code> или ответом"
    )
    await msg.answer(text, reply_markup=main_menu_kb())

# ─── Профиль ─────────────────────────────────────────────────────────────────

@router.message(F.text == "Профиль")
async def profile_handler(msg: Message):
    await upsert_user(msg.from_user.id, msg.from_user.username, msg.from_user.full_name)
    async with db_pool.acquire() as conn:
        teams_count = await conn.fetchval("SELECT COUNT(*) FROM teams WHERE owner_id=$1", msg.from_user.id)
        mod_count   = await conn.fetchval("SELECT COUNT(*) FROM moderators WHERE user_id=$1", msg.from_user.id)
    await msg.answer(
        f'{E_PROFILE} <b>Профиль</b>\n\n'
        f'{E_TAG} Имя: <b>{msg.from_user.full_name}</b>\n'
        f'{E_TAG} Username: @{msg.from_user.username or "—"}\n'
        f'{E_TAG} ID: <code>{msg.from_user.id}</code>\n\n'
        f'{E_FOLDER} Команд создано: <b>{teams_count}</b>\n'
        f'{E_PEOPLE} Модератор в: <b>{mod_count}</b> командах',
        reply_markup=main_menu_kb()
    )

# ─── Мои команды ─────────────────────────────────────────────────────────────

@router.message(F.text == "Мои команды")
async def my_teams_handler(msg: Message):
    await upsert_user(msg.from_user.id, msg.from_user.username, msg.from_user.full_name)
    teams = await get_user_teams(msg.from_user.id, msg.from_user.username)
    if not teams:
        await msg.answer(
            f'{E_FOLDER} У вас пока нет команд.\n'
            f'Нажмите <b>«Создать команду»</b>, чтобы создать первую!',
            reply_markup=main_menu_kb()
        )
        return
    await msg.answer(
        f'{E_FOLDER} <b>Ваши команды</b>\n\nВыберите команду для управления:',
        reply_markup=teams_kb(teams)
    )

@router.callback_query(F.data == "my_teams")
async def my_teams_cb(cb: CallbackQuery):
    teams = await get_user_teams(cb.from_user.id, cb.from_user.username)
    if not teams:
        await cb.message.edit_text(f'{E_FOLDER} У вас пока нет команд.', reply_markup=back_kb("main_menu"))
        return
    await cb.message.edit_text(
        f'{E_FOLDER} <b>Ваши команды</b>\n\nВыберите команду:',
        reply_markup=teams_kb(teams)
    )
    await cb.answer()

@router.callback_query(F.data == "main_menu")
async def main_menu_cb(cb: CallbackQuery):
    await cb.message.delete()
    await cb.answer()

# ─── Создать команду ─────────────────────────────────────────────────────────

@router.message(F.text == "Создать команду")
async def create_team_handler(msg: Message, state: FSMContext):
    await upsert_user(msg.from_user.id, msg.from_user.username, msg.from_user.full_name)
    async with db_pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM teams WHERE owner_id=$1", msg.from_user.id)
    if count >= 10:
        await msg.answer(f'{E_CROSS} Вы можете создать не более 10 команд.')
        return
    await state.set_state(CreateTeam.name)
    await msg.answer(
        f'{E_PENCIL} Введите название новой команды:',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text=f"{E_BACK} Отмена", callback_data="cancel_state")
        ]])
    )

@router.message(CreateTeam.name)
async def create_team_name(msg: Message, state: FSMContext):
    name = msg.text.strip()
    if len(name) < 2 or len(name) > 64:
        await msg.answer(f'{E_CROSS} Название должно быть от 2 до 64 символов.')
        return
    async with db_pool.acquire() as conn:
        team = await conn.fetchrow(
            "INSERT INTO teams(name, owner_id) VALUES($1,$2) RETURNING id, name",
            name, msg.from_user.id
        )
    await state.clear()
    await msg.answer(
        f'{E_CHECK} Команда <b>«{team["name"]}»</b> создана!\n\n'
        f'{E_INFO} Добавьте группы и модераторов в разделе «Мои команды».',
        reply_markup=main_menu_kb()
    )

# ─── Карточка команды ────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("team:"))
async def team_card(cb: CallbackQuery):
    team_id = int(cb.data.split(":")[1])
    async with db_pool.acquire() as conn:
        team = await conn.fetchrow("SELECT * FROM teams WHERE id=$1", team_id)
    if not team:
        await cb.answer("Команда не найдена.", show_alert=True)
        return
    if not await is_team_member(team_id, cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа.", show_alert=True)
        return
    is_owner = team["owner_id"] == cb.from_user.id
    groups = await get_team_groups(team_id)
    mods   = await get_team_mods(team_id)
    async with db_pool.acquire() as conn:
        owner = await conn.fetchrow("SELECT full_name, username FROM users WHERE user_id=$1", team["owner_id"])
    owner_name = f"@{owner['username']}" if owner and owner["username"] else (owner["full_name"] if owner else "—")
    await cb.message.edit_text(
        f'{E_FOLDER} <b>{team["name"]}</b>\n\n'
        f'{E_PROFILE} Владелец: {owner_name}\n'
        f'{E_PEOPLE} Модераторов: <b>{len(mods)}</b>\n'
        f'{E_HOME} Групп: <b>{len(groups)}</b>/10',
        reply_markup=team_menu_kb(team_id, is_owner)
    )
    await cb.answer()

@router.callback_query(F.data.startswith("del_team:"))
async def del_team(cb: CallbackQuery):
    team_id = int(cb.data.split(":")[1])
    if not await is_team_owner(team_id, cb.from_user.id):
        await cb.answer("Только владелец может удалить команду.", show_alert=True)
        return
    async with db_pool.acquire() as conn:
        team = await conn.fetchrow("SELECT name FROM teams WHERE id=$1", team_id)
        await conn.execute("DELETE FROM teams WHERE id=$1", team_id)
    await cb.message.edit_text(f'{E_CHECK} Команда <b>«{team["name"]}»</b> удалена.', reply_markup=back_kb("my_teams"))
    await cb.answer()

# ─── Группы ──────────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("groups:"))
async def groups_list(cb: CallbackQuery):
    team_id = int(cb.data.split(":")[1])
    if not await is_team_member(team_id, cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа.", show_alert=True)
        return
    groups = await get_team_groups(team_id)
    async with db_pool.acquire() as conn:
        team = await conn.fetchrow("SELECT name FROM teams WHERE id=$1", team_id)
    await cb.message.edit_text(
        f'{E_HOME} <b>Группы команды «{team["name"]}»</b>\n\n'
        f'{E_INFO} Привязано: <b>{len(groups)}</b>/10',
        reply_markup=groups_kb(team_id, groups)
    )
    await cb.answer()

@router.callback_query(F.data.startswith("group_info:"))
async def group_info_cb(cb: CallbackQuery):
    _, team_id, group_id = cb.data.split(":")
    team_id, group_id = int(team_id), int(group_id)
    if not await is_team_member(team_id, cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа.", show_alert=True)
        return
    async with db_pool.acquire() as conn:
        g = await conn.fetchrow("SELECT * FROM team_groups WHERE id=$1 AND team_id=$2", group_id, team_id)
    if not g:
        await cb.answer("Группа не найдена.", show_alert=True)
        return
    await cb.message.edit_text(
        f'{E_HOME} <b>{g["title"] or g["username"] or str(g["chat_id"])}</b>\n\n'
        f'{E_TAG} Username: @{g["username"] or "—"}\n'
        f'{E_TAG} Chat ID: <code>{g["chat_id"]}</code>',
        reply_markup=group_info_kb(team_id, group_id)
    )
    await cb.answer()

@router.callback_query(F.data.startswith("del_group:"))
async def del_group(cb: CallbackQuery):
    _, team_id, group_id = cb.data.split(":")
    team_id, group_id = int(team_id), int(group_id)
    if not await is_team_owner(team_id, cb.from_user.id):
        await cb.answer("Только владелец может удалять группы.", show_alert=True)
        return
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM team_groups WHERE id=$1 AND team_id=$2", group_id, team_id)
    await cb.message.edit_text(f'{E_CHECK} Группа удалена.', reply_markup=back_kb(f"groups:{team_id}"))
    await cb.answer()

@router.callback_query(F.data.startswith("add_group:"))
async def add_group_start(cb: CallbackQuery, state: FSMContext):
    team_id = int(cb.data.split(":")[1])
    if not await is_team_owner(team_id, cb.from_user.id):
        await cb.answer("Только владелец может добавлять группы.", show_alert=True)
        return
    await state.set_state(AddGroup.username)
    await state.update_data(team_id=team_id)
    await cb.message.edit_text(
        f'{E_PENCIL} Введите <b>username</b> группы (например: @mygroup)\n\n'
        f'{E_INFO} Бот должен быть админом с правом ограничивать участников.',
        reply_markup=back_kb(f"groups:{team_id}")
    )
    await cb.answer()

@router.message(AddGroup.username)
async def add_group_username(msg: Message, state: FSMContext):
    data = await state.get_data()
    team_id = data["team_id"]
    username = msg.text.strip().lstrip("@")

    groups = await get_team_groups(team_id)
    if len(groups) >= 10:
        await state.clear()
        await msg.answer(f'{E_CROSS} Максимум 10 групп в команде.', reply_markup=main_menu_kb())
        return

    try:
        chat = await bot.get_chat(f"@{username}")
    except Exception:
        await msg.answer(
            f'{E_CROSS} Не удалось найти чат <b>@{username}</b>. '
            f'Бот должен быть добавлен в группу.'
        )
        return

    if chat.type not in ("group", "supergroup"):
        await msg.answer(f'{E_CROSS} Это не группа/супергруппа.')
        return

    ok, err = await check_bot_permissions(chat.id)
    if not ok:
        await msg.answer(f'{E_CROSS} {err}')
        return

    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT 1 FROM team_groups WHERE team_id=$1 AND chat_id=$2", team_id, chat.id)
        if existing:
            await msg.answer(f'{E_CROSS} Эта группа уже привязана.')
            await state.clear()
            return
        await conn.execute(
            "INSERT INTO team_groups(team_id, chat_id, username, title) VALUES($1,$2,$3,$4)",
            team_id, chat.id, username, chat.title
        )

    await state.clear()
    await msg.answer(
        f'{E_CHECK} Группа <b>{chat.title}</b> (@{username}) добавлена!',
        reply_markup=main_menu_kb()
    )

# ─── Модераторы ───────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("mods:"))
async def mods_list(cb: CallbackQuery):
    team_id = int(cb.data.split(":")[1])
    if not await is_team_member(team_id, cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа.", show_alert=True)
        return
    is_owner = await is_team_owner(team_id, cb.from_user.id)
    mods = await get_team_mods(team_id)
    async with db_pool.acquire() as conn:
        team = await conn.fetchrow("SELECT name FROM teams WHERE id=$1", team_id)
    has_pending = any(m["user_id"] is None for m in mods)
    hint = f'\n{E_INFO} ⏳ — ещё не писал /start' if has_pending else ""
    await cb.message.edit_text(
        f'{E_PEOPLE} <b>Модераторы «{team["name"]}»</b>\n\n'
        f'{E_INFO} Всего: <b>{len(mods)}</b>{hint}',
        reply_markup=mods_kb(team_id, mods, is_owner)
    )
    await cb.answer()

@router.callback_query(F.data.startswith("mod_info:"))
async def mod_info_cb(cb: CallbackQuery):
    _, team_id, mod_row_id = cb.data.split(":")
    team_id, mod_row_id = int(team_id), int(mod_row_id)
    if not await is_team_owner(team_id, cb.from_user.id):
        await cb.answer("Только владелец.", show_alert=True)
        return
    async with db_pool.acquire() as conn:
        m = await conn.fetchrow("SELECT * FROM moderators WHERE id=$1", mod_row_id)
    if not m:
        await cb.answer("Не найден.", show_alert=True)
        return
    status = (f'{E_CHECK} Активен (ID: <code>{m["user_id"]}</code>)'
              if m["user_id"] else '⏳ Ещё не писал /start')
    await cb.message.edit_text(
        f'{E_PROFILE} <b>Модератор:</b> @{m["username"]}\n'
        f'{E_TAG} Статус: {status}',
        reply_markup=mod_info_kb(team_id, mod_row_id)
    )
    await cb.answer()

@router.callback_query(F.data.startswith("del_mod:"))
async def del_mod(cb: CallbackQuery):
    _, team_id, mod_row_id = cb.data.split(":")
    team_id, mod_row_id = int(team_id), int(mod_row_id)
    if not await is_team_owner(team_id, cb.from_user.id):
        await cb.answer("Только владелец.", show_alert=True)
        return
    async with db_pool.acquire() as conn:
        m = await conn.fetchrow("SELECT username FROM moderators WHERE id=$1", mod_row_id)
        await conn.execute("DELETE FROM moderators WHERE id=$1", mod_row_id)
    await cb.message.edit_text(
        f'{E_CHECK} Модератор @{m["username"] if m else "?"} удалён.',
        reply_markup=back_kb(f"mods:{team_id}")
    )
    await cb.answer()

@router.callback_query(F.data.startswith("add_mod:"))
async def add_mod_start(cb: CallbackQuery, state: FSMContext):
    team_id = int(cb.data.split(":")[1])
    if not await is_team_owner(team_id, cb.from_user.id):
        await cb.answer("Только владелец.", show_alert=True)
        return
    await state.set_state(AddModerator.username)
    await state.update_data(team_id=team_id)
    await cb.message.edit_text(
        f'{E_PENCIL} Введите <b>username</b> модератора (@username)',
        reply_markup=back_kb(f"mods:{team_id}")
    )
    await cb.answer()

@router.message(AddModerator.username)
async def add_mod_username(msg: Message, state: FSMContext):
    data = await state.get_data()
    team_id = data["team_id"]
    raw = msg.text.strip().lstrip("@").split()[0]
    username_lower = raw.lower()

    if not username_lower:
        await msg.answer(f'{E_CROSS} Неверный username.')
        return

    async with db_pool.acquire() as conn:
        owner_username = await conn.fetchval(
            "SELECT username FROM users WHERE user_id=(SELECT owner_id FROM teams WHERE id=$1)", team_id
        )
        if owner_username and owner_username.lower() == username_lower:
            await msg.answer(f'{E_CROSS} Владелец не может быть модератором.')
            await state.clear()
            return

        existing = await conn.fetchrow(
            "SELECT 1 FROM moderators WHERE team_id=$1 AND LOWER(username)=$2", team_id, username_lower
        )
        if existing:
            await msg.answer(f'{E_CROSS} @{raw} уже модератор.')
            await state.clear()
            return

        u = await conn.fetchrow("SELECT user_id FROM users WHERE LOWER(username)=$1", username_lower)
        user_id = u["user_id"] if u else None

        await conn.execute(
            "INSERT INTO moderators(team_id, username, user_id) VALUES($1,$2,$3)",
            team_id, raw, user_id
        )

    await state.clear()
    status = f'{E_CHECK} Активен' if user_id else '⏳ Активируется после /start'
    await msg.answer(
        f'{E_CHECK} @{raw} добавлен!\n{E_INFO} {status}',
        reply_markup=main_menu_kb()
    )

# ─── Отмена FSM ──────────────────────────────────────────────────────────────

@router.callback_query(F.data == "cancel_state")
async def cancel_state(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.delete()
    await cb.answer()

# ─── Команды в чате: helpers (обычные emoji) ─────────────────────────────────

async def get_target(msg: Message) -> Optional[tuple]:
    if msg.reply_to_message:
        u = msg.reply_to_message.from_user
        return u.id, u.username
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        return None
    first_arg = parts[1].strip().split()[0]
    if not first_arg.startswith("@"):
        return None
    username = first_arg.lstrip("@")
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT user_id, username FROM users WHERE LOWER(username)=LOWER($1)", username)
    if row:
        return row["user_id"], row["username"]
    return None, username

def parse_comment(msg: Message) -> str:
    if msg.reply_to_message:
        parts = msg.text.split(maxsplit=1)
        return parts[1].strip() if len(parts) > 1 else ""
    else:
        parts = msg.text.split(maxsplit=2)
        return parts[2].strip() if len(parts) > 2 else ""

async def do_ban_action(chat_id: int, user_id: int) -> str:
    try:
        await bot.ban_chat_member(chat_id, user_id, revoke_messages=True)
        return "✅ Заблокирован, сообщения удалены"
    except TelegramBadRequest as e:
        return f"❌ {e.message}"

async def chat_mod_check(msg: Message) -> Optional[dict]:
    if msg.chat.type not in ("group", "supergroup"):
        await msg.answer("❌ Команда только для групп.")
        return None
    result = await find_team_for_chat(msg.chat.id, msg.from_user.id, msg.from_user.username)
    if not result:
        await msg.answer("❌ Группа не привязана или у вас нет прав модератора.")
    return result

async def apply_punishment(msg: Message, ptype: str, team_id: int,
                           target_user_id: Optional[int], target_username: Optional[str]):
    chat_id = msg.chat.id
    comment = parse_comment(msg)
    target_name = f"@{target_username}" if target_username else str(target_user_id)
    mod_name = f"@{msg.from_user.username}" if msg.from_user.username else msg.from_user.full_name

    if target_user_id is None:
        await msg.answer(f"❌ @{target_username} не найден. Нужен /start.")
        return

    async with db_pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO punishments(team_id, chat_id, target_user_id, target_username, mod_user_id, type, comment)
               VALUES($1,$2,$3,$4,$5,$6,$7)""",
            team_id, chat_id, target_user_id, target_username, msg.from_user.id, ptype, comment or None
        )

    if ptype == "log":
        logs  = await count_active(team_id, chat_id, target_user_id, "log")
        warns = await count_active(team_id, chat_id, target_user_id, "warn")
        text = (
            f"📁 <b>Лог выдан</b>\n"
            f"👤 {target_name}\n"
            f"🛡 {mod_name}\n"
            f"📊 Логов: <b>{logs}/3</b> | Варнов: <b>{warns}/3</b>"
        )
        if comment:
            text += f"\n💬 {comment}"
        if logs % 3 == 0:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO punishments(team_id, chat_id, target_user_id, target_username, mod_user_id, type, comment)
                       VALUES($1,$2,$3,$4,$5,'warn','Авто за 3 лога')""",
                    team_id, chat_id, target_user_id, target_username, msg.from_user.id
                )
            warns_new = await count_active(team_id, chat_id, target_user_id, "warn")
            text += f"\n\n⚠ <b>3 лога → варн!</b> ({warns_new}/3)"
            if warns_new % 3 == 0:
                ban_res = await do_ban_action(chat_id, target_user_id)
                async with db_pool.acquire() as conn:
                    await conn.execute(
                        """INSERT INTO punishments(team_id, chat_id, target_user_id, target_username, mod_user_id, type, comment)
                           VALUES($1,$2,$3,$4,$5,'ban','Авто за 3 варна')""",
                        team_id, chat_id, target_user_id, target_username, msg.from_user.id
                    )
                text += f"\n🔒 <b>3 варна → бан!</b> {ban_res}"
        await msg.answer(text)

    elif ptype == "warn":
        warns = await count_active(team_id, chat_id, target_user_id, "warn")
        text = (
            f"⚠ <b>Варн выдан</b>\n"
            f"👤 {target_name}\n"
            f"🛡 {mod_name}\n"
            f"📊 Варнов: <b>{warns}/3</b>"
        )
        if comment:
            text += f"\n💬 {comment}"
        if warns % 3 == 0:
            ban_res = await do_ban_action(chat_id, target_user_id)
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO punishments(team_id, chat_id, target_user_id, target_username, mod_user_id, type, comment)
                       VALUES($1,$2,$3,$4,$5,'ban','Авто за 3 варна')""",
                    team_id, chat_id, target_user_id, target_username, msg.from_user.id
                )
            text += f"\n\n🔒 <b>3 варна → бан!</b> {ban_res}"
        await msg.answer(text)

    elif ptype == "ban":
        ban_res = await do_ban_action(chat_id, target_user_id)
        text = (
            f"🔒 <b>Бан</b>\n"
            f"👤 {target_name}\n"
            f"🛡 {mod_name}\n"
            f"ℹ {ban_res}"
        )
        if comment:
            text += f"\n💬 {comment}"
        await msg.answer(text)

# ─── /log /warn /ban /unlog /unwarn /unban /stats ─────────────────────────────

@router.message(Command("log"))
async def cmd_log(msg: Message):
    if not (ti := await chat_mod_check(msg)):
        return
    t = await get_target(msg)
    if not t:
        await msg.answer("❌ <code>/log @user</code> или ответом")
        return
    await apply_punishment(msg, "log", ti["team_id"], t[0], t[1])

@router.message(Command("warn"))
async def cmd_warn(msg: Message):
    if not (ti := await chat_mod_check(msg)):
        return
    t = await get_target(msg)
    if not t:
        await msg.answer("❌ <code>/warn @user</code> или ответом")
        return
    await apply_punishment(msg, "warn", ti["team_id"], t[0], t[1])

@router.message(Command("ban"))
async def cmd_ban(msg: Message):
    if not (ti := await chat_mod_check(msg)):
        return
    t = await get_target(msg)
    if not t:
        await msg.answer("❌ <code>/ban @user</code> или ответом")
        return
    await apply_punishment(msg, "ban", ti["team_id"], t[0], t[1])

@router.message(Command("unlog"))
async def cmd_unlog(msg: Message):
    if not (ti := await chat_mod_check(msg)):
        return
    t = await get_target(msg)
    if not t or t[0] is None:
        await msg.answer("❌ <code>/unlog @user</code>")
        return
    uid, uname = t
    uname_str = f"@{uname}" if uname else str(uid)
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """UPDATE punishments SET active=FALSE
               WHERE id=(SELECT id FROM punishments
                         WHERE team_id=$1 AND chat_id=$2 AND target_user_id=$3 AND type='log' AND active=TRUE
                         ORDER BY created_at DESC LIMIT 1)
               RETURNING id""",
            ti["team_id"], msg.chat.id, uid
        )
    if row:
        remaining = await count_active(ti["team_id"], msg.chat.id, uid, "log")
        await msg.answer(f"🔓 Лог снят с {uname_str}. Активных: <b>{remaining}</b>")
    else:
        await msg.answer("❌ Нет активных логов.")

@router.message(Command("unwarn"))
async def cmd_unwarn(msg: Message):
    if not (ti := await chat_mod_check(msg)):
        return
    t = await get_target(msg)
    if not t or t[0] is None:
        await msg.answer("❌ <code>/unwarn @user</code>")
        return
    uid, uname = t
    uname_str = f"@{uname}" if uname else str(uid)
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """UPDATE punishments SET active=FALSE
               WHERE id=(SELECT id FROM punishments
                         WHERE team_id=$1 AND chat_id=$2 AND target_user_id=$3 AND type='warn' AND active=TRUE
                         ORDER BY created_at DESC LIMIT 1)
               RETURNING id""",
            ti["team_id"], msg.chat.id, uid
        )
    if row:
        remaining = await count_active(ti["team_id"], msg.chat.id, uid, "warn")
        await msg.answer(f"🔓 Варн снят с {uname_str}. Активных: <b>{remaining}</b>")
    else:
        await msg.answer("❌ Нет активных варнов.")

@router.message(Command("unban"))
async def cmd_unban(msg: Message):
    if not (ti := await chat_mod_check(msg)):
        return
    t = await get_target(msg)
    if not t or t[0] is None:
        await msg.answer("❌ <code>/unban @user</code>")
        return
    uid, uname = t
    uname_str = f"@{uname}" if uname else str(uid)
    try:
        await bot.unban_chat_member(msg.chat.id, uid, only_if_banned=True)
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE punishments SET active=FALSE WHERE team_id=$1 AND chat_id=$2 AND target_user_id=$3 AND type='ban' AND active=TRUE",
                ti["team_id"], msg.chat.id, uid
            )
        await msg.answer(f"🔓 {uname_str} разблокирован.")
    except TelegramBadRequest as e:
        await msg.answer(f"❌ {e.message}")

@router.message(Command("stats"))
async def cmd_stats(msg: Message):
    if not (ti := await chat_mod_check(msg)):
        return
    t = await get_target(msg)
    if not t or t[0] is None:
        await msg.answer("❌ <code>/stats @user</code> или ответом")
        return
    uid, uname = t
    uname_str = f"@{uname}" if uname else str(uid)
    async with db_pool.acquire() as conn:
        logs  = await conn.fetchval(
            "SELECT COUNT(*) FROM punishments WHERE team_id=$1 AND chat_id=$2 AND target_user_id=$3 AND type='log' AND active=TRUE",
            ti["team_id"], msg.chat.id, uid)
        warns = await conn.fetchval(
            "SELECT COUNT(*) FROM punishments WHERE team_id=$1 AND chat_id=$2 AND target_user_id=$3 AND type='warn' AND active=TRUE",
            ti["team_id"], msg.chat.id, uid)
        bans  = await conn.fetchval(
            "SELECT COUNT(*) FROM punishments WHERE team_id=$1 AND chat_id=$2 AND target_user_id=$3 AND type='ban'",
            ti["team_id"], msg.chat.id, uid)
    l2w = (3 - logs  % 3) if logs  % 3 != 0 else 3
    w2b = (3 - warns % 3) if warns % 3 != 0 else 3
    await msg.answer(
        f"📊 <b>{uname_str}</b>\n"
        f"📁 Логов: <b>{logs}</b> (до варна: {l2w})\n"
        f"⚠ Варнов: <b>{warns}</b> (до бана: {w2b})\n"
        f"🔒 Банов: <b>{bans}</b>"
    )

# ─── Запуск ───────────────────────────────────────────────────────────────────

async def main():
    dp.include_router(router)
    await init_db()
    logger.info("Database initialized. Starting bot...")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
