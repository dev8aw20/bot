"""
Master bot menu handlers: STARTUP MENU -> MANAGE CLONE'S MENU -> Add Clone
flow. Wire these into your existing master bot.py's Application alongside
your current handlers — this file doesn't replace bot.py, it's additive.

Requires in bot.py (or wherever main() builds the Application):

    from db import Database
    from clone_runner import CloneRunner
    from bot_instance import BotInstance
    import master_menu

    central_db = Database(DATABASE_URL)          # your existing central db
    runner = CloneRunner(central_db, instance_factory=lambda row: BotInstance(row, ...))
    application.bot_data["central_db"] = central_db
    application.bot_data["runner"] = runner
    master_menu.register(application)

    # on startup (post_init):
    await runner.start_all()

NOT yet implemented here (stubbed with a "coming soon" callback so the
buttons exist and don't dead-end, per the dashboard spec): START MSG,
FORCE SUB, MODERATORS, AUTO DELETE, NO FORWARD, TRANSFER DB, MODE, and
the STATS/RESTART/DELETE operational buttons. Each of those is its own
scoped piece of work — building all of them blind in one pass isn't
something I'd stand behind without testing each flow.
"""

import logging
import os

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    ConversationHandler, filters, ContextTypes,
)
from telegram.error import InvalidToken

from db import Database

logger = logging.getLogger(__name__)

# Same env var bot.py reads for itself — read independently here rather
# than importing OWNER_ID from bot.py, which would create a circular
# import (bot.py imports master_menu).
OWNER_ID = int(os.environ["OWNER_ID"])
# Defaults for the ABOUT page's two links — used only when the owner
# hasn't set an override with /aboutset (see cb_about below). Optional:
# missing env vars just mean that line is left off until /aboutset sets one.
DEFAULT_SUPPORT_GROUP_LINK = os.environ.get("UPDATE_SUPPORT_GROUP", "").strip()
DEFAULT_ANOTHER_BOT_LINK = os.environ.get("OTHER_BOT_URL", "").strip()

# Defaults for the ABOUT menu's two links — optional at deploy time.
# /aboutset (below) lets the owner override either one from inside
# Telegram without touching env vars; whichever bot_settings has wins.
DEFAULT_SUPPORT_GROUP_LINK = os.environ.get("UPDATE_SUPPORT_GROUP", "").strip()
DEFAULT_ANOTHER_BOT_LINK = os.environ.get("OTHER_BOT_URL", "").strip()

WAITING_FOR_TOKEN = 1
WAITING_FOR_SUPABASE_URL = 2
WAITING_FOR_SUPABASE_KEY = 3
AWAITING_CAPTION_TEXT = 4
AWAITING_BUTTON_LINE = 5
AWAITING_ABOUT_EXTRA_LINK = 6
MAX_CLONES_PER_USER = 2

UPDATE_CHANNEL_URL = None  # bot.py sets this at import time — see wiring note below.


# ── STARTUP MENU (called FROM bot.py's own cmd_start — not a handler here) ──
def startup_menu(first_name: str):
    """Returns (text, InlineKeyboardMarkup) for the STARTUP MENU. bot.py's
    existing cmd_start calls this for the plain-/start, non-owner,
    no-batch-payload case, instead of registering a competing /start here."""
    text = (
        f"Hello {first_name} \u2728\n\n"
        "I am a permanent file store bot and users can access stored "
        "messages by using a shareable link given by me.\n\n"
        "To know more click the help button below."
    )
    buttons = [
        [InlineKeyboardButton("HELP", callback_data="menu_help"),
         InlineKeyboardButton("ABOUT", callback_data="menu_about")],
        [InlineKeyboardButton("CREATE MY OWN CLONE", callback_data="menu_manage_clones")],
    ]
    if UPDATE_CHANNEL_URL:
        buttons.append([InlineKeyboardButton("\U0001F4DF UPDATE CHANNEL \u2197\ufe0f", url=UPDATE_CHANNEL_URL)])
    return text, InlineKeyboardMarkup(buttons)


async def cb_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    text = (
        "\U0001F335 Help Menu\n\n"
        "I am a permanent file store bot. You can store files from your "
        "public channel.\n\n"
        "Available Commands: /start /settings\n"
        "Moderator Commands: /broadcast, /ban, /unban"
    )
    buttons = []
    if q.from_user.id == OWNER_ID:
        buttons.append(
            [InlineKeyboardButton("SETTINGS", callback_data="menu_settings"),
             InlineKeyboardButton("STATS", callback_data="menu_stats")]
        )
    buttons.append([InlineKeyboardButton("BACK", callback_data="menu_startup")])
    await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(buttons))


async def cb_startup(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    text, markup = startup_menu(q.from_user.first_name)
    await q.edit_message_text(text, reply_markup=markup)


# ── MANAGE CLONE'S MENU ──────────────────────────────────────────────────
async def _manage_clones_content(ctx: ContextTypes.DEFAULT_TYPE, user_id: str, requester_id: int):
    central_db = ctx.application.bot_data["central_db"]
    clones = await central_db.list_clones(user_id)

    text = (
        "\u2728 Manage Clone's\n\n"
        "You can now manage and create your very own identical clone bot, "
        "mirroring all my awesome features, using the given buttons."
    )
    buttons = [[InlineKeyboardButton("\u2795 Add Clone", callback_data="clone_add")]]
    for c in clones:
        status_emoji = "\u2705" if c["is_active"] else "\u274c"
        label = f"{status_emoji} {c.get('bot_name') or c['bot_username'] or c['id']}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"clone_dash_{c['id']}")])
    # Back target depends on WHO is looking, not how they navigated here —
    # Manage Clone's is the same screen for everyone, but menu_settings is
    # owner-gated (see cb_settings), so routing a non-owner's back button
    # through it would just bounce them off "\u26d4 Only the bot owner can
    # use Settings" with no way forward. So:
    #   owner     -> menu_settings -> back -> menu_help -> back -> menu_startup
    #   non-owner -> menu_help -> back -> menu_startup   (Settings skipped)
    back_target = "menu_settings" if requester_id == OWNER_ID else "menu_help"
    buttons.append([InlineKeyboardButton("\u2039 back", callback_data=back_target)])
    return text, InlineKeyboardMarkup(buttons)


async def cb_manage_clones(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    text, markup = await _manage_clones_content(ctx, str(q.from_user.id), q.from_user.id)
    await q.edit_message_text(text, reply_markup=markup)


async def send_manage_clones_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """For the plain-message case — a clone's 'CREATE MY OWN CLONE' button
    deep-links here via /start manage_clones (see bot_instance.py), which
    has no callback_query to edit. NOT owner-gated, unlike
    send_settings_menu — Manage Clone's is open to every user, that's the
    whole point of letting clone users create their own clones."""
    user_id = str(update.effective_user.id)
    text, markup = await _manage_clones_content(ctx, user_id, update.effective_user.id)
    await update.effective_message.reply_text(text, reply_markup=markup)



# ── SETTINGS MENU ─────────────────────────────────────────────────────────
# Master bot's own settings — NOT per-clone (clones have their own
# dashboard, reached via "MY CLONE BOT" below). Backed by db.bot_settings,
# a singleton row (id=1).
async def _settings_menu_content(ctx: ContextTypes.DEFAULT_TYPE):
    central_db = ctx.application.bot_data["central_db"]
    settings = await central_db.get_bot_settings()
    protect_label = "PROTECT CONTENT \u2611\ufe0f" if settings["protect_content"] else "PROTECT CONTENT \u2610"
    buttons = [
        [InlineKeyboardButton("MY CLONE BOT \U0001F916", callback_data="menu_manage_clones")],
        [InlineKeyboardButton("CUSTOM CAPTION \U0001F58A\ufe0f", callback_data="settings_caption_menu"),
         InlineKeyboardButton("CUSTOM BUTTON \u2728", callback_data="settings_button_menu")],
        [InlineKeyboardButton(protect_label, callback_data="settings_protect_toggle"),
         InlineKeyboardButton("\u2039 BACK", callback_data="menu_help")],
    ]
    text = "\U0001F6E0\ufe0f Settings... Customize your settings as your need"
    return text, InlineKeyboardMarkup(buttons)


async def _render_settings_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text, markup = await _settings_menu_content(ctx)
    await update.callback_query.edit_message_text(text, reply_markup=markup)


async def send_settings_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """For the plain-message case — reached via the /setting command (see
    register() below), which has no callback_query to edit. (Previously
    also reached via a clone's 'CREATE MY OWN CLONE' deep-link with
    /start settings — that button now deep-links to /start manage_clones
    instead, see bot_instance.py, since Settings is owner-gated and
    Manage Clone's isn't.)"""
    if update.effective_user.id != OWNER_ID:
        await update.effective_message.reply_text(
            "\u26d4 Only the bot owner can use Settings."
        )
        return
    text, markup = await _settings_menu_content(ctx)
    await update.effective_message.reply_text(text, reply_markup=markup)


async def cb_settings(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != OWNER_ID:
        await q.answer("\u26d4 Only the bot owner can use Settings.", show_alert=True)
        return
    await q.answer()
    await _render_settings_menu(update, ctx)


async def cb_settings_protect_toggle(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != OWNER_ID:
        await q.answer("Not allowed.", show_alert=True)
        return
    central_db = ctx.application.bot_data["central_db"]
    settings = await central_db.get_bot_settings()
    new_state = not settings["protect_content"]
    await central_db.update_bot_settings(protect_content=new_state)
    await update.callback_query.answer("Enabled." if new_state else "Disabled.")
    await _render_settings_menu(update, ctx)


# ── CUSTOM CAPTION SUB-MENU ───────────────────────────────────────────────
CUSTOM_CAPTION_HELP = (
    "Custom Caption: You can add a custom caption to your media messages "
    "instead of its original caption.\n\n"
    "Fillings:\n"
    "\u2022 {file_name}: File Name\n"
    "\u2022 {file_size}: File size\n"
    "\u2022 {caption}: Original Caption"
)


async def cb_settings_caption_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != OWNER_ID:
        await q.answer("Not allowed.", show_alert=True)
        return
    await q.answer()
    buttons = [
        [InlineKeyboardButton("Edit", callback_data="settings_caption_edit"),
         InlineKeyboardButton("See", callback_data="settings_caption_see")],
        [InlineKeyboardButton("Delete", callback_data="settings_caption_delete")],
        [InlineKeyboardButton("\u2039 back", callback_data="menu_settings")],
    ]
    await update.callback_query.edit_message_text(
        CUSTOM_CAPTION_HELP, reply_markup=InlineKeyboardMarkup(buttons)
    )


async def cb_settings_caption_edit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != OWNER_ID:
        await q.answer("Not allowed.", show_alert=True)
        return ConversationHandler.END
    await q.answer()
    await q.edit_message_text(
        "Send the new caption template. You can use {file_name}, "
        "{file_size}, {caption}. /cancel to stop."
    )
    return AWAITING_CAPTION_TEXT


async def receive_custom_caption(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    central_db = ctx.application.bot_data["central_db"]
    await central_db.update_bot_settings(custom_caption=update.message.text)
    await update.message.reply_text(
        "\u2705 Custom caption updated.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("\u2039 back", callback_data="settings_caption_menu")]]
        ),
    )
    return ConversationHandler.END


async def cb_settings_caption_see(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != OWNER_ID:
        await q.answer("Not allowed.", show_alert=True)
        return
    await q.answer()
    central_db = ctx.application.bot_data["central_db"]
    settings = await central_db.get_bot_settings()
    current = settings["custom_caption"] or "(not set — original captions are used as-is)"
    await q.message.reply_text(
        current,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("\u2039 back", callback_data="settings_caption_menu")]]
        ),
    )


async def cb_settings_caption_delete(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != OWNER_ID:
        await q.answer("Not allowed.", show_alert=True)
        return
    central_db = ctx.application.bot_data["central_db"]
    await central_db.update_bot_settings(custom_caption=None)
    await q.answer("Deleted.")
    await cb_settings_caption_menu(update, ctx)


# ── CUSTOM BUTTON SUB-MENU ────────────────────────────────────────────────
# Stored as raw text: one row per line, buttons on a row separated by "|",
# each button "Label - URL". Parsed at send time (see bot.py's
# _parse_custom_buttons) so one bad line never blocks a save.
def _preview_button_markup(raw: str):
    if not raw or not raw.strip():
        return None
    rows = []
    for line in raw.strip().splitlines():
        row = []
        for chunk in line.split("|"):
            chunk = chunk.strip()
            if not chunk or " - " not in chunk:
                continue
            label, url = chunk.rsplit(" - ", 1)
            label, url = label.strip(), url.strip()
            if label and url:
                row.append(InlineKeyboardButton(f"{label} \u2197\ufe0f", url=url))
        if row:
            rows.append(row)
    return rows or None


async def cb_settings_button_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != OWNER_ID:
        await q.answer("Not allowed.", show_alert=True)
        return
    await q.answer()
    central_db = ctx.application.bot_data["central_db"]
    settings = await central_db.get_bot_settings()
    preview_rows = _preview_button_markup(settings["custom_buttons"]) or []

    rows = list(preview_rows)
    rows.append([InlineKeyboardButton("\u2795", callback_data="settings_button_add")])
    rows.append([InlineKeyboardButton("Delete", callback_data="settings_button_delete")])
    rows.append([InlineKeyboardButton("\u2039 back", callback_data="menu_settings")])

    text = "Custom Button: You can add a custom button to your message"
    if not preview_rows:
        text += "\n\n(none set yet)"
    await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows))


async def cb_settings_button_add(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != OWNER_ID:
        await q.answer("Not allowed.", show_alert=True)
        return ConversationHandler.END
    await q.answer()
    await q.edit_message_text(
        "Send a new button row: \"Label - URL\", or two on the same row "
        "with \"Label1 - URL1 | Label2 - URL2\". This is ADDED as a new "
        "row below your existing buttons. /cancel to stop."
    )
    return AWAITING_BUTTON_LINE


async def receive_custom_button_line(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    central_db = ctx.application.bot_data["central_db"]
    text = update.message.text.strip()
    if not _preview_button_markup(text):
        await update.message.reply_text(
            "\u26a0\ufe0f Couldn't parse that — use \"Label - URL\". Send again, or /cancel."
        )
        return AWAITING_BUTTON_LINE
    settings = await central_db.get_bot_settings()
    existing = settings["custom_buttons"] or ""
    updated = (existing.rstrip() + "\n" + text).strip() if existing.strip() else text
    await central_db.update_bot_settings(custom_buttons=updated)
    await update.message.reply_text(
        "\u2705 Button row added.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("\u2039 back", callback_data="settings_button_menu")]]
        ),
    )
    return ConversationHandler.END


async def cb_settings_button_delete(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != OWNER_ID:
        await q.answer("Not allowed.", show_alert=True)
        return
    central_db = ctx.application.bot_data["central_db"]
    await central_db.update_bot_settings(custom_buttons=None)
    await q.answer("Deleted.")
    await cb_settings_button_menu(update, ctx)


async def cancel_settings_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END


# ── Add Clone: ConversationHandler (token input) ─────────────────────────
async def cb_clone_add_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    central_db = ctx.application.bot_data["central_db"]
    user_id = str(q.from_user.id)

    current = await central_db.count_active_clones(user_id)
    if current >= MAX_CLONES_PER_USER:
        await q.edit_message_text(
            f"\u26d4 You already have {current}/{MAX_CLONES_PER_USER} clone bots active. "
            "Deactivate or delete one before creating another.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("\u2039 back", callback_data="menu_manage_clones")]]
            ),
        )
        return ConversationHandler.END

    await q.edit_message_text(
        "Send me the Telegram Bot HTTP API token for your new clone.\n\n"
        "Get one from @BotFather \u2192 /newbot. I'll validate it before "
        "creating the clone.\n\n"
        "Send /cancel to stop.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("\u2039 back", callback_data="clone_add_cancel")]]
        ),
    )
    return WAITING_FOR_TOKEN


async def cb_clone_add_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.pop("pending_clone_token", None)
    ctx.user_data.pop("pending_clone_username", None)
    ctx.user_data.pop("pending_clone_name", None)
    ctx.user_data.pop("pending_clone_supabase_url", None)
    await cb_manage_clones(update, ctx)
    return ConversationHandler.END


async def receive_clone_token(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    token = update.message.text.strip()
    central_db = ctx.application.bot_data["central_db"]

    if await central_db.token_already_registered(token):
        await update.message.reply_text(
            "That token is already registered as a clone on this platform. "
            "Send a different token, or /cancel."
        )
        return WAITING_FOR_TOKEN

    try:
        temp_bot = Bot(token=token)
        me = await temp_bot.get_me()
    except InvalidToken:
        await update.message.reply_text(
            "That doesn't look like a valid bot token. Double-check it and "
            "send again, or /cancel."
        )
        return WAITING_FOR_TOKEN
    except Exception:
        logger.exception("get_me() failed while validating a new clone token")
        await update.message.reply_text(
            "Couldn't reach Telegram to validate that token — try again in "
            "a moment, or /cancel."
        )
        return WAITING_FOR_TOKEN

    # Every clone must bring its own Supabase — there is no shared-db mode.
    # A clone without its own database has no clone_id column to be scoped
    # by anywhere in the schema, so it would silently read/write the SAME
    # folders/batches/audios rows as the master bot and every other
    # shared-db clone. Token is validated; hold it in conversation state
    # until the clone's own database is validated too, then create the row
    # once, atomically, with everything required present.
    ctx.user_data["pending_clone_token"] = token
    ctx.user_data["pending_clone_username"] = me.username
    ctx.user_data["pending_clone_name"] = me.first_name or me.username

    await update.message.reply_text(
        "\u2705 Token verified for @" + me.username + ".\n\n"
        "This platform requires every clone to use its own Supabase project — "
        "your folders/files are never shared with the main bot or other clones.\n\n"
        "Send your Supabase Postgres connection string "
        "(Project Settings \u2192 Database \u2192 Connection string \u2192 URI, "
        "Session Pooler mode).\n\n"
        "Send /cancel to stop."
    )
    return WAITING_FOR_SUPABASE_URL


async def receive_supabase_url(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    url = update.message.text.strip()
    if not url.startswith("postgres"):
        await update.message.reply_text(
            "That doesn't look like a Postgres connection string — it should "
            "start with postgres:// or postgresql://. Send it again, or /cancel."
        )
        return WAITING_FOR_SUPABASE_URL

    ctx.user_data["pending_clone_supabase_url"] = url
    await update.message.reply_text(
        "Got it. Now send your Supabase service_role (or anon) API key, "
        "so the dashboard can store it alongside the connection string.\n\n"
        "Send /cancel to stop."
    )
    return WAITING_FOR_SUPABASE_KEY


async def receive_supabase_key(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    key = update.message.text.strip()
    if not key:
        await update.message.reply_text("Key cannot be empty. Send it again, or /cancel.")
        return WAITING_FOR_SUPABASE_KEY

    url = ctx.user_data.get("pending_clone_supabase_url")
    token = ctx.user_data.get("pending_clone_token")
    username = ctx.user_data.get("pending_clone_username")
    bot_name = ctx.user_data.get("pending_clone_name") or username
    if not (url and token and username):
        await update.message.reply_text(
            "Something went wrong tracking this setup — start over with /cancel then Add Clone."
        )
        return ConversationHandler.END

    # Validate the connection actually works BEFORE writing anything —
    # a bad host/password should surface now, not as a mystery crash the
    # first time the clone tries to fetch its folders.
    probe = Database(url)
    try:
        await probe.connect()
    except Exception as e:
        logger.warning("Clone Supabase connection failed during setup: %s", e)
        await update.message.reply_text(
            "\u274c Couldn't connect to that database with the given URL/key.\n"
            "Double-check the connection string and key, then send the "
            "connection string again, or /cancel.\n\n"
            f"Error: {e}"
        )
        return WAITING_FOR_SUPABASE_URL
    finally:
        await probe.disconnect()

    central_db = ctx.application.bot_data["central_db"]
    runner = ctx.application.bot_data["runner"]
    user_id = str(update.effective_user.id)

    current = await central_db.count_active_clones(user_id)
    if current >= MAX_CLONES_PER_USER:
        ctx.user_data.pop("pending_clone_token", None)
        ctx.user_data.pop("pending_clone_username", None)
        ctx.user_data.pop("pending_clone_name", None)
        ctx.user_data.pop("pending_clone_supabase_url", None)
        await update.message.reply_text(
            f"\u26d4 You hit the {MAX_CLONES_PER_USER}-clone limit while this was being set up."
        )
        return ConversationHandler.END

    clone_id = await central_db.create_clone(
        user_id=user_id, bot_token=token, bot_username=username, bot_name=bot_name,
        supabase_url=url, supabase_key=key,
        max_clones=MAX_CLONES_PER_USER,
    )
    ctx.user_data.pop("pending_clone_token", None)
    ctx.user_data.pop("pending_clone_username", None)
    ctx.user_data.pop("pending_clone_name", None)
    ctx.user_data.pop("pending_clone_supabase_url", None)

    if clone_id is None:
        await update.message.reply_text(
            f"\u26d4 You hit the {MAX_CLONES_PER_USER}-clone limit while this was being set up."
        )
        return ConversationHandler.END

    clone_row = await central_db.get_clone(clone_id)
    await runner.start_one(clone_row)

    buttons = [[InlineKeyboardButton(f"\U0001FA84 {bot_name}", callback_data=f"clone_dash_{clone_id}")],
               [InlineKeyboardButton("\u2039 back", callback_data="menu_manage_clones")]]
    await update.message.reply_text(
        f"\u2705 Clone @{username} created and started, using its own database.",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    return ConversationHandler.END


async def cancel_clone_add(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.pop("pending_clone_token", None)
    ctx.user_data.pop("pending_clone_username", None)
    ctx.user_data.pop("pending_clone_name", None)
    ctx.user_data.pop("pending_clone_supabase_url", None)
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END


# ── CUSTOMIZE CLONE DASHBOARD (shell — sub-features stubbed) ─────────────
async def cb_clone_dashboard(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    clone_id = int(q.data.rsplit("_", 1)[1])
    central_db = ctx.application.bot_data["central_db"]
    clone = await central_db.get_clone(clone_id)

    if not clone or clone["user_id"] != str(q.from_user.id):
        await q.edit_message_text("Clone not found or not yours.")
        return

    text = (
        f"\U0001FA84 Customize Clone \u2794 Name: @{clone['bot_username']}\n\n"
        "Configure your clone settings using the buttons below."
    )
    def b(label, cd):
        return InlineKeyboardButton(label, callback_data=cd)
    buttons = [
        [b("START MSG", f"csm_menu_{clone_id}"), b("FORCE SUB", f"fsub_menu_{clone_id}")],
        [b("CUSTOM CAPTION", f"ccap_menu_{clone_id}"), b("CUSTOM BUTTON", f"cbtn_menu_{clone_id}")],
        [b("MODERATORS", f"mod_menu_{clone_id}"), b("AUTO DELETE", f"ad_menu_{clone_id}")],
        [b("NO FORWARD", f"nofwd_menu_{clone_id}"), b("ACCESS TOKEN", f"atok_menu_{clone_id}")],
        [b("TRANSFER DB", f"tdb_menu_{clone_id}"), b("ACTIVATE" if not clone["is_active"] else "DEACTIVATE",
                                       f"clone_toggle_{clone_id}")],
        [b("MODE", f"mode_menu_{clone_id}"), b("RESTART", f"clone_restart_{clone_id}")],
        [b("STATS", f"stats_show_{clone_id}"), b("DELETE", f"clone_delete_confirm_{clone_id}")],
        [b("\u2039 BACK", "menu_manage_clones")],
    ]
    await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(buttons))


# ── ABOUT MENU ────────────────────────────────────────────────────────────
# Support Group + Another Bot are FIXED — always the env-var defaults
# (UPDATE_SUPPORT_GROUP, OTHER_BOT_URL), not owner-editable. /aboutset
# manages everything else: a growable, editable list of extra links shown
# below those two — add, and now remove/update.
def _parse_about_extra_links(raw):
    if not raw or not raw.strip():
        return []
    out = []
    for line in raw.strip().splitlines():
        line = line.strip()
        if not line or " - " not in line:
            continue
        label, url = line.rsplit(" - ", 1)
        label, url = label.strip(), url.strip()
        if label and url:
            out.append((label, url))
    return out


def _about_extra_links_to_raw(links):
    return "\n".join(f"{label} - {url}" for label, url in links) or None


async def cb_about(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    central_db = ctx.application.bot_data["central_db"]
    settings = await central_db.get_bot_settings()

    lines = ["\u2139\ufe0f About"]
    if DEFAULT_SUPPORT_GROUP_LINK:
        lines.append(f"\nSupport group: [(Link)]({DEFAULT_SUPPORT_GROUP_LINK})")
    if DEFAULT_ANOTHER_BOT_LINK:
        lines.append(f"\nAnother bot: [(Link)]({DEFAULT_ANOTHER_BOT_LINK})")
    for label, url in _parse_about_extra_links(settings["about_extra_links"]):
        lines.append(f"\n{label}: [(Link)]({url})")
    if len(lines) == 1:
        lines.append("\n(Nothing set yet.)")

    buttons = [[InlineKeyboardButton("\u2039 back", callback_data="menu_startup")]]
    await q.edit_message_text(
        "\n".join(lines), reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="Markdown", disable_web_page_preview=True,
    )


def _aboutset_menu_content(settings):
    links = _parse_about_extra_links(settings["about_extra_links"])
    lines = [
        "\U0001F527 Manage ABOUT extra links.",
        "(Support Group / Another Bot are fixed via env vars — not editable here.)",
    ]
    rows = []
    if links:
        lines.append("")
        for i, (label, url) in enumerate(links):
            lines.append(f"{i + 1}. {label} \u2192 {url}")
            rows.append([InlineKeyboardButton(f"\u274c Remove \"{label}\"", callback_data=f"aboutset_remove_{i}")])
    else:
        lines.append("\n(No extra links yet.)")
    rows.append([InlineKeyboardButton("\u2795 Add Link", callback_data="aboutset_add")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


async def cmd_aboutset(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("\u26d4 Only the bot owner can use /aboutset.")
        return
    central_db = ctx.application.bot_data["central_db"]
    settings = await central_db.get_bot_settings()
    text, markup = _aboutset_menu_content(settings)
    await update.message.reply_text(text, reply_markup=markup, disable_web_page_preview=True)


async def cb_aboutset_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != OWNER_ID:
        await q.answer("Not allowed.", show_alert=True)
        return
    central_db = ctx.application.bot_data["central_db"]
    settings = await central_db.get_bot_settings()
    text, markup = _aboutset_menu_content(settings)
    await q.edit_message_text(text, reply_markup=markup, disable_web_page_preview=True)


async def cb_aboutset_remove(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != OWNER_ID:
        await q.answer("Not allowed.", show_alert=True)
        return
    idx = int(q.data.replace("aboutset_remove_", ""))
    central_db = ctx.application.bot_data["central_db"]
    settings = await central_db.get_bot_settings()
    links = _parse_about_extra_links(settings["about_extra_links"])
    if 0 <= idx < len(links):
        del links[idx]
    await central_db.update_bot_settings(about_extra_links=_about_extra_links_to_raw(links))
    await q.answer("Removed.")
    await cb_aboutset_menu(update, ctx)


async def cb_aboutset_add(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.from_user.id != OWNER_ID:
        return ConversationHandler.END
    await q.edit_message_text(
        "Send the new link as \"Label - URL\" (e.g. \"Backup Channel - "
        "https://t.me/mychannel\"). /cancel to stop."
    )
    return AWAITING_ABOUT_EXTRA_LINK


async def receive_about_extra_link(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not _parse_about_extra_links(text):
        await update.message.reply_text(
            "\u26a0\ufe0f Couldn't parse that — use \"Label - URL\". Send again, or /cancel."
        )
        return AWAITING_ABOUT_EXTRA_LINK
    central_db = ctx.application.bot_data["central_db"]
    settings = await central_db.get_bot_settings()
    existing = settings["about_extra_links"] or ""
    updated = (existing.rstrip() + "\n" + text).strip() if existing.strip() else text
    await central_db.update_bot_settings(about_extra_links=updated)
    settings = await central_db.get_bot_settings()
    menu_text, markup = _aboutset_menu_content(settings)
    await update.message.reply_text(
        f"\u2705 Added.\n\n{menu_text}", reply_markup=markup, disable_web_page_preview=True
    )
    return ConversationHandler.END


async def cancel_aboutset(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END


async def cb_stub(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer("Coming soon.", show_alert=True)


async def cb_clone_toggle(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = int(q.data.rsplit("_", 1)[1])
    central_db = ctx.application.bot_data["central_db"]
    runner = ctx.application.bot_data["runner"]
    clone = await central_db.get_clone(clone_id)
    if not clone or clone["user_id"] != str(q.from_user.id):
        await q.answer("Not yours.", show_alert=True)
        return
    new_state = not clone["is_active"]
    await central_db.set_clone_active(clone_id, new_state)
    if new_state:
        clone["is_active"] = True
        await runner.start_one(clone)
    else:
        await runner.stop_one(clone_id)
    await q.answer("Activated." if new_state else "Deactivated.")
    await cb_clone_dashboard(update, ctx)


async def cb_clone_restart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = int(q.data.rsplit("_", 1)[1])
    central_db = ctx.application.bot_data["central_db"]
    runner = ctx.application.bot_data["runner"]
    clone = await central_db.get_clone(clone_id)
    if not clone or clone["user_id"] != str(q.from_user.id):
        await q.answer("Not yours.", show_alert=True)
        return
    await runner.stop_one(clone_id)
    await runner.start_one(clone)
    await q.answer("Restarted.")


async def cb_clone_delete_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    clone_id = int(q.data.rsplit("_", 1)[1])
    central_db = ctx.application.bot_data["central_db"]
    clone = await central_db.get_clone(clone_id)
    if not clone or clone["user_id"] != str(q.from_user.id):
        await q.answer("Not yours.", show_alert=True)
        return
    text = (
        f"\u26a0\ufe0f Delete @{clone['bot_username']}?\n\n"
        "This stops the clone and removes it from the platform. Its stored "
        "folders/batches/audios are NOT deleted by this button yet — that's "
        "still open, see the note below."
    )
    buttons = [
        [InlineKeyboardButton("\u2705 Yes, delete", callback_data=f"clone_delete_go_{clone_id}"),
         InlineKeyboardButton("\u274c Cancel", callback_data=f"clone_dash_{clone_id}")],
    ]
    await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(buttons))


async def cb_clone_delete_go(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = int(q.data.rsplit("_", 1)[1])
    central_db = ctx.application.bot_data["central_db"]
    runner = ctx.application.bot_data["runner"]
    clone = await central_db.get_clone(clone_id)
    if not clone or clone["user_id"] != str(q.from_user.id):
        await q.answer("Not yours.", show_alert=True)
        return
    await runner.stop_one(clone_id)
    await central_db.delete_clone(clone_id)
    await q.answer("Deleted.")
    await cb_manage_clones(update, ctx)


def register(application: Application):
    """Registers only the clone-menu callback/conversation handlers.
    /start is NOT registered here — bot.py's existing cmd_start owns that
    command and calls startup_menu() directly for the plain-/start case."""
    application.add_handler(CallbackQueryHandler(cb_startup, pattern=r"^menu_startup$"))
    application.add_handler(CallbackQueryHandler(cb_help, pattern=r"^menu_help$"))
    application.add_handler(CallbackQueryHandler(cb_about, pattern=r"^menu_about$"))
    application.add_handler(CallbackQueryHandler(cb_manage_clones, pattern=r"^menu_manage_clones$"))
    application.add_handler(CallbackQueryHandler(cb_settings, pattern=r"^menu_settings$"))
    application.add_handler(CallbackQueryHandler(cb_settings_protect_toggle, pattern=r"^settings_protect_toggle$"))
    application.add_handler(CallbackQueryHandler(cb_settings_caption_menu, pattern=r"^settings_caption_menu$"))
    application.add_handler(CallbackQueryHandler(cb_settings_caption_see, pattern=r"^settings_caption_see$"))
    application.add_handler(CallbackQueryHandler(cb_settings_caption_delete, pattern=r"^settings_caption_delete$"))
    application.add_handler(CallbackQueryHandler(cb_settings_button_menu, pattern=r"^settings_button_menu$"))
    application.add_handler(CallbackQueryHandler(cb_settings_button_delete, pattern=r"^settings_button_delete$"))
    # /setting: direct command entry to the owner-only global Settings menu
    # (send_settings_menu already gates to OWNER_ID and works from a plain
    # message, no callback_query — see its docstring).
    application.add_handler(CommandHandler("setting", send_settings_menu))
    application.add_handler(CallbackQueryHandler(cb_clone_dashboard, pattern=r"^clone_dash_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_clone_toggle, pattern=r"^clone_toggle_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_clone_restart, pattern=r"^clone_restart_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_clone_delete_confirm, pattern=r"^clone_delete_confirm_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_clone_delete_go, pattern=r"^clone_delete_go_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_stub, pattern=r"^stub$"))
    application.add_handler(CallbackQueryHandler(cb_aboutset_menu, pattern=r"^aboutset_menu$"))
    application.add_handler(CallbackQueryHandler(cb_aboutset_remove, pattern=r"^aboutset_remove_\d+$"))

    add_clone_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(cb_clone_add_start, pattern=r"^clone_add$")],
        states={
            WAITING_FOR_TOKEN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_clone_token),
            ],
            WAITING_FOR_SUPABASE_URL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_supabase_url),
            ],
            WAITING_FOR_SUPABASE_KEY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_supabase_key),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_clone_add),
            CallbackQueryHandler(cb_clone_add_cancel, pattern=r"^clone_add_cancel$"),
        ],
    )
    application.add_handler(add_clone_conv)

    settings_input_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(cb_settings_caption_edit, pattern=r"^settings_caption_edit$"),
            CallbackQueryHandler(cb_settings_button_add, pattern=r"^settings_button_add$"),
        ],
        states={
            AWAITING_CAPTION_TEXT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_custom_caption),
            ],
            AWAITING_BUTTON_LINE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_custom_button_line),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_settings_input)],
    )
    application.add_handler(settings_input_conv)

    aboutset_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(cb_aboutset_add, pattern=r"^aboutset_add$")],
        states={
            AWAITING_ABOUT_EXTRA_LINK: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_about_extra_link),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_aboutset)],
    )
    application.add_handler(CommandHandler("aboutset", cmd_aboutset))
    application.add_handler(aboutset_conv)
