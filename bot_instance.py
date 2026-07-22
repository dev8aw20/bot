"""
Per-clone bot instance: every piece of state that is currently a module
global in bot.py becomes `self.` here instead, so N clones can run in one
process without stepping on each other's conversation state or DB pool.

STATUS: this is the converted pattern, not a full mechanical port of all
1600 lines of bot.py. Ported so far: construction, DB pool, owner-state
dict, /start (including batch_ deep-link delivery), the FULL /folders
group, the FULL audio-ingestion + page-rendering group, the FULL
delivery path (force-join gate, _deliver_batch, cancel-send, "UPDATE
CHANNEL" button via the shared UPDATE_CHANNEL_URL env var), the FULL
/forcejoin management group (add/remove/edit-link, chat-join-request
recording, capped at FORCE_JOIN_LIMIT channels per clone), and
auto-delete (job_delete_expired, run every 60s via job_queue — reads
clone_settings.auto_delete_enabled/minutes/message instead of a hardcoded
constant, and actually deletes the messages logged in sent_logs, which
previously never happened). All gated on self.owner_id, i.e. THIS clone's
own owner, not the master bot's admin.

STILL NOT PORTED, so don't be surprised by these:
  - broadcast, episode search (non-owner text fallthrough), /refreshbuttons.

Porting each of those is the same mechanical transform repeated per
remaining group:

    1. Move any `global X` variable referenced by the function into
       __init__ as self.X.
    2. Turn the free function into `async def name(self, update, ctx):`.
    3. Replace bare `db.` calls with `self.db.`.
    4. Replace bare `OWNER_ID` with `self.owner_id`, etc.
    5. Register it in build_application() as
       `app.add_handler(CommandHandler("x", self.cmd_x))`.

I stopped here deliberately rather than machine-porting all 1584 lines
blind — that volume of untested, mechanically-transformed code is exactly
the kind of thing that looks done and isn't. Happy to do the rest in
follow-up passes, function group by function group, so each can actually
be checked.
"""

import asyncio
import html
import json
import logging
import os
import re
from datetime import datetime, timedelta

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    BotCommand, BotCommandScopeDefault, BotCommandScopeChat,
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    ChatJoinRequestHandler, filters, ContextTypes,
)
from telegram.request import HTTPXRequest
from telegram.error import Forbidden

from db import Database

logger = logging.getLogger(__name__)

BATCH_MAX = 50
PAGE_SIZE = 20  # max inline buttons per channel message
FORCE_JOIN_LIMIT = 6  # cap on how many channels a clone owner can force-join to
DEFAULT_AUTO_DELETE_MSG = (
    "\u26a0\ufe0f 📂 𝙵𝙸𝙻𝙴𝚂 𝚆𝙸𝙻𝙻 𝙱𝙴 𝙳𝙴𝙻𝙴𝚃𝙴𝙳 𝙰𝙵𝚃𝙴𝚁 [{minutes} minutes] 𝙿𝙻𝙴𝙰𝚂𝙴 𝚂𝙰𝚅𝙴 𝚃𝙷𝙴𝙼 𝚂𝙾𝙼𝙴𝚆𝙷𝙴𝚁𝙴 𝚂𝙰𝙵𝙴 .."
)  # kept in sync with clone_features.py's copy — shown when the owner hasn't set a custom one

# Same env var bot.py uses for the master's "UPDATE CHANNEL" button — shared
# across master + every clone rather than a per-clone setting, since nothing
# asked for per-clone customization here and clone_settings has no column
# for it. If that changes, move this to clone_settings and read it per-row
# in BotInstance.__init__ instead.
UPDATE_CHANNEL_URL = os.environ.get("UPDATE_CHANNEL_URL", "").strip()
# Same BOT_USERNAME env var bot.py (the master) requires for itself — one
# process, one env, so it's already set. Used to build a deep link back to
# the master bot from inside a clone (see _continue_after_gates below).
# Read here only as a startup fallback: a hand-typed env var is one typo
# away from pointing at a username that doesn't exist (e.g. a stray/missing
# underscore), which would silently break every such deep link and show
# "Bot not found" when tapped. bot.py's post_init verifies the real
# username via get_me() once at startup and calls set_master_bot_username()
# below to correct this value in place — so by the time any clone actually
# serves traffic, this holds the real username regardless of what the env
# var said.
MASTER_BOT_USERNAME = os.environ.get("BOT_USERNAME", "").strip().lstrip("@")


def set_master_bot_username(username: str) -> None:
    """Called once from bot.py's post_init with the Telegram-verified
    username (from application.bot.get_me()). Overrides the env-var-based
    default above for every clone in this process — clones read
    MASTER_BOT_USERNAME as a plain module global, not a constant captured
    at import time, so this takes effect immediately for all of them."""
    global MASTER_BOT_USERNAME
    if username:
        MASTER_BOT_USERNAME = username.strip().lstrip("@")

EPISODE_EXTRACT_PATTERNS = [
    re.compile(r'(?:episode|ep)\.?\s*#?\s*(\d+)', re.IGNORECASE),
    re.compile(r'#\s*(\d+)'),
]
# NOTE: a bare r'(\d+)' tier used to sit here as a last resort. Removed —
# it matched the FIRST number anywhere in the filename/title (bitrate,
# year, track number, quality tag, whatever came first), not necessarily
# the real episode number. Two unrelated files could extract the same
# digit and get wrongly flagged as duplicates. Anything without
# "episode"/"ep"/"#N" now correctly falls through to
# _fallback_name_identifier() below, same as a song with no number in it.

# Same two env vars master_menu.py reads for its own ABOUT page — reused
# here verbatim so the clone's ABOUT shows the same platform support/about
# links as the master bot, not a separately-invented one.
DEFAULT_SUPPORT_GROUP_LINK = os.environ.get("UPDATE_SUPPORT_GROUP", "").strip()
DEFAULT_ANOTHER_BOT_LINK = os.environ.get("OTHER_BOT_URL", "").strip()


def _parse_about_extra_links(raw):
    """Same parsing as master_menu.py's _parse_about_extra_links — reads
    the SAME central bot_settings.about_extra_links row (there's only one,
    set by the master bot owner via /aboutset), so any extra link the
    owner adds there shows up on every clone's ABOUT too."""
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


def _human_file_size(num_bytes):
    if not num_bytes:
        return ""
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{int(size)} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def _render_caption(template, audio_row: dict):
    """Fills {file_name}/{file_size}/{caption} from this clone's own
    CUSTOM CAPTION setting (clone_settings.custom_caption). None if no
    template is set (Telegram then keeps the file with no caption, same
    as today's default). Kept as its own copy rather than imported from
    bot.py — bot_instance.py has no dependency on bot.py's internals
    (see module docstring)."""
    if not template:
        return None
    values = {
        "file_name": audio_row.get("file_name") or "",
        "file_size": _human_file_size(audio_row.get("file_size")),
        "caption": audio_row.get("caption") or "",
    }
    try:
        return template.format(**values)
    except (KeyError, IndexError):
        # Unknown {placeholder} in a saved template — send as-is rather
        # than fail every delivery over one bad caption.
        return template


def _parse_custom_buttons(raw):
    """This clone's own CUSTOM BUTTON setting (clone_settings.custom_buttons):
    one row per line, buttons on a row separated by '|', each button
    'Label - URL'. Malformed lines/buttons are silently skipped, not
    fatal — a typo in one line shouldn't break delivery of every file in
    the batch."""
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
            if not label or not url:
                continue
            row.append(InlineKeyboardButton(label, url=url))
        if row:
            rows.append(row)
    return InlineKeyboardMarkup(rows) if rows else None


class BotInstance:
    def __init__(self, clone_row: dict, central_db: Database):
        """
        clone_row: decrypted row from central_db.get_clone() / list_all_active_clones()
            {id, user_id, bot_token, supabase_url, supabase_key, bot_username, is_active, is_public}
        central_db: connection to the CENTRAL Supabase (user_bots table),
            used here only to record activity/errors back against this clone's row.
        """
        self.clone_id = clone_row["id"]
        self.owner_id = int(clone_row["user_id"])
        self.bot_token = clone_row["bot_token"]
        self.bot_username = clone_row["bot_username"]
        self.is_public = clone_row.get("is_public", True)
        self.central_db = central_db

        # Every clone MUST have its own Supabase — see db.create_clone's
        # docstring for why a shared db is unsafe (no clone_id column
        # anywhere in the schema). Fail loudly here rather than silently
        # falling back to central_db, so a legacy row created before this
        # was enforced surfaces as an obvious startup error, not silent
        # data mixing with the master bot or other clones.
        clone_db_url = clone_row.get("supabase_url")
        if not clone_db_url:
            raise ValueError(
                f"Clone {clone_row.get('id')} (@{clone_row.get('bot_username')}) has no "
                "supabase_url — refusing to start it against the shared central db. "
                "This clone was likely created before per-clone databases were "
                "required; give it its own Supabase project and update its row."
            )
        self.db = Database(clone_db_url)

        # ── formerly module-level globals in bot.py, now per-instance ──
        self.awaiting_new_folder_name: bool = False
        self.awaiting_channel_id_for_folder: int | None = None
        self.awaiting_source_channel_id_for_folder: int | None = None
        self.awaiting_rename_folder_id: int | None = None
        self.new_folder_pending_source: bool = False
        self.pending_broadcast_text: str | None = None
        self.awaiting_force_join_step: str | None = None   # None | "id" | "link"
        self.force_join_pending_channel_id: str | None = None
        self.force_join_pending_title: str | None = None
        self.awaiting_force_join_edit_channel_id: str | None = None
        # Per-clone Settings menu (Custom Caption / Custom Button /
        # Protect Content) — the clone-owner-facing equivalent of
        # master_menu.py's owner-only Settings menu, but scoped to THIS
        # clone's own clone_settings row instead of the master's singleton
        # bot_settings row. See cb_settings_menu below.
        self.awaiting_caption_text: bool = False
        self.awaiting_button_line: bool = False
        self.cancelled_deliveries: set[tuple[int, int]] = set()
        # Per-folder asyncio.Lock so concurrent audio posts to the same
        # source channel don't race on batch total_links/page rendering.
        # Per-instance (not module-level like bot.py's) so two clones'
        # folder id=1 don't share a lock.
        self._folder_ingest_locks: dict[int, asyncio.Lock] = {}

    def _reset_owner_state(self):
        self.awaiting_new_folder_name = False
        self.awaiting_channel_id_for_folder = None
        self.awaiting_source_channel_id_for_folder = None
        self.awaiting_rename_folder_id = None
        self.new_folder_pending_source = False
        self.pending_broadcast_text = None
        self.awaiting_force_join_step = None
        self.force_join_pending_channel_id = None
        self.force_join_pending_title = None
        self.awaiting_force_join_edit_channel_id = None
        self.awaiting_caption_text = False
        self.awaiting_button_line = False

    # ── /start (converted from cmd_start in bot.py) ─────────────────────
    async def cmd_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await self.central_db.touch_clone_activity(self.clone_id)
        settings = await self.central_db.get_clone_settings(self.clone_id)
        user = update.effective_user
        args = ctx.args

        if user.id == self.owner_id:
            # /start doubles as the owner's "cancel" out of any pending
            # text-wizard (caption edit, button-line add, folder rename,
            # etc.) — there's no separate /cancel command wired up in this
            # codebase (see handle_owner_text: it's a single dispatcher on
            # self.awaiting_* flags, not a ConversationHandler), so this is
            # the only exit. Don't tell owners to send /cancel elsewhere;
            # it isn't a registered command and would just be swallowed.
            self._reset_owner_state()

        await self.db.execute(
            """INSERT INTO users (user_id) VALUES ($1)
               ON CONFLICT (user_id) DO UPDATE SET last_seen = NOW()""",
            str(user.id)
        )

        if user.id != self.owner_id and await self.db.is_user_banned(str(user.id)):
            await update.effective_message.reply_text("\U0001F6AB You are banned from using this bot.")
            return

        if user.id == self.owner_id and (not args or not args[0].startswith("batch_")):
            label = "the owner" if settings["hide_owner"] else f"owner of @{self.bot_username}"
            await update.effective_message.reply_text(
                f"Welcome back, {label}.\n\nUse /folders to manage your folders.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("\U0001F4C1 Manage Folders", callback_data="folder_list")],
                    [InlineKeyboardButton("\u2699\ufe0f Settings", callback_data="cs_menu")],
                ]),
            )
            return

        if not self.is_public and user.id != self.owner_id:
            await update.effective_message.reply_text(
                "This bot is private — only the owner can use it."
            )
            return

        batch_id = int(args[0].replace("batch_", "")) if args and args[0].startswith("batch_") else None
        if not await self._check_force_join(update, ctx, batch_id):
            return

        await self._continue_after_gates(update, ctx, settings, args)

    def _resolve_ban_target(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> tuple[str | None, str | None]:
        """Mirrors bot.py's helper of the same purpose: `/ban 12345 [reason]`,
        or a reply to the target's message with `/ban [reason]`."""
        reply = update.message.reply_to_message if update.message else None
        if reply and reply.from_user:
            reason = " ".join(ctx.args).strip() if ctx.args else None
            return str(reply.from_user.id), (reason or None)
        if ctx.args and ctx.args[0].strip().isdigit():
            target = ctx.args[0].strip()
            reason = " ".join(ctx.args[1:]).strip() if len(ctx.args) > 1 else None
            return target, (reason or None)
        return None, None

    async def _notify_banned_user(self, ctx: ContextTypes.DEFAULT_TYPE, target: str, reason: str | None):
        """Best-effort DM via THIS clone's own bot — never lets a failure
        here (blocked bot, user never started this clone, etc.) undo the
        ban itself, which already happened in the DB before this runs."""
        text = "\U0001F6AB You have been banned from this bot."
        if reason:
            text += f"\n\nReason: {reason}"
        try:
            await ctx.bot.send_message(chat_id=int(target), text=text)
        except Forbidden:
            logger.warning("Clone %s: couldn't notify banned user %s — bot blocked/not started", self.clone_id, target)

    # ── /ban /unban — THIS clone's owner only, scoped to THIS clone's own
    # users table (self.db, not central_db) — has no effect on the master
    # bot or any other clone. Master-owner-only clone-level banning
    # (banning the whole bot, not one user) is /cban /cunban in bot.py. ──
    async def cmd_ban(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != self.owner_id:
            return
        target, reason = self._resolve_ban_target(update, ctx)
        if not target:
            await update.message.reply_text(
                "Usage: /ban <user_id> [reason] — or reply to their message with /ban [reason]."
            )
            return
        if target == str(self.owner_id):
            await update.message.reply_text("Can't ban the owner.")
            return
        await self.db.ban_user(target)
        await self._notify_banned_user(ctx, target, reason)
        reply = f"\U0001F6AB Banned user {target}."
        if reason:
            reply += f" Reason: {reason}"
        await update.message.reply_text(reply)

    async def cmd_unban(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != self.owner_id:
            return
        target, _reason = self._resolve_ban_target(update, ctx)
        if not target:
            await update.message.reply_text("Usage: /unban <user_id> — or reply to their message with /unban.")
            return
        await self.db.unban_user(target)
        await update.message.reply_text(f"\u2705 Unbanned user {target}.")

    def _start_menu_markup(self) -> InlineKeyboardMarkup | None:
        """Buttons shown under the /start message: HELP + ABOUT on one row,
        UPDATE CHANNEL on its own row (only if UPDATE_CHANNEL_URL is set),
        and the existing CREATE MY OWN CLONE row underneath. Also reused as
        the "‹ back" target from cb_help / cb_about."""
        rows = [[
            InlineKeyboardButton("\u2139\ufe0f HELP", callback_data="start_help"),
            InlineKeyboardButton("\U0001F4DC ABOUT", callback_data="start_about"),
        ]]
        if MASTER_BOT_USERNAME:
            rows.append([InlineKeyboardButton(
                "CREATE MY OWN CLONE",
                url=f"https://t.me/{MASTER_BOT_USERNAME}?start=manage_clones",
            )])
        if UPDATE_CHANNEL_URL:
            rows.append([InlineKeyboardButton("\U0001F4DF UPDATE CHANNEL", url=UPDATE_CHANNEL_URL)])
       
        return InlineKeyboardMarkup(rows) if rows else None

    async def _continue_after_gates(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE, settings: dict, args: list):
        """Whatever cmd_start does once the owner/private/force-join checks
        are done — factored out so cb_checkjoin can resume here after the
        user clears the force-join gate, instead of duplicating the
        start_msg / batch-delivery branching. The force-join check itself
        already ran (in cmd_start, or in cb_checkjoin before calling this)
        — do NOT re-check here, or a re-verify inside cb_checkjoin loops."""
        if not args or not args[0].startswith("batch_"):
            user = update.effective_user
            text = settings["start_msg"] or (
                f"Hello {user.first_name} \u2728\n\n"
                "Send me a shared link to get your files, or ask the owner "
                "of this bot for one."
            )
            await update.effective_message.reply_text(text, reply_markup=self._start_menu_markup())
            return

        batch_id = int(args[0].replace("batch_", ""))
        await self._deliver_batch(batch_id, update.effective_chat.id, update.effective_user.id, ctx)

    # ── HELP / ABOUT (new: shown as buttons under /start) ────────────────
    async def cb_help(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        text = (
            f"\U0001F435 Help Menu\n\n"
            f"\u2139\ufe0f <b>This Bot:</b> @{html.escape(self.bot_username)}\n"
            + (f"\U0001F916 <b>Master Bot:</b> @{html.escape(MASTER_BOT_USERNAME)}\n" if MASTER_BOT_USERNAME else "")
            + "\n"
            "I am a permanent file store bot. Send me a shared link to get "
            "your files.\n\n"
            "Available Commands: /start\n"
            "If the bot asks you to join a channel first, join it and "
            "press \u201cTry Again\u201d, then send the link again."
        )
        await query.edit_message_text(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("\u2039 back", callback_data="start_back")]]
            ),
        )

    async def cb_about(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        # ABOUT reuses the master bot's own about content (support group,
        # other-bot link, owner-managed extra links via /aboutset on the
        # master) instead of a separate per-clone text — same source of
        # truth as master_menu.py's cb_about, just with this clone's own
        # name (and the master's) prefixed on top.
        settings = await self.central_db.get_bot_settings()

        lines = [f"\u2139\ufe0f <b>This Bot:</b> @{html.escape(self.bot_username)}"]
        if MASTER_BOT_USERNAME:
            lines.append(f"\U0001F916 <b>Master Bot:</b> @{html.escape(MASTER_BOT_USERNAME)}")
        if DEFAULT_SUPPORT_GROUP_LINK:
            lines.append(f'\nSupport group: <a href="{html.escape(DEFAULT_SUPPORT_GROUP_LINK)}">(Link)</a>')
        if DEFAULT_ANOTHER_BOT_LINK:
            lines.append(f'\nAnother bot: <a href="{html.escape(DEFAULT_ANOTHER_BOT_LINK)}">(Link)</a>')
        for label, url in _parse_about_extra_links(settings["about_extra_links"]):
            lines.append(f'\n{html.escape(label)}: <a href="{html.escape(url)}">(Link)</a>')

        await query.edit_message_text(
            "\n".join(lines),
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("\u2039 back", callback_data="start_back")]]
            ),
        )

    async def cb_start_back(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        settings = await self.central_db.get_clone_settings(self.clone_id)
        user = query.from_user
        text = settings["start_msg"] or (
            f"Hello {user.first_name} \u2728\n\n"
            "Send me a shared link to get your files, or ask the owner "
            "of this bot for one."
        )
        await query.edit_message_text(text, reply_markup=self._start_menu_markup())

    # ── SETTINGS (owner-only, THIS clone's own clone_settings row) ───────
    # The clone-owner-facing equivalent of master_menu.py's Settings menu.
    # That one edits the master bot's singleton bot_settings row and is
    # gated to the MASTER bot's OWNER_ID — it must never be reachable from
    # here. This one edits clone_settings WHERE clone_id = self.clone_id
    # and is gated to self.owner_id, THIS clone's own owner. Mirrors the
    # UI/UX of master_menu.py's screen but scoped per-clone, and uses the
    # same handle_owner_text single-flag pattern as the folder/forcejoin
    # flows above (no ConversationHandler — see module docstring).
    async def cb_settings_menu(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        if q.from_user.id != self.owner_id:
            await q.answer("\u26d4 Only the bot owner can use Settings.", show_alert=True)
            return
        await q.answer()
        settings = await self.central_db.get_clone_settings(self.clone_id)
        protect_label = (
            "PROTECT CONTENT \u2611\ufe0f" if settings["no_forward_enabled"]
            else "PROTECT CONTENT \u2610"
        )
        buttons = [
            [InlineKeyboardButton("CUSTOM CAPTION \U0001F58A\ufe0f", callback_data="cs_caption_menu"),
             InlineKeyboardButton("CUSTOM BUTTON \u2728", callback_data="cs_button_menu")],
            [InlineKeyboardButton(protect_label, callback_data="cs_protect_toggle"),
             InlineKeyboardButton("\u2039 back", callback_data="start_back")],
        ]
        await q.edit_message_text(
            "\u2699\ufe0f Settings... Customize your bot's settings as your need",
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    async def cb_settings_protect_toggle(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        if q.from_user.id != self.owner_id:
            await q.answer("Not allowed.", show_alert=True)
            return
        settings = await self.central_db.get_clone_settings(self.clone_id)
        new_state = not settings["no_forward_enabled"]
        await self.central_db.update_clone_settings(self.clone_id, no_forward_enabled=new_state)
        await q.answer("Enabled." if new_state else "Disabled.")
        await self.cb_settings_menu(update, ctx)

    async def cb_settings_caption_menu(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        if q.from_user.id != self.owner_id:
            await q.answer("Not allowed.", show_alert=True)
            return
        await q.answer()
        text = (
            "Custom Caption: add a custom caption to your media messages "
            "instead of its original caption.\n\n"
            "Fillings:\n"
            "\u2022 {file_name}: File Name\n"
            "\u2022 {file_size}: File size\n"
            "\u2022 {caption}: Original Caption"
        )
        buttons = [
            [InlineKeyboardButton("Edit", callback_data="cs_caption_edit"),
             InlineKeyboardButton("See", callback_data="cs_caption_see")],
            [InlineKeyboardButton("Delete", callback_data="cs_caption_delete")],
            [InlineKeyboardButton("\u2039 back", callback_data="cs_menu")],
        ]
        await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(buttons))

    async def cb_settings_caption_edit(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        if q.from_user.id != self.owner_id:
            await q.answer("Not allowed.", show_alert=True)
            return
        await q.answer()
        self.awaiting_caption_text = True
        await q.edit_message_text(
            "Send the new caption template. You can use {file_name}, "
            "{file_size}, {caption}. Send /start to cancel."
        )

    async def cb_settings_caption_see(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        if q.from_user.id != self.owner_id:
            await q.answer("Not allowed.", show_alert=True)
            return
        await q.answer()
        settings = await self.central_db.get_clone_settings(self.clone_id)
        current = settings["custom_caption"] or "(not set — original captions are used as-is)"
        await q.message.reply_text(
            current,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("\u2039 back", callback_data="cs_caption_menu")]]
            ),
        )

    async def cb_settings_caption_delete(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        if q.from_user.id != self.owner_id:
            await q.answer("Not allowed.", show_alert=True)
            return
        await self.central_db.update_clone_settings(self.clone_id, custom_caption=None)
        await q.answer("Deleted.")
        await self.cb_settings_caption_menu(update, ctx)

    async def cb_settings_button_menu(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        if q.from_user.id != self.owner_id:
            await q.answer("Not allowed.", show_alert=True)
            return
        await q.answer()
        settings = await self.central_db.get_clone_settings(self.clone_id)
        preview = _parse_custom_buttons(settings["custom_buttons"])
        rows = list(preview.inline_keyboard) if preview else []
        rows.append([InlineKeyboardButton("\u2795", callback_data="cs_button_add")])
        rows.append([InlineKeyboardButton("Delete", callback_data="cs_button_delete")])
        rows.append([InlineKeyboardButton("\u2039 back", callback_data="cs_menu")])
        text = "Custom Button: add a custom button to your media messages"
        if not preview:
            text += "\n\n(none set yet)"
        await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows))

    async def cb_settings_button_add(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        if q.from_user.id != self.owner_id:
            await q.answer("Not allowed.", show_alert=True)
            return
        await q.answer()
        self.awaiting_button_line = True
        await q.edit_message_text(
            "Send a new button row: \"Label - URL\", or two on the same row "
            "with \"Label1 - URL1 | Label2 - URL2\". This is ADDED as a new "
            "row below your existing buttons. Send /start to cancel."
        )

    async def cb_settings_button_delete(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        if q.from_user.id != self.owner_id:
            await q.answer("Not allowed.", show_alert=True)
            return
        await self.central_db.update_clone_settings(self.clone_id, custom_buttons=None)
        await q.answer("Deleted.")
        await self.cb_settings_button_menu(update, ctx)


    # ── Force-join gate (converted from _has_join_request / _is_member /
    # _check_force_join / cb_checkjoin). Reads force_join_channels, which
    # the /forcejoin management group below now populates per-clone,
    # capped at FORCE_JOIN_LIMIT rows. Still passes through if a clone
    # owner hasn't added any channels yet. ───────────────────────────────
    async def _has_join_request(self, channel_id: str, user_id: int) -> bool:
        row = await self.db.fetchrow(
            "SELECT 1 FROM join_requests WHERE channel_id = $1 AND user_id = $2",
            channel_id, str(user_id)
        )
        return row is not None

    async def _is_member(self, bot, channel_id: str, user_id: int) -> bool:
        try:
            member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            if member.status not in ("left", "kicked"):
                return True
        except Exception as e:
            logger.warning("get_chat_member failed for channel %s, user %s: %s", channel_id, user_id, e)
        return await self._has_join_request(channel_id, user_id)

    async def _check_force_join(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE, batch_id: int | None) -> bool:
        """Returns True if the user may proceed. Otherwise sends a join
        prompt and returns False. This clone's owner always bypasses."""
        user = update.effective_user
        if user.id == self.owner_id:
            return True

        channels = await self.db.fetch(
            "SELECT id, channel_id, invite_link, title FROM force_join_channels ORDER BY id"
        )
        if not channels:
            return True

        not_joined = [c for c in channels if not await self._is_member(ctx.bot, c["channel_id"], user.id)]
        if not not_joined:
            return True

        recheck_data = f"checkjoin_{batch_id}" if batch_id is not None else "checkjoin_0"
        await self._send_join_prompt(
            update, ctx,
            invite_links=[c["invite_link"] for c in not_joined],
            recheck_data=recheck_data,
            not_joined_alert="You have not joined all the channels yet.",
        )
        return False

    async def _send_join_prompt(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                                 invite_links: list[str], recheck_data: str, not_joined_alert: str):
        """Shared rendering for both the /forcejoin gate (possibly several
        channels) and the force-sub gate (always exactly one channel) so
        the two look and behave identically to the user — same button
        style, same copy, same "Try Again" mechanics."""
        rows = [
            [InlineKeyboardButton(f"\U0001F517 Join Channel {i}", url=url)]
            for i, url in enumerate(invite_links, start=1)
        ]
        rows.append([InlineKeyboardButton("\U0001F504 Try Again", callback_data=recheck_data)])

        arrows = " ".join(["\u2b07\ufe0f"] * min(len(invite_links) * 3, 9))
        text = (
            f"\u2764\ufe0f HEY THERE \u2728\n\n"
            f"\U0001F525 TO USE THIS BOT, YOU MUST\n"
            f"JOIN ALL [{len(invite_links)}] CHANNELS.\n\n"
            f"\U0001F447 JOIN ALL CHANNELS AND\n"
            f"PRESS \"TRY AGAIN\".\n\n"
            f"{arrows}\n\n"
            f"\u26a0\ufe0f If a channel is private, you'll need to send a join request "
            f"(no need to wait for approval — as soon as you've sent the "
            f"request, press \"Try Again\")."
        )
        if update.callback_query:
            await update.callback_query.answer(not_joined_alert, show_alert=True)
            await update.callback_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(rows))
        else:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(rows))

    async def cb_checkjoin(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        data = update.callback_query.data.replace("checkjoin_", "")
        batch_id = int(data) if data != "0" else None

        ok = await self._check_force_join(update, ctx, batch_id)
        if not ok:
            return

        await update.callback_query.answer("\u2705 Verified!")
        settings = await self.central_db.get_clone_settings(self.clone_id)
        args = [f"batch_{batch_id}"] if batch_id is not None else []
        await self._continue_after_gates(update, ctx, settings, args)

    # ── Batch delivery (converted from _deliver_batch / cb_cancel_send).
    # Auto-delete is now real: if clone_settings.auto_delete_enabled is
    # True, the closing message's deletion line uses the configured
    # minutes/custom message, sent_logs gets a row, and job_delete_expired
    # (registered in build_application) sweeps and deletes it on schedule.
    # If disabled, no deletion warning is shown and no sent_logs row is
    # written at all. ─────────────────────────────────────────────────────
    async def _deliver_batch(self, batch_id: int, chat_id: int, user_id_int: int, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = str(user_id_int)

        batch = await self.db.fetchrow("SELECT id, total_links FROM batches WHERE id = $1", batch_id)
        if not batch:
            await ctx.bot.send_message(chat_id=chat_id, text="\u274c This collection does not exist.")
            return

        settings = await self.central_db.get_clone_settings(self.clone_id)
        auto_delete_on = settings["auto_delete_enabled"]
        auto_delete_minutes = settings["auto_delete_minutes"]
        custom_markup = _parse_custom_buttons(settings["custom_buttons"])

        def _closing_text():
            hands = " ".join(["\U0001F590\ufe0f"] * 8)
            if auto_delete_on:
                warning = settings["auto_delete_message"] or DEFAULT_AUTO_DELETE_MSG
                try:
                    warning = warning.format(minutes=auto_delete_minutes)
                except (KeyError, IndexError):
                    pass  # bad {placeholder} in a saved custom message — show as-is
                body = f"\U0001F4C1 {warning}\nTO GET IT AGAIN, REPEAT THE SAME PROCESS.\n\n"
            else:
                body = "TO GET IT AGAIN, REPEAT THE SAME PROCESS.\n\n"
            return f"\u2764\ufe0f HEY BRO \u2b07\ufe0f\n\n{body}{hands}"

        wait_rows = [[InlineKeyboardButton("\u2022 Cancel", callback_data=f"cancelsend_{batch_id}")]]
        if UPDATE_CHANNEL_URL:
            wait_rows.append([InlineKeyboardButton("\U0001F4DF UPDATE CHANNEL", url=UPDATE_CHANNEL_URL)])

        warn = await ctx.bot.send_message(
            chat_id=chat_id,
            text="\u23f3 Please wait...",
            reply_markup=InlineKeyboardMarkup(wait_rows)
        )

        audios = await self.db.fetch(
            "SELECT id, telegram_file_id, file_name, file_size, caption "
            "FROM audios WHERE batch_id = $1 "
            "ORDER BY CASE WHEN episode_no ~ '^[0-9]+$' THEN 0 ELSE 1 END, "
            "CASE WHEN episode_no ~ '^[0-9]+$' THEN episode_no::int END, id",
            batch_id
        )
        sent_ids = []

        failed_audios = []
        uncached_missing = []
        MAX_ATTEMPTS = 3
        RETRY_DELAY = 3
        cancel_key = (chat_id, batch_id)
        was_cancelled = False
        sent_audio_count = 0

        for audio in audios:
            if cancel_key in self.cancelled_deliveries:
                was_cancelled = True
                break

            msg = None

            if not audio["telegram_file_id"]:
                uncached_missing.append(audio["id"])
                failed_audios.append(audio["id"])
                continue

            caption = _render_caption(settings["custom_caption"], audio)

            for attempt in range(1, MAX_ATTEMPTS + 1):
                try:
                    msg = await ctx.bot.send_audio(
                        chat_id=chat_id,
                        audio=audio["telegram_file_id"],
                        caption=caption,
                        protect_content=settings["no_forward_enabled"],
                        reply_markup=custom_markup,
                    )
                    break
                except Exception as e:
                    logger.error(
                        "Audio %s attempt %s/%s — SEND failed: %s",
                        audio["id"], attempt, MAX_ATTEMPTS, e,
                    )
                    if attempt < MAX_ATTEMPTS:
                        await asyncio.sleep(RETRY_DELAY)

            if msg is not None:
                sent_ids.append(msg.message_id)
                sent_audio_count += 1
            else:
                failed_audios.append(audio["id"])

        self.cancelled_deliveries.discard(cancel_key)
        try:
            await ctx.bot.delete_message(chat_id=chat_id, message_id=warn.message_id)
        except Exception as e:
            logger.warning("Could not remove please-wait message for batch %s: %s", batch_id, e)

        if was_cancelled:
            if sent_audio_count > 0:
                closing_rows = None
                if UPDATE_CHANNEL_URL:
                    closing_rows = InlineKeyboardMarkup(
                        [[InlineKeyboardButton("\U0001F4DF UPDATE CHANNEL", url=UPDATE_CHANNEL_URL)]]
                    )
                closing = await ctx.bot.send_message(
                    chat_id=chat_id,
                    text=_closing_text(),
                    reply_markup=closing_rows,
                )
                sent_ids.append(closing.message_id)
        else:
            if uncached_missing:
                await ctx.bot.send_message(chat_id=chat_id, text="\u26a0\ufe0f This audio is not available right now.")

            other_failures = len(failed_audios) - len(uncached_missing)
            if other_failures > 0:
                await ctx.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"\u26a0\ufe0f Failed to send {other_failures}/{len(audios)} audio files "
                        f"(even after {MAX_ATTEMPTS} attempts). Please try /start again."
                    )
                )

            if sent_audio_count > 0:
                closing_rows = None
                if UPDATE_CHANNEL_URL:
                    closing_rows = InlineKeyboardMarkup(
                        [[InlineKeyboardButton("\U0001F4DF UPDATE CHANNEL", url=UPDATE_CHANNEL_URL)]]
                    )
                closing = await ctx.bot.send_message(
                    chat_id=chat_id,
                    text=_closing_text(),
                    reply_markup=closing_rows,
                )
                sent_ids.append(closing.message_id)

        if not sent_ids:
            return

        if auto_delete_on:
            delete_at = datetime.utcnow() + timedelta(minutes=auto_delete_minutes)
            await self.db.execute(
                "INSERT INTO sent_logs (user_id, batch_id, message_ids, delete_at) VALUES ($1,$2,$3,$4)",
                user_id, batch_id, json.dumps(sent_ids), delete_at
            )

    async def job_delete_expired(self, ctx: ContextTypes.DEFAULT_TYPE):
        """Registered in build_application() via job_queue.run_repeating.
        This is the piece that was missing entirely (see module docstring)
        — _deliver_batch wrote sent_logs rows and told the user files
        would be deleted, but nothing ever read them and did it."""
        rows = await self.db.fetch(
            "SELECT id, user_id, message_ids FROM sent_logs WHERE delete_at <= NOW()"
        )
        for row in rows:
            chat_id = int(row["user_id"])  # private-chat delivery: chat_id == user_id
            for message_id in json.loads(row["message_ids"]):
                try:
                    await ctx.bot.delete_message(chat_id=chat_id, message_id=message_id)
                except Exception as e:
                    # Already deleted, user blocked the bot, Telegram's 48h
                    # delete window passed — none of these should leave the
                    # row stuck so this job retries it forever.
                    logger.debug(
                        "Auto-delete: couldn't delete message %s in chat %s: %s",
                        message_id, chat_id, e,
                    )
            await self.db.execute("DELETE FROM sent_logs WHERE id = $1", row["id"])

    async def cb_cancel_send(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Marks a batch delivery as cancelled. Checked once per audio,
        between sends — doesn't abort an upload already in progress."""
        batch_id = int(update.callback_query.data.replace("cancelsend_", ""))
        self.cancelled_deliveries.add((update.effective_chat.id, batch_id))
        await update.callback_query.answer("Cancelling after the current file finishes...")

    # ── /forcejoin management group (converted from bot.py's cmd_forcejoin
    # and everything it fans out to). Gated on self.owner_id. The gate that
    # ENFORCES these channels (_check_force_join above) already existed and
    # reads force_join_channels from this clone's own db — this group is
    # what actually lets the owner populate that table, capped at
    # FORCE_JOIN_LIMIT channels so one clone owner can't force-sub users to
    # an unbounded channel list. ─────────────────────────────────────────
    async def cmd_forcejoin(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != self.owner_id:
            return
        await self._show_force_join_management(update, ctx)

    async def _show_force_join_management(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        channels = await self.db.fetch("SELECT id, title, channel_id FROM force_join_channels ORDER BY id")
        rows = [
            [
                InlineKeyboardButton(f"\u274c {c['title'] or c['channel_id']}", callback_data=f"forcejoin_remove_{c['id']}"),
                InlineKeyboardButton("\u270f\ufe0f Edit Link", callback_data=f"forcejoin_editlink_{c['id']}"),
            ]
            for c in channels
        ]
        if len(channels) < FORCE_JOIN_LIMIT:
            rows.append([InlineKeyboardButton("\u2795 Add Channel/Group", callback_data="forcejoin_add")])

        if channels:
            text = (
                f"\U0001F512 *Force Join Channels* ({len(channels)}/{FORCE_JOIN_LIMIT})\n\n"
                "Tap to remove, or add a new one:"
            )
        else:
            text = f"\U0001F512 No force-join channel set yet (0/{FORCE_JOIN_LIMIT}).\n\n\u2795 Start with Add Channel/Group."

        if update.callback_query:
            await update.callback_query.edit_message_text(
                text, reply_markup=InlineKeyboardMarkup(rows), parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(
                text, reply_markup=InlineKeyboardMarkup(rows), parse_mode="Markdown"
            )

    async def cb_forcejoin_list(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != self.owner_id:
            return
        await self._show_force_join_management(update, ctx)

    async def cb_forcejoin_add(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != self.owner_id:
            return
        count = await self.db.fetchval("SELECT COUNT(*) FROM force_join_channels")
        if count >= FORCE_JOIN_LIMIT:
            await update.callback_query.answer(
                f"\u26a0\ufe0f Limit reached — max {FORCE_JOIN_LIMIT} force-join channels. "
                "Remove one before adding another.",
                show_alert=True,
            )
            return
        self._reset_owner_state()
        self.awaiting_force_join_step = "id"
        await update.callback_query.edit_message_text(
            "\U0001F4E1 Send the Channel/Group ID or @username (e.g. @channelusername or -100xxxxxxxxxx).\n\n"
            "\u26a0\ufe0f The bot must be made an admin there (to see members, and to receive join "
            "requests — the bot will NOT approve them, only record them)."
        )

    async def cb_forcejoin_remove(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != self.owner_id:
            return
        row_id = int(update.callback_query.data.replace("forcejoin_remove_", ""))
        await self.db.execute("DELETE FROM force_join_channels WHERE id = $1", row_id)
        await self._show_force_join_management(update, ctx)

    async def cb_forcejoin_editlink(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != self.owner_id:
            return
        self._reset_owner_state()
        row_id = int(update.callback_query.data.replace("forcejoin_editlink_", ""))
        row = await self.db.fetchrow(
            "SELECT id, channel_id, title FROM force_join_channels WHERE id = $1", row_id
        )
        if not row:
            await update.callback_query.answer("\u26a0\ufe0f Channel not found (it may already have been removed).", show_alert=True)
            await self._show_force_join_management(update, ctx)
            return
        self.awaiting_force_join_edit_channel_id = row["channel_id"]
        await update.callback_query.edit_message_text(
            f"\U0001F517 Send a new invite link for \"{row['title'] or row['channel_id']}\".\n\n"
            "\u26a0\ufe0f If the link is expiring or showing 'invalid', keep both the expiry date "
            "and member limit OFF/blank when creating a new link in Telegram — otherwise "
            "it will go invalid again after some time/uses."
        )

    async def cb_chat_join_request(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Records join requests for THIS clone, for whatever chat sent
        one — no longer gated on the chat being a registered
        force_join_channels row, because force_sub's channel (a single ID
        in clone_settings, not that table) needs the same recording now
        that _check_force_sub falls back to _has_join_request too. A
        recorded request for a channel nobody is gating on is harmless
        (never read), so it's simpler and less fragile than resolving and
        matching both channel_id formats (username vs numeric) here just
        to decide whether to store it. Does NOT approve requests —
        approval is left to the clone owner (manually, in Telegram)."""
        req = update.chat_join_request
        chat_id_str = str(req.chat.id)
        try:
            await self.db.execute(
                """INSERT INTO join_requests (channel_id, user_id)
                   VALUES ($1, $2)
                   ON CONFLICT (channel_id, user_id) DO NOTHING""",
                chat_id_str, str(req.from_user.id)
            )
        except Exception as e:
            logger.warning("Failed to record join request for clone %s, chat %s, user %s: %s",
                            self.clone_id, chat_id_str, req.from_user.id, e)

    # ── /folders group (converted from bot.py's cmd_folders and everything
    # it fans out to: detail view, output/source channel setting with live
    # Telegram verification, and the text-wizard those trigger). All of it
    # is gated on self.owner_id, which is THIS clone's owner (from
    # clone_row["user_id"]), not the master bot's admin — so each clone
    # owner already gets independent access to their own folders, they
    # were just missing everything past folder creation. ────────────────
    async def cmd_folders(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != self.owner_id:
            return
        await self._show_folder_management(update, ctx)

    async def _show_folder_management(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        folders = await self.db.fetch(
            "SELECT id, name, channel_id, source_channel_id FROM folders ORDER BY name"
        )
        rows = []
        for f in folders:
            missing = []
            if not f["channel_id"]:
                missing.append("no output channel")
            if not f["source_channel_id"]:
                missing.append("no source channel")
            label = f["name"] if not missing else f"{f['name']} (\u26a0\ufe0f {', '.join(missing)})"
            rows.append([InlineKeyboardButton(label, callback_data=f"folder_manage_{f['id']}")])
        rows.append([InlineKeyboardButton("\u2795 New Folder", callback_data="folder_new")])

        text = "\U0001F4C1 *Folders*\n\nTap to manage, or create a new one:" if folders \
            else "\U0001F4C1 No folders yet.\n\n\u2795 Start with New Folder."

        if update.callback_query:
            await update.callback_query.edit_message_text(
                text, reply_markup=InlineKeyboardMarkup(rows), parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(
                text, reply_markup=InlineKeyboardMarkup(rows), parse_mode="Markdown"
            )

    async def cb_folder_new(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        if q.from_user.id != self.owner_id:
            return
        self._reset_owner_state()
        self.awaiting_new_folder_name = True
        await q.edit_message_text("\U0001F4C1 Send the name for the new folder:")

    async def cb_folder_manage(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        if q.from_user.id != self.owner_id:
            return
        folder_id = int(q.data.replace("folder_manage_", ""))
        folder = await self.db.fetchrow(
            "SELECT id, name, channel_id, source_channel_id FROM folders WHERE id = $1", folder_id
        )
        if not folder:
            await q.edit_message_text("\u274c Folder not found.")
            return

        channel_line = folder["channel_id"] or "\u26a0\ufe0f not set"
        source_line = folder["source_channel_id"] or "\u26a0\ufe0f not set"
        text = (
            f"\U0001F4C1 *{folder['name']}*\n\n"
            f"\U0001F4E4 Output channel (buttons posted here): `{channel_line}`\n"
            f"\U0001F4E5 Source channel (bot reads audio from here): `{source_line}`"
        )
        rows = [
            [InlineKeyboardButton("\U0001F4DD Rename Folder", callback_data=f"folder_rename_{folder_id}")],
            [InlineKeyboardButton("\u270f\ufe0f Update Output Channel", callback_data=f"folder_setchannel_{folder_id}")],
            [InlineKeyboardButton("\u270f\ufe0f Update Source Channel", callback_data=f"folder_setsource_{folder_id}")],
            [InlineKeyboardButton("\U0001F5D1\ufe0f Delete Folder", callback_data=f"folder_delete_{folder_id}")],
            [InlineKeyboardButton("\u2b05\ufe0f Back", callback_data="folder_list")],
        ]
        await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows), parse_mode="Markdown")

    async def cb_folder_rename(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        if q.from_user.id != self.owner_id:
            return
        folder_id = int(q.data.replace("folder_rename_", ""))
        folder = await self.db.fetchrow("SELECT name FROM folders WHERE id = $1", folder_id)
        if not folder:
            await q.edit_message_text("\u274c Folder not found.")
            return
        self._reset_owner_state()
        self.awaiting_rename_folder_id = folder_id
        await q.edit_message_text(f"\U0001F4DD Send the new name for \"{folder['name']}\":")

    async def cb_folder_delete_confirm(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """First tap — asks for confirmation, doesn't delete anything yet.
        This is destructive: it wipes the folder's batches and audio
        records too (delete_folder_cascade), not just the folder row."""
        q = update.callback_query
        await q.answer()
        if q.from_user.id != self.owner_id:
            return
        folder_id = int(q.data.replace("folder_delete_", ""))
        folder = await self.db.fetchrow("SELECT name FROM folders WHERE id = $1", folder_id)
        if not folder:
            await q.edit_message_text("\u274c Folder not found.")
            return
        rows = [
            [InlineKeyboardButton("\u2705 Yes, delete it", callback_data=f"folder_delete_yes_{folder_id}")],
            [InlineKeyboardButton("\u274c Cancel", callback_data=f"folder_manage_{folder_id}")],
        ]
        await q.edit_message_text(
            f"\u26a0\ufe0f Delete *{folder['name']}*?\n\n"
            f"This permanently removes the folder AND every batch/audio "
            f"record under it. This cannot be undone.",
            reply_markup=InlineKeyboardMarkup(rows), parse_mode="Markdown",
        )

    async def cb_folder_delete_execute(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        if q.from_user.id != self.owner_id:
            return
        folder_id = int(q.data.replace("folder_delete_yes_", ""))
        folder = await self.db.fetchrow("SELECT name FROM folders WHERE id = $1", folder_id)
        if not folder:
            await q.answer()
            await q.edit_message_text("\u274c Folder not found.")
            return
        try:
            await self.db.delete_folder_cascade(folder_id)
        except Exception:
            logger.exception("Failed to delete folder %s", folder_id)
            await q.answer("\u274c Delete failed — check logs.", show_alert=True)
            return
        await q.answer("Deleted.")
        await self._show_folder_management(update, ctx)

    async def cb_folder_list(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        if q.from_user.id != self.owner_id:
            return
        await self._show_folder_management(update, ctx)

    async def cb_folder_setchannel(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        if q.from_user.id != self.owner_id:
            return
        folder_id = int(q.data.replace("folder_setchannel_", ""))
        self._reset_owner_state()
        self.awaiting_channel_id_for_folder = folder_id
        await q.edit_message_text(
            "\U0001F4E1 Send the Channel ID (e.g. @channelusername or -100xxxxxxxxxx).\n\n"
            "\u26a0\ufe0f The bot must be made an admin in that channel (with Post Messages permission)."
        )

    async def cb_folder_setsource(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        if q.from_user.id != self.owner_id:
            return
        folder_id = int(q.data.replace("folder_setsource_", ""))
        self._reset_owner_state()
        self.awaiting_source_channel_id_for_folder = folder_id
        await q.edit_message_text(
            "\U0001F4E5 Send the *source* Channel ID (e.g. @channelusername or -100xxxxxxxxxx) — "
            "this is the private channel the bot will watch for new audio.\n\n"
            "\u26a0\ufe0f The bot must be made an admin in that channel (any admin right is enough — "
            "it only needs to *read* posts there, not send).",
            parse_mode="Markdown"
        )

    def _get_folder_lock(self, folder_id: int) -> asyncio.Lock:
        lock = self._folder_ingest_locks.get(folder_id)
        if lock is None:
            lock = asyncio.Lock()
            self._folder_ingest_locks[folder_id] = lock
        return lock

    async def _repost_all_pages_for_folder(self, folder_id, folder_name, new_channel_id, update, ctx):
        batches = await self.db.fetch(
            "SELECT id, total_links FROM batches WHERE folder_id = $1 ORDER BY id",
            folder_id
        )
        if not batches:
            await update.message.reply_text("\u2139\ufe0f This folder has no batches yet — nothing to repost.")
            return

        total_pages = (len(batches) + PAGE_SIZE - 1) // PAGE_SIZE
        await update.message.reply_text(
            f"\U0001F501 Reposting {total_pages} message(s) to the new channel... this will take some time."
        )

        REPOST_DELAY = 2
        success_count = 0
        failed_pages = []

        for page_index in range(1, total_pages + 1):
            try:
                # New channel = old message_id is invalid there, so
                # force_new=True skips the edit attempt and sends fresh.
                await self.render_folder_page(
                    folder_id, folder_name, new_channel_id, page_index, ctx, force_new=True
                )
                success_count += 1
            except Exception as e:
                logger.error("Repost failed for folder %s page %s: %s", folder_id, page_index, e)
                failed_pages.append(page_index)

            await asyncio.sleep(REPOST_DELAY)

        summary = f"\u2705 {success_count}/{total_pages} messages reposted to the new channel."
        if failed_pages:
            summary += f"\n\u26a0\ufe0f Failed: page #{', #'.join(str(i) for i in failed_pages)}"
        await update.message.reply_text(
            summary,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("\u2039 back", callback_data=f"folder_manage_{folder_id}")]]
            ),
        )

    # ── Automatic ingestion from a folder's source channel (converted from
    # bot.py's handle_channel_audio / _ingest_channel_audio / render_folder_page
    # group). Fires on any audio posted in a channel the bot is admin in;
    # everything not matching a registered source_channel_id for THIS
    # clone's folders is ignored, so one clone's ingestion can't pick up
    # another clone's channel even if both bots are admins in it. ───────
    def _extract_episode_no(self, text: str) -> str | None:
        if not text:
            return None
        for pattern in EPISODE_EXTRACT_PATTERNS:
            m = pattern.search(text)
            if m:
                return str(int(m.group(1)))
        return None

    def _fallback_name_identifier(self, file_name: str | None, title: str | None) -> str | None:
        """For songs — filenames with no episode number in them at all.
        Builds an identifier like "A_Aashiqui_2_mp3" from the first letter
        of the name plus the full (sanitized) name, so two different songs
        starting with the same letter don't collide as "duplicates" the
        way a bare first-letter identifier would."""
        name = file_name or title
        if not name:
            return None
        safe = re.sub(r'[^A-Za-z0-9]+', '_', name).strip('_')
        if not safe:
            return None
        first_letter_match = re.search(r'[A-Za-z]', name)
        first_letter = first_letter_match.group(0).upper() if first_letter_match else "#"
        return f"{first_letter}_{safe}"[:200]  # 200: comfortably under any TEXT-column/index limits

    async def _ingest_channel_audio(self, folder, telegram_file_id: str, message_id: str,
                                     episode_no: str, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        folder_id = folder["id"]
        channel_id = folder["channel_id"]

        dup = await self.db.fetchrow(
            """SELECT a.id, a.telegram_file_id, a.message_id FROM audios a
               JOIN batches b ON b.id = a.batch_id
               WHERE b.folder_id = $1 AND a.episode_no = $2""",
            folder_id, episode_no
        )
        if dup:
            # Same episode_no AND same file/message = genuine duplicate
            # (e.g. Telegram retry, or the same post re-forwarded).
            if dup["telegram_file_id"] == telegram_file_id or dup["message_id"] == message_id:
                logger.info(
                    "Episode %s already ingested for folder %s (message %s) — skipping duplicate.",
                    episode_no, folder_id, message_id,
                )
                return
            # Same episode_no but a DIFFERENT file/message: the number
            # extraction collided (bad caption/filename), not a real
            # re-upload. Don't silently drop it — alert the owner and
            # ingest anyway so the file isn't lost.
            logger.warning(
                "Episode_no %s collides with existing audio id=%s in folder %s, "
                "but file_id/message_id differ (new message %s) — treating as a "
                "NEW file with a bad/duplicate episode number, not a duplicate.",
                episode_no, dup["id"], folder_id, message_id,
            )
            try:
                await ctx.bot.send_message(
                    chat_id=self.owner_id,
                    text=(
                        f"\u26a0\ufe0f New audio in \"{folder['name']}\" (message {message_id}) "
                        f"extracted episode number {episode_no}, which is already used by "
                        f"another audio in this folder. It was ingested anyway (not dropped) "
                        f"— please check the filename/caption and fix the episode number if needed."
                    )
                )
            except Exception:
                pass

        # Attach the new row to whichever batch is currently last — this
        # is just a holding spot. rebalance_folder_batches() below
        # immediately re-sorts every audio in the folder by episode
        # number and repacks ALL batches from scratch, so it doesn't
        # matter where it starts: a late Ep3 will get moved out of here
        # into Batch 1, and whatever Batch 1 pushes out (e.g. Ep51)
        # cascades forward automatically.
        holding_batch_id = await self.db.fetchval(
            "SELECT id FROM batches WHERE folder_id = $1 ORDER BY id DESC LIMIT 1",
            folder_id
        )
        if holding_batch_id is None:
            holding_batch_id = await self.db.fetchval(
                "INSERT INTO batches (folder_id, total_links, name) VALUES ($1, 0, $2) RETURNING id",
                folder_id, f"{folder['name']} — Batch 1"
            )

        await self.db.execute(
            "INSERT INTO audios (batch_id, telegram_file_id, episode_no, message_id) VALUES ($1, $2, $3, $4)",
            holding_batch_id, telegram_file_id, episode_no, message_id
        )

        touched_batch_ids = await self.db.rebalance_folder_batches(folder_id, folder["name"], BATCH_MAX)

        if channel_id:
            page_indices = set()
            for batch_id in touched_batch_ids:
                try:
                    page_indices.add(await self._page_index_for_batch(folder_id, batch_id))
                except Exception as e:
                    logger.error("Could not resolve page index for batch %s: %s", batch_id, e)
            for page_index in sorted(page_indices):
                try:
                    await self.render_folder_page(folder_id, folder["name"], channel_id, page_index, ctx)
                except Exception as e:
                    logger.error("Channel page render failed for folder %s page %s after ingest: %s", folder_id, page_index, e)

    async def handle_channel_audio(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Fires on every audio posted in ANY channel this clone's bot is
        admin in — filtered down to folders' registered source_channel_id
        (scoped to this clone's own db, so no cross-clone leakage)."""
        message = update.effective_message
        if message is None or message.audio is None:
            return

        chat_id_str = str(message.chat.id)
        folder = await self.db.fetchrow(
            "SELECT id, name, channel_id FROM folders WHERE source_channel_id = $1", chat_id_str
        )
        if not folder:
            return

        # Caption is deliberately NOT used for episode-number extraction —
        # only filename and title. (A caption with no digits in it — just
        # an episode title — could otherwise shadow the real number
        # sitting in the filename.)
        episode_no = None
        for source_text in (message.audio.file_name, message.audio.title):
            episode_no = self._extract_episode_no(source_text or "")
            if episode_no is not None:
                break

        # Songs typically have no number anywhere in filename/title at
        # all (unlike episodes) — for those, fall back to a
        # first-letter + full-name identifier instead of rejecting them.
        if episode_no is None:
            episode_no = self._fallback_name_identifier(
                message.audio.file_name, message.audio.title
            )

        if episode_no is None:
            logger.warning(
                "Could not extract an episode number for message %s in source "
                "channel %s (folder \"%s\"); skipping.",
                message.message_id, chat_id_str, folder["name"],
            )
            try:
                await ctx.bot.send_message(
                    chat_id=self.owner_id,
                    text=(
                        f"\u26a0\ufe0f Could not detect an episode number for a new audio in "
                        f"\"{folder['name']}\" (message {message.message_id}). "
                        f"It was NOT saved — add a number to the caption/filename and repost."
                    )
                )
            except Exception:
                pass
            return

        lock = self._get_folder_lock(folder["id"])
        async with lock:
            await self._ingest_channel_audio(
                folder, message.audio.file_id, str(message.message_id), episode_no, ctx
            )

    async def _page_index_for_batch(self, folder_id: int, batch_id: int) -> int:
        """1-based page number for this batch's position within the folder
        (batches ordered oldest-to-newest by id)."""
        position = await self.db.fetchval(
            "SELECT COUNT(*) FROM batches WHERE folder_id = $1 AND id <= $2",
            folder_id, batch_id
        )
        return ((position - 1) // PAGE_SIZE) + 1

    def _page_text(self, folder_name: str, page_index: int, total_pages: int, total_in_page: int) -> str:
        display_name = (folder_name or "Audio Collection").upper()

        part_text = (
            f"『 ℙ𝕒𝕣𝕥 {page_index} 』"
            if total_pages > 1
            else "『 ℂ𝕠𝕞𝕡𝕝𝕖𝕥𝕖 』"
        )

        total_start = (page_index - 1) * 1000 + 1
        total_end = min(page_index * 1000, total_pages * 1000)

        return (
            "╔════❖•❄️•❖════╗\n"
            f"🎧 {display_name}\n"
            f"{part_text}\n"
            "╚════❖•❄️•❖════╝\n\n"
            f"📦 Total Episodes: {total_start} to {total_end}\n"
            "⚡ Instant Delivery\n"
            "🎶 Premium Audio Collection\n\n"
            "👇 Click the button below\n"
            "to receive your episodes instantly."
        )

    def _page_buttons(self, batches_in_page: list, start_offset: int) -> InlineKeyboardMarkup:
        rows = []
        running = start_offset
        for b in batches_in_page:
            end = running + b["total_links"] - 1
            label = f"Ep ❄️ {running} to {end}" if b["total_links"] > 1 else f"Ep ❄️ {running}"
            rows.append([InlineKeyboardButton(label, url=f"https://t.me/{self.bot_username}?start=batch_{b['id']}")])
            running = end + 1
        return InlineKeyboardMarkup(rows)

    async def render_folder_page(self, folder_id: int, folder_name: str, channel_id: str, page_index: int, ctx,
                                  force_new: bool = False) -> None:
        """(Re)builds a folder's page (max 20 batches/buttons) channel
        message. Edits the existing message if one exists for this page,
        otherwise sends a new one. force_new=True (used on channel switch)
        always sends fresh rather than trying to edit a message_id that
        belonged to the OLD channel."""
        all_batches = await self.db.fetch(
            "SELECT id, total_links FROM batches WHERE folder_id = $1 ORDER BY id",
            folder_id
        )
        total_pages = (len(all_batches) + PAGE_SIZE - 1) // PAGE_SIZE
        if total_pages == 0 or page_index > total_pages:
            return

        start_slice = (page_index - 1) * PAGE_SIZE
        end_slice = start_slice + PAGE_SIZE
        batches_in_page = all_batches[start_slice:end_slice]
        start_offset = sum(b["total_links"] for b in all_batches[:start_slice]) + 1
        total_in_page = sum(b["total_links"] for b in batches_in_page)

        text = self._page_text(folder_name, page_index, total_pages, total_in_page)
        markup = self._page_buttons(batches_in_page, start_offset)

        page_row = await self.db.fetchrow(
            "SELECT channel_message_id FROM folder_pages WHERE folder_id = $1 AND page_index = $2",
            folder_id, page_index
        )

        edited = False
        if page_row and page_row["channel_message_id"] and not force_new:
            try:
                await ctx.bot.edit_message_text(
                    chat_id=channel_id,
                    message_id=int(page_row["channel_message_id"]),
                    text=text,
                    reply_markup=markup
                )
                edited = True
            except Exception as e:
                logger.warning("Edit failed for folder %s page %s, sending new: %s", folder_id, page_index, e)

        if not edited:
            msg = await ctx.bot.send_message(chat_id=channel_id, text=text, reply_markup=markup)
            await self.db.execute(
                """
                INSERT INTO folder_pages (folder_id, page_index, channel_message_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (folder_id, page_index)
                DO UPDATE SET channel_message_id = EXCLUDED.channel_message_id
                """,
                folder_id, page_index, str(msg.message_id)
            )

    # ── text-wizard state machine for the folder flows above (converted
    # from the folder-related branches of bot.py's handle_links). Scoped
    # to self.owner_id only — non-owner text (episode search / public
    # link intake) is NOT ported here yet, so non-owner messages are
    # ignored rather than silently mishandled. ──────────────────────────
    async def handle_owner_text(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != self.owner_id:
            return
        text = (update.message.text or "").strip()

        if self.awaiting_caption_text:
            self.awaiting_caption_text = False
            await self.central_db.update_clone_settings(self.clone_id, custom_caption=text)
            await update.message.reply_text(
                "\u2705 Custom caption updated.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("\u2039 back", callback_data="cs_caption_menu")]]
                ),
            )
            return

        if self.awaiting_button_line:
            if not _parse_custom_buttons(text):
                await update.message.reply_text(
                    "\u26a0\ufe0f Couldn't parse that — use \"Label - URL\". Send again, or /start to cancel."
                )
                return
            self.awaiting_button_line = False
            settings = await self.central_db.get_clone_settings(self.clone_id)
            existing = settings["custom_buttons"] or ""
            updated = (existing.rstrip() + "\n" + text).strip() if existing.strip() else text
            await self.central_db.update_clone_settings(self.clone_id, custom_buttons=updated)
            await update.message.reply_text(
                "\u2705 Button row added.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("\u2039 back", callback_data="cs_button_menu")]]
                ),
            )
            return

        if self.awaiting_rename_folder_id is not None:
            folder_id = self.awaiting_rename_folder_id
            if not text:
                await update.message.reply_text("\u26a0\ufe0f Folder name cannot be empty.")
                return
            self.awaiting_rename_folder_id = None
            try:
                await self.db.execute("UPDATE folders SET name = $1 WHERE id = $2", text, folder_id)
            except Exception:
                await update.message.reply_text(
                    f"\u26a0\ufe0f A folder named \"{text}\" already exists. Try again from /folders."
                )
                return
            await update.message.reply_text(
                f"\u2705 Folder renamed to \"{text}\".",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("\u2039 back", callback_data=f"folder_manage_{folder_id}")]]
                ),
            )
            return

        if self.awaiting_new_folder_name:
            if not text:
                await update.message.reply_text("\u26a0\ufe0f Folder name cannot be empty.")
                return
            self.awaiting_new_folder_name = False
            try:
                folder_id = await self.db.fetchval(
                    "INSERT INTO folders (name) VALUES ($1) RETURNING id", text
                )
            except Exception:
                await update.message.reply_text(
                    f"\u26a0\ufe0f A folder named \"{text}\" already exists. Try /folders again."
                )
                return

            self.awaiting_channel_id_for_folder = folder_id
            self.new_folder_pending_source = True
            await update.message.reply_text(
                f"\u2705 Folder \"{text}\" created.\n\n"
                f"\U0001F4E1 Now send this folder's *Output* Channel ID (e.g. @channelusername or -100xxxxxxxxxx) "
                f"— where the batch/page buttons will be posted.\n\n"
                f"\u26a0\ufe0f The bot must be made an admin in that channel (with Post Messages permission).",
                parse_mode="Markdown"
            )
            return

        if self.awaiting_channel_id_for_folder is not None:
            if not text:
                await update.message.reply_text("\u26a0\ufe0f Channel ID cannot be empty.")
                return
            folder_id = self.awaiting_channel_id_for_folder
            is_new_folder_wizard = self.new_folder_pending_source

            folder_before = await self.db.fetchrow(
                "SELECT channel_id FROM folders WHERE id = $1", folder_id
            )
            had_previous_channel = bool(folder_before and folder_before["channel_id"])

            try:
                await ctx.bot.send_message(chat_id=text, text="\u2705 Channel linked successfully.")
                await self.db.execute(
                    "UPDATE folders SET channel_id = $1 WHERE id = $2", text, folder_id
                )
                self.awaiting_channel_id_for_folder = None
                self.new_folder_pending_source = False
                # No back button when this is mid-wizard or about to be
                # followed by a repost summary — those carry their own.
                terminal = not is_new_folder_wizard and not had_previous_channel
                await update.message.reply_text(
                    "\u2705 Output Channel ID saved and verified.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("\u2039 back", callback_data=f"folder_manage_{folder_id}")]]
                    ) if terminal else None,
                )
            except Exception as e:
                self.awaiting_channel_id_for_folder = None
                self.new_folder_pending_source = False
                logger.error("Channel verify failed for folder %s: %s", folder_id, e)
                await update.message.reply_text(
                    "\u274c Channel ID not saved — the bot could not post there.\n"
                    "Please check: (1) the ID is correct (2) the bot is an admin in that channel "
                    "(3) Post Messages permission is ON.\n\n"
                    "Try again via /folders."
                )
                return

            if is_new_folder_wizard:
                self.awaiting_source_channel_id_for_folder = folder_id
                await update.message.reply_text(
                    "\U0001F4E5 Now send this folder's *Source* Channel ID (e.g. @channelusername or -100xxxxxxxxxx) "
                    "— the private channel the bot will watch for new audio.\n\n"
                    "\u26a0\ufe0f The bot must be made an admin there too (any admin right is enough — "
                    "it only needs to *read* posts there, not send).",
                    parse_mode="Markdown"
                )
                return

            if had_previous_channel:
                folder_row = await self.db.fetchrow("SELECT name FROM folders WHERE id = $1", folder_id)
                await self._repost_all_pages_for_folder(folder_id, folder_row["name"], text, update, ctx)
            return

        if self.awaiting_source_channel_id_for_folder is not None:
            if not text:
                await update.message.reply_text("\u26a0\ufe0f Channel ID cannot be empty.")
                return
            folder_id = self.awaiting_source_channel_id_for_folder

            try:
                chat = await ctx.bot.get_chat(text)
                me = await ctx.bot.get_me()
                member = await ctx.bot.get_chat_member(chat_id=chat.id, user_id=me.id)
                if member.status not in ("administrator", "creator"):
                    raise ValueError("bot is not an admin in that channel")
            except Exception as e:
                self.awaiting_source_channel_id_for_folder = None
                logger.error("Source channel verify failed for folder %s: %s", folder_id, e)
                await update.message.reply_text(
                    "\u274c Source Channel ID not saved.\n"
                    "Please check: (1) the ID is correct (2) the bot is an admin there.\n\n"
                    "Try again via /folders."
                )
                return

            existing_owner = await self.db.fetchrow(
                "SELECT id, name FROM folders WHERE source_channel_id = $1 AND id != $2",
                str(chat.id), folder_id
            )
            if existing_owner:
                self.awaiting_source_channel_id_for_folder = None
                await update.message.reply_text(
                    f"\u26a0\ufe0f That channel is already the source channel for folder \"{existing_owner['name']}\"."
                )
                return

            await self.db.execute(
                "UPDATE folders SET source_channel_id = $1 WHERE id = $2", str(chat.id), folder_id
            )
            self.awaiting_source_channel_id_for_folder = None
            await update.message.reply_text(
                "\u2705 Source channel saved and verified.\n\n"
                "Any audio posted in that channel from now on will be picked up automatically.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("\u2039 back", callback_data=f"folder_manage_{folder_id}")]]
                ),
            )
            return

        if self.awaiting_force_join_edit_channel_id is not None:
            if not text:
                await update.message.reply_text("\u26a0\ufe0f Invite link cannot be empty.")
                return
            await self.db.execute(
                "UPDATE force_join_channels SET invite_link = $1 WHERE channel_id = $2",
                text, self.awaiting_force_join_edit_channel_id
            )
            self.awaiting_force_join_edit_channel_id = None
            await update.message.reply_text(
                "\u2705 Invite link updated.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("\u2039 back", callback_data="forcejoin_list")]]
                ),
            )
            return

        if self.awaiting_force_join_step == "id":
            if not text:
                await update.message.reply_text("\u26a0\ufe0f Channel/Group ID cannot be empty.")
                return

            count = await self.db.fetchval("SELECT COUNT(*) FROM force_join_channels")
            if count >= FORCE_JOIN_LIMIT:
                self.awaiting_force_join_step = None
                await update.message.reply_text(
                    f"\u26a0\ufe0f Limit reached — max {FORCE_JOIN_LIMIT} force-join channels. "
                    "Remove one via /forcejoin before adding another."
                )
                return

            try:
                chat = await ctx.bot.get_chat(text)

                if chat.type not in ("channel", "supergroup", "group"):
                    await update.message.reply_text("\u274c Only channels and groups can be added.")
                    return

                me = await ctx.bot.get_me()
                member = await ctx.bot.get_chat_member(chat_id=chat.id, user_id=me.id)
                if member.status not in ("administrator", "creator"):
                    raise ValueError("bot is not an admin")

            except Exception as e:
                logger.exception("Force-join verification failed for clone %s", self.clone_id)
                self.awaiting_force_join_step = None
                await update.message.reply_text(
                    f"\u274c Verification failed:\n\n{e}\n\n"
                    "Please check:\n"
                    "• The ID is correct\n"
                    "• The bot is an admin\n"
                    "• The group/channel is accessible"
                )
                return

            existing = await self.db.fetchrow(
                "SELECT id FROM force_join_channels WHERE channel_id = $1", str(chat.id)
            )
            if existing:
                self.awaiting_force_join_step = None
                await update.message.reply_text("\u26a0\ufe0f This channel/group is already in the force-join list.")
                return

            self.force_join_pending_channel_id = str(chat.id)
            self.force_join_pending_title = chat.title or chat.username or text
            self.awaiting_force_join_step = "link"
            await update.message.reply_text(
                f"\u2705 \"{self.force_join_pending_title}\" verified.\n\n"
                f"\U0001F517 Now send its invite link — for a public channel, https://t.me/username "
                f"also works; for a private one, use a link exported/created via the bot.\n\n"
                f"\u2139\ufe0f If you need an approval-required (join request) link, generate that "
                f"link yourself in Telegram and paste it here — the bot does not create "
                f"an approval-required link on its own."
            )
            return

        if self.awaiting_force_join_step == "link":
            if not text:
                await update.message.reply_text("\u26a0\ufe0f Invite link cannot be empty.")
                return
            await self.db.execute(
                "INSERT INTO force_join_channels (channel_id, invite_link, title) VALUES ($1, $2, $3)",
                self.force_join_pending_channel_id, text, self.force_join_pending_title
            )
            title_done = self.force_join_pending_title
            self.awaiting_force_join_step = None
            self.force_join_pending_channel_id = None
            self.force_join_pending_title = None
            await update.message.reply_text(
                f"\u2705 \"{title_done}\" added to the force-join list.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("\u2039 back", callback_data="forcejoin_list")]]
                ),
            )
            return

        # No owner-wizard state pending and no other owner-text feature
        # (broadcast) ported yet — see module docstring.

    # ── build & wire the Application for this clone ─────────────────────
    def build_application(self) -> Application:
        request = HTTPXRequest(connection_pool_size=8)
        app = (
            Application.builder()
            .token(self.bot_token)
            .request(request)
            # Without this, PTB processes this clone's updates one at a
            # time — every user is queued behind whoever's _deliver_batch
            # is currently running, and the Cancel button can't even be
            # dequeued until delivery finishes. Matches bot.py's master
            # setup; kept at 8 (not higher) to stay under
            # connection_pool_size=8 above and db.py's per-clone pool size.
            .concurrent_updates(8)
            .build()
        )
        app.add_handler(CommandHandler("start", self.cmd_start))
        app.add_handler(CommandHandler("folders", self.cmd_folders))
        app.add_handler(CommandHandler("forcejoin", self.cmd_forcejoin))
        app.add_handler(CommandHandler("ban", self.cmd_ban))
        app.add_handler(CommandHandler("unban", self.cmd_unban))
        app.add_handler(CallbackQueryHandler(self.cb_help, pattern=r"^start_help$"))
        app.add_handler(CallbackQueryHandler(self.cb_about, pattern=r"^start_about$"))
        app.add_handler(CallbackQueryHandler(self.cb_start_back, pattern=r"^start_back$"))
        app.add_handler(CallbackQueryHandler(self.cb_folder_new, pattern=r"^folder_new$"))
        app.add_handler(CallbackQueryHandler(self.cb_folder_manage, pattern=r"^folder_manage_\d+$"))
        app.add_handler(CallbackQueryHandler(self.cb_folder_list, pattern=r"^folder_list$"))
        app.add_handler(CallbackQueryHandler(self.cb_folder_setchannel, pattern=r"^folder_setchannel_\d+$"))
        app.add_handler(CallbackQueryHandler(self.cb_folder_setsource, pattern=r"^folder_setsource_\d+$"))
        app.add_handler(CallbackQueryHandler(self.cb_folder_rename, pattern=r"^folder_rename_\d+$"))
        app.add_handler(CallbackQueryHandler(self.cb_folder_delete_confirm, pattern=r"^folder_delete_\d+$"))
        app.add_handler(CallbackQueryHandler(self.cb_folder_delete_execute, pattern=r"^folder_delete_yes_\d+$"))
        app.add_handler(CallbackQueryHandler(self.cb_checkjoin, pattern=r"^checkjoin_\d+$"))
        app.add_handler(CallbackQueryHandler(self.cb_cancel_send, pattern=r"^cancelsend_\d+$", block=False))
        app.add_handler(CallbackQueryHandler(self.cb_forcejoin_list, pattern=r"^forcejoin_list$"))
        app.add_handler(CallbackQueryHandler(self.cb_forcejoin_add, pattern=r"^forcejoin_add$"))
        app.add_handler(CallbackQueryHandler(self.cb_forcejoin_remove, pattern=r"^forcejoin_remove_\d+$"))
        app.add_handler(CallbackQueryHandler(self.cb_forcejoin_editlink, pattern=r"^forcejoin_editlink_\d+$"))
        app.add_handler(CallbackQueryHandler(self.cb_settings_menu, pattern=r"^cs_menu$"))
        app.add_handler(CallbackQueryHandler(self.cb_settings_protect_toggle, pattern=r"^cs_protect_toggle$"))
        app.add_handler(CallbackQueryHandler(self.cb_settings_caption_menu, pattern=r"^cs_caption_menu$"))
        app.add_handler(CallbackQueryHandler(self.cb_settings_caption_edit, pattern=r"^cs_caption_edit$"))
        app.add_handler(CallbackQueryHandler(self.cb_settings_caption_see, pattern=r"^cs_caption_see$"))
        app.add_handler(CallbackQueryHandler(self.cb_settings_caption_delete, pattern=r"^cs_caption_delete$"))
        app.add_handler(CallbackQueryHandler(self.cb_settings_button_menu, pattern=r"^cs_button_menu$"))
        app.add_handler(CallbackQueryHandler(self.cb_settings_button_add, pattern=r"^cs_button_add$"))
        app.add_handler(CallbackQueryHandler(self.cb_settings_button_delete, pattern=r"^cs_button_delete$"))
        app.add_handler(ChatJoinRequestHandler(self.cb_chat_join_request))
        # Must be registered before the text handler: both could in
        # principle match overlapping updates, and only the first matching
        # handler in a group runs. ChatType.CHANNEL scopes this to channel
        # posts only, so it never intercepts audio sent to the bot in a
        # private chat.
        app.add_handler(MessageHandler(filters.AUDIO & filters.ChatType.CHANNEL, self.handle_channel_audio))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_owner_text))

        # Sweeps sent_logs for expired deliveries and actually deletes them —
        # see job_delete_expired's docstring. Every 60s is a compromise: fine
        # granularity for the "N minutes" promise without hammering the DB;
        # first=15 so a just-started clone isn't silently idle for a full
        # minute before its first sweep.
        app.job_queue.run_repeating(self.job_delete_expired, interval=60, first=15)
        # ... remaining handlers from bot.py's main() go here, each bound
        # to `self` once ported per the checklist at the top of this file.

        return app

    async def connect_db(self):
        """Must be called explicitly before the Application starts polling.

        NOTE: this used to be wired up as `app.post_init`, but post_init is
        only invoked by PTB's own run_polling()/run_webhook() convenience
        wrappers. CloneRunner drives the Application lifecycle manually
        (initialize/start/updater.start_polling), so post_init never fired —
        any clone with its own supabase_url had self.db.pool stuck at None
        and crashed on first query. Call this from CloneRunner._run_one
        before app.initialize() instead.
        """
        if isinstance(self.db, Database):
            await self.db.connect()
            await self.db.init_schema()

    async def setup_commands(self, app: Application):
        """Telegram '/' command menu for THIS clone. Same split as the
        master bot's _setup_bot_commands: a public menu for everyone, and
        a chat-scoped menu (only inside self.owner_id's own chat) with the
        owner-only commands added on top. Must be called from
        CloneRunner._run_one after app.initialize() — set_my_commands is
        an API call and needs a live bot connection, same reasoning as
        connect_db above being called before app.initialize() rather than
        in build_application (which is sync and runs before the bot is
        connected)."""
        public_commands = [BotCommand("start", "Start the bot")]
        owner_commands = public_commands + [
            BotCommand("folders", "Manage folders (output + source channel)"),
            BotCommand("ban", "Ban a user from this bot"),
            BotCommand("unban", "Unban a user from this bot"),
        ]
        try:
            await app.bot.set_my_commands(public_commands, scope=BotCommandScopeDefault())
            await app.bot.set_my_commands(
                owner_commands, scope=BotCommandScopeChat(chat_id=self.owner_id)
            )
        except Exception:
            # Non-fatal — command menu is cosmetic, the clone should still
            # run even if this API call fails (e.g. owner never started
            # a DM with this bot yet, so the chat-scoped call 400s).
            logger.exception("Clone %s: failed to set command menu", self.clone_id)
