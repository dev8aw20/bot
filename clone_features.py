"""
Clone dashboard sub-features: START MSG, FORCE SUB, NO FORWARD, MODERATORS,
AUTO DELETE, MODE, ACCESS TOKEN, STATS, TRANSFER DB.

All text-input flows (edit start msg, set force-sub channel, add
moderator, edit auto-delete message, transfer-db URI) share ONE
ConversationHandler + ONE state (AWAITING_INPUT). Which field is being
edited is tracked in ctx.user_data["editing"] = (field_name, clone_id),
set by the entry-point callback that opens each prompt. This avoids nine
near-identical ConversationHandlers.

Ownership check: every handler here re-verifies clone["user_id"] ==
requesting user before touching anything — dashboard callback_data
embeds only a clone_id, never a user_id, so this is the actual boundary
that stops user B from managing user A's clone by guessing/replaying a
callback_data string.
"""

import ipaddress
import logging
import re
import secrets
import socket
from urllib.parse import urlparse, parse_qs

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CallbackQueryHandler, MessageHandler, CommandHandler,
    ConversationHandler, filters, ContextTypes,
)

logger = logging.getLogger(__name__)

AWAITING_INPUT = 100

DEFAULT_START_MSG = (
    "\U0001F44B Welcome! Send me a shared link to get your files."
)
DEFAULT_AUTO_DELETE_MSG = (
    "\u26a0\ufe0f These files will self-destruct in {minutes} minutes."
)

# ── CUSTOM CAPTION / CUSTOM BUTTON ───────────────────────────────────────
# Per-clone versions of the master bot's Settings > CUSTOM CAPTION / CUSTOM
# BUTTON menu (see master_menu.py) — stored in clone_settings.custom_caption
# / custom_buttons instead of the master's singleton bot_settings row, and
# rendered per audio by bot_instance.py's own _render_caption /
# _parse_custom_buttons (kept as separate copies there, same reasoning as
# this file's ownership-check docstring: no cross-file dependency).
CUSTOM_CAPTION_HELP = (
    "Custom Caption: add a custom caption to your media messages instead "
    "of the original caption.\n\n"
    "Fillings:\n"
    "\u2022 {file_name}: File Name\n"
    "\u2022 {file_size}: File size\n"
    "\u2022 {caption}: Original Caption"
)


def _preview_button_markup(raw: str):
    """Same parsing as bot_instance.py's _parse_custom_buttons, but
    returns plain button rows (for embedding in a bigger keyboard here)
    rather than an InlineKeyboardMarkup."""
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


async def _get_owned_clone(update: Update, ctx: ContextTypes.DEFAULT_TYPE, clone_id: int):
    """Fetch a clone and verify the callback sender actually owns it.
    Returns None (and answers the callback with a denial) if not."""
    central_db = ctx.application.bot_data["central_db"]
    clone = await central_db.get_clone(clone_id)
    q = update.callback_query
    if not clone or clone["user_id"] != str(q.from_user.id):
        await q.answer("Not yours.", show_alert=True)
        return None
    return clone


def _clone_id_from(data: str) -> int:
    return int(data.rsplit("_", 1)[1])


# ── START MSG ─────────────────────────────────────────────────────────────
async def cb_startmsg_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    clone_id = _clone_id_from(q.data)
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    buttons = [
        [InlineKeyboardButton("Edit", callback_data=f"csm_edit_{clone_id}"),
         InlineKeyboardButton("See", callback_data=f"csm_see_{clone_id}")],
        [InlineKeyboardButton("Use Default", callback_data=f"csm_default_{clone_id}")],
        [InlineKeyboardButton("\u2039 back", callback_data=f"clone_dash_{clone_id}")],
    ]
    await q.edit_message_text(
        "START MSG: shown to users who /start this clone with no link.",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def cb_startmsg_edit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    clone_id = _clone_id_from(q.data)
    if not await _get_owned_clone(update, ctx, clone_id):
        return ConversationHandler.END
    ctx.user_data["editing"] = ("start_msg", clone_id)
    await q.edit_message_text("Send the new start message text. /cancel to stop.")
    return AWAITING_INPUT


async def cb_startmsg_see(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = _clone_id_from(q.data)
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    central_db = ctx.application.bot_data["central_db"]
    settings = await central_db.get_clone_settings(clone_id)
    current = settings["start_msg"] or f"{DEFAULT_START_MSG} (default — not set)"
    await q.answer()
    await q.message.reply_text(
        current,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("\u2039 back", callback_data=f"csm_menu_{clone_id}")]]
        ),
    )


async def cb_startmsg_default(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = _clone_id_from(q.data)
    if not await _get_owned_clone(update, ctx, clone_id):
        return
    central_db = ctx.application.bot_data["central_db"]
    await central_db.update_clone_settings(clone_id, start_msg=None)
    await q.answer("Reset to default.")
    await cb_startmsg_menu(update, ctx)


# ── CUSTOM CAPTION ────────────────────────────────────────────────────────
async def cb_customcaption_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    clone_id = _clone_id_from(q.data)
    if not await _get_owned_clone(update, ctx, clone_id):
        return
    buttons = [
        [InlineKeyboardButton("Edit", callback_data=f"ccap_edit_{clone_id}"),
         InlineKeyboardButton("See", callback_data=f"ccap_see_{clone_id}")],
        [InlineKeyboardButton("Delete", callback_data=f"ccap_delete_{clone_id}")],
        [InlineKeyboardButton("\u2039 back", callback_data=f"clone_dash_{clone_id}")],
    ]
    await q.edit_message_text(CUSTOM_CAPTION_HELP, reply_markup=InlineKeyboardMarkup(buttons))


async def cb_customcaption_edit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    clone_id = _clone_id_from(q.data)
    if not await _get_owned_clone(update, ctx, clone_id):
        return ConversationHandler.END
    ctx.user_data["editing"] = ("custom_caption", clone_id)
    await q.edit_message_text(
        "Send the new caption template. You can use {file_name}, "
        "{file_size}, {caption}. /cancel to stop."
    )
    return AWAITING_INPUT


async def cb_customcaption_see(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = _clone_id_from(q.data)
    if not await _get_owned_clone(update, ctx, clone_id):
        return
    central_db = ctx.application.bot_data["central_db"]
    settings = await central_db.get_clone_settings(clone_id)
    current = settings["custom_caption"] or "(not set — original captions are used as-is)"
    await q.answer()
    await q.message.reply_text(
        current,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("\u2039 back", callback_data=f"ccap_menu_{clone_id}")]]
        ),
    )


async def cb_customcaption_delete(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = _clone_id_from(q.data)
    if not await _get_owned_clone(update, ctx, clone_id):
        return
    central_db = ctx.application.bot_data["central_db"]
    await central_db.update_clone_settings(clone_id, custom_caption=None)
    await q.answer("Deleted.")
    await cb_customcaption_menu(update, ctx)


# ── CUSTOM BUTTON ─────────────────────────────────────────────────────────
async def cb_custombutton_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    clone_id = _clone_id_from(q.data)
    if not await _get_owned_clone(update, ctx, clone_id):
        return
    central_db = ctx.application.bot_data["central_db"]
    settings = await central_db.get_clone_settings(clone_id)
    preview_rows = _preview_button_markup(settings["custom_buttons"]) or []

    rows = list(preview_rows)
    rows.append([InlineKeyboardButton("\u2795", callback_data=f"cbtn_add_{clone_id}")])
    rows.append([InlineKeyboardButton("Delete", callback_data=f"cbtn_delete_{clone_id}")])
    rows.append([InlineKeyboardButton("\u2039 back", callback_data=f"clone_dash_{clone_id}")])

    text = "Custom Button: add a custom button to your media messages"
    if not preview_rows:
        text += "\n\n(none set yet)"
    await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows))


async def cb_custombutton_add(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    clone_id = _clone_id_from(q.data)
    if not await _get_owned_clone(update, ctx, clone_id):
        return ConversationHandler.END
    ctx.user_data["editing"] = ("custom_button_add", clone_id)
    await q.edit_message_text(
        "Send a new button row: \"Label - URL\", or two on the same row "
        "with \"Label1 - URL1 | Label2 - URL2\". This is ADDED as a new "
        "row below your existing buttons. /cancel to stop."
    )
    return AWAITING_INPUT


async def cb_custombutton_delete(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = _clone_id_from(q.data)
    if not await _get_owned_clone(update, ctx, clone_id):
        return
    central_db = ctx.application.bot_data["central_db"]
    await central_db.update_clone_settings(clone_id, custom_buttons=None)
    await q.answer("Deleted.")
    await cb_custombutton_menu(update, ctx)


# ── FORCE SUB ─────────────────────────────────────────────────────────────
# Reworked to be the SAME feature as the clone's own /forcejoin command,
# not a separate single-channel implementation — this dashboard menu is
# just a remote control for that clone's `force_join_channels` table.
# See the conversation for why this used to be a duplicate single-channel
# system and why that was a bug, not a design choice.
#
# force_join_channels lives in the CLONE's OWN database, not central_db
# (each clone can have its own Supabase project). We reach it the same
# way cb_clone_stats already does: open a short-lived connection using
# the clone's stored (decrypted) supabase_url, falling back to central_db
# for clones that share central storage. That means channels can be
# listed/removed even while the clone is stopped.
#
# Verifying a NEW channel (bot-is-admin check) is different: it must be
# done through THAT CLONE's own bot, not the master bot — ctx.bot here is
# the master bot and has no relationship to the clone's channels. So
# adding a channel requires the clone to be currently running
# (CloneRunner.get_bot), same as /forcejoin already effectively requires
# since it only runs inside the live clone process. Removing a channel
# has no such requirement.
FORCE_SUB_LIMIT = 6  # keep in sync with bot_instance.py's FORCE_JOIN_LIMIT


async def _clone_db_connect(ctx: ContextTypes.DEFAULT_TYPE, clone: dict):
    """Returns (db, owns_connection). owns_connection=True means the
    caller must _clone_db_release it; False means it's the shared
    central_db and must NOT be disconnected."""
    central_db = ctx.application.bot_data["central_db"]
    if not clone["supabase_url"]:
        return central_db, False
    from db import Database
    clone_db = Database(clone["supabase_url"])
    await clone_db.connect()
    return clone_db, True


async def _clone_db_release(clone_db, owns_connection: bool):
    if owns_connection:
        await clone_db.disconnect()


async def _render_forcesub_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE, clone: dict):
    clone_id = clone["id"]
    clone_db, owns = await _clone_db_connect(ctx, clone)
    try:
        channels = await clone_db.fetch(
            "SELECT id, title, channel_id FROM force_join_channels ORDER BY id"
        )
    except Exception:
        logger.exception("Couldn't read force-join channels for clone %s", clone_id)
        await update.callback_query.message.reply_text(
            "\u26a0\ufe0f Couldn't reach this clone's database."
        )
        return
    finally:
        await _clone_db_release(clone_db, owns)

    rows = [
        [InlineKeyboardButton(f"\u274c {c['title'] or c['channel_id']}",
                               callback_data=f"fsub_remove_{clone_id}_{c['id']}")]
        for c in channels
    ]
    if len(channels) < FORCE_SUB_LIMIT:
        rows.append([InlineKeyboardButton("\u2795 Add Channel", callback_data=f"fsub_add_{clone_id}")])
    rows.append([InlineKeyboardButton("\u2039 back", callback_data=f"clone_dash_{clone_id}")])

    if channels:
        text = (
            f"FORCE SUB ({len(channels)}/{FORCE_SUB_LIMIT})\n\n"
            "Users must join all of these before using the bot. "
            "Private channels: users can send a join request instead of "
            "waiting for approval.\n\n"
            + "\n".join(f"\u2022 {c['title'] or c['channel_id']}" for c in channels)
        )
    else:
        text = (
            f"FORCE SUB (0/{FORCE_SUB_LIMIT})\n\n"
            "No channels set — users can use the bot freely. Add up to "
            f"{FORCE_SUB_LIMIT} channels below."
        )
    await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows))


async def cb_forcesub_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    clone_id = _clone_id_from(q.data)
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    await _render_forcesub_menu(update, ctx, clone)


async def cb_forcesub_remove(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id, row_id = (int(x) for x in q.data.replace("fsub_remove_", "").split("_"))
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    clone_db, owns = await _clone_db_connect(ctx, clone)
    try:
        await clone_db.execute("DELETE FROM force_join_channels WHERE id = $1", row_id)
    finally:
        await _clone_db_release(clone_db, owns)
    await q.answer("Removed.")
    await _render_forcesub_menu(update, ctx, clone)


async def cb_forcesub_add(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = _clone_id_from(q.data)
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return ConversationHandler.END

    runner = ctx.application.bot_data["runner"]
    bot = runner.get_bot(clone_id)
    if bot is None:
        await q.answer(
            "This clone must be running to verify a new channel — start it first.",
            show_alert=True,
        )
        return ConversationHandler.END

    clone_db, owns = await _clone_db_connect(ctx, clone)
    try:
        count = await clone_db.fetchval("SELECT COUNT(*) FROM force_join_channels")
    finally:
        await _clone_db_release(clone_db, owns)
    if count >= FORCE_SUB_LIMIT:
        await q.answer(f"Limit reached — max {FORCE_SUB_LIMIT} channels.", show_alert=True)
        return ConversationHandler.END

    await q.answer()
    ctx.user_data["editing"] = ("fsub_add_id", clone_id)
    await q.edit_message_text(
        "Send the channel/group ID or @username — the clone's bot must "
        "already be admin there. /cancel to stop."
    )
    return AWAITING_INPUT


# ── NO FORWARD ────────────────────────────────────────────────────────────
async def cb_noforward_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = _clone_id_from(q.data)
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    central_db = ctx.application.bot_data["central_db"]
    s = await central_db.get_clone_settings(clone_id)
    status = "Enabled \u2705" if s["no_forward_enabled"] else "Disabled \u274c"
    await q.answer()
    buttons = [
        [InlineKeyboardButton(
            "Disable \u274c" if s["no_forward_enabled"] else "Enable \u2705",
            callback_data=f"nofwd_toggle_{clone_id}")],
        [InlineKeyboardButton("\u2039 back", callback_data=f"clone_dash_{clone_id}")],
    ]
    await q.edit_message_text(
        f"No Forward: restricts clone users from forwarding messages from "
        f"shareable links.\nStatus: {status}",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def cb_noforward_toggle(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = _clone_id_from(q.data)
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    central_db = ctx.application.bot_data["central_db"]
    s = await central_db.get_clone_settings(clone_id)
    new_state = not s["no_forward_enabled"]
    await central_db.update_clone_settings(clone_id, no_forward_enabled=new_state)
    await q.answer("Enabled." if new_state else "Disabled.")
    await cb_noforward_menu(update, ctx)


# ── MODERATORS ────────────────────────────────────────────────────────────
async def cb_moderators_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    clone_id = _clone_id_from(q.data)
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    buttons = [
        [InlineKeyboardButton("Add Moderator", callback_data=f"mod_add_{clone_id}")],
        [InlineKeyboardButton("View User List", callback_data=f"mod_list_{clone_id}")],
        [InlineKeyboardButton("\u2039 back", callback_data=f"clone_dash_{clone_id}")],
    ]
    await q.edit_message_text("MODERATORS", reply_markup=InlineKeyboardMarkup(buttons))


async def cb_moderators_add(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    clone_id = _clone_id_from(q.data)
    if not await _get_owned_clone(update, ctx, clone_id):
        return ConversationHandler.END
    ctx.user_data["editing"] = ("add_moderator", clone_id)
    await q.edit_message_text("Send the Telegram numeric user ID to add as moderator. /cancel to stop.")
    return AWAITING_INPUT


async def cb_moderators_list(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = _clone_id_from(q.data)
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    central_db = ctx.application.bot_data["central_db"]
    mods = await central_db.list_moderators(clone_id)
    await q.answer()
    text = "Moderators:\n" + ("\n".join(mods) if mods else "(none)")
    await q.message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("\u2039 back", callback_data=f"mod_menu_{clone_id}")]]
        ),
    )


# ── AUTO DELETE ───────────────────────────────────────────────────────────
async def cb_autodelete_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = _clone_id_from(q.data)
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    central_db = ctx.application.bot_data["central_db"]
    s = await central_db.get_clone_settings(clone_id)
    current_minutes = s["auto_delete_minutes"]
    status = f"Enabled \u2705 - Time: {current_minutes} Mins" if s["auto_delete_enabled"] else "Disabled \u274c"
    await q.answer()

    def _label(text, minutes):
        return f"\u2705 {text}" if s["auto_delete_enabled"] and current_minutes == minutes else text

    buttons = [
        [InlineKeyboardButton("Disable \u274c", callback_data=f"ad_toggle_{clone_id}")],
        [InlineKeyboardButton(_label("5m", 5), callback_data=f"ad_time_{clone_id}_5"),
         InlineKeyboardButton(_label("15m", 15), callback_data=f"ad_time_{clone_id}_15"),
         InlineKeyboardButton(_label("1h", 60), callback_data=f"ad_time_{clone_id}_60")],
        [InlineKeyboardButton("Custom Time", callback_data=f"ad_time_custom_{clone_id}")],
        [InlineKeyboardButton("Message", callback_data=f"ad_msg_{clone_id}")],
        [InlineKeyboardButton("\u2039 back", callback_data=f"clone_dash_{clone_id}")],
    ]
    await q.edit_message_text(f"AUTO DELETE\nStatus: {status}", reply_markup=InlineKeyboardMarkup(buttons))


async def cb_autodelete_toggle(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = _clone_id_from(q.data)
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    central_db = ctx.application.bot_data["central_db"]
    s = await central_db.get_clone_settings(clone_id)
    new_state = not s["auto_delete_enabled"]
    await central_db.update_clone_settings(clone_id, auto_delete_enabled=new_state)
    await q.answer("Enabled." if new_state else "Disabled.")
    await cb_autodelete_menu(update, ctx)


async def cb_autodelete_time(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split("_")
    clone_id, minutes = int(parts[2]), int(parts[3])
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    central_db = ctx.application.bot_data["central_db"]
    await central_db.update_clone_settings(
        clone_id, auto_delete_minutes=minutes, auto_delete_enabled=True
    )
    await q.answer(f"Set to {minutes} minutes.")
    await cb_autodelete_menu(update, ctx)


async def cb_autodelete_time_custom(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    clone_id = _clone_id_from(q.data)
    if not await _get_owned_clone(update, ctx, clone_id):
        return ConversationHandler.END
    ctx.user_data["editing"] = ("auto_delete_minutes_custom", clone_id)
    await q.edit_message_text("Send the number of minutes (1-1440). /cancel to stop.")
    return AWAITING_INPUT


async def cb_autodelete_msg_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    clone_id = _clone_id_from(q.data)
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    buttons = [
        [InlineKeyboardButton("Edit", callback_data=f"ad_msg_edit_{clone_id}"),
         InlineKeyboardButton("See", callback_data=f"ad_msg_see_{clone_id}")],
        [InlineKeyboardButton("Use Default", callback_data=f"ad_msg_default_{clone_id}")],
        [InlineKeyboardButton("\u2039 back", callback_data=f"ad_menu_{clone_id}")],
    ]
    await q.edit_message_text(
        "Auto-delete warning message. Use {minutes} to insert the countdown.",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def cb_autodelete_msg_edit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    clone_id = _clone_id_from(q.data)
    if not await _get_owned_clone(update, ctx, clone_id):
        return ConversationHandler.END
    ctx.user_data["editing"] = ("auto_delete_message", clone_id)
    await q.edit_message_text("Send the new auto-delete message. /cancel to stop.")
    return AWAITING_INPUT


async def cb_autodelete_msg_see(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = _clone_id_from(q.data)
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    central_db = ctx.application.bot_data["central_db"]
    s = await central_db.get_clone_settings(clone_id)
    current = s["auto_delete_message"] or f"(using default) {DEFAULT_AUTO_DELETE_MSG}"
    await q.answer()
    await q.message.reply_text(
        current,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("\u2039 back", callback_data=f"ad_msg_{clone_id}")]]
        ),
    )


async def cb_autodelete_msg_default(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = _clone_id_from(q.data)
    if not await _get_owned_clone(update, ctx, clone_id):
        return
    central_db = ctx.application.bot_data["central_db"]
    await central_db.update_clone_settings(clone_id, auto_delete_message=None)
    await q.answer("Reset to default.")
    await cb_autodelete_msg_menu(update, ctx)


# ── MODE ──────────────────────────────────────────────────────────────────
async def cb_mode_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = _clone_id_from(q.data)
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    central_db = ctx.application.bot_data["central_db"]
    s = await central_db.get_clone_settings(clone_id)
    current_mode = "Public" if clone["is_public"] else "Private"
    await q.answer()
    buttons = [
        [InlineKeyboardButton("Make Private", callback_data=f"mode_private_{clone_id}"),
         InlineKeyboardButton("Make Public", callback_data=f"mode_public_{clone_id}")],
        [InlineKeyboardButton(
            "Unhide Owner" if s["hide_owner"] else "Hide Owner",
            callback_data=f"mode_hideowner_{clone_id}")],
        [InlineKeyboardButton("\u2039 back", callback_data=f"clone_dash_{clone_id}")],
    ]
    await q.edit_message_text(
        f"Clone Mode\n\nPublic Mode: any Telegram user can use this bot.\n"
        f"Private Mode: only the owner can use it.\n\nCurrent Mode: {current_mode}",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def cb_mode_set(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    is_public = "_public_" in q.data
    clone_id = _clone_id_from(q.data)
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    central_db = ctx.application.bot_data["central_db"]
    await central_db.execute(
        "UPDATE user_bots SET is_public = $1 WHERE id = $2", is_public, clone_id
    )
    await q.answer("Set to Public." if is_public else "Set to Private.")
    await cb_mode_menu(update, ctx)


async def cb_mode_hideowner(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = _clone_id_from(q.data)
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    central_db = ctx.application.bot_data["central_db"]
    s = await central_db.get_clone_settings(clone_id)
    new_state = not s["hide_owner"]
    await central_db.update_clone_settings(clone_id, hide_owner=new_state)
    await q.answer("Owner hidden." if new_state else "Owner visible.")
    await cb_mode_menu(update, ctx)


# ── ACCESS TOKEN ──────────────────────────────────────────────────────────
async def cb_access_token_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = _clone_id_from(q.data)
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    central_db = ctx.application.bot_data["central_db"]
    s = await central_db.get_clone_settings(clone_id)
    token = s["access_token"] or "(none generated yet)"
    await q.answer()
    buttons = [
        [InlineKeyboardButton("\U0001F504 Regenerate", callback_data=f"atok_regen_{clone_id}")],
        [InlineKeyboardButton("\u2039 back", callback_data=f"clone_dash_{clone_id}")],
    ]
    await q.edit_message_text(
        f"ACCESS TOKEN (used for API/automation access to this clone's data):\n\n"
        f"`{token}`\n\nRegenerating invalidates the old one immediately.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def cb_access_token_regen(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = _clone_id_from(q.data)
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    central_db = ctx.application.bot_data["central_db"]
    new_token = secrets.token_urlsafe(24)
    await central_db.update_clone_settings(clone_id, access_token=new_token)
    await q.answer("Regenerated.")
    await cb_access_token_menu(update, ctx)


# ── STATS ─────────────────────────────────────────────────────────────────
async def cb_clone_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    clone_id = _clone_id_from(q.data)
    clone = await _get_owned_clone(update, ctx, clone_id)
    if not clone:
        return
    await q.answer()
    from db import Database
    clone_db = Database(clone["supabase_url"]) if clone["supabase_url"] else ctx.application.bot_data["central_db"]
    try:
        if clone_db is not ctx.application.bot_data["central_db"]:
            await clone_db.connect()
        folders = await clone_db.fetchval("SELECT COUNT(*) FROM folders") or 0
        batches = await clone_db.fetchval("SELECT COUNT(*) FROM batches") or 0
        audios = await clone_db.fetchval("SELECT COUNT(*) FROM audios") or 0
        users = await clone_db.fetchval("SELECT COUNT(*) FROM users") or 0
    except Exception:
        logger.exception("Stats query failed for clone %s", clone_id)
        await q.message.reply_text(
            "Couldn't read stats — clone's database may not be reachable.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("\u2039 back", callback_data=f"clone_dash_{clone_id}")]]
            ),
        )
        return
    finally:
        if clone_db is not ctx.application.bot_data["central_db"]:
            await clone_db.disconnect()

    runner = ctx.application.bot_data["runner"]
    running = "running" if runner.is_running(clone_id) else "stopped"
    await q.message.reply_text(
        f"\U0001F4CA Stats for @{clone['bot_username']} ({running})\n\n"
        f"Folders: {folders}\nBatches: {batches}\nAudios: {audios}\nUsers: {users}",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("\u2039 back", callback_data=f"clone_dash_{clone_id}")]]
        ),
    )


# ── TRANSFER DB ───────────────────────────────────────────────────────────
_PG_URI_RE = re.compile(r"^postgres(?:ql)?://", re.IGNORECASE)


def _validate_transfer_uri(uri: str) -> str | None:
    """Returns an error message if the URI is invalid, else None."""

    if not _PG_URI_RE.match(uri):
        return "Must be a postgresql:// or postgres:// URI."

    try:
        parsed = urlparse(uri)
    except Exception:
        return "Couldn't parse that as a URI."

    if not parsed.hostname or not parsed.port:
        return "URI must include host and port (e.g. :5432)."

    # Session Pooler port check
    if parsed.port != 5432:
        return "Please use the Supabase Session Pooler (Port 5432)."

    try:
        infos = socket.getaddrinfo(parsed.hostname, parsed.port)
    except socket.gaierror:
        return "Couldn't resolve that hostname."

    for info in infos:
        ip = ipaddress.ip_address(info[4][0])

        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
        ):
            return "That host resolves to a private/internal address — not allowed."

    return None


async def cb_transferdb_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    clone_id = _clone_id_from(q.data)
    if not await _get_owned_clone(update, ctx, clone_id):
        return
    buttons = [
        [InlineKeyboardButton("Transfer", callback_data=f"tdb_start_{clone_id}")],
        [InlineKeyboardButton("\u2039 back", callback_data=f"clone_dash_{clone_id}")],
    ]
    await q.edit_message_text(
        "Transfer DB: migrate this clone onto your own Supabase Session "
        "Pooler (port 5432, SSL required). Existing platform-hosted data "
        "for this clone will NOT be copied over automatically.",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def cb_transferdb_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    clone_id = _clone_id_from(q.data)
    if not await _get_owned_clone(update, ctx, clone_id):
        return ConversationHandler.END
    ctx.user_data["editing"] = ("transfer_db_uri", clone_id)
    await q.edit_message_text(
        "Send your Supabase Session Pooler URI.\n"
        "Example:postgresql://postgres.togbupweckjkrnsxwiwo:password@aws-1-ap-northeast-2.pooler.supabase.com:5432/postgres\n\n."
        "/cancel to stop."
    )
    return AWAITING_INPUT


async def _handle_transfer_db_uri(update, ctx, clone_id, uri):
    err = _validate_transfer_uri(uri)
    if err:
        await update.message.reply_text(f"\u26d4 {err} Send a corrected URI, or /cancel.")
        return AWAITING_INPUT

    from db import Database
    test_db = Database(uri)
    try:
        await test_db.connect()
        await test_db.fetchval("SELECT 1")
    except Exception as e:
        logger.warning("Transfer DB connection test failed for clone %s: %s", clone_id, e)
        await update.message.reply_text(
            "\u26d4 Couldn't connect with that URI — check credentials. Send again, or /cancel."
        )
        return AWAITING_INPUT
    finally:
        await test_db.disconnect()

    central_db = ctx.application.bot_data["central_db"]
    runner = ctx.application.bot_data["runner"]
    await central_db.execute(
        "UPDATE user_bots SET supabase_url = $1 WHERE id = $2",
        __import__("crypto").encrypt(uri), clone_id,
    )
    clone = await central_db.get_clone(clone_id)
    if runner.is_running(clone_id):
        await runner.stop_one(clone_id)
        await runner.start_one(clone)
    await update.message.reply_text(
        "\u2705 Transferred. This clone now runs on your own database and has been restarted."
    )
    return ConversationHandler.END


# ── Shared text-input dispatcher ─────────────────────────────────────────
async def receive_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    editing = ctx.user_data.get("editing")
    if not editing:
        return ConversationHandler.END
    field, clone_id = editing
    text = update.message.text.strip()
    central_db = ctx.application.bot_data["central_db"]

    if field == "start_msg":
        await central_db.update_clone_settings(clone_id, start_msg=text)
        await update.message.reply_text(
            "\u2705 Start message updated.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("\u2039 back", callback_data=f"csm_menu_{clone_id}")]]
            ),
        )
        ctx.user_data.pop("editing", None)
        return ConversationHandler.END

    if field == "custom_caption":
        await central_db.update_clone_settings(clone_id, custom_caption=text)
        await update.message.reply_text(
            "\u2705 Custom caption updated.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("\u2039 back", callback_data=f"ccap_menu_{clone_id}")]]
            ),
        )
        ctx.user_data.pop("editing", None)
        return ConversationHandler.END

    if field == "custom_button_add":
        if not _preview_button_markup(text):
            await update.message.reply_text(
                "\u26a0\ufe0f Couldn't parse that — use \"Label - URL\". Send again, or /cancel."
            )
            return AWAITING_INPUT
        settings = await central_db.get_clone_settings(clone_id)
        existing = settings["custom_buttons"] or ""
        updated = (existing.rstrip() + "\n" + text).strip() if existing.strip() else text
        await central_db.update_clone_settings(clone_id, custom_buttons=updated)
        await update.message.reply_text(
            "\u2705 Button row added.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("\u2039 back", callback_data=f"cbtn_menu_{clone_id}")]]
            ),
        )
        ctx.user_data.pop("editing", None)
        return ConversationHandler.END

    if field == "fsub_add_id":
        clone = await central_db.get_clone(clone_id)
        runner = ctx.application.bot_data["runner"]
        bot = runner.get_bot(clone_id)
        if clone is None or bot is None:
            await update.message.reply_text(
                "\u26a0\ufe0f Clone stopped mid-flow — start it and try again."
            )
            ctx.user_data.pop("editing", None)
            return ConversationHandler.END

        try:
            chat = await bot.get_chat(text)
            if chat.type not in ("channel", "supergroup", "group"):
                await update.message.reply_text("\u274c Only channels and groups can be added.")
                return AWAITING_INPUT
            me = await bot.get_me()
            member = await bot.get_chat_member(chat_id=chat.id, user_id=me.id)
            if member.status not in ("administrator", "creator"):
                raise ValueError("bot is not an admin there")
        except Exception as e:
            await update.message.reply_text(
                f"\u274c Verification failed: {e}\n\n"
                "Check the ID/username, and that the clone's bot is admin "
                "there. Send again, or /cancel."
            )
            return AWAITING_INPUT

        clone_db, owns = await _clone_db_connect(ctx, clone)
        try:
            existing = await clone_db.fetchrow(
                "SELECT id FROM force_join_channels WHERE channel_id = $1", str(chat.id)
            )
        finally:
            await _clone_db_release(clone_db, owns)
        if existing:
            await update.message.reply_text("\u26a0\ufe0f Already in the FORCE SUB list.")
            ctx.user_data.pop("editing", None)
            return ConversationHandler.END

        ctx.user_data["fsub_pending"] = {
            "channel_id": str(chat.id),
            "title": chat.title or chat.username or text,
        }
        ctx.user_data["editing"] = ("fsub_add_link", clone_id)
        await update.message.reply_text(
            f"\u2705 \"{ctx.user_data['fsub_pending']['title']}\" verified.\n\n"
            "Now send its invite link (for approval-required/join-request "
            "mode, paste a link you generated with that setting on — the "
            "bot won't create one for you). /cancel to stop."
        )
        return AWAITING_INPUT

    if field == "fsub_add_link":
        pending = ctx.user_data.get("fsub_pending")
        if not pending:
            ctx.user_data.pop("editing", None)
            return ConversationHandler.END
        clone = await central_db.get_clone(clone_id)
        clone_db, owns = await _clone_db_connect(ctx, clone)
        try:
            await clone_db.execute(
                "INSERT INTO force_join_channels (channel_id, invite_link, title) VALUES ($1, $2, $3)",
                pending["channel_id"], text, pending["title"],
            )
        finally:
            await _clone_db_release(clone_db, owns)
        await update.message.reply_text(
            f"\u2705 \"{pending['title']}\" added to FORCE SUB.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("\u2039 back", callback_data=f"fsub_menu_{clone_id}")]]
            ),
        )
        ctx.user_data.pop("fsub_pending", None)
        ctx.user_data.pop("editing", None)
        return ConversationHandler.END

    if field == "add_moderator":
        if not text.isdigit():
            await update.message.reply_text("That's not a numeric Telegram user ID. Send again, or /cancel.")
            return AWAITING_INPUT
        await central_db.add_moderator(clone_id, text)
        await update.message.reply_text(
            f"\u2705 {text} added as moderator.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("\u2039 back", callback_data=f"mod_menu_{clone_id}")]]
            ),
        )
        ctx.user_data.pop("editing", None)
        return ConversationHandler.END

    if field == "auto_delete_minutes_custom":
        if not text.isdigit() or not (1 <= int(text) <= 1440):
            await update.message.reply_text(
                "\u26a0\ufe0f Send a whole number of minutes between 1 and 1440. Or /cancel."
            )
            return AWAITING_INPUT
        minutes = int(text)
        await central_db.update_clone_settings(
            clone_id, auto_delete_minutes=minutes, auto_delete_enabled=True
        )
        await update.message.reply_text(
            f"\u2705 Set to {minutes} minutes.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("\u2039 back", callback_data=f"ad_menu_{clone_id}")]]
            ),
        )
        ctx.user_data.pop("editing", None)
        return ConversationHandler.END

    if field == "auto_delete_message":
        await central_db.update_clone_settings(clone_id, auto_delete_message=text)
        await update.message.reply_text(
            "\u2705 Auto-delete message updated.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("\u2039 back", callback_data=f"ad_msg_{clone_id}")]]
            ),
        )
        ctx.user_data.pop("editing", None)
        return ConversationHandler.END

    if field == "transfer_db_uri":
        result = await _handle_transfer_db_uri(update, ctx, clone_id, text)
        if result == ConversationHandler.END:
            ctx.user_data.pop("editing", None)
        return result

    ctx.user_data.pop("editing", None)
    return ConversationHandler.END


async def cancel_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.pop("editing", None)
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END


def register(application: Application):
    application.add_handler(CallbackQueryHandler(cb_startmsg_menu, pattern=r"^csm_menu_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_startmsg_see, pattern=r"^csm_see_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_startmsg_default, pattern=r"^csm_default_\d+$"))

    application.add_handler(CallbackQueryHandler(cb_customcaption_menu, pattern=r"^ccap_menu_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_customcaption_see, pattern=r"^ccap_see_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_customcaption_delete, pattern=r"^ccap_delete_\d+$"))

    application.add_handler(CallbackQueryHandler(cb_custombutton_menu, pattern=r"^cbtn_menu_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_custombutton_delete, pattern=r"^cbtn_delete_\d+$"))

    application.add_handler(CallbackQueryHandler(cb_forcesub_menu, pattern=r"^fsub_menu_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_forcesub_remove, pattern=r"^fsub_remove_\d+_\d+$"))

    application.add_handler(CallbackQueryHandler(cb_noforward_menu, pattern=r"^nofwd_menu_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_noforward_toggle, pattern=r"^nofwd_toggle_\d+$"))

    application.add_handler(CallbackQueryHandler(cb_moderators_menu, pattern=r"^mod_menu_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_moderators_list, pattern=r"^mod_list_\d+$"))

    application.add_handler(CallbackQueryHandler(cb_autodelete_menu, pattern=r"^ad_menu_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_autodelete_toggle, pattern=r"^ad_toggle_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_autodelete_time, pattern=r"^ad_time_\d+_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_autodelete_msg_menu, pattern=r"^ad_msg_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_autodelete_msg_see, pattern=r"^ad_msg_see_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_autodelete_msg_default, pattern=r"^ad_msg_default_\d+$"))

    application.add_handler(CallbackQueryHandler(cb_mode_menu, pattern=r"^mode_menu_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_mode_set, pattern=r"^mode_(public|private)_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_mode_hideowner, pattern=r"^mode_hideowner_\d+$"))

    application.add_handler(CallbackQueryHandler(cb_access_token_menu, pattern=r"^atok_menu_\d+$"))
    application.add_handler(CallbackQueryHandler(cb_access_token_regen, pattern=r"^atok_regen_\d+$"))

    application.add_handler(CallbackQueryHandler(cb_clone_stats, pattern=r"^stats_show_\d+$"))

    application.add_handler(CallbackQueryHandler(cb_transferdb_menu, pattern=r"^tdb_menu_\d+$"))

    text_input_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(cb_startmsg_edit, pattern=r"^csm_edit_\d+$"),
            CallbackQueryHandler(cb_customcaption_edit, pattern=r"^ccap_edit_\d+$"),
            CallbackQueryHandler(cb_custombutton_add, pattern=r"^cbtn_add_\d+$"),
            CallbackQueryHandler(cb_forcesub_add, pattern=r"^fsub_add_\d+$"),
            CallbackQueryHandler(cb_moderators_add, pattern=r"^mod_add_\d+$"),
            CallbackQueryHandler(cb_autodelete_msg_edit, pattern=r"^ad_msg_edit_\d+$"),
            CallbackQueryHandler(cb_autodelete_time_custom, pattern=r"^ad_time_custom_\d+$"),
            CallbackQueryHandler(cb_transferdb_start, pattern=r"^tdb_start_\d+$"),
        ],
        states={
            AWAITING_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_input)],
        },
        fallbacks=[CommandHandler("cancel", cancel_input)],
    )
    application.add_handler(text_input_conv)
