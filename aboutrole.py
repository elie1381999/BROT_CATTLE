import logging
from typing import Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from farmcore import async_get_user_by_telegram, async_get_user_with_farm_by_telegram
from farmcore_role import (
    async_create_invitation,
    async_redeem_invitation,
    async_list_invitations,
    async_get_farm_members,
    async_revoke_member,
    async_get_user_role_in_farm,
    async_notify_owner,
    async_find_user_primary_farm,
    async_update_member_role,
    FARM_ROLES,
)

logger = logging.getLogger(__name__)

role_handlers = {}

# ---- Small constants for UX ----
MAX_REDEEM_ATTEMPTS = 3
MAX_REVOKE_ATTEMPTS = 3
EXIT_COMMANDS = {"cancel", "back", "/cancel", "/back", "/skip", "menu", "/menu"}

# --------------------------
# Inline keyboards (reuse)
# --------------------------
def _roles_base_kb():
    kb = [
        [InlineKeyboardButton("📩 Generate Invitation", callback_data='role:generate')],
        [InlineKeyboardButton("👥 View Members", callback_data='role:view_members')],
        [InlineKeyboardButton("✏️ Edit Member Role", callback_data='role:edit')],
        [InlineKeyboardButton("🗑️ Revoke Member", callback_data='role:revoke')],
        [InlineKeyboardButton("🔑 Redeem Invitation", callback_data='role:redeem')],
        [InlineKeyboardButton("🏠 Back", callback_data='role:back')],
        [InlineKeyboardButton("⏭️ /skip", callback_data='skip')],
    ]
    return InlineKeyboardMarkup(kb)

def _action_cancel_kb(cancel_label: str = "❌ Cancel", back_label: str = "◀ Back to Roles"):
    kb = [
        [
            InlineKeyboardButton(cancel_label, callback_data='role:cancel'),
            InlineKeyboardButton(back_label, callback_data='role:back'),
        ]
    ]
    return InlineKeyboardMarkup(kb)

def _invalid_retry_kb(retry_label: str = "🔁 Try Again", cancel_label: str = "❌ Cancel"):
    kb = [
        [
            InlineKeyboardButton(retry_label, callback_data='role:redeem'),
            InlineKeyboardButton(cancel_label, callback_data='role:cancel'),
        ]
    ]
    return InlineKeyboardMarkup(kb)

def _role_choice_kb(exclude_owner: bool = True):
    rows = []
    for r in FARM_ROLES:
        if exclude_owner and r == 'owner':
            continue
        rows.append([InlineKeyboardButton(f"{r.capitalize()}", callback_data=f'role:generate:{r}')])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data='role:cancel')])
    return InlineKeyboardMarkup(rows)

def _role_set_kb(member_id: str, exclude_owner: bool = True):
    rows = []
    for r in FARM_ROLES:
        if exclude_owner and r == 'owner':
            continue
        rows.append([InlineKeyboardButton(f"{r.capitalize()}", callback_data=f'role:setrole:{member_id}:{r}')])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data='role:cancel')])
    return InlineKeyboardMarkup(rows)

# --------------------------
# Helpers
# --------------------------
def _normalize_cmd(text: str) -> str:
    if not text:
        return ""
    return text.strip().lower()

async def _find_primary_farm_and_role_for_user(user_id: str) -> (Optional[str], Optional[str]):
    try:
        primary = await async_find_user_primary_farm(user_id)
        return primary.get("farm_id"), primary.get("role")
    except Exception:
        logger.exception("Failed to find primary farm for user %s", user_id)
        return None, None

# --------------------------
# MENU
# --------------------------
async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str = None):
    telegram_id = update.effective_user.id
    user_row = await async_get_user_by_telegram(telegram_id)
    if not user_row:
        if update.callback_query:
            await update.callback_query.edit_message_text("⚠️ You must register first. Use /start.")
        else:
            await update.message.reply_text("⚠️ You must register first. Use /start.")
        return

    user_id = user_row["id"]
    user_farm = await async_get_user_with_farm_by_telegram(telegram_id) or {}
    farm = user_farm.get("farm") or {}
    farm_id = farm.get("id")

    role = None
    if farm_id:
        try:
            role = await async_get_user_role_in_farm(user_id, farm_id)
        except Exception:
            logger.exception("Failed to get role for user %s in farm %s", user_id, farm_id)
    else:
        try:
            fallback = await async_find_user_primary_farm(user_id)
            farm_id = fallback.get("farm_id")
            role = fallback.get("role")
            logger.info("Fallback farm discovery for user %s -> farm=%s role=%s", user_id, farm_id, role)
        except Exception:
            logger.exception("Fallback farm discovery failed for %s", user_id)

    kb_rows = []
    premium = bool(user_row.get("premium_status"))
    if role in ['owner', 'manager']:
        if premium:
            kb_rows.append([InlineKeyboardButton("📩 Generate Invitation", callback_data='role:generate')])
            kb_rows.append([InlineKeyboardButton("👥 View Members", callback_data='role:view_members')])
            kb_rows.append([InlineKeyboardButton("✏️ Edit Member Role", callback_data='role:edit')])
            kb_rows.append([InlineKeyboardButton("🗑️ Revoke Member", callback_data='role:revoke')])
        else:
            kb_rows.append([InlineKeyboardButton("🔒 Upgrade to premium to manage members", callback_data='role:upgrade')])

    kb_rows.append([InlineKeyboardButton("🔑 Redeem Invitation", callback_data='role:redeem')])
    kb_rows.append([InlineKeyboardButton("🏠 Back", callback_data='role:back')])
    kb_rows.append([InlineKeyboardButton("⏭️ /skip", callback_data='skip')])

    kb = InlineKeyboardMarkup(kb_rows)

    text = f"👥 *Role Management* (Your role: {role or 'none'}):\n\n"
    if role in ['owner', 'manager']:
        if premium:
            text += "Invite workers, manage roles, or revoke access.\n"
        else:
            text += "You are an owner/manager but management features require premium.\n"
    text += "\nUse *Redeem Invitation* to join another farm with a code.\n\n"
    text += "Tip: type `cancel` anytime or press *Back to Roles* to return."

    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
        except Exception as e:
            if "Message is not modified" in str(e):
                await update.callback_query.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
            else:
                raise
        await update.callback_query.answer()
    else:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)

role_handlers['menu'] = menu
role_handlers['roles'] = menu  # Alias for /roles command

# --------------------------
# GENERATE INVITATION
# --------------------------
async def generate(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str = None):
    query = update.callback_query
    data = query.data or ""
    telegram_id = query.from_user.id
    user_row = await async_get_user_by_telegram(telegram_id)
    if not user_row:
        await query.edit_message_text("⚠️ You must register first. Use /start.")
        await query.answer()
        return

    if not user_row.get("premium_status"):
        await query.edit_message_text("🔒 Invitation generation requires a premium account.")
        await query.answer()
        return

    user_id = user_row["id"]
    farm_id, user_role = await _find_primary_farm_and_role_for_user(user_id)
    if not farm_id or user_role not in ['owner', 'manager']:
        await query.edit_message_text("⚠️ Only owners/managers of a farm can generate invitations.")
        await query.answer()
        return

    parts = data.split(":", 2)
    if len(parts) == 2:  # Initial press: role:generate
        await query.edit_message_text(
            "📩 *Generate Invitation* — choose role to invite:\n\nChoose the role the new person should have in your farm.",
            parse_mode="Markdown",
            reply_markup=_role_choice_kb(exclude_owner=True)
        )
        await query.answer()
        return

    chosen_role = parts[2] if len(parts) >= 3 else None
    if not chosen_role or chosen_role not in FARM_ROLES:
        await query.edit_message_text("⚠️ Invalid role selected. Operation cancelled.", reply_markup=_action_cancel_kb())
        await query.answer()
        return

    invite = await async_create_invitation(farm_id=farm_id, role=chosen_role, created_by=user_id)
    if not invite:
        await query.edit_message_text("⚠️ Failed to generate invitation.")
        await query.answer()
        return

    await query.edit_message_text(
        f"🎉 Invitation Code: *{invite['code']}*\n\n"
        f"Role: *{invite['role']}*\nExpires: *{invite.get('expires_at', 'unknown')}*\n\n"
        "Share this code with the person to join your farm.",
        parse_mode="Markdown"
    )
    await query.answer()

role_handlers['generate'] = generate

# --------------------------
# REDEEM INVITATION
# --------------------------
async def redeem(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str = None):
    query = update.callback_query
    telegram_id = query.from_user.id
    user_row = await async_get_user_by_telegram(telegram_id)
    if not user_row:
        await query.edit_message_text("⚠️ You must register first. Use /start.")
        await query.answer()
        return

    context.user_data['flow'] = 'role:redeem'
    context.user_data['role_redeem_attempts'] = 0

    await query.edit_message_text(
        "🔑 *Redeem Invitation*\n\n"
        "Please enter the invitation code to join a farm.\n\n"
        "If you don't have a code, press *Back to Roles* or type `cancel`.",
        parse_mode="Markdown",
        reply_markup=_action_cancel_kb()
    )
    await query.answer()

role_handlers['redeem'] = redeem

# --------------------------
# REVOKE MEMBER
# --------------------------
async def revoke(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str = None):
    query = update.callback_query
    telegram_id = query.from_user.id
    user_row = await async_get_user_by_telegram(telegram_id)
    if not user_row:
        await query.edit_message_text("⚠️ You must register first. Use /start.")
        await query.answer()
        return

    if not user_row.get("premium_status"):
        await query.edit_message_text("🔒 Revoking members requires a premium account.")
        await query.answer()
        return

    user_id = user_row["id"]
    primary = await async_find_user_primary_farm(user_id)
    farm_id = primary.get("farm_id")
    role = primary.get("role")
    if role not in ['owner', 'manager']:
        await query.edit_message_text("⚠️ Only owners/managers can revoke members.")
        await query.answer()
        return

    members = await async_get_farm_members(farm_id)
    if not members:
        await query.edit_message_text("🙁 No members to revoke.")
        await query.answer()
        return

    lines = []
    for m in members:
        mid = m.get('id') or "<id?>"
        uid = m.get('user_id') or "<user_id?>"
        lines.append(f"• `{mid}` — user_id: `{uid}` — role: *{m.get('role')}*")

    text = "🗑️ *Revoke Member*\n\n" + "\n".join(lines)
    text += "\n\nEnter the *farm_members.id* or *user_id* to revoke. Type `cancel` to exit."

    context.user_data['flow'] = 'role:revoke'
    context.user_data['role_revoke_attempts'] = 0

    await query.edit_message_text(text, parse_mode="Markdown", reply_markup=_action_cancel_kb())
    await query.answer()

role_handlers['revoke'] = revoke

# --------------------------
# VIEW MEMBERS
# --------------------------
async def view_members(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str = None):
    query = update.callback_query
    telegram_id = query.from_user.id
    user_row = await async_get_user_by_telegram(telegram_id)
    if not user_row:
        await query.edit_message_text("⚠️ You must register first. Use /start.")
        await query.answer()
        return

    if not user_row.get("premium_status"):
        await query.edit_message_text("🔒 Viewing members requires a premium account.")
        await query.answer()
        return

    user_id = user_row["id"]
    primary = await async_find_user_primary_farm(user_id)
    farm_id = primary.get("farm_id")
    role = primary.get("role")
    if role not in ['owner', 'manager']:
        await query.edit_message_text("⚠️ Only owners/managers can view members.")
        await query.answer()
        return

    members = await async_get_farm_members(farm_id)
    if not members:
        await query.edit_message_text("🙁 No members in your farm yet.")
        await query.answer()
        return

    lines = []
    for m in members:
        mid = m.get('id') or "<id?>"
        uid = m.get('user_id') or "<user_id?>"
        lines.append(f"• `{mid}` — user_id: `{uid}` — role: *{m.get('role') or 'unknown'}* (can_edit: {m.get('can_edit')})")

    text = "👥 *Farm Members*:\n\n" + "\n".join(lines) + "\n\n_To revoke a member, use Revoke Member and enter the `farm_members.id` or the `user_id` shown above._"

    await query.edit_message_text(text, parse_mode="Markdown", reply_markup=_action_cancel_kb())
    await query.answer()

role_handlers['view_members'] = view_members

# --------------------------
# EDIT MEMBER ROLE
# --------------------------
async def edit(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str = None):
    query = update.callback_query
    data = query.data or ""
    telegram_id = query.from_user.id
    user_row = await async_get_user_by_telegram(telegram_id)
    if not user_row:
        await query.edit_message_text("⚠️ You must register first. Use /start.")
        await query.answer()
        return

    if not user_row.get("premium_status"):
        await query.edit_message_text("🔒 Editing roles requires a premium account.")
        await query.answer()
        return

    user_id = user_row["id"]
    primary = await async_find_user_primary_farm(user_id)
    farm_id = primary.get("farm_id")
    role = primary.get("role")
    if role not in ['owner', 'manager']:
        await query.edit_message_text("⚠️ Only owners/managers can edit member roles.")
        await query.answer()
        return

    parts = data.split(":", 2)
    if len(parts) == 2:  # Initial press: role:edit
        members = await async_get_farm_members(farm_id)
        if not members:
            await query.edit_message_text("🙁 No members to edit.", reply_markup=_action_cancel_kb())
            await query.answer()
            return

        kb = []
        for m in members:
            mid = m.get('id') or "<id?>"
            mrole = m.get('role') or 'unknown'
            label = f"{m.get('user_name') or m.get('user_id') or mid} — {mrole}"
            kb.append([InlineKeyboardButton(label, callback_data=f'role:edit:{mid}')])
        kb.append([InlineKeyboardButton("❌ Cancel", callback_data='role:cancel')])
        await query.edit_message_text("✏️ Select a member to change their role:", reply_markup=InlineKeyboardMarkup(kb))
        await query.answer()
        return

    member_id = parts[2] if len(parts) >= 3 else None
    if not member_id:
        await query.edit_message_text("⚠️ Invalid member selected.", reply_markup=_action_cancel_kb())
        await query.answer()
        return

    await query.edit_message_text(
        "✏️ Choose new role for selected member:",
        reply_markup=_role_set_kb(member_id, exclude_owner=True)
    )
    await query.answer()

role_handlers['edit'] = edit

# --------------------------
# SET ROLE
# --------------------------
async def setrole(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str = None):
    query = update.callback_query
    data = query.data or ""
    telegram_id = query.from_user.id
    user_row = await async_get_user_by_telegram(telegram_id)
    if not user_row:
        await query.edit_message_text("⚠️ You must register first. Use /start.")
        await query.answer()
        return

    user_id = user_row["id"]
    parts = data.split(":")
    if len(parts) < 4:
        await query.edit_message_text("⚠️ Invalid operation.", reply_markup=_action_cancel_kb())
        await query.answer()
        return

    member_id = parts[2]
    new_role = parts[3]
    if new_role not in FARM_ROLES:
        await query.edit_message_text("⚠️ Invalid role.", reply_markup=_action_cancel_kb())
        await query.answer()
        return

    primary = await async_find_user_primary_farm(user_id)
    farm_id = primary.get("farm_id")
    role = primary.get("role")
    if role not in ['owner', 'manager']:
        await query.edit_message_text("⚠️ Only owners/managers can change member roles.", reply_markup=_action_cancel_kb())
        await query.answer()
        return

    ok = await async_update_member_role(member_id=member_id, new_role=new_role, changed_by=user_id)
    if not ok:
        await query.edit_message_text("⚠️ Failed to update member role. Check logs.", reply_markup=_action_cancel_kb())
        await query.answer()
        return

    await query.edit_message_text(f"✅ Member role updated to *{new_role}*.", parse_mode="Markdown")
    await query.answer()

    try:
        await async_notify_owner(
            farm_id,
            f"🔔 Member {member_id} role changed to {new_role} by {user_row.get('name')} (tg:{telegram_id})",
            context.bot
        )
    except Exception:
        logger.exception("Failed to notify owner after role change (non-fatal)")

role_handlers['setrole'] = setrole

# --------------------------
# CANCEL / BACK
# --------------------------
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str = None):
    query = update.callback_query
    context.user_data.pop('flow', None)
    context.user_data.pop('role_redeem_attempts', None)
    context.user_data.pop('role_revoke_attempts', None)
    await menu(update, context)
    await query.answer()

role_handlers['cancel'] = cancel

async def back(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str = None):
    query = update.callback_query
    from keyboard import get_inline_main_menu
    context.user_data.pop('flow', None)
    context.user_data.pop('role_redeem_attempts', None)
    context.user_data.pop('role_revoke_attempts', None)
    await query.edit_message_text("🔵 Back to main menu:", reply_markup=get_inline_main_menu())
    await query.answer()

role_handlers['back'] = back

# --------------------------
# TEXT HANDLER
# --------------------------
async def handle_text_dispatcher(update: Update, context: ContextTypes.DEFAULT_TYPE):
    flow = context.user_data.get('flow', '')
    if not flow:
        return

    if flow == 'role:redeem':
        await _handle_redeem_text_impl(update, context)
    elif flow == 'role:revoke':
        await _handle_revoke_text_impl(update, context)

role_handlers['handle_text'] = handle_text_dispatcher

# --------------------------
# Redeem text implementation
# --------------------------
async def _handle_redeem_text_impl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    cmd = _normalize_cmd(text)
    user_telegram = update.effective_user.id

    if cmd in EXIT_COMMANDS:
        context.user_data.pop('flow', None)
        context.user_data.pop('role_redeem_attempts', None)
        await update.message.reply_text("⏭️ Cancelled. Back to roles.", reply_markup=_action_cancel_kb())
        await menu(update, context)
        return

    user_row = await async_get_user_by_telegram(user_telegram)
    if not user_row:
        context.user_data.pop('flow', None)
        await update.message.reply_text("⚠️ You must register first. Use /start.")
        return

    user_id = user_row["id"]
    code = text.upper().strip()

    result = await async_redeem_invitation(code, user_id)
    if result:
        farm_id = result["farm_id"]
        role = result["role"]
        context.user_data.pop('flow', None)
        context.user_data.pop('role_redeem_attempts', None)
        await update.message.reply_text(
            f"✅ Joined farm as *{role}*! You can now access permitted features.",
            parse_mode="Markdown"
        )
        try:
            await async_notify_owner(
                farm_id,
                f"🔔 New member joined: {user_row.get('name')} as {role} (tg:{user_telegram})",
                context.bot
            )
        except Exception:
            logger.exception("Failed to notify owner after redeem (non-fatal)")
        return

    attempts = context.user_data.get('role_redeem_attempts', 0) + 1
    context.user_data['role_redeem_attempts'] = attempts

    if attempts >= MAX_REDEEM_ATTEMPTS:
        context.user_data.pop('flow', None)
        context.user_data.pop('role_redeem_attempts', None)
        await update.message.reply_text(
            "⚠️ Invalid or expired code. You've reached the maximum attempts. Returning to Roles menu.",
            reply_markup=_action_cancel_kb()
        )
        await menu(update, context)
        return

    await update.message.reply_text(
        f"⚠️ Invalid or expired code. Attempts: {attempts}/{MAX_REDEEM_ATTEMPTS}. "
        "Please check the code and try again, or press Cancel to exit.",
        parse_mode="Markdown",
        reply_markup=_invalid_retry_kb()
    )

# --------------------------
# Revoke text implementation
# --------------------------
async def _handle_redeem_text_impl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    cmd = _normalize_cmd(text)
    user_telegram = update.effective_user.id

    if cmd in EXIT_COMMANDS:
        context.user_data.pop('flow', None)
        context.user_data.pop('role_redeem_attempts', None)
        await update.message.reply_text("⏭️ Cancelled. Back to roles.", reply_markup=_action_cancel_kb())
        await menu(update, context)
        return

    user_row = await async_get_user_by_telegram(user_telegram)
    if not user_row:
        context.user_data.pop('flow', None)
        await update.message.reply_text("⚠️ You must register first. Use /start.")
        return

    user_id = user_row["id"]
    code = text.upper().strip()

    result = await async_redeem_invitation(code, user_id)
    if result:
        farm_id = result["farm_id"]
        role = result["role"]
        context.user_data.pop('flow', None)
        context.user_data.pop('role_redeem_attempts', None)
        await update.message.reply_text(
            f"✅ Joined farm as *{role}*! You can now access permitted features.",
            parse_mode="Markdown"
        )
        try:
            await async_notify_owner(
                farm_id,
                f"🔔 New member joined: {user_row.get('name')} as {role} (tg:{user_telegram})",
                context.bot
            )
        except Exception:
            logger.exception("Failed to notify owner after redeem (non-fatal)")
        return

    attempts = context.user_data.get('role_redeem_attempts', 0) + 1
    context.user_data['role_redeem_attempts'] = attempts

    if attempts >= MAX_REDEEM_ATTEMPTS:
        context.user_data.pop('flow', None)
        context.user_data.pop('role_redeem_attempts', None)
        await update.message.reply_text(
            "⚠️ Invalid or expired code. You've reached the maximum attempts. Returning to Roles menu.",
            reply_markup=_action_cancel_kb()
        )
        await menu(update, context)
        return

    await update.message.reply_text(
        f"⚠️ Invalid or expired code. Attempts: {attempts}/{MAX_REDEEM_ATTEMPTS}. "
        "Please check the code and try again, or press Cancel to exit.",
        parse_mode="Markdown",
        reply_markup=_invalid_retry_kb()
    )

# --------------------------
# Revoke text implementation
# --------------------------
async def _handle_revoke_text_impl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    cmd = _normalize_cmd(text)
    user_telegram = update.effective_user.id

    if cmd in EXIT_COMMANDS:
        context.user_data.pop('flow', None)
        context.user_data.pop('role_revoke_attempts', None)
        await update.message.reply_text("⏭️ Cancelled. Back to roles.", reply_markup=_action_cancel_kb())
        await menu(update, context)
        return

    user_row = await async_get_user_by_telegram(user_telegram)
    if not user_row:
        context.user_data.pop('flow', None)
        await update.message.reply_text("⚠️ You must register first. Use /start.")
        return

    user_id = user_row["id"]
    primary = await async_find_user_primary_farm(user_id)
    farm_id = primary.get("farm_id")
    if not farm_id:
        context.user_data.pop('flow', None)
        await update.message.reply_text("⚠️ Could not determine your farm. Returning to Roles.")
        await menu(update, context)
        return

    member_identifier = text
    result = await async_revoke_member(farm_id=farm_id, member_id=member_identifier)
    if not result:
        result = await async_revoke_member(farm_id=farm_id, member_user_id=member_identifier)

    if result:
        context.user_data.pop('flow', None)
        context.user_data.pop('role_revoke_attempts', None)
        await update.message.reply_text("✅ Member revoked successfully!")
        try:
            await async_notify_owner(
                farm_id,
                f"🔔 Member {member_identifier} revoked by {user_row.get('name')} (tg:{user_telegram})",
                context.bot
            )
        except Exception:
            logger.exception("Failed to notify owner after revoke (non-fatal)")
        return

    attempts = context.user_data.get('role_revoke_attempts', 0) + 1
    context.user_data['role_revoke_attempts'] = attempts

    if attempts >= MAX_REVOKE_ATTEMPTS:
        context.user_data.pop('flow', None)
        context.user_data.pop('role_revoke_attempts', None)
        await update.message.reply_text(
            "⚠️ Failed to revoke member after several attempts. Returning to Roles menu.",
            reply_markup=_action_cancel_kb()
        )
        await menu(update, context)
        return

    await update.message.reply_text(
        f"⚠️ Failed to revoke member (invalid id or permission). Attempts: {attempts}/{MAX_REVOKE_ATTEMPTS}. "
        "Please retry with the `farm_members.id` shown in View Members or type `cancel` to exit.",
        parse_mode="Markdown",
        reply_markup=_action_cancel_kb()
    )






'''# aboutrole.py
import logging
from typing import Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from farmcore import async_get_user_by_telegram, async_get_user_with_farm_by_telegram
from farmcore_role import (
    async_create_invitation,
    async_redeem_invitation,
    async_list_invitations,
    async_get_farm_members,
    async_revoke_member,
    async_get_user_role_in_farm,
    async_notify_owner,
    async_find_user_primary_farm,
)

logger = logging.getLogger(__name__)
role_handlers = {}

# ---- small constants for UX ----
MAX_REDEEM_ATTEMPTS = 3
MAX_REVOKE_ATTEMPTS = 3
EXIT_COMMANDS = {"cancel", "back", "/cancel", "/back", "/skip", "menu", "/menu"}

# --------------------------
# Inline keyboards (reuse)
# --------------------------
def _roles_base_kb():
    # shown on the main roles menu
    kb = [
        [InlineKeyboardButton("📩 Generate Invitation", callback_data='role:generate')],
        [InlineKeyboardButton("👥 View Members", callback_data='role:view_members')],
        [InlineKeyboardButton("🗑️ Revoke Member", callback_data='role:revoke')],
        [InlineKeyboardButton("🔑 Redeem Invitation", callback_data='role:redeem')],
        [InlineKeyboardButton("🏠 Back", callback_data='role:back')],
        [InlineKeyboardButton("⏭️ /skip", callback_data='skip')]
    ]
    return InlineKeyboardMarkup(kb)

def _action_cancel_kb(cancel_label: str = "❌ Cancel", back_label: str = "◀ Back to Roles"):
    # Used during multi-step text flows so user can always exit
    kb = [
        [
            InlineKeyboardButton(cancel_label, callback_data='role:cancel'),
            InlineKeyboardButton(back_label, callback_data='role:back')
        ]
    ]
    return InlineKeyboardMarkup(kb)

def _invalid_retry_kb(retry_label: str = "🔁 Try Again", cancel_label: str = "❌ Cancel"):
    kb = [
        [
            InlineKeyboardButton(retry_label, callback_data='role:redeem'),
            InlineKeyboardButton(cancel_label, callback_data='role:cancel')
        ]
    ]
    return InlineKeyboardMarkup(kb)

# --------------------------
# Helpers
# --------------------------
def _normalize_cmd(text: str) -> str:
    if not text:
        return ""
    return text.strip().lower()

async def _find_primary_farm_and_role_for_user(user_id: str) -> (Optional[str], Optional[str]):
    """
    Try to detect a farm_id and role for the given user_id.
    Uses user_with_farm first (fast path) and then fallback search.
    """
    try:
        primary = await async_find_user_primary_farm(user_id)
        return primary.get("farm_id"), primary.get("role")
    except Exception:
        logger.exception("Failed to find primary farm for user %s", user_id)
        return None, None

# --------------------------
# MENU
# --------------------------
async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    telegram_id = update.effective_user.id
    user_row = await async_get_user_by_telegram(telegram_id)
    if not user_row:
        await (update.callback_query.edit_message_text if update.callback_query else update.message.reply_text)(
            "⚠️ You must register first. Use /start."
        )
        return

    user_id = user_row["id"]
    # Try the helper that your farmcore may have; but fall back to find_user_primary_farm if needed.
    user_farm = await async_get_user_with_farm_by_telegram(telegram_id) or {}
    farm = user_farm.get("farm") or {}
    farm_id = farm.get("id")

    role = None
    if farm_id:
        try:
            role = await async_get_user_role_in_farm(user_id, farm_id)
        except Exception:
            logger.exception("Failed to get role for user %s in farm %s", user_id, farm_id)
    else:
        # fallback discovery
        try:
            fallback = await async_find_user_primary_farm(user_id)
            farm_id = fallback.get("farm_id")
            role = fallback.get("role")
            logger.info("Fallback farm discovery for user %s -> farm=%s role=%s", user_id, farm_id, role)
        except Exception:
            logger.exception("Fallback farm discovery failed for %s", user_id)

    # Build keyboard based on role and premium
    kb_rows = []
    premium = bool(user_row.get("premium_status"))
    # If owner/manager and premium -> show management rows; else show message about premium if owner/manager
    if role in ['owner', 'manager']:
        if premium:
            kb_rows.append([InlineKeyboardButton("📩 Generate Invitation", callback_data='role:generate')])
            kb_rows.append([InlineKeyboardButton("👥 View Members", callback_data='role:view_members')])
            kb_rows.append([InlineKeyboardButton("🗑️ Revoke Member", callback_data='role:revoke')])
        else:
            kb_rows.append([InlineKeyboardButton("🔒 Upgrade to premium to manage members", callback_data='role:upgrade')])

    # Redeem always available
    kb_rows.append([InlineKeyboardButton("🔑 Redeem Invitation", callback_data='role:redeem')])
    kb_rows.append([InlineKeyboardButton("🏠 Back", callback_data='role:back')])
    kb_rows.append([InlineKeyboardButton("⏭️ /skip", callback_data='skip')])

    kb = InlineKeyboardMarkup(kb_rows)

    text = f"👥 *Role Management* (Your role: {role or 'none'}):\n\n"
    if role in ['owner', 'manager']:
        if premium:
            text += "Invite workers, manage roles, or revoke access.\n"
        else:
            text += "You are an owner/manager but management features require premium.\n"
    text += "\nUse *Redeem Invitation* to join another farm with a code.\n\n"
    text += "Tip: type `cancel` anytime or press *Back to Roles* to return."

    await (update.callback_query.edit_message_text if update.callback_query else update.message.reply_text)(
        text, parse_mode="Markdown", reply_markup=kb
    )

role_handlers['menu'] = menu

# --------------------------
# GENERATE INVITATION
# --------------------------
async def generate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    telegram_id = query.from_user.id
    user_row = await async_get_user_by_telegram(telegram_id)
    if not user_row:
        await query.edit_message_text("⚠️ You must register first. Use /start.")
        return

    if not user_row.get("premium_status"):
        await query.edit_message_text("🔒 Invitation generation requires a premium account.")
        return

    user_id = user_row["id"]
    farm_id, role = await _find_primary_farm_and_role_for_user(user_id)
    if not farm_id or role not in ['owner', 'manager']:
        await query.edit_message_text("⚠️ Only owners/managers of a farm can generate invitations.")
        return

    invite = await async_create_invitation(farm_id=farm_id, role='worker', created_by=user_id)
    if not invite:
        await query.edit_message_text("⚠️ Failed to generate invitation.")
        return

    await query.edit_message_text(
        f"🎉 Invitation Code: *{invite['code']}*\n\n"
        f"Role: *{invite['role']}*\nExpires: *{invite.get('expires_at', 'unknown')}*\n\n"
        "Share this code with workers to join your farm.",
        parse_mode="Markdown"
    )

role_handlers['generate'] = generate

# --------------------------
# REDEEM INVITATION (start)
# --------------------------
async def redeem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    User tapped Redeem Invitation — start a text-flow.
    We set context.user_data['flow'] = 'role:redeem' and initialize attempt counter.
    Always show an inline cancel/back keyboard so user can exit.
    """
    query = update.callback_query
    telegram_id = query.from_user.id
    user_row = await async_get_user_by_telegram(telegram_id)
    if not user_row:
        await query.edit_message_text("⚠️ You must register first. Use /start.")
        return

    context.user_data['flow'] = 'role:redeem'
    context.user_data['role_redeem_attempts'] = 0

    await query.edit_message_text(
        "🔑 *Redeem Invitation*\n\n"
        "Please enter the invitation code to join a farm.\n\n"
        "If you don't have a code, press *Back to Roles* or type `cancel`.",
        parse_mode="Markdown",
        reply_markup=_action_cancel_kb()
    )

role_handlers['redeem'] = redeem

# --------------------------
# REVOKE MEMBER (start)
# --------------------------
async def revoke(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Start revoke flow: set flow, list members and instruct user to enter id.
    Provide cancel/back inline keyboard.
    """
    query = update.callback_query
    telegram_id = query.from_user.id
    user_row = await async_get_user_by_telegram(telegram_id)
    if not user_row:
        await query.edit_message_text("⚠️ You must register first. Use /start.")
        return

    if not user_row.get("premium_status"):
        await query.edit_message_text("🔒 Revoking members requires a premium account.")
        return

    user_id = user_row["id"]
    primary = await async_find_user_primary_farm_for_user(user_id := user_id) if False else None
    # Use existing helper
    primary = await async_find_user_primary_farm(user_id)
    farm_id = primary.get("farm_id")
    role = primary.get("role")
    if role not in ['owner', 'manager']:
        await query.edit_message_text("⚠️ Only owners/managers can revoke members.")
        return

    members = await async_get_farm_members(farm_id)
    if not members:
        await query.edit_message_text("🙁 No members to revoke.")
        return

    lines = []
    for m in members:
        mid = m.get('id') or "<id?>"
        uid = m.get('user_id') or "<user_id?>"
        lines.append(f"• `{mid}` — user_id: `{uid}` — role: *{m.get('role')}*")

    text = "🗑️ *Revoke Member*\n\n" + "\n".join(lines)
    text += "\n\nEnter the *farm_members.id* or *user_id* to revoke. Type `cancel` to exit."

    context.user_data['flow'] = 'role:revoke'
    context.user_data['role_revoke_attempts'] = 0

    await query.edit_message_text(text, parse_mode="Markdown", reply_markup=_action_cancel_kb())

role_handlers['revoke'] = revoke

# --------------------------
# VIEW MEMBERS
# --------------------------
async def view_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    telegram_id = query.from_user.id
    user_row = await async_get_user_by_telegram(telegram_id)
    if not user_row:
        await query.edit_message_text("⚠️ You must register first. Use /start.")
        return

    if not user_row.get("premium_status"):
        await query.edit_message_text("🔒 Viewing members requires a premium account.")
        return

    user_id = user_row["id"]
    primary = await async_find_user_primary_farm(user_id)
    farm_id = primary.get("farm_id")
    role = primary.get("role")
    if role not in ['owner', 'manager']:
        await query.edit_message_text("⚠️ Only owners/managers can view members.")
        return

    members = await async_get_farm_members(farm_id)
    if not members:
        await query.edit_message_text("🙁 No members in your farm yet.")
        return

    lines = []
    for m in members:
        mid = m.get('id') or "<id?>"
        uid = m.get('user_id') or "<user_id?>"
        lines.append(f"• `{mid}` — user_id: `{uid}` — role: *{m.get('role') or 'unknown'}* (can_edit: {m.get('can_edit')})")

    text = "👥 *Farm Members*:\n\n" + "\n".join(lines) + "\n\n_To revoke a member, use Revoke Member and enter the `farm_members.id` or the `user_id` shown above._"

    await query.edit_message_text(text, parse_mode="Markdown", reply_markup=_action_cancel_kb())

role_handlers['view_members'] = view_members

# --------------------------
# CANCEL / BACK handlers
# --------------------------
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Generic cancel callback (role:cancel). Clear any role flow keys and return to role menu.
    """
    query = update.callback_query
    # Clear role-specific flow keys
    context.user_data.pop('flow', None)
    context.user_data.pop('role_redeem_attempts', None)
    context.user_data.pop('role_revoke_attempts', None)

    # Show main roles menu
    await menu(update, context)

role_handlers['cancel'] = cancel

async def back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Back to main menu (inline main menu from keyboard module)
    query = update.callback_query
    from keyboard import get_inline_main_menu
    context.user_data.pop('flow', None)
    context.user_data.pop('role_redeem_attempts', None)
    context.user_data.pop('role_revoke_attempts', None)
    await query.edit_message_text("🔵 Back to main menu:", reply_markup=get_inline_main_menu())

role_handlers['back'] = back

# --------------------------
# Text handling dispatcher
# --------------------------
async def handle_text_dispatcher(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Central text handler for role flows.
    The main app calls role_handlers['handle_text'] for any text while flow.startswith('role').
    We respect context.user_data['flow'] to dispatch to the correct handler.
    """
    flow = context.user_data.get('flow', '')
    if not flow:
        return  # nothing for us to do

    if flow == 'role:redeem':
        await _handle_redeem_text_impl(update, context)
        return
    if flow == 'role:revoke':
        await _handle_revoke_text_impl(update, context)
        return

# register dispatcher as the module's text handler
role_handlers['handle_text'] = handle_text_dispatcher

# --------------------------
# Redeem text implementation
# --------------------------
async def _handle_redeem_text_impl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    cmd = _normalize_cmd(text)
    user_telegram = update.effective_user.id

    # allow user to exit via typed commands
    if cmd in EXIT_COMMANDS:
        context.user_data.pop('flow', None)
        context.user_data.pop('role_redeem_attempts', None)
        await update.message.reply_text("⏭️ Cancelled. Back to roles.", reply_markup=_action_cancel_kb())
        # Show roles menu
        await menu(update, context)
        return

    user_row = await async_get_user_by_telegram(user_telegram)
    if not user_row:
        context.user_data.pop('flow', None)
        await update.message.reply_text("⚠️ You must register first. Use /start.")
        return

    user_id = user_row["id"]
    code = text.upper().strip()

    # Attempt redeem
    result = await async_redeem_invitation(code, user_id)
    if result:
        farm_id = result["farm_id"]
        role = result["role"]
        context.user_data.pop('flow', None)
        context.user_data.pop('role_redeem_attempts', None)
        await update.message.reply_text(
            f"✅ Joined farm as *{role}*! You can now access permitted features.",
            parse_mode="Markdown"
        )
        # Notify owner (best-effort)
        try:
            await async_notify_owner(
                farm_id,
                f"🔔 New member joined: {user_row.get('name')} as {role} (tg:{user_telegram})",
                context.bot
            )
        except Exception:
            logger.exception("Failed to notify owner after redeem (non-fatal)")
        return

    # If we get here, redeem failed
    attempts = context.user_data.get('role_redeem_attempts', 0) + 1
    context.user_data['role_redeem_attempts'] = attempts

    if attempts >= MAX_REDEEM_ATTEMPTS:
        # auto-cancel after too many tries
        context.user_data.pop('flow', None)
        context.user_data.pop('role_redeem_attempts', None)
        await update.message.reply_text(
            "⚠️ Invalid or expired code. You've reached the maximum attempts. Returning to Roles menu.",
            reply_markup=_action_cancel_kb()
        )
        await menu(update, context)
        return

    # Give the user another chance, show helpful text and inline buttons
    await update.message.reply_text(
        f"⚠️ Invalid or expired code. Attempts: {attempts}/{MAX_REDEEM_ATTEMPTS}. "
        "Please check the code and try again, or press Cancel to exit.",
        parse_mode="Markdown",
        reply_markup=_invalid_retry_kb()
    )
    # keep flow active so next text is handled here

# --------------------------
# Revoke text implementation
# --------------------------
async def _handle_revoke_text_impl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    cmd = _normalize_cmd(text)
    user_telegram = update.effective_user.id

    # allow user to exit via typed commands
    if cmd in EXIT_COMMANDS:
        context.user_data.pop('flow', None)
        context.user_data.pop('role_revoke_attempts', None)
        await update.message.reply_text("⏭️ Cancelled. Back to roles.", reply_markup=_action_cancel_kb())
        await menu(update, context)
        return

    user_row = await async_get_user_by_telegram(user_telegram)
    if not user_row:
        context.user_data.pop('flow', None)
        await update.message.reply_text("⚠️ You must register first. Use /start.")
        return

    user_id = user_row["id"]
    primary = await async_find_user_primary_farm(user_id)
    farm_id = primary.get("farm_id")
    if not farm_id:
        context.user_data.pop('flow', None)
        await update.message.reply_text("⚠️ Could not determine your farm. Returning to Roles.")
        await menu(update, context)
        return

    member_identifier = text  # accept either farm_members.id or user_id

    # Try revoke by farm_members.id first, then by user_id
    result = await async_revoke_member(farm_id=farm_id, member_id=member_identifier)
    if not result:
        result = await async_revoke_member(farm_id=farm_id, member_user_id=member_identifier)

    if result:
        context.user_data.pop('flow', None)
        context.user_data.pop('role_revoke_attempts', None)
        await update.message.reply_text("✅ Member revoked successfully!")
        try:
            await async_notify_owner(
                farm_id,
                f"🔔 Member {member_identifier} revoked by {user_row.get('name')} (tg:{user_telegram})",
                context.bot
            )
        except Exception:
            logger.exception("Failed to notify owner after revoke (non-fatal)")
        return

    # failed revoke -> increment attempts and allow retry or cancel
    attempts = context.user_data.get('role_revoke_attempts', 0) + 1
    context.user_data['role_revoke_attempts'] = attempts

    if attempts >= MAX_REVOKE_ATTEMPTS:
        context.user_data.pop('flow', None)
        context.user_data.pop('role_revoke_attempts', None)
        await update.message.reply_text(
            "⚠️ Failed to revoke member after several attempts. Returning to Roles menu.",
            reply_markup=_action_cancel_kb()
        )
        await menu(update, context)
        return

    await update.message.reply_text(
        f"⚠️ Failed to revoke member (invalid id or permission). Attempts: {attempts}/{MAX_REVOKE_ATTEMPTS}. "
        "Please retry with the `farm_members.id` shown in View Members or type `cancel` to exit.",
        reply_markup=_action_cancel_kb()
    )

# ensure the module exports the handlers map
# role_handlers already populated above with keys:
# 'menu','generate','redeem','revoke','view_members','back','cancel','handle_text'

'''