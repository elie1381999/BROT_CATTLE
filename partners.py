# partners.py
import os
import uuid
import logging
from typing import List, Any

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

# farmcore exports: supabase client and a helper to get user id from telegram_id
from farmcore import supabase, get_user_id

logger = logging.getLogger("partners")

partner_handlers = {}

# Bot usernames (set in .env). Fallback values if not present.
FARM_BOT_USERNAME = os.getenv("FARM_BOT_USERNAME", "brot_cattle_farm_bot")
PARTNER_BOT_USERNAME = os.getenv("PARTNER_BOT_USERNAME", "brot_partner_bot")

# ------------------------
# Helpers
# ------------------------
def md_escape_v2(text: str) -> str:
    """Escape text for Telegram MarkdownV2 (simple implementation)."""
    if text is None:
        return ""
    text = str(text)
    # escape backslash first
    text = text.replace("\\", "\\\\")
    # escape the MarkdownV2 special characters
    for ch in "_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text

# ================= MENU ==================
async def menu(update: Update, context):
    kb = [
        [InlineKeyboardButton("🧾 Invite Farmers (free month)", callback_data="partner:invite")],
        [InlineKeyboardButton("🤝 Join Partner Program (earn $)", callback_data="partner:join")],
        [InlineKeyboardButton("📈 Who joined by me?", callback_data="partner:joined")],
        [InlineKeyboardButton("📈 View Referrals (debug)", callback_data="partner:view")],
        [InlineKeyboardButton("🏠 Back", callback_data="partner:back")],
    ]
    text = (
        "🤝 *Partners & Marketing*\n\n"
        "Invite friends (farmers) to FarmBot to earn *free months* when they upgrade to premium.\n\n"
        "Or join our Partner program to earn *real money* (open PartnerBot)."
    )

    # Use MarkdownV2; no dynamic content here so safe (but keep consistent escaping if needed)
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(
                md_escape_v2(text), parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup(kb)
            )
        except Exception:
            # fallback to reply (in case edit fails)
            await update.callback_query.message.reply_text(md_escape_v2(text), parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup(kb))
    else:
        await update.message.reply_text(md_escape_v2(text), parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup(kb))


partner_handlers["menu"] = menu


# =============== INVITE (farmer -> free month) =================
async def invite(update: Update, context):
    """
    Generate an invite deep-link for farmers that gives the inviter a free month
    when the invited user upgrades to premium. FarmBot must handle the start param.
    """
    # get caller's DB user id
    if not update.callback_query or not update.callback_query.from_user:
        return

    tg_user = update.callback_query.from_user
    tg_id = tg_user.id

    user_id = None
    try:
        user_id = get_user_id(tg_id)
    except Exception:
        logger.exception("get_user_id failed")

    if not user_id:
        await update.callback_query.edit_message_text(
            md_escape_v2("⚠️ You must be registered first. Use /start."),
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    # create a short referral token and try to persist in invitation_codes
    referral_token = f"ref_{user_id}"
    try:
        code = (str(uuid.uuid4())[:8]).upper()
        insert_payload = {
            "farm_id": None,
            "code": f"REF-{code}",
            "role": "farmer_referral",
            "created_by": user_id,
            "meta": {"inviter_app_user_id": user_id},
        }
        res = supabase.table("invitation_codes").insert(insert_payload).execute()
        # supabase-py response: res.data may be present
        if res and getattr(res, "data", None):
            stored_code = res.data[0].get("code")
            referral_token = stored_code or referral_token
    except Exception:
        logger.exception("Failed to insert invitation_codes (non-fatal), falling back to token mapping")

    # Build deep link to FarmBot with start param.
    invite_link = f"https://t.me/{FARM_BOT_USERNAME}?start={referral_token}"
    # Put link in a button to avoid Markdown escaping issues
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Open FarmBot (invite)", url=invite_link)]])

    text = (
        "📢 *Invite a farmer*\n\n"
        "Share the button below with your farmer friends. When they register and later upgrade to premium, "
        "you'll get *1 month free* as a referral reward.\n\n"
        "Note: If they'd rather join the Partner program to earn cash, use *Join Partner Program*."
    )

    await update.callback_query.edit_message_text(md_escape_v2(text), parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)


partner_handlers["invite"] = invite


# ================= JOIN PARTNER (open PartnerBot) =================
async def join(update: Update, context, action=None):
    """
    Prompts the user to open the PartnerBot to onboard as a cash-earning partner.
    Uses PARTNER_BOT_USERNAME from env to craft t.me link.
    """
    partner_link = f"https://t.me/{PARTNER_BOT_USERNAME}?start=become_partner"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Open PartnerBot", url=partner_link)]])

    text = (
        "💼 *Join the Partner Program*\n\n"
        "Open PartnerBot to complete onboarding (KYC / payout details) and create promo links that pay cash commissions.\n\n"
        "After joining, you'll be able to generate promo codes, track commissions, and request payouts."
    )

    try:
        await update.callback_query.edit_message_text(md_escape_v2(text), parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    except Exception:
        # fallback: try to reply instead
        try:
            await update.callback_query.message.reply_text(md_escape_v2(text), parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
        except Exception:
            logger.exception("Failed to send join message")


partner_handlers["join"] = join


# =============== WHO JOINED BY ME? ==================
async def joined(update: Update, context):
    """
    Show users who joined by the caller's invite (inviter -> referred_by).
    Displays name, telegram id, premium status and joined date.
    """
    if not update.callback_query or not update.callback_query.from_user:
        return

    tg_user = update.callback_query.from_user
    tg_id = tg_user.id

    # find caller app_user row
    try:
        user_db_resp = supabase.table("app_users").select("id").eq("telegram_id", tg_id).single().execute()
        user_db = getattr(user_db_resp, "data", None)
    except Exception:
        logger.exception("DB error fetching caller app_user")
        await update.callback_query.edit_message_text(md_escape_v2("⚠️ Couldn’t find your account. Try /start again."), parse_mode=ParseMode.MARKDOWN_V2)
        return

    if not user_db:
        await update.callback_query.edit_message_text(md_escape_v2("⚠️ Couldn’t find your account. Try /start again."), parse_mode=ParseMode.MARKDOWN_V2)
        return

    try:
        # select more fields, ordered by created_at descending
        refs_resp = (
            supabase.table("app_users")
            .select("id,name,telegram_id,premium_status,created_at")
            .eq("referred_by", user_db["id"])
            .order("created_at", desc=True)
            .execute()
        )
        refs = getattr(refs_resp, "data", []) or []
    except Exception:
        logger.exception("DB error fetching referred users")
        refs = []

    if not refs:
        await update.callback_query.edit_message_text(md_escape_v2("🙁 No users have joined with your invite yet."), parse_mode=ParseMode.MARKDOWN_V2)
        return

    # Build output lines
    lines = []
    for r in refs:
        name = md_escape_v2(r.get("name", "-"))
        tg = md_escape_v2(str(r.get("telegram_id", "-")))
        premium = "Yes" if r.get("premium_status") else "No"
        premium = md_escape_v2(premium)
        joined_at = md_escape_v2(str(r.get("created_at", "-")))
        lines.append(f"• {name} (tg:{tg}) — Premium: {premium} — Joined: {joined_at}")

    text = "📋 *Users who joined via your link*\n\n" + "\n".join(lines)
    await update.callback_query.edit_message_text(md_escape_v2(text), parse_mode=ParseMode.MARKDOWN_V2)


partner_handlers["joined"] = joined


# =============== VIEW REFERRALS (debug / legacy) ==================
# kept for backward compatibility / debugging; can be removed later
async def view(update: Update, context):
    if not update.callback_query or not update.callback_query.from_user:
        return
    user = update.callback_query.from_user

    # find app_user row
    try:
        user_db_resp = supabase.table("app_users").select("id").eq("telegram_id", user.id).single().execute()
        user_db = getattr(user_db_resp, "data", None)
    except Exception:
        logger.exception("DB error fetching user_db for referrals")
        await update.callback_query.edit_message_text(
            md_escape_v2("⚠️ Couldn’t find your account. Try /start again."),
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    if not user_db:
        await update.callback_query.edit_message_text(
            md_escape_v2("⚠️ Couldn’t find your account. Try /start again."), parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    try:
        refs_resp = (
            supabase.table("app_users")
            .select("id,name,telegram_id,premium_status")
            .eq("referred_by", user_db["id"])
            .execute()
        )
        refs = getattr(refs_resp, "data", []) or []
    except Exception:
        logger.exception("DB error fetching referrals")
        refs = []

    if not refs:
        await update.callback_query.edit_message_text(md_escape_v2("🙁 You have no referrals yet."), parse_mode=ParseMode.MARKDOWN_V2)
        return

    # Build a safe list string
    lines = []
    for r in refs:
        name = md_escape_v2(r.get("name", "-"))
        tg = md_escape_v2(r.get("telegram_id", "-"))
        premium = md_escape_v2(str(r.get("premium_status", False)))
        lines.append(f"• {name} (tg:{tg}) — Premium: {premium}")

    text = "📈 *Your Referrals*:\n\n" + "\n".join(lines)
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2)


partner_handlers["view"] = view

# End of file
# partners.py
import os
import uuid
import logging
from typing import List, Any

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

# farmcore exports: supabase client and a helper to get user id from telegram_id
from farmcore import supabase, get_user_id

logger = logging.getLogger("partners")

partner_handlers = {}

# Bot usernames (set in .env). Fallback values if not present.
FARM_BOT_USERNAME = os.getenv("FARM_BOT_USERNAME", "brot_cattle_farm_bot")
PARTNER_BOT_USERNAME = os.getenv("PARTNER_BOT_USERNAME", "brot_partner_bot")

# ------------------------
# Helpers
# ------------------------
def md_escape_v2(text: str) -> str:
    """Escape text for Telegram MarkdownV2 (simple implementation)."""
    if text is None:
        return ""
    text = str(text)
    # escape backslash first
    text = text.replace("\\", "\\\\")
    # escape the MarkdownV2 special characters
    for ch in "_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text

# ================= MENU ==================
async def menu(update: Update, context):
    kb = [
        [InlineKeyboardButton("🧾 Invite Farmers (free month)", callback_data="partner:invite")],
        [InlineKeyboardButton("🤝 Join Partner Program (earn $)", callback_data="partner:join")],
        [InlineKeyboardButton("📈 Who joined by me?", callback_data="partner:joined")],
        [InlineKeyboardButton("📈 View Referrals (debug)", callback_data="partner:view")],
        [InlineKeyboardButton("🏠 Back", callback_data="partner:back")],
    ]
    text = (
        "🤝 *Partners & Marketing*\n\n"
        "Invite friends (farmers) to FarmBot to earn *free months* when they upgrade to premium.\n\n"
        "Or join our Partner program to earn *real money* (open PartnerBot)."
    )

    # Use MarkdownV2; no dynamic content here so safe (but keep consistent escaping if needed)
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(
                md_escape_v2(text), parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup(kb)
            )
        except Exception:
            # fallback to reply (in case edit fails)
            await update.callback_query.message.reply_text(md_escape_v2(text), parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup(kb))
    else:
        await update.message.reply_text(md_escape_v2(text), parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup(kb))


partner_handlers["menu"] = menu


# =============== INVITE (farmer -> free month) =================
async def invite(update: Update, context):
    """
    Generate an invite deep-link for farmers that gives the inviter a free month
    when the invited user upgrades to premium. FarmBot must handle the start param.
    """
    # get caller's DB user id
    if not update.callback_query or not update.callback_query.from_user:
        return

    tg_user = update.callback_query.from_user
    tg_id = tg_user.id

    user_id = None
    try:
        user_id = get_user_id(tg_id)
    except Exception:
        logger.exception("get_user_id failed")

    if not user_id:
        await update.callback_query.edit_message_text(
            md_escape_v2("⚠️ You must be registered first. Use /start."),
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    # create a short referral token and try to persist in invitation_codes
    referral_token = f"ref_{user_id}"
    try:
        code = (str(uuid.uuid4())[:8]).upper()
        insert_payload = {
            "farm_id": None,
            "code": f"REF-{code}",
            "role": "farmer_referral",
            "created_by": user_id,
            "meta": {"inviter_app_user_id": user_id},
        }
        res = supabase.table("invitation_codes").insert(insert_payload).execute()
        # supabase-py response: res.data may be present
        if res and getattr(res, "data", None):
            stored_code = res.data[0].get("code")
            referral_token = stored_code or referral_token
    except Exception:
        logger.exception("Failed to insert invitation_codes (non-fatal), falling back to token mapping")

    # Build deep link to FarmBot with start param.
    invite_link = f"https://t.me/{FARM_BOT_USERNAME}?start={referral_token}"
    # Put link in a button to avoid Markdown escaping issues
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Open FarmBot (invite)", url=invite_link)]])

    text = (
        "📢 *Invite a farmer*\n\n"
        "Share the button below with your farmer friends. When they register and later upgrade to premium, "
        "you'll get *1 month free* as a referral reward.\n\n"
        "Note: If they'd rather join the Partner program to earn cash, use *Join Partner Program*."
    )

    await update.callback_query.edit_message_text(md_escape_v2(text), parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)


partner_handlers["invite"] = invite


# ================= JOIN PARTNER (open PartnerBot) =================
async def join(update: Update, context, action=None):
    """
    Prompts the user to open the PartnerBot to onboard as a cash-earning partner.
    Uses PARTNER_BOT_USERNAME from env to craft t.me link.
    """
    partner_link = f"https://t.me/{PARTNER_BOT_USERNAME}?start=become_partner"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Open PartnerBot", url=partner_link)]])

    text = (
        "💼 *Join the Partner Program*\n\n"
        "Open PartnerBot to complete onboarding (KYC / payout details) and create promo links that pay cash commissions.\n\n"
        "After joining, you'll be able to generate promo codes, track commissions, and request payouts."
    )

    try:
        await update.callback_query.edit_message_text(md_escape_v2(text), parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    except Exception:
        # fallback: try to reply instead
        try:
            await update.callback_query.message.reply_text(md_escape_v2(text), parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
        except Exception:
            logger.exception("Failed to send join message")


partner_handlers["join"] = join


# =============== WHO JOINED BY ME? ==================
async def joined(update: Update, context):
    """
    Show users who joined by the caller's invite (inviter -> referred_by).
    Displays name, telegram id, premium status and joined date.
    """
    if not update.callback_query or not update.callback_query.from_user:
        return

    tg_user = update.callback_query.from_user
    tg_id = tg_user.id

    # find caller app_user row
    try:
        user_db_resp = supabase.table("app_users").select("id").eq("telegram_id", tg_id).single().execute()
        user_db = getattr(user_db_resp, "data", None)
    except Exception:
        logger.exception("DB error fetching caller app_user")
        await update.callback_query.edit_message_text(md_escape_v2("⚠️ Couldn’t find your account. Try /start again."), parse_mode=ParseMode.MARKDOWN_V2)
        return

    if not user_db:
        await update.callback_query.edit_message_text(md_escape_v2("⚠️ Couldn’t find your account. Try /start again."), parse_mode=ParseMode.MARKDOWN_V2)
        return

    try:
        # select more fields, ordered by created_at descending
        refs_resp = (
            supabase.table("app_users")
            .select("id,name,telegram_id,premium_status,created_at")
            .eq("referred_by", user_db["id"])
            .order("created_at", desc=True)
            .execute()
        )
        refs = getattr(refs_resp, "data", []) or []
    except Exception:
        logger.exception("DB error fetching referred users")
        refs = []

    if not refs:
        await update.callback_query.edit_message_text(md_escape_v2("🙁 No users have joined with your invite yet."), parse_mode=ParseMode.MARKDOWN_V2)
        return

    # Build output lines
    lines = []
    for r in refs:
        name = md_escape_v2(r.get("name", "-"))
        tg = md_escape_v2(str(r.get("telegram_id", "-")))
        premium = "Yes" if r.get("premium_status") else "No"
        premium = md_escape_v2(premium)
        joined_at = md_escape_v2(str(r.get("created_at", "-")))
        lines.append(f"• {name} (tg:{tg}) — Premium: {premium} — Joined: {joined_at}")

    text = "📋 *Users who joined via your link*\n\n" + "\n".join(lines)
    await update.callback_query.edit_message_text(md_escape_v2(text), parse_mode=ParseMode.MARKDOWN_V2)


partner_handlers["joined"] = joined


# =============== VIEW REFERRALS (debug / legacy) ==================
# kept for backward compatibility / debugging; can be removed later
async def view(update: Update, context):
    if not update.callback_query or not update.callback_query.from_user:
        return
    user = update.callback_query.from_user

    # find app_user row
    try:
        user_db_resp = supabase.table("app_users").select("id").eq("telegram_id", user.id).single().execute()
        user_db = getattr(user_db_resp, "data", None)
    except Exception:
        logger.exception("DB error fetching user_db for referrals")
        await update.callback_query.edit_message_text(
            md_escape_v2("⚠️ Couldn’t find your account. Try /start again."),
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    if not user_db:
        await update.callback_query.edit_message_text(
            md_escape_v2("⚠️ Couldn’t find your account. Try /start again."), parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    try:
        refs_resp = (
            supabase.table("app_users")
            .select("id,name,telegram_id,premium_status")
            .eq("referred_by", user_db["id"])
            .execute()
        )
        refs = getattr(refs_resp, "data", []) or []
    except Exception:
        logger.exception("DB error fetching referrals")
        refs = []

    if not refs:
        await update.callback_query.edit_message_text(md_escape_v2("🙁 You have no referrals yet."), parse_mode=ParseMode.MARKDOWN_V2)
        return

    # Build a safe list string
    lines = []
    for r in refs:
        name = md_escape_v2(r.get("name", "-"))
        tg = md_escape_v2(r.get("telegram_id", "-"))
        premium = md_escape_v2(str(r.get("premium_status", False)))
        lines.append(f"• {name} (tg:{tg}) — Premium: {premium}")

    text = "📈 *Your Referrals*:\n\n" + "\n".join(lines)
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2)


partner_handlers["view"] = view

# End of file



'''
# partners.py
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from farmcore import supabase, get_user_id
import uuid

partner_handlers = {}

# ================= MENU ==================
async def menu(update: Update, context):
    kb = [
        [InlineKeyboardButton("🤝 Generate Promo Code", callback_data='partner:generate')],
        [InlineKeyboardButton("📈 View Referrals", callback_data='partner:view')],
        [InlineKeyboardButton("💳 Payments", callback_data='partner:payments')],
        [InlineKeyboardButton("🏠 Back", callback_data='partner:back')],
        [InlineKeyboardButton("⏭️ /skip", callback_data='skip')]
    ]
    await (update.callback_query.edit_message_text if update.callback_query else update.message.reply_text)(
        "🤝 *Partners & Marketing*:\nInvite friends, earn commissions, and track your referrals!",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb)
    )

partner_handlers['menu'] = menu


# =============== GENERATE PROMO CODE =================
async def generate(update: Update, context):
    tg_id = update.callback_query.from_user.id
    user_id = get_user_id(tg_id)
    if not user_id:
        await update.callback_query.edit_message_text("⚠️ You must be registered first.")
        return

    # check if user already has an active promo code
    existing = supabase.table("promo_codes").select("*").eq("generated_by", user_id).limit(1).execute().data
    if existing:
        code = existing[0]["code"]
        await update.callback_query.edit_message_text(
            f"✅ You already have a promo code: *{code}*\nShare it to earn commissions!",
            parse_mode="Markdown"
        )
        return

    # otherwise, create new
    code = str(uuid.uuid4())[:8].upper()
    supabase.table('promo_codes').insert({
        'code': code,
        'generated_by': user_id,
        'discount_percent': 10,
        'commission_structure': {"level1": 3, "level2": 1},  # flexible JSONB
        'uses': 0,
        'max_uses': 100
    }).execute()

    await update.callback_query.edit_message_text(
        f"🎉 Your new Promo Code: *{code}*\n\n"
        "📢 Share it with others. They’ll get *10% off*, "
        "and you’ll earn *commissions* on their purchases!",
        parse_mode='Markdown'
    )

partner_handlers['generate'] = generate


# ================== VIEW REFERRALS ==================
async def view(update: Update, context):
    user = update.callback_query.from_user
    user_db = supabase.table('app_users').select('id').eq('telegram_id', user.id).single().execute().data
    if not user_db:
        await update.callback_query.edit_message_text("⚠️ Couldn’t find your account. Try /start again.")
        return

    refs = supabase.table('app_users').select(
        'id,name,telegram_id,premium_status'
    ).eq('referred_by', user_db['id']).execute().data or []

    if not refs:
        await update.callback_query.edit_message_text("🙁 You have no referrals yet.")
        return

    text = "📈 *Your Referrals*:\n\n" + "\n".join(
        f"• {r['name']} (tg:{r['telegram_id']}) — Premium: {r.get('premium_status', False)}"
        for r in refs
    )

    await update.callback_query.edit_message_text(text, parse_mode="Markdown")

partner_handlers['view'] = view


# ================ PAYMENTS PLACEHOLDER =================
async def payments(update: Update, context):
    await update.callback_query.edit_message_text(
        "💳 *Payments & Commissions*\n\n"
        "Here you will later track commission payouts and balances.",
        parse_mode="Markdown"
    )

partner_handlers['payments'] = payments
'''