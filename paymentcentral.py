# ----- paymentcentral.py -----
import os
import logging
import math
from uuid import uuid4
from datetime import datetime, timedelta
import asyncio
from typing import Optional

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Update,
)
from telegram.ext import ContextTypes

from farmcore import supabase, async_get_user_by_telegram

LOG = logging.getLogger(__name__)

# CONFIG - set in .env
TELEGRAM_PROVIDER_TOKEN = os.getenv("TELEGRAM_PROVIDER_TOKEN", "YOUR_PROVIDER_TOKEN")
# How many Stars equal 1 USD in your price mapping (adjust to your desired conversion)
STARS_PER_USD = int(os.getenv("STARS_PER_USD", "100"))
EXTERNAL_CHECKOUT_BASE = os.getenv("EXTERNAL_CHECKOUT_BASE", "https://agrivet.com/pay/checkout")

PLANS = {
    "1m": {"months": 1, "price_usd": 5.00, "label": "1 month"},
    "3m": {"months": 3, "price_usd": 13.00, "label": "3 months (save!)"},
    "6m": {"months": 6, "price_usd": 24.00, "label": "6 months (best value)"},
}


def _back_kb(payload="profile:menu") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=payload)]])


# New helper: nicer plans keyboard
def _plans_keyboard():
    """
    Return InlineKeyboardMarkup with each plan on its own row. Each plan row is followed by an action row
    with pay buttons (Stars / Online).
    """
    kb = []
    for pid, p in PLANS.items():
        stars_needed = math.ceil(p["price_usd"] * STARS_PER_USD)
        if stars_needed < 1:
            stars_needed = 1

        # Plan label row (one-line descriptive text as button text for full-width look)
        plan_label = f"{p['label']} — ${p['price_usd']:.2f}"
        kb.append([InlineKeyboardButton(plan_label, callback_data=f"payment:plan:{pid}")])

        # Action row for this plan
        kb.append([
            InlineKeyboardButton(f"⭐ Pay {stars_needed} Stars", callback_data=f"payment:method:{pid}:stars"),
            InlineKeyboardButton("💳 Pay online", url=f"{EXTERNAL_CHECKOUT_BASE}?plan={pid}")
        ])

    kb.append([InlineKeyboardButton("⬅️ Back", callback_data="profile:menu")])
    return InlineKeyboardMarkup(kb)


# ------------------------
# Helpers: DB operations
# ------------------------
async def save_pending_payment(user_id: str, telegram_id: int, stars_amount: int, months: int, txn_id: str):
    """
    Insert a pending payment record in payments table.
    """
    try:
        payload = {
            "user_id": user_id,
            "telegram_stars_amount": stars_amount,
            "premium_duration": months,
            "transaction_id": txn_id,
            "status": "pending",
            "meta": {"via": "telegram_stars"},
        }

        def _insert():
            return supabase.table("payments").insert(payload).execute()

        res = await asyncio.to_thread(_insert)
        LOG.info("Saved pending payment txn=%s res_rows=%s", txn_id, len(res.data) if res and hasattr(res, "data") and res.data else 0)
        return res
    except Exception:
        LOG.exception("Failed to save pending payment")
        return None


async def mark_payment_paid(transaction_id: str, telegram_charge_id: Optional[str], stars_amount: int, months: int):
    """
    Mark an existing payment row as paid and update user premium fields.
    """
    try:
        # update payments row
        def _update_payment():
            return supabase.table("payments").update({
                "status": "paid",
                "meta": {"telegram_payment_charge_id": telegram_charge_id}
            }).eq("transaction_id", transaction_id).execute()
        await asyncio.to_thread(_update_payment)

        # fetch the payment row to get user_id
        def _get_payment():
            return supabase.table("payments").select("user_id").eq("transaction_id", transaction_id).single().execute()
        pay_res = await asyncio.to_thread(_get_payment)
        if not pay_res or not pay_res.data:
            LOG.error("Payment row not found for txn=%s", transaction_id)
            return False

        user_id = pay_res.data.get("user_id")
        if not user_id:
            LOG.error("Payment row missing user_id txn=%s", transaction_id)
            return False

        # compute expiry
        expiry = datetime.utcnow() + timedelta(days=30 * months)
        # merge meta.premium_expiry
        # fetch current user meta
        def _get_user():
            return supabase.table("app_users").select("meta").eq("id", user_id).single().execute()
        user_res = await asyncio.to_thread(_get_user)

        new_meta = {}
        if user_res and user_res.data:
            existing_meta = user_res.data.get("meta") or {}
            new_meta = dict(existing_meta)
        new_meta["premium_expiry"] = expiry.isoformat()

        # update user: set premium_status True and update meta with expiry
        def _update_user():
            return supabase.table("app_users").update({
                "premium_status": True,
                "meta": new_meta
            }).eq("id", user_id).execute()
        await asyncio.to_thread(_update_user)
        LOG.info("Marked payment paid and updated user %s expiry=%s", user_id, expiry.isoformat())
        return True
    except Exception:
        LOG.exception("Failed to mark payment paid")
        return False


# ------------------------
# Menu shown inside profile (entrypoint)
# ------------------------
async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return

    telegram_id = user.id
    user_row = await async_get_user_by_telegram(telegram_id)
    if not user_row:
        await update.effective_message.reply_text("❌ You must register first. Use /start.")
        return

    premium = user_row.get("premium_status", False)
    expiry = user_row.get("meta", {}).get("premium_expiry")
    expiry_text = f"\nPremium until: {expiry}" if expiry else ""

    text = (
        f"👤 *Profile*\n\n"
        f"*Name:* {user_row.get('name') or '—'}\n"
        f"*Farm:* {user_row.get('farm_name') or '—'}\n"
        f"*Status:* {'⭐ Premium' if premium else 'Free'}{expiry_text}\n\n"
        "Choose a plan below — tap a plan to see payment options."
    )

    await update.effective_message.reply_text(text, parse_mode="Markdown", reply_markup=_plans_keyboard())


# ------------------------
# Router for callbacks
# ------------------------
async def router(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str = ""):
    """
    callback_data patterns:
      payment:start
      payment:plan:<plan_id>
      payment:method:<plan_id>:stars
      payment:confirm_stars:<plan_id>:<stars_needed>
    """
    query = update.callback_query
    if not query:
        return
    await query.answer()

    telegram_id = update.effective_user.id

    # Show plans
    if action == "start" or action == "choose_plan" or action == "":
        await query.edit_message_text("Choose a plan:", reply_markup=_plans_keyboard())
        return

    # plan selected
    if action.startswith("plan:"):
        _, plan_id = action.split(":", 1)
        plan = PLANS.get(plan_id)
        if not plan:
            await query.edit_message_text("⚠️ Unknown plan. Please try again.", reply_markup=_back_kb("payment:start"))
            return

        kb = [
            [InlineKeyboardButton("⭐ Pay with Telegram Stars", callback_data=f"payment:method:{plan_id}:stars")],
            [InlineKeyboardButton("💳 Pay with Card (Online)", url=f"{EXTERNAL_CHECKOUT_BASE}?user_id={telegram_id}&plan={plan_id}")],
            [InlineKeyboardButton("⬅️ Back", callback_data="payment:start")],
        ]
        text = f"Selected: *{plan['label']}*\nPrice: *${plan['price_usd']:.2f}*\n\nChoose payment method:"
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return

    # payment method chosen (stars)
    if action.startswith("method:"):
        parts = action.split(":")
        if len(parts) >= 3:
            plan_id = parts[1]
            method = parts[2]
        else:
            await query.edit_message_text("⚠️ Invalid selection.", reply_markup=_back_kb("payment:start"))
            return

        plan = PLANS.get(plan_id)
        if not plan:
            await query.edit_message_text("⚠️ Unknown plan.", reply_markup=_back_kb("payment:start"))
            return

        if method == "stars":
            stars_needed = math.ceil(plan["price_usd"] * STARS_PER_USD)
            if stars_needed < 1:
                stars_needed = 1

            text = (
                f"You chose *{plan['label']}* — ${plan['price_usd']:.2f}.\n\n"
                f"To pay with Telegram Stars you need *{stars_needed} Stars*.\n\n"
                "Press Confirm to open the Telegram payment."
            )

            kb = [
                [InlineKeyboardButton(f"✅ Confirm pay {stars_needed} ⭐", callback_data=f"payment:confirm_stars:{plan_id}:{stars_needed}")],
                [InlineKeyboardButton("⬅️ Back", callback_data=f"payment:plan:{plan_id}")],
            ]
            await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
            return

    # confirm stars -> send invoice
    if action.startswith("confirm_stars:"):
        # format: confirm_stars:<plan_id>:<stars_needed>
        try:
            _, plan_id, stars_str = action.split(":", 2)
            stars_needed = int(stars_str)
        except Exception:
            await query.edit_message_text("⚠️ Invalid payment request.", reply_markup=_back_kb("payment:start"))
            return

        plan = PLANS.get(plan_id)
        if not plan:
            await query.edit_message_text("⚠️ Unknown plan.", reply_markup=_back_kb("payment:start"))
            return

        payment_uuid = str(uuid4())
        invoice_payload = f"agrivet:premium:{telegram_id}:{plan_id}:{payment_uuid}"

        # resolve app user id
        user_row = await async_get_user_by_telegram(telegram_id)
        if not user_row:
            await query.edit_message_text("❌ User not found. Please /start to register.", reply_markup=_back_kb("profile:menu"))
            return
        user_id = user_row.get("id")

        # save pending payment
        await save_pending_payment(user_id=user_id, telegram_id=telegram_id, stars_amount=stars_needed, months=plan["months"], txn_id=payment_uuid)

        # build invoice
        title = f"AGRIVET Premium — {plan['label']}"
        description = f"{plan['months']} month(s) premium subscription"
        prices = [LabeledPrice(label=title, amount=stars_needed)]

        try:
            await context.bot.send_invoice(
                chat_id=telegram_id,
                title=title,
                description=description,
                payload=invoice_payload,
                provider_token=TELEGRAM_PROVIDER_TOKEN,
                currency="XTR",  # Stars
                prices=prices,
                start_parameter=f"agrivet_{payment_uuid[:8]}",
            )
            try:
                await query.edit_message_text("Opening payment... please complete payment within Telegram.")
            except Exception:
                pass
        except Exception:
            LOG.exception("Failed to send invoice")
            await query.edit_message_text("❌ Failed to open payment. Try again later.", reply_markup=_back_kb("payment:start"))
        return

    # fallback
    await query.edit_message_text("⚠️ Action not recognized.", reply_markup=_back_kb("profile:menu"))


# ------------------------
# Handle successful payments from Telegram
# ------------------------
async def handle_successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Register this handler in main: MessageHandler(filters.SUCCESSFUL_PAYMENT, paymentcentral.handle_successful_payment)
    """
    msg = update.message
    if not msg or not getattr(msg, "successful_payment", None):
        return

    payment = msg.successful_payment

    # prefer the invoice_payload produced earlier
    payload = getattr(payment, "invoice_payload", None) or (payment.get("invoice_payload") if isinstance(payment, dict) else None)
    if not payload:
        LOG.warning("Payment without payload: %s", payment)
        return

    try:
        parts = payload.split(":")
        if len(parts) < 5 or parts[0] != "agrivet" or parts[1] != "premium":
            LOG.warning("Unexpected invoice payload: %s", payload)
            return
        telegram_id = int(parts[2])
        plan_id = parts[3]
        payment_uuid = parts[4]
    except Exception:
        LOG.exception("Failed to parse payload")
        return

    payer = update.effective_user
    if not payer or payer.id != telegram_id:
        LOG.warning("Telegram id mismatch in payment: payload %s != update user %s", telegram_id, payer.id if payer else None)
        # don't proceed if mismatch
        return

    # confirm numeric fields
    stars_paid = getattr(payment, "total_amount", None) or (payment.get("total_amount") if isinstance(payment, dict) else None)
    # optional: verify currency is XTR
    currency = getattr(payment, "currency", None) or (payment.get("currency") if isinstance(payment, dict) else None)

    if currency != "XTR":
        LOG.warning("Unexpected currency in payment: %s", currency)

    # Determine plan months
    plan = PLANS.get(plan_id)
    months = plan["months"] if plan else 1

    # Mark payment as paid and update user premium
    try:
        telegram_charge_id = getattr(payment, "telegram_payment_charge_id", None) or (payment.get("telegram_payment_charge_id") if isinstance(payment, dict) else None)
        ok = await mark_payment_paid(transaction_id=payment_uuid, telegram_charge_id=telegram_charge_id, stars_amount=stars_paid or 0, months=months)
        if not ok:
            await msg.reply_text("⚠️ Payment received but failed to update account. Contact support.")
            return
    except Exception:
        LOG.exception("Error marking payment paid")
        await msg.reply_text("⚠️ Payment received but failed to update account. Contact support.")
        return

    # successful
    expiry = datetime.utcnow() + timedelta(days=30 * months)
    try:
        await msg.reply_text(f"✅ Payment confirmed. You are premium until {expiry.date().isoformat()}.")
    except Exception:
        pass

    LOG.info("Processed successful payment for telegram_id=%s plan=%s txn=%s", telegram_id, plan_id, payment_uuid)













'''# paymentcentral.py
import os
import logging
import math
from uuid import uuid4
from datetime import datetime, timedelta
import asyncio
from typing import Optional

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Update,
)
from telegram.ext import ContextTypes

from farmcore import supabase, async_get_user_by_telegram

LOG = logging.getLogger(__name__)

# CONFIG - set in .env
TELEGRAM_PROVIDER_TOKEN = os.getenv("TELEGRAM_PROVIDER_TOKEN", "YOUR_PROVIDER_TOKEN")
# How many Stars equal 1 USD in your price mapping (adjust to your desired conversion)
STARS_PER_USD = int(os.getenv("STARS_PER_USD", "100"))
EXTERNAL_CHECKOUT_BASE = os.getenv("EXTERNAL_CHECKOUT_BASE", "https://agrivet.com/pay/checkout")

PLANS = {
    "1m": {"months": 1, "price_usd": 5.00, "label": "1 month"},
    "3m": {"months": 3, "price_usd": 13.00, "label": "3 months (save!)"},
    "6m": {"months": 6, "price_usd": 24.00, "label": "6 months (best value)"},
}

def _back_kb(payload="profile:menu") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=payload)]])

# ------------------------
# Helpers: DB operations
# ------------------------
async def save_pending_payment(user_id: str, telegram_id: int, stars_amount: int, months: int, txn_id: str):
    """
    Insert a pending payment record in payments table.
    """
    try:
        payload = {
            "user_id": user_id,
            "telegram_stars_amount": stars_amount,
            "premium_duration": months,
            "transaction_id": txn_id,
            "status": "pending",
            "meta": {"via": "telegram_stars"},
        }
        def _insert():
            return supabase.table("payments").insert(payload).execute()
        res = await asyncio.to_thread(_insert)
        LOG.info("Saved pending payment txn=%s res_rows=%s", txn_id, len(res.data) if res and hasattr(res, "data") and res.data else 0)
        return res
    except Exception:
        LOG.exception("Failed to save pending payment")
        return None

async def mark_payment_paid(transaction_id: str, telegram_charge_id: Optional[str], stars_amount: int, months: int):
    """
    Mark an existing payment row as paid and update user premium fields.
    """
    try:
        # update payments row
        def _update_payment():
            return supabase.table("payments").update({
                "status": "paid",
                "meta": {"telegram_payment_charge_id": telegram_charge_id}
            }).eq("transaction_id", transaction_id).execute()
        await asyncio.to_thread(_update_payment)

        # fetch the payment row to get user_id
        def _get_payment():
            return supabase.table("payments").select("user_id").eq("transaction_id", transaction_id).single().execute()
        pay_res = await asyncio.to_thread(_get_payment)
        if not pay_res or not pay_res.data:
            LOG.error("Payment row not found for txn=%s", transaction_id)
            return False

        user_id = pay_res.data.get("user_id")
        if not user_id:
            LOG.error("Payment row missing user_id txn=%s", transaction_id)
            return False

        # compute expiry
        expiry = datetime.utcnow() + timedelta(days=30 * months)
        # merge meta.premium_expiry
        # fetch current user meta
        def _get_user():
            return supabase.table("app_users").select("meta").eq("id", user_id).single().execute()
        user_res = await asyncio.to_thread(_get_user)

        new_meta = {}
        if user_res and user_res.data:
            existing_meta = user_res.data.get("meta") or {}
            new_meta = dict(existing_meta)
        new_meta["premium_expiry"] = expiry.isoformat()

        # update user: set premium_status True and update meta with expiry
        def _update_user():
            return supabase.table("app_users").update({
                "premium_status": True,
                "meta": new_meta
            }).eq("id", user_id).execute()
        await asyncio.to_thread(_update_user)
        LOG.info("Marked payment paid and updated user %s expiry=%s", user_id, expiry.isoformat())
        return True
    except Exception:
        LOG.exception("Failed to mark payment paid")
        return False

# ------------------------
# Menu shown inside profile (entrypoint)
# ------------------------
async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return

    telegram_id = user.id
    user_row = await async_get_user_by_telegram(telegram_id)
    if not user_row:
        await update.effective_message.reply_text("❌ You must register first. Use /start.")
        return

    premium = user_row.get("premium_status", False)
    expiry = user_row.get("meta", {}).get("premium_expiry")
    expiry_text = f"\nPremium until: {expiry}" if expiry else ""

    text = (
        f"👤 *Profile*\n\n"
        f"Name: {user_row.get('name') or '—'}\n"
        f"Farm: {user_row.get('farm_name') or '—'}\n"
        f"Status: {'⭐ Premium' if premium else 'Free'}{expiry_text}\n\n"
        "You can buy subscription below:"
    )

    buttons = [
        [InlineKeyboardButton("💳 Pay subscription", callback_data="payment:start")],
    ]
    await update.effective_message.reply_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(buttons))

# ------------------------
# Router for callbacks
# ------------------------
async def router(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str = ""):
    """
    callback_data patterns:
      payment:start
      payment:plan:<plan_id>
      payment:method:<plan_id>:stars
      payment:confirm_stars:<plan_id>:<stars_needed>
    """
    query = update.callback_query
    if not query:
        return
    await query.answer()

    telegram_id = update.effective_user.id

    # Show plans
    if action == "start" or action == "choose_plan" or action == "":
        kb = []
        for pid, p in PLANS.items():
            kb.append([InlineKeyboardButton(f"{p['label']} — ${p['price_usd']:.2f}", callback_data=f"payment:plan:{pid}")])
        kb.append([InlineKeyboardButton("⬅️ Back", callback_data="profile:menu")])
        await query.edit_message_text("Choose a plan:", reply_markup=InlineKeyboardMarkup(kb))
        return

    # plan selected
    if action.startswith("plan:"):
        _, plan_id = action.split(":", 1)
        plan = PLANS.get(plan_id)
        if not plan:
            await query.edit_message_text("⚠️ Unknown plan. Please try again.", reply_markup=_back_kb("payment:start"))
            return

        kb = [
            [InlineKeyboardButton("⭐ Pay with Telegram Stars", callback_data=f"payment:method:{plan_id}:stars")],
            [InlineKeyboardButton("💳 Pay with Card (Online)", url=f"{EXTERNAL_CHECKOUT_BASE}?user_id={telegram_id}&plan={plan_id}")],
            [InlineKeyboardButton("⬅️ Back", callback_data="payment:start")],
        ]
        text = f"Selected: *{plan['label']}*\nPrice: *${plan['price_usd']:.2f}*\n\nChoose payment method:"
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return

    # payment method chosen (stars)
    if action.startswith("method:"):
        parts = action.split(":")
        if len(parts) >= 3:
            plan_id = parts[1]
            method = parts[2]
        else:
            await query.edit_message_text("⚠️ Invalid selection.", reply_markup=_back_kb("payment:start"))
            return

        plan = PLANS.get(plan_id)
        if not plan:
            await query.edit_message_text("⚠️ Unknown plan.", reply_markup=_back_kb("payment:start"))
            return

        if method == "stars":
            stars_needed = math.ceil(plan["price_usd"] * STARS_PER_USD)
            if stars_needed < 1:
                stars_needed = 1

            text = (
                f"You chose *{plan['label']}* — ${plan['price_usd']:.2f}.\n\n"
                f"To pay with Telegram Stars you need *{stars_needed} Stars*.\n\n"
                "Press Confirm to open the Telegram payment."
            )

            kb = [
                [InlineKeyboardButton(f"✅ Confirm pay {stars_needed} ⭐", callback_data=f"payment:confirm_stars:{plan_id}:{stars_needed}")],
                [InlineKeyboardButton("⬅️ Back", callback_data=f"payment:plan:{plan_id}")],
            ]
            await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
            return

    # confirm stars -> send invoice
    if action.startswith("confirm_stars:"):
        # format: confirm_stars:<plan_id>:<stars_needed>
        try:
            _, plan_id, stars_str = action.split(":", 2)
            stars_needed = int(stars_str)
        except Exception:
            await query.edit_message_text("⚠️ Invalid payment request.", reply_markup=_back_kb("payment:start"))
            return

        plan = PLANS.get(plan_id)
        if not plan:
            await query.edit_message_text("⚠️ Unknown plan.", reply_markup=_back_kb("payment:start"))
            return

        payment_uuid = str(uuid4())
        invoice_payload = f"agrivet:premium:{telegram_id}:{plan_id}:{payment_uuid}"

        # resolve app user id
        user_row = await async_get_user_by_telegram(telegram_id)
        if not user_row:
            await query.edit_message_text("❌ User not found. Please /start to register.", reply_markup=_back_kb("profile:menu"))
            return
        user_id = user_row.get("id")

        # save pending payment
        await save_pending_payment(user_id=user_id, telegram_id=telegram_id, stars_amount=stars_needed, months=plan["months"], txn_id=payment_uuid)

        # build invoice
        title = f"AGRIVET Premium — {plan['label']}"
        description = f"{plan['months']} month(s) premium subscription"
        prices = [LabeledPrice(label=title, amount=stars_needed)]

        try:
            await context.bot.send_invoice(
                chat_id=telegram_id,
                title=title,
                description=description,
                payload=invoice_payload,
                provider_token=TELEGRAM_PROVIDER_TOKEN,
                currency="XTR",  # Stars
                prices=prices,
                start_parameter=f"agrivet_{payment_uuid[:8]}",
            )
            try:
                await query.edit_message_text("Opening payment... please complete payment within Telegram.")
            except Exception:
                pass
        except Exception:
            LOG.exception("Failed to send invoice")
            await query.edit_message_text("❌ Failed to open payment. Try again later.", reply_markup=_back_kb("payment:start"))
        return

    # fallback
    await query.edit_message_text("⚠️ Action not recognized.", reply_markup=_back_kb("profile:menu"))

# ------------------------
# Handle successful payments from Telegram
# ------------------------
async def handle_successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Register this handler in main: MessageHandler(filters.SUCCESSFUL_PAYMENT, paymentcentral.handle_successful_payment)
    """
    msg = update.message
    if not msg or not getattr(msg, "successful_payment", None):
        return

    payment = msg.successful_payment

    # prefer the invoice_payload produced earlier
    payload = getattr(payment, "invoice_payload", None) or (payment.get("invoice_payload") if isinstance(payment, dict) else None)
    if not payload:
        LOG.warning("Payment without payload: %s", payment)
        return

    try:
        parts = payload.split(":")
        if len(parts) < 5 or parts[0] != "agrivet" or parts[1] != "premium":
            LOG.warning("Unexpected invoice payload: %s", payload)
            return
        telegram_id = int(parts[2])
        plan_id = parts[3]
        payment_uuid = parts[4]
    except Exception:
        LOG.exception("Failed to parse payload")
        return

    payer = update.effective_user
    if not payer or payer.id != telegram_id:
        LOG.warning("Telegram id mismatch in payment: payload %s != update user %s", telegram_id, payer.id if payer else None)
        # don't proceed if mismatch
        return

    # confirm numeric fields
    stars_paid = getattr(payment, "total_amount", None) or (payment.get("total_amount") if isinstance(payment, dict) else None)
    # optional: verify currency is XTR
    currency = getattr(payment, "currency", None) or (payment.get("currency") if isinstance(payment, dict) else None)

    if currency != "XTR":
        LOG.warning("Unexpected currency in payment: %s", currency)

    # Determine plan months
    plan = PLANS.get(plan_id)
    months = plan["months"] if plan else 1

    # Mark payment as paid and update user premium
    try:
        telegram_charge_id = getattr(payment, "telegram_payment_charge_id", None) or (payment.get("telegram_payment_charge_id") if isinstance(payment, dict) else None)
        ok = await mark_payment_paid(transaction_id=payment_uuid, telegram_charge_id=telegram_charge_id, stars_amount=stars_paid or 0, months=months)
        if not ok:
            await msg.reply_text("⚠️ Payment received but failed to update account. Contact support.")
            return
    except Exception:
        LOG.exception("Error marking payment paid")
        await msg.reply_text("⚠️ Payment received but failed to update account. Contact support.")
        return

    # successful
    expiry = datetime.utcnow() + timedelta(days=30 * months)
    try:
        await msg.reply_text(f"✅ Payment confirmed. You are premium until {expiry.date().isoformat()}.")
    except Exception:
        pass

    LOG.info("Processed successful payment for telegram_id=%s plan=%s txn=%s", telegram_id, plan_id, payment_uuid)
'''