# ----- profile.py -----
import asyncio
import logging
from typing import Optional

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes

from farmcore import supabase, async_get_user_by_telegram, async_get_user_with_farm_by_telegram

logger = logging.getLogger(__name__)

profile_handlers = {}


async def _reply_or_edit(update: Update, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None, parse_mode: Optional[str] = None):
    """
    Utility to either edit the callback message or send a new message depending on how the handler was invoked.
    """
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
        except Exception as e:
            logger.warning("Failed to edit callback message: %s — sending new message", e)
            await update.effective_message.reply_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, parse_mode=parse_mode, reply_markup=reply_markup)


async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Improved profile menu: shows a long prominent pay button and cleaner layout."""
    user = update.effective_user
    if not user:
        await _reply_or_edit(update, "⚠️ Unable to read user info.")
        return

    try:
        combined = await async_get_user_with_farm_by_telegram(user.id)
        if not combined or not combined.get("user"):
            await _reply_or_edit(update, "⚠️ No profile found. Please /start to register.")
            return

        user_data = combined["user"]
        farm = combined.get("farm")
        farm_name = farm.get("name") if farm else "Not set"

        # Full-width pay button is created by placing it on its own row
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Get AGRIVET Premium — Quick pay", callback_data="payment:start")],
            [InlineKeyboardButton("✏️ Edit Name", callback_data="profile:edit_name"),
             InlineKeyboardButton("🏡 Edit Farm", callback_data="profile:edit_farm")],
            [InlineKeyboardButton("🔙 Back", callback_data="skip")]
        ])

        premium_flag = "✅ Premium" if user_data.get("premium_status") else "❌ Free"
        expiry_iso = (user_data.get("meta") or {}).get("premium_expiry")
        expiry_text = f"\n\nPremium until: `{expiry_iso}`" if expiry_iso else ""

        text = (
            f"👤 *Your Profile*\n\n"
            f"*Name:* {user_data.get('name', 'Not set')}\n"
            f"*Farm:* {farm_name}\n"
            f"*Status:* {premium_flag}{expiry_text}\n\n"
            "_Tap the button above to subscribe or manage plans._"
        )

        await _reply_or_edit(update, text, reply_markup=kb, parse_mode="Markdown")
    except Exception:
        logger.exception("Error in profile menu")
        if update.callback_query:
            try:
                await update.callback_query.edit_message_text("❌ Error loading profile.")
            except Exception:
                await update.effective_message.reply_text("❌ Error loading profile.")
        else:
            await update.message.reply_text("❌ Error loading profile.")


profile_handlers["menu"] = menu


async def edit_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["flow"] = "profile_edit_name"
    if update.callback_query:
        await update.callback_query.edit_message_text("✏️ Send me your new *name*:", parse_mode="Markdown")
    else:
        await update.message.reply_text("✏️ Send me your new *name*:", parse_mode="Markdown")


profile_handlers["edit_name"] = edit_name


async def edit_farm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["flow"] = "profile_edit_farm"
    if update.callback_query:
        await update.callback_query.edit_message_text("🏡 Send me your new *farm name*:", parse_mode="Markdown")
    else:
        await update.message.reply_text("🏡 Send me your new *farm name*:", parse_mode="Markdown")


profile_handlers["edit_farm"] = edit_farm


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handles flow states:
      - profile_edit_name: update app_users.name by telegram_id
      - profile_edit_farm: update farms.name for the user's farm (owner_id -> app_user.id)
    """
    flow = context.user_data.get("flow")
    user = update.effective_user
    if not user:
        return

    message = update.effective_message
    if not message or not message.text:
        return

    text = message.text.strip()

    # If user presses a button like "Animals", cancel the flow safely
    if text.lower() in ["animal", "animals", "menu", "back"]:
        context.user_data.pop("flow", None)
        await message.reply_text("ℹ️ Action cancelled. Please choose again from the menu.")
        return

    # Update name
    if flow == "profile_edit_name":
        new_name = text
        if not new_name:
            await message.reply_text("⚠️ Name cannot be empty. Please send a valid name.")
            return
        try:
            def _update_user_name():
                return supabase.table("app_users").update({"name": new_name}).eq("telegram_id", user.id).execute()

            out = await asyncio.to_thread(_update_user_name)
            if not out.data:
                await message.reply_text("❌ Failed to update name.")
                return

            context.user_data.pop("flow", None)
            await message.reply_text(f"✅ Name updated to *{new_name}*.", parse_mode="Markdown")
        except Exception:
            logger.exception("Error updating name")
            await message.reply_text("❌ Failed to update name.")
        return

    # Update farm name
    if flow == "profile_edit_farm":
        new_farm = text
        if not new_farm:
            await message.reply_text("⚠️ Farm name cannot be empty. Please send a valid farm name.")
            return
        try:
            user_row = await async_get_user_by_telegram(user.id)
            if not user_row:
                await message.reply_text("❌ User not found.")
                return
            user_id = user_row.get("id")
            if not user_id:
                await message.reply_text("❌ User id not found.")
                return

            def _update_farm():
                return supabase.table("farms").update({"name": new_farm}).eq("owner_id", user_id).execute()

            out = await asyncio.to_thread(_update_farm)
            if not out.data:
                await message.reply_text("❌ Failed to update farm name.")
                return

            context.user_data.pop("flow", None)
            await message.reply_text(f"✅ Farm name updated to *{new_farm}*.", parse_mode="Markdown")
        except Exception:
            logger.exception("Error updating farm name")
            await message.reply_text("❌ Failed to update farm name.")
        return


profile_handlers["handle_text"] = handle_text



















'''# profile.py
import asyncio
import logging
from typing import Optional

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes

from farmcore import supabase, async_get_user_by_telegram, async_get_user_with_farm_by_telegram

logger = logging.getLogger(__name__)

profile_handlers = {}

async def _reply_or_edit(update: Update, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None, parse_mode: Optional[str] = None):
    """
    Utility to either edit the callback message or send a new message depending on how the handler was invoked.
    """
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
        except Exception as e:
            logger.warning("Failed to edit callback message: %s — sending new message", e)
            await update.effective_message.reply_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, parse_mode=parse_mode, reply_markup=reply_markup)

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        await _reply_or_edit(update, "⚠️ Unable to read user info.")
        return

    try:
        combined = await async_get_user_with_farm_by_telegram(user.id)
        if not combined or not combined.get("user"):
            await _reply_or_edit(update, "⚠️ No profile found. Please /start to register.")
            return

        user_data = combined["user"]
        farm = combined.get("farm")
        farm_name = farm.get("name") if farm else "Not set"

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Edit Name", callback_data="profile:edit_name")],
            [InlineKeyboardButton("🏡 Edit Farm Name", callback_data="profile:edit_farm")],
            [InlineKeyboardButton("💳 Pay subscription", callback_data="payment:start")],
            [InlineKeyboardButton("🔙 Back", callback_data="skip")]
        ])

        text = (
            f"👤 **Your Profile**\n\n"
            f"Name: {user_data.get('name', 'Not set')}\n"
            f"Farm: {farm_name}\n"
            f"Premium: {'✅' if user_data.get('premium_status') else '❌'}"
        )

        await _reply_or_edit(update, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.exception("Error in profile menu")
        if update.callback_query:
            try:
                await update.callback_query.edit_message_text("❌ Error loading profile.")
            except Exception:
                await update.effective_message.reply_text("❌ Error loading profile.")
        else:
            await update.message.reply_text("❌ Error loading profile.")

profile_handlers["menu"] = menu

async def edit_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["flow"] = "profile_edit_name"
    if update.callback_query:
        await update.callback_query.edit_message_text("✏️ Send me your new *name*:", parse_mode="Markdown")
    else:
        await update.message.reply_text("✏️ Send me your new *name*:", parse_mode="Markdown")

profile_handlers["edit_name"] = edit_name

async def edit_farm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["flow"] = "profile_edit_farm"
    if update.callback_query:
        await update.callback_query.edit_message_text("🏡 Send me your new *farm name*:", parse_mode="Markdown")
    else:
        await update.message.reply_text("🏡 Send me your new *farm name*:", parse_mode="Markdown")

profile_handlers["edit_farm"] = edit_farm

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handles flow states:
      - profile_edit_name: update app_users.name by telegram_id
      - profile_edit_farm: update farms.name for the user's farm (owner_id -> app_user.id)
    """
    flow = context.user_data.get("flow")
    user = update.effective_user
    if not user:
        return

    message = update.effective_message
    if not message or not message.text:
        return

    text = message.text.strip()

    # If user presses a button like "Animals", cancel the flow safely
    if text.lower() in ["animal", "animals", "menu", "back"]:
        context.user_data.pop("flow", None)
        await message.reply_text("ℹ️ Action cancelled. Please choose again from the menu.")
        return

    # Update name
    if flow == "profile_edit_name":
        new_name = text
        if not new_name:
            await message.reply_text("⚠️ Name cannot be empty. Please send a valid name.")
            return
        try:
            def _update_user_name():
                return supabase.table("app_users").update({"name": new_name}).eq("telegram_id", user.id).execute()

            out = await asyncio.to_thread(_update_user_name)
            if not out.data:
                await message.reply_text("❌ Failed to update name.")
                return

            context.user_data.pop("flow", None)
            await message.reply_text(f"✅ Name updated to *{new_name}*.", parse_mode="Markdown")
        except Exception:
            logger.exception("Error updating name")
            await message.reply_text("❌ Failed to update name.")
        return

    # Update farm name
    if flow == "profile_edit_farm":
        new_farm = text
        if not new_farm:
            await message.reply_text("⚠️ Farm name cannot be empty. Please send a valid farm name.")
            return
        try:
            user_row = await async_get_user_by_telegram(user.id)
            if not user_row:
                await message.reply_text("❌ User not found.")
                return
            user_id = user_row.get("id")
            if not user_id:
                await message.reply_text("❌ User id not found.")
                return

            def _update_farm():
                return supabase.table("farms").update({"name": new_farm}).eq("owner_id", user_id).execute()

            out = await asyncio.to_thread(_update_farm)
            if not out.data:
                await message.reply_text("❌ Failed to update farm name.")
                return

            context.user_data.pop("flow", None)
            await message.reply_text(f"✅ Farm name updated to *{new_farm}*.", parse_mode="Markdown")
        except Exception:
            logger.exception("Error updating farm name")
            await message.reply_text("❌ Failed to update farm name.")
        return


profile_handlers["handle_text"] = handle_text
'''