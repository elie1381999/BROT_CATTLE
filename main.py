import os
import re
import asyncio
import logging
import types
import inspect
from dotenv import load_dotenv
from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from telegram.error import NetworkError

# Payment & feed modules
import paymentcentral
from aboutfeedformula import feed_handlers  # new feed formula handlers

# Try to apply nest_asyncio so it can run in notebooks or nested loops
try:
    import nest_asyncio

    nest_asyncio.apply()
except Exception:
    pass

# Local modules / handlers
from keyboard import get_side_reply_keyboard, get_inline_main_menu
from aboutanimal import animal_handlers
from aboutmilk import milk_handlers
from aboutmoney import money_handlers
from partners import partner_handlers
from profile import profile_handlers
from aboutrole import role_handlers
from aboutbreeding import breeding_handlers
from aboutinventory import inventory_handlers

# AI: prefer consolidated aiconnection package (DeepSeek-only)
# Code 1 exposes `aiask_handlers` and `ask_gpt` at package level; attempt that first,
# then fall back to legacy submodule imports. Also handle the case where ask_gpt
# returns (content, latency).
try:
    from aiconnection import aiask_handlers, ask_gpt
    logging.getLogger(__name__).info("Loaded aiconnection package exports: aiask_handlers, ask_gpt")
except Exception:
    try:
        # legacy/submodule style
        from aiconnection.aiask import aiask_handlers
        from aiconnection.aicentral import ask_gpt
        logging.getLogger(__name__).info("Loaded aiconnection submodules: aiask, aicentral")
    except Exception as e:
        # Log full traceback so you can see why import failed
        logging.getLogger(__name__).exception("Failed to import aiconnection — AI Ask disabled for now.")
        aiask_handlers = {}
        ask_gpt = None

# Farm DB helpers
from farmcore import async_get_user_by_telegram, async_register_user

# Try AI helper (chat) — already imported above; keep name for backward compatibility
# (ask_gpt may be None if import failed)

load_dotenv()

# Environment
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DEBUG_CALLBACK = os.getenv("DEBUG_CALLBACK", "0") == "1"

# Basic logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


def _clear_flow_keys(context_user_data: dict):
    """
    Remove flow-related keys from user_data (cleanup helper).
    """
    for k in list(context_user_data.keys()):
        if k.startswith(
            (
                "flow",
                "animal",
                "milk",
                "money",
                "register",
                "breeding",
                "inventory",
                "profile",
                "role",
                "aiask",
            )
        ):
            context_user_data.pop(k, None)


# ------------------------
# Helpers for calling handlers safely
# ------------------------
async def _call_maybe_with_action(fn, update, context, action=None):
    """
    Call handler with action only if the handler accepts an 'action' parameter.
    Works for coroutine functions and synchronous callables.
    """
    try:
        accepts_action = False
        try:
            sig = inspect.signature(fn)
            accepts_action = "action" in sig.parameters
        except (ValueError, TypeError):
            accepts_action = False

        if asyncio.iscoroutinefunction(fn):
            if accepts_action:
                return await fn(update, context, action=action)
            else:
                return await fn(update, context)
        else:
            loop = asyncio.get_event_loop()
            if accepts_action:
                return await loop.run_in_executor(None, lambda: fn(update, context, action))
            else:
                return await loop.run_in_executor(None, lambda: fn(update, context))
    except TypeError:
        try:
            if asyncio.iscoroutinefunction(fn):
                return await fn(update, context)
            else:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, lambda: fn(update, context))
        except Exception:
            logger.exception("Handler call raised exception (fallback)")
            raise
    except Exception:
        logger.exception("Handler call raised exception")
        raise


def _is_module(obj):
    return isinstance(obj, types.ModuleType)


# ------------------------
# Error handler
# ------------------------
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Update caused error: %s", context.error)
    if isinstance(context.error, NetworkError):
        if update and getattr(update, "effective_message", None):
            try:
                await update.effective_message.reply_text(
                    "⚠️ Temporary network issue. Please try again."
                )
            except Exception:
                pass
    else:
        if update and getattr(update, "effective_message", None):
            try:
                await update.effective_message.reply_text(
                    "❌ An error occurred. Please try again later."
                )
            except Exception:
                pass


# ------------------------
# Start
# ------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        # effective_message can be None in some callback contexts, but handle gracefully
        if getattr(update, "effective_message", None):
            await update.effective_message.reply_text("⚠️ Unable to read user info.")
        return
    telegram_id = user.id

    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in /start")
        if update.message:
            await update.message.reply_text("⚠️ Database error. Try again later.")
        return

    # If user not registered, start registration flow
    if not user_row:
        context.user_data["register_flow"] = "name"
        await update.message.reply_text(
            "👋 Welcome to FarmBot!\n\n"
            "Before using the app, let's register your account.\n"
            "👉 Please enter your *full name* to continue:",
            parse_mode="Markdown",
        )
        return

    # Registered user: show ONLY the reply keyboard
    reply_keyboard = get_side_reply_keyboard()

    await update.message.reply_text(
        "Please choose an option from the keyboard below 👇",
        reply_markup=reply_keyboard,
    )


# ------------------------
# Help / Roles / Ask
# ------------------------
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👋 *FarmBot help*\n\n"
        "/start - Start or register\n"
        "/help - Show this help\n"
        "/roles - Open Role Management menu\n"
        "/ask - Ask BROT the AI\n\n"
        "You can also use the quick keyboard for actions."
    )
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown")
    elif update.callback_query:
        # callback_query may not have message; guard just in case
        if update.callback_query.message:
            await update.callback_query.message.reply_text(text, parse_mode="Markdown")


async def cmd_roles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    menu_fn = None
    if isinstance(role_handlers, dict):
        menu_fn = role_handlers.get("menu")
    elif hasattr(role_handlers, "menu"):
        menu_fn = getattr(role_handlers, "menu")
    if menu_fn:
        await _call_maybe_with_action(menu_fn, update, context)
    else:
        if update.message:
            await update.message.reply_text("⚠️ Roles menu is unavailable right now.")
        else:
            if update.callback_query and update.callback_query.message:
                await update.callback_query.message.reply_text(
                    "⚠️ Roles menu is unavailable right now"
                )


async def cmd_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Open the AI Ask menu (delegates to aiask_handlers.menu).
    aiask_handlers may be an imported module dict (from aiconnection.aiask) or a fallback empty dict.
    """
    menu_fn = None
    if isinstance(aiask_handlers, dict):
        menu_fn = aiask_handlers.get("menu")
    elif hasattr(aiask_handlers, "menu"):
        menu_fn = getattr(aiask_handlers, "menu")
    if menu_fn:
        await _call_maybe_with_action(menu_fn, update, context)
    else:
        # Provide a clearer message than generic unavailable
        if update.message:
            await update.message.reply_text(
                "⚠️ AI Ask is unavailable right now. The bot is running but the AI module failed to load."
            )
        else:
            if update.callback_query and update.callback_query.message:
                await update.callback_query.message.reply_text(
                    "⚠️ AI Ask is unavailable right now. The bot is running but the AI module failed to load."
                )


# ------------------------
# Message handler (reply keyboard + flows)
# ------------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message:
        return

    text = (message.text or "").strip()
    text_lower = text.lower()
    # use raw string to avoid syntax warnings
    normalized = re.sub(r"[^\w\s]", "", text_lower).strip()
    words = normalized.split()

    telegram_id = update.effective_user.id
    logger.info("Received message from telegram_id=%s: %s", telegram_id, text)

    # Map left reply keyboard texts to handlers
    reply_map = {
        "🐮 Animals": animal_handlers,
        "Animals": animal_handlers,
        "🥛 Milk": milk_handlers,
        "Milk": milk_handlers,
        "💰 Finance": money_handlers,
        "Finance": money_handlers,
        "🤝 Partners": partner_handlers,
        "Partners": partner_handlers,
        "📦 Inventory": inventory_handlers,
        "Inventory": inventory_handlers,
        "🧾 Feed Formula": feed_handlers,
        "Feed Formula": feed_handlers,
        "🐄 Breeding": breeding_handlers,
        "Breeding": breeding_handlers,
        "👤 Profile": profile_handlers,
        "Profile": profile_handlers,
        "👥 Roles": role_handlers,
        "Roles": role_handlers,
    }
    if text in reply_map:
        await _dispatch_menu(reply_map[text], update, context)
        return

    # Registration flow
    register_flow = context.user_data.get("register_flow")
    if register_flow == "name":
        context.user_data["register_name"] = text
        context.user_data["register_flow"] = "farm_name"
        await message.reply_text("🏡 Great! Now enter your *farm name*:", parse_mode="Markdown")
        return

    elif register_flow == "farm_name":
        name = context.user_data.get("register_name")
        farm_name = text

        if not name:
            context.user_data["register_flow"] = "name"
            await message.reply_text("⚠️ I didn't catch your name. Please enter your full name:")
            return

        try:
            result = await async_register_user(
                telegram_id=telegram_id, name=name, farm_name=farm_name, timezone="UTC"
            )
            if result.get("error"):
                await message.reply_text("⚠️ Failed to register. Try again later.")
            else:
                context.user_data.pop("register_flow", None)
                context.user_data.pop("register_name", None)

                # Registration success: ONLY tell user to press /start
                await message.reply_text(
                    f"✅ Registered successfully, {name}!\nYour farm '{farm_name}' is set up."
                )
                await message.reply_text("To open the main menu, please press /start.")
        except Exception:
            logger.exception("Failed to register user telegram_id=%s", telegram_id)
            await message.reply_text("⚠️ Failed to register. Try again later.")
        return

    # Ensure user registered for further actions
    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in handle_message")
        await message.reply_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        await message.reply_text("❌ You must register first. Use /start.")
        return

    # If a flow is set, route to the handler's handle_text
    flow = context.user_data.get("flow", "")
    if flow:
        handler_map = {
            "animal": animal_handlers,
            "milk": milk_handlers,
            "money": money_handlers,
            "profile": profile_handlers,
            "breeding": breeding_handlers,
            "inventory": inventory_handlers,
            "role": role_handlers,
            "roles": role_handlers,
            "aiask": aiask_handlers,
            "feed": feed_handlers,
        }
        for prefix, handlers in handler_map.items():
            if flow.startswith(prefix):
                handler = None
                if isinstance(handlers, dict):
                    handler = handlers.get("handle_text")
                elif hasattr(handlers, "handle_text"):
                    handler = getattr(handlers, "handle_text")
                if handler:
                    await _call_maybe_with_action(handler, update, context)
                    return

    def contains_any(cands):
        return any(c in words or c in normalized for c in cands)

    # Basic keyword routing
    if contains_any(["animals", "animal"]):
        await _dispatch_menu(animal_handlers, update, context)
        return
    if contains_any(["milk"]):
        await _dispatch_menu(milk_handlers, update, context)
        return
    if contains_any(["finance", "money"]):
        await _dispatch_menu(money_handlers, update, context)
        return
    if contains_any(["partners", "partner"]):
        await _dispatch_menu(partner_handlers, update, context)
        return
    if contains_any(["feed", "formula"]):
        await _dispatch_menu(feed_handlers, update, context)
        return
    if contains_any(["profile"]):
        await _dispatch_menu(profile_handlers, update, context)
        return
    if contains_any(["breeding", "breed"]):
        await _dispatch_menu(breeding_handlers, update, context)
        return
    if contains_any(["inventory", "stock"]):
        await _dispatch_menu(inventory_handlers, update, context)
        return
    if contains_any(["role", "roles", "workers"]):
        await _dispatch_menu(role_handlers, update, context)
        return
    if contains_any(["ask", "brot", "ai"]):
        await _dispatch_menu(aiask_handlers, update, context)
        return

    # Fallback to AI or canned fallback
    user_text = text
    if ask_gpt:
        try:
            # ask_gpt in Code 1 returns (content, latency) — handle both cases
            result = await ask_gpt(user_text)
            if isinstance(result, (list, tuple)):
                bot_reply = result[0]
            else:
                bot_reply = result
        except Exception:
            logger.exception("AI call failed")
            bot_reply = "⚠️ Sorry — I couldn't reach the AI service right now. Try again later."
    else:
        bot_reply = "I didn't understand that. Use /start to open the menu."

    # Ensure bot_reply is a string
    if isinstance(bot_reply, (list, dict)):
        bot_reply = str(bot_reply)

    await message.reply_text(bot_reply)


# helper to dispatch menu
async def _dispatch_menu(handlers, update, context):
    try:
        if isinstance(handlers, dict):
            menu_fn = handlers.get("menu")
        elif _is_module(handlers) and hasattr(handlers, "menu"):
            menu_fn = getattr(handlers, "menu")
        else:
            menu_fn = None

        if menu_fn:
            await _call_maybe_with_action(menu_fn, update, context)
        else:
            if update.message:
                await update.message.reply_text("⚠️ Menu not available right now.")
            elif update.callback_query and update.callback_query.message:
                await update.callback_query.message.reply_text("⚠️ Menu not available right now.")
    except Exception:
        logger.exception("Error dispatching menu")
        if update.message:
            await update.message.reply_text("❌ An error occurred while opening the menu.")


# ------------------------
# Button callback
# ------------------------
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        logger.warning("Received empty callback query")
        return

    data = query.data or ""
    telegram_id = update.effective_user.id
    logger.info("Received callback from telegram_id=%s: %s", telegram_id, data)

    try:
        await query.answer()
    except Exception:
        logger.exception("Failed to answer callbackQuery (non-fatal)")

    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in button_callback")
        try:
            await query.edit_message_text("⚠️ Database error. Try again later.")
        except Exception:
            pass
        return

    if not user_row:
        try:
            await query.edit_message_text("❌ You must register first. Use /start.")
        except Exception:
            pass
        return

    prefix, _, action = data.partition(":" )
    logger.info("Parsed callback: prefix=%s, action=%s", prefix, action)

    routers = {
        "animal": animal_handlers,
        "milk": milk_handlers,
        "money": money_handlers,
        "partner": partner_handlers,
        "profile": profile_handlers,
        "breeding": breeding_handlers,
        "inventory": inventory_handlers,
        "role": role_handlers,
        "roles": role_handlers,
        "aiask": aiask_handlers,
        "feed": feed_handlers,            # feed formulas routing
        "payment": paymentcentral,        # payment routing (invoices)
    }
    handlers = routers.get(prefix)
    if not handlers:
        logger.error("No handler found for prefix=%s", prefix)
        await query.edit_message_text("⚠️ Invalid action. Try the main menu.", reply_markup=get_inline_main_menu())
        return

    try:
        called = False
        # prefer dict-style handler with router key
        if isinstance(handlers, dict) and "router" in handlers:
            await _call_maybe_with_action(handlers["router"], update, context, action=action)
            called = True
        # module style with router attribute
        elif _is_module(handlers) and hasattr(handlers, "router"):
            await _call_maybe_with_action(getattr(handlers, "router"), update, context, action=action)
            called = True
        else:
            action_base = action.split(":")[0] if action else ""
            if isinstance(handlers, dict) and action_base in handlers:
                await _call_maybe_with_action(handlers[action_base], update, context, action=action)
                called = True
            elif _is_module(handlers) and hasattr(handlers, action_base):
                await _call_maybe_with_action(getattr(handlers, action_base), update, context, action=action)
                called = True
            elif isinstance(handlers, dict) and "menu" in handlers:
                await _call_maybe_with_action(handlers["menu"], update, context)
                called = True
            elif _is_module(handlers) and hasattr(handlers, "menu"):
                await _call_maybe_with_action(getattr(handlers, "menu"), update, context)
                called = True

        if not called:
            await query.edit_message_text("⚠️ Action not recognized. Try the main menu.", reply_markup=get_inline_main_menu())
            return

    except Exception as e:
        logger.exception("Error routing callback=%s: %s", data, e)
        try:
            await query.edit_message_text("❌ An error occurred. Try the main menu.", reply_markup=get_inline_main_menu())
        except Exception:
            pass
        return

    if DEBUG_CALLBACK:
        try:
            if query.message:
                await query.message.reply_text(f"DEBUG: handled {prefix}:{action}")
        except Exception:
            pass


# ------------------------
# Main entry
# ------------------------
async def main():
    if not TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set in environment (see .env.example)")
        return

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("roles", cmd_roles))
    app.add_handler(CommandHandler("ask", cmd_ask))
    # text handler
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    # successful Telegram Stars payments
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, paymentcentral.handle_successful_payment))
    # callback handler
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_error_handler(error_handler)

    await app.bot.set_my_commands(
        [
            BotCommand("start", "Start or register with FarmBot"),
            BotCommand("help", "Show help and commands"),
            BotCommand("roles", "Open Role Management"),
            BotCommand("ask", "Ask BROT the AI"),
        ]
    )

    logger.info("Starting FarmBot...")
    await app.run_polling()


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # already running (e.g. in a notebook); schedule task
            loop.create_task(main())
            loop.run_forever()
        else:
            asyncio.run(main())
    except RuntimeError as e:
        if "event loop is already running" in str(e):
            try:
                import nest_asyncio

                nest_asyncio.apply()
                asyncio.run(main())
            except Exception:
                logger.exception("Failed to recover from event loop error")
        else:
            raise





        



'''
#without rules
import os
import re
import asyncio
import logging
import types
import inspect
from dotenv import load_dotenv
from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from telegram.error import NetworkError

# Payment & feed modules
import paymentcentral
from aboutfeedformula import feed_handlers  # new feed formula handlers

# Try to apply nest_asyncio so it can run in notebooks or nested loops
try:
    import nest_asyncio

    nest_asyncio.apply()
except Exception:
    pass

# Local modules / handlers
from keyboard import get_side_reply_keyboard, get_inline_main_menu
from aboutanimal import animal_handlers
from aboutmilk import milk_handlers
from aboutmoney import money_handlers
from partners import partner_handlers
from profile import profile_handlers
from aboutrole import role_handlers
from aboutbreeding import breeding_handlers
from aboutinventory import inventory_handlers

# AI: prefer consolidated aiconnection package (DeepSeek-only)
# Code 1 exposes `aiask_handlers` and `ask_gpt` at package level; attempt that first,
# then fall back to legacy submodule imports. Also handle the case where ask_gpt
# returns (content, latency).
try:
    from aiconnection import aiask_handlers, ask_gpt
    logging.getLogger(__name__).info("Loaded aiconnection package exports: aiask_handlers, ask_gpt")
except Exception:
    try:
        # legacy/submodule style
        from aiconnection.aiask import aiask_handlers
        from aiconnection.aicentral import ask_gpt
        logging.getLogger(__name__).info("Loaded aiconnection submodules: aiask, aicentral")
    except Exception as e:
        # Log full traceback so you can see why import failed
        logging.getLogger(__name__).exception("Failed to import aiconnection — AI Ask disabled for now.")
        aiask_handlers = {}
        ask_gpt = None

# Farm DB helpers
from farmcore import async_get_user_by_telegram, async_register_user

# Try AI helper (chat) — already imported above; keep name for backward compatibility
# (ask_gpt may be None if import failed)

load_dotenv()

# Environment
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DEBUG_CALLBACK = os.getenv("DEBUG_CALLBACK", "0") == "1"

# Basic logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


def _clear_flow_keys(context_user_data: dict):
    """
    Remove flow-related keys from user_data (cleanup helper).
    """
    for k in list(context_user_data.keys()):
        if k.startswith(
            (
                "flow",
                "animal",
                "milk",
                "money",
                "register",
                "breeding",
                "inventory",
                "profile",
                "role",
                "aiask",
            )
        ):
            context_user_data.pop(k, None)


# ------------------------
# Helpers for calling handlers safely
# ------------------------
async def _call_maybe_with_action(fn, update, context, action=None):
    """
    Call handler with action only if the handler accepts an 'action' parameter.
    Works for coroutine functions and synchronous callables.
    """
    try:
        accepts_action = False
        try:
            sig = inspect.signature(fn)
            accepts_action = "action" in sig.parameters
        except (ValueError, TypeError):
            accepts_action = False

        if asyncio.iscoroutinefunction(fn):
            if accepts_action:
                return await fn(update, context, action=action)
            else:
                return await fn(update, context)
        else:
            loop = asyncio.get_event_loop()
            if accepts_action:
                return await loop.run_in_executor(None, lambda: fn(update, context, action))
            else:
                return await loop.run_in_executor(None, lambda: fn(update, context))
    except TypeError:
        try:
            if asyncio.iscoroutinefunction(fn):
                return await fn(update, context)
            else:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, lambda: fn(update, context))
        except Exception:
            logger.exception("Handler call raised exception (fallback)")
            raise
    except Exception:
        logger.exception("Handler call raised exception")
        raise


def _is_module(obj):
    return isinstance(obj, types.ModuleType)


# ------------------------
# Error handler
# ------------------------
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Update caused error: %s", context.error)
    if isinstance(context.error, NetworkError):
        if update and getattr(update, "effective_message", None):
            try:
                await update.effective_message.reply_text(
                    "⚠️ Temporary network issue. Please try again."
                )
            except Exception:
                pass
    else:
        if update and getattr(update, "effective_message", None):
            try:
                await update.effective_message.reply_text(
                    "❌ An error occurred. Please try again later."
                )
            except Exception:
                pass


# ------------------------
# Start
# ------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        # effective_message can be None in some callback contexts, but handle gracefully
        if getattr(update, "effective_message", None):
            await update.effective_message.reply_text("⚠️ Unable to read user info.")
        return
    telegram_id = user.id

    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in /start")
        if update.message:
            await update.message.reply_text("⚠️ Database error. Try again later.")
        return

    # If user not registered, start registration flow
    if not user_row:
        context.user_data["register_flow"] = "name"
        await update.message.reply_text(
            "👋 Welcome to FarmBot!\n\n"
            "Before using the app, let's register your account.\n"
            "👉 Please enter your *full name* to continue:",
            parse_mode="Markdown",
        )
        return

    # Registered user: show ONLY the reply keyboard
    reply_keyboard = get_side_reply_keyboard()

    await update.message.reply_text(
        "Please choose an option from the keyboard below 👇",
        reply_markup=reply_keyboard,
    )


# ------------------------
# Help / Roles / Ask
# ------------------------
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👋 *FarmBot help*\n\n"
        "/start - Start or register\n"
        "/help - Show this help\n"
        "/roles - Open Role Management menu\n"
        "/ask - Ask BROT the AI\n\n"
        "You can also use the quick keyboard for actions."
    )
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown")
    elif update.callback_query:
        # callback_query may not have message; guard just in case
        if update.callback_query.message:
            await update.callback_query.message.reply_text(text, parse_mode="Markdown")


async def cmd_roles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    menu_fn = None
    if isinstance(role_handlers, dict):
        menu_fn = role_handlers.get("menu")
    elif hasattr(role_handlers, "menu"):
        menu_fn = getattr(role_handlers, "menu")
    if menu_fn:
        await _call_maybe_with_action(menu_fn, update, context)
    else:
        if update.message:
            await update.message.reply_text("⚠️ Roles menu is unavailable right now.")
        else:
            if update.callback_query and update.callback_query.message:
                await update.callback_query.message.reply_text(
                    "⚠️ Roles menu is unavailable right now"
                )


async def cmd_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Open the AI Ask menu (delegates to aiask_handlers.menu).
    aiask_handlers may be an imported module dict (from aiconnection.aiask) or a fallback empty dict.
    """
    menu_fn = None
    if isinstance(aiask_handlers, dict):
        menu_fn = aiask_handlers.get("menu")
    elif hasattr(aiask_handlers, "menu"):
        menu_fn = getattr(aiask_handlers, "menu")
    if menu_fn:
        await _call_maybe_with_action(menu_fn, update, context)
    else:
        # Provide a clearer message than generic unavailable
        if update.message:
            await update.message.reply_text(
                "⚠️ AI Ask is unavailable right now. The bot is running but the AI module failed to load."
            )
        else:
            if update.callback_query and update.callback_query.message:
                await update.callback_query.message.reply_text(
                    "⚠️ AI Ask is unavailable right now. The bot is running but the AI module failed to load."
                )


# ------------------------
# Message handler (reply keyboard + flows)
# ------------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message:
        return

    text = (message.text or "").strip()
    text_lower = text.lower()
    # use raw string to avoid syntax warnings
    normalized = re.sub(r"[^\w\s]", "", text_lower).strip()
    words = normalized.split()

    telegram_id = update.effective_user.id
    logger.info("Received message from telegram_id=%s: %s", telegram_id, text)

    # Map left reply keyboard texts to handlers
    reply_map = {
        "🐮 Animals": animal_handlers,
        "Animals": animal_handlers,
        "🥛 Milk": milk_handlers,
        "Milk": milk_handlers,
        "💰 Finance": money_handlers,
        "Finance": money_handlers,
        "🤝 Partners": partner_handlers,
        "Partners": partner_handlers,
        "📦 Inventory": inventory_handlers,
        "Inventory": inventory_handlers,
        "🧾 Feed Formula": feed_handlers,
        "Feed Formula": feed_handlers,
        "🐄 Breeding": breeding_handlers,
        "Breeding": breeding_handlers,
        "👤 Profile": profile_handlers,
        "Profile": profile_handlers,
        "👥 Roles": role_handlers,
        "Roles": role_handlers,
    }
    if text in reply_map:
        await _dispatch_menu(reply_map[text], update, context)
        return

    # Registration flow
    register_flow = context.user_data.get("register_flow")
    if register_flow == "name":
        context.user_data["register_name"] = text
        context.user_data["register_flow"] = "farm_name"
        await message.reply_text("🏡 Great! Now enter your *farm name*:", parse_mode="Markdown")
        return

    elif register_flow == "farm_name":
        name = context.user_data.get("register_name")
        farm_name = text

        if not name:
            context.user_data["register_flow"] = "name"
            await message.reply_text("⚠️ I didn't catch your name. Please enter your full name:")
            return

        try:
            result = await async_register_user(
                telegram_id=telegram_id, name=name, farm_name=farm_name, timezone="UTC"
            )
            if result.get("error"):
                await message.reply_text("⚠️ Failed to register. Try again later.")
            else:
                context.user_data.pop("register_flow", None)
                context.user_data.pop("register_name", None)

                # Registration success: ONLY tell user to press /start
                await message.reply_text(
                    f"✅ Registered successfully, {name}!\nYour farm '{farm_name}' is set up."
                )
                await message.reply_text("To open the main menu, please press /start.")
        except Exception:
            logger.exception("Failed to register user telegram_id=%s", telegram_id)
            await message.reply_text("⚠️ Failed to register. Try again later.")
        return

    # Ensure user registered for further actions
    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in handle_message")
        await message.reply_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        await message.reply_text("❌ You must register first. Use /start.")
        return

    # If a flow is set, route to the handler's handle_text
    flow = context.user_data.get("flow", "")
    if flow:
        handler_map = {
            "animal": animal_handlers,
            "milk": milk_handlers,
            "money": money_handlers,
            "profile": profile_handlers,
            "breeding": breeding_handlers,
            "inventory": inventory_handlers,
            "role": role_handlers,
            "roles": role_handlers,
            "aiask": aiask_handlers,
            "feed": feed_handlers,
        }
        for prefix, handlers in handler_map.items():
            if flow.startswith(prefix):
                handler = None
                if isinstance(handlers, dict):
                    handler = handlers.get("handle_text")
                elif hasattr(handlers, "handle_text"):
                    handler = getattr(handlers, "handle_text")
                if handler:
                    await _call_maybe_with_action(handler, update, context)
                    return

    def contains_any(cands):
        return any(c in words or c in normalized for c in cands)

    # Basic keyword routing
    if contains_any(["animals", "animal"]):
        await _dispatch_menu(animal_handlers, update, context)
        return
    if contains_any(["milk"]):
        await _dispatch_menu(milk_handlers, update, context)
        return
    if contains_any(["finance", "money"]):
        await _dispatch_menu(money_handlers, update, context)
        return
    if contains_any(["partners", "partner"]):
        await _dispatch_menu(partner_handlers, update, context)
        return
    if contains_any(["feed", "formula"]):
        await _dispatch_menu(feed_handlers, update, context)
        return
    if contains_any(["profile"]):
        await _dispatch_menu(profile_handlers, update, context)
        return
    if contains_any(["breeding", "breed"]):
        await _dispatch_menu(breeding_handlers, update, context)
        return
    if contains_any(["inventory", "stock"]):
        await _dispatch_menu(inventory_handlers, update, context)
        return
    if contains_any(["role", "roles", "workers"]):
        await _dispatch_menu(role_handlers, update, context)
        return
    if contains_any(["ask", "brot", "ai"]):
        await _dispatch_menu(aiask_handlers, update, context)
        return

    # Fallback to AI or canned fallback
    user_text = text
    if ask_gpt:
        try:
            # ask_gpt in Code 1 returns (content, latency) — handle both cases
            result = await ask_gpt(user_text)
            if isinstance(result, (list, tuple)):
                bot_reply = result[0]
            else:
                bot_reply = result
        except Exception:
            logger.exception("AI call failed")
            bot_reply = "⚠️ Sorry — I couldn't reach the AI service right now. Try again later."
    else:
        bot_reply = "I didn't understand that. Use /start to open the menu."

    # Ensure bot_reply is a string
    if isinstance(bot_reply, (list, dict)):
        bot_reply = str(bot_reply)

    await message.reply_text(bot_reply)


# helper to dispatch menu
async def _dispatch_menu(handlers, update, context):
    try:
        if isinstance(handlers, dict):
            menu_fn = handlers.get("menu")
        elif _is_module(handlers) and hasattr(handlers, "menu"):
            menu_fn = getattr(handlers, "menu")
        else:
            menu_fn = None

        if menu_fn:
            await _call_maybe_with_action(menu_fn, update, context)
        else:
            if update.message:
                await update.message.reply_text("⚠️ Menu not available right now.")
            elif update.callback_query and update.callback_query.message:
                await update.callback_query.message.reply_text("⚠️ Menu not available right now.")
    except Exception:
        logger.exception("Error dispatching menu")
        if update.message:
            await update.message.reply_text("❌ An error occurred while opening the menu.")


# ------------------------
# Button callback
# ------------------------
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        logger.warning("Received empty callback query")
        return

    data = query.data or ""
    telegram_id = update.effective_user.id
    logger.info("Received callback from telegram_id=%s: %s", telegram_id, data)

    try:
        await query.answer()
    except Exception:
        logger.exception("Failed to answer callbackQuery (non-fatal)")

    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in button_callback")
        try:
            await query.edit_message_text("⚠️ Database error. Try again later.")
        except Exception:
            pass
        return

    if not user_row:
        try:
            await query.edit_message_text("❌ You must register first. Use /start.")
        except Exception:
            pass
        return

    prefix, _, action = data.partition(":" )
    logger.info("Parsed callback: prefix=%s, action=%s", prefix, action)

    routers = {
        "animal": animal_handlers,
        "milk": milk_handlers,
        "money": money_handlers,
        "partner": partner_handlers,
        "profile": profile_handlers,
        "breeding": breeding_handlers,
        "inventory": inventory_handlers,
        "role": role_handlers,
        "roles": role_handlers,
        "aiask": aiask_handlers,
        "feed": feed_handlers,            # feed formulas routing
        "payment": paymentcentral,        # payment routing (invoices)
    }
    handlers = routers.get(prefix)
    if not handlers:
        logger.error("No handler found for prefix=%s", prefix)
        await query.edit_message_text("⚠️ Invalid action. Try the main menu.", reply_markup=get_inline_main_menu())
        return

    try:
        called = False
        # prefer dict-style handler with router key
        if isinstance(handlers, dict) and "router" in handlers:
            await _call_maybe_with_action(handlers["router"], update, context, action=action)
            called = True
        # module style with router attribute
        elif _is_module(handlers) and hasattr(handlers, "router"):
            await _call_maybe_with_action(getattr(handlers, "router"), update, context, action=action)
            called = True
        else:
            action_base = action.split(":")[0] if action else ""
            if isinstance(handlers, dict) and action_base in handlers:
                await _call_maybe_with_action(handlers[action_base], update, context, action=action)
                called = True
            elif _is_module(handlers) and hasattr(handlers, action_base):
                await _call_maybe_with_action(getattr(handlers, action_base), update, context, action=action)
                called = True
            elif isinstance(handlers, dict) and "menu" in handlers:
                await _call_maybe_with_action(handlers["menu"], update, context)
                called = True
            elif _is_module(handlers) and hasattr(handlers, "menu"):
                await _call_maybe_with_action(getattr(handlers, "menu"), update, context)
                called = True

        if not called:
            await query.edit_message_text("⚠️ Action not recognized. Try the main menu.", reply_markup=get_inline_main_menu())
            return

    except Exception as e:
        logger.exception("Error routing callback=%s: %s", data, e)
        try:
            await query.edit_message_text("❌ An error occurred. Try the main menu.", reply_markup=get_inline_main_menu())
        except Exception:
            pass
        return

    if DEBUG_CALLBACK:
        try:
            if query.message:
                await query.message.reply_text(f"DEBUG: handled {prefix}:{action}")
        except Exception:
            pass


# ------------------------
# Main entry
# ------------------------
async def main():
    if not TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set in environment (see .env.example)")
        return

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("roles", cmd_roles))
    app.add_handler(CommandHandler("ask", cmd_ask))
    # text handler
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    # successful Telegram Stars payments
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, paymentcentral.handle_successful_payment))
    # callback handler
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_error_handler(error_handler)

    await app.bot.set_my_commands(
        [
            BotCommand("start", "Start or register with FarmBot"),
            BotCommand("help", "Show help and commands"),
            BotCommand("roles", "Open Role Management"),
            BotCommand("ask", "Ask BROT the AI"),
        ]
    )

    logger.info("Starting FarmBot...")
    await app.run_polling()


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # already running (e.g. in a notebook); schedule task
            loop.create_task(main())
            loop.run_forever()
        else:
            asyncio.run(main())
    except RuntimeError as e:
        if "event loop is already running" in str(e):
            try:
                import nest_asyncio

                nest_asyncio.apply()
                asyncio.run(main())
            except Exception:
                logger.exception("Failed to recover from event loop error")
        else:
            raise

'''








'''
#without the full upgrade of ai!
import os
import re
import asyncio
import logging
import types
import inspect
from dotenv import load_dotenv
from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from telegram.error import NetworkError

# Payment & feed modules
import paymentcentral
from aboutfeedformula import feed_handlers  # new feed formula handlers

# Try to apply nest_asyncio so it can run in notebooks or nested loops
try:
    import nest_asyncio

    nest_asyncio.apply()
except Exception:
    pass

# Local modules / handlers
from keyboard import get_side_reply_keyboard, get_inline_main_menu
from aboutanimal import animal_handlers
from aboutmilk import milk_handlers
from aboutmoney import money_handlers
from partners import partner_handlers
from profile import profile_handlers
from aboutrole import role_handlers
from aboutbreeding import breeding_handlers
from aboutinventory import inventory_handlers

# AI: prefer consolidated aiconnection package (DeepSeek-only)
try:
    # aiask_handlers is expected to be a dict with "menu", "handle_text", "router"
    from aiconnection.aiask import aiask_handlers
    logging.getLogger(__name__).info("Loaded aiconnection.aiask.aiask_handlers")
except Exception as e:
    # Log full traceback so you can see why import failed
    logging.getLogger(__name__).exception("Failed to import aiconnection.aiask — AI Ask disabled for now.")
    aiask_handlers = {}

# Farm DB helpers
from farmcore import async_get_user_by_telegram, async_register_user

# Try AI helper (chat)
try:
    from aiconnection.aicentral import ask_gpt
except Exception:
    ask_gpt = None

load_dotenv()

# Environment
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DEBUG_CALLBACK = os.getenv("DEBUG_CALLBACK", "0") == "1"

# Basic logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


def _clear_flow_keys(context_user_data: dict):
    """
    Remove flow-related keys from user_data (cleanup helper).
    """
    for k in list(context_user_data.keys()):
        if k.startswith(
            (
                "flow",
                "animal",
                "milk",
                "money",
                "register",
                "breeding",
                "inventory",
                "profile",
                "role",
                "aiask",
            )
        ):
            context_user_data.pop(k, None)


# ------------------------
# Helpers for calling handlers safely
# ------------------------
async def _call_maybe_with_action(fn, update, context, action=None):
    """
    Call handler with action only if the handler accepts an 'action' parameter.
    Works for coroutine functions and synchronous callables.
    """
    try:
        accepts_action = False
        try:
            sig = inspect.signature(fn)
            accepts_action = "action" in sig.parameters
        except (ValueError, TypeError):
            accepts_action = False

        if asyncio.iscoroutinefunction(fn):
            if accepts_action:
                return await fn(update, context, action=action)
            else:
                return await fn(update, context)
        else:
            loop = asyncio.get_event_loop()
            if accepts_action:
                return await loop.run_in_executor(None, lambda: fn(update, context, action))
            else:
                return await loop.run_in_executor(None, lambda: fn(update, context))
    except TypeError:
        try:
            if asyncio.iscoroutinefunction(fn):
                return await fn(update, context)
            else:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, lambda: fn(update, context))
        except Exception:
            logger.exception("Handler call raised exception (fallback)")
            raise
    except Exception:
        logger.exception("Handler call raised exception")
        raise


def _is_module(obj):
    return isinstance(obj, types.ModuleType)


# ------------------------
# Error handler
# ------------------------
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Update caused error: %s", context.error)
    if isinstance(context.error, NetworkError):
        if update and getattr(update, "effective_message", None):
            try:
                await update.effective_message.reply_text(
                    "⚠️ Temporary network issue. Please try again."
                )
            except Exception:
                pass
    else:
        if update and getattr(update, "effective_message", None):
            try:
                await update.effective_message.reply_text(
                    "❌ An error occurred. Please try again later."
                )
            except Exception:
                pass


# ------------------------
# Start
# ------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        # effective_message can be None in some callback contexts, but handle gracefully
        if getattr(update, "effective_message", None):
            await update.effective_message.reply_text("⚠️ Unable to read user info.")
        return
    telegram_id = user.id

    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in /start")
        if update.message:
            await update.message.reply_text("⚠️ Database error. Try again later.")
        return

    # If user not registered, start registration flow
    if not user_row:
        context.user_data["register_flow"] = "name"
        await update.message.reply_text(
            "👋 Welcome to FarmBot!\n\n"
            "Before using the app, let's register your account.\n"
            "👉 Please enter your *full name* to continue:",
            parse_mode="Markdown",
        )
        return

    # Registered user: show ONLY the reply keyboard
    reply_keyboard = get_side_reply_keyboard()

    await update.message.reply_text(
        "Please choose an option from the keyboard below 👇",
        reply_markup=reply_keyboard,
    )


# ------------------------
# Help / Roles / Ask
# ------------------------
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👋 *FarmBot help*\n\n"
        "/start - Start or register\n"
        "/help - Show this help\n"
        "/roles - Open Role Management menu\n"
        "/ask - Ask BROT the AI\n\n"
        "You can also use the quick keyboard for actions."
    )
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown")
    elif update.callback_query:
        # callback_query may not have message; guard just in case
        if update.callback_query.message:
            await update.callback_query.message.reply_text(text, parse_mode="Markdown")


async def cmd_roles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    menu_fn = None
    if isinstance(role_handlers, dict):
        menu_fn = role_handlers.get("menu")
    elif hasattr(role_handlers, "menu"):
        menu_fn = getattr(role_handlers, "menu")
    if menu_fn:
        await _call_maybe_with_action(menu_fn, update, context)
    else:
        if update.message:
            await update.message.reply_text("⚠️ Roles menu is unavailable right now.")
        else:
            if update.callback_query and update.callback_query.message:
                await update.callback_query.message.reply_text(
                    "⚠️ Roles menu is unavailable right now"
                )


async def cmd_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Open the AI Ask menu (delegates to aiask_handlers.menu).
    aiask_handlers may be an imported module dict (from aiconnection.aiask) or a fallback empty dict.
    """
    menu_fn = None
    if isinstance(aiask_handlers, dict):
        menu_fn = aiask_handlers.get("menu")
    elif hasattr(aiask_handlers, "menu"):
        menu_fn = getattr(aiask_handlers, "menu")
    if menu_fn:
        await _call_maybe_with_action(menu_fn, update, context)
    else:
        # Provide a clearer message than generic unavailable
        if update.message:
            await update.message.reply_text(
                "⚠️ AI Ask is unavailable right now. The bot is running but the AI module failed to load."
            )
        else:
            if update.callback_query and update.callback_query.message:
                await update.callback_query.message.reply_text(
                    "⚠️ AI Ask is unavailable right now. The bot is running but the AI module failed to load."
                )


# ------------------------
# Message handler (reply keyboard + flows)
# ------------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message:
        return

    text = (message.text or "").strip()
    text_lower = text.lower()
    # use raw string to avoid syntax warnings
    normalized = re.sub(r"[^\w\s]", "", text_lower).strip()
    words = normalized.split()

    telegram_id = update.effective_user.id
    logger.info("Received message from telegram_id=%s: %s", telegram_id, text)

    # Map left reply keyboard texts to handlers
    reply_map = {
        "🐮 Animals": animal_handlers,
        "Animals": animal_handlers,
        "🥛 Milk": milk_handlers,
        "Milk": milk_handlers,
        "💰 Finance": money_handlers,
        "Finance": money_handlers,
        "🤝 Partners": partner_handlers,
        "Partners": partner_handlers,
        "📦 Inventory": inventory_handlers,
        "Inventory": inventory_handlers,
        "🧾 Feed Formula": feed_handlers,
        "Feed Formula": feed_handlers,
        "🐄 Breeding": breeding_handlers,
        "Breeding": breeding_handlers,
        "👤 Profile": profile_handlers,
        "Profile": profile_handlers,
        "👥 Roles": role_handlers,
        "Roles": role_handlers,
    }
    if text in reply_map:
        await _dispatch_menu(reply_map[text], update, context)
        return

    # Registration flow
    register_flow = context.user_data.get("register_flow")
    if register_flow == "name":
        context.user_data["register_name"] = text
        context.user_data["register_flow"] = "farm_name"
        await message.reply_text("🏡 Great! Now enter your *farm name*:", parse_mode="Markdown")
        return

    elif register_flow == "farm_name":
        name = context.user_data.get("register_name")
        farm_name = text

        if not name:
            context.user_data["register_flow"] = "name"
            await message.reply_text("⚠️ I didn't catch your name. Please enter your full name:")
            return

        try:
            result = await async_register_user(
                telegram_id=telegram_id, name=name, farm_name=farm_name, timezone="UTC"
            )
            if result.get("error"):
                await message.reply_text("⚠️ Failed to register. Try again later.")
            else:
                context.user_data.pop("register_flow", None)
                context.user_data.pop("register_name", None)

                # Registration success: ONLY tell user to press /start
                await message.reply_text(
                    f"✅ Registered successfully, {name}!\nYour farm '{farm_name}' is set up."
                )
                await message.reply_text("To open the main menu, please press /start.")
        except Exception:
            logger.exception("Failed to register user telegram_id=%s", telegram_id)
            await message.reply_text("⚠️ Failed to register. Try again later.")
        return

    # Ensure user registered for further actions
    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in handle_message")
        await message.reply_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        await message.reply_text("❌ You must register first. Use /start.")
        return

    # If a flow is set, route to the handler's handle_text
    flow = context.user_data.get("flow", "")
    if flow:
        handler_map = {
            "animal": animal_handlers,
            "milk": milk_handlers,
            "money": money_handlers,
            "profile": profile_handlers,
            "breeding": breeding_handlers,
            "inventory": inventory_handlers,
            "role": role_handlers,
            "roles": role_handlers,
            "aiask": aiask_handlers,
            "feed": feed_handlers,
        }
        for prefix, handlers in handler_map.items():
            if flow.startswith(prefix):
                handler = None
                if isinstance(handlers, dict):
                    handler = handlers.get("handle_text")
                elif hasattr(handlers, "handle_text"):
                    handler = getattr(handlers, "handle_text")
                if handler:
                    await _call_maybe_with_action(handler, update, context)
                    return

    def contains_any(cands):
        return any(c in words or c in normalized for c in cands)

    # Basic keyword routing
    if contains_any(["animals", "animal"]):
        await _dispatch_menu(animal_handlers, update, context)
        return
    if contains_any(["milk"]):
        await _dispatch_menu(milk_handlers, update, context)
        return
    if contains_any(["finance", "money"]):
        await _dispatch_menu(money_handlers, update, context)
        return
    if contains_any(["partners", "partner"]):
        await _dispatch_menu(partner_handlers, update, context)
        return
    if contains_any(["feed", "formula"]):
        await _dispatch_menu(feed_handlers, update, context)
        return
    if contains_any(["profile"]):
        await _dispatch_menu(profile_handlers, update, context)
        return
    if contains_any(["breeding", "breed"]):
        await _dispatch_menu(breeding_handlers, update, context)
        return
    if contains_any(["inventory", "stock"]):
        await _dispatch_menu(inventory_handlers, update, context)
        return
    if contains_any(["role", "roles", "workers"]):
        await _dispatch_menu(role_handlers, update, context)
        return
    if contains_any(["ask", "brot", "ai"]):
        await _dispatch_menu(aiask_handlers, update, context)
        return

    # Fallback to AI or canned fallback
    user_text = text
    if ask_gpt:
        try:
            # If aiask flow is available it will provide a richer RAG flow; for freeform messages, use one-shot
            bot_reply = await ask_gpt(user_text)
        except Exception:
            logger.exception("AI call failed")
            bot_reply = "⚠️ Sorry — I couldn't reach the AI service right now. Try again later."
    else:
        bot_reply = "I didn't understand that. Use /start to open the menu."

    await message.reply_text(bot_reply)


# helper to dispatch menu
async def _dispatch_menu(handlers, update, context):
    try:
        if isinstance(handlers, dict):
            menu_fn = handlers.get("menu")
        elif _is_module(handlers) and hasattr(handlers, "menu"):
            menu_fn = getattr(handlers, "menu")
        else:
            menu_fn = None

        if menu_fn:
            await _call_maybe_with_action(menu_fn, update, context)
        else:
            if update.message:
                await update.message.reply_text("⚠️ Menu not available right now.")
            elif update.callback_query and update.callback_query.message:
                await update.callback_query.message.reply_text("⚠️ Menu not available right now.")
    except Exception:
        logger.exception("Error dispatching menu")
        if update.message:
            await update.message.reply_text("❌ An error occurred while opening the menu.")


# ------------------------
# Button callback
# ------------------------
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        logger.warning("Received empty callback query")
        return

    data = query.data or ""
    telegram_id = update.effective_user.id
    logger.info("Received callback from telegram_id=%s: %s", telegram_id, data)

    try:
        await query.answer()
    except Exception:
        logger.exception("Failed to answer callbackQuery (non-fatal)")

    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in button_callback")
        try:
            await query.edit_message_text("⚠️ Database error. Try again later.")
        except Exception:
            pass
        return

    if not user_row:
        try:
            await query.edit_message_text("❌ You must register first. Use /start.")
        except Exception:
            pass
        return

    prefix, _, action = data.partition(":")
    logger.info("Parsed callback: prefix=%s, action=%s", prefix, action)

    routers = {
        "animal": animal_handlers,
        "milk": milk_handlers,
        "money": money_handlers,
        "partner": partner_handlers,
        "profile": profile_handlers,
        "breeding": breeding_handlers,
        "inventory": inventory_handlers,
        "role": role_handlers,
        "roles": role_handlers,
        "aiask": aiask_handlers,
        "feed": feed_handlers,            # feed formulas routing
        "payment": paymentcentral,        # payment routing (invoices)
    }
    handlers = routers.get(prefix)
    if not handlers:
        logger.error("No handler found for prefix=%s", prefix)
        await query.edit_message_text("⚠️ Invalid action. Try the main menu.", reply_markup=get_inline_main_menu())
        return

    try:
        called = False
        # prefer dict-style handler with router key
        if isinstance(handlers, dict) and "router" in handlers:
            await _call_maybe_with_action(handlers["router"], update, context, action=action)
            called = True
        # module style with router attribute
        elif _is_module(handlers) and hasattr(handlers, "router"):
            await _call_maybe_with_action(getattr(handlers, "router"), update, context, action=action)
            called = True
        else:
            action_base = action.split(":")[0] if action else ""
            if isinstance(handlers, dict) and action_base in handlers:
                await _call_maybe_with_action(handlers[action_base], update, context, action=action)
                called = True
            elif _is_module(handlers) and hasattr(handlers, action_base):
                await _call_maybe_with_action(getattr(handlers, action_base), update, context, action=action)
                called = True
            elif isinstance(handlers, dict) and "menu" in handlers:
                await _call_maybe_with_action(handlers["menu"], update, context)
                called = True
            elif _is_module(handlers) and hasattr(handlers, "menu"):
                await _call_maybe_with_action(getattr(handlers, "menu"), update, context)
                called = True

        if not called:
            await query.edit_message_text("⚠️ Action not recognized. Try the main menu.", reply_markup=get_inline_main_menu())
            return

    except Exception as e:
        logger.exception("Error routing callback=%s: %s", data, e)
        try:
            await query.edit_message_text("❌ An error occurred. Try the main menu.", reply_markup=get_inline_main_menu())
        except Exception:
            pass
        return

    if DEBUG_CALLBACK:
        try:
            if query.message:
                await query.message.reply_text(f"DEBUG: handled {prefix}:{action}")
        except Exception:
            pass


# ------------------------
# Main entry
# ------------------------
async def main():
    if not TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set in environment (see .env.example)")
        return

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("roles", cmd_roles))
    app.add_handler(CommandHandler("ask", cmd_ask))
    # text handler
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    # successful Telegram Stars payments
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, paymentcentral.handle_successful_payment))
    # callback handler
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_error_handler(error_handler)

    await app.bot.set_my_commands(
        [
            BotCommand("start", "Start or register with FarmBot"),
            BotCommand("help", "Show help and commands"),
            BotCommand("roles", "Open Role Management"),
            BotCommand("ask", "Ask BROT the AI"),
        ]
    )

    logger.info("Starting FarmBot...")
    await app.run_polling()


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(main())
            loop.run_forever()
        else:
            asyncio.run(main())
    except RuntimeError as e:
        if "event loop is already running" in str(e):
            import nest_asyncio

            nest_asyncio.apply()
            asyncio.run(main())
        else:
            raise
'''










'''
# main.py (full - integrated with aiconnection / DeepSeek-only) before grok
import os
import re
import asyncio
import logging
import types
import inspect
from dotenv import load_dotenv
from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from telegram.error import NetworkError

# Payment & feed modules
import paymentcentral
from aboutfeedformula import feed_handlers  # new feed formula handlers

# Try to apply nest_asyncio so it can run in notebooks or nested loops
try:
    import nest_asyncio

    nest_asyncio.apply()
except Exception:
    pass

# Local modules / handlers
from keyboard import get_side_reply_keyboard, get_inline_main_menu
from aboutanimal import animal_handlers
from aboutmilk import milk_handlers
from aboutmoney import money_handlers
from partners import partner_handlers
from profile import profile_handlers
from aboutrole import role_handlers
from aboutbreeding import breeding_handlers
from aboutinventory import inventory_handlers

# AI: prefer consolidated aiconnection package (DeepSeek-only)
try:
    # aiask_handlers is expected to be a dict with "menu", "handle_text", "router"
    from aiconnection.aiask import aiask_handlers
    logging.getLogger(__name__).info("Loaded aiconnection.aiask.aiask_handlers")
except Exception as e:
    # Log full traceback so you can see why import failed
    logging.getLogger(__name__).exception("Failed to import aiconnection.aiask — AI Ask disabled for now.")
    aiask_handlers = {}

# Farm DB helpers
from farmcore import async_get_user_by_telegram, async_register_user

# Try AI helper (chat)
try:
    from aiconnection.aicentral import ask_gpt
except Exception:
    ask_gpt = None

load_dotenv()

# Environment
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DEBUG_CALLBACK = os.getenv("DEBUG_CALLBACK", "0") == "1"

# Basic logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


def _clear_flow_keys(context_user_data: dict):
    """
    Remove flow-related keys from user_data (cleanup helper).
    """
    for k in list(context_user_data.keys()):
        if k.startswith(
            (
                "flow",
                "animal",
                "milk",
                "money",
                "register",
                "breeding",
                "inventory",
                "profile",
                "role",
                "aiask",
            )
        ):
            context_user_data.pop(k, None)


# ------------------------
# Helpers for calling handlers safely
# ------------------------
async def _call_maybe_with_action(fn, update, context, action=None):
    """
    Call handler with action only if the handler accepts an 'action' parameter.
    Works for coroutine functions and synchronous callables.
    """
    try:
        accepts_action = False
        try:
            sig = inspect.signature(fn)
            accepts_action = "action" in sig.parameters
        except (ValueError, TypeError):
            accepts_action = False

        if asyncio.iscoroutinefunction(fn):
            if accepts_action:
                return await fn(update, context, action=action)
            else:
                return await fn(update, context)
        else:
            loop = asyncio.get_event_loop()
            if accepts_action:
                return await loop.run_in_executor(None, lambda: fn(update, context, action))
            else:
                return await loop.run_in_executor(None, lambda: fn(update, context))
    except TypeError:
        try:
            if asyncio.iscoroutinefunction(fn):
                return await fn(update, context)
            else:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, lambda: fn(update, context))
        except Exception:
            logger.exception("Handler call raised exception (fallback)")
            raise
    except Exception:
        logger.exception("Handler call raised exception")
        raise


def _is_module(obj):
    return isinstance(obj, types.ModuleType)


# ------------------------
# Error handler
# ------------------------
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Update caused error: %s", context.error)
    if isinstance(context.error, NetworkError):
        if update and getattr(update, "effective_message", None):
            try:
                await update.effective_message.reply_text(
                    "⚠️ Temporary network issue. Please try again."
                )
            except Exception:
                pass
    else:
        if update and getattr(update, "effective_message", None):
            try:
                await update.effective_message.reply_text(
                    "❌ An error occurred. Please try again later."
                )
            except Exception:
                pass


# ------------------------
# Start
# ------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        # effective_message can be None in some callback contexts, but handle gracefully
        if getattr(update, "effective_message", None):
            await update.effective_message.reply_text("⚠️ Unable to read user info.")
        return
    telegram_id = user.id

    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in /start")
        if update.message:
            await update.message.reply_text("⚠️ Database error. Try again later.")
        return

    # If user not registered, start registration flow
    if not user_row:
        context.user_data["register_flow"] = "name"
        await update.message.reply_text(
            "👋 Welcome to FarmBot!\n\n"
            "Before using the app, let's register your account.\n"
            "👉 Please enter your *full name* to continue:",
            parse_mode="Markdown",
        )
        return

    # Registered user: show ONLY the reply keyboard
    reply_keyboard = get_side_reply_keyboard()

    await update.message.reply_text(
        "Please choose an option from the keyboard below 👇",
        reply_markup=reply_keyboard,
    )


# ------------------------
# Help / Roles / Ask
# ------------------------
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👋 *FarmBot help*\n\n"
        "/start - Start or register\n"
        "/help - Show this help\n"
        "/roles - Open Role Management menu\n"
        "/ask - Ask BROT the AI\n\n"
        "You can also use the quick keyboard for actions."
    )
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown")
    elif update.callback_query:
        # callback_query may not have message; guard just in case
        if update.callback_query.message:
            await update.callback_query.message.reply_text(text, parse_mode="Markdown")


async def cmd_roles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    menu_fn = None
    if isinstance(role_handlers, dict):
        menu_fn = role_handlers.get("menu")
    elif hasattr(role_handlers, "menu"):
        menu_fn = getattr(role_handlers, "menu")
    if menu_fn:
        await _call_maybe_with_action(menu_fn, update, context)
    else:
        if update.message:
            await update.message.reply_text("⚠️ Roles menu is unavailable right now.")
        else:
            if update.callback_query and update.callback_query.message:
                await update.callback_query.message.reply_text(
                    "⚠️ Roles menu is unavailable right now"
                )


async def cmd_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Open the AI Ask menu (delegates to aiask_handlers.menu).
    aiask_handlers may be an imported module dict (from aiconnection.aiask) or a fallback empty dict.
    """
    menu_fn = None
    if isinstance(aiask_handlers, dict):
        menu_fn = aiask_handlers.get("menu")
    elif hasattr(aiask_handlers, "menu"):
        menu_fn = getattr(aiask_handlers, "menu")
    if menu_fn:
        await _call_maybe_with_action(menu_fn, update, context)
    else:
        # Provide a clearer message than generic unavailable
        if update.message:
            await update.message.reply_text(
                "⚠️ AI Ask is unavailable right now. The bot is running but the AI module failed to load."
            )
        else:
            if update.callback_query and update.callback_query.message:
                await update.callback_query.message.reply_text(
                    "⚠️ AI Ask is unavailable right now. The bot is running but the AI module failed to load."
                )


# ------------------------
# Message handler (reply keyboard + flows)
# ------------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message:
        return

    text = (message.text or "").strip()
    text_lower = text.lower()
    # use raw string to avoid syntax warnings
    normalized = re.sub(r"[^\w\s]", "", text_lower).strip()
    words = normalized.split()

    telegram_id = update.effective_user.id
    logger.info("Received message from telegram_id=%s: %s", telegram_id, text)

    # Map left reply keyboard texts to handlers
    reply_map = {
        "🐮 Animals": animal_handlers,
        "Animals": animal_handlers,
        "🥛 Milk": milk_handlers,
        "Milk": milk_handlers,
        "💰 Finance": money_handlers,
        "Finance": money_handlers,
        "🤝 Partners": partner_handlers,
        "Partners": partner_handlers,
        "📦 Inventory": inventory_handlers,
        "Inventory": inventory_handlers,
        "🧾 Feed Formula": feed_handlers,
        "Feed Formula": feed_handlers,
        "🐄 Breeding": breeding_handlers,
        "Breeding": breeding_handlers,
        "👤 Profile": profile_handlers,
        "Profile": profile_handlers,
        "👥 Roles": role_handlers,
        "Roles": role_handlers,
    }
    if text in reply_map:
        await _dispatch_menu(reply_map[text], update, context)
        return

    # Registration flow
    register_flow = context.user_data.get("register_flow")
    if register_flow == "name":
        context.user_data["register_name"] = text
        context.user_data["register_flow"] = "farm_name"
        await message.reply_text("🏡 Great! Now enter your *farm name*:", parse_mode="Markdown")
        return

    elif register_flow == "farm_name":
        name = context.user_data.get("register_name")
        farm_name = text

        if not name:
            context.user_data["register_flow"] = "name"
            await message.reply_text("⚠️ I didn't catch your name. Please enter your full name:")
            return

        try:
            result = await async_register_user(
                telegram_id=telegram_id, name=name, farm_name=farm_name, timezone="UTC"
            )
            if result.get("error"):
                await message.reply_text("⚠️ Failed to register. Try again later.")
            else:
                context.user_data.pop("register_flow", None)
                context.user_data.pop("register_name", None)

                # Registration success: ONLY tell user to press /start
                await message.reply_text(
                    f"✅ Registered successfully, {name}!\nYour farm '{farm_name}' is set up."
                )
                await message.reply_text("To open the main menu, please press /start.")
        except Exception:
            logger.exception("Failed to register user telegram_id=%s", telegram_id)
            await message.reply_text("⚠️ Failed to register. Try again later.")
        return

    # Ensure user registered for further actions
    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in handle_message")
        await message.reply_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        await message.reply_text("❌ You must register first. Use /start.")
        return

    # If a flow is set, route to the handler's handle_text
    flow = context.user_data.get("flow", "")
    if flow:
        handler_map = {
            "animal": animal_handlers,
            "milk": milk_handlers,
            "money": money_handlers,
            "profile": profile_handlers,
            "breeding": breeding_handlers,
            "inventory": inventory_handlers,
            "role": role_handlers,
            "roles": role_handlers,
            "aiask": aiask_handlers,
            "feed": feed_handlers,
        }
        for prefix, handlers in handler_map.items():
            if flow.startswith(prefix):
                handler = None
                if isinstance(handlers, dict):
                    handler = handlers.get("handle_text")
                elif hasattr(handlers, "handle_text"):
                    handler = getattr(handlers, "handle_text")
                if handler:
                    await _call_maybe_with_action(handler, update, context)
                    return

    def contains_any(cands):
        return any(c in words or c in normalized for c in cands)

    # Basic keyword routing
    if contains_any(["animals", "animal"]):
        await _dispatch_menu(animal_handlers, update, context)
        return
    if contains_any(["milk"]):
        await _dispatch_menu(milk_handlers, update, context)
        return
    if contains_any(["finance", "money"]):
        await _dispatch_menu(money_handlers, update, context)
        return
    if contains_any(["partners", "partner"]):
        await _dispatch_menu(partner_handlers, update, context)
        return
    if contains_any(["feed", "formula"]):
        await _dispatch_menu(feed_handlers, update, context)
        return
    if contains_any(["profile"]):
        await _dispatch_menu(profile_handlers, update, context)
        return
    if contains_any(["breeding", "breed"]):
        await _dispatch_menu(breeding_handlers, update, context)
        return
    if contains_any(["inventory", "stock"]):
        await _dispatch_menu(inventory_handlers, update, context)
        return
    if contains_any(["role", "roles", "workers"]):
        await _dispatch_menu(role_handlers, update, context)
        return
    if contains_any(["ask", "brot", "ai"]):
        await _dispatch_menu(aiask_handlers, update, context)
        return

    # Fallback to AI or canned fallback
    user_text = text
    if ask_gpt:
        try:
            # If aiask flow is available it will provide a richer RAG flow; for freeform messages, use one-shot
            bot_reply = await ask_gpt(user_text)
        except Exception:
            logger.exception("AI call failed")
            bot_reply = "⚠️ Sorry — I couldn't reach the AI service right now. Try again later."
    else:
        bot_reply = "I didn't understand that. Use /start to open the menu."

    await message.reply_text(bot_reply)


# helper to dispatch menu
async def _dispatch_menu(handlers, update, context):
    try:
        if isinstance(handlers, dict):
            menu_fn = handlers.get("menu")
        elif _is_module(handlers) and hasattr(handlers, "menu"):
            menu_fn = getattr(handlers, "menu")
        else:
            menu_fn = None

        if menu_fn:
            await _call_maybe_with_action(menu_fn, update, context)
        else:
            if update.message:
                await update.message.reply_text("⚠️ Menu not available right now.")
            elif update.callback_query and update.callback_query.message:
                await update.callback_query.message.reply_text("⚠️ Menu not available right now.")
    except Exception:
        logger.exception("Error dispatching menu")
        if update.message:
            await update.message.reply_text("❌ An error occurred while opening the menu.")


# ------------------------
# Button callback
# ------------------------
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        logger.warning("Received empty callback query")
        return

    data = query.data or ""
    telegram_id = update.effective_user.id
    logger.info("Received callback from telegram_id=%s: %s", telegram_id, data)

    try:
        await query.answer()
    except Exception:
        logger.exception("Failed to answer callbackQuery (non-fatal)")

    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in button_callback")
        try:
            await query.edit_message_text("⚠️ Database error. Try again later.")
        except Exception:
            pass
        return

    if not user_row:
        try:
            await query.edit_message_text("❌ You must register first. Use /start.")
        except Exception:
            pass
        return

    prefix, _, action = data.partition(":")
    logger.info("Parsed callback: prefix=%s, action=%s", prefix, action)

    routers = {
        "animal": animal_handlers,
        "milk": milk_handlers,
        "money": money_handlers,
        "partner": partner_handlers,
        "profile": profile_handlers,
        "breeding": breeding_handlers,
        "inventory": inventory_handlers,
        "role": role_handlers,
        "roles": role_handlers,
        "aiask": aiask_handlers,
        "feed": feed_handlers,            # feed formulas routing
        "payment": paymentcentral,        # payment routing (invoices)
    }
    handlers = routers.get(prefix)
    if not handlers:
        logger.error("No handler found for prefix=%s", prefix)
        await query.edit_message_text("⚠️ Invalid action. Try the main menu.", reply_markup=get_inline_main_menu())
        return

    try:
        called = False
        # prefer dict-style handler with router key
        if isinstance(handlers, dict) and "router" in handlers:
            await _call_maybe_with_action(handlers["router"], update, context, action=action)
            called = True
        # module style with router attribute
        elif _is_module(handlers) and hasattr(handlers, "router"):
            await _call_maybe_with_action(getattr(handlers, "router"), update, context, action=action)
            called = True
        else:
            action_base = action.split(":")[0] if action else ""
            if isinstance(handlers, dict) and action_base in handlers:
                await _call_maybe_with_action(handlers[action_base], update, context, action=action)
                called = True
            elif _is_module(handlers) and hasattr(handlers, action_base):
                await _call_maybe_with_action(getattr(handlers, action_base), update, context, action=action)
                called = True
            elif isinstance(handlers, dict) and "menu" in handlers:
                await _call_maybe_with_action(handlers["menu"], update, context)
                called = True
            elif _is_module(handlers) and hasattr(handlers, "menu"):
                await _call_maybe_with_action(getattr(handlers, "menu"), update, context)
                called = True

        if not called:
            await query.edit_message_text("⚠️ Action not recognized. Try the main menu.", reply_markup=get_inline_main_menu())
            return

    except Exception as e:
        logger.exception("Error routing callback=%s: %s", data, e)
        try:
            await query.edit_message_text("❌ An error occurred. Try the main menu.", reply_markup=get_inline_main_menu())
        except Exception:
            pass
        return

    if DEBUG_CALLBACK:
        try:
            if query.message:
                await query.message.reply_text(f"DEBUG: handled {prefix}:{action}")
        except Exception:
            pass


# ------------------------
# Main entry
# ------------------------
async def main():
    if not TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set in environment (see .env.example)")
        return

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("roles", cmd_roles))
    app.add_handler(CommandHandler("ask", cmd_ask))
    # text handler
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    # successful Telegram Stars payments
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, paymentcentral.handle_successful_payment))
    # callback handler
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_error_handler(error_handler)

    await app.bot.set_my_commands(
        [
            BotCommand("start", "Start or register with FarmBot"),
            BotCommand("help", "Show help and commands"),
            BotCommand("roles", "Open Role Management"),
            BotCommand("ask", "Ask BROT the AI"),
        ]
    )

    logger.info("Starting FarmBot...")
    await app.run_polling()


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(main())
            loop.run_forever()
        else:
            asyncio.run(main())
    except RuntimeError as e:
        if "event loop is already running" in str(e):
            import nest_asyncio

            nest_asyncio.apply()
            asyncio.run(main())
        else:
            raise
'''










'''# main.py (working with bot 2)
import os
import re
import asyncio
import logging
import types
from dotenv import load_dotenv
from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from telegram.error import NetworkError

# Payment & feed modules
import paymentcentral
from aboutfeedformula import feed_handlers  # new feed formula handlers

# Try to apply nest_asyncio so it can run in notebooks or nested loops
try:
    import nest_asyncio
    nest_asyncio.apply()
except Exception:
    pass

# Local modules / handlers
from keyboard import get_side_reply_keyboard, get_inline_main_menu
from aboutanimal import animal_handlers
from aboutmilk import milk_handlers
from aboutmoney import money_handlers
from partners import partner_handlers
from profile import profile_handlers
from aboutrole import role_handlers
from aboutbreeding import breeding_handlers
from aboutinventory import inventory_handlers
from aiask import aiask_handlers

from farmcore import async_get_user_by_telegram, async_register_user

# Try AI helper
try:
    from aicentral import ask_gpt
except Exception:
    ask_gpt = None

load_dotenv()

# Environment
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DEBUG_CALLBACK = os.getenv("DEBUG_CALLBACK", "0") == "1"

# Basic logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

def _clear_flow_keys(context_user_data: dict):
    for k in list(context_user_data.keys()):
        if k.startswith(
            (
                "flow",
                "animal",
                "milk",
                "money",
                "register",
                "breeding",
                "inventory",
                "profile",
                "role",
                "aiask",
            )
        ):
            context_user_data.pop(k, None)

# ------------------------
# Helpers for calling handlers safely
# ------------------------
async def _call_maybe_with_action(fn, update, context, action=None):
    try:
        if asyncio.iscoroutinefunction(fn):
            try:
                return await fn(update, context, action=action)
            except TypeError:
                return await fn(update, context)
        else:
            loop = asyncio.get_event_loop()
            try:
                return await loop.run_in_executor(None, lambda: fn(update, context, action))
            except TypeError:
                return await loop.run_in_executor(None, lambda: fn(update, context))
    except Exception:
        logger.exception("Handler call raised exception")
        raise

def _is_module(obj):
    return isinstance(obj, types.ModuleType)

# ------------------------
# Error handler
# ------------------------
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Update caused error: %s", context.error)
    if isinstance(context.error, NetworkError):
        if update and getattr(update, "effective_message", None):
            try:
                await update.effective_message.reply_text(
                    "⚠️ Temporary network issue. Please try again."
                )
            except Exception:
                pass
    else:
        if update and getattr(update, "effective_message", None):
            try:
                await update.effective_message.reply_text(
                    "❌ An error occurred. Please try again later."
                )
            except Exception:
                pass

# ------------------------
# Start
# ------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        await update.effective_message.reply_text("⚠️ Unable to read user info.")
        return
    telegram_id = user.id

    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in /start")
        if update.message:
            await update.message.reply_text("⚠️ Database error. Try again later.")
        return

    # If user not registered, start registration flow
    if not user_row:
        context.user_data["register_flow"] = "name"
        await update.message.reply_text(
            "👋 Welcome to FarmBot!\n\n"
            "Before using the app, let's register your account.\n"
            "👉 Please enter your *full name* to continue:",
            parse_mode="Markdown",
        )
        return

    # Registered user: show ONLY the reply keyboard
    reply_keyboard = get_side_reply_keyboard()

    await update.message.reply_text(
        "Please choose an option from the keyboard below 👇",
        reply_markup=reply_keyboard,
    )

# ------------------------
# Help / Roles / Ask
# ------------------------
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👋 *FarmBot help*\n\n"
        "/start - Start or register\n"
        "/help - Show this help\n"
        "/roles - Open Role Management menu\n"
        "/ask - Ask BROT the AI\n\n"
        "You can also use the quick keyboard for actions."
    )
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown")
    elif update.callback_query:
        await update.callback_query.message.reply_text(text, parse_mode="Markdown")

async def cmd_roles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    menu_fn = None
    if isinstance(role_handlers, dict):
        menu_fn = role_handlers.get("menu")
    elif hasattr(role_handlers, "menu"):
        menu_fn = getattr(role_handlers, "menu")
    if menu_fn:
        await _call_maybe_with_action(menu_fn, update, context)
    else:
        if update.message:
            await update.message.reply_text("⚠️ Roles menu is unavailable right now.")
        else:
            await update.callback_query.message.reply_text(
                "⚠️ Roles menu is unavailable right now"
            )

async def cmd_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    menu_fn = None
    if isinstance(aiask_handlers, dict):
        menu_fn = aiask_handlers.get("menu")
    elif hasattr(aiask_handlers, "menu"):
        menu_fn = getattr(aiask_handlers, "menu")
    if menu_fn:
        await _call_maybe_with_action(menu_fn, update, context)
    else:
        if update.message:
            await update.message.reply_text("⚠️ AI Ask is unavailable right now.")
        else:
            await update.callback_query.message.reply_text("⚠️ AI Ask is unavailable right now.")

# ------------------------
# Message handler (reply keyboard + flows)
# ------------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message:
        return

    text = (message.text or "").strip()
    text_lower = text.lower()
    normalized = re.sub(r"[^\w\s]", "", text_lower).strip()
    words = normalized.split()

    telegram_id = update.effective_user.id
    logger.info("Received message from telegram_id=%s: %s", telegram_id, text)

    # Map left reply keyboard texts to handlers
    reply_map = {
        "🐮 Animals": animal_handlers,
        "Animals": animal_handlers,
        "🥛 Milk": milk_handlers,
        "Milk": milk_handlers,
        "💰 Finance": money_handlers,
        "Finance": money_handlers,
        "🤝 Partners": partner_handlers,
        "Partners": partner_handlers,
        "📦 Inventory": inventory_handlers,
        "Inventory": inventory_handlers,
        "🧾 Feed Formula": feed_handlers,
        "Feed Formula": feed_handlers,
        "🤰 Breeding": breeding_handlers,
        "Breeding": breeding_handlers,
        "👤 Profile": profile_handlers,
        "Profile": profile_handlers,
        "👥 Roles": role_handlers,
        "Roles": role_handlers,
    }
    if text in reply_map:
        await _dispatch_menu(reply_map[text], update, context)
        return

    # Registration flow
    register_flow = context.user_data.get("register_flow")
    if register_flow == "name":
        context.user_data["register_name"] = text
        context.user_data["register_flow"] = "farm_name"
        await message.reply_text("🏡 Great! Now enter your *farm name*:", parse_mode="Markdown")
        return

    elif register_flow == "farm_name":
        name = context.user_data.get("register_name")
        farm_name = text

        if not name:
            context.user_data["register_flow"] = "name"
            await message.reply_text("⚠️ I didn't catch your name. Please enter your full name:")
            return

        try:
            result = await async_register_user(
                telegram_id=telegram_id, name=name, farm_name=farm_name, timezone="UTC"
            )
            if result.get("error"):
                await message.reply_text("⚠️ Failed to register. Try again later.")
            else:
                context.user_data.pop("register_flow", None)
                context.user_data.pop("register_name", None)

                # Registration success: ONLY tell user to press /start
                await message.reply_text(
                    f"✅ Registered successfully, {name}!\nYour farm '{farm_name}' is set up."
                )
                await message.reply_text("To open the main menu, please press /start.")
        except Exception:
            logger.exception("Failed to register user telegram_id=%s", telegram_id)
            await message.reply_text("⚠️ Failed to register. Try again later.")
        return

    # Ensure user registered for further actions
    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in handle_message")
        await message.reply_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        await message.reply_text("❌ You must register first. Use /start.")
        return

    # If a flow is set, route to the handler's handle_text
    flow = context.user_data.get("flow", "")
    if flow:
        handler_map = {
            "animal": animal_handlers,
            "milk": milk_handlers,
            "money": money_handlers,
            "profile": profile_handlers,
            "breeding": breeding_handlers,
            "inventory": inventory_handlers,
            "role": role_handlers,
            "roles": role_handlers,
            "aiask": aiask_handlers,
            "feed": feed_handlers,
        }
        for prefix, handlers in handler_map.items():
            if flow.startswith(prefix):
                handler = None
                if isinstance(handlers, dict):
                    handler = handlers.get("handle_text")
                elif hasattr(handlers, "handle_text"):
                    handler = getattr(handlers, "handle_text")
                if handler:
                    await _call_maybe_with_action(handler, update, context)
                    return

    def contains_any(cands):
        return any(c in words or c in normalized for c in cands)

    # Basic keyword routing
    if contains_any(["animals", "animal"]):
        await _dispatch_menu(animal_handlers, update, context)
        return
    if contains_any(["milk"]):
        await _dispatch_menu(milk_handlers, update, context)
        return
    if contains_any(["finance", "money"]):
        await _dispatch_menu(money_handlers, update, context)
        return
    if contains_any(["partners", "partner"]):
        await _dispatch_menu(partner_handlers, update, context)
        return
    if contains_any(["feed", "formula"]):
        await _dispatch_menu(feed_handlers, update, context)
        return
    if contains_any(["profile"]):
        await _dispatch_menu(profile_handlers, update, context)
        return
    if contains_any(["breeding", "breed"]):
        await _dispatch_menu(breeding_handlers, update, context)
        return
    if contains_any(["inventory", "stock"]):
        await _dispatch_menu(inventory_handlers, update, context)
        return
    if contains_any(["role", "roles", "workers"]):
        await _dispatch_menu(role_handlers, update, context)
        return
    if contains_any(["ask", "brot", "ai"]):
        await _dispatch_menu(aiask_handlers, update, context)
        return

    # Fallback to AI or canned fallback
    user_text = text
    if ask_gpt:
        try:
            bot_reply = await ask_gpt(user_text)
        except Exception:
            logger.exception("AI call failed")
            bot_reply = "⚠️ Sorry — I couldn't reach the AI service right now. Try again later."
    else:
        bot_reply = "I didn't understand that. Use /start to open the menu."

    await message.reply_text(bot_reply)

# helper to dispatch menu
async def _dispatch_menu(handlers, update, context):
    try:
        if isinstance(handlers, dict):
            menu_fn = handlers.get("menu")
        elif _is_module(handlers) and hasattr(handlers, "menu"):
            menu_fn = getattr(handlers, "menu")
        else:
            menu_fn = None

        if menu_fn:
            await _call_maybe_with_action(menu_fn, update, context)
        else:
            if update.message:
                await update.message.reply_text("⚠️ Menu not available right now.")
            elif update.callback_query and update.callback_query.message:
                await update.callback_query.message.reply_text("⚠️ Menu not available right now.")
    except Exception:
        logger.exception("Error dispatching menu")
        if update.message:
            await update.message.reply_text("❌ An error occurred while opening the menu.")

# ------------------------
# Button callback
# ------------------------
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        logger.warning("Received empty callback query")
        return

    data = query.data or ""
    telegram_id = update.effective_user.id
    logger.info("Received callback from telegram_id=%s: %s", telegram_id, data)

    try:
        await query.answer()
    except Exception:
        logger.exception("Failed to answer callbackQuery (non-fatal)")

    if data == "skip":
        _clear_flow_keys(context.user_data)
        try:
            await query.edit_message_text("⏭️ Skipped. Use the menu for next action.")
        except Exception:
            logger.exception("Failed to edit message for skip")
        return

    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in button_callback")
        try:
            await query.edit_message_text("⚠️ Database error. Try again later.")
        except Exception:
            pass
        return

    if not user_row:
        try:
            await query.edit_message_text("❌ You must register first. Use /start.")
        except Exception:
            pass
        return

    prefix, _, action = data.partition(":")
    logger.info("Parsed callback: prefix=%s, action=%s", prefix, action)

    routers = {
        "animal": animal_handlers,
        "milk": milk_handlers,
        "money": money_handlers,
        "partner": partner_handlers,
        "profile": profile_handlers,
        "breeding": breeding_handlers,
        "inventory": inventory_handlers,
        "role": role_handlers,
        "roles": role_handlers,
        "aiask": aiask_handlers,
        "feed": feed_handlers,            # feed formulas routing
        "payment": paymentcentral,        # payment routing (invoices)
    }
    handlers = routers.get(prefix)
    if not handlers:
        logger.error("No handler found for prefix=%s", prefix)
        await query.edit_message_text("⚠️ Invalid action. Try the main menu.", reply_markup=get_inline_main_menu())
        return

    try:
        called = False
        # prefer dict-style handler with router key
        if isinstance(handlers, dict) and "router" in handlers:
            await _call_maybe_with_action(handlers["router"], update, context, action=action)
            called = True
        # module style with router attribute
        elif _is_module(handlers) and hasattr(handlers, "router"):
            await _call_maybe_with_action(getattr(handlers, "router"), update, context, action=action)
            called = True
        else:
            action_base = action.split(":")[0] if action else ""
            if isinstance(handlers, dict) and action_base in handlers:
                await _call_maybe_with_action(handlers[action_base], update, context, action=action)
                called = True
            elif _is_module(handlers) and hasattr(handlers, action_base):
                await _call_maybe_with_action(getattr(handlers, action_base), update, context, action=action)
                called = True
            elif isinstance(handlers, dict) and "menu" in handlers:
                await _call_maybe_with_action(handlers["menu"], update, context)
                called = True
            elif _is_module(handlers) and hasattr(handlers, "menu"):
                await _call_maybe_with_action(getattr(handlers, "menu"), update, context)
                called = True

        if not called:
            await query.edit_message_text("⚠️ Action not recognized. Try the main menu.", reply_markup=get_inline_main_menu())
            return

    except Exception as e:
        logger.exception("Error routing callback=%s: %s", data, e)
        try:
            await query.edit_message_text("❌ An error occurred. Try the main menu.", reply_markup=get_inline_main_menu())
        except Exception:
            pass
        return

    if DEBUG_CALLBACK:
        try:
            await query.message.reply_text(f"DEBUG: handled {prefix}:{action}")
        except Exception:
            pass

# ------------------------
# Main entry
# ------------------------
async def main():
    if not TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set in environment (see .env.example)")
        return

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("roles", cmd_roles))
    app.add_handler(CommandHandler("ask", cmd_ask))
    # text handler
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    # successful Telegram Stars payments
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, paymentcentral.handle_successful_payment))
    # callback handler
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_error_handler(error_handler)

    await app.bot.set_my_commands(
        [
            BotCommand("start", "Start or register with FarmBot"),
            BotCommand("help", "Show help and commands"),
            BotCommand("roles", "Open Role Management"),
            BotCommand("ask", "Ask BROT the AI"),
        ]
    )

    logger.info("Starting FarmBot...")
    await app.run_polling()

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(main())
            loop.run_forever()
        else:
            asyncio.run(main())
    except RuntimeError as e:
        if "event loop is already running" in str(e):
            import nest_asyncio
            nest_asyncio.apply()
            asyncio.run(main())
        else:
            raise

'''













'''
#with gpt
import os
import re
import asyncio
import logging
from dotenv import load_dotenv
from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from telegram.error import NetworkError

# Apply nest_asyncio early to handle nested event loops
try:
    import nest_asyncio

    nest_asyncio.apply()
except ImportError:
    print("Warning: nest_asyncio not installed. Install with 'pip install nest_asyncio' if running in a nested loop environment.")

# Local handlers / app modules
from keyboard import get_side_reply_keyboard, get_inline_main_menu
from aboutanimal import animal_handlers
from aboutmilk import milk_handlers
from aboutmoney import money_handlers
from partners import partner_handlers
from profile import profile_handlers
from aboutrole import role_handlers
from aboutbreeding import breeding_handlers
from aboutinventory import inventory_handlers
from aiask import aiask_handlers  # New import for AI Ask handlers

from farmcore import (
    async_get_user_by_telegram,
    async_register_user,
)

# Try to import AI helper from aicentral; fall back to a no-op if missing
try:
    from aicentral import ask_gpt
except Exception as e:
    ask_gpt = None
    print(
        "Warning: failed to import aicentral.ask_gpt(). Make sure aicentral.py exists and exposes async def ask_gpt(prompt, system_prompt=None) -> str. Error:",
        e,
    )

# Supabase client (optional)
try:
    from supabase import create_client, Client
except Exception:
    create_client = None
    Client = None

load_dotenv()

# Environment
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Initialize Supabase client if available
supabase = None
if create_client and SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception:
        supabase = None

# Basic logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def _clear_flow_keys(context_user_data: dict):
    """Clear flow-related keys for safety."""
    for k in list(context_user_data.keys()):
        if k.startswith(
            (
                "flow",
                "animal",
                "milk",
                "money",
                "register",
                "breeding",
                "inventory",
                "profile",
                "role",
                "aiask",
            )
        ):
            context_user_data.pop(k, None)


# ------------------------
# ERROR HANDLER
# ------------------------
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Update caused error: %s", context.error)
    if isinstance(context.error, NetworkError):
        if update and getattr(update, "effective_message", None):
            try:
                await update.effective_message.reply_text(
                    "⚠️ Temporary network issue. Please try again."
                )
            except Exception:
                pass
    else:
        if update and getattr(update, "effective_message", None):
            try:
                await update.effective_message.reply_text(
                    "❌ An error occurred. Please try again later."
                )
            except Exception:
                pass


# ------------------------
# START / REGISTRATION
# ------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        await update.effective_message.reply_text("⚠️ Unable to read user info.")
        return
    telegram_id = user.id

    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in /start")
        await update.message.reply_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        context.user_data["register_flow"] = "name"
        await update.message.reply_text(
            "👋 Welcome to FarmBot!\n\n"
            "Before using the app, let's register your account.\n"
            "👉 Please enter your *full name* to continue:",
            parse_mode="Markdown",
        )
        return

    reply_kb = get_side_reply_keyboard()
    inline_menu = get_inline_main_menu()
    await update.message.reply_text(
        "🔵 Welcome back to FarmBot! Use the keyboard or quick actions below.",
        reply_markup=reply_kb,
    )
    await update.message.reply_text("Quick actions:", reply_markup=inline_menu)


# ------------------------
# HELP and ROLES commands
# ------------------------
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👋 *FarmBot help*\n\n"
        "/start - Start or register\n"
        "/help - Show this help\n"
        "/roles - Open Role Management menu\n"
        "/ask - Ask BROT the AI\n\n"
        "You can also use the quick keyboard or inline buttons for actions."
    )
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown")
    elif update.callback_query:
        await update.callback_query.message.reply_text(text, parse_mode="Markdown")


async def cmd_roles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("Received /roles command from telegram_id=%s", update.effective_user.id)
    menu_fn = role_handlers.get("menu")
    if menu_fn:
        await menu_fn(update, context)
    else:
        logger.error("Role menu handler not found. Available handlers: %s", list(role_handlers.keys()))
        if update.message:
            await update.message.reply_text("⚠️ Roles menu is unavailable right now.")
        else:
            await update.callback_query.message.reply_text(
                "⚠️ Roles menu is unavailable right now."
            )


# ------------------------
# AI ASK command
# ------------------------
async def cmd_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("Received /ask command from telegram_id=%s", update.effective_user.id)
    menu_fn = aiask_handlers.get("menu")
    if menu_fn:
        await menu_fn(update, context)
    else:
        logger.error("AI Ask menu handler not found. Available handlers: %s", list(aiask_handlers.keys()))
        if update.message:
            await update.message.reply_text("⚠️ AI Ask is unavailable right now.")
        else:
            await update.callback_query.message.reply_text("⚠️ AI Ask is unavailable right now.")


# ------------------------
# MESSAGE HANDLER
# ------------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message:
        return

    text = (message.text or "").strip()
    text_lower = text.lower()
    normalized = re.sub(r"[^\w\s]", "", text_lower).strip()  # Fixed regex escape
    words = normalized.split()

    telegram_id = update.effective_user.id
    logger.info("Received message from telegram_id=%s: %s", telegram_id, text)

    # Registration flow
    register_flow = context.user_data.get("register_flow")
    if register_flow == "name":
        context.user_data["register_name"] = text
        context.user_data["register_flow"] = "farm_name"
        await message.reply_text("🏡 Great! Now enter your *farm name*:", parse_mode="Markdown")
        return

    elif register_flow == "farm_name":
        name = context.user_data.get("register_name")
        farm_name = text

        if not name:
            context.user_data["register_flow"] = "name"
            await message.reply_text("⚠️ I didn't catch your name. Please enter your full name:")
            return

        try:
            result = await async_register_user(
                telegram_id=telegram_id, name=name, farm_name=farm_name, timezone="UTC"
            )
            if result.get("error"):
                await message.reply_text("⚠️ Failed to register. Try again later.")
            else:
                context.user_data.pop("register_flow", None)
                context.user_data.pop("register_name", None)
                await message.reply_text(
                    f"✅ Registered successfully, {name}!\n"
                    f"Your farm '{farm_name}' is set up."
                )
                reply_kb = get_side_reply_keyboard()
                inline_menu = get_inline_main_menu()
                await message.reply_text("Main menu:", reply_markup=reply_kb)
                await message.reply_text("Quick actions:", reply_markup=inline_menu)
        except Exception:
            logger.exception("Failed to register user telegram_id=%s", telegram_id)
            await message.reply_text("⚠️ Failed to register. Try again later.")
        return

    # Ensure user is registered
    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in handle_message for telegram_id=%s", telegram_id)
        await message.reply_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        await message.reply_text("❌ You must register first. Use /start.")
        return

    # Flow routing
    flow = context.user_data.get("flow", "")
    if flow:
        handler_map = {
            "animal": animal_handlers,
            "milk": milk_handlers,
            "money": money_handlers,
            "profile": profile_handlers,
            "breeding": breeding_handlers,
            "inventory": inventory_handlers,
            "role": role_handlers,
            "roles": role_handlers,
            "aiask": aiask_handlers,
        }
        for prefix, handlers in handler_map.items():
            if flow.startswith(prefix):
                handler = handlers.get("handle_text")
                if handler:
                    logger.info("Routing flow=%s to handler %s", flow, prefix)
                    await handler(update, context)
                    return

    def contains_any(cands):
        return any(c in words or c in normalized for c in cands)

    if contains_any(["animals", "animal"]):
        return await animal_handlers.get("menu")(update, context)
    if contains_any(["milk"]):
        return await milk_handlers.get("menu")(update, context)
    if contains_any(["finance", "money"]):
        return await money_handlers.get("menu")(update, context)
    if contains_any(["partners", "partner"]):
        return await partner_handlers.get("menu")(update, context)
    if contains_any(["profile"]):
        return await profile_handlers.get("menu")(update, context)
    if contains_any(["breeding", "breed"]):
        return await breeding_handlers.get("menu")(update, context)
    if contains_any(["inventory", "stock"]):
        return await inventory_handlers.get("menu")(update, context)
    if contains_any(["role", "roles", "workers"]):
        logger.info("Routing text 'roles' to role_handlers['menu']")
        return await role_handlers.get("menu")(update, context)
    if contains_any(["ask", "brot", "ai"]):
        return await aiask_handlers.get("menu")(update, context)

    # ------------------
    # Fallback: ask AI (via aicentral.ask_gpt if available)
    # ------------------
    inline_menu = get_inline_main_menu()

    user_text = text
    if ask_gpt:
        try:
            gpt_prompt = (
                f"User: {user_text}\n\n"
                "Context: This bot helps manage a small farm — be concise and friendly, and suggest using menus/actions if appropriate."
            )
            # ask_gpt is expected to be async and return a string
            bot_reply = await ask_gpt(gpt_prompt, system_prompt="You are FarmBot, a helpful assistant for a small farm.")
        except Exception:
            logger.exception("AI call failed")
            bot_reply = "⚠️ Sorry — I couldn't reach the AI service right now. Try again later."
    else:
        bot_reply = (
            "I didn't understand that. Choose an action quickly (use the keyboard or /help)."
        )

    # Optionally log conversation to Supabase (if configured)
    if supabase:
        try:
            supabase.table("messages").insert({
                "telegram_id": telegram_id,
                "user_message": user_text,
                "bot_reply": bot_reply,
            }).execute()
        except Exception:
            logger.exception("Failed to save message to Supabase")

    await message.reply_text(bot_reply, reply_markup=inline_menu)


# ------------------------
# BUTTON HANDLER
# ------------------------
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        logger.warning("Received empty callback query")
        return
    data = query.data or ""
    telegram_id = update.effective_user.id
    logger.info("Received callback from telegram_id=%s: %s (available role_handlers: %s)", telegram_id, data, list(role_handlers.keys()))
    await query.answer()

    if data == "skip":
        _clear_flow_keys(context.user_data)
        try:
            await query.edit_message_text("⏭️ Skipped. Use the menu or left keyboard for next action.")
        except Exception:
            logger.exception("Failed to edit message for skip")
        return

    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in button_callback for telegram_id=%s", telegram_id)
        await query.edit_message_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        await query.edit_message_text("❌ You must register first. Use /start.")
        return

    prefix, _, action = data.partition(":")
    logger.info("Parsed callback: prefix=%s, action=%s", prefix, action)

    try:
        routers = {
            "animal": animal_handlers,
            "milk": milk_handlers,
            "money": money_handlers,
            "partner": partner_handlers,
            "profile": profile_handlers,
            "breeding": breeding_handlers,
            "inventory": inventory_handlers,
            "role": role_handlers,
            "roles": role_handlers,
            "aiask": aiask_handlers,
        }
        handlers = routers.get(prefix)
        if not handlers:
            logger.error("No handler found for prefix=%s in callback=%s", prefix, data)
            await query.edit_message_text("⚠️ Invalid action. Try the main menu.", reply_markup=get_inline_main_menu())
            return
        if "router" in handlers:
            logger.info("Routing to %s router with action=%s", prefix, action)
            await handlers["router"](update, context, action)
            return
        # Check for multi-part actions (e.g., generate:manager)
        action_parts = action.split(":")
        base_action = action_parts[0]
        if base_action in handlers:
            logger.info("Routing to %s handler for base_action=%s with full action=%s", prefix, base_action, action)
            await handlers[base_action](update, context, action=action)
            return
        logger.error("Action=%s (base_action=%s) not found in %s handlers: %s", action, base_action, prefix, list(handlers.keys()))
        await query.edit_message_text("Action not recognized. Try the main menu.", reply_markup=get_inline_main_menu())
    except Exception as e:
        logger.exception("Error routing callback=%s: %s", data, e)
        await query.edit_message_text("❌ An error occurred. Try the main menu.", reply_markup=get_inline_main_menu())


# ------------------------
# MAIN ENTRY (async)
# ------------------------
async def main():
    if not TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set in environment")
        return

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("roles", cmd_roles))
    app.add_handler(CommandHandler("ask", cmd_ask))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_error_handler(error_handler)

    # Register slash commands
    await app.bot.set_my_commands(
        [
            BotCommand("start", "Start or register with FarmBot"),
            BotCommand("help", "Show help and commands"),
            BotCommand("roles", "Open Role Management"),
            BotCommand("ask", "Ask BROT the AI"),
        ]
    )

    logger.info("Starting FarmBot...")
    await app.run_polling()


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            logger.warning("Event loop already running, using existing loop")
            loop.create_task(main())
            loop.run_forever()
        else:
            asyncio.run(main())
    except RuntimeError as e:
        if "event loop is already running" in str(e):
            logger.warning("Event loop already running, applying nest_asyncio")
            import nest_asyncio

            nest_asyncio.apply()
            asyncio.run(main())
        else:
            raise
'''







'''import os
import re
import asyncio
import logging
from dotenv import load_dotenv
from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from telegram.error import NetworkError

# Apply nest_asyncio early to handle nested event loops
try:
    import nest_asyncio
    nest_asyncio.apply()
except ImportError:
    print("Warning: nest_asyncio not installed. Install with 'pip install nest_asyncio' if running in a nested loop environment.")

from keyboard import get_side_reply_keyboard, get_inline_main_menu
from aboutanimal import animal_handlers
from aboutmilk import milk_handlers
from aboutmoney import money_handlers
from partners import partner_handlers
from profile import profile_handlers
from aboutrole import role_handlers
from aboutbreeding import breeding_handlers
from aboutinventory import inventory_handlers

from farmcore import (
    async_get_user_by_telegram,
    async_register_user,
)

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

def _clear_flow_keys(context_user_data: dict):
    """Clear flow-related keys for safety."""
    for k in list(context_user_data.keys()):
        if k.startswith(
            (
                "flow",
                "animal",
                "milk",
                "money",
                "register",
                "breeding",
                "inventory",
                "profile",
                "role",
            )
        ):
            context_user_data.pop(k, None)

# ------------------------
# ERROR HANDLER
# ------------------------
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Update caused error: %s", context.error)
    if isinstance(context.error, NetworkError):
        if update and getattr(update, "effective_message", None):
            try:
                await update.effective_message.reply_text(
                    "⚠️ Temporary network issue. Please try again."
                )
            except Exception:
                pass
    else:
        if update and getattr(update, "effective_message", None):
            try:
                await update.effective_message.reply_text(
                    "❌ An error occurred. Please try again later."
                )
            except Exception:
                pass

# ------------------------
# START / REGISTRATION
# ------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        await update.effective_message.reply_text("⚠️ Unable to read user info.")
        return
    telegram_id = user.id

    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in /start")
        await update.message.reply_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        context.user_data["register_flow"] = "name"
        await update.message.reply_text(
            "👋 Welcome to FarmBot!\n\n"
            "Before using the app, let's register your account.\n"
            "👉 Please enter your *full name* to continue:",
            parse_mode="Markdown",
        )
        return

    reply_kb = get_side_reply_keyboard()
    inline_menu = get_inline_main_menu()
    await update.message.reply_text(
        "🔵 Welcome back to FarmBot! Use the keyboard or quick actions below.",
        reply_markup=reply_kb,
    )
    await update.message.reply_text("Quick actions:", reply_markup=inline_menu)

# ------------------------
# HELP and ROLES commands
# ------------------------
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👋 *FarmBot help*\n\n"
        "/start - Start or register\n"
        "/help - Show this help\n"
        "/roles - Open Role Management menu\n\n"
        "You can also use the quick keyboard or inline buttons for actions."
    )
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown")
    elif update.callback_query:
        await update.callback_query.message.reply_text(text, parse_mode="Markdown")

async def cmd_roles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("Received /roles command from telegram_id=%s", update.effective_user.id)
    menu_fn = role_handlers.get("menu")
    if menu_fn:
        await menu_fn(update, context)
    else:
        logger.error("Role menu handler not found. Available handlers: %s", list(role_handlers.keys()))
        if update.message:
            await update.message.reply_text("⚠️ Roles menu is unavailable right now.")
        else:
            await update.callback_query.message.reply_text(
                "⚠️ Roles menu is unavailable right now."
            )

# ------------------------
# MESSAGE HANDLER
# ------------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message:
        return

    text = (message.text or "").strip()
    text_lower = text.lower()
    normalized = re.sub(r"[^\w\s]", "", text_lower).strip()  # Confirmed correct regex
    words = normalized.split()

    telegram_id = update.effective_user.id
    logger.info("Received message from telegram_id=%s: %s", telegram_id, text)

    # Registration flow
    register_flow = context.user_data.get("register_flow")
    if register_flow == "name":
        context.user_data["register_name"] = text
        context.user_data["register_flow"] = "farm_name"
        await message.reply_text("🏡 Great! Now enter your *farm name*:", parse_mode="Markdown")
        return

    elif register_flow == "farm_name":
        name = context.user_data.get("register_name")
        farm_name = text

        if not name:
            context.user_data["register_flow"] = "name"
            await message.reply_text("⚠️ I didn't catch your name. Please enter your full name:")
            return

        try:
            result = await async_register_user(
                telegram_id=telegram_id, name=name, farm_name=farm_name, timezone="UTC"
            )
            if result.get("error"):
                await message.reply_text("⚠️ Failed to register. Try again later.")
            else:
                context.user_data.pop("register_flow", None)
                context.user_data.pop("register_name", None)
                await message.reply_text(
                    f"✅ Registered successfully, {name}!\n"
                    f"Your farm '{farm_name}' is now set up."
                )
                reply_kb = get_side_reply_keyboard()
                inline_menu = get_inline_main_menu()
                await message.reply_text("Main menu:", reply_markup=reply_kb)
                await message.reply_text("Quick actions:", reply_markup=inline_menu)
        except Exception:
            logger.exception("Failed to register user telegram_id=%s", telegram_id)
            await message.reply_text("⚠️ Failed to register. Try again later.")
        return

    # Ensure user is registered
    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in handle_message for telegram_id=%s", telegram_id)
        await message.reply_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        await message.reply_text("❌ You must register first. Use /start.")
        return

    # Flow routing
    flow = context.user_data.get("flow", "")
    if flow:
        handler_map = {
            "animal": animal_handlers,
            "milk": milk_handlers,
            "money": money_handlers,
            "profile": profile_handlers,
            "breeding": breeding_handlers,
            "inventory": inventory_handlers,
            "role": role_handlers,
            "roles": role_handlers,  # Added alias for potential callback mismatch
        }
        for prefix, handlers in handler_map.items():
            if flow.startswith(prefix):
                handler = handlers.get("handle_text")
                if handler:
                    logger.info("Routing flow=%s to handler %s", flow, prefix)
                    await handler(update, context)
                    return

    def contains_any(cands):
        return any(c in words or c in normalized for c in cands)

    if contains_any(["animals", "animal"]):
        return await animal_handlers.get("menu")(update, context)
    if contains_any(["milk"]):
        return await milk_handlers.get("menu")(update, context)
    if contains_any(["finance", "money"]):
        return await money_handlers.get("menu")(update, context)
    if contains_any(["partners", "partner"]):
        return await partner_handlers.get("menu")(update, context)
    if contains_any(["profile"]):
        return await profile_handlers.get("menu")(update, context)
    if contains_any(["breeding", "breed"]):
        return await breeding_handlers.get("menu")(update, context)
    if contains_any(["inventory", "stock"]):
        return await inventory_handlers.get("menu")(update, context)
    if contains_any(["role", "roles", "workers"]):
        logger.info("Routing text 'roles' to role_handlers['menu']")
        return await role_handlers.get("menu")(update, context)

    inline_menu = get_inline_main_menu()
    await message.reply_text("I didn't understand that. Choose an action quickly:", reply_markup=inline_menu)

# ------------------------
# BUTTON HANDLER
# ------------------------
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        logger.warning("Received empty callback query")
        return
    data = query.data or ""
    telegram_id = update.effective_user.id
    logger.info("Received callback from telegram_id=%s: %s (available role_handlers: %s)", telegram_id, data, list(role_handlers.keys()))
    await query.answer()

    if data == "skip":
        _clear_flow_keys(context.user_data)
        try:
            await query.edit_message_text("⏭️ Skipped. Use the menu or left keyboard for next action.")
        except Exception:
            logger.exception("Failed to edit message for skip")
        return

    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in button_callback for telegram_id=%s", telegram_id)
        await query.edit_message_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        await query.edit_message_text("❌ You must register first. Use /start.")
        return

    prefix, _, action = data.partition(":")
    logger.info("Parsed callback: prefix=%s, action=%s", prefix, action)

    try:
        routers = {
            "animal": animal_handlers,
            "milk": milk_handlers,
            "money": money_handlers,
            "partner": partner_handlers,
            "profile": profile_handlers,
            "breeding": breeding_handlers,
            "inventory": inventory_handlers,
            "role": role_handlers,
            "roles": role_handlers,  # Added alias for potential callback mismatch
        }
        handlers = routers.get(prefix)
        if not handlers:
            logger.error("No handler found for prefix=%s in callback=%s", prefix, data)
            await query.edit_message_text("⚠️ Invalid action. Try the main menu.", reply_markup=get_inline_main_menu())
            return
        if "router" in handlers:
            logger.info("Routing to %s router with action=%s", prefix, action)
            await handlers["router"](update, context, action)
            return
        if action in handlers:
            logger.info("Routing to %s handler for action=%s", prefix, action)
            await handlers[action](update, context)
            return
        logger.error("Action=%s not found in %s handlers: %s", action, prefix, list(handlers.keys()))
        await query.edit_message_text("Action not recognized. Try the main menu.", reply_markup=get_inline_main_menu())
    except Exception as e:
        logger.exception("Error routing callback=%s: %s", data, e)
        await query.edit_message_text("❌ An error occurred. Try the main menu.", reply_markup=get_inline_main_menu())

# ------------------------
# MAIN ENTRY (async)
# ------------------------
async def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("roles", cmd_roles))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_error_handler(error_handler)

    # Register slash commands
    await app.bot.set_my_commands(
        [
            BotCommand("start", "Start or register with FarmBot"),
            BotCommand("help", "Show help and commands"),
            BotCommand("roles", "Open Role Management"),
        ]
    )

    logger.info("Starting FarmBot...")
    await app.run_polling()

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            logger.warning("Event loop already running, using existing loop")
            loop.create_task(main())
            loop.run_forever()
        else:
            asyncio.run(main())
    except RuntimeError as e:
        if "event loop is already running" in str(e):
            logger.warning("Event loop already running, applying nest_asyncio")
            import nest_asyncio
            nest_asyncio.apply()
            asyncio.run(main())
        else:
            raise
'''








'''
# main.py (with ROLE support and slash commands, async-safe)
import os
import re
import asyncio
import logging
from dotenv import load_dotenv
from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from telegram.error import NetworkError

from keyboard import get_side_reply_keyboard, get_inline_main_menu
from aboutanimal import animal_handlers
from aboutmilk import milk_handlers
from aboutmoney import money_handlers
from partners import partner_handlers
from profile import profile_handlers
from aboutrole import role_handlers
from aboutbreeding import breeding_handlers
from aboutinventory import inventory_handlers

from farmcore import (
    async_get_user_by_telegram,
    async_register_user,
)

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def _clear_flow_keys(context_user_data: dict):
    """Clear flow-related keys for safety."""
    for k in list(context_user_data.keys()):
        if k.startswith(
            (
                "flow",
                "animal",
                "milk",
                "money",
                "register",
                "breeding",
                "inventory",
                "profile",
                "role",
            )
        ):
            context_user_data.pop(k, None)


# ------------------------
# ERROR HANDLER
# ------------------------
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Update caused error: %s", context.error)
    if isinstance(context.error, NetworkError):
        if update and getattr(update, "effective_message", None):
            try:
                await update.effective_message.reply_text(
                    "⚠️ Temporary network issue. Please try again."
                )
            except Exception:
                pass
    else:
        if update and getattr(update, "effective_message", None):
            try:
                await update.effective_message.reply_text(
                    "❌ An error occurred. Please try again later."
                )
            except Exception:
                pass


# ------------------------
# START / REGISTRATION
# ------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        await update.effective_message.reply_text("⚠️ Unable to read user info.")
        return
    telegram_id = user.id

    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in /start")
        await update.message.reply_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        context.user_data["register_flow"] = "name"
        await update.message.reply_text(
            "👋 Welcome to FarmBot!\n\n"
            "Before using the app, let's register your account.\n"
            "👉 Please enter your *full name* to continue:",
            parse_mode="Markdown",
        )
        return

    reply_kb = get_side_reply_keyboard()
    inline_menu = get_inline_main_menu()
    await update.message.reply_text(
        "🔵 Welcome back to FarmBot! Use the keyboard or quick actions below.",
        reply_markup=reply_kb,
    )
    await update.message.reply_text("Quick actions:", reply_markup=inline_menu)


# ------------------------
# HELP and ROLES commands
# ------------------------
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👋 *FarmBot help*\n\n"
        "/start - Start or register\n"
        "/help - Show this help\n"
        "/roles - Open Role Management menu\n\n"
        "You can also use the quick keyboard or inline buttons for actions."
    )
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown")
    elif update.callback_query:
        await update.callback_query.message.reply_text(text, parse_mode="Markdown")


async def cmd_roles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    menu_fn = role_handlers.get("menu")
    if menu_fn:
        await menu_fn(update, context)
    else:
        if update.message:
            await update.message.reply_text("⚠️ Roles menu is unavailable right now.")
        else:
            await update.callback_query.message.reply_text(
                "⚠️ Roles menu is unavailable right now."
            )


# ------------------------
# MESSAGE HANDLER
# ------------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message:
        return

    text = (message.text or "").strip()
    text_lower = text.lower()
    normalized = re.sub(r"[^\w\s]", "", text_lower).strip()
    words = normalized.split()

    telegram_id = update.effective_user.id

    # Registration flow
    register_flow = context.user_data.get("register_flow")
    if register_flow == "name":
        context.user_data["register_name"] = text
        context.user_data["register_flow"] = "farm_name"
        await message.reply_text("🏡 Great! Now enter your *farm name*:", parse_mode="Markdown")
        return

    elif register_flow == "farm_name":
        name = context.user_data.get("register_name")
        farm_name = text

        if not name:
            context.user_data["register_flow"] = "name"
            await message.reply_text("⚠️ I didn't catch your name. Please enter your full name:")
            return

        try:
            result = await async_register_user(
                telegram_id=telegram_id, name=name, farm_name=farm_name, timezone="UTC"
            )
            if result.get("error"):
                await message.reply_text("⚠️ Failed to register. Try again later.")
            else:
                context.user_data.pop("register_flow", None)
                context.user_data.pop("register_name", None)
                await message.reply_text(
                    f"✅ Registered successfully, {name}!\n"
                    f"Your farm '{farm_name}' is now set up."
                )
                reply_kb = get_side_reply_keyboard()
                inline_menu = get_inline_main_menu()
                await message.reply_text("Main menu:", reply_markup=reply_kb)
                await message.reply_text("Quick actions:", reply_markup=inline_menu)
        except Exception:
            await message.reply_text("⚠️ Failed to register. Try again later.")
        return

    # Ensure user is registered
    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        await message.reply_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        await message.reply_text("❌ You must register first. Use /start.")
        return

    # Flow routing
    flow = context.user_data.get("flow", "")
    if flow:
        handler_map = {
            "animal": animal_handlers,
            "milk": milk_handlers,
            "money": money_handlers,
            "profile": profile_handlers,
            "breeding": breeding_handlers,
            "inventory": inventory_handlers,
            "role": role_handlers,
        }
        for prefix, handlers in handler_map.items():
            if flow.startswith(prefix):
                handler = handlers.get("handle_text")
                if handler:
                    await handler(update, context)
                    return

    def contains_any(cands):
        return any(c in words or c in normalized for c in cands)

    if contains_any(["animals", "animal"]):
        return await animal_handlers.get("menu")(update, context)
    if contains_any(["milk"]):
        return await milk_handlers.get("menu")(update, context)
    if contains_any(["finance", "money"]):
        return await money_handlers.get("menu")(update, context)
    if contains_any(["partners", "partner"]):
        return await partner_handlers.get("menu")(update, context)
    if contains_any(["profile"]):
        return await profile_handlers.get("menu")(update, context)
    if contains_any(["breeding", "breed"]):
        return await breeding_handlers.get("menu")(update, context)
    if contains_any(["inventory", "stock"]):
        return await inventory_handlers.get("menu")(update, context)
    if contains_any(["role", "roles", "workers"]):
        return await role_handlers.get("menu")(update, context)

    inline_menu = get_inline_main_menu()
    await message.reply_text("I didn't understand that. Choose an action quickly:", reply_markup=inline_menu)


# ------------------------
# BUTTON HANDLER
# ------------------------
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    await query.answer()

    if data == "skip":
        _clear_flow_keys(context.user_data)
        try:
            await query.edit_message_text("⏭️ Skipped. Use the menu or left keyboard for next action.")
        except Exception:
            pass
        return

    telegram_id = update.effective_user.id
    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        await query.edit_message_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        await query.edit_message_text("❌ You must register first. Use /start.")
        return

    prefix, _, action = data.partition(":")

    try:
        routers = {
            "animal": animal_handlers,
            "milk": milk_handlers,
            "money": money_handlers,
            "partner": partner_handlers,
            "profile": profile_handlers,
            "breeding": breeding_handlers,
            "inventory": inventory_handlers,
            "role": role_handlers,
        }
        handlers = routers.get(prefix)
        if not handlers:
            return
        if "router" in handlers:
            await handlers["router"](update, context, action)
            return
        if action in handlers:
            await handlers[action](update, context)
            return
    except Exception:
        logger.exception("Error routing callback")

    try:
        await query.edit_message_text("Action not recognized. Try the main menu.")
    except Exception:
        pass


# ------------------------
# MAIN ENTRY (async)
# ------------------------
async def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("roles", cmd_roles))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_error_handler(error_handler)

    # Register slash commands
    await app.bot.set_my_commands(
        [
            BotCommand("start", "Start or register with FarmBot"),
            BotCommand("help", "Show help and commands"),
            BotCommand("roles", "Open Role Management"),
        ]
    )

    logger.info("Starting FarmBot...")
    await app.run_polling()


if __name__ == "__main__":
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("roles", cmd_roles))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_error_handler(error_handler)

    # Register slash commands
    app.bot.set_my_commands(
        [
            BotCommand("start", "Start or register with FarmBot"),
            BotCommand("help", "Show help and commands"),
            BotCommand("roles", "Open Role Management"),
        ]
    )

    logger.info("Starting FarmBot...")
    app.run_polling()   # <-- no await, no asyncio.run()
'''


'''
# main.py (fixed) the last without ROLE
import os
import logging
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.error import NetworkError, TelegramError

from keyboard import get_side_reply_keyboard, get_inline_main_menu
from aboutanimal import animal_handlers
from aboutmilk import milk_handlers
from aboutmoney import money_handlers
from partners import partner_handlers
from profile import profile_handlers

from aboutbreeding import breeding_handlers
from aboutinventory import inventory_handlers

from farmcore import (
    supabase,
    async_get_user_by_telegram,
    async_register_user,
    async_get_user_with_farm_by_telegram,
)

load_dotenv()
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def _clear_flow_keys(context_user_data: dict):
    # include breeding/inventory and be more robust
    for k in list(context_user_data.keys()):
        if k.startswith(("flow", "animal", "milk", "money", "register", "breeding", "inventory", "profile")):
            context_user_data.pop(k, None)

# ------------------------
# ERROR HANDLER
# ------------------------
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors, especially network issues, and retry where appropriate."""
    logger.error("Update caused error: %s", context.error)
    if isinstance(context.error, NetworkError):
        logger.warning("Network error encountered: %s. Retrying...", context.error)
        # Optionally notify user
        if update and getattr(update, "effective_message", None):
            try:
                await update.effective_message.reply_text("⚠️ Temporary network issue. Please try again.")
            except Exception:
                logger.warning("Failed to notify user of network error")
    else:
        logger.exception("Unhandled error: %s", context.error)
        if update and getattr(update, "effective_message", None):
            try:
                await update.effective_message.reply_text("❌ An error occurred. Please try again later.")
            except Exception:
                pass

# ------------------------
# START / REGISTRATION
# ------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        await update.effective_message.reply_text("⚠️ Unable to read user info.")
        return
    telegram_id = user.id

    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in /start")
        await update.message.reply_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        # New user → begin registration flow
        context.user_data['register_flow'] = 'name'
        await update.message.reply_text(
            "👋 Welcome to FarmBot!\n\n"
            "Before using the app, let's register your account.\n"
            "👉 Please enter your *full name* to continue:",
            parse_mode="Markdown"
        )
        return

    # Returning user → normal welcome
    reply_kb = get_side_reply_keyboard()
    inline_menu = get_inline_main_menu()
    await update.message.reply_text(
        "🔵 Welcome back to FarmBot! Use the keyboard or quick actions below.",
        reply_markup=reply_kb
    )
    await update.message.reply_text("Quick actions:", reply_markup=inline_menu)

# ------------------------
# MESSAGE HANDLER
# ------------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message:
        return

    text = (message.text or "").strip()
    text_lower = text.lower()  # ensure defined early
    telegram_id = update.effective_user.id

    # Check registration flow first (typing during registration)
    register_flow = context.user_data.get('register_flow')

    if register_flow == 'name':
        context.user_data['register_name'] = text
        context.user_data['register_flow'] = 'farm_name'
        await message.reply_text(
            "🏡 Great! Now enter your *farm name*:",
            parse_mode="Markdown"
        )
        return

    elif register_flow == 'farm_name':
        name = context.user_data.get('register_name')
        farm_name = text

        if not name:
            # fallback: ask for name again
            context.user_data['register_flow'] = 'name'
            await message.reply_text("⚠️ I didn't catch your name. Please enter your full name:")
            return

        try:
            # Use async register helper which creates user + farm
            result = await async_register_user(telegram_id=telegram_id, name=name, farm_name=farm_name, timezone="UTC")
            if result.get("error"):
                logger.error("Registration error: %s", result.get("error"))
                await message.reply_text("⚠️ Failed to register. Try again later.")
            else:
                # Clear registration data
                context.user_data.pop('register_flow', None)
                context.user_data.pop('register_name', None)

                await message.reply_text(
                    f"✅ Registered successfully, {name}!\n"
                    f"Your farm '{farm_name}' is now set up."
                )

                reply_kb = get_side_reply_keyboard()
                inline_menu = get_inline_main_menu()
                await message.reply_text("Main menu:", reply_markup=reply_kb)
                await message.reply_text("Quick actions:", reply_markup=inline_menu)

        except Exception:
            logger.exception("Failed registration")
            await message.reply_text("⚠️ Failed to register. Try again later.")
        return

    # Check if user is registered (DB)
    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error while checking user")
        await message.reply_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        await message.reply_text("❌ You must register first. Use /start.")
        return

    # If a flow is active, give flow handlers priority
    flow = context.user_data.get('flow', '')
    if flow:
        if flow.startswith('animal'):
            handler = animal_handlers.get('handle_text')
            if handler:
                await handler(update, context)
                return
        if flow.startswith('milk'):
            handler = milk_handlers.get('handle_text')
            if handler:
                await handler(update, context)
                return
        if flow.startswith('money'):
            handler = money_handlers.get('handle_text')
            if handler:
                await handler(update, context)
                return
        if flow.startswith('profile'):
            handler = profile_handlers.get('handle_text')
            if handler:
                await handler(update, context)
                return
        if flow.startswith('breeding'):
            handler = breeding_handlers.get('handle_text')
            if handler:
                await handler(update, context)
                return
        if flow.startswith('inventory'):
            handler = inventory_handlers.get('handle_text')
            if handler:
                await handler(update, context)
                return

    # Quick keyword shortcuts (now safe because text_lower is defined)
    if text_lower in ['🐮 animals', 'animals', 'animals 🐮', '🐄 animals']:
        await animal_handlers.get('menu')(update, context)
        return
    if text_lower in ['🥛 milk', 'milk']:
        await milk_handlers.get('menu')(update, context)
        return
    if text_lower in ['💰 finance', 'finance', 'money']:
        await money_handlers.get('menu')(update, context)
        return
    if text_lower in ['🤝 partners', 'partners']:
        await partner_handlers.get('menu')(update, context)
        return
    if text_lower in ['👤 profile', 'profile']:
        await profile_handlers.get('menu')(update, context)
        return
    if text_lower in ['🤰 breeding', 'breeding', 'breeding 🤰', 'breeding events']:
        await breeding_handlers.get('menu')(update, context)
        return
    if text_lower in ['📦 inventory', 'inventory', 'stock']:
        await inventory_handlers.get('menu')(update, context)
        return

    # Fallback
    inline_menu = get_inline_main_menu()
    await message.reply_text(
        "I didn't understand that. Choose an action quickly:",
        reply_markup=inline_menu
    )

# ------------------------
# BUTTON HANDLER
# ------------------------
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    await query.answer()

    # Universal skip: clear flows
    if data == 'skip':
        _clear_flow_keys(context.user_data)
        try:
            await query.edit_message_text("⏭️ Skipped. Use the menu or left keyboard for next action.")
        except Exception:
            pass
        return

    telegram_id = update.effective_user.id

    # Check registration (non-blocking)
    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error while checking user in callback")
        await query.edit_message_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        await query.edit_message_text("❌ You must register first. Use /start.")
        return

    prefix, _, action = data.partition(':')

    try:
        if prefix == 'animal':
            router = animal_handlers.get('router')
            if router:
                await router(update, context, action)
                return
            handler = animal_handlers.get(action)
            if handler:
                await handler(update, context)
                return

        if prefix == 'milk':
            router = milk_handlers.get('router')
            if router:
                await router(update, context, action)
                return
            handler = milk_handlers.get(action)
            if handler:
                await handler(update, context)
                return

        if prefix == 'money':
            router = money_handlers.get('router')
            if router:
                await router(update, context, action)
                return
            handler = money_handlers.get(action)
            if handler:
                await handler(update, context)
                return

        if prefix == 'partner':
            handler = partner_handlers.get(action)
            if handler:
                await handler(update, context)
                return

        if prefix == 'profile':
            handler = profile_handlers.get(action)
            if handler:
                await handler(update, context)
                return

        if prefix == 'breeding':
            router = breeding_handlers.get('router')
            if router:
                await router(update, context, action)
                return
            cb = breeding_handlers.get('callback_router')
            if cb:
                await cb(update, context)
                return

        if prefix == 'inventory':
            router = inventory_handlers.get('router')
            if router:
                await router(update, context, action)
                return

    except Exception:
        logger.exception("Error routing callback")

    try:
        await query.edit_message_text("Action not recognized. Try the main menu.")
    except Exception:
        pass

# ------------------------
# MAIN ENTRY
# ------------------------
def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_error_handler(error_handler)  # Add error handler
    logger.info("Starting FarmBot...")
    app.run_polling()

if __name__ == '__main__':
    main()
'''