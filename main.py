# main.py
import os
import re
import asyncio
import logging
import types
import inspect
from dotenv import load_dotenv

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

# Telegram imports
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

# Local modules (keep your existing modules as-is)
import paymentcentral
from aboutfeedformula import feed_handlers  # new feed formula handlers

try:
    import nest_asyncio

    nest_asyncio.apply()
except Exception:
    pass

from keyboard import get_side_reply_keyboard, get_inline_main_menu
from aboutanimal import animal_handlers
from aboutmilk import milk_handlers
from aboutmoney import money_handlers
from partners import partner_handlers
from profile import profile_handlers
from aboutrole import role_handlers
from aboutbreeding import breeding_handlers
from aboutinventory import inventory_handlers

# AI connection (same fallbacks as before)
try:
    from aiconnection import aiask_handlers, ask_gpt
    logging.getLogger(__name__).info("Loaded aiconnection package exports: aiask_handlers, ask_gpt")
except Exception:
    try:
        from aiconnection.aiask import aiask_handlers
        from aiconnection.aicentral import ask_gpt
        logging.getLogger(__name__).info("Loaded aiconnection submodules: aiask, aicentral")
    except Exception as e:
        logging.getLogger(__name__).exception("Failed to import aiconnection — AI Ask disabled for now.")
        aiask_handlers = {}
        ask_gpt = None

from farmcore import async_get_user_by_telegram, async_register_user

load_dotenv()

# Environment
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DEBUG_CALLBACK = os.getenv("DEBUG_CALLBACK", "0") == "1"

# Basic logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# FastAPI app (exposed to Render)
app = FastAPI(title="FarmBot web health & bot host")

# Will hold the running Telegram Application and background task
telegram_app: Application | None = None
_bot_task: asyncio.Task | None = None


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


async def _call_maybe_with_action(fn, update, context, action=None):
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
# Telegram handlers (copied/adapted from your code)
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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
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

    if not user_row:
        context.user_data["register_flow"] = "name"
        await update.message.reply_text(
            "👋 Welcome to FarmBot!\n\n"
            "Before using the app, let's register your account.\n"
            "👉 Please enter your *full name* to continue:",
            parse_mode="Markdown",
        )
        return

    reply_keyboard = get_side_reply_keyboard()
    await update.message.reply_text(
        "Please choose an option from the keyboard below 👇",
        reply_markup=reply_keyboard,
    )


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
    menu_fn = None
    if isinstance(aiask_handlers, dict):
        menu_fn = aiask_handlers.get("menu")
    elif hasattr(aiask_handlers, "menu"):
        menu_fn = getattr(aiask_handlers, "menu")
    if menu_fn:
        await _call_maybe_with_action(menu_fn, update, context)
    else:
        if update.message:
            await update.message.reply_text(
                "⚠️ AI Ask is unavailable right now. The bot is running but the AI module failed to load."
            )
        else:
            if update.callback_query and update.callback_query.message:
                await update.callback_query.message.reply_text(
                    "⚠️ AI Ask is unavailable right now. The bot is running but the AI module failed to load."
                )


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
                    f"✅ Registered successfully, {name}!\nYour farm '{farm_name}' is set up."
                )
                await message.reply_text("To open the main menu, please press /start.")
        except Exception:
            logger.exception("Failed to register user telegram_id=%s", telegram_id)
            await message.reply_text("⚠️ Failed to register. Try again later.")
        return

    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in handle_message")
        await message.reply_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        await message.reply_text("❌ You must register first. Use /start.")
        return

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

    user_text = text
    if ask_gpt:
        try:
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

    if isinstance(bot_reply, (list, dict)):
        bot_reply = str(bot_reply)

    await message.reply_text(bot_reply)


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
        "feed": feed_handlers,
        "payment": paymentcentral,
    }
    handlers = routers.get(prefix)
    if not handlers:
        logger.error("No handler found for prefix=%s", prefix)
        await query.edit_message_text("⚠️ Invalid action. Try the main menu.", reply_markup=get_inline_main_menu())
        return

    try:
        called = False
        if isinstance(handlers, dict) and "router" in handlers:
            await _call_maybe_with_action(handlers["router"], update, context, action=action)
            called = True
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
# Build telegram Application and add handlers
# ------------------------
def build_telegram_app() -> Application:
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN env var is not set")

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("roles", cmd_roles))
    app.add_handler(CommandHandler("ask", cmd_ask))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, paymentcentral.handle_successful_payment))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_error_handler(error_handler)

    # Set the bot commands (so when bot starts, users see /start etc.)
    async def set_commands():
        try:
            await app.bot.set_my_commands(
                [
                    BotCommand("start", "Start or register with FarmBot"),
                    BotCommand("help", "Show help and commands"),
                    BotCommand("roles", "Open Role Management"),
                    BotCommand("ask", "Ask BROT the AI"),
                ]
            )
        except Exception:
            logger.exception("Failed to set bot commands")

    # schedule the set_commands coroutine on startup (Application will call this)
    app.post_init = lambda _: asyncio.create_task(set_commands())

    return app


# ------------------------
# FastAPI startup/shutdown events to host the bot
# ------------------------
@app.on_event("startup")
async def startup_event():
    global telegram_app, _bot_task

    if not TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set. Bot will not start.")
        return

    logger.info("FastAPI startup — building telegram app")
    telegram_app = build_telegram_app()

    # run polling in the background as a task (non-blocking for FastAPI)
    logger.info("Starting Telegram polling in background task")
    _bot_task = asyncio.create_task(telegram_app.run_polling())

    # small sleep to let the bot try to start and log anything early
    await asyncio.sleep(0.1)


@app.on_event("shutdown")
async def shutdown_event():
    global telegram_app, _bot_task
    logger.info("FastAPI shutdown — stopping Telegram bot")

    if _bot_task:
        _bot_task.cancel()
        try:
            await _bot_task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Exception while waiting for bot task cancellation")

    if telegram_app:
        try:
            # stop() and shutdown() to allow graceful cleanup
            await telegram_app.stop()
            await telegram_app.shutdown()
        except Exception:
            logger.exception("Exception while stopping telegram app")


# A lightweight health endpoint for Render / load balancers
@app.get("/", response_class=PlainTextResponse)
async def root():
    return "OK - FarmBot web root"

# Optional: a simple health endpoint that checks bot status
@app.get("/health", response_class=PlainTextResponse)
async def health():
    bot_running = _bot_task is not None and not _bot_task.done()
    return f"ok\nbot_running={bot_running}\n"


# Allow running locally with uvicorn for dev:
if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info")
