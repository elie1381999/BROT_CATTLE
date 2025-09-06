# main.py
import os
import re
import asyncio
import logging
import types
import inspect
from dotenv import load_dotenv
from urllib.parse import unquote
from easysite import menu as easysite_menu

from fastapi import FastAPI, Request, HTTPException
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

# optional: nest_asyncio may be present for interactive envs; harmless here
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

# AI connection (safe fallback — AI disabled if not installed)
try:
    from aiconnection import aiask_handlers, ask_gpt
    logging.getLogger(__name__).info("Loaded aiconnection package exports: aiask_handlers, ask_gpt")
except Exception:
    try:
        from aiconnection.aiask import aiask_handlers
        from aiconnection.aicentral import ask_gpt
        logging.getLogger(__name__).info("Loaded aiconnection submodules: aiask, aicentral")
    except Exception:
        logging.getLogger(__name__).exception("Failed to import aiconnection — AI Ask disabled for now.")
        aiask_handlers = {}
        ask_gpt = None

from farmcore import async_get_user_by_telegram, async_register_user

load_dotenv()

# Environment
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
# Optional secret path — prefer using this for production. If set, webhook must use this secret.
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip() or None
DEBUG_CALLBACK = os.getenv("DEBUG_CALLBACK", "0") == "1"

# Basic logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Attempt to import promo helper (best-effort)
try:
    from promo_helper import apply_promo_for_user
except Exception:
    apply_promo_for_user = None
    logger.warning("promo_helper.apply_promo_for_user not available; promo link tracking will be disabled until imported.")


# FastAPI app (exposed to Render)
app = FastAPI(title="FarmBot web health & bot host")

# Will hold the running Telegram Application reference
telegram_app: Application | None = None


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

    # --- extract start param early (supports /start <arg> and message text with arg) ---
    start_param = None
    try:
        if context and getattr(context, "args", None):
            if len(context.args) > 0:
                start_param = context.args[0]
        if not start_param and update.message and update.message.text:
            parts = (update.message.text or "").split()
            if len(parts) > 1:
                start_param = parts[1]
    except Exception:
        start_param = None

    promo_code = None
    if start_param and isinstance(start_param, str) and start_param.startswith("promo_"):
        promo_code = start_param.split("_", 1)[1]
    # --- end extraction ---

    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        logger.exception("DB error in /start")
        if update.message:
            await update.message.reply_text("⚠️ Database error. Try again later.")
        return

    if not user_row:
        # preserve promo for post-registration application
        if promo_code:
            context.user_data["promo_code"] = promo_code

        context.user_data["register_flow"] = "name"
        await update.message.reply_text(
            "👋 Welcome to FarmBot!\n\n"
            "Before using the app, let's register your account.\n"
            "👉 Please enter your *full name* to continue:",
            parse_mode="Markdown",
        )
        return

    # Registered user: if there's a promo code, apply it immediately (best-effort)
    if promo_code:
        if apply_promo_for_user:
            try:
                await apply_promo_for_user(telegram_id, promo_code, existing_user=True)
                try:
                    # Inform the user (non-blocking)
                    if update.message:
                        await update.message.reply_text("✅ Promo applied — the referring partner will be notified.")
                except Exception:
                    pass
            except Exception:
                logger.exception("Promo handling in /start failed (non-fatal)")
        else:
            logger.warning("apply_promo_for_user not configured; skipping promo apply")

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
        "🌐 Easy Site": {"menu": easysite_menu},   # 👈 Added
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
                # registration success path
                context.user_data.pop("register_flow", None)
                context.user_data.pop("register_name", None)

                # apply promo if present in context.user_data (preserve and then pop)
                promo_code = context.user_data.pop("promo_code", None)
                if promo_code and apply_promo_for_user:
                    try:
                        await apply_promo_for_user(telegram_id, promo_code, existing_user=False)
                        # inform user (optional)
                        await message.reply_text(f"✅ Registered successfully, {name}!\nPromo applied.")
                    except Exception:
                        logger.exception("Failed to apply promo after registration (non-fatal)")
                        await message.reply_text(f"✅ Registered successfully, {name}!\n(But we couldn't apply promo — it will be retried)")
                else:
                    await message.reply_text(f"✅ Registered successfully, {name}!")

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
        "easysite": {"menu": easysite_menu},   # 👈 Added
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

    app.post_init = lambda _: asyncio.create_task(set_commands())

    return app


# ------------------------
# FastAPI startup/shutdown events to host the bot (non-blocking)
# ------------------------
@app.on_event("startup")
async def startup_event():
    global telegram_app

    if not TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set. Bot will not start.")
        return

    logger.info("FastAPI startup — building telegram app")
    telegram_app = build_telegram_app()

    try:
        logger.info("Initializing Telegram application...")
        # initialize prepares internal resources but does not run a new loop
        await telegram_app.initialize()
        logger.info("Starting Telegram application...")
        # start schedules background tasks on the current loop
        await telegram_app.start()
        logger.info("Telegram application initialized and started")
    except Exception:
        logger.exception("Failed to initialize/start Telegram application")


@app.on_event("shutdown")
async def shutdown_event():
    global telegram_app
    logger.info("FastAPI shutdown — stopping Telegram bot")

    if telegram_app:
        try:
            logger.info("Stopping Telegram application...")
            await telegram_app.stop()
            logger.info("Shutting down Telegram application...")
            await telegram_app.shutdown()
            logger.info("Telegram application stopped and shutdown completed")
        except Exception:
            logger.exception("Exception while stopping telegram app")


# ------------------------
# Webhook endpoint (accepts percent-encoded path segments)
# ------------------------
@app.post("/webhook/{rest_of_path:path}")
async def telegram_webhook(rest_of_path: str, request: Request):
    """
    Accepts incoming Telegram updates at /webhook/<path>.
    If WEBHOOK_SECRET is set in env, the path must match that secret.
    Otherwise the last path segment is decoded and matched against the full bot token.
    This accepts encoded tokens (e.g. %3A).
    """
    # determine the expected secret
    expected = WEBHOOK_SECRET if WEBHOOK_SECRET else TOKEN

    # decode last segment (handles %3A)
    token_in_path = unquote(rest_of_path).split("/")[-1]

    logger.info("Incoming webhook POST path last-segment=%s", token_in_path)

    if expected is None:
        logger.error("No expected webhook secret/token configured")
        raise HTTPException(status_code=500, detail="Server misconfigured")

    if token_in_path != expected:
        logger.warning("Forbidden webhook call (path token mismatch). Received '%s'", token_in_path)
        raise HTTPException(status_code=403, detail="Forbidden (invalid webhook token)")

    # read JSON body
    try:
        data = await request.json()
    except Exception:
        logger.exception("Failed to read JSON body from webhook request")
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # log for debugging
    logger.info("Webhook received update keys: %s", list(data.keys()))

    # ensure telegram_app ready
    if telegram_app is None:
        logger.error("telegram_app is not initialized yet")
        raise HTTPException(status_code=503, detail="Bot not initialized")

    # convert to Update and process
    try:
        try:
            update = Update.de_json(data, telegram_app.bot)
        except Exception:
            update = Update(**data)
    except Exception:
        logger.exception("Failed to build Update object from JSON")
        raise HTTPException(status_code=400, detail="Bad update payload")

    try:
        # process the update using the running application
        await telegram_app.process_update(update)
    except Exception:
        logger.exception("Exception while processing webhook update")
        # return 200 to avoid endless Telegram retries; logs will reveal the error
        return {"ok": False}

    return {"ok": True}


# A lightweight health endpoint for Render / load balancers
@app.get("/", response_class=PlainTextResponse)
async def root():
    return "OK - FarmBot web root"

# Optional: a simple health endpoint that checks bot status
@app.get("/health", response_class=PlainTextResponse)
async def health():
    bot_running = telegram_app is not None
    return f"ok\nbot_present={bot_running}\n"


# Allow running locally with uvicorn for dev:
if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info")
