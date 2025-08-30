from telegram import Update
from telegram.ext import ContextTypes
import jwt
import time

# Secret used to sign JWTs (keep private on your bot server)
SECRET_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFpanBha2p6aWxxZXFnY2prcGRnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTU4NjgxNjMsImV4cCI6MjA3MTQ0NDE2M30.PWk4rP0eKC9uV7w6E3y25pEzY6zzHSWKEnRyXMpdcNs"
FLUTTER_WEB_URL = "https://brotcattle.loca.lt"

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        await update.message.reply_text("⚠️ Could not get your Telegram ID.")
        return

    telegram_id = user.id
    payload = {
        "telegram_id": telegram_id,
        "exp": int(time.time()) + 3600  # JWT valid for 1 hour
    }

    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    url = f"{FLUTTER_WEB_URL}/?token={token}"

    text = (
        "🌐 *Easy Site Access*\n\n"
        "Click below to open your personal site:\n"
        f"[👉 Open Easy Site]({url})"
    )

    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)
    elif update.callback_query and update.callback_query.message:
        await update.callback_query.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)
