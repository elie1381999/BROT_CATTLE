import os
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ContextTypes
import jwt
from datetime import datetime, timedelta

# Load .env variables
load_dotenv()

# Now read from env
SECRET_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
FLUTTER_WEB_URL = os.getenv("FLUTTER_WEB_URL")

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        await update.message.reply_text("⚠️ Could not get your Telegram ID.")
        return

    telegram_id = user.id
    
    payload = {
        "sub": str(telegram_id),
        "role": "authenticated",
        "exp": datetime.utcnow() + timedelta(minutes=5),
        "aud": "authenticated",
        "telegram_id": telegram_id
    }

    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    url = f"{FLUTTER_WEB_URL}/#token={token}"

    text = (
        "🌐 *Easy Site Access*\n\n"
        "Click below to open your personal site:\n"
        f"[👉 Open Easy Site]({url})"
    )

    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)
    elif update.callback_query and update.callback_query.message:
        await update.callback_query.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)
