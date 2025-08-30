from telegram import Update
from telegram.ext import ContextTypes
import jwt
import time

SECRET_KEY = "M17byWm43SFabxTkgLxv03ycSDVuC+QN0annqmuvLN/+DKSHiCFYeTnF9dPIkJYXVY3xXtS/AhS6zDo5J0haHA=="  # keep this private
FLUTTER_WEB_URL = "https://brotcattle.loca.lt"

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        await update.message.reply_text("⚠️ Could not get your Telegram ID.")
        return

    telegram_id = user.id
    payload = {
        "telegram_id": telegram_id,
        "exp": int(time.time()) + 3600  # 1 hour expiry
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
