from telegram import Update
from telegram.ext import ContextTypes

# Change this to your actual Flutter web app URL (deployed link)
FLUTTER_WEB_URL = "https://your-flutter-app-url.web.app"

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        await update.message.reply_text("⚠️ Could not get your Telegram ID.")
        return

    telegram_id = user.id
    url = f"{FLUTTER_WEB_URL}/?id={telegram_id}"

    text = (
        "🌐 *Easy Site Access*\n\n"
        "Click below to open your personal site:\n"
        f"[👉 Open Easy Site]({url})"
    )

    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)
    elif update.callback_query and update.callback_query.message:
        await update.callback_query.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)
