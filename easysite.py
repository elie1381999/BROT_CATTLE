from telegram import Update
from telegram.ext import ContextTypes
import jwt
import time
from datetime import datetime, timedelta

# Use your actual Supabase service role key here
SECRET_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFpanBha2p6aWxxZXFnY2prcGRnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NTg2ODE2MywiZXhwIjoyMDcxNDQ0MTYzfQ.8U3Sauybh8Kme2wjLkhm1D0K9SQ_11pufAoToTDkLTo"  # Replace with your actual key
FLUTTER_WEB_URL = "https://brotcattle.loca.lt"

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        await update.message.reply_text("⚠️ Could not get your Telegram ID.")
        return

    telegram_id = user.id
    
    # Create JWT payload with shorter expiration (5 minutes)
    payload = {
        "sub": str(telegram_id),       # Subject is the user's Telegram ID
        "role": "authenticated",       # Supabase role
        "exp": datetime.utcnow() + timedelta(minutes=5),  # 5 minute expiration
        "aud": "authenticated",        # Audience
        "telegram_id": telegram_id     # Custom claim for easy access in RLS
    }

    # Generate JWT token
    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    url = f"{FLUTTER_WEB_URL}/#token={token}"  # Using hash fragment for security

    text = (
        "🌐 *Easy Site Access*\n\n"
        "Click below to open your personal site:\n"
        f"[👉 Open Easy Site]({url})"
    )

    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)
    elif update.callback_query and update.callback_query.message:
        await update.callback_query.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)
