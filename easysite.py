import jwt
import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

# !!! IMPORTANT !!!
# Get this from your Supabase project settings -> API -> service_role secret
# NEVER commit this to public GitHub repositories! Use environment variables.
SUPABASE_JWT_SECRET = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFpanBha2p6aWxxZXFnY2prcGRnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NTg2ODE2MywiZXhwIjoyMDcxNDQ0MTYzfQ.8U3Sauybh8Kme2wjLkhm1D0K9SQ_11pufAoToTDkLTo"

def generate_milk_jwt(telegram_user_id):
    """Generates a JWT token for the Flutter app with the user's Telegram ID."""
    
    # Set the expiration time (e.g., 10 minutes from now)
    expiration = datetime.datetime.utcnow() + datetime.timedelta(minutes=10)
    
    # Create the JWT payload (the data inside the token)
    payload = {
        "sub": str(telegram_user_id),  # 'sub' is a standard JWT claim for subject
        "telegram_id": telegram_user_id,  # Our custom claim for easy access in RLS
        "exp": expiration,  # Standard expiration time claim
        "iss": "your-telegram-bot",  # Optional: Issuer of the token
    }
    
    # Generate the JWT using the Supabase secret
    token = jwt.encode(
        payload, 
        SUPABASE_JWT_SECRET, 
        algorithm="HS256"  # HMAC-SHA256, a common algorithm for shared secrets
    )
    
    return token

async def milk_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /milk command from the user."""
    user = update.effective_user
    
    # 1. Generate the secure JWT for this user
    auth_token = generate_milk_jwt(user.id)
    
    # 2. Create the URL for the Flutter app, including the token
    # URL-encode the token as it can contain special characters
    from urllib.parse import quote
    encoded_token = quote(auth_token)
    flutter_app_url = f"https://your-flutter-app.com/?token={encoded_token}"
    
    # 3. Send the URL to the user inside Telegram
    keyboard = [[InlineKeyboardButton("🐄 Open Milk Tracker", url=flutter_app_url)]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "Click the button below to securely open the milk tracker. This link will expire in 10 minutes.",
        reply_markup=reply_markup
    )
