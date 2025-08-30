from telegram import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

def get_side_reply_keyboard():
    keyboard = [
        [KeyboardButton("🐮 Animals"), KeyboardButton("🥛 Milk")],
        [KeyboardButton("💰 Finance"), KeyboardButton("🤝 Partners")],
        [KeyboardButton("📦 Inventory"), KeyboardButton("🧾 Feed Formula")],
        [KeyboardButton("🐄 Breeding"), KeyboardButton("👤 Profile")],
        [KeyboardButton("👥 Roles"), KeyboardButton("🌐 Easy Site")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_inline_main_menu():
    keyboard = [
        [InlineKeyboardButton("🐮 Animals", callback_data="animal:menu"),
         InlineKeyboardButton("🥛 Milk", callback_data="milk:menu")],
        [InlineKeyboardButton("📦 Inventory", callback_data="inventory:menu"),
         InlineKeyboardButton("🧾 Feed Formula", callback_data="feed:menu")],
        [InlineKeyboardButton("🐄 Breeding", callback_data="breeding:menu"),
         InlineKeyboardButton("💰 Finance", callback_data="money:menu")],
        [InlineKeyboardButton("🤝 Partners", callback_data="partner:menu"),
         InlineKeyboardButton("👤 Profile", callback_data="profile:menu")],
        [InlineKeyboardButton("👥 Roles", callback_data="role:menu"),
         InlineKeyboardButton("🌐 Easy Site", callback_data="easysite:menu")],
        [InlineKeyboardButton("⏭ Skip", callback_data="skip")]
    ]
    return InlineKeyboardMarkup(keyboard)
