import re
import logging
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import ContextTypes
from farmcore import CattleCore

logger = logging.getLogger("cattlebot")

class FeedHandler:
    def __init__(self, core: CattleCore):
        self.core = core
        self._PATTERN_FEED = re.compile(
            r"(?:feed\s+)?([a-zA-Z]+)\s+([+-]?\d+\.?\d*)$",
            re.IGNORECASE,
        )

    async def handle_feed_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_states: dict) -> None:
        user_id = update.effective_user.id
        farmer = self.core.get_or_create_farmer(user_id)
        inventory = self.core.get_feed_inventory(farmer["id"])
        inventory_text = "📦 *No feed records.*"
        if inventory:
            inventory_lines = "\n".join(f"• {item['feed_type']}: {item['quantity']}kg" for item in inventory)
            inventory_text = "📦 *Feed Inventory:*\n" + inventory_lines
        await update.effective_message.reply_text(
            f"{inventory_text}\n\n🌾 *Update Feed*\nEnter: Feed Type and Quantity (e.g., Silage 50 or Silage -10).",
            parse_mode="Markdown",
            reply_markup=self.get_persistent_keyboard()
        )
        user_states[user_id] = 40  # STATE_AWAITING_FEED_DATA

    async def process_feed_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
        match = self._PATTERN_FEED.search(text)
        if not match:
            await update.message.reply_text(
                "Invalid feed command. Use 'feed Silage 50'",
                reply_markup=self.get_persistent_keyboard()
            )
            return
        feed_type = match.group(1).capitalize()
        quantity = match.group(2)
        await self.process_feed_data(update, context, f"{feed_type} {quantity}", {})

    async def process_feed_data(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, user_states: dict) -> None:
        user_id = update.message.from_user.id
        farmer = self.core.get_or_create_farmer(user_id)
        parts = text.split()
        if len(parts) < 2:
            await update.message.reply_text("Missing feed details.", reply_markup=self.get_persistent_keyboard())
            return
        feed_type, quantity_raw = parts[0], parts[1]
        try:
            quantity = float(quantity_raw)
        except Exception:
            await update.message.reply_text(
                "Invalid quantity. Enter a number.",
                reply_markup=self.get_persistent_keyboard()
            )
            return
        try:
            self.core.update_feed_inventory(farmer["id"], feed_type, quantity)
            inventory = self.core.get_feed_inventory(farmer["id"])
            current_qty = next((item["quantity"] for item in inventory if item["feed_type"] == feed_type.title()), 0)
        except Exception as e:
            logger.exception("Failed to update feed inventory")
            await update.message.reply_text(
                f"Failed to update feed: {e}",
                reply_markup=self.get_persistent_keyboard()
            )
            return
        alert = " ⚠️ Low stock!" if float(current_qty) < 100 else ""
        await update.message.reply_text(
            f"Updated: {feed_type.title()} now {current_qty}kg.{alert}",
            reply_markup=self.get_persistent_keyboard()
        )
        user_states[user_id] = 0  # STATE_IDLE

    def get_persistent_keyboard(self) -> ReplyKeyboardMarkup:
        keyboard = [
            [KeyboardButton("🥛 Log Milk"), KeyboardButton("💰 Finances")],
            [KeyboardButton("🐄 Animals"), KeyboardButton("🌾 Feed")],
            [KeyboardButton("📊 Reports"), KeyboardButton("❔ Help")],
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)