from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import ContextTypes
from farmcore import CattleCore
from datetime import datetime


class ReportHandler:
    def __init__(self, core: CattleCore):
        self.core = core

    async def show_reports(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show reports triggered by button press (callback query)."""
        query = update.callback_query
        await self._send_report(query.from_user.id, query.message.reply_text)

    async def show_reports_direct(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show reports triggered by direct command (/reports)."""
        await self._send_report(update.message.from_user.id, update.message.reply_text)

    async def _send_report(self, tg_user_id: int, send_func) -> None:
        """Core reporting logic shared by both entry points."""
        farmer = self.core.get_or_create_farmer(tg_user_id)
        now = datetime.now()

        # Safely fetch financial summary
        financials = self.core.get_monthly_financial_summary(farmer["id"], now.year, now.month) or {}
        income = float(financials.get("income", 0))
        expenses = float(financials.get("expenses", 0))
        profit = float(financials.get("profit", income - expenses))

        report_lines = [
            f"📊 *{now.strftime('%B %Y')} Report*",
            f"💰 Income: ₦{income:,.2f}",
            f"💸 Expenses: ₦{expenses:,.2f}",
            f"📈 Profit: ₦{profit:,.2f}",
            "",
            "🥛 *Milk Production (7 days)*",
        ]

        # Milk production section
        animals = self.core.get_all_animals(farmer["id"]) or []
        if animals:
            for animal in animals:
                milk_data = self.core.get_recent_milk_production(animal["id"])
                if milk_data:
                    avg_yield = sum(float(r["quantity"]) for r in milk_data) / len(milk_data)
                    report_lines.append(f"• {animal['cow_id']}: {avg_yield:.1f}L avg")
                else:
                    report_lines.append(f"• {animal['cow_id']}: No data")
        else:
            report_lines.append("• No animals registered")

        # Feed inventory section
        inventory = self.core.get_feed_inventory(farmer["id"]) or []
        report_lines.append("")
        report_lines.append("📦 *Feed Inventory:*")
        if inventory:
            for item in inventory:
                qty = item.get("quantity", 0)
                ftype = item.get("feed_type", "Unknown")
                report_lines.append(f"• {ftype}: {qty}kg")
        else:
            report_lines.append("• No feed records")

        # Send final report
        await send_func(
            "\n".join(report_lines),
            parse_mode="Markdown",
            reply_markup=self.get_persistent_keyboard()
        )

    def get_persistent_keyboard(self) -> ReplyKeyboardMarkup:
        """Returns a persistent menu keyboard for user navigation."""
        keyboard = [
            [KeyboardButton("🥛 Log Milk"), KeyboardButton("💰 Finances")],
            [KeyboardButton("🐄 Animals"), KeyboardButton("🌾 Feed")],
            [KeyboardButton("📊 Reports"), KeyboardButton("❔ Help")],
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)
