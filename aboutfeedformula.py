# aboutfeedformula.py
import logging
import re
import math
from typing import Optional

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes

import farmcore_feedformula as fcore
from farmcore import async_get_user_by_telegram

LOG = logging.getLogger(__name__)

feed_handlers = {}

# Top-level menu (shows the buttons you asked for)
async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Show first screen with:
      Magic feed formula | Add my formula | Calculate formula | Report or analyse | Back
    """
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔮 Magic feed formula", callback_data="feed:magic")],
        [InlineKeyboardButton("➕ Add my formula", callback_data="feed:add")],
        [InlineKeyboardButton("🧮 Calculate formula", callback_data="feed:calculate")],
        [InlineKeyboardButton("📊 Report or analyse", callback_data="feed:report")],
        [InlineKeyboardButton("🔙 Back", callback_data="skip")]
    ])

    text = "🧾 *Feed Formula*\n\nChoose an action:"
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
        except Exception:
            await update.effective_message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
    else:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)

feed_handlers["menu"] = menu

# Router for callbacks
async def router(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str = ""):
    query = update.callback_query
    if not query:
        return
    await query.answer()
    telegram_id = update.effective_user.id

    # Magic generator (placeholder)
    if action == "magic" or action == "":
        text = (
            "🔮 *Magic feed formula*\n\n"
            "This feature helps generate a suggested formula based on a target nutrient (e.g. protein %).\n\n"
            "Currently it's a simple helper. Send the desired crude protein percent (e.g. `16` for 16%).\n\n"
            "Or press Back."
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="feed:menu")]])
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
        # set flow to receive a number
        context.user_data["flow"] = "feed_magic"
        return

    # Add my formula (start flow)
    if action == "add":
        text = (
            "➕ *Add my formula*\n\n"
            "Please send the *formula name* (e.g. `Lactation Mix A`)."
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="feed:menu")]])
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
        context.user_data["flow"] = "feed_add_name"
        return

    # Calculate -> list formulas for user
    if action == "calculate":
        # list formulas
        formulas = await fcore.async_list_formulas_by_user(telegram_id)
        if not formulas:
            await query.edit_message_text("ℹ️ You have no saved formulas. Use 'Add my formula' first.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="feed:menu")]]))
            return
        kb = []
        for f in formulas:
            kb.append([InlineKeyboardButton(f"{f.get('name')}", callback_data=f"feed:calc_pick:{f.get('id')}")])
        kb.append([InlineKeyboardButton("⬅️ Back", callback_data="feed:menu")])
        await query.edit_message_text("Select a formula to calculate:", reply_markup=InlineKeyboardMarkup(kb))
        return

    # Report -> list formulas then show brief summary (default 100 kg)
    if action == "report":
        formulas = await fcore.async_list_formulas_by_user(telegram_id)
        if not formulas:
            await query.edit_message_text("ℹ️ You have no saved formulas. Use 'Add my formula' first.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="feed:menu")]]))
            return
        kb = []
        for f in formulas:
            kb.append([InlineKeyboardButton(f"{f.get('name')}", callback_data=f"feed:report_show:{f.get('id')}")])
        kb.append([InlineKeyboardButton("⬅️ Back", callback_data="feed:menu")])
        await query.edit_message_text("Select a formula to view report (100 kg default):", reply_markup=InlineKeyboardMarkup(kb))
        return

    # When user picks a formula to calculate
    if action.startswith("calc_pick:"):
        _, fid = action.split(":", 1)
        # ask for target amount
        context.user_data["flow"] = f"feed_calc_amount:{fid}"
        await query.edit_message_text("Enter target total weight in kg (e.g. `100`):", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="feed:calculate")]]))
        return

    # When user picks report show
    if action.startswith("report_show:"):
        _, fid = action.split(":", 1)
        res = await fcore.async_calculate_formula(fid, target_kg=100.0)
        if not res:
            await query.edit_message_text("⚠️ Failed to generate report for this formula.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="feed:report")]]))
            return
        # build text summary
        lines = [f"📊 Report for 100 kg"]
        lines.append(f"Total cost: {res.get('total_cost')}")
        lines.append("Components:")
        for c in res.get("components", []):
            lines.append(f" - {c['name']}: {c['weight_kg']} kg — cost {c['cost']}")
        lines.append("\nNutrients (kg):")
        for k, v in res.get("nutrients", {}).items():
            lines.append(f" - {k}: {v}")
        await query.edit_message_text("\n".join(lines), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="feed:report")]]))
        return

    # fallback to menu
    await query.edit_message_text("⚠️ Action not recognized.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="feed:menu")]]))


# ------------------------
# Text handler for flows (add formula, magic, calculate)
# ------------------------
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message or not message.text:
        return
    text = message.text.strip()
    flow = context.user_data.get("flow", "")

    # Cancel if user typed 'back' or 'menu'
    if text.lower() in ("back", "menu", "cancel"):
        context.user_data.pop("flow", None)
        await message.reply_text("Cancelled. Use /start or open the feed formula menu.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Open Feed Menu", callback_data="feed:menu")]]))
        return

    # Magic flow: user entered desired CP %
    if flow == "feed_magic":
        try:
            cp = float(re.sub(r"[^\d\.]", "", text))
            # Placeholder simple magic algorithm:
            # Create a simple mix of "maize" and "soybean meal" if exist
            maize = await fcore.async_find_feed_item_by_name("maize")
            soybean = await fcore.async_find_feed_item_by_name("soy")
            if not maize or not soybean:
                await message.reply_text("Couldn't find maize or soybean feed items in database to auto-generate. Please add feed items first or use Add my formula.")
                context.user_data.pop("flow", None)
                return
            # simple proportions to reach target CP: assume maize cp=8, soybean cp=44 (these are examples)
            maize_cp = float((maize.get("nutrients") or {}).get("cp", 8))
            soy_cp = float((soybean.get("nutrients") or {}).get("cp", 44))
            # solve simple two ingredient balance: x*maize_cp + (100-x)*soy_cp = desired_cp*100
            # proportion of maize = (soy_cp - cp) / (soy_cp - maize_cp) * 100
            denom = (soy_cp - maize_cp) if (soy_cp - maize_cp) != 0 else 1
            maize_prop = (soy_cp - cp) / denom * 100
            maize_prop = max(0, min(100, maize_prop))
            soy_prop = 100 - maize_prop
            # Build temporary formula components referencing actual ids
            components = [
                {"feed_item_id": maize["id"], "proportion": round(maize_prop, 2)},
                {"feed_item_id": soybean["id"], "proportion": round(soy_prop, 2)}
            ]
            # Save formula automatically with name
            user = await async_get_user_by_telegram(update.effective_user.id)
            name = f"Magic_{int(cp)}CP"
            saved = await fcore.async_create_formula(update.effective_user.id, name, components)
            if not saved:
                await message.reply_text("⚠️ Failed to save magic formula. Try again later.")
                context.user_data.pop("flow", None)
                return
            context.user_data.pop("flow", None)
            await message.reply_text(f"🔮 Magic formula created: *{name}*\nUse Calculate to compute costs.", parse_mode="Markdown")
        except Exception:
            LOG.exception("feed_magic failed")
            await message.reply_text("⚠️ Failed to generate magic formula. Send a number like `16` for 16% CP.")
        return

    # Add formula: first step receive name
    if flow == "feed_add_name":
        context.user_data["feed_add_name"] = text
        context.user_data["flow"] = "feed_add_components"
        await message.reply_text(
            "Now send components, one per line, in the format:\n"
            "`ingredient_name - proportion`\n"
            "Example:\n"
            "`Maize - 60`\n"
            "`Soybean meal - 40`\n\n"
            "You can use partial names. When done send `done`.",
            parse_mode="Markdown"
        )
        return

    # Add components: expect lines until 'done'
    if flow == "feed_add_components":
        if text.lower() == "done":
            # collect parsed components in context
            comps = context.user_data.get("feed_add_components", [])
            name = context.user_data.get("feed_add_name")
            if not name or not comps:
                await message.reply_text("⚠️ Name or components missing. Start again with Add my formula.")
                context.user_data.pop("flow", None)
                context.user_data.pop("feed_add_components", None)
                context.user_data.pop("feed_add_name", None)
                return
            # validate proportions sum
            total = sum([c.get("proportion", 0) for c in comps])
            if abs(total - 100.0) > 1.0:
                await message.reply_text(f"⚠️ Proportions sum to {total}%. They should sum ~100%. Adjust proportions and send again or re-start Add flow.")
                return
            # save formula
            saved = await fcore.async_create_formula(update.effective_user.id, name, comps)
            context.user_data.pop("flow", None)
            context.user_data.pop("feed_add_components", None)
            context.user_data.pop("feed_add_name", None)
            if saved:
                await message.reply_text(f"✅ Formula *{name}* saved.", parse_mode="Markdown")
            else:
                await message.reply_text("❌ Failed to save formula. Try again later.")
            return

        # parse a single component line
        # accept patterns: "name - proportion" or "name, proportion" or "name proportion"
        parts = re.split(r"[-,]", text, maxsplit=1)
        if len(parts) == 2:
            raw_name = parts[0].strip()
            raw_prop = parts[1].strip()
        else:
            # try split by whitespace last token being number
            m = re.match(r"^(.*?)[\s]+([0-9]+(?:\.[0-9]+)?)\s*$", text)
            if m:
                raw_name = m.group(1).strip()
                raw_prop = m.group(2)
            else:
                await message.reply_text("⚠️ Can't parse this line. Use `Name - proportion` (e.g. `Maize - 60`).")
                return
        # parse proportion
        try:
            prop = float(re.sub(r"[^\d\.]", "", raw_prop))
        except Exception:
            await message.reply_text("⚠️ Couldn't read proportion number. Try again.")
            return
        # find feed_item by name
        item = await fcore.async_find_feed_item_by_name(raw_name)
        if not item:
            await message.reply_text(f"⚠️ Couldn't find feed item matching '{raw_name}'. Add that feed item to inventory first or try a different name.")
            return
        entry = {"feed_item_id": item["id"], "proportion": round(prop, 4)}
        comps = context.user_data.get("feed_add_components", [])
        comps.append(entry)
        context.user_data["feed_add_components"] = comps
        await message.reply_text(f"Added component: *{item['name']}* — {prop}%", parse_mode="Markdown")
        return

    # Calculate flow amount entry
    if flow.startswith("feed_calc_amount:"):
        try:
            _, fid = flow.split(":", 1)
        except Exception:
            await message.reply_text("⚠️ Invalid internal state. Start over.")
            context.user_data.pop("flow", None)
            return
        # parse number
        try:
            kg = float(re.sub(r"[^\d\.]", "", text))
            if kg <= 0:
                raise ValueError()
        except Exception:
            await message.reply_text("⚠️ Send a valid number for kg (e.g. `100`).")
            return
        res = await fcore.async_calculate_formula(fid, target_kg=kg)
        context.user_data.pop("flow", None)
        if not res:
            await message.reply_text("⚠️ Failed to calculate formula. Try again later.")
            return
        lines = [f"🧮 Calculation for {kg} kg"]
        lines.append(f"Total cost: {res.get('total_cost')}")
        lines.append("Components:")
        for c in res.get("components", []):
            lines.append(f" - {c['name']}: {c['weight_kg']} kg — cost {c['cost']}")
        lines.append("\nNutrients (kg):")
        for k, v in res.get("nutrients", {}).items():
            lines.append(f" - {k}: {v}")
        await message.reply_text("\n".join(lines))
        return

    # Default fallback
    await message.reply_text("I didn't understand that. Use the Feed Formula menu.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Open Feed Menu", callback_data="feed:menu")]]))


feed_handlers["router"] = router
feed_handlers["handle_text"] = handle_text
