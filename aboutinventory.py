# aboutinventory.py
import logging
import datetime
import asyncio
from typing import List, Optional, Dict, Any

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from farmcore import (
    async_get_user_with_farm_by_telegram,
    async_list_inventory_items,
    async_create_inventory_item,
    async_update_inventory_item,
    async_delete_inventory_item,
    supabase,  # used only for preferred-membership resolution
)

logger = logging.getLogger(__name__)
inventory_handlers = {}

_PAGE_SIZE = 12
_ADD_PREFIX = "inventory_add_"

# --- Helpers ---
def _mk_inv_line(item: Dict[str, Any]) -> str:
    # keep line short and markdown-safe
    name = item.get("name") or "Unnamed"
    qty = item.get("quantity") or 0
    unit = item.get("unit") or "unit"
    iid = item.get("id") or "—"
    return f"• {name} — {qty} {unit} — id: `{iid}`"

def _reply_or_edit(update: Update, text: str, **kwargs):
    """Helper to reply or edit message depending on callback vs message."""
    if update.callback_query:
        return update.callback_query.edit_message_text(text, **kwargs)
    return update.effective_message.reply_text(text, **kwargs)

async def _resolve_user_and_farm_preferring_membership(telegram_id: int) -> Optional[Dict[str, Any]]:
    """
    Try to get combined {'user':..., 'farm':..., 'member':...} preferring farm_members 
    (i.e. if user is a member of a farm use that farm), otherwise fallback to
    async_get_user_with_farm_by_telegram (owner/current farm).
    This mirrors the resolver used in other modules so managers see the farm they're assigned to.
    """
    try:
        # fast path: let farmcore try its usual approach first
        combined = await async_get_user_with_farm_by_telegram(telegram_id)
    except Exception:
        combined = None

    # If a farm was found and seems fine, return it
    if combined and combined.get("farm"):
        return {"user": combined.get("user"), "farm": combined.get("farm"), "member": None}

    # Otherwise attempt membership-based lookup (use supabase directly but in thread)
    try:
        # get app_user row
        def _fn_user():
            return supabase.table("app_users").select("*").eq("telegram_id", telegram_id).limit(1).execute()
        out_user = await asyncio.to_thread(_fn_user)
        user_rows = getattr(out_user, "data", None) or (out_user.get("data") if isinstance(out_user, dict) else None)
        user_row = user_rows[0] if user_rows and isinstance(user_rows, list) and len(user_rows) > 0 else None
        if not user_row:
            return None
        user_id = user_row.get("id")

        # find latest membership if exists
        def _fn_mem():
            return supabase.table("farm_members").select("*").eq("user_id", user_id).order("created_at", desc=True).limit(1).execute()
        mem_out = await asyncio.to_thread(_fn_mem)
        mem_rows = getattr(mem_out, "data", None) or (mem_out.get("data") if isinstance(mem_out, dict) else None)
        if mem_rows and isinstance(mem_rows, list) and len(mem_rows) > 0:
            member = mem_rows[0]
            farm_id = member.get("farm_id")
            # fetch farm
            def _fn_farm():
                return supabase.table("farms").select("*").eq("id", farm_id).limit(1).execute()
            farm_out = await asyncio.to_thread(_fn_farm)
            farm_rows = getattr(farm_out, "data", None) or (farm_out.get("data") if isinstance(farm_out, dict) else None)
            if farm_rows and len(farm_rows) > 0:
                return {"user": user_row, "farm": farm_rows[0], "member": member}

    except Exception:
        logger.exception("Error resolving membership-based farm for telegram_id=%s", telegram_id)

    # fallback to whatever farmcore returned earlier (may be None)
    return combined

# --- Menu ---
async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Item", callback_data="inventory:add")],
        [InlineKeyboardButton("📦 Inventory List", callback_data="inventory:list:0")],
        [InlineKeyboardButton("🔙 Back", callback_data="skip")]
    ])
    try:
        await _reply_or_edit(update, "📦 Inventory — choose an action:", reply_markup=kb)
    except Exception:
        logger.exception("Failed to show inventory menu")

inventory_handlers["menu"] = menu

# --- Router ---
async def router(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str):
    try:
        parts = action.split(":") if action else []
        cmd = parts[0] if parts else ""
        arg = parts[1] if len(parts) > 1 else None

        if cmd == "add":
            context.user_data["flow"] = "inventory_add"
            context.user_data[_ADD_PREFIX + "step"] = "name"
            await _reply_or_edit(update, "Send item name:")
            return

        if cmd == "list":
            page = int(arg) if arg and arg.isdigit() else 0
            resolved = await _resolve_user_and_farm_preferring_membership(update.effective_user.id)
            if not resolved or not resolved.get("farm"):
                await _reply_or_edit(update, "⚠️ Farm not found. Register or join a farm first.")
                return
            farm_id = resolved["farm"]["id"]
            items = await async_list_inventory_items(farm_id=farm_id, limit=1000)
            total = len(items)
            start = page * _PAGE_SIZE
            end = start + _PAGE_SIZE
            page_items = items[start:end]
            header = f"*Inventory* — page {page+1} / {max(1, (total + _PAGE_SIZE - 1) // _PAGE_SIZE)}\n\n"
            body = "\n".join([_mk_inv_line(i) for i in page_items]) if page_items else "No items on this page."
            text = header + body
            kb_rows = []
            for i in page_items:
                kb_rows.append([InlineKeyboardButton(f"{i.get('name')}", callback_data=f"inventory:view:{i.get('id')}")])
            nav = []
            if start > 0:
                nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"inventory:list:{page-1}"))
            if end < total:
                nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"inventory:list:{page+1}"))
            if nav:
                kb_rows.append(nav)
            kb_rows.append([InlineKeyboardButton("➕ Add", callback_data="inventory:add"), InlineKeyboardButton("🔙 Back", callback_data="skip")])
            kb = InlineKeyboardMarkup(kb_rows)
            await _reply_or_edit(update, text, parse_mode="Markdown", reply_markup=kb)
            return

        if cmd == "view" and arg:
            item_id = arg
            resolved = await _resolve_user_and_farm_preferring_membership(update.effective_user.id)
            if not resolved or not resolved.get("farm"):
                await _reply_or_edit(update, "⚠️ Farm not found.")
                return
            farm_id = resolved["farm"]["id"]
            items = await async_list_inventory_items(farm_id=farm_id, limit=1000)
            sel = [it for it in items if it.get("id") == item_id]
            if not sel:
                await _reply_or_edit(update, "⚠️ Item not found.")
                return
            it = sel[0]
            text = (
                f"*{it.get('name')}*\n\n"
                f"Category: {it.get('category') or '—'}\n"
                f"Quantity: {it.get('quantity') or 0} {it.get('unit') or 'unit'}\n"
                f"Cost per unit: {it.get('cost_per_unit') or '—'}\n"
                f"Notes: {(it.get('meta') or {}).get('notes') or '—'}\n"
            )
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Update qty", callback_data=f"inventory:update_qty:{item_id}"),
                 InlineKeyboardButton("🗑 Delete", callback_data=f"inventory:delete:{item_id}")],
                [InlineKeyboardButton("🔙 Back", callback_data="inventory:list:0")]
            ])
            await _reply_or_edit(update, text, parse_mode="Markdown", reply_markup=kb)
            return

        if cmd == "update_qty" and arg:
            item_id = arg
            context.user_data["flow"] = "inventory_update"
            context.user_data["inventory_update_item"] = item_id
            await _reply_or_edit(update, "Send new quantity (numeric), or send `-` to cancel:", parse_mode="Markdown")
            return

        if cmd == "delete" and arg:
            item_id = arg
            ok = await async_delete_inventory_item(item_id)
            if ok:
                # refresh list
                await _reply_or_edit(update, "✅ Item deleted.")
                # show first page
                await router(update, context, "list:0")
            else:
                await _reply_or_edit(update, "❌ Failed to delete item.")
            return

    except Exception:
        logger.exception("Error in inventory router")
        try:
            await _reply_or_edit(update, "❌ Error handling inventory action.")
        except Exception:
            logger.exception("Failed to notify user of inventory error")

inventory_handlers["router"] = router

# --- Text flow handler ---
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    flow = context.user_data.get("flow")
    text = (update.effective_message.text or "").strip()
    try:
        if flow == "inventory_add":
            step = context.user_data.get(_ADD_PREFIX + "step", "name")
            if step == "name":
                context.user_data[_ADD_PREFIX + "name"] = text
                context.user_data[_ADD_PREFIX + "step"] = "category"
                await update.effective_message.reply_text("Send category (or `-` to skip):", parse_mode="Markdown")
                return
            if step == "category":
                context.user_data[_ADD_PREFIX + "category"] = None if text == "-" else text
                context.user_data[_ADD_PREFIX + "step"] = "quantity"
                await update.effective_message.reply_text("Send initial quantity (numeric, e.g. 10):")
                return
            if step == "quantity":
                try:
                    qty = float(text.replace(",", "."))
                except Exception:
                    await update.effective_message.reply_text("Invalid number. Send numeric like `10`.", parse_mode="Markdown")
                    return
                context.user_data[_ADD_PREFIX + "quantity"] = qty
                context.user_data[_ADD_PREFIX + "step"] = "unit"
                await update.effective_message.reply_text("Send unit (e.g. kg, unit) or `-` for default 'unit':", parse_mode="Markdown")
                return
            if step == "unit":
                unit = None if text == "-" else text
                context.user_data[_ADD_PREFIX + "unit"] = unit or "unit"
                context.user_data[_ADD_PREFIX + "step"] = "cost"
                await update.effective_message.reply_text("Optional: cost per unit (numeric) or `-` to skip:", parse_mode="Markdown")
                return
            if step == "cost":
                cost = None
                if text != "-":
                    try:
                        cost = float(text.replace(",", "."))
                    except Exception:
                        await update.effective_message.reply_text("Invalid number. Send numeric or `-` to skip.", parse_mode="Markdown")
                        return
                # create item
                resolved = await _resolve_user_and_farm_preferring_membership(update.effective_user.id)
                if not resolved or not resolved.get("farm"):
                    await update.effective_message.reply_text("⚠️ Farm not found.")
                    # clear flow
                    for k in list(context.user_data.keys()):
                        if k.startswith(_ADD_PREFIX) or k == "flow":
                            context.user_data.pop(k, None)
                    return
                farm_id = resolved["farm"]["id"]
                created = await async_create_inventory_item(
                    farm_id=farm_id,
                    name=context.user_data.get(_ADD_PREFIX + "name"),
                    category=context.user_data.get(_ADD_PREFIX + "category"),
                    quantity=context.user_data.get(_ADD_PREFIX + "quantity") or 0,
                    unit=context.user_data.get(_ADD_PREFIX + "unit") or "unit",
                    cost_per_unit=cost,
                    meta={}
                )
                if not created:
                    await update.effective_message.reply_text("❌ Failed to create item.")
                else:
                    await update.effective_message.reply_text("✅ Item created.")
                    # show list refreshed
                    await router(update, context, "list:0")
                # clear
                for key in list(context.user_data.keys()):
                    if key.startswith(_ADD_PREFIX) or key == "flow":
                        context.user_data.pop(key, None)
                return

        if flow == "inventory_update":
            if text == "-" or text == "":
                context.user_data.pop("inventory_update_item", None)
                context.user_data.pop("flow", None)
                await update.effective_message.reply_text("Cancelled.")
                return
            try:
                qty = float(text.replace(",", "."))
            except Exception:
                await update.effective_message.reply_text("Invalid number. Send numeric like `5` or `-` to cancel.")
                return
            item_id = context.user_data.get("inventory_update_item")
            if not item_id:
                await update.effective_message.reply_text("No item specified. Cancelled.")
                context.user_data.pop("flow", None)
                return
            updated = await async_update_inventory_item(item_id, {"quantity": qty})
            if not updated:
                await update.effective_message.reply_text("❌ Failed to update quantity.")
            else:
                await update.effective_message.reply_text("✅ Quantity updated.")
                # refresh list
                await router(update, context, "list:0")
            context.user_data.pop("flow", None)
            context.user_data.pop("inventory_update_item", None)
            return

    except Exception:
        logger.exception("Error in inventory flow")
        await update.effective_message.reply_text("❌ Error processing input. Flow cancelled.")
        context.user_data.pop("flow", None)
        # best-effort cleanup
        for k in list(context.user_data.keys()):
            if k.startswith(_ADD_PREFIX) or k in ("inventory_update_item", "flow"):
                context.user_data.pop(k, None)
        return

# expose
inventory_handlers["router"] = router
inventory_handlers["handle_text"] = handle_text
inventory_handlers["menu"] = menu
