# aboutmilk.py (roles + membership-preferred farm resolution)
import asyncio
import logging
import datetime
from typing import List, Optional, Dict, Any

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from farmcore import (
    async_list_animals,
    async_list_milk,
    async_get_user_by_telegram,
    supabase,
)
# role helpers (async)
from farmcore_role import async_user_has_permission, async_get_user_role_in_farm

logger = logging.getLogger(__name__)
milk_handlers = {}

_PAGE_SIZE = 10  # smaller pages so UI fits

# --------------------
# Small async DB helpers (use asyncio.to_thread for supabase operations)
# --------------------
async def _db_insert(table: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        resp = await asyncio.to_thread(lambda: supabase.table(table).insert(payload).execute())
        data = getattr(resp, "data", None) or (resp.get("data") if isinstance(resp, dict) else None)
        error = getattr(resp, "error", None) or (resp.get("error") if isinstance(resp, dict) else None)
        return {"data": data, "error": error}
    except Exception as exc:
        logger.exception("db insert failed")
        return {"error": str(exc)}

async def _db_select_one(table: str, filters: Optional[List[tuple]] = None, order_by: Optional[tuple] = None) -> Dict[str, Any]:
    """
    Return single row or {}. filters: list of (col, op, val) but we only support simple eq if provided as list of 2-tuples.
    order_by: ('col', {'ascending': False}) style accepted (kept small compatibility).
    """
    try:
        def _fn():
            q = supabase.table(table).select("*")
            if filters:
                # support list of ("col","eq","val") or simple [("col","val")]
                for f in filters:
                    if len(f) == 3:
                        col, op, val = f
                        if op == "eq":
                            q = q.eq(col, val)
                        elif op == "is":
                            # supabase python client doesn't use 'is' directly; we'll emulate by sending None eq? skip here
                            if val is None:
                                q = q.is_(col, None)
                        # other ops could be added
                    elif len(f) == 2:
                        q = q.eq(f[0], f[1])
            if order_by:
                col, opts = order_by
                asc = opts.get("ascending", True)
                # supabase python client uses .order(column, desc=True/False)
                q = q.order(col, desc=not asc)
            q = q.limit(1)
            return q.execute()
        resp = await asyncio.to_thread(_fn)
        data = getattr(resp, "data", None) or (resp.get("data") if isinstance(resp, dict) else None)
        if data and isinstance(data, list):
            return data[0] or {}
        return {}
    except Exception:
        logger.exception("db select one failed for %s", table)
        return {}

async def _db_select_many(table: str, eq_filter: Optional[tuple] = None, limit: Optional[int] = None, order_by: Optional[tuple] = None) -> Dict[str, Any]:
    try:
        def _fn():
            q = supabase.table(table).select("*")
            if eq_filter:
                q = q.eq(eq_filter[0], eq_filter[1])
            if order_by:
                col, opts = order_by
                asc = opts.get("ascending", True)
                q = q.order(col, desc=not asc)
            if limit:
                q = q.limit(limit)
            return q.execute()
        resp = await asyncio.to_thread(_fn)
        data = getattr(resp, "data", None) or (resp.get("data") if isinstance(resp, dict) else None)
        error = getattr(resp, "error", None) or (resp.get("error") if isinstance(resp, dict) else None)
        return {"data": data, "error": error}
    except Exception:
        logger.exception("db select failed")
        return {"error": "db-select-failed"}

async def _db_delete(table: str, record_id: str) -> Dict[str, Any]:
    try:
        resp = await asyncio.to_thread(lambda: supabase.table(table).delete().eq("id", record_id).execute())
        data = getattr(resp, "data", None) or (resp.get("data") if isinstance(resp, dict) else None)
        error = getattr(resp, "error", None) or (resp.get("error") if isinstance(resp, dict) else None)
        return {"data": data, "error": error}
    except Exception:
        logger.exception("db delete failed")
        return {"error": "db-delete-failed"}

# --------------------
# Resolve user + farm (prefer membership over owner)
# --------------------
async def _resolve_user_and_farm_preferring_membership(telegram_id: int) -> Optional[Dict[str, Any]]:
    """
    Returns dict: {"user": <app_user_row>, "farm": <farm_row>, "member": <farm_members_row or None>}
    Preference order:
      1) farm_members where user_id = user.id (most recent)
      2) app_users.current_farm_id if set and valid
      3) farms where owner_id = user.id (most recent)
    If none found returns None.
    """
    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        user_row = None
    if not user_row:
        return None

    user_id = user_row.get("id")

    # 1) membership
    try:
        # find latest membership for the user
        def _fn_member():
            return supabase.table("farm_members").select("*").eq("user_id", user_id).order("created_at", desc=True).limit(1).execute()
        out = await asyncio.to_thread(_fn_member)
        mem_data = getattr(out, "data", None) or (out.get("data") if isinstance(out, dict) else None)
        if mem_data and isinstance(mem_data, list) and len(mem_data) > 0:
            member = mem_data[0]
            farm_id = member.get("farm_id")
            # fetch farm row
            def _fn_farm():
                return supabase.table("farms").select("*").eq("id", farm_id).limit(1).execute()
            farm_out = await asyncio.to_thread(_fn_farm)
            farm_rows = getattr(farm_out, "data", None) or (farm_out.get("data") if isinstance(farm_out, dict) else None)
            if farm_rows and len(farm_rows) > 0:
                return {"user": user_row, "farm": farm_rows[0], "member": member}

    except Exception:
        logger.exception("Error checking membership for user_id=%s", user_id)

    # 2) current_farm_id on user
    try:
        current_farm_id = user_row.get("current_farm_id")
        if current_farm_id:
            def _fn_cf():
                return supabase.table("farms").select("*").eq("id", current_farm_id).limit(1).execute()
            cf_out = await asyncio.to_thread(_fn_cf)
            cf_rows = getattr(cf_out, "data", None) or (cf_out.get("data") if isinstance(cf_out, dict) else None)
            if cf_rows and len(cf_rows) > 0:
                return {"user": user_row, "farm": cf_rows[0], "member": None}
    except Exception:
        logger.exception("Error checking current_farm_id for user_id=%s", user_id)

    # 3) farms owned by user (fallback)
    try:
        def _fn_owner():
            return supabase.table("farms").select("*").eq("owner_id", user_id).order("created_at", desc=True).limit(1).execute()
        owner_out = await asyncio.to_thread(_fn_owner)
        owner_rows = getattr(owner_out, "data", None) or (owner_out.get("data") if isinstance(owner_out, dict) else None)
        if owner_rows and len(owner_rows) > 0:
            return {"user": user_row, "farm": owner_rows[0], "member": None}
    except Exception:
        logger.exception("Error checking owned farms for user_id=%s", user_id)

    # nothing found
    return {"user": user_row, "farm": None, "member": None}

# --------------------
# Utilities & formatting
# --------------------
def _format_milk_line(r: Dict[str, Any]) -> str:
    date = r.get("date") or r.get("created_at") or "—"
    qty = r.get("quantity") or 0
    aid = r.get("animal_id") or "bulk"
    return f"• {date} — {qty} L — animal: `{aid}` (id: `{r.get('id')}`)"

def _mk_milk_list_text(records: List[dict]) -> str:
    if not records:
        return "No milk records found."
    lines = [ _format_milk_line(r) for r in records ]
    total = sum(float(r.get("quantity") or 0) for r in records)
    header = f"*Recent milk records* — total shown: {len(records)}  •  Sum: {total:.2f} L\n\n"
    return header + "\n".join(lines)

def _clear_flow(context_user_data: dict, prefix: str = "milk"):
    for k in list(context_user_data.keys()):
        if k.startswith((prefix,)):
            context_user_data.pop(k, None)

# --------------------
# Permission helper (uses new resolver)
# --------------------
async def _ensure_milk_permission(update: Update, context: ContextTypes.DEFAULT_TYPE, *, must_have_edit: bool = False) -> Optional[Dict[str, Any]]:
    """
    Ensure the calling user has a farm + appropriate permission to use the 'milk' module.
    If must_have_edit=True, also require role in ('owner','manager','worker') for destructive edits.
    Returns a dict {'user_id': ..., 'farm_id': ..., 'role': ...} on success, or None (and sends a message) on failure.
    """
    try:
        combined = await _resolve_user_and_farm_preferring_membership(update.effective_user.id)
    except Exception:
        combined = None
    if not combined or not combined.get("farm") or not combined.get("user"):
        txt = "⚠️ Farm or user not found. Register a farm or join one first with an invitation."
        try:
            if update.callback_query:
                await update.callback_query.edit_message_text(txt)
            else:
                await update.message.reply_text(txt)
        except Exception:
            pass
        return None

    user_id = combined["user"]["id"]
    farm_id = combined["farm"]["id"]

    try:
        allowed = await async_user_has_permission(user_id, farm_id, "milk")
    except Exception:
        allowed = False

    if not allowed:
        try:
            role = await async_get_user_role_in_farm(user_id, farm_id)
        except Exception:
            role = None
        txt = f"⚠️ Your role '{role or 'unknown'}' does not have permission to use the Milk module."
        try:
            if update.callback_query:
                await update.callback_query.edit_message_text(txt)
            else:
                await update.message.reply_text(txt)
        except Exception:
            pass
        return None

    if must_have_edit:
        try:
            role = await async_get_user_role_in_farm(user_id, farm_id)
        except Exception:
            role = None
        if role not in ("owner", "manager", "worker"):
            txt = f"⚠️ Your role '{role or 'unknown'}' cannot perform this action (requires owner/manager/worker)."
            try:
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt)
                else:
                    await update.message.reply_text(txt)
            except Exception:
                pass
            return None
        return {"user_id": user_id, "farm_id": farm_id, "role": role}

    try:
        role = await async_get_user_role_in_farm(user_id, farm_id)
    except Exception:
        role = None
    return {"user_id": user_id, "farm_id": farm_id, "role": role}

# --------------------
# Menu & Router
# --------------------
async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Show menu only if user has at least view permission
    perm = await _ensure_milk_permission(update, context, must_have_edit=False)
    if not perm:
        return

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add bulk (quick)", callback_data="milk:bulk_start")],
        [InlineKeyboardButton("🐄 Per-cow (per-animal)", callback_data="milk:per_start")],
        [InlineKeyboardButton("📄 Recent Records", callback_data="milk:list:0")],
        [InlineKeyboardButton("📊 Summary (monthly)", callback_data="milk:summary")],
        [InlineKeyboardButton("🔙 Back", callback_data="skip")],
    ])
    try:
        if update.callback_query:
            await update.callback_query.edit_message_text("🥛 Milk — choose an action:", reply_markup=kb)
        else:
            await update.message.reply_text("🥛 Milk — choose an action:", reply_markup=kb)
    except Exception:
        logger.exception("Failed to show milk menu")

milk_handlers["menu"] = menu

# --------------------
# Per-animal helper: render animals page with short tokens
# --------------------
async def _render_animals_page_with_tokens(update: Update, context: ContextTypes.DEFAULT_TYPE, farm_id: str, date_str: str, page: int = 0):
    animals = await async_list_animals(farm_id=farm_id, limit=1000)
    total = len(animals)
    start = page * _PAGE_SIZE
    end = start + _PAGE_SIZE
    page_animals = animals[start:end]

    # create / extend token map stored in user_data
    token_map: Dict[str, str] = context.user_data.get("milk_animal_map", {})
    # token index base based on global mapping length to avoid collisions during session
    base_idx = len(token_map)
    kb_rows = []
    header = f"*Select animal to record for {date_str}:* (page {page+1}/{max(1, (total + _PAGE_SIZE -1)//_PAGE_SIZE)})\n"
    for i, a in enumerate(page_animals):
        token = f"T{base_idx + i}"  # short token
        token_map[token] = a.get("id")
        label = f"{a.get('name') or a.get('tag')} ({a.get('tag')})"
        # callback_data short: milk:ps:<token>:<date>:<page>
        cb = f"milk:ps:{token}:{date_str}:{page}"
        kb_rows.append([InlineKeyboardButton(label, callback_data=cb)])

    # save token map
    context.user_data["milk_animal_map"] = token_map

    # bulk row (bulk uses token 'BULK')
    kb_rows.append([InlineKeyboardButton("➕ Add bulk (whole farm)", callback_data=f"milk:pb:{date_str}:{page}")])

    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"milk:pp:{date_str}:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"milk:pp:{date_str}:{page+1}"))
    if nav:
        kb_rows.append(nav)
    kb_rows.append([InlineKeyboardButton("🔙 Cancel", callback_data="skip")])
    kb = InlineKeyboardMarkup(kb_rows)

    text = header + "\n(press a cow to record milk quickly)"
    try:
        if update.callback_query:
            await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
        else:
            await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
    except Exception:
        logger.exception("Failed rendering animals page")

# --------------------
# Record helper: insert and notify
# --------------------
async def _record_milk_and_notify_simple(update: Update, context: ContextTypes.DEFAULT_TYPE, farm_id: str, animal_id: Optional[str], qty: float, date_str: str):
    # recorded_by
    user_row = await async_get_user_by_telegram(update.effective_user.id)
    recorded_by = user_row.get("id") if user_row else None
    payload = {
        "farm_id": farm_id,
        "animal_id": animal_id,
        "quantity": qty,
        "recorded_by": recorded_by,
        "date": date_str,
        "note": None,
    }
    out = await _db_insert("milk_production", payload)
    if out.get("error"):
        txt = "❌ Failed to record milk. Try again later."
    else:
        who = f"animal {animal_id}" if animal_id else "whole farm (bulk)"
        txt = f"✅ Recorded {qty} L for {who} on {date_str}."
    try:
        if update.callback_query:
            # answer callback then edit
            try:
                await update.callback_query.answer()
            except Exception:
                pass
            try:
                await update.callback_query.edit_message_text(txt)
            except Exception:
                # fallback: send new message
                await update.effective_message.reply_text(txt)
        else:
            await update.effective_message.reply_text(txt)
    except Exception:
        logger.exception("Failed to send confirmation message")

# --------------------
# Router: handles bulk + per-animal and existing list/view/edit/delete/summary flows
# --------------------
async def router(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str):
    """
    actions (short forms to keep callback_data small):
      - bulk_start
      - bulk_date:<YYYY-MM-DD>
      - bulk_pickdate
      - bulk_qty:<date>:<qty>
      - bulk_custom:<date>  (set waiting-for-input)
      - per_start
      - per_date:<YYYY-MM-DD>
      - pp:<date>:<page>   (per page navigation)
      - ps:<token>:<date>:<page>  (select animal token)
      - pb:<date>:<page>  (per-animal bulk quick)
      - per_qty:<token_or_BULK>:<date>:<qty>  (quick qty)
      - per_custom:<token_or_BULK>:<date>:<page>  (ask typed qty)
      - plus existing add/list/view/edit/delete/summary actions kept
    """
    try:
        parts = action.split(":")
        cmd = parts[0] if parts else ""

        # ---- BULK (direct quick add) ----
        if cmd == "bulk_start":
            # permission to record required
            perm = await _ensure_milk_permission(update, context, must_have_edit=False)
            if not perm:
                return

            # ask date: Today or pick date
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📅 Today", callback_data=f"milk:bulk_date:{datetime.date.today().isoformat()}")],
                [InlineKeyboardButton("🖊 Pick date (type)", callback_data="milk:bulk_pickdate")],
                [InlineKeyboardButton("🔙 Cancel", callback_data="skip")],
            ])
            if update.callback_query:
                await update.callback_query.edit_message_text("Choose date for bulk record:", reply_markup=kb)
            else:
                await update.message.reply_text("Choose date for bulk record:", reply_markup=kb)
            return

        if cmd == "bulk_pickdate":
            perm = await _ensure_milk_permission(update, context, must_have_edit=False)
            if not perm:
                return
            context.user_data["milk_bulk_waiting_date"] = True
            if update.callback_query:
                await update.callback_query.edit_message_text("Please send date YYYY-MM-DD (e.g. 2025-08-25) or /cancel:")
            else:
                await update.message.reply_text("Please send date YYYY-MM-DD (e.g. 2025-08-25) or /cancel:")
            return

        if cmd == "bulk_date" and len(parts) >= 2:
            perm = await _ensure_milk_permission(update, context, must_have_edit=False)
            if not perm:
                return
            date_str = parts[1]
            # show quick amounts
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("1 L", callback_data=f"milk:bulk_qty:{date_str}:1"),
                 InlineKeyboardButton("5 L", callback_data=f"milk:bulk_qty:{date_str}:5")],
                [InlineKeyboardButton("10 L", callback_data=f"milk:bulk_qty:{date_str}:10"),
                 InlineKeyboardButton("✏️ Custom", callback_data=f"milk:bulk_custom:{date_str}")],
                [InlineKeyboardButton("🔙 Cancel", callback_data="skip")],
            ])
            txt = f"Record bulk milk for {date_str}. Choose quick amount or Custom."
            if update.callback_query:
                await update.callback_query.edit_message_text(txt, reply_markup=kb)
            else:
                await update.message.reply_text(txt, reply_markup=kb)
            return

        if cmd == "bulk_qty" and len(parts) >= 3:
            perm = await _ensure_milk_permission(update, context, must_have_edit=False)
            if not perm:
                return
            date_str = parts[1]
            try:
                qty = float(parts[2])
            except Exception:
                qty = None
            if qty is None:
                if update.callback_query:
                    await update.callback_query.edit_message_text("Invalid quick quantity.")
                else:
                    await update.message.reply_text("Invalid quick quantity.")
                return
            # perform insert
            resolved = await _resolve_user_and_farm_preferring_membership(update.effective_user.id)
            if not resolved or not resolved.get("farm"):
                if update.callback_query:
                    await update.callback_query.edit_message_text("⚠️ Farm not found.")
                else:
                    await update.message.reply_text("⚠️ Farm not found.")
                return
            farm_id = resolved["farm"]["id"]
            await _record_milk_and_notify_simple(update, context, farm_id, None, qty, date_str)
            return

        if cmd == "bulk_custom" and len(parts) >= 2:
            perm = await _ensure_milk_permission(update, context, must_have_edit=False)
            if not perm:
                return
            date_str = parts[1]
            # set waiting state for custom amount
            context.user_data["milk_bulk_custom"] = {"date": date_str}
            if update.callback_query:
                await update.callback_query.edit_message_text("Send custom quantity in liters (e.g. 12.5) or /cancel:")
            else:
                await update.message.reply_text("Send custom quantity in liters (e.g. 12.5) or /cancel:")
            return

        # ---- PER-ANIMAL (starts) ----
        if cmd == "per_start":
            perm = await _ensure_milk_permission(update, context, must_have_edit=False)
            if not perm:
                return
            context.user_data["milk_flow_per"] = True
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📅 Today", callback_data=f"milk:per_date:{datetime.date.today().isoformat()}")],
                [InlineKeyboardButton("🖊 Pick date (type)", callback_data="milk:per_pickdate")],
                [InlineKeyboardButton("🔙 Cancel", callback_data="skip")],
            ])
            if update.callback_query:
                await update.callback_query.edit_message_text("Choose date for per-cow recording:", reply_markup=kb)
            else:
                await update.message.reply_text("Choose date for per-cow recording:", reply_markup=kb)
            return

        if cmd == "per_pickdate":
            perm = await _ensure_milk_permission(update, context, must_have_edit=False)
            if not perm:
                return
            context.user_data["milk_per_waiting_date"] = True
            if update.callback_query:
                await update.callback_query.edit_message_text("Please send date YYYY-MM-DD (e.g. 2025-08-25) or /cancel:")
            else:
                await update.message.reply_text("Please send date YYYY-MM-DD (e.g. 2025-08-25) or /cancel:")
            return

        if cmd == "per_date" and len(parts) >= 2:
            perm = await _ensure_milk_permission(update, context, must_have_edit=False)
            if not perm:
                return
            date_str = parts[1]
            resolved = await _resolve_user_and_farm_preferring_membership(update.effective_user.id)
            if not resolved or not resolved.get("farm"):
                txt = "⚠️ Farm not found. Please register a farm first with /start."
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt)
                else:
                    await update.message.reply_text(txt)
                return
            farm_id = resolved["farm"]["id"]
            await _render_animals_page_with_tokens(update, context, farm_id, date_str, page=0)
            return

        # per page navigation (pp)
        if cmd == "pp" and len(parts) >= 3:
            perm = await _ensure_milk_permission(update, context, must_have_edit=False)
            if not perm:
                return
            date_str = parts[1]
            page = int(parts[2]) if parts[2].isdigit() else 0
            resolved = await _resolve_user_and_farm_preferring_membership(update.effective_user.id)
            if not resolved or not resolved.get("farm"):
                if update.callback_query:
                    await update.callback_query.edit_message_text("⚠️ Farm not found.")
                else:
                    await update.message.reply_text("⚠️ Farm not found.")
                return
            farm_id = resolved["farm"]["id"]
            await _render_animals_page_with_tokens(update, context, farm_id, date_str, page=page)
            return

        # per select (short token) -> show quick amounts or custom
        if cmd == "ps" and len(parts) >= 4:
            perm = await _ensure_milk_permission(update, context, must_have_edit=False)
            if not perm:
                return
            token = parts[1]
            date_str = parts[2]
            page = int(parts[3]) if parts[3].isdigit() else 0
            token_map: Dict[str, str] = context.user_data.get("milk_animal_map", {})
            animal_id = token_map.get(token)
            if not animal_id:
                # token not found (stale) — re-render page to refresh tokens
                resolved = await _resolve_user_and_farm_preferring_membership(update.effective_user.id)
                if not resolved or not resolved.get("farm"):
                    if update.callback_query:
                        await update.callback_query.edit_message_text("⚠️ Farm not found.")
                    else:
                        await update.message.reply_text("⚠️ Farm not found.")
                    return
                farm_id = resolved["farm"]["id"]
                await _render_animals_page_with_tokens(update, context, farm_id, date_str, page=page)
                return
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("0.5 L", callback_data=f"milk:per_qty:{token}:{date_str}:0.5"),
                 InlineKeyboardButton("1 L", callback_data=f"milk:per_qty:{token}:{date_str}:1")],
                [InlineKeyboardButton("2 L", callback_data=f"milk:per_qty:{token}:{date_str}:2"),
                 InlineKeyboardButton("✏️ Custom", callback_data=f"milk:per_custom:{token}:{date_str}:{page}")],
                [InlineKeyboardButton("🔙 Back", callback_data=f"milk:pp:{date_str}:{page}")],
            ])
            txt = f"Record milk for this animal on {date_str}. Choose quick amount or Custom."
            if update.callback_query:
                await update.callback_query.edit_message_text(txt, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text(txt, parse_mode="Markdown", reply_markup=kb)
            return

        # per-animal bulk quick from list (pb)
        if cmd == "pb" and len(parts) >= 3:
            perm = await _ensure_milk_permission(update, context, must_have_edit=False)
            if not perm:
                return
            date_str = parts[1]
            page = int(parts[2]) if parts[2].isdigit() else 0
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("1 L", callback_data=f"milk:per_qty:BULK:{date_str}:1"),
                 InlineKeyboardButton("5 L", callback_data=f"milk:per_qty:BULK:{date_str}:5")],
                [InlineKeyboardButton("10 L", callback_data=f"milk:per_qty:BULK:{date_str}:10"),
                 InlineKeyboardButton("✏️ Custom", callback_data=f"milk:per_custom:BULK:{date_str}:{page}")],
                [InlineKeyboardButton("🔙 Back", callback_data=f"milk:pp:{date_str}:{page}")],
            ])
            txt = f"Record bulk (whole farm) for {date_str}. Choose quick amount or Custom."
            if update.callback_query:
                await update.callback_query.edit_message_text(txt, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text(txt, parse_mode="Markdown", reply_markup=kb)
            return

        # quick per quantity (token or BULK)
        if cmd == "per_qty" and len(parts) >= 4:
            perm = await _ensure_milk_permission(update, context, must_have_edit=False)
            if not perm:
                return
            token = parts[1]
            date_str = parts[2]
            try:
                qty = float(parts[3])
            except Exception:
                qty = None
            if qty is None:
                if update.callback_query:
                    await update.callback_query.edit_message_text("Invalid quick quantity.")
                else:
                    await update.message.reply_text("Invalid quick quantity.")
                return
            token_map: Dict[str, str] = context.user_data.get("milk_animal_map", {})
            animal_id = None if token == "BULK" else token_map.get(token)
            resolved = await _resolve_user_and_farm_preferring_membership(update.effective_user.id)
            if not resolved or not resolved.get("farm"):
                if update.callback_query:
                    await update.callback_query.edit_message_text("⚠️ Farm not found.")
                else:
                    await update.message.reply_text("⚠️ Farm not found.")
                return
            farm_id = resolved["farm"]["id"]
            await _record_milk_and_notify_simple(update, context, farm_id, animal_id, qty, date_str)
            return

        # per custom -> set waiting state for typed qty
        if cmd == "per_custom" and len(parts) >= 4:
            perm = await _ensure_milk_permission(update, context, must_have_edit=False)
            if not perm:
                return
            token = parts[1]
            date_str = parts[2]
            page = int(parts[3]) if parts[3].isdigit() else 0
            # store token and date for next typed message
            context.user_data["milk_per_custom"] = {"token": token, "date": date_str, "page": page}
            if update.callback_query:
                await update.callback_query.edit_message_text("Send custom quantity in liters (e.g. 4.25) or /cancel:")
            else:
                await update.message.reply_text("Send custom quantity in liters (e.g. 4.25) or /cancel:")
            return

        # --- FALL BACK to other existing flows (list/view/edit/delete/summary) ---
        if cmd == "add":
            perm = await _ensure_milk_permission(update, context, must_have_edit=False)
            if not perm:
                return
            # quick original add flow
            context.user_data["milk_flow"] = "add"
            context.user_data["milk_step"] = "who"
            if update.callback_query:
                await update.callback_query.edit_message_text("Record milk — send the *animal tag* (or `bulk` for whole-farm):", parse_mode="Markdown")
            else:
                await update.message.reply_text("Record milk — send the *animal tag* (or `bulk` for whole-farm):", parse_mode="Markdown")
            return

        if cmd == "list":
            perm = await _ensure_milk_permission(update, context, must_have_edit=False)
            if not perm:
                return
            page = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
            resolved = await _resolve_user_and_farm_preferring_membership(update.effective_user.id)
            if not resolved or not resolved.get("farm"):
                text = "⚠️ Farm not found. Please register a farm first with /start."
                if update.callback_query:
                    await update.callback_query.edit_message_text(text)
                else:
                    await update.message.reply_text(text)
                return
            farm_id = resolved["farm"]["id"]
            records = await async_list_milk(farm_id=farm_id, limit=200)
            total = len(records)
            start = page * _PAGE_SIZE
            end = start + _PAGE_SIZE
            page_records = records[start:end]
            header = f"*Milk records* — page {page+1} / {max(1, (total + _PAGE_SIZE -1)//_PAGE_SIZE)}\n\n"
            text = header + (_mk_milk_list_text(page_records) if page_records else "No records on this page.")
            kb_rows = []
            for r in page_records:
                label = f"{r.get('date') or r.get('created_at') or 'date'} — {r.get('quantity')} L"
                cb = f"milk:v:{r.get('id')}:{page}"
                kb_rows.append([InlineKeyboardButton(label, callback_data=cb)])
            nav = []
            if start > 0:
                nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"milk:list:{page-1}"))
            if end < total:
                nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"milk:list:{page+1}"))
            if nav:
                kb_rows.append(nav)
            kb_rows.append([InlineKeyboardButton("➕ Record new", callback_data="milk:add"), InlineKeyboardButton("🔙 Back", callback_data="skip")])
            kb = InlineKeyboardMarkup(kb_rows)
            if update.callback_query:
                await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
            return

        if cmd == "v" and len(parts) >= 2:
            perm = await _ensure_milk_permission(update, context, must_have_edit=False)
            if not perm:
                return
            record_id = parts[1]
            page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            out = await _db_select_many("milk_production", eq_filter=("id", record_id), limit=1)
            rows = out.get("data") or []
            if not rows:
                txt = "⚠️ Milk record not found."
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt)
                else:
                    await update.message.reply_text(txt)
                return
            r = rows[0]
            text = (
                f"🥛 *Milk record*\n\n"
                f"ID: `{r.get('id')}`\n"
                f"Date: {r.get('date')}\n"
                f"Quantity: {r.get('quantity')} L\n"
                f"Animal ID: `{r.get('animal_id') or 'bulk'}`\n"
                f"Recorded by: `{r.get('recorded_by') or 'unknown'}`\n"
                f"Note: { (r.get('note') or '—') }\n"
            )
            resolved = await _resolve_user_and_farm_preferring_membership(update.effective_user.id)
            farm_id = resolved["farm"]["id"] if resolved and resolved.get("farm") else None
            user_role = None
            if farm_id:
                try:
                    user_role = await async_get_user_role_in_farm(resolved["user"]["id"], farm_id)
                except Exception:
                    user_role = None
            can_edit = user_role in ("owner", "manager", "worker")
            kb_rows = []
            if can_edit:
                kb_rows.append([InlineKeyboardButton("✏️ Edit", callback_data=f"milk:edit:{record_id}:{page}"),
                                InlineKeyboardButton("🗑 Delete", callback_data=f"milk:confirm_delete:{record_id}:{page}")])
            kb_rows.append([InlineKeyboardButton("🔙 Back to list", callback_data=f"milk:list:{page}")])
            kb = InlineKeyboardMarkup(kb_rows)
            if update.callback_query:
                await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
            return

        if cmd == "confirm_delete" and len(parts) >= 2:
            perm = await _ensure_milk_permission(update, context, must_have_edit=True)
            if not perm:
                return
            record_id = parts[1]
            page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Yes, delete", callback_data=f"milk:delete:{record_id}:{page}"),
                 InlineKeyboardButton("No, cancel", callback_data=f"milk:v:{record_id}:{page}")]
            ])
            if update.callback_query:
                await update.callback_query.edit_message_text("⚠️ Are you sure you want to permanently delete this milk record?", reply_markup=kb)
            else:
                await update.message.reply_text("⚠️ Are you sure you want to permanently delete this milk record?", reply_markup=kb)
            return

        if cmd == "delete" and len(parts) >= 2:
            perm = await _ensure_milk_permission(update, context, must_have_edit=True)
            if not perm:
                return
            record_id = parts[1]
            page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            out = await _db_delete("milk_production", record_id)
            if out.get("error"):
                txt = "❌ Failed to delete record."
            else:
                txt = "✅ Milk record deleted."
            if update.callback_query:
                await update.callback_query.edit_message_text(txt)
            else:
                await update.message.reply_text(txt)
            await router(update, context, f"milk:list:{page}")
            return

        if cmd == "edit" and len(parts) >= 2:
            perm = await _ensure_milk_permission(update, context, must_have_edit=True)
            if not perm:
                return
            record_id = parts[1]
            page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            context.user_data["milk_flow"] = "edit"
            context.user_data["milk_edit_id"] = record_id
            context.user_data["milk_edit_step"] = "qty"
            context.user_data["milk_edit_return_page"] = page
            if update.callback_query:
                await update.callback_query.edit_message_text("Send new quantity in liters (e.g. 12.5) or `-` to cancel:")
            else:
                await update.message.reply_text("Send new quantity in liters (e.g. 12.5) or `-` to cancel:")
            return

        if cmd == "summary":
            perm = await _ensure_milk_permission(update, context, must_have_edit=False)
            if not perm:
                return
            resolved = await _resolve_user_and_farm_preferring_membership(update.effective_user.id)
            if not resolved or not resolved.get("farm"):
                txt = "⚠️ Farm not found. Please register a farm first with /start."
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt)
                else:
                    await update.message.reply_text(txt)
                return
            farm_id = resolved["farm"]["id"]
            records = await async_list_milk(farm_id=farm_id, limit=1000)
            cutoff = datetime.date.today() - datetime.timedelta(days=30)
            recent = [r for r in records if (r.get("date") and datetime.date.fromisoformat(str(r.get("date"))) >= cutoff) or (not r.get("date") and True)]
            total = sum(float(r.get("quantity") or 0) for r in recent)
            per_animal: Dict[str, float] = {}
            for r in recent:
                aid = r.get("animal_id") or "bulk"
                per_animal[aid] = per_animal.get(aid, 0.0) + float(r.get("quantity") or 0)
            parts = [f"*Last 30 days total:* {total:.2f} L\n\n*By animal:*"]
            for aid, s in sorted(per_animal.items(), key=lambda kv: -kv[1])[:20]:
                parts.append(f"• `{aid}`: {s:.2f} L")
            text = "\n".join(parts)
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="skip")]])
            if update.callback_query:
                await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
            return

        # unknown action
        if update.callback_query:
            try:
                await update.callback_query.answer("Action not recognized.")
            except Exception:
                pass
        else:
            await update.message.reply_text("Action not recognized.")
    except Exception:
        logger.exception("Error routing milk action")
        try:
            if update.callback_query:
                await update.callback_query.edit_message_text("❌ Error handling milk action.")
            else:
                await update.message.reply_text("❌ Error handling milk action.")
        except Exception:
            pass

milk_handlers["router"] = router

# --------------------
# Flow handler for typed inputs (bulk custom, per-animal custom, add/edit legacy)
# --------------------
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 1) bulk custom typed
    bulk_custom = context.user_data.get("milk_bulk_custom")
    if bulk_custom:
        perm = await _ensure_milk_permission(update, context, must_have_edit=False)
        if not perm:
            _clear_flow(context.user_data)
            return

        text = (update.effective_message.text or "").strip()
        if text.lower() in ("/cancel", "cancel", "-"):
            _clear_flow(context.user_data)
            await update.effective_message.reply_text("Cancelled bulk custom.")
            return
        try:
            qty = float(text.replace(",", "."))
            if qty <= 0:
                raise ValueError()
        except Exception:
            await update.effective_message.reply_text("Invalid quantity. Send a positive number like `12.5` or /cancel.")
            return
        date_str = bulk_custom.get("date")
        resolved = await _resolve_user_and_farm_preferring_membership(update.effective_user.id)
        if not resolved or not resolved.get("farm"):
            await update.effective_message.reply_text("⚠️ Farm not found.")
            _clear_flow(context.user_data)
            return
        farm_id = resolved["farm"]["id"]
        await _record_milk_and_notify_simple(update, context, farm_id, None, qty, date_str)
        _clear_flow(context.user_data)
        return

    # 2) per-animal custom typed
    per_custom = context.user_data.get("milk_per_custom")
    if per_custom:
        perm = await _ensure_milk_permission(update, context, must_have_edit=False)
        if not perm:
            _clear_flow(context.user_data)
            return

        text = (update.effective_message.text or "").strip()
        if text.lower() in ("/cancel", "cancel", "-"):
            _clear_flow(context.user_data)
            await update.effective_message.reply_text("Cancelled custom amount.")
            return
        try:
            qty = float(text.replace(",", "."))
            if qty <= 0:
                raise ValueError()
        except Exception:
            await update.effective_message.reply_text("Invalid quantity. Send a positive number like `4.25` or /cancel.")
            return
        token = per_custom.get("token")
        date_str = per_custom.get("date")
        token_map: Dict[str, str] = context.user_data.get("milk_animal_map", {})
        animal_id = None if token == "BULK" else token_map.get(token)
        resolved = await _resolve_user_and_farm_preferring_membership(update.effective_user.id)
        if not resolved or not resolved.get("farm"):
            await update.effective_message.reply_text("⚠️ Farm not found.")
            _clear_flow(context.user_data)
            return
        farm_id = resolved["farm"]["id"]
        await _record_milk_and_notify_simple(update, context, farm_id, animal_id, qty, date_str)
        _clear_flow(context.user_data)
        return

    # 3) legacy quick-add / edit flows (kept from your previous code)
    flow = context.user_data.get("milk_flow", "")
    if not flow:
        return

    message = update.effective_message
    if not message or not message.text:
        return

    text = message.text.strip()
    try:
        # ADD flow (legacy quick)
        if flow == "add":
            perm = await _ensure_milk_permission(update, context, must_have_edit=False)
            if not perm:
                _clear_flow(context.user_data)
                return

            step = context.user_data.get("milk_step", "who")
            if step == "who":
                who = text.strip()
                if not who:
                    await message.reply_text("Please send an animal tag or `bulk`.")
                    return
                context.user_data["milk_who"] = who
                context.user_data["milk_step"] = "qty"
                await message.reply_text("Send quantity in liters (e.g. 12.5):")
                return

            if step == "qty":
                try:
                    qty = float(text.replace(",", "."))
                    if qty <= 0:
                        raise ValueError("non-positive")
                except Exception:
                    await message.reply_text("Invalid quantity. Send a positive number like `12.5`.")
                    return

                who = context.user_data.get("milk_who")
                animal_id = None
                if who and who.lower() != "bulk":
                    resolved = await _resolve_user_and_farm_preferring_membership(update.effective_user.id)
                    if not resolved or not resolved.get("farm"):
                        await message.reply_text("⚠️ Farm not found.")
                        _clear_flow(context.user_data)
                        return
                    farm_id = resolved["farm"]["id"]
                    animals = await async_list_animals(farm_id=farm_id, limit=500)
                    matched = None
                    for a in animals:
                        if a.get("tag", "").lower() == who.lower() or (a.get("name") and a.get("name").lower() == who.lower()):
                            matched = a
                            break
                    if not matched:
                        await message.reply_text("Animal tag/name not found. Send `bulk` or try again with valid tag/name. Returning to 'who' step.")
                        context.user_data["milk_step"] = "who"
                        return
                    animal_id = matched.get("id")

                context.user_data["milk_pending_qty"] = qty
                context.user_data["milk_pending_animal_id"] = animal_id
                context.user_data["milk_step"] = "date"
                await message.reply_text("Optional: send date YYYY-MM-DD to record on a specific day, or send `-` to use today:")
                return

            if step == "date":
                if text == "-" or text == "":
                    date_val = datetime.date.today().isoformat()
                else:
                    try:
                        datetime.datetime.strptime(text, "%Y-%m-%d")
                        date_val = text
                    except Exception:
                        await message.reply_text("Invalid date format. Use YYYY-MM-DD or `-`.")
                        return

                qty = context.user_data.get("milk_pending_qty")
                animal_id = context.user_data.get("milk_pending_animal_id")
                user_row = await async_get_user_by_telegram(update.effective_user.id)
                recorded_by = user_row.get("id") if user_row else None
                resolved = await _resolve_user_and_farm_preferring_membership(update.effective_user.id)
                farm_id = resolved["farm"]["id"] if resolved and resolved.get("farm") else None
                payload = {
                    "farm_id": farm_id,
                    "animal_id": animal_id,
                    "quantity": qty,
                    "recorded_by": recorded_by,
                    "date": date_val,
                    "note": None
                }
                out = await _db_insert("milk_production", payload)
                if out.get("error"):
                    await message.reply_text("❌ Failed to record milk. Try again later.")
                else:
                    await message.reply_text(f"✅ Recorded {qty} L for {'animal '+(animal_id or 'bulk')}.")
                _clear_flow(context.user_data)
                return

        # EDIT flow (legacy)
        if flow == "edit":
            perm = await _ensure_milk_permission(update, context, must_have_edit=True)
            if not perm:
                _clear_flow(context.user_data)
                return

            step = context.user_data.get("milk_edit_step")
            record_id = context.user_data.get("milk_edit_id")
            if not record_id:
                await message.reply_text("Edit flow lost the record id. Cancelled.")
                _clear_flow(context.user_data)
                return

            if step == "qty":
                if text == "-" or text == "":
                    await message.reply_text("Edit cancelled.")
                    _clear_flow(context.user_data)
                    return
                try:
                    qty = float(text.replace(",", "."))
                    if qty <= 0:
                        raise ValueError()
                except Exception:
                    await message.reply_text("Invalid quantity. Send a positive number like `12.5` or `-` to cancel.")
                    return
                context.user_data["milk_edit_qty"] = qty
                context.user_data["milk_edit_step"] = "date"
                await message.reply_text("Send new date YYYY-MM-DD or `-` to keep existing:")
                return

            if step == "date":
                if text == "-" or text == "":
                    date_val = None
                else:
                    try:
                        datetime.datetime.strptime(text, "%Y-%m-%d")
                        date_val = text
                    except Exception:
                        await message.reply_text("Invalid date format. Use YYYY-MM-DD or `-` to keep existing.")
                        return
                context.user_data["milk_edit_date"] = date_val
                context.user_data["milk_edit_step"] = "note"
                await message.reply_text("Optional: send a note or `-` to keep existing / clear:")
                return

            if step == "note":
                note = None if text == "-" else text
                upd: Dict[str, Any] = {}
                if "milk_edit_qty" in context.user_data:
                    upd["quantity"] = context.user_data.get("milk_edit_qty")
                if "milk_edit_date" in context.user_data:
                    if context.user_data.get("milk_edit_date") is not None:
                        upd["date"] = context.user_data.get("milk_edit_date")
                if note is not None:
                    upd["note"] = note
                if not upd:
                    await message.reply_text("No changes provided. Cancelled.")
                    _clear_flow(context.user_data)
                    return
                out = await asyncio.to_thread(lambda: supabase.table("milk_production").update(upd).eq("id", record_id).execute())
                if getattr(out, "error", None) or (isinstance(out, dict) and out.get("error")):
                    await message.reply_text("❌ Failed to update record.")
                else:
                    await message.reply_text("✅ Milk record updated.")
                page = context.user_data.get("milk_edit_return_page", 0)
                _clear_flow(context.user_data)
                await router(update, context, f"milk:list:{page}")
                return

    except Exception:
        logger.exception("Error in milk flow")
        try:
            await update.effective_message.reply_text("❌ Error processing input. Flow cancelled.")
        except Exception:
            pass
        _clear_flow(context.user_data)

milk_handlers["handle_text"] = handle_text















'''
# aboutmilk.py (upgraded — direct "Add bulk" + short callback tokens for per-animal)
import asyncio
import logging
import datetime
from typing import List, Optional, Dict, Any

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from farmcore import (
    async_get_user_with_farm_by_telegram,
    async_list_animals,
    async_record_milk,
    async_list_milk,
    async_get_user_by_telegram,
    supabase,
)

logger = logging.getLogger(__name__)
milk_handlers = {}

_PAGE_SIZE = 10  # smaller pages so UI fits

# --------------------
# Small async DB helpers (use asyncio.to_thread for supabase operations)
# --------------------
async def _db_insert(table: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        resp = await asyncio.to_thread(lambda: supabase.table(table).insert(payload).execute())
        data = getattr(resp, "data", None) or (resp.get("data") if isinstance(resp, dict) else None)
        error = getattr(resp, "error", None) or (resp.get("error") if isinstance(resp, dict) else None)
        return {"data": data, "error": error}
    except Exception as exc:
        logger.exception("db insert failed")
        return {"error": str(exc)}

async def _db_select(table: str, eq_filter: Optional[tuple] = None, limit: Optional[int] = None) -> Dict[str, Any]:
    try:
        def _fn():
            q = supabase.table(table).select("*")
            if eq_filter:
                q = q.eq(eq_filter[0], eq_filter[1])
            if limit:
                q = q.limit(limit)
            return q.execute()
        resp = await asyncio.to_thread(_fn)
        data = getattr(resp, "data", None) or (resp.get("data") if isinstance(resp, dict) else None)
        error = getattr(resp, "error", None) or (resp.get("error") if isinstance(resp, dict) else None)
        return {"data": data, "error": error}
    except Exception:
        logger.exception("db select failed")
        return {"error": "db-select-failed"}

async def _db_delete(table: str, record_id: str) -> Dict[str, Any]:
    try:
        resp = await asyncio.to_thread(lambda: supabase.table(table).delete().eq("id", record_id).execute())
        data = getattr(resp, "data", None) or (resp.get("data") if isinstance(resp, dict) else None)
        error = getattr(resp, "error", None) or (resp.get("error") if isinstance(resp, dict) else None)
        return {"data": data, "error": error}
    except Exception:
        logger.exception("db delete failed")
        return {"error": "db-delete-failed"}

# --------------------
# Utilities & formatting
# --------------------
def _format_milk_line(r: Dict[str, Any]) -> str:
    date = r.get("date") or r.get("created_at") or "—"
    qty = r.get("quantity") or 0
    aid = r.get("animal_id") or "bulk"
    return f"• {date} — {qty} L — animal: `{aid}` (id: `{r.get('id')}`)"

def _mk_milk_list_text(records: List[dict]) -> str:
    if not records:
        return "No milk records found."
    lines = [ _format_milk_line(r) for r in records ]
    total = sum(float(r.get("quantity") or 0) for r in records)
    header = f"*Recent milk records* — total shown: {len(records)}  •  Sum: {total:.2f} L\n\n"
    return header + "\n".join(lines)

def _clear_flow(context_user_data: dict, prefix: str = "milk"):
    for k in list(context_user_data.keys()):
        if k.startswith((prefix,)):
            context_user_data.pop(k, None)

# --------------------
# Menu & Router
# --------------------
async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add bulk (quick)", callback_data="milk:bulk_start")],
        [InlineKeyboardButton("🐄 Per-cow (per-animal)", callback_data="milk:per_start")],
        [InlineKeyboardButton("📄 Recent Records", callback_data="milk:list:0")],
        [InlineKeyboardButton("📊 Summary (monthly)", callback_data="milk:summary")],
        [InlineKeyboardButton("🔙 Back", callback_data="skip")],
    ])
    try:
        if update.callback_query:
            await update.callback_query.edit_message_text("🥛 Milk — choose an action:", reply_markup=kb)
        else:
            await update.message.reply_text("🥛 Milk — choose an action:", reply_markup=kb)
    except Exception:
        logger.exception("Failed to show milk menu")

milk_handlers["menu"] = menu

# --------------------
# Per-animal helper: render animals page with short tokens
# --------------------
async def _render_animals_page_with_tokens(update: Update, context: ContextTypes.DEFAULT_TYPE, farm_id: str, date_str: str, page: int = 0):
    animals = await async_list_animals(farm_id=farm_id, limit=1000)
    total = len(animals)
    start = page * _PAGE_SIZE
    end = start + _PAGE_SIZE
    page_animals = animals[start:end]

    # create / extend token map stored in user_data
    token_map: Dict[str, str] = context.user_data.get("milk_animal_map", {})
    # token index base based on global mapping length to avoid collisions during session
    base_idx = len(token_map)
    kb_rows = []
    header = f"*Select animal to record for {date_str}:* (page {page+1}/{max(1, (total + _PAGE_SIZE -1)//_PAGE_SIZE)})\n"
    for i, a in enumerate(page_animals):
        token = f"T{base_idx + i}"  # short token
        token_map[token] = a.get("id")
        label = f"{a.get('name') or a.get('tag')} ({a.get('tag')})"
        # callback_data short: milk:ps:<token>:<date>:<page>
        cb = f"milk:ps:{token}:{date_str}:{page}"
        kb_rows.append([InlineKeyboardButton(label, callback_data=cb)])

    # save token map
    context.user_data["milk_animal_map"] = token_map

    # bulk row (bulk uses token 'BULK')
    kb_rows.append([InlineKeyboardButton("➕ Add bulk (whole farm)", callback_data=f"milk:pb:{date_str}:{page}")])

    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"milk:pp:{date_str}:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"milk:pp:{date_str}:{page+1}"))
    if nav:
        kb_rows.append(nav)
    kb_rows.append([InlineKeyboardButton("🔙 Cancel", callback_data="skip")])
    kb = InlineKeyboardMarkup(kb_rows)

    text = header + "\n(press a cow to record milk quickly)"
    try:
        if update.callback_query:
            await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
        else:
            await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
    except Exception:
        logger.exception("Failed rendering animals page")

# --------------------
# Record helper: insert and notify
# --------------------
async def _record_milk_and_notify_simple(update: Update, context: ContextTypes.DEFAULT_TYPE, farm_id: str, animal_id: Optional[str], qty: float, date_str: str):
    # recorded_by
    user_row = await async_get_user_by_telegram(update.effective_user.id)
    recorded_by = user_row.get("id") if user_row else None
    payload = {
        "farm_id": farm_id,
        "animal_id": animal_id,
        "quantity": qty,
        "recorded_by": recorded_by,
        "date": date_str,
        "note": None,
    }
    out = await _db_insert("milk_production", payload)
    if out.get("error"):
        txt = "❌ Failed to record milk. Try again later."
    else:
        who = f"animal {animal_id}" if animal_id else "whole farm (bulk)"
        txt = f"✅ Recorded {qty} L for {who} on {date_str}."
    try:
        if update.callback_query:
            # answer callback then edit
            try:
                await update.callback_query.answer()
            except Exception:
                pass
            try:
                await update.callback_query.edit_message_text(txt)
            except Exception:
                # fallback: send new message
                await update.effective_message.reply_text(txt)
        else:
            await update.effective_message.reply_text(txt)
    except Exception:
        logger.exception("Failed to send confirmation message")

# --------------------
# Router: handles bulk + per-animal and existing list/view/edit flows
# --------------------
async def router(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str):
    """
    actions (short forms to keep callback_data small):
      - bulk_start
      - bulk_date:<YYYY-MM-DD>
      - bulk_pickdate
      - bulk_qty:<date>:<qty>
      - bulk_custom:<date>  (set waiting-for-input)
      - per_start
      - per_date:<YYYY-MM-DD>
      - pp:<date>:<page>   (per page navigation)
      - ps:<token>:<date>:<page>  (select animal token)
      - pb:<date>:<page>  (per-animal bulk quick)
      - per_qty:<token_or_BULK>:<date>:<qty>  (quick qty)
      - per_custom:<token_or_BULK>:<date>:<page>  (ask typed qty)
      - plus existing add/list/view/edit/delete/summary actions kept
    """
    try:
        parts = action.split(":")
        cmd = parts[0] if parts else ""

        # ---- BULK (direct quick add) ----
        if cmd == "bulk_start":
            # ask date: Today or pick date
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📅 Today", callback_data=f"milk:bulk_date:{datetime.date.today().isoformat()}")],
                [InlineKeyboardButton("🖊 Pick date (type)", callback_data="milk:bulk_pickdate")],
                [InlineKeyboardButton("🔙 Cancel", callback_data="skip")],
            ])
            if update.callback_query:
                await update.callback_query.edit_message_text("Choose date for bulk record:", reply_markup=kb)
            else:
                await update.message.reply_text("Choose date for bulk record:", reply_markup=kb)
            return

        if cmd == "bulk_pickdate":
            context.user_data["milk_bulk_waiting_date"] = True
            if update.callback_query:
                await update.callback_query.edit_message_text("Please send date YYYY-MM-DD (e.g. 2025-08-25) or /cancel:")
            else:
                await update.message.reply_text("Please send date YYYY-MM-DD (e.g. 2025-08-25) or /cancel:")
            return

        if cmd == "bulk_date" and len(parts) >= 2:
            date_str = parts[1]
            # show quick amounts
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("1 L", callback_data=f"milk:bulk_qty:{date_str}:1"),
                 InlineKeyboardButton("5 L", callback_data=f"milk:bulk_qty:{date_str}:5")],
                [InlineKeyboardButton("10 L", callback_data=f"milk:bulk_qty:{date_str}:10"),
                 InlineKeyboardButton("✏️ Custom", callback_data=f"milk:bulk_custom:{date_str}")],
                [InlineKeyboardButton("🔙 Cancel", callback_data="skip")],
            ])
            txt = f"Record bulk milk for {date_str}. Choose quick amount or Custom."
            if update.callback_query:
                await update.callback_query.edit_message_text(txt, reply_markup=kb)
            else:
                await update.message.reply_text(txt, reply_markup=kb)
            return

        if cmd == "bulk_qty" and len(parts) >= 3:
            date_str = parts[1]
            try:
                qty = float(parts[2])
            except Exception:
                qty = None
            if qty is None:
                if update.callback_query:
                    await update.callback_query.edit_message_text("Invalid quick quantity.")
                else:
                    await update.message.reply_text("Invalid quick quantity.")
                return
            # perform insert
            combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
            if not combined or not combined.get("farm"):
                if update.callback_query:
                    await update.callback_query.edit_message_text("⚠️ Farm not found.")
                else:
                    await update.message.reply_text("⚠️ Farm not found.")
                return
            farm_id = combined["farm"]["id"]
            await _record_milk_and_notify_simple(update, context, farm_id, None, qty, date_str)
            return

        if cmd == "bulk_custom" and len(parts) >= 2:
            date_str = parts[1]
            # set waiting state for custom amount
            context.user_data["milk_bulk_custom"] = {"date": date_str}
            if update.callback_query:
                await update.callback_query.edit_message_text("Send custom quantity in liters (e.g. 12.5) or /cancel:")
            else:
                await update.message.reply_text("Send custom quantity in liters (e.g. 12.5) or /cancel:")
            return

        # ---- PER-ANIMAL (starts) ----
        if cmd == "per_start":
            context.user_data["milk_flow_per"] = True
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📅 Today", callback_data=f"milk:per_date:{datetime.date.today().isoformat()}")],
                [InlineKeyboardButton("🖊 Pick date (type)", callback_data="milk:per_pickdate")],
                [InlineKeyboardButton("🔙 Cancel", callback_data="skip")],
            ])
            if update.callback_query:
                await update.callback_query.edit_message_text("Choose date for per-cow recording:", reply_markup=kb)
            else:
                await update.message.reply_text("Choose date for per-cow recording:", reply_markup=kb)
            return

        if cmd == "per_pickdate":
            context.user_data["milk_per_waiting_date"] = True
            if update.callback_query:
                await update.callback_query.edit_message_text("Please send date YYYY-MM-DD (e.g. 2025-08-25) or /cancel:")
            else:
                await update.message.reply_text("Please send date YYYY-MM-DD (e.g. 2025-08-25) or /cancel:")
            return

        if cmd == "per_date" and len(parts) >= 2:
            date_str = parts[1]
            combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
            if not combined or not combined.get("farm"):
                txt = "⚠️ Farm not found. Please register a farm first with /start."
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt)
                else:
                    await update.message.reply_text(txt)
                return
            farm_id = combined["farm"]["id"]
            await _render_animals_page_with_tokens(update, context, farm_id, date_str, page=0)
            return

        # per page navigation (pp)
        if cmd == "pp" and len(parts) >= 3:
            date_str = parts[1]
            page = int(parts[2]) if parts[2].isdigit() else 0
            combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
            if not combined or not combined.get("farm"):
                if update.callback_query:
                    await update.callback_query.edit_message_text("⚠️ Farm not found.")
                else:
                    await update.message.reply_text("⚠️ Farm not found.")
                return
            farm_id = combined["farm"]["id"]
            await _render_animals_page_with_tokens(update, context, farm_id, date_str, page=page)
            return

        # per select (short token) -> show quick amounts or custom
        if cmd == "ps" and len(parts) >= 4:
            token = parts[1]
            date_str = parts[2]
            page = int(parts[3]) if parts[3].isdigit() else 0
            token_map: Dict[str, str] = context.user_data.get("milk_animal_map", {})
            animal_id = token_map.get(token)
            if not animal_id:
                # token not found (stale) — re-render page to refresh tokens
                combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
                if not combined or not combined.get("farm"):
                    if update.callback_query:
                        await update.callback_query.edit_message_text("⚠️ Farm not found.")
                    else:
                        await update.message.reply_text("⚠️ Farm not found.")
                    return
                farm_id = combined["farm"]["id"]
                await _render_animals_page_with_tokens(update, context, farm_id, date_str, page=page)
                return
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("0.5 L", callback_data=f"milk:per_qty:{token}:{date_str}:0.5"),
                 InlineKeyboardButton("1 L", callback_data=f"milk:per_qty:{token}:{date_str}:1")],
                [InlineKeyboardButton("2 L", callback_data=f"milk:per_qty:{token}:{date_str}:2"),
                 InlineKeyboardButton("✏️ Custom", callback_data=f"milk:per_custom:{token}:{date_str}:{page}")],
                [InlineKeyboardButton("🔙 Back", callback_data=f"milk:pp:{date_str}:{page}")],
            ])
            txt = f"Record milk for this animal on {date_str}. Choose quick amount or Custom."
            if update.callback_query:
                await update.callback_query.edit_message_text(txt, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text(txt, parse_mode="Markdown", reply_markup=kb)
            return

        # per-animal bulk quick from list (pb)
        if cmd == "pb" and len(parts) >= 3:
            date_str = parts[1]
            page = int(parts[2]) if parts[2].isdigit() else 0
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("1 L", callback_data=f"milk:per_qty:BULK:{date_str}:1"),
                 InlineKeyboardButton("5 L", callback_data=f"milk:per_qty:BULK:{date_str}:5")],
                [InlineKeyboardButton("10 L", callback_data=f"milk:per_qty:BULK:{date_str}:10"),
                 InlineKeyboardButton("✏️ Custom", callback_data=f"milk:per_custom:BULK:{date_str}:{page}")],
                [InlineKeyboardButton("🔙 Back", callback_data=f"milk:pp:{date_str}:{page}")],
            ])
            txt = f"Record bulk (whole farm) for {date_str}. Choose quick amount or Custom."
            if update.callback_query:
                await update.callback_query.edit_message_text(txt, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text(txt, parse_mode="Markdown", reply_markup=kb)
            return

        # quick per quantity (token or BULK)
        if cmd == "per_qty" and len(parts) >= 4:
            token = parts[1]
            date_str = parts[2]
            try:
                qty = float(parts[3])
            except Exception:
                qty = None
            if qty is None:
                if update.callback_query:
                    await update.callback_query.edit_message_text("Invalid quick quantity.")
                else:
                    await update.message.reply_text("Invalid quick quantity.")
                return
            token_map: Dict[str, str] = context.user_data.get("milk_animal_map", {})
            animal_id = None if token == "BULK" else token_map.get(token)
            combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
            if not combined or not combined.get("farm"):
                if update.callback_query:
                    await update.callback_query.edit_message_text("⚠️ Farm not found.")
                else:
                    await update.message.reply_text("⚠️ Farm not found.")
                return
            farm_id = combined["farm"]["id"]
            await _record_milk_and_notify_simple(update, context, farm_id, animal_id, qty, date_str)
            return

        # per custom -> set waiting state for typed qty
        if cmd == "per_custom" and len(parts) >= 4:
            token = parts[1]
            date_str = parts[2]
            page = int(parts[3]) if parts[3].isdigit() else 0
            # store token and date for next typed message
            context.user_data["milk_per_custom"] = {"token": token, "date": date_str, "page": page}
            if update.callback_query:
                await update.callback_query.edit_message_text("Send custom quantity in liters (e.g. 4.25) or /cancel:")
            else:
                await update.message.reply_text("Send custom quantity in liters (e.g. 4.25) or /cancel:")
            return

        # --- FALL BACK to other existing flows (list/view/edit/delete/summary) ---
        # Reuse earlier 'add/list/view/edit/delete/summary' behavior (kept short)
        if cmd == "add":
            # quick original add flow
            context.user_data["milk_flow"] = "add"
            context.user_data["milk_step"] = "who"
            if update.callback_query:
                await update.callback_query.edit_message_text("Record milk — send the *animal tag* (or `bulk` for whole-farm):", parse_mode="Markdown")
            else:
                await update.message.reply_text("Record milk — send the *animal tag* (or `bulk` for whole-farm):", parse_mode="Markdown")
            return

        if cmd == "list":
            page = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
            combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
            if not combined or not combined.get("farm"):
                text = "⚠️ Farm not found. Please register a farm first with /start."
                if update.callback_query:
                    await update.callback_query.edit_message_text(text)
                else:
                    await update.message.reply_text(text)
                return
            farm_id = combined["farm"]["id"]
            records = await async_list_milk(farm_id=farm_id, limit=200)
            total = len(records)
            start = page * _PAGE_SIZE
            end = start + _PAGE_SIZE
            page_records = records[start:end]
            header = f"*Milk records* — page {page+1} / {max(1, (total + _PAGE_SIZE -1)//_PAGE_SIZE)}\n\n"
            text = header + (_mk_milk_list_text(page_records) if page_records else "No records on this page.")
            kb_rows = []
            for r in page_records:
                label = f"{r.get('date') or r.get('created_at') or 'date'} — {r.get('quantity')} L"
                # keep callback_data short: milk:v:<id>:<page>  (id is still a UUID but should be <64 bytes; use with care)
                cb = f"milk:v:{r.get('id')}:{page}"
                kb_rows.append([InlineKeyboardButton(label, callback_data=cb)])
            nav = []
            if start > 0:
                nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"milk:list:{page-1}"))
            if end < total:
                nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"milk:list:{page+1}"))
            if nav:
                kb_rows.append(nav)
            kb_rows.append([InlineKeyboardButton("➕ Record new", callback_data="milk:add"), InlineKeyboardButton("🔙 Back", callback_data="skip")])
            kb = InlineKeyboardMarkup(kb_rows)
            if update.callback_query:
                await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
            return

        if cmd == "v" and len(parts) >= 2:
            # view single record (milk:v:<id>:<page>)
            record_id = parts[1]
            page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            out = await _db_select("milk_production", eq_filter=("id", record_id), limit=1)
            rows = out.get("data") or []
            if not rows:
                txt = "⚠️ Milk record not found."
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt)
                else:
                    await update.message.reply_text(txt)
                return
            r = rows[0]
            text = (
                f"🥛 *Milk record*\n\n"
                f"ID: `{r.get('id')}`\n"
                f"Date: {r.get('date')}\n"
                f"Quantity: {r.get('quantity')} L\n"
                f"Animal ID: `{r.get('animal_id') or 'bulk'}`\n"
                f"Recorded by: `{r.get('recorded_by') or 'unknown'}`\n"
                f"Note: { (r.get('note') or '—') }\n"
            )
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ Edit", callback_data=f"milk:edit:{record_id}:{page}"),
                 InlineKeyboardButton("🗑 Delete", callback_data=f"milk:confirm_delete:{record_id}:{page}")],
                [InlineKeyboardButton("🔙 Back to list", callback_data=f"milk:list:{page}")]
            ])
            if update.callback_query:
                await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
            return

        if cmd == "confirm_delete" and len(parts) >= 2:
            record_id = parts[1]
            page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Yes, delete", callback_data=f"milk:delete:{record_id}:{page}"),
                 InlineKeyboardButton("No, cancel", callback_data=f"milk:v:{record_id}:{page}")]
            ])
            if update.callback_query:
                await update.callback_query.edit_message_text("⚠️ Are you sure you want to permanently delete this milk record?", reply_markup=kb)
            else:
                await update.message.reply_text("⚠️ Are you sure you want to permanently delete this milk record?", reply_markup=kb)
            return

        if cmd == "delete" and len(parts) >= 2:
            record_id = parts[1]
            page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            out = await _db_delete("milk_production", record_id)
            if out.get("error"):
                txt = "❌ Failed to delete record."
            else:
                txt = "✅ Milk record deleted."
            if update.callback_query:
                await update.callback_query.edit_message_text(txt)
            else:
                await update.message.reply_text(txt)
            await router(update, context, f"milk:list:{page}")
            return

        if cmd == "edit" and len(parts) >= 2:
            record_id = parts[1]
            page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            context.user_data["milk_flow"] = "edit"
            context.user_data["milk_edit_id"] = record_id
            context.user_data["milk_edit_step"] = "qty"
            context.user_data["milk_edit_return_page"] = page
            if update.callback_query:
                await update.callback_query.edit_message_text("Send new quantity in liters (e.g. 12.5) or `-` to cancel:")
            else:
                await update.message.reply_text("Send new quantity in liters (e.g. 12.5) or `-` to cancel:")
            return

        if cmd == "summary":
            combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
            if not combined or not combined.get("farm"):
                txt = "⚠️ Farm not found. Please register a farm first with /start."
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt)
                else:
                    await update.message.reply_text(txt)
                return
            farm_id = combined["farm"]["id"]
            records = await async_list_milk(farm_id=farm_id, limit=1000)
            cutoff = datetime.date.today() - datetime.timedelta(days=30)
            recent = [r for r in records if (r.get("date") and datetime.date.fromisoformat(str(r.get("date"))) >= cutoff) or (not r.get("date") and True)]
            total = sum(float(r.get("quantity") or 0) for r in recent)
            per_animal: Dict[str, float] = {}
            for r in recent:
                aid = r.get("animal_id") or "bulk"
                per_animal[aid] = per_animal.get(aid, 0.0) + float(r.get("quantity") or 0)
            parts = [f"*Last 30 days total:* {total:.2f} L\n\n*By animal:*"]
            for aid, s in sorted(per_animal.items(), key=lambda kv: -kv[1])[:20]:
                parts.append(f"• `{aid}`: {s:.2f} L")
            text = "\n".join(parts)
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="skip")]])
            if update.callback_query:
                await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
            return

        # unknown action
        if update.callback_query:
            try:
                await update.callback_query.answer("Action not recognized.")
            except Exception:
                pass
        else:
            await update.message.reply_text("Action not recognized.")
    except Exception:
        logger.exception("Error routing milk action")
        try:
            if update.callback_query:
                await update.callback_query.edit_message_text("❌ Error handling milk action.")
            else:
                await update.message.reply_text("❌ Error handling milk action.")
        except Exception:
            pass

milk_handlers["router"] = router

# --------------------
# Flow handler for typed inputs (bulk custom, per-animal custom, add/edit legacy)
# --------------------
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 1) bulk custom typed
    bulk_custom = context.user_data.get("milk_bulk_custom")
    if bulk_custom:
        text = (update.effective_message.text or "").strip()
        if text.lower() in ("/cancel", "cancel", "-"):
            _clear_flow(context.user_data)
            await update.effective_message.reply_text("Cancelled bulk custom.")
            return
        try:
            qty = float(text.replace(",", "."))
            if qty <= 0:
                raise ValueError()
        except Exception:
            await update.effective_message.reply_text("Invalid quantity. Send a positive number like `12.5` or /cancel.")
            return
        date_str = bulk_custom.get("date")
        combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
        if not combined or not combined.get("farm"):
            await update.effective_message.reply_text("⚠️ Farm not found.")
            _clear_flow(context.user_data)
            return
        farm_id = combined["farm"]["id"]
        await _record_milk_and_notify_simple(update, context, farm_id, None, qty, date_str)
        _clear_flow(context.user_data)
        return

    # 2) per-animal custom typed
    per_custom = context.user_data.get("milk_per_custom")
    if per_custom:
        text = (update.effective_message.text or "").strip()
        if text.lower() in ("/cancel", "cancel", "-"):
            _clear_flow(context.user_data)
            await update.effective_message.reply_text("Cancelled custom amount.")
            return
        try:
            qty = float(text.replace(",", "."))
            if qty <= 0:
                raise ValueError()
        except Exception:
            await update.effective_message.reply_text("Invalid quantity. Send a positive number like `4.25` or /cancel.")
            return
        token = per_custom.get("token")
        date_str = per_custom.get("date")
        token_map: Dict[str, str] = context.user_data.get("milk_animal_map", {})
        animal_id = None if token == "BULK" else token_map.get(token)
        # If token not found, abort and ask user to restart per-animal flow
        combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
        if not combined or not combined.get("farm"):
            await update.effective_message.reply_text("⚠️ Farm not found.")
            _clear_flow(context.user_data)
            return
        farm_id = combined["farm"]["id"]
        await _record_milk_and_notify_simple(update, context, farm_id, animal_id, qty, date_str)
        _clear_flow(context.user_data)
        return

    # 3) legacy quick-add / edit flows (kept from your previous code)
    flow = context.user_data.get("milk_flow", "")
    if not flow:
        return

    message = update.effective_message
    if not message or not message.text:
        return

    text = message.text.strip()
    try:
        # ADD flow (legacy quick)
        if flow == "add":
            step = context.user_data.get("milk_step", "who")
            if step == "who":
                who = text.strip()
                if not who:
                    await message.reply_text("Please send an animal tag or `bulk`.")
                    return
                context.user_data["milk_who"] = who
                context.user_data["milk_step"] = "qty"
                await message.reply_text("Send quantity in liters (e.g. 12.5):")
                return

            if step == "qty":
                try:
                    qty = float(text.replace(",", "."))
                    if qty <= 0:
                        raise ValueError("non-positive")
                except Exception:
                    await message.reply_text("Invalid quantity. Send a positive number like `12.5`.")
                    return

                who = context.user_data.get("milk_who")
                animal_id = None
                if who and who.lower() != "bulk":
                    # look for animal by tag/name
                    combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
                    if not combined or not combined.get("farm"):
                        await message.reply_text("⚠️ Farm not found.")
                        _clear_flow(context.user_data)
                        return
                    farm_id = combined["farm"]["id"]
                    animals = await async_list_animals(farm_id=farm_id, limit=500)
                    matched = None
                    for a in animals:
                        if a.get("tag", "").lower() == who.lower() or (a.get("name") and a.get("name").lower() == who.lower()):
                            matched = a
                            break
                    if not matched:
                        await message.reply_text("Animal tag/name not found. Send `bulk` or try again with valid tag/name. Returning to 'who' step.")
                        context.user_data["milk_step"] = "who"
                        return
                    animal_id = matched.get("id")

                context.user_data["milk_pending_qty"] = qty
                context.user_data["milk_pending_animal_id"] = animal_id
                context.user_data["milk_step"] = "date"
                await message.reply_text("Optional: send date YYYY-MM-DD to record on a specific day, or send `-` to use today:")
                return

            if step == "date":
                if text == "-" or text == "":
                    date_val = datetime.date.today().isoformat()
                else:
                    try:
                        datetime.datetime.strptime(text, "%Y-%m-%d")
                        date_val = text
                    except Exception:
                        await message.reply_text("Invalid date format. Use YYYY-MM-DD or `-`.")
                        return

                qty = context.user_data.get("milk_pending_qty")
                animal_id = context.user_data.get("milk_pending_animal_id")
                user_row = await async_get_user_by_telegram(update.effective_user.id)
                recorded_by = user_row.get("id") if user_row else None
                combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
                farm_id = combined["farm"]["id"] if combined and combined.get("farm") else None
                payload = {
                    "farm_id": farm_id,
                    "animal_id": animal_id,
                    "quantity": qty,
                    "recorded_by": recorded_by,
                    "date": date_val,
                    "note": None
                }
                out = await _db_insert("milk_production", payload)
                if out.get("error"):
                    await message.reply_text("❌ Failed to record milk. Try again later.")
                else:
                    await message.reply_text(f"✅ Recorded {qty} L for {'animal '+(animal_id or 'bulk')}.")
                _clear_flow(context.user_data)
                return

        # EDIT flow (legacy)
        if flow == "edit":
            step = context.user_data.get("milk_edit_step")
            record_id = context.user_data.get("milk_edit_id")
            if not record_id:
                await message.reply_text("Edit flow lost the record id. Cancelled.")
                _clear_flow(context.user_data)
                return

            if step == "qty":
                if text == "-" or text == "":
                    await message.reply_text("Edit cancelled.")
                    _clear_flow(context.user_data)
                    return
                try:
                    qty = float(text.replace(",", "."))
                    if qty <= 0:
                        raise ValueError()
                except Exception:
                    await message.reply_text("Invalid quantity. Send a positive number like `12.5` or `-` to cancel.")
                    return
                context.user_data["milk_edit_qty"] = qty
                context.user_data["milk_edit_step"] = "date"
                await message.reply_text("Send new date YYYY-MM-DD or `-` to keep existing:")
                return

            if step == "date":
                if text == "-" or text == "":
                    date_val = None
                else:
                    try:
                        datetime.datetime.strptime(text, "%Y-%m-%d")
                        date_val = text
                    except Exception:
                        await message.reply_text("Invalid date format. Use YYYY-MM-DD or `-` to keep existing.")
                        return
                context.user_data["milk_edit_date"] = date_val
                context.user_data["milk_edit_step"] = "note"
                await message.reply_text("Optional: send a note or `-` to keep existing / clear:")
                return

            if step == "note":
                note = None if text == "-" else text
                upd: Dict[str, Any] = {}
                if "milk_edit_qty" in context.user_data:
                    upd["quantity"] = context.user_data.get("milk_edit_qty")
                if "milk_edit_date" in context.user_data:
                    if context.user_data.get("milk_edit_date") is not None:
                        upd["date"] = context.user_data.get("milk_edit_date")
                if note is not None:
                    upd["note"] = note
                if not upd:
                    await message.reply_text("No changes provided. Cancelled.")
                    _clear_flow(context.user_data)
                    return
                out = await asyncio.to_thread(lambda: supabase.table("milk_production").update(upd).eq("id", record_id).execute())
                if getattr(out, "error", None) or (isinstance(out, dict) and out.get("error")):
                    await message.reply_text("❌ Failed to update record.")
                else:
                    await message.reply_text("✅ Milk record updated.")
                page = context.user_data.get("milk_edit_return_page", 0)
                _clear_flow(context.user_data)
                await router(update, context, f"milk:list:{page}")
                return

    except Exception:
        logger.exception("Error in milk flow")
        try:
            await update.effective_message.reply_text("❌ Error processing input. Flow cancelled.")
        except Exception:
            pass
        _clear_flow(context.user_data)

milk_handlers["handle_text"] = handle_text
'''