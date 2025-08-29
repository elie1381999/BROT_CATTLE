from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
import asyncio
import datetime
import logging
from typing import Optional, List, Dict, Any

from farmcore import supabase, get_user_by_telegram_id, create_user
from farmcore_role import find_user_primary_farm, user_has_permission, get_user_role_in_farm

logger = logging.getLogger(__name__)
money_handlers = {}

def _mk_skip():
    return InlineKeyboardMarkup([[InlineKeyboardButton("/skip", callback_data='skip')]])

def _reply_or_edit(update: Update, text: str, reply_markup=None):
    """Return coroutine for editing/replying. Caller should await."""
    if update.callback_query:
        return update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return update.message.reply_text(text, reply_markup=reply_markup)

def ensure_app_user_uuid(telegram_id: int, full_name: str = None) -> Optional[str]:
    """Ensure user exists in app_users and return their UUID."""
    u = get_user_by_telegram_id(telegram_id)
    if not u:
        created = create_user(telegram_id, full_name or str(telegram_id))
        if not created:
            logger.error("Failed to create user for telegram_id=%s", telegram_id)
            return None
        u = get_user_by_telegram_id(telegram_id)
    return u['id'] if u else None

async def _get_user_farm_id_and_role(telegram_id: int) -> Dict[str, Optional[str]]:
    """Get user's primary farm_id and role, checking both farms and farm_members."""
    user_id = ensure_app_user_uuid(telegram_id)
    if not user_id:
        logger.error("No user found for telegram_id=%s", telegram_id)
        return {"farm_id": None, "role": None}
    primary = find_user_primary_farm(user_id)
    if not primary["farm_id"]:
        logger.warning("No farm found for user_id=%s (telegram_id=%s)", user_id, telegram_id)
    return primary

# -------------------------
# Async DB helpers (run blocking supabase calls off the event loop)
# -------------------------
async def _db_insert(table: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        resp = await asyncio.to_thread(lambda: supabase.table(table).insert(payload).execute())
        data = getattr(resp, "data", None) or (resp.get("data") if isinstance(resp, dict) else None)
        error = getattr(resp, "error", None) or (resp.get("error") if isinstance(resp, dict) else None)
        return {"data": data, "error": error}
    except Exception as exc:
        logger.exception("DB insert failed for %s", table)
        return {"error": str(exc)}

async def _db_select(table: str, eq_filter: Optional[tuple] = None, order_by: Optional[tuple] = None, limit: Optional[int] = None) -> Dict[str, Any]:
    try:
        def _fn():
            q = supabase.table(table).select("*")
            if eq_filter:
                q = q.eq(eq_filter[0], eq_filter[1])
            if order_by:
                q = q.order(order_by[0], desc=not order_by[1].get("ascending", True))
            if limit:
                q = q.limit(limit)
            return q.execute()
        resp = await asyncio.to_thread(_fn)
        data = getattr(resp, "data", None) or (resp.get("data") if isinstance(resp, dict) else None)
        error = getattr(resp, "error", None) or (resp.get("error") if isinstance(resp, dict) else None)
        return {"data": data, "error": error}
    except Exception:
        logger.exception("DB select failed for %s", table)
        return {"error": "db-select-failed"}

# -------------------------
# Menu / router
# -------------------------
async def menu(update: Update, context):
    """Show finance menu if user has permission."""
    telegram_id = update.callback_query.from_user.id if update.callback_query else update.message.from_user.id
    farm_info = await _get_user_farm_id_and_role(telegram_id)
    farm_id = farm_info["farm_id"]
    role = farm_info["role"]
    
    if not farm_id:
        await _reply_or_edit(update, "⚠️ You don't have a farm yet. Register with /start first.")
        return
    
    user_id = ensure_app_user_uuid(telegram_id)
    if not user_has_permission(user_id, farm_id, "finance"):
        await _reply_or_edit(update, f"⚠️ Your role '{role}' does not have permission to access financial data.")
        return

    kb = [
        [InlineKeyboardButton("➕ Add Income", callback_data='money:add_income')],
        [InlineKeyboardButton("➖ Add Expense", callback_data='money:add_expense')],
        [InlineKeyboardButton("📊 View Records", callback_data='money:view')],
        [InlineKeyboardButton("🏠 Back", callback_data='money:back')],
        [InlineKeyboardButton("⏭️ /skip", callback_data='skip')]
    ]
    markup = InlineKeyboardMarkup(kb)
    await _reply_or_edit(update, f"💰 Finance Management (Farm ID: {farm_id})\nChoose an action:", reply_markup=markup)

money_handlers['menu'] = menu

async def router(update: Update, context, action: str):
    try:
        telegram_id = update.callback_query.from_user.id if update.callback_query else update.message.from_user.id
        farm_info = await _get_user_farm_id_and_role(telegram_id)
        farm_id = farm_info["farm_id"]
        role = farm_info["role"]
        
        if not farm_id:
            await _reply_or_edit(update, "⚠️ You don't have a farm yet. Register with /start first.")
            return
        
        user_id = ensure_app_user_uuid(telegram_id)
        if not user_has_permission(user_id, farm_id, "finance"):
            await _reply_or_edit(update, f"⚠️ Your role '{role}' does not have permission to access financial data.")
            return

        parts = action.split(':') if action else []
        cmd = parts[0] if parts else ''
        if cmd == 'add_income':
            return await start_add(update, context, kind='income', farm_id=farm_id)
        if cmd == 'add_expense':
            return await start_add(update, context, kind='expense', farm_id=farm_id)
        if cmd == 'view':
            return await view_records(update, context, farm_id=farm_id)
        if cmd == 'back':
            return await menu(update, context)
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text("Unknown finance action. Try menu.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data='money:back')]]))
    except Exception:
        logger.exception("money.router error")
        if update.callback_query:
            try:
                await update.callback_query.edit_message_text("Error in finance action.")
            except Exception:
                pass

money_handlers['router'] = router

# -------------------------
# Add flow (3 steps)
# -------------------------
async def start_add(update: Update, context, kind: str = 'income', farm_id: str = None):
    """Start adding income or expense, ensuring permission."""
    telegram_id = update.callback_query.from_user.id if update.callback_query else update.message.from_user.id
    user_id = ensure_app_user_uuid(telegram_id)
    if not farm_id:
        farm_info = await _get_user_farm_id_and_role(telegram_id)
        farm_id = farm_info["farm_id"]
        role = farm_info["role"]
        if not farm_id:
            await _reply_or_edit(update, "⚠️ You don't have a farm yet. Register with /start first.")
            return
        if not user_has_permission(user_id, farm_id, "finance"):
            await _reply_or_edit(update, f"⚠️ Your role '{role}' does not have permission to access financial data.")
            return

    context.user_data['flow'] = 'money_add'
    context.user_data['money_flow'] = {'kind': kind, 'step': 'category', 'farm_id': farm_id}
    label = "income" if kind == 'income' else 'expense'
    await _reply_or_edit(update, f"Add {label} — Step 1/3\nEnter category (e.g., milk_sales) or /skip to use 'general'.", reply_markup=_mk_skip())

money_handlers['add_income'] = lambda u, c: start_add(u, c, kind='income')
money_handlers['add_expense'] = lambda u, c: start_add(u, c, kind='expense')

async def handle_text(update: Update, context):
    flow = context.user_data.get('flow')
    msg = update.message
    if not msg:
        return
    text = (msg.text or "").strip()
    
    # Universal skip handling
    if text.lower() == '/skip':
        mf = context.user_data.get('money_flow')
        if not mf:
            context.user_data.pop('flow', None)
            await update.message.reply_text("Skipped.")
            return
        step = mf.get('step')
        if step == 'category':
            mf['category'] = 'general'
            mf['step'] = 'amount'
            context.user_data['money_flow'] = mf
            await update.message.reply_text("Enter amount (e.g., 120.50) or /skip to cancel", reply_markup=_mk_skip())
            return
        if step == 'amount':
            context.user_data.pop('flow', None)
            context.user_data.pop('money_flow', None)
            await update.message.reply_text("Add cancelled.")
            return
        if step == 'date':
            mf['date'] = datetime.date.today().isoformat()

    if flow == 'money_add':
        mf = context.user_data.get('money_flow', {})
        farm_id = mf.get('farm_id')
        if not farm_id:
            await update.message.reply_text("⚠️ No farm associated. Please start again.")
            context.user_data.pop('flow', None)
            context.user_data.pop('money_flow', None)
            return
        
        telegram_id = update.message.from_user.id
        user_id = ensure_app_user_uuid(telegram_id)
        role = get_user_role_in_farm(user_id, farm_id)
        if not user_has_permission(user_id, farm_id, "finance"):
            await update.message.reply_text(f"⚠️ Your role '{role}' does not have permission to access financial data.")
            context.user_data.pop('flow', None)
            context.user_data.pop('money_flow', None)
            return

        step = mf.get('step', 'category')
        if step == 'category':
            mf['category'] = 'general' if text.lower() == '/skip' or text == '' else text
            mf['step'] = 'amount'
            context.user_data['money_flow'] = mf
            await update.message.reply_text("Step 2/3 — Enter amount (e.g., 120.50) or /skip to cancel", reply_markup=_mk_skip())
            return

        if step == 'amount':
            try:
                amount = float(text)
            except Exception:
                await update.message.reply_text("Invalid amount. Use e.g. 120.50 or /skip", reply_markup=_mk_skip())
                return
            if amount <= 0:
                await update.message.reply_text("Amount must be positive.", reply_markup=_mk_skip())
                return
            mf['amount'] = round(amount, 2)
            mf['step'] = 'date'
            context.user_data['money_flow'] = mf
            await update.message.reply_text("Step 3/3 — Enter date YYYY-MM-DD or /skip to use today", reply_markup=_mk_skip())
            return

        if step == 'date':
            if text.lower() == '/skip' or text == '':
                date_str = datetime.date.today().isoformat()
            else:
                try:
                    datetime.datetime.strptime(text, "%Y-%m-%d")
                    date_str = text
                except Exception:
                    await update.message.reply_text("Invalid date. Use YYYY-MM-DD or /skip", reply_markup=_mk_skip())
                    return
            mf['date'] = date_str

            # Build record and insert
            kind = mf.get('kind', 'income')
            creator_uuid = user_id
            record = {
                'farm_id': farm_id,
                'type': 'income' if kind == 'income' else 'expense',
                'category': mf.get('category'),
                'amount': mf.get('amount'),
                'date': mf.get('date'),
                'created_by': creator_uuid,
                'currency': 'USD'
            }
            try:
                db_out = await _db_insert('financial_records', record)
                if db_out.get("error"):
                    logger.error("Failed to save financial record: %s", db_out.get("error"))
                    await update.message.reply_text("Failed to save record. Try again later.")
                else:
                    await update.message.reply_text(f"Recorded {record['type']} {record['category']} ${record['amount']:.2f} on {record['date']}.")
            except Exception:
                logger.exception("Failed to save financial record")
                await update.message.reply_text("Failed to save record.")
            finally:
                context.user_data.pop('flow', None)
                context.user_data.pop('money_flow', None)
            return

money_handlers['handle_text'] = handle_text

# -------------------------
# View records
# -------------------------
async def view_records(update: Update, context, farm_id: str = None, limit: int = 30):
    """View financial records if user has permission."""
    is_cb = bool(update.callback_query)
    telegram_id = update.callback_query.from_user.id if is_cb else update.message.from_user.id
    user_id = ensure_app_user_uuid(telegram_id)
    
    if not farm_id:
        farm_info = await _get_user_farm_id_and_role(telegram_id)
        farm_id = farm_info["farm_id"]
        role = farm_info["role"]
        if not farm_id:
            if is_cb:
                await update.callback_query.edit_message_text("You don't have a farm yet. Register with /start first.")
            else:
                await update.message.reply_text("You don't have a farm yet. Register with /start first.")
            return
        if not user_has_permission(user_id, farm_id, "finance"):
            if is_cb:
                await update.callback_query.edit_message_text(f"Your role '{role}' does not have permission to access financial data.")
            else:
                await update.message.reply_text(f"Your role '{role}' does not have permission to access financial data.")
            return

    try:
        db_out = await _db_select('financial_records', eq_filter=('farm_id', farm_id), order_by=('date', {'ascending': False}), limit=limit)
        if db_out.get('error'):
            logger.error("Error fetching financial records: %s", db_out.get('error'))
            rows = []
        else:
            rows = db_out.get('data') or []
    except Exception:
        logger.exception("Error fetching financial records")
        rows = []

    if not rows:
        if is_cb:
            await update.callback_query.edit_message_text("No financial records found.")
        else:
            await update.message.reply_text("No financial records found.")
        return

    lines = []
    total_income = 0.0
    total_expense = 0.0
    for r in rows:
        d = r.get('date', 'N/A')
        typ = r.get('type', 'N/A')
        cat = r.get('category', 'N/A')
        try:
            amt = float(r.get('amount') or 0)
        except Exception:
            amt = 0.0
        lines.append(f"{d}: {typ} {cat} ${amt:.2f}")
        if typ == 'income':
            total_income += amt
        elif typ == 'expense':
            total_expense += amt

    summary = f"Total income: ${total_income:.2f}\nTotal expense: ${total_expense:.2f}\nNet: ${total_income - total_expense:.2f}\n\n"
    text = f"Recent Financial Records (Farm ID: {farm_id}):\n" + summary + "\n".join(lines)
    if is_cb:
        await update.callback_query.edit_message_text(text)
    else:
        await update.message.reply_text(text)

money_handlers['view'] = view_records










'''# aboutmoney.py
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
import asyncio
import datetime
import logging
from typing import Optional, List, Dict, Any

from farmcore import supabase, get_user_farm_id, get_user_by_telegram_id, create_user

logger = logging.getLogger(__name__)
money_handlers = {}

def _mk_skip():
    return InlineKeyboardMarkup([[InlineKeyboardButton("/skip", callback_data='skip')]])

def _reply_or_edit(update: Update, text: str, reply_markup=None):
    """Return coroutine for editing/replying. Caller should await."""
    if update.callback_query:
        return update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return update.message.reply_text(text, reply_markup=reply_markup)

def ensure_app_user_uuid(telegram_id: int, full_name: str = None) -> Optional[str]:
    u = get_user_by_telegram_id(telegram_id)
    if not u:
        create_user(telegram_id, full_name or str(telegram_id))
        u = get_user_by_telegram_id(telegram_id)
    return u['id'] if u else None

# -------------------------
# Async DB helpers (run blocking supabase calls off the event loop)
# -------------------------
async def _db_insert(table: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        resp = await asyncio.to_thread(lambda: supabase.table(table).insert(payload).execute())
        data = getattr(resp, "data", None) or (resp.get("data") if isinstance(resp, dict) else None)
        error = getattr(resp, "error", None) or (resp.get("error") if isinstance(resp, dict) else None)
        return {"data": data, "error": error}
    except Exception as exc:
        logger.exception("DB insert failed for %s", table)
        return {"error": str(exc)}

async def _db_select(table: str, eq_filter: Optional[tuple] = None, order_by: Optional[tuple] = None, limit: Optional[int] = None) -> Dict[str, Any]:
    try:
        def _fn():
            q = supabase.table(table).select("*")
            if eq_filter:
                q = q.eq(eq_filter[0], eq_filter[1])
            if order_by:
                q = q.order(order_by[0], desc=not order_by[1].get("ascending", True))
            if limit:
                q = q.limit(limit)
            return q.execute()
        resp = await asyncio.to_thread(_fn)
        data = getattr(resp, "data", None) or (resp.get("data") if isinstance(resp, dict) else None)
        error = getattr(resp, "error", None) or (resp.get("error") if isinstance(resp, dict) else None)
        return {"data": data, "error": error}
    except Exception:
        logger.exception("DB select failed for %s", table)
        return {"error": "db-select-failed"}

# -------------------------
# Menu / router
# -------------------------
async def menu(update: Update, context):
    kb = [
        [InlineKeyboardButton("➕ Add Income", callback_data='money:add_income')],
        [InlineKeyboardButton("➖ Add Expense", callback_data='money:add_expense')],
        [InlineKeyboardButton("📊 View Records", callback_data='money:view')],
        [InlineKeyboardButton("🏠 Back", callback_data='money:back')],
        [InlineKeyboardButton("⏭️ /skip", callback_data='skip')]
    ]
    markup = InlineKeyboardMarkup(kb)
    await _reply_or_edit(update, "💰 Finance Management\nChoose an action:", reply_markup=markup)

money_handlers['menu'] = menu

async def router(update: Update, context, action: str):
    try:
        parts = action.split(':') if action else []
        cmd = parts[0] if parts else ''
        if cmd == 'add_income':
            return await start_add(update, context, kind='income')
        if cmd == 'add_expense':
            return await start_add(update, context, kind='expense')
        if cmd == 'view':
            return await view_records(update, context)
        if cmd == 'back':
            return await menu(update, context)
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text("Unknown finance action. Try menu.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data='money:back')]]))
    except Exception:
        logger.exception("money.router error")
        if update.callback_query:
            try:
                await update.callback_query.edit_message_text("Error in finance action.")
            except Exception:
                pass

money_handlers['router'] = router

# -------------------------
# Add flow (3 steps)
# -------------------------
async def start_add(update: Update, context, kind: str = 'income'):
    context.user_data['flow'] = 'money_add'
    context.user_data['money_flow'] = {'kind': kind, 'step': 'category'}
    label = "income" if kind == 'income' else 'expense'
    await _reply_or_edit(update, f"Add {label} — Step 1/3\nEnter category (e.g., milk_sales) or /skip to use 'general'.", reply_markup=_mk_skip())

money_handlers['add_income'] = lambda u, c: start_add(u, c, kind='income')
money_handlers['add_expense'] = lambda u, c: start_add(u, c, kind='expense')

async def handle_text(update: Update, context):
    flow = context.user_data.get('flow')
    msg = update.message
    if not msg:
        return
    text = (msg.text or "").strip()
    # universal skip handling (when user sends /skip as text)
    if text.lower() == '/skip':
        mf = context.user_data.get('money_flow')
        if not mf:
            context.user_data.pop('flow', None)
            await update.message.reply_text("Skipped.")
            return
        step = mf.get('step')
        if step == 'category':
            mf['category'] = 'general'
            mf['step'] = 'amount'
            context.user_data['money_flow'] = mf
            await update.message.reply_text("Enter amount (e.g., 120.50) or /skip to cancel", reply_markup=_mk_skip())
            return
        if step == 'amount':
            context.user_data.pop('flow', None)
            context.user_data.pop('money_flow', None)
            await update.message.reply_text("Add cancelled.")
            return
        if step == 'date':
            mf['date'] = datetime.date.today().isoformat()

    if flow == 'money_add':
        mf = context.user_data.get('money_flow', {})
        step = mf.get('step', 'category')
        if step == 'category':
            mf['category'] = 'general' if text.lower() == '/skip' or text == '' else text
            mf['step'] = 'amount'
            context.user_data['money_flow'] = mf
            await update.message.reply_text("Step 2/3 — Enter amount (e.g., 120.50) or /skip to cancel", reply_markup=_mk_skip())
            return

        if step == 'amount':
            # validate amount
            try:
                amount = float(text)
            except Exception:
                await update.message.reply_text("Invalid amount. Use e.g. 120.50 or /skip", reply_markup=_mk_skip())
                return
            if amount <= 0:
                await update.message.reply_text("Amount must be positive.", reply_markup=_mk_skip())
                return
            mf['amount'] = round(amount, 2)
            mf['step'] = 'date'
            context.user_data['money_flow'] = mf
            await update.message.reply_text("Step 3/3 — Enter date YYYY-MM-DD or /skip to use today", reply_markup=_mk_skip())
            return

        if step == 'date':
            if text.lower() == '/skip' or text == '':
                date_str = datetime.date.today().isoformat()
            else:
                try:
                    datetime.datetime.strptime(text, "%Y-%m-%d")
                    date_str = text
                except Exception:
                    await update.message.reply_text("Invalid date. Use YYYY-MM-DD or /skip", reply_markup=_mk_skip())
                    return
            mf['date'] = date_str

            # Build record and insert
            kind = mf.get('kind', 'income')
            creator_uuid = ensure_app_user_uuid(update.message.from_user.id, update.message.from_user.full_name)
            farm_id = get_user_farm_id(update.message.from_user.id)
            if not farm_id:
                await update.message.reply_text("⚠️ You don't have a farm yet. Please register with /start and create a farm first.")
                # cleanup flow to avoid confusion
                context.user_data.pop('flow', None)
                context.user_data.pop('money_flow', None)
                return

            record = {
                'farm_id': farm_id,
                'type': 'income' if kind == 'income' else 'expense',
                'category': mf.get('category'),
                'amount': mf.get('amount'),
                'date': mf.get('date'),
                'created_by': creator_uuid,
                'currency': 'USD'
            }
            try:
                db_out = await _db_insert('financial_records', record)
                if db_out.get("error"):
                    logger.error("Failed to save financial record: %s", db_out.get("error"))
                    await update.message.reply_text("Failed to save record. Try again later.")
                else:
                    await update.message.reply_text(f"Recorded {record['type']} {record['category']} ${record['amount']:.2f} on {record['date']}.")
            except Exception:
                logger.exception("Failed to save financial record")
                await update.message.reply_text("Failed to save record.")
            finally:
                context.user_data.pop('flow', None)
                context.user_data.pop('money_flow', None)
            return

money_handlers['handle_text'] = handle_text

# -------------------------
# View records
# -------------------------
async def view_records(update: Update, context, limit: int = 30):
    is_cb = bool(update.callback_query)
    user_id = update.callback_query.from_user.id if is_cb else update.message.from_user.id
    farm_id = get_user_farm_id(user_id)
    if not farm_id:
        if is_cb:
            await update.callback_query.edit_message_text("You don't have a farm yet. Register with /start first.")
        else:
            await update.message.reply_text("You don't have a farm yet. Register with /start first.")
        return

    try:
        db_out = await _db_select('financial_records', eq_filter=('farm_id', farm_id), order_by=('date', {'ascending': False}), limit=limit)
        if db_out.get('error'):
            logger.error("Error fetching financial records: %s", db_out.get('error'))
            rows = []
        else:
            rows = db_out.get('data') or []
    except Exception:
        logger.exception("Error fetching financial records")
        rows = []

    if not rows:
        if is_cb:
            await update.callback_query.edit_message_text("No financial records found.")
        else:
            await update.message.reply_text("No financial records found.")
        return

    lines = []
    total_income = 0.0
    total_expense = 0.0
    for r in rows:
        d = r.get('date', 'N/A')
        typ = r.get('type', 'N/A')
        cat = r.get('category', 'N/A')
        try:
            amt = float(r.get('amount') or 0)
        except Exception:
            amt = 0.0
        lines.append(f"{d}: {typ} {cat} ${amt:.2f}")
        if typ == 'income':
            total_income += amt
        elif typ == 'expense':
            total_expense += amt

    summary = f"Total income: ${total_income:.2f}\nTotal expense: ${total_expense:.2f}\nNet: ${total_income - total_expense:.2f}\n\n"
    text = "Recent Financial Records:\n" + summary + "\n".join(lines)
    if is_cb:
        await update.callback_query.edit_message_text(text)
    else:
        await update.message.reply_text(text)

money_handlers['view'] = view_records
'''