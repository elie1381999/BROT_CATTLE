# animals_handlers.py
import asyncio
import logging
import datetime
from typing import Optional, List, Dict, Any
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from farmcore import (
    async_list_animals,
    async_create_animal,
    async_get_animal,
    async_update_animal,
    async_delete_animal,
    async_get_user_by_telegram,
    supabase,
)
from farmcore_role import async_user_has_permission, async_get_user_role_in_farm

logger = logging.getLogger(__name__)
animal_handlers: Dict[str, Any] = {}

_PAGE_SIZE = 8
_ADD_PREFIX = "animal_add_"  # prefix used in context.user_data for add-flow keys

# -----------------------
# Utilities
# -----------------------
def _format_animal_full(a: dict) -> str:
    """Return a multiline detailed string for a single animal row (plain text, safe)."""
    name = a.get("name") or "—"
    tag = a.get("tag") or "—"
    breed = a.get("breed") or "—"
    sex = a.get("sex") or "—"
    stage = a.get("stage") or ((a.get("meta") or {}).get("stage") if isinstance(a.get("meta"), dict) else "—")
    lact = a.get("lactation_stage") or ((a.get("meta") or {}).get("lactation_stage") if isinstance(a.get("meta"), dict) else "—")
    repro = a.get("repro_phase") or "—"
    birth = a.get("birth_date") or "—"
    weight = a.get("weight") or "—"
    weight_unit = a.get("weight_unit") or ""
    status = a.get("status") or "—"
    created = a.get("created_at") or "—"
    updated = a.get("updated_at") or "—"
    notes = (a.get("meta") or {}).get("notes") if isinstance(a.get("meta"), dict) else None
    sire = a.get("sire_id") or ((a.get("meta") or {}).get("sire_tag") if isinstance(a.get("meta"), dict) else None)

    lines = [
        f"Name: {name}  — Tag: {tag}",
        f"Breed: {breed}   • Sex: {sex}   • Stage: {stage}   • Lactation: {lact}",
        f"Repro phase: {repro}",
        f"Birth date: {birth}   • Status: {status}",
        f"Weight: {weight} {weight_unit}",
        f"Sire/father: {sire or '—'}",
        f"Created: {created}   • Updated: {updated}",
        f"Notes: {notes or '—'}",
    ]
    return "\n".join(lines)


def _clear_add_flow(user_data: dict):
    for k in list(user_data.keys()):
        if k.startswith(_ADD_PREFIX) or k in ("flow",):
            user_data.pop(k, None)


def _footer_kb(cancel_label: str = "Cancel"):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(cancel_label, callback_data="animal:add_cancel")]
    ])


def _make_yesno_row(values: List[str], cb_prefix: str):
    rows = []
    for i in range(0, len(values), 2):
        pair = values[i:i+2]
        row = [InlineKeyboardButton(v.capitalize(), callback_data=f"{cb_prefix}:{v}") for v in pair]
        rows.append(row)
    return rows

# -----------------------
# Optional fields menu builder
# -----------------------
_OPTION_LABELS = {
    "birth_date": "📅 Birth Date",
    "breed": "🐮 Breed",
    "lactation": "🍼 Milk / Lactation",
    "reproduction": "👶 Reproduction",
    "sire": "👨‍👧 Sire / Father",
    "notes": "📝 Notes",
}


def _build_optional_menu(available: List[str]):
    """Return InlineKeyboardMarkup for the list of available optional fields plus Finish."""
    rows = []
    temp = []
    for key in available:
        label = _OPTION_LABELS.get(key, key)
        temp.append(InlineKeyboardButton(label, callback_data=f"animal:add_field:{key}"))
    for i in range(0, len(temp), 2):
        rows.append(temp[i:i+2])
    rows.append([InlineKeyboardButton("✅ Finish", callback_data="animal:add_field:finish")])
    return InlineKeyboardMarkup(rows)


# -----------------------
# Membership-first resolver
# -----------------------
async def async_resolve_user_and_farm_preferring_membership(telegram_id: int) -> Optional[Dict[str, Any]]:
    """
    Return {'user': user_row, 'farm': farm_row, 'member': farm_member_row_or_None}
    Preference:
      1) latest farm_members row for this user
      2) app_users.current_farm_id (if set)
      3) farms owned by user (latest)
    """
    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception:
        user_row = None
    if not user_row:
        return None

    user_id = user_row.get("id")

    # 1) membership (most recent membership)
    try:
        def _q_member():
            return supabase.table("farm_members").select("*").eq("user_id", user_id).order("created_at", desc=True).limit(1).execute()
        out = await asyncio.to_thread(_q_member)
        mem_data = getattr(out, "data", None) or (out.get("data") if isinstance(out, dict) else None)
        if mem_data and isinstance(mem_data, list) and len(mem_data) > 0:
            member = mem_data[0]
            farm_id = member.get("farm_id")
            def _q_farm():
                return supabase.table("farms").select("*").eq("id", farm_id).limit(1).execute()
            fo = await asyncio.to_thread(_q_farm)
            farms = getattr(fo, "data", None) or (fo.get("data") if isinstance(fo, dict) else None)
            if farms and len(farms) > 0:
                logger.info("resolver: membership -> farm_id=%s (member_id=%s)", farm_id, member.get("id"))
                return {"user": user_row, "farm": farms[0], "member": member}
    except Exception:
        logger.exception("Error checking membership for user_id=%s", user_id)

    # 2) current_farm_id on user
    try:
        current_farm_id = user_row.get("current_farm_id")
        if current_farm_id:
            def _q_cf():
                return supabase.table("farms").select("*").eq("id", current_farm_id).limit(1).execute()
            cf = await asyncio.to_thread(_q_cf)
            rows = getattr(cf, "data", None) or (cf.get("data") if isinstance(cf, dict) else None)
            if rows and len(rows) > 0:
                logger.info("resolver: current_farm_id -> farm_id=%s", current_farm_id)
                return {"user": user_row, "farm": rows[0], "member": None}
    except Exception:
        logger.exception("Error checking current_farm_id for user_id=%s", user_id)

    # 3) farms owned by user
    try:
        def _q_owner():
            return supabase.table("farms").select("*").eq("owner_id", user_id).order("created_at", desc=True).limit(1).execute()
        owner_out = await asyncio.to_thread(_q_owner)
        owner_rows = getattr(owner_out, "data", None) or (owner_out.get("data") if isinstance(owner_out, dict) else None)
        if owner_rows and len(owner_rows) > 0:
            logger.info("resolver: owner -> farm_id=%s", owner_rows[0].get("id"))
            return {"user": user_row, "farm": owner_rows[0], "member": None}
    except Exception:
        logger.exception("Error checking owned farms for user_id=%s", user_id)

    logger.info("resolver: no farm found for user_id=%s", user_id)
    return {"user": user_row, "farm": None, "member": None}


# --------------------
# Permission helper
# --------------------
async def _ensure_animal_permission(update: Update, context: ContextTypes.DEFAULT_TYPE, *, must_have_edit: bool = False) -> Optional[Dict[str, Any]]:
    """
    Resolve user+farm (membership-first) and ensure the user has permission for the 'animals' module on that farm.
    If must_have_edit=True, also require edit-level role.
    Returns {'user':..., 'farm':..., 'user_id':..., 'farm_id':..., 'role': ...} on success, or None on failure.
    """
    try:
        combined = await async_resolve_user_and_farm_preferring_membership(update.effective_user.id)
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

    user_row = combined["user"]
    farm_row = combined["farm"]
    user_id = user_row.get("id")
    farm_id = farm_row.get("id")

    try:
        allowed = await async_user_has_permission(user_id, farm_id, "animals")
    except Exception:
        allowed = False

    if not allowed:
        try:
            role = await async_get_user_role_in_farm(user_id, farm_id)
        except Exception:
            role = None
        txt = f"⚠️ Your role '{role or 'unknown'}' does not have permission to use the Animals module."
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
        return {"user": user_row, "farm": farm_row, "user_id": user_id, "farm_id": farm_id, "role": role}

    try:
        role = await async_get_user_role_in_farm(user_id, farm_id)
    except Exception:
        role = None
    return {"user": user_row, "farm": farm_row, "user_id": user_id, "farm_id": farm_id, "role": role}


# -----------------------
# Menu
# -----------------------
async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # show menu only if user has at least view permission
    perm = await _ensure_animal_permission(update, context, must_have_edit=False)
    if not perm:
        return

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Animal", callback_data="animal:add")],
        [InlineKeyboardButton("📋 List Animals", callback_data="animal:list:0")],
        [InlineKeyboardButton("🔙 Back", callback_data="skip")]
    ])
    try:
        if update.callback_query:
            await update.callback_query.edit_message_text("🐮 Animals — choose an action:", reply_markup=kb)
        else:
            await update.message.reply_text("🐮 Animals — choose an action:", reply_markup=kb)
    except Exception:
        logger.exception("Failed to show animals menu")

animal_handlers["menu"] = menu

# -----------------------
# Router
# -----------------------
async def router(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str):
    """
    Actions supported (focused on new add flow additions):
      - add
      - list:<page>
      - view:<animal_id>:<page>
      - edit:<animal_id>:<page>
      - confirm_delete:<animal_id>:<page>
      - delete:<animal_id>:<page>
      - add_field:<field>   (for optional fields after basic save)
      - add_cancel
      - add_start
      - add_sex:<val>       (callback)
      - add_lact_opt:<val>
      - add_repro_opt:<val>
    """
    try:
        parts = action.split(":")
        cmd = parts[0] if parts else ""
        arg = parts[1] if len(parts) > 1 else ""
        arg2 = parts[2] if len(parts) > 2 else None

        # ---------- Start add flow ----------
        if cmd == "add":
            # require edit permission to create animals
            perm = await _ensure_animal_permission(update, context, must_have_edit=True)
            if not perm:
                return

            context.user_data["flow"] = "animal_add"
            context.user_data[_ADD_PREFIX + "data"] = {}
            context.user_data[_ADD_PREFIX + "step"] = "tag"
            context.user_data[_ADD_PREFIX + "available"] = [
                "birth_date",
                "breed",
                "lactation",
                "reproduction",
                "sire",
                "notes",
            ]
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Start", callback_data="animal:add_start")],
                [InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")]
            ])
            msg = (
                "➕ Adding a new animal — we will save basic info first (tag, name, sex, weight).\n\n"
                "You can add other details after the first save."
            )
            if update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text(msg, reply_markup=kb)
            else:
                await update.message.reply_text(msg, reply_markup=kb)
            return

        if cmd == "add_start":
            perm = await _ensure_animal_permission(update, context, must_have_edit=True)
            if not perm:
                return
            context.user_data[_ADD_PREFIX + "step"] = "tag"
            await _prompt_for_step(update, context, "tag")
            return

        # ---------- Sex selection (callback) ----------
        if cmd == "add_sex" and arg:
            perm = await _ensure_animal_permission(update, context, must_have_edit=True)
            if not perm:
                return

            sex_val = arg.lower()
            data = context.user_data.get(_ADD_PREFIX + "data", {})
            sex = "female" if sex_val in ("female", "f") else ("male" if sex_val in ("male", "m") else "unknown")

            if data.get("sex") == sex:
                if update.callback_query:
                    try:
                        await update.callback_query.answer("Already selected.")
                    except Exception:
                        pass
                    await _prompt_for_step(update, context, "weight")
                else:
                    await update.message.reply_text("Already selected. What’s the weight in kg?")
                return

            data["sex"] = sex
            context.user_data[_ADD_PREFIX + "data"] = data
            context.user_data[_ADD_PREFIX + "step"] = "weight"

            if update.callback_query:
                try:
                    await update.callback_query.answer()
                except Exception:
                    pass
                await update.callback_query.edit_message_text(f"Selected sex: {sex.capitalize()}.\n\nWhat’s the weight in kg? (number)", reply_markup=_footer_kb())
            else:
                await update.message.reply_text(f"Selected sex: {sex.capitalize()}.\n\nWhat’s the weight in kg? (number)", reply_markup=_footer_kb())
            return

        # ---------- Handler for optional field buttons ----------
        if cmd == "add_field" and arg:
            perm = await _ensure_animal_permission(update, context, must_have_edit=True)
            if not perm:
                return

            field = arg
            if field == "finish":
                created_id = context.user_data.get(_ADD_PREFIX + "created_id")
                if not created_id:
                    _clear_add_flow(context.user_data)
                    if update.callback_query:
                        await update.callback_query.answer()
                        await update.callback_query.edit_message_text("❌ No animal in progress. Start again with Add Animal.")
                    else:
                        await update.message.reply_text("❌ No animal in progress. Start again with Add Animal.")
                    return
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Add Another", callback_data="animal:add"), InlineKeyboardButton("📋 Back to list", callback_data="animal:list:0")]
                ])
                if update.callback_query:
                    await update.callback_query.answer()
                    await update.callback_query.edit_message_text("✅ Done. What would you like to do next?", reply_markup=kb)
                else:
                    await update.message.reply_text("✅ Done. What would you like to do next?", reply_markup=kb)
                for k in list(context.user_data.keys()):
                    if k.startswith(_ADD_PREFIX) or k == "flow":
                        context.user_data.pop(k, None)
                return

            available = context.user_data.get(_ADD_PREFIX + "available", [])
            if field not in available:
                if update.callback_query:
                    try:
                        await update.callback_query.answer("This field is already filled or not available.")
                    except Exception:
                        pass
                else:
                    await update.message.reply_text("This field is already filled or not available.")
                return

            if field in ("birth_date", "breed", "sire", "notes"):
                context.user_data[_ADD_PREFIX + "step"] = f"opt_{field}"
                context.user_data[_ADD_PREFIX + "editing_field"] = field
                if field == "birth_date":
                    msg = "Send birth date (YYYY or YYYY-MM or YYYY-MM-DD) or send - to cancel."
                elif field == "breed":
                    msg = "Send the breed (e.g. Jersey) or send - to cancel."
                elif field == "sire":
                    msg = "Send sire (father) tag to link this animal or send - to cancel."
                else:
                    msg = "Send notes (or - to cancel)."
                if update.callback_query:
                    await update.callback_query.answer()
                    await update.callback_query.edit_message_text(msg)
                else:
                    await update.message.reply_text(msg)
                return

            if field == "lactation":
                choices = ["1", "2", "3", "dry", "unknown"]
                kb_rows = _make_yesno_row(choices, "animal:add_lact_opt")
                kb_rows.append([InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")])
                kb = InlineKeyboardMarkup(kb_rows)
                txt = "Choose lactation phase (1 / 2 / 3 / dry) or cancel:"
                if update.callback_query:
                    await update.callback_query.answer()
                    await update.callback_query.edit_message_text(txt, reply_markup=kb)
                else:
                    await update.message.reply_text(txt, reply_markup=kb)
                context.user_data[_ADD_PREFIX + "editing_field"] = "lactation"
                context.user_data[_ADD_PREFIX + "step"] = "opt_lactation"
                return

            if field == "reproduction":
                repro_choices = [
                    ("pregnant", "🤰 Pregnant"), ("dry_off", "🛌 Dry Off"), ("lactating", "🍼 Lactating"), ("estrus", "🔄 Estrus"),
                    ("immature", "🚫 Immature"), ("inseminated", "📅 Inseminated"), ("postpartum", "🩹 Postpartum"), ("aborted", "❌ Aborted"), ("unknown", "❓ Unknown"),
                ]
                kb_rows = []
                for i in range(0, len(repro_choices), 2):
                    row = []
                    for j in range(2):
                        if i+j < len(repro_choices):
                            val, label = repro_choices[i+j]
                            row.append(InlineKeyboardButton(label, callback_data=f"animal:add_repro_opt:{val}"))
                    kb_rows.append(row)
                kb_rows.append([InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")])
                kb = InlineKeyboardMarkup(kb_rows)
                txt = "Choose reproduction stage or cancel:"
                if update.callback_query:
                    await update.callback_query.answer()
                    await update.callback_query.edit_message_text(txt, reply_markup=kb)
                else:
                    await update.message.reply_text(txt, reply_markup=kb)
                context.user_data[_ADD_PREFIX + "editing_field"] = "reproduction"
                context.user_data[_ADD_PREFIX + "step"] = "opt_reproduction"
                return

        # ---------- Cancel during add ----------
        if cmd == "add_cancel":
            _clear_add_flow(context.user_data)
            if update.callback_query:
                try:
                    await update.callback_query.answer()
                except Exception:
                    pass
                await update.callback_query.edit_message_text("❌ Animal registration cancelled.")
            else:
                await update.message.reply_text("❌ Animal registration cancelled.")
            return

        # ---------- List animals (detailed) ----------
        if cmd == "list":
            perm = await _ensure_animal_permission(update, context, must_have_edit=False)
            if not perm:
                return

            page = int(arg) if arg and arg.isdigit() else 0
            farm_id = perm["farm_id"]
            animals = await async_list_animals(farm_id=farm_id, limit=1000)
            total = len(animals)
            start = page * _PAGE_SIZE
            end = start + _PAGE_SIZE
            page_animals = animals[start:end]

            header = f"Animals on your farm — page {page+1} / {max(1, (total + _PAGE_SIZE -1)//_PAGE_SIZE)}\n\n"
            if page_animals:
                parts = []
                for idx, a in enumerate(page_animals, start=1):
                    parts.append(f"{idx + start}. " + _format_animal_full(a))
                    parts.append("")
                body = "\n".join(parts).rstrip()
            else:
                body = "No animals on this page."

            text = header + body

            kb_rows = []
            for a in page_animals:
                display = f"{a.get('name') or a.get('tag') or 'Unnamed'}"
                kb_rows.append([
                    InlineKeyboardButton(f"View {display}", callback_data=f"animal:view:{a.get('id')}:{page}"),
                    InlineKeyboardButton("Edit", callback_data=f"animal:edit:{a.get('id')}:{page}"),
                    InlineKeyboardButton("Delete", callback_data=f"animal:confirm_delete:{a.get('id')}:{page}")
                ])

            nav_row = []
            if start > 0:
                nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"animal:list:{page-1}"))
            if end < total:
                nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"animal:list:{page+1}"))
            if nav_row:
                kb_rows.append(nav_row)

            kb_rows.append([InlineKeyboardButton("➕ Add new", callback_data="animal:add"), InlineKeyboardButton("🔙 Back", callback_data="skip")])
            kb = InlineKeyboardMarkup(kb_rows)

            if update.callback_query:
                try:
                    try:
                        await update.callback_query.answer()
                    except Exception:
                        pass
                    await update.callback_query.edit_message_text(text, reply_markup=kb)
                except Exception:
                    logger.warning("edit_message_text failed for long animal list; sending new message instead")
                    try:
                        await update.callback_query.message.reply_text(text, reply_markup=kb)
                    except Exception:
                        logger.exception("Failed to send fallback message for animal list")
            else:
                await update.message.reply_text(text, reply_markup=kb)
            return

        # ---------- View single animal ----------
        if cmd == "view" and arg:
            perm = await _ensure_animal_permission(update, context, must_have_edit=False)
            if not perm:
                return

            animal_id = arg
            page = int(arg2) if arg2 and arg2.isdigit() else 0
            a = await async_get_animal(animal_id)
            if not a:
                txt = "⚠️ Animal not found."
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt)
                else:
                    await update.message.reply_text(txt)
                return

            if a.get("farm_id") != perm["farm_id"]:
                txt = "⚠️ Animal not found on your farm."
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt)
                else:
                    await update.message.reply_text(txt)
                return

            text = (
                f"Name: {a.get('name') or 'Unnamed'}\n\n"
                f"Tag: {a.get('tag')}\n"
                f"Breed: {a.get('breed') or '—'}\n"
                f"Sex: {a.get('sex') or '—'}\n"
                f"Stage: {a.get('stage') or ((a.get('meta') or {}).get('stage') if isinstance(a.get('meta'), dict) else '—')}\n"
                f"Lactation: {a.get('lactation_stage') or ((a.get('meta') or {}).get('lactation_stage') if isinstance(a.get('meta'), dict) else '—')}\n"
                f"Repro phase: {a.get('repro_phase') or '—'}\n"
                f"Weight: {a.get('weight') or '—'} {a.get('weight_unit') or ''}\n"
                f"Status: {a.get('status') or '—'}\n"
                f"Birth date: {a.get('birth_date') or '—'}\n"
                f"Created: {a.get('created_at') or '—'}\n"
                f"Updated: {a.get('updated_at') or '—'}\n"
                f"Notes: {(a.get('meta') or {}).get('notes') if isinstance(a.get('meta'), dict) else '—'}\n"
            )
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Edit", callback_data=f"animal:edit:{animal_id}:{page}"),
                 InlineKeyboardButton("Delete", callback_data=f"animal:confirm_delete:{animal_id}:{page}")],
                [InlineKeyboardButton("Back to list", callback_data=f"animal:list:{page}")]
            ])
            if update.callback_query:
                try:
                    await update.callback_query.answer()
                except Exception:
                    pass
                await update.callback_query.edit_message_text(text, reply_markup=kb)
            else:
                await update.message.reply_text(text, reply_markup=kb)
            return

        # ---------- Confirm & Delete ----------
        if cmd == "confirm_delete" and arg:
            perm = await _ensure_animal_permission(update, context, must_have_edit=True)
            if not perm:
                return

            animal_id = arg
            page = int(arg2) if arg2 and arg2.isdigit() else 0
            a = await async_get_animal(animal_id)
            if not a or a.get("farm_id") != perm["farm_id"]:
                txt = "⚠️ Animal not found on your farm."
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt)
                else:
                    await update.message.reply_text(txt)
                return

            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Yes, delete", callback_data=f"animal:delete:{animal_id}:{page}"),
                 InlineKeyboardButton("No, cancel", callback_data=f"animal:view:{animal_id}:{page}")]
            ])
            if update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text("⚠️ Are you sure you want to permanently delete this animal?", reply_markup=kb)
            else:
                await update.message.reply_text("⚠️ Are you sure you want to permanently delete this animal?", reply_markup=kb)
            return

        if cmd == "delete" and arg:
            perm = await _ensure_animal_permission(update, context, must_have_edit=True)
            if not perm:
                return

            animal_id = arg
            page = int(arg2) if arg2 and arg2.isdigit() else 0
            a = await async_get_animal(animal_id)
            if not a or a.get("farm_id") != perm["farm_id"]:
                txt = "⚠️ Animal not found on your farm."
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt)
                else:
                    await update.message.reply_text(txt)
                return

            ok = await async_delete_animal(animal_id)
            if ok:
                txt = "✅ Animal deleted."
            else:
                txt = "❌ Failed to delete animal."
            if update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text(txt)
            else:
                await update.message.reply_text(txt)
            await router(update, context, f"animal:list:{page}")
            return

        # ---------- Edit flow (simple) ----------
        if cmd == "edit" and arg:
            perm = await _ensure_animal_permission(update, context, must_have_edit=True)
            if not perm:
                return

            animal_id = arg
            page = int(arg2) if arg2 and arg2.isdigit() else 0
            a = await async_get_animal(animal_id)
            if not a or a.get("farm_id") != perm["farm_id"]:
                txt = "⚠️ Animal not found on your farm."
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt)
                else:
                    await update.message.reply_text(txt)
                return
            context.user_data["flow"] = "animal_edit"
            context.user_data["animal_edit_step"] = "name"
            context.user_data["animal_edit_id"] = animal_id
            context.user_data["animal_edit_return_page"] = page
            if update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text(f"Editing {a.get('name') or a.get('tag')} — send new name or - to keep:")
            else:
                await update.message.reply_text(f"Editing {a.get('name') or a.get('tag')} — send new name or - to keep:")
            return

        # ---------- Add lactation option (from optional menu) ----------
        if cmd == "add_lact_opt" and arg:
            perm = await _ensure_animal_permission(update, context, must_have_edit=True)
            if not perm:
                return

            val = arg.lower()
            animal_id = context.user_data.get(_ADD_PREFIX + "created_id")
            if not animal_id:
                if update.callback_query:
                    await update.callback_query.answer()
                    await update.callback_query.edit_message_text("❌ No animal in progress. Start again.")
                else:
                    await update.message.reply_text("❌ No animal in progress. Start again.")
                return
            upd = {"lactation_stage": val}
            await async_update_animal(animal_id, upd)
            available = context.user_data.get(_ADD_PREFIX + "available", [])
            if "lactation" in available:
                available.remove("lactation")
            context.user_data[_ADD_PREFIX + "available"] = available
            if update.callback_query:
                try:
                    await update.callback_query.answer()
                except Exception:
                    pass
                await update.callback_query.edit_message_text(f"✅ Lactation saved: {val}", reply_markup=_build_optional_menu(available))
            else:
                await update.message.reply_text(f"✅ Lactation saved: {val}")
            return

        # ---------- Add reproduction option ----------
        if cmd == "add_repro_opt" and arg:
            perm = await _ensure_animal_permission(update, context, must_have_edit=True)
            if not perm:
                return

            val = arg.lower()
            animal_id = context.user_data.get(_ADD_PREFIX + "created_id")
            if not animal_id:
                if update.callback_query:
                    await update.callback_query.answer()
                    await update.callback_query.edit_message_text("❌ No animal in progress. Start again.")
                else:
                    await update.message.reply_text("❌ No animal in progress. Start again.")
                return
            upd = {"initial_phase": val}
            await async_update_animal(animal_id, upd)
            available = context.user_data.get(_ADD_PREFIX + "available", [])
            if "reproduction" in available:
                available.remove("reproduction")
            context.user_data[_ADD_PREFIX + "available"] = available
            if update.callback_query:
                try:
                    await update.callback_query.answer()
                except Exception:
                    pass
                await update.callback_query.edit_message_text(f"✅ Reproduction saved: {val}", reply_markup=_build_optional_menu(available))
            else:
                await update.message.reply_text(f"✅ Reproduction saved: {val}")
            return

        # ---------- Unknown action ----------
        if update.callback_query:
            try:
                await update.callback_query.answer("Action not recognized.")
            except Exception:
                pass
        else:
            await update.message.reply_text("Action not recognized.")

    except Exception:
        logger.exception("Error in animal router")
        try:
            if update.callback_query:
                await update.callback_query.edit_message_text("❌ Error handling animal action.")
            else:
                await update.message.reply_text("❌ Error handling animal action.")
        except Exception:
            pass

animal_handlers["router"] = router

# -----------------------
# Helper: prompt text for a given step (new simplified steps)
# -----------------------
async def _prompt_for_step(update: Update, context: ContextTypes.DEFAULT_TYPE, step: str):
    data = context.user_data.get(_ADD_PREFIX + "data", {})
    if step == "tag":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")]
        ])
        msg = "What’s the ear tag / ID? (or send - to skip)"
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return
    if step == "name":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Skip", callback_data="animal:add_field:breed"), InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")]
        ])
        msg = "What’s the name? (or send - to skip)"
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return
    if step == "sex":
        kb_rows = [
            [InlineKeyboardButton("Female 🐄", callback_data="animal:add_sex:female"),
             InlineKeyboardButton("Male 🐂", callback_data="animal:add_sex:male")],
            [InlineKeyboardButton("Unknown ❓", callback_data="animal:add_sex:unknown")],
            [InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")]
        ]
        kb = InlineKeyboardMarkup(kb_rows)
        msg = "Is it Female or Male?"
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return
    if step == "weight":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")]
        ])
        msg = "What’s the weight in kg? (number) — this is important."
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return

# -----------------------
# Save helper: create initial animal after basics (tag/name/sex/weight)
# -----------------------
async def _create_initial_animal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get(_ADD_PREFIX + "data", {})
    perm = await _ensure_animal_permission(update, context, must_have_edit=True)
    if not perm:
        _clear_add_flow(context.user_data)
        return
    farm_id = perm["farm_id"]

    tag = data.get("tag") or ""
    name = data.get("name")
    sex = data.get("sex", "female")
    weight = data.get("weight")

    created = await async_create_animal(farm_id=farm_id, tag=tag, name=name, sex=sex, weight=weight)
    if not created:
        txt = "❌ Failed to create animal (maybe tag duplicate)."
        _clear_add_flow(context.user_data)
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(txt)
        else:
            await update.message.reply_text(txt)
        return

    created_id = created.get("id")
    context.user_data[_ADD_PREFIX + "created_id"] = created_id

    display = name or tag or created_id
    txt = f"✅ Saved {display} (tag {tag or '—'}, {sex.capitalize()}, {weight or '—'} kg)\n\nWhat else do you want to add?"
    available = context.user_data.get(_ADD_PREFIX + "available", [])
    kb = _build_optional_menu(available)
    if update.callback_query:
        try:
            await update.callback_query.answer()
        except Exception:
            pass
        await update.callback_query.edit_message_text(txt, reply_markup=kb)
    else:
        await update.message.reply_text(txt, reply_markup=kb)

# -----------------------
# Text handler for typed steps
# -----------------------
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Add flow typed states
    if context.user_data.get("flow") == "animal_add":
        perm = await _ensure_animal_permission(update, context, must_have_edit=True)
        if not perm:
            _clear_add_flow(context.user_data)
            return

        step = context.user_data.get(_ADD_PREFIX + "step", "tag")
        text = (update.effective_message.text or "").strip()
        if text.lower() in ("/cancel", "cancel"):
            _clear_add_flow(context.user_data)
            await update.effective_message.reply_text("❌ Animal registration cancelled.")
            return
        if text == "-":
            if step == "tag":
                data = context.user_data.get(_ADD_PREFIX + "data", {})
                data["tag"] = None
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "name"
                await _prompt_for_step(update, context, "name")
                return
            if step == "name":
                data = context.user_data.get(_ADD_PREFIX + "data", {})
                data["name"] = None
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "sex"
                await _prompt_for_step(update, context, "sex")
                return
            if step and step.startswith("opt_"):
                context.user_data[_ADD_PREFIX + "step"] = None
                context.user_data.pop(_ADD_PREFIX + "editing_field", None)
                available = context.user_data.get(_ADD_PREFIX + "available", [])
                kb = _build_optional_menu(available)
                await update.effective_message.reply_text("Cancelled. Back to options:", reply_markup=kb)
                return

        try:
            data = context.user_data.get(_ADD_PREFIX + "data", {})
            if step == "tag":
                data["tag"] = text
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "name"
                await _prompt_for_step(update, context, "name")
                return
            if step == "name":
                data["name"] = text
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "sex"
                await _prompt_for_step(update, context, "sex")
                return
            if step == "sex":
                val = text.strip().lower()
                if val in ("female", "f", "male", "m", "unknown", "u"):
                    sex = "female" if val.startswith("f") else ("male" if val.startswith("m") else "unknown")
                    data["sex"] = sex
                    context.user_data[_ADD_PREFIX + "data"] = data
                    context.user_data[_ADD_PREFIX + "step"] = "weight"
                    await _prompt_for_step(update, context, "weight")
                    return
                else:
                    await update.effective_message.reply_text("Please reply with 'Female', 'Male' or 'Unknown' (or press the button).")
                    return

            if step == "opt_birth_date":
                val = text
                try:
                    if len(val) == 4:
                        int(val)
                    else:
                        datetime.datetime.strptime(val, "%Y-%m-%d")
                except Exception:
                    try:
                        datetime.datetime.strptime(val, "%Y-%m")
                    except Exception:
                        await update.effective_message.reply_text("Invalid date. Use YYYY or YYYY-MM or YYYY-MM-DD or send - to cancel.")
                        return
                animal_id = context.user_data.get(_ADD_PREFIX + "created_id")
                if not animal_id:
                    await update.effective_message.reply_text("No animal in progress. Start again.")
                    return
                upd = {"birth_date": val}
                await async_update_animal(animal_id, upd)
                available = context.user_data.get(_ADD_PREFIX + "available", [])
                if "birth_date" in available:
                    available.remove("birth_date")
                context.user_data[_ADD_PREFIX + "available"] = available
                context.user_data[_ADD_PREFIX + "step"] = None
                context.user_data.pop(_ADD_PREFIX + "editing_field", None)
                kb = _build_optional_menu(available)
                await update.effective_message.reply_text(f"✅ Birth date saved: {val}", reply_markup=kb)
                return
            if step == "opt_breed":
                val = text
                animal_id = context.user_data.get(_ADD_PREFIX + "created_id")
                if not animal_id:
                    await update.effective_message.reply_text("No animal in progress. Start again.")
                    return
                upd = {"breed": val}
                await async_update_animal(animal_id, upd)
                available = context.user_data.get(_ADD_PREFIX + "available", [])
                if "breed" in available:
                    available.remove("breed")
                context.user_data[_ADD_PREFIX + "available"] = available
                context.user_data[_ADD_PREFIX + "step"] = None
                context.user_data.pop(_ADD_PREFIX + "editing_field", None)
                kb = _build_optional_menu(available)
                await update.effective_message.reply_text(f"✅ Breed saved: {val}", reply_markup=kb)
                return
            if step == "opt_sire":
                val = text
                animal_id = context.user_data.get(_ADD_PREFIX + "created_id")
                if not animal_id:
                    await update.effective_message.reply_text("No animal in progress. Start again.")
                    return
                upd = {"meta": {"sire_tag": val}}
                await async_update_animal(animal_id, upd)
                available = context.user_data.get(_ADD_PREFIX + "available", [])
                if "sire" in available:
                    available.remove("sire")
                context.user_data[_ADD_PREFIX + "available"] = available
                context.user_data[_ADD_PREFIX + "step"] = None
                context.user_data.pop(_ADD_PREFIX + "editing_field", None)
                kb = _build_optional_menu(available)
                await update.effective_message.reply_text(f"✅ Sire saved: {val}", reply_markup=kb)
                return
            if step == "opt_notes":
                val = text
                animal_id = context.user_data.get(_ADD_PREFIX + "created_id")
                if not animal_id:
                    await update.effective_message.reply_text("No animal in progress. Start again.")
                    return
                upd = {"meta": {"notes": val}}
                await async_update_animal(animal_id, upd)
                available = context.user_data.get(_ADD_PREFIX + "available", [])
                if "notes" in available:
                    available.remove("notes")
                context.user_data[_ADD_PREFIX + "available"] = available
                context.user_data[_ADD_PREFIX + "step"] = None
                context.user_data.pop(_ADD_PREFIX + "editing_field", None)
                kb = _build_optional_menu(available)
                await update.effective_message.reply_text(f"✅ Notes saved.", reply_markup=kb)
                return

            if step == "weight":
                try:
                    val = float(text)
                    data["weight"] = val
                    context.user_data[_ADD_PREFIX + "data"] = data
                except ValueError:
                    await update.effective_message.reply_text("Invalid weight. Send a number (e.g. 420) or send - to cancel.")
                    return
                await _create_initial_animal(update, context)
                return

        except Exception:
            logger.exception("Error handling add flow text")
            await update.effective_message.reply_text("❌ Error processing input — registration cancelled.")
            _clear_add_flow(context.user_data)
            return

    # Edit flow
    if context.user_data.get("flow") == "animal_edit":
        perm = await _ensure_animal_permission(update, context, must_have_edit=True)
        if not perm:
            for k in list(context.user_data.keys()):
                if k.startswith("animal_edit"):
                    context.user_data.pop(k, None)
            context.user_data.pop("flow", None)
            return

        step = context.user_data.get("animal_edit_step")
        animal_id = context.user_data.get("animal_edit_id")
        if not animal_id:
            await update.effective_message.reply_text("⚠️ Edit flow lost the animal id. Cancelled.")
            context.user_data.pop("flow", None)
            return
        text = (update.effective_message.text or "").strip()
        try:
            if step == "name":
                new_name = None if text == "-" else text
                context.user_data["animal_edit_name"] = new_name
                context.user_data["animal_edit_step"] = "breed_sex"
                await update.effective_message.reply_text("Send new breed and sex separated by comma (e.g. Jersey, female) or - to keep:")
                return
            if step == "breed_sex":
                breed = None
                sex = None
                if text != "-":
                    parts = [p.strip() for p in text.split(",")]
                    if parts:
                        breed = parts[0] or None
                    if len(parts) > 1 and parts[1]:
                        s = parts[1].lower()
                        sex = s if s in ("female", "male", "unknown") else "unknown"
                context.user_data["animal_edit_breed"] = breed
                context.user_data["animal_edit_sex"] = sex
                context.user_data["animal_edit_step"] = "birth_date"
                await update.effective_message.reply_text("Send new birth date YYYY-MM-DD or - to keep:")
                return
            if step == "birth_date":
                birth_date = None
                if text != "-":
                    try:
                        datetime.datetime.strptime(text, "%Y-%m-%d")
                        birth_date = text
                    except Exception:
                        await update.effective_message.reply_text("Invalid date. Use YYYY-MM-DD or - to keep.")
                        return
                context.user_data["animal_edit_birth_date"] = birth_date
                context.user_data["animal_edit_step"] = "weight"
                await update.effective_message.reply_text("Send new weight (number in kg) or - to keep:")
                return
            if step == "weight":
                weight = None
                if text != "-":
                    try:
                        weight = float(text)
                    except ValueError:
                        await update.effective_message.reply_text("Invalid weight. Use number or - to keep.")
                        return
                context.user_data["animal_edit_weight"] = weight
                context.user_data["animal_edit_step"] = "notes"
                await update.effective_message.reply_text("Send new notes or - to keep:")
                return
            if step == "notes":
                notes = None if text == "-" else text
                upd: Dict[str, Any] = {}
                if "animal_edit_name" in context.user_data:
                    nm = context.user_data.get("animal_edit_name")
                    if nm is not None:
                        upd["name"] = nm
                if "animal_edit_breed" in context.user_data:
                    br = context.user_data.get("animal_edit_breed")
                    if br is not None:
                        upd["breed"] = br
                if "animal_edit_sex" in context.user_data:
                    sx = context.user_data.get("animal_edit_sex")
                    if sx is not None:
                        upd["sex"] = sx
                if "animal_edit_birth_date" in context.user_data:
                    bd = context.user_data.get("animal_edit_birth_date")
                    if bd is not None:
                        upd["birth_date"] = bd
                if "animal_edit_weight" in context.user_data:
                    wt = context.user_data.get("animal_edit_weight")
                    if wt is not None:
                        upd["weight"] = wt
                meta_update = {}
                if notes is not None:
                    meta_update["notes"] = notes
                if meta_update:
                    upd["meta"] = meta_update
                if not upd:
                    await update.effective_message.reply_text("No changes provided. Cancelled.")
                    for k in list(context.user_data.keys()):
                        if k.startswith("animal_edit"):
                            context.user_data.pop(k, None)
                    context.user_data.pop("flow", None)
                    return

                a = await async_get_animal(animal_id)
                if not a or a.get("farm_id") != perm["farm_id"]:
                    await update.effective_message.reply_text("⚠️ Animal not found on your farm. Cancelled.")
                    for k in list(context.user_data.keys()):
                        if k.startswith("animal_edit"):
                            context.user_data.pop(k, None)
                    context.user_data.pop("flow", None)
                    return

                updated = await async_update_animal(animal_id, upd)
                if not updated:
                    await update.effective_message.reply_text("❌ Failed to update animal.")
                else:
                    await update.effective_message.reply_text("✅ Animal updated.")
                page = context.user_data.get("animal_edit_return_page", 0)
                for k in list(context.user_data.keys()):
                    if k.startswith("animal_edit"):
                        context.user_data.pop(k, None)
                context.user_data.pop("flow", None)
                await router(update, context, f"animal:list:{page}")
                return
        except Exception:
            logger.exception("Error in edit flow")
            await update.effective_message.reply_text("❌ Error processing edit. Cancelled.")
            for k in list(context.user_data.keys()):
                if k.startswith("animal_edit"):
                    context.user_data.pop(k, None)
            context.user_data.pop("flow", None)
            return

# Expose handlers map
animal_handlers["router"] = router
animal_handlers["handle_text"] = handle_text
animal_handlers["menu"] = menu















'''
#without manager
import asyncio
import logging
import datetime
from typing import Optional, List, Dict, Any
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from farmcore import (
    async_get_user_with_farm_by_telegram,
    async_list_animals,
    async_create_animal,
    async_get_animal,
    async_update_animal,
    async_delete_animal,
)

logger = logging.getLogger(__name__)
animal_handlers: Dict[str, Any] = {}

_PAGE_SIZE = 8
_ADD_PREFIX = "animal_add_"  # prefix used in context.user_data for add-flow keys

# -----------------------
# Utilities
# -----------------------
def _format_animal_full(a: dict) -> str:
    """Return a multiline detailed string for a single animal row (plain text, safe)."""
    name = a.get("name") or "—"
    tag = a.get("tag") or "—"
    breed = a.get("breed") or "—"
    sex = a.get("sex") or "—"
    stage = a.get("stage") or ((a.get("meta") or {}).get("stage") if isinstance(a.get("meta"), dict) else "—")
    lact = a.get("lactation_stage") or ((a.get("meta") or {}).get("lactation_stage") if isinstance(a.get("meta"), dict) else "—")
    repro = a.get("repro_phase") or "—"
    birth = a.get("birth_date") or "—"
    weight = a.get("weight") or "—"
    weight_unit = a.get("weight_unit") or ""
    status = a.get("status") or "—"
    created = a.get("created_at") or "—"
    updated = a.get("updated_at") or "—"
    notes = (a.get("meta") or {}).get("notes") if isinstance(a.get("meta"), dict) else None
    sire = a.get("sire_id") or ((a.get("meta") or {}).get("sire_tag") if isinstance(a.get("meta"), dict) else None)

    lines = [
        f"Name: {name}  — Tag: {tag}",
        f"Breed: {breed}   • Sex: {sex}   • Stage: {stage}   • Lactation: {lact}",
        f"Repro phase: {repro}",
        f"Birth date: {birth}   • Status: {status}",
        f"Weight: {weight} {weight_unit}",
        f"Sire/father: {sire or '—'}",
        f"Created: {created}   • Updated: {updated}",
        f"Notes: {notes or '—'}",
    ]
    return "\n".join(lines)


def _clear_add_flow(user_data: dict):
    for k in list(user_data.keys()):
        if k.startswith(_ADD_PREFIX) or k in ("flow",):
            user_data.pop(k, None)


def _footer_kb(cancel_label: str = "Cancel"):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(cancel_label, callback_data="animal:add_cancel")]
    ])


def _make_yesno_row(values: List[str], cb_prefix: str):
    rows = []
    for i in range(0, len(values), 2):
        pair = values[i:i+2]
        row = [InlineKeyboardButton(v.capitalize(), callback_data=f"{cb_prefix}:{v}") for v in pair]
        rows.append(row)
    return rows

# -----------------------
# New: optional fields menu builder
# -----------------------
_OPTION_LABELS = {
    "birth_date": "📅 Birth Date",
    "breed": "🐮 Breed",
    "lactation": "🍼 Milk / Lactation",
    "reproduction": "👶 Reproduction",
    "sire": "👨‍👧 Sire / Father",
    "notes": "📝 Notes",
}


def _build_optional_menu(available: List[str]):
    """Return InlineKeyboardMarkup for the list of available optional fields plus Finish."""
    rows = []
    # show two per row for compactness
    temp = []
    for key in available:
        label = _OPTION_LABELS.get(key, key)
        temp.append(InlineKeyboardButton(label, callback_data=f"animal:add_field:{key}"))
    # chunk into rows of 2
    for i in range(0, len(temp), 2):
        rows.append(temp[i:i+2])
    # always add Finish row
    rows.append([InlineKeyboardButton("✅ Finish", callback_data="animal:add_field:finish")])
    return InlineKeyboardMarkup(rows)

# -----------------------
# Menu
# -----------------------
async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Animal", callback_data="animal:add")],
        [InlineKeyboardButton("📋 List Animals", callback_data="animal:list:0")],
        [InlineKeyboardButton("🔙 Back", callback_data="skip")]
    ])
    try:
        if update.callback_query:
            await update.callback_query.edit_message_text("🐮 Animals — choose an action:", reply_markup=kb)
        else:
            await update.message.reply_text("🐮 Animals — choose an action:", reply_markup=kb)
    except Exception:
        logger.exception("Failed to show animals menu")

animal_handlers["menu"] = menu

# -----------------------
# Router
# -----------------------
async def router(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str):
    """
    Actions supported (focused on new add flow additions):
      - add
      - list:<page>
      - view:<animal_id>:<page>
      - edit:<animal_id>:<page>
      - confirm_delete:<animal_id>:<page>
      - delete:<animal_id>:<page>
      - add_field:<field>   (for optional fields after basic save)
      - add_cancel
      - add_start
      - add_sex:<val>       (callback)
      - add_lact_opt:<val>
      - add_repro_opt:<val>
    """
    try:
        parts = action.split(":")
        cmd = parts[0] if parts else ""
        arg = parts[1] if len(parts) > 1 else ""
        arg2 = parts[2] if len(parts) > 2 else None

        # ---------- Start add flow ----------
        if cmd == "add":
            # New flow: collect minimal required basics first: tag, name (optional), sex, weight (important)
            context.user_data["flow"] = "animal_add"
            context.user_data[_ADD_PREFIX + "data"] = {}
            context.user_data[_ADD_PREFIX + "step"] = "tag"
            # optional fields available after create
            context.user_data[_ADD_PREFIX + "available"] = [
                "birth_date",
                "breed",
                "lactation",
                "reproduction",
                "sire",
                "notes",
            ]
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Start", callback_data="animal:add_start")],
                [InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")]
            ])
            msg = (
                "➕ Adding a new animal — we will save basic info first (tag, name, sex, weight).\n\n"
                "You can add other details after the first save."
            )
            if update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text(msg, reply_markup=kb)
            else:
                await update.message.reply_text(msg, reply_markup=kb)
            return

        if cmd == "add_start":
            # begin prompting for tag
            context.user_data[_ADD_PREFIX + "step"] = "tag"
            await _prompt_for_step(update, context, "tag")
            return

        # ---------- Sex selection (callback) ----------
        if cmd == "add_sex" and arg:
            sex_val = arg.lower()
            data = context.user_data.get(_ADD_PREFIX + "data", {})
            sex = "female" if sex_val in ("female", "f") else ("male" if sex_val in ("male", "m") else "unknown")

            # idempotency: if already selected, just acknowledge and continue
            if data.get("sex") == sex:
                if update.callback_query:
                    try:
                        await update.callback_query.answer("Already selected.")
                    except Exception:
                        pass
                    await _prompt_for_step(update, context, "weight")
                else:
                    await update.message.reply_text("Already selected. What’s the weight in kg?")
                return

            # save selection and move to weight
            data["sex"] = sex
            context.user_data[_ADD_PREFIX + "data"] = data
            context.user_data[_ADD_PREFIX + "step"] = "weight"

            # acknowledge the callback so the client stops showing a spinner
            if update.callback_query:
                try:
                    await update.callback_query.answer()
                except Exception:
                    pass
                await update.callback_query.edit_message_text(f"Selected sex: {sex.capitalize()}.\n\nWhat’s the weight in kg? (number)", reply_markup=_footer_kb())
            else:
                await update.message.reply_text(f"Selected sex: {sex.capitalize()}.\n\nWhat’s the weight in kg? (number)", reply_markup=_footer_kb())
            return

        # ---------- Handler for optional field buttons ----------
        if cmd == "add_field" and arg:
            field = arg
            if field == "finish":
                # finish flow
                created_id = context.user_data.get(_ADD_PREFIX + "created_id")
                if not created_id:
                    # shouldn't happen, but handle gracefully
                    _clear_add_flow(context.user_data)
                    if update.callback_query:
                        await update.callback_query.answer()
                        await update.callback_query.edit_message_text("❌ No animal in progress. Start again with Add Animal.")
                    else:
                        await update.message.reply_text("❌ No animal in progress. Start again with Add Animal.")
                    return
                # Offer to add another or go back to list
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Add Another", callback_data="animal:add"), InlineKeyboardButton("📋 Back to list", callback_data="animal:list:0")]
                ])
                if update.callback_query:
                    await update.callback_query.answer()
                    await update.callback_query.edit_message_text("✅ Done. What would you like to do next?", reply_markup=kb)
                else:
                    await update.message.reply_text("✅ Done. What would you like to do next?", reply_markup=kb)
                # clear state for safety
                for k in list(context.user_data.keys()):
                    if k.startswith(_ADD_PREFIX) or k == "flow":
                        context.user_data.pop(k, None)
                return

            # if asking to add a specific optional field, prompt accordingly
            available = context.user_data.get(_ADD_PREFIX + "available", [])
            if field not in available:
                # already filled or not available
                if update.callback_query:
                    try:
                        await update.callback_query.answer("This field is already filled or not available.")
                    except Exception:
                        pass
                else:
                    await update.message.reply_text("This field is already filled or not available.")
                return

            # set flow to expect the field value
            if field in ("birth_date", "breed", "sire", "notes"):
                # prompt for text input
                context.user_data[_ADD_PREFIX + "step"] = f"opt_{field}"
                context.user_data[_ADD_PREFIX + "editing_field"] = field
                if field == "birth_date":
                    msg = "Send birth date (YYYY or YYYY-MM or YYYY-MM-DD) or send - to cancel."
                elif field == "breed":
                    msg = "Send the breed (e.g. Jersey) or send - to cancel."
                elif field == "sire":
                    msg = "Send sire (father) tag to link this animal or send - to cancel."
                else:
                    msg = "Send notes (or - to cancel)."
                if update.callback_query:
                    await update.callback_query.answer()
                    await update.callback_query.edit_message_text(msg)
                else:
                    await update.message.reply_text(msg)
                return

            if field == "lactation":
                # show lactation choices
                choices = ["1", "2", "3", "dry", "unknown"]
                kb_rows = _make_yesno_row(choices, "animal:add_lact_opt")
                kb_rows.append([InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")])
                kb = InlineKeyboardMarkup(kb_rows)
                txt = "Choose lactation phase (1 / 2 / 3 / dry) or cancel:"
                if update.callback_query:
                    await update.callback_query.answer()
                    await update.callback_query.edit_message_text(txt, reply_markup=kb)
                else:
                    await update.message.reply_text(txt, reply_markup=kb)
                # record editing field
                context.user_data[_ADD_PREFIX + "editing_field"] = "lactation"
                context.user_data[_ADD_PREFIX + "step"] = "opt_lactation"
                return

            if field == "reproduction":
                repro_choices = [
                    ("pregnant", "🤰 Pregnant"), ("dry_off", "🛌 Dry Off"), ("lactating", "🍼 Lactating"), ("estrus", "🔄 Estrus"),
                    ("immature", "🚫 Immature"), ("inseminated", "📅 Inseminated"), ("postpartum", "🩹 Postpartum"), ("aborted", "❌ Aborted"), ("unknown", "❓ Unknown"),
                ]
                kb_rows = []
                for i in range(0, len(repro_choices), 2):
                    row = []
                    for j in range(2):
                        if i+j < len(repro_choices):
                            val, label = repro_choices[i+j]
                            row.append(InlineKeyboardButton(label, callback_data=f"animal:add_repro_opt:{val}"))
                    kb_rows.append(row)
                kb_rows.append([InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")])
                kb = InlineKeyboardMarkup(kb_rows)
                txt = "Choose reproduction stage or cancel:"
                if update.callback_query:
                    await update.callback_query.answer()
                    await update.callback_query.edit_message_text(txt, reply_markup=kb)
                else:
                    await update.message.reply_text(txt, reply_markup=kb)
                context.user_data[_ADD_PREFIX + "editing_field"] = "reproduction"
                context.user_data[_ADD_PREFIX + "step"] = "opt_reproduction"
                return

        # ---------- Cancel during add ----------
        if cmd == "add_cancel":
            _clear_add_flow(context.user_data)
            if update.callback_query:
                try:
                    await update.callback_query.answer()
                except Exception:
                    pass
                await update.callback_query.edit_message_text("❌ Animal registration cancelled.")
            else:
                await update.message.reply_text("❌ Animal registration cancelled.")
            return

        # ---------- List animals (detailed) ----------
        if cmd == "list":
            page = int(arg) if arg and arg.isdigit() else 0
            combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
            if not combined or not combined.get("farm"):
                text = "⚠️ Farm not found. Please register a farm first with /start."
                if update.callback_query:
                    await update.callback_query.edit_message_text(text)
                else:
                    await update.message.reply_text(text)
                return
            farm = combined["farm"]
            animals = await async_list_animals(farm["id"], limit=1000)
            total = len(animals)
            start = page * _PAGE_SIZE
            end = start + _PAGE_SIZE
            page_animals = animals[start:end]

            # Build detailed text for each animal on page (plain text)
            header = f"Animals on your farm — page {page+1} / {max(1, (total + _PAGE_SIZE -1)//_PAGE_SIZE)}\n\n"
            if page_animals:
                parts = []
                for idx, a in enumerate(page_animals, start=1):
                    parts.append(f"{idx + start}. " + _format_animal_full(a))
                    parts.append("")  # blank line
                body = "\n".join(parts).rstrip()
            else:
                body = "No animals on this page."

            text = header + body

            # Build keyboard: one row per animal with actions (view/edit/delete)
            kb_rows = []
            for a in page_animals:
                display = f"{a.get('name') or a.get('tag') or 'Unnamed'}"
                # Keep callback_data short but include id & page
                kb_rows.append([
                    InlineKeyboardButton(f"View {display}", callback_data=f"animal:view:{a.get('id')}:{page}"),
                    InlineKeyboardButton("Edit", callback_data=f"animal:edit:{a.get('id')}:{page}"),
                    InlineKeyboardButton("Delete", callback_data=f"animal:confirm_delete:{a.get('id')}:{page}")
                ])

            # Paging buttons
            nav_row = []
            if start > 0:
                nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"animal:list:{page-1}"))
            if end < total:
                nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"animal:list:{page+1}"))
            if nav_row:
                kb_rows.append(nav_row)

            kb_rows.append([InlineKeyboardButton("➕ Add new", callback_data="animal:add"), InlineKeyboardButton("🔙 Back", callback_data="skip")])
            kb = InlineKeyboardMarkup(kb_rows)

            # Edit message if callback, else send new message
            if update.callback_query:
                try:
                    # answer callback to clear spinner
                    try:
                        await update.callback_query.answer()
                    except Exception:
                        pass
                    # prefer edit; if fails (too long) fallback to sending new message
                    await update.callback_query.edit_message_text(text, reply_markup=kb)
                except Exception:
                    logger.warning("edit_message_text failed for long animal list; sending new message instead")
                    try:
                        await update.callback_query.message.reply_text(text, reply_markup=kb)
                    except Exception:
                        logger.exception("Failed to send fallback message for animal list")
            else:
                await update.message.reply_text(text, reply_markup=kb)
            return

        # ---------- View single animal ----------
        if cmd == "view" and arg:
            animal_id = arg
            page = int(arg2) if arg2 and arg2.isdigit() else 0
            a = await async_get_animal(animal_id)
            if not a:
                txt = "⚠️ Animal not found."
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt)
                else:
                    await update.message.reply_text(txt)
                return
            # plain-text view
            text = (
                f"Name: {a.get('name') or 'Unnamed'}\n\n"
                f"Tag: {a.get('tag')}\n"
                f"Breed: {a.get('breed') or '—'}\n"
                f"Sex: {a.get('sex') or '—'}\n"
                f"Stage: {a.get('stage') or ((a.get('meta') or {}).get('stage') if isinstance(a.get('meta'), dict) else '—')}\n"
                f"Lactation: {a.get('lactation_stage') or ((a.get('meta') or {}).get('lactation_stage') if isinstance(a.get('meta'), dict) else '—')}\n"
                f"Repro phase: {a.get('repro_phase') or '—'}\n"
                f"Weight: {a.get('weight') or '—'} {a.get('weight_unit') or ''}\n"
                f"Status: {a.get('status') or '—'}\n"
                f"Birth date: {a.get('birth_date') or '—'}\n"
                f"Created: {a.get('created_at') or '—'}\n"
                f"Updated: {a.get('updated_at') or '—'}\n"
                f"Notes: {(a.get('meta') or {}).get('notes') if isinstance(a.get('meta'), dict) else '—'}\n"
            )
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Edit", callback_data=f"animal:edit:{animal_id}:{page}"),
                 InlineKeyboardButton("Delete", callback_data=f"animal:confirm_delete:{animal_id}:{page}")],
                [InlineKeyboardButton("Back to list", callback_data=f"animal:list:{page}")]
            ])
            if update.callback_query:
                try:
                    await update.callback_query.answer()
                except Exception:
                    pass
                await update.callback_query.edit_message_text(text, reply_markup=kb)
            else:
                await update.message.reply_text(text, reply_markup=kb)
            return

        # ---------- Confirm & Delete ----------
        if cmd == "confirm_delete" and arg:
            animal_id = arg
            page = int(arg2) if arg2 and arg2.isdigit() else 0
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Yes, delete", callback_data=f"animal:delete:{animal_id}:{page}"),
                 InlineKeyboardButton("No, cancel", callback_data=f"animal:view:{animal_id}:{page}")]
            ])
            if update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text("⚠️ Are you sure you want to permanently delete this animal?", reply_markup=kb)
            else:
                await update.message.reply_text("⚠️ Are you sure you want to permanently delete this animal?", reply_markup=kb)
            return

        if cmd == "delete" and arg:
            animal_id = arg
            page = int(arg2) if arg2 and arg2.isdigit() else 0
            ok = await async_delete_animal(animal_id)
            if ok:
                txt = "✅ Animal deleted."
            else:
                txt = "❌ Failed to delete animal."
            if update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text(txt)
            else:
                await update.message.reply_text(txt)
            # refresh list page
            await router(update, context, f"animal:list:{page}")
            return

        # ---------- Edit flow (simple) ----------
        if cmd == "edit" and arg:
            animal_id = arg
            page = int(arg2) if arg2 and arg2.isdigit() else 0
            a = await async_get_animal(animal_id)
            if not a:
                txt = "⚠️ Animal not found."
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt)
                else:
                    await update.message.reply_text(txt)
                return
            context.user_data["flow"] = "animal_edit"
            context.user_data["animal_edit_step"] = "name"
            context.user_data["animal_edit_id"] = animal_id
            context.user_data["animal_edit_return_page"] = page
            if update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text(f"Editing {a.get('name') or a.get('tag')} — send new name or - to keep:")
            else:
                await update.message.reply_text(f"Editing {a.get('name') or a.get('tag')} — send new name or - to keep:")
            return

        # ---------- Add lactation option (from optional menu) ----------
        if cmd == "add_lact_opt" and arg:
            val = arg.lower()
            animal_id = context.user_data.get(_ADD_PREFIX + "created_id")
            if not animal_id:
                if update.callback_query:
                    await update.callback_query.answer()
                    await update.callback_query.edit_message_text("❌ No animal in progress. Start again.")
                else:
                    await update.message.reply_text("❌ No animal in progress. Start again.")
                return
            upd = {"lactation_stage": val}
            await async_update_animal(animal_id, upd)
            # remove from available
            available = context.user_data.get(_ADD_PREFIX + "available", [])
            if "lactation" in available:
                available.remove("lactation")
            context.user_data[_ADD_PREFIX + "available"] = available
            # update menu
            if update.callback_query:
                try:
                    await update.callback_query.answer()
                except Exception:
                    pass
                await update.callback_query.edit_message_text(f"✅ Lactation saved: {val}", reply_markup=_build_optional_menu(available))
            else:
                await update.message.reply_text(f"✅ Lactation saved: {val}")
            return

        # ---------- Add reproduction option ----------
        if cmd == "add_repro_opt" and arg:
            val = arg.lower()
            animal_id = context.user_data.get(_ADD_PREFIX + "created_id")
            if not animal_id:
                if update.callback_query:
                    await update.callback_query.answer()
                    await update.callback_query.edit_message_text("❌ No animal in progress. Start again.")
                else:
                    await update.message.reply_text("❌ No animal in progress. Start again.")
                return
            upd = {"initial_phase": val}
            await async_update_animal(animal_id, upd)
            available = context.user_data.get(_ADD_PREFIX + "available", [])
            if "reproduction" in available:
                available.remove("reproduction")
            context.user_data[_ADD_PREFIX + "available"] = available
            if update.callback_query:
                try:
                    await update.callback_query.answer()
                except Exception:
                    pass
                await update.callback_query.edit_message_text(f"✅ Reproduction saved: {val}", reply_markup=_build_optional_menu(available))
            else:
                await update.message.reply_text(f"✅ Reproduction saved: {val}")
            return

        # ---------- Unknown action ----------
        if update.callback_query:
            try:
                await update.callback_query.answer("Action not recognized.")
            except Exception:
                pass
        else:
            await update.message.reply_text("Action not recognized.")

    except Exception:
        logger.exception("Error in animal router")
        try:
            if update.callback_query:
                await update.callback_query.edit_message_text("❌ Error handling animal action.")
            else:
                await update.message.reply_text("❌ Error handling animal action.")
        except Exception:
            pass

animal_handlers["router"] = router

# -----------------------
# Helper: prompt text for a given step (new simplified steps)
# -----------------------
async def _prompt_for_step(update: Update, context: ContextTypes.DEFAULT_TYPE, step: str):
    data = context.user_data.get(_ADD_PREFIX + "data", {})
    if step == "tag":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")]
        ])
        msg = "What’s the ear tag / ID? (or send - to skip)"
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return
    if step == "name":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Skip", callback_data="animal:add_field:breed"), InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")]
        ])
        msg = "What’s the name? (or send - to skip)"
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return
    if step == "sex":
        kb_rows = [
            [InlineKeyboardButton("Female 🐄", callback_data="animal:add_sex:female"),
             InlineKeyboardButton("Male 🐂", callback_data="animal:add_sex:male")],
            [InlineKeyboardButton("Unknown ❓", callback_data="animal:add_sex:unknown")],
            [InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")]
        ]
        kb = InlineKeyboardMarkup(kb_rows)
        msg = "Is it Female or Male?"
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return
    if step == "weight":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")]
        ])
        msg = "What’s the weight in kg? (number) — this is important."
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return

# -----------------------
# Save helper: create initial animal after basics (tag/name/sex/weight)
# -----------------------
async def _create_initial_animal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get(_ADD_PREFIX + "data", {})
    combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
    if not combined or not combined.get("farm"):
        _clear_add_flow(context.user_data)
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text("⚠️ Farm not found. Please register a farm first with /start.")
        else:
            await update.message.reply_text("⚠️ Farm not found. Please register a farm first with /start.")
        return
    farm_id = combined["farm"]["id"]

    tag = data.get("tag") or ""
    name = data.get("name")
    sex = data.get("sex", "female")
    weight = data.get("weight")

    created = await async_create_animal(farm_id=farm_id, tag=tag, name=name, sex=sex, weight=weight)
    if not created:
        txt = "❌ Failed to create animal (maybe tag duplicate)."
        # clear state
        _clear_add_flow(context.user_data)
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(txt)
        else:
            await update.message.reply_text(txt)
        return

    # save created id and show optional menu
    created_id = created.get("id")
    context.user_data[_ADD_PREFIX + "created_id"] = created_id

    display = name or tag or created_id
    txt = f"✅ Saved {display} (tag {tag or '—'}, {sex.capitalize()}, {weight or '—'} kg)\n\nWhat else do you want to add?"
    available = context.user_data.get(_ADD_PREFIX + "available", [])
    kb = _build_optional_menu(available)
    if update.callback_query:
        try:
            await update.callback_query.answer()
        except Exception:
            pass
        await update.callback_query.edit_message_text(txt, reply_markup=kb)
    else:
        await update.message.reply_text(txt, reply_markup=kb)

# -----------------------
# Text handler for typed steps
# -----------------------
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Add flow typed states
    if context.user_data.get("flow") == "animal_add":
        step = context.user_data.get(_ADD_PREFIX + "step", "tag")
        text = (update.effective_message.text or "").strip()
        if text.lower() in ("/cancel", "cancel"):
            _clear_add_flow(context.user_data)
            await update.effective_message.reply_text("❌ Animal registration cancelled.")
            return
        # allow user to send '-' to cancel the specific prompt
        if text == "-":
            # if we're in basic steps and user skips tag or name we just set nothing and go next
            if step == "tag":
                data = context.user_data.get(_ADD_PREFIX + "data", {})
                data["tag"] = None
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "name"
                await _prompt_for_step(update, context, "name")
                return
            if step == "name":
                data = context.user_data.get(_ADD_PREFIX + "data", {})
                data["name"] = None
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "sex"
                await _prompt_for_step(update, context, "sex")
                return
            # '-' during optional input cancels that optional edit and returns to menu
            if step and step.startswith("opt_"):
                context.user_data[_ADD_PREFIX + "step"] = None
                context.user_data.pop(_ADD_PREFIX + "editing_field", None)
                available = context.user_data.get(_ADD_PREFIX + "available", [])
                kb = _build_optional_menu(available)
                await update.effective_message.reply_text("Cancelled. Back to options:", reply_markup=kb)
                return

        try:
            data = context.user_data.get(_ADD_PREFIX + "data", {})
            # Basic flow
            if step == "tag":
                data["tag"] = text
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "name"
                await _prompt_for_step(update, context, "name")
                return
            if step == "name":
                data["name"] = text
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "sex"
                await _prompt_for_step(update, context, "sex")
                return

            # typed sex handling (accept typed answers like 'female' or 'f')
            if step == "sex":
                val = text.strip().lower()
                if val in ("female", "f", "male", "m", "unknown", "u"):
                    sex = "female" if val.startswith("f") else ("male" if val.startswith("m") else "unknown")
                    data["sex"] = sex
                    context.user_data[_ADD_PREFIX + "data"] = data
                    context.user_data[_ADD_PREFIX + "step"] = "weight"
                    await _prompt_for_step(update, context, "weight")
                    return
                else:
                    await update.effective_message.reply_text("Please reply with 'Female', 'Male' or 'Unknown' (or press the button).")
                    return

            if step == "opt_birth_date":
                # accept flexible formats YYYY, YYYY-MM, YYYY-MM-DD
                val = text
                # basic validation
                try:
                    if len(val) == 4:
                        int(val)
                    else:
                        datetime.datetime.strptime(val, "%Y-%m-%d")
                except Exception:
                    # try YYYY-MM
                    try:
                        datetime.datetime.strptime(val, "%Y-%m")
                    except Exception:
                        await update.effective_message.reply_text("Invalid date. Use YYYY or YYYY-MM or YYYY-MM-DD or send - to cancel.")
                        return
                animal_id = context.user_data.get(_ADD_PREFIX + "created_id")
                if not animal_id:
                    await update.effective_message.reply_text("No animal in progress. Start again.")
                    return
                upd = {"birth_date": val}
                await async_update_animal(animal_id, upd)
                available = context.user_data.get(_ADD_PREFIX + "available", [])
                if "birth_date" in available:
                    available.remove("birth_date")
                context.user_data[_ADD_PREFIX + "available"] = available
                context.user_data[_ADD_PREFIX + "step"] = None
                context.user_data.pop(_ADD_PREFIX + "editing_field", None)
                kb = _build_optional_menu(available)
                await update.effective_message.reply_text(f"✅ Birth date saved: {val}", reply_markup=kb)
                return
            if step == "opt_breed":
                val = text
                animal_id = context.user_data.get(_ADD_PREFIX + "created_id")
                if not animal_id:
                    await update.effective_message.reply_text("No animal in progress. Start again.")
                    return
                upd = {"breed": val}
                await async_update_animal(animal_id, upd)
                available = context.user_data.get(_ADD_PREFIX + "available", [])
                if "breed" in available:
                    available.remove("breed")
                context.user_data[_ADD_PREFIX + "available"] = available
                context.user_data[_ADD_PREFIX + "step"] = None
                context.user_data.pop(_ADD_PREFIX + "editing_field", None)
                kb = _build_optional_menu(available)
                await update.effective_message.reply_text(f"✅ Breed saved: {val}", reply_markup=kb)
                return
            if step == "opt_sire":
                val = text
                animal_id = context.user_data.get(_ADD_PREFIX + "created_id")
                if not animal_id:
                    await update.effective_message.reply_text("No animal in progress. Start again.")
                    return
                # merge into meta
                upd = {"meta": {"sire_tag": val}}
                await async_update_animal(animal_id, upd)
                available = context.user_data.get(_ADD_PREFIX + "available", [])
                if "sire" in available:
                    available.remove("sire")
                context.user_data[_ADD_PREFIX + "available"] = available
                context.user_data[_ADD_PREFIX + "step"] = None
                context.user_data.pop(_ADD_PREFIX + "editing_field", None)
                kb = _build_optional_menu(available)
                await update.effective_message.reply_text(f"✅ Sire saved: {val}", reply_markup=kb)
                return
            if step == "opt_notes":
                val = text
                animal_id = context.user_data.get(_ADD_PREFIX + "created_id")
                if not animal_id:
                    await update.effective_message.reply_text("No animal in progress. Start again.")
                    return
                upd = {"meta": {"notes": val}}
                await async_update_animal(animal_id, upd)
                available = context.user_data.get(_ADD_PREFIX + "available", [])
                if "notes" in available:
                    available.remove("notes")
                context.user_data[_ADD_PREFIX + "available"] = available
                context.user_data[_ADD_PREFIX + "step"] = None
                context.user_data.pop(_ADD_PREFIX + "editing_field", None)
                kb = _build_optional_menu(available)
                await update.effective_message.reply_text(f"✅ Notes saved.", reply_markup=kb)
                return

            # Weight is a required basic step (after sex)
            if step == "weight":
                try:
                    val = float(text)
                    data["weight"] = val
                    context.user_data[_ADD_PREFIX + "data"] = data
                except ValueError:
                    await update.effective_message.reply_text("Invalid weight. Send a number (e.g. 420) or send - to cancel.")
                    return
                # Create initial animal record now
                await _create_initial_animal(update, context)
                return

        except Exception:
            logger.exception("Error handling add flow text")
            await update.effective_message.reply_text("❌ Error processing input — registration cancelled.")
            _clear_add_flow(context.user_data)
            return

    # Edit flow
    if context.user_data.get("flow") == "animal_edit":
        step = context.user_data.get("animal_edit_step")
        animal_id = context.user_data.get("animal_edit_id")
        if not animal_id:
            await update.effective_message.reply_text("⚠️ Edit flow lost the animal id. Cancelled.")
            context.user_data.pop("flow", None)
            return
        text = (update.effective_message.text or "").strip()
        try:
            if step == "name":
                new_name = None if text == "-" else text
                context.user_data["animal_edit_name"] = new_name
                context.user_data["animal_edit_step"] = "breed_sex"
                await update.effective_message.reply_text("Send new breed and sex separated by comma (e.g. Jersey, female) or - to keep:")
                return
            if step == "breed_sex":
                breed = None
                sex = None
                if text != "-":
                    parts = [p.strip() for p in text.split(",")]
                    if parts:
                        breed = parts[0] or None
                    if len(parts) > 1 and parts[1]:
                        s = parts[1].lower()
                        sex = s if s in ("female", "male", "unknown") else "unknown"
                context.user_data["animal_edit_breed"] = breed
                context.user_data["animal_edit_sex"] = sex
                context.user_data["animal_edit_step"] = "birth_date"
                await update.effective_message.reply_text("Send new birth date YYYY-MM-DD or - to keep:")
                return
            if step == "birth_date":
                birth_date = None
                if text != "-":
                    try:
                        datetime.datetime.strptime(text, "%Y-%m-%d")
                        birth_date = text
                    except Exception:
                        await update.effective_message.reply_text("Invalid date. Use YYYY-MM-DD or - to keep.")
                        return
                context.user_data["animal_edit_birth_date"] = birth_date
                context.user_data["animal_edit_step"] = "weight"
                await update.effective_message.reply_text("Send new weight (number in kg) or - to keep:")
                return
            if step == "weight":
                weight = None
                if text != "-":
                    try:
                        weight = float(text)
                    except ValueError:
                        await update.effective_message.reply_text("Invalid weight. Use number or - to keep.")
                        return
                context.user_data["animal_edit_weight"] = weight
                context.user_data["animal_edit_step"] = "notes"
                await update.effective_message.reply_text("Send new notes or - to keep:")
                return
            if step == "notes":
                notes = None if text == "-" else text
                upd: Dict[str, Any] = {}
                if "animal_edit_name" in context.user_data:
                    nm = context.user_data.get("animal_edit_name")
                    if nm is not None:
                        upd["name"] = nm
                if "animal_edit_breed" in context.user_data:
                    br = context.user_data.get("animal_edit_breed")
                    if br is not None:
                        upd["breed"] = br
                if "animal_edit_sex" in context.user_data:
                    sx = context.user_data.get("animal_edit_sex")
                    if sx is not None:
                        upd["sex"] = sx
                if "animal_edit_birth_date" in context.user_data:
                    bd = context.user_data.get("animal_edit_birth_date")
                    if bd is not None:
                        upd["birth_date"] = bd
                if "animal_edit_weight" in context.user_data:
                    wt = context.user_data.get("animal_edit_weight")
                    if wt is not None:
                        upd["weight"] = wt
                meta_update = {}
                if notes is not None:
                    meta_update["notes"] = notes
                if meta_update:
                    upd["meta"] = meta_update
                if not upd:
                    await update.effective_message.reply_text("No changes provided. Cancelled.")
                    # clear edit state
                    for k in list(context.user_data.keys()):
                        if k.startswith("animal_edit"):
                            context.user_data.pop(k, None)
                    context.user_data.pop("flow", None)
                    return
                updated = await async_update_animal(animal_id, upd)
                if not updated:
                    await update.effective_message.reply_text("❌ Failed to update animal.")
                else:
                    await update.effective_message.reply_text("✅ Animal updated.")
                page = context.user_data.get("animal_edit_return_page", 0)
                for k in list(context.user_data.keys()):
                    if k.startswith("animal_edit"):
                        context.user_data.pop(k, None)
                context.user_data.pop("flow", None)
                await router(update, context, f"animal:list:{page}")
                return
        except Exception:
            logger.exception("Error in edit flow")
            await update.effective_message.reply_text("❌ Error processing edit. Cancelled.")
            for k in list(context.user_data.keys()):
                if k.startswith("animal_edit"):
                    context.user_data.pop(k, None)
            context.user_data.pop("flow", None)
            return

# Expose handlers map
animal_handlers["router"] = router
animal_handlers["handle_text"] = handle_text
animal_handlers["menu"] = menu
'''









'''
#99
import asyncio
import logging
import datetime
from typing import Optional, List, Dict, Any
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from farmcore import (
    async_get_user_with_farm_by_telegram,
    async_list_animals,
    async_create_animal,
    async_get_animal,
    async_update_animal,
    async_delete_animal,
)

logger = logging.getLogger(__name__)
animal_handlers: Dict[str, Any] = {}

_PAGE_SIZE = 8
_ADD_PREFIX = "animal_add_"  # prefix used in context.user_data for add-flow keys

# -----------------------
# Utilities
# -----------------------
def _format_animal_full(a: dict) -> str:
    """Return a multiline detailed string for a single animal row."""
    name = a.get("name") or "—"
    tag = a.get("tag") or "—"
    breed = a.get("breed") or "—"
    sex = a.get("sex") or "—"
    stage = a.get("stage") or ( (a.get("meta") or {}).get("stage") if isinstance(a.get("meta"), dict) else "—")
    lact = a.get("lactation_stage") or ( (a.get("meta") or {}).get("lactation_stage") if isinstance(a.get("meta"), dict) else "—")
    repro = a.get("repro_phase") or "—"
    birth = a.get("birth_date") or "—"
    weight = a.get("weight") or "—"
    weight_unit = a.get("weight_unit") or ""
    status = a.get("status") or "—"
    created = a.get("created_at") or "—"
    updated = a.get("updated_at") or "—"
    notes = (a.get("meta") or {}).get("notes") if isinstance(a.get("meta"), dict) else None
    sire = a.get("sire_id") or ( (a.get("meta") or {}).get("sire_tag") if isinstance(a.get("meta"), dict) else None)
    lines = [
        f"🐄 *{name}*  — tag: `{tag}`",
        f"• Breed: {breed}   • Sex: {sex}   • Stage: {stage}   • Lactation: {lact}",
        f"• Repro phase: {repro}",
        f"• Birth date: {birth}   • Status: {status}",
        f"• Weight: {weight} {weight_unit}",
        f"• Sire/father: `{sire or '—'}`",
        f"• Created: {created}   • Updated: {updated}",
        f"• Notes: {notes or '—'}",
    ]
    return "\n".join(lines)

def _clear_add_flow(user_data: dict):
    for k in list(user_data.keys()):
        if k.startswith(_ADD_PREFIX) or k in ("flow",):
            user_data.pop(k, None)

def _footer_kb(skip_label: str = "Skip", save_label: str = "Save & finish", cancel_label: str = "Cancel"):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(skip_label, callback_data="animal:add_skip"),
         InlineKeyboardButton(save_label, callback_data="animal:add_save")],
        [InlineKeyboardButton(cancel_label, callback_data="animal:add_cancel")]
    ])

def _make_yesno_row(values: List[str], cb_prefix: str):
    rows = []
    for i in range(0, len(values), 2):
        pair = values[i:i+2]
        row = [InlineKeyboardButton(v.capitalize(), callback_data=f"{cb_prefix}:{v}") for v in pair]
        rows.append(row)
    return rows

# -----------------------
# Menu
# -----------------------
async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Animal", callback_data="animal:add")],
        [InlineKeyboardButton("📋 List Animals", callback_data="animal:list:0")],
        [InlineKeyboardButton("🔙 Back", callback_data="skip")]
    ])
    try:
        if update.callback_query:
            await update.callback_query.edit_message_text("🐮 Animals — choose an action:", reply_markup=kb)
        else:
            await update.message.reply_text("🐮 Animals — choose an action:", reply_markup=kb)
    except Exception:
        logger.exception("Failed to show animals menu")

animal_handlers["menu"] = menu

# -----------------------
# Router
# -----------------------
async def router(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str):
    """
    Actions:
      - add
      - list:<page>
      - view:<animal_id>:<page>
      - edit:<animal_id>:<page>
      - confirm_delete:<animal_id>:<page>
      - delete:<animal_id>:<page>
      - add_gender:<val>
      - add_stage:<val>
      - add_lact:<val>
      - add_repro:<val>
      - add_skip
      - add_save
      - add_cancel
    """
    try:
        parts = action.split(":")
        cmd = parts[0] if parts else ""
        arg = parts[1] if len(parts) > 1 else ""
        arg2 = parts[2] if len(parts) > 2 else None

        # ---------- Start add flow ----------
        if cmd == "add":
            context.user_data["flow"] = "animal_add"
            context.user_data[_ADD_PREFIX + "data"] = {}
            context.user_data[_ADD_PREFIX + "step"] = "tag"
            kb = _footer_kb()
            msg = (
                "➕ Adding a new animal — send the *tag* (unique id) or send `-` to skip.\n\n"
                "You can press *Save & finish* at any time to persist what's filled so far."
            )
            if update.callback_query:
                await update.callback_query.edit_message_text(msg, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=kb)
            return

        # ---------- List animals (detailed) ----------
        if cmd == "list":
            page = int(arg) if arg and arg.isdigit() else 0
            combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
            if not combined or not combined.get("farm"):
                text = "⚠️ Farm not found. Please register a farm first with /start."
                if update.callback_query:
                    await update.callback_query.edit_message_text(text)
                else:
                    await update.message.reply_text(text)
                return
            farm = combined["farm"]
            animals = await async_list_animals(farm["id"], limit=1000)
            total = len(animals)
            start = page * _PAGE_SIZE
            end = start + _PAGE_SIZE
            page_animals = animals[start:end]

            # Build detailed text for each animal on page
            header = f"*Animals on your farm* — page {page+1} / {max(1, (total + _PAGE_SIZE -1)//_PAGE_SIZE)}\n\n"
            if page_animals:
                parts = []
                for idx, a in enumerate(page_animals, start=1):
                    parts.append(f"*{idx + start}.* " + _format_animal_full(a))
                    parts.append("")  # blank line
                body = "\n".join(parts).rstrip()
            else:
                body = "No animals on this page."

            text = header + body

            # Build keyboard: one row per animal with actions (view/edit/delete)
            kb_rows = []
            for a in page_animals:
                display = f"{a.get('name') or a.get('tag') or 'Unnamed'}"
                # Keep callback_data short but include id & page
                kb_rows.append([
                    InlineKeyboardButton(f"View {display}", callback_data=f"animal:view:{a.get('id')}:{page}"),
                    InlineKeyboardButton("Edit", callback_data=f"animal:edit:{a.get('id')}:{page}"),
                    InlineKeyboardButton("Delete", callback_data=f"animal:confirm_delete:{a.get('id')}:{page}")
                ])

            # Paging buttons
            nav_row = []
            if start > 0:
                nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"animal:list:{page-1}"))
            if end < total:
                nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"animal:list:{page+1}"))
            if nav_row:
                kb_rows.append(nav_row)

            kb_rows.append([InlineKeyboardButton("➕ Add new", callback_data="animal:add"), InlineKeyboardButton("🔙 Back", callback_data="skip")])
            kb = InlineKeyboardMarkup(kb_rows)

            # Edit message if callback, else send new message
            if update.callback_query:
                # sometimes editing long messages fails; catch and fallback to sending a new message
                try:
                    await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
                except Exception:
                    logger.warning("edit_message_text failed for long animal list; sending new message instead")
                    await update.callback_query.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
            return

        # ---------- View single animal ----------
        if cmd == "view" and arg:
            animal_id = arg
            page = int(arg2) if arg2 and arg2.isdigit() else 0
            a = await async_get_animal(animal_id)
            if not a:
                txt = "⚠️ Animal not found."
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt)
                else:
                    await update.message.reply_text(txt)
                return
            text = (
                f"🐄 *{a.get('name') or 'Unnamed'}*\n\n"
                f"Tag: `{a.get('tag')}`\n"
                f"Breed: {a.get('breed') or '—'}\n"
                f"Sex: {a.get('sex') or '—'}\n"
                f"Stage: {a.get('stage') or ( (a.get('meta') or {}).get('stage') if isinstance(a.get('meta'), dict) else '—')}\n"
                f"Lactation: {a.get('lactation_stage') or ( (a.get('meta') or {}).get('lactation_stage') if isinstance(a.get('meta'), dict) else '—')}\n"
                f"Repro phase: {a.get('repro_phase') or '—'}\n"
                f"Weight: {a.get('weight') or '—'} {a.get('weight_unit') or ''}\n"
                f"Status: {a.get('status') or '—'}\n"
                f"Birth date: {a.get('birth_date') or '—'}\n"
                f"Created: {a.get('created_at') or '—'}\n"
                f"Updated: {a.get('updated_at') or '—'}\n"
                f"Notes: { (a.get('meta') or {}).get('notes') if isinstance(a.get('meta'), dict) else '—' }\n"
            )
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ Edit", callback_data=f"animal:edit:{animal_id}:{page}"),
                 InlineKeyboardButton("🗑 Delete", callback_data=f"animal:confirm_delete:{animal_id}:{page}")],
                [InlineKeyboardButton("🔙 Back to list", callback_data=f"animal:list:{page}")]
            ])
            if update.callback_query:
                await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
            return

        # ---------- Confirm & Delete ----------
        if cmd == "confirm_delete" and arg:
            animal_id = arg
            page = int(arg2) if arg2 and arg2.isdigit() else 0
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Yes, delete", callback_data=f"animal:delete:{animal_id}:{page}"),
                 InlineKeyboardButton("No, cancel", callback_data=f"animal:view:{animal_id}:{page}")]
            ])
            if update.callback_query:
                await update.callback_query.edit_message_text("⚠️ Are you sure you want to *permanently delete* this animal?", parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text("⚠️ Are you sure you want to *permanently delete* this animal?", parse_mode="Markdown", reply_markup=kb)
            return

        if cmd == "delete" and arg:
            animal_id = arg
            page = int(arg2) if arg2 and arg2.isdigit() else 0
            ok = await async_delete_animal(animal_id)
            if ok:
                txt = "✅ Animal deleted."
            else:
                txt = "❌ Failed to delete animal."
            if update.callback_query:
                await update.callback_query.edit_message_text(txt)
            else:
                await update.message.reply_text(txt)
            # refresh list page
            await router(update, context, f"animal:list:{page}")
            return

        # ---------- Edit flow (simple) ----------
        if cmd == "edit" and arg:
            animal_id = arg
            page = int(arg2) if arg2 and arg2.isdigit() else 0
            a = await async_get_animal(animal_id)
            if not a:
                txt = "⚠️ Animal not found."
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt)
                else:
                    await update.message.reply_text(txt)
                return
            context.user_data["flow"] = "animal_edit"
            context.user_data["animal_edit_step"] = "name"
            context.user_data["animal_edit_id"] = animal_id
            context.user_data["animal_edit_return_page"] = page
            if update.callback_query:
                await update.callback_query.edit_message_text(f"Editing *{a.get('name') or a.get('tag')}* — send new *name* or `-` to keep:", parse_mode="Markdown")
            else:
                await update.message.reply_text(f"Editing *{a.get('name') or a.get('tag')}* — send new *name* or `-` to keep:", parse_mode="Markdown")
            return

        # ---------- Add flow inline choices: gender / stage / lactation / repro ----------
        if cmd == "add_gender" and arg:
            g = arg.lower()
            data = context.user_data.get(_ADD_PREFIX + "data", {})
            data["sex"] = "female" if g in ("female", "f") else ("male" if g in ("male", "m") else "unknown")
            context.user_data[_ADD_PREFIX + "data"] = data
            context.user_data[_ADD_PREFIX + "step"] = "stage"
            if data["sex"] == "female":
                choices = ["calf", "heifer", "cow", "unknown"]
            else:
                choices = ["calf", "bull", "steer", "unknown"]
            kb_rows = _make_yesno_row(choices, "animal:add_stage")
            kb_rows.append([InlineKeyboardButton("Skip", callback_data="animal:add_skip"),
                            InlineKeyboardButton("Save & finish", callback_data="animal:add_save")])
            kb_rows.append([InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")])
            kb = InlineKeyboardMarkup(kb_rows)
            txt = f"Selected gender: *{data['sex']}* — now choose stage:"
            if update.callback_query:
                await update.callback_query.edit_message_text(txt, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text(txt, parse_mode="Markdown", reply_markup=kb)
            return

        if cmd == "add_stage" and arg:
            st = arg.lower()
            data = context.user_data.get(_ADD_PREFIX + "data", {})
            data["stage"] = st
            context.user_data[_ADD_PREFIX + "data"] = data
            if data.get("sex") == "female":
                if st == "cow":
                    context.user_data[_ADD_PREFIX + "step"] = "lact"
                    choices = ["1", "2", "3", "dry", "unknown"]
                    kb_rows = _make_yesno_row(choices, "animal:add_lact")
                    kb_rows.append([InlineKeyboardButton("Skip", callback_data="animal:add_skip"),
                                    InlineKeyboardButton("Save & finish", callback_data="animal:add_save")])
                    kb_rows.append([InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")])
                    kb = InlineKeyboardMarkup(kb_rows)
                    txt = "This is a cow — choose lactation phase (1 / 2 / 3 / dry) or skip:"
                    if update.callback_query:
                        await update.callback_query.edit_message_text(txt, reply_markup=kb)
                    else:
                        await update.message.reply_text(txt, reply_markup=kb)
                    return
                else:
                    context.user_data[_ADD_PREFIX + "step"] = "repro"
                    txt = "Choose initial reproductive phase:"
                    repro_choices = [
                        "🤰 Pregnant", "🛌 Dry Off", "🍼 Lactating", "🔄 Estrus",
                        "🚫 Immature", "📅 Inseminated", "🩹 Postpartum",
                        "❌ Aborted", "❓ Unknown"
                    ]
                    repro_values = ["pregnant", "dry_off", "lactating", "estrus", "immature", "inseminated", "postpartum", "aborted", "unknown"]
                    kb_rows = []
                    for i in range(0, len(repro_choices), 2):
                        row = []
                        for j in range(2):
                            if i + j < len(repro_choices):
                                row.append(InlineKeyboardButton(repro_choices[i + j], callback_data=f"animal:add_repro:{repro_values[i + j]}"))
                        if row:
                            kb_rows.append(row)
                    kb_rows.append([InlineKeyboardButton("Skip", callback_data="animal:add_skip"),
                                    InlineKeyboardButton("Save & finish", callback_data="animal:add_save")])
                    kb_rows.append([InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")])
                    kb = InlineKeyboardMarkup(kb_rows)
                    if update.callback_query:
                        await update.callback_query.edit_message_text(txt, reply_markup=kb)
                    else:
                        await update.message.reply_text(txt, reply_markup=kb)
                    return
            else:
                context.user_data[_ADD_PREFIX + "step"] = "birth_date"
                kb = _footer_kb()
                txt = "Send birth date YYYY-MM-DD (or `-` to skip)."
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt, reply_markup=kb)
                else:
                    await update.message.reply_text(txt, reply_markup=kb)
                return

        if cmd == "add_lact" and arg:
            val = arg.lower()
            data = context.user_data.get(_ADD_PREFIX + "data", {})
            data["lactation_stage"] = val
            context.user_data[_ADD_PREFIX + "data"] = data
            context.user_data[_ADD_PREFIX + "step"] = "repro"
            txt = "Choose initial reproductive phase:"
            repro_choices = [
                "🤰 Pregnant", "🛌 Dry Off", "🍼 Lactating", "🔄 Estrus",
                "🚫 Immature", "📅 Inseminated", "🩹 Postpartum",
                "❌ Aborted", "❓ Unknown"
            ]
            repro_values = ["pregnant", "dry_off", "lactating", "estrus", "immature", "inseminated", "postpartum", "aborted", "unknown"]
            kb_rows = []
            for i in range(0, len(repro_choices), 2):
                row = []
                for j in range(2):
                    if i + j < len(repro_choices):
                        row.append(InlineKeyboardButton(repro_choices[i + j], callback_data=f"animal:add_repro:{repro_values[i + j]}"))
                if row:
                    kb_rows.append(row)
            kb_rows.append([InlineKeyboardButton("Skip", callback_data="animal:add_skip"),
                            InlineKeyboardButton("Save & finish", callback_data="animal:add_save")])
            kb_rows.append([InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")])
            kb = InlineKeyboardMarkup(kb_rows)
            if update.callback_query:
                await update.callback_query.edit_message_text(txt, reply_markup=kb)
            else:
                await update.message.reply_text(txt, reply_markup=kb)
            return

        if cmd == "add_repro" and arg:
            val = arg.lower()
            data = context.user_data.get(_ADD_PREFIX + "data", {})
            data["initial_phase"] = val
            context.user_data[_ADD_PREFIX + "data"] = data
            context.user_data[_ADD_PREFIX + "step"] = "birth_date"
            kb = _footer_kb()
            txt = "Send birth date YYYY-MM-DD (or `-` to skip)."
            if update.callback_query:
                await update.callback_query.edit_message_text(txt, reply_markup=kb)
            else:
                await update.message.reply_text(txt, reply_markup=kb)
            return

        # ---------- Skip / Save / Cancel ----------
        if cmd == "add_skip":
            step = context.user_data.get(_ADD_PREFIX + "step", "tag")
            order = ["tag", "name", "gender", "stage", "lact", "repro", "birth_date", "weight", "breed", "sire", "notes"]
            try:
                idx = order.index(step)
            except ValueError:
                idx = 0
            next_idx = idx + 1
            if next_idx >= len(order):
                await _save_current_animal(update, context)
                return
            next_step = order[next_idx]
            data = context.user_data.get(_ADD_PREFIX + "data", {})
            # Conditional skips
            while True:
                if next_step == "lact" and not (data.get("sex") == "female" and data.get("stage") == "cow"):
                    next_idx += 1
                elif next_step == "repro" and data.get("sex") != "female":
                    next_idx += 1
                else:
                    break
                if next_idx >= len(order):
                    await _save_current_animal(update, context)
                    return
                next_step = order[next_idx]
            context.user_data[_ADD_PREFIX + "step"] = next_step
            await _prompt_for_step(update, context, next_step)
            return

        if cmd == "add_save":
            await _save_current_animal(update, context)
            return

        if cmd == "add_cancel":
            _clear_add_flow(context.user_data)
            if update.callback_query:
                await update.callback_query.edit_message_text("❌ Animal registration cancelled.")
            else:
                await update.message.reply_text("❌ Animal registration cancelled.")
            return

        # Unknown action
        if update.callback_query:
            await update.callback_query.answer("Action not recognized.")
        else:
            await update.message.reply_text("Action not recognized.")

    except Exception:
        logger.exception("Error in animal router")
        try:
            if update.callback_query:
                await update.callback_query.edit_message_text("❌ Error handling animal action.")
            else:
                await update.message.reply_text("❌ Error handling animal action.")
        except Exception:
            pass

animal_handlers["router"] = router

# -----------------------
# Helper: prompt text for a given step
# -----------------------
async def _prompt_for_step(update: Update, context: ContextTypes.DEFAULT_TYPE, step: str):
    data = context.user_data.get(_ADD_PREFIX + "data", {})
    if step == "tag":
        kb = _footer_kb()
        msg = "Send the *tag* (unique id) for the animal or send `-` to skip."
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, parse_mode="Markdown", reply_markup=kb)
        else:
            await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=kb)
        return
    if step == "name":
        kb = _footer_kb()
        msg = "Send the *name* of the animal or `-` to skip."
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, parse_mode="Markdown", reply_markup=kb)
        else:
            await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=kb)
        return
    if step == "gender":
        kb_rows = [
            [InlineKeyboardButton("Female", callback_data="animal:add_gender:f"),
             InlineKeyboardButton("Male", callback_data="animal:add_gender:m")],
            [InlineKeyboardButton("Unknown", callback_data="animal:add_gender:unknown")],
            [InlineKeyboardButton("Skip", callback_data="animal:add_skip"),
             InlineKeyboardButton("Save & finish", callback_data="animal:add_save")],
            [InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")]
        ]
        kb = InlineKeyboardMarkup(kb_rows)
        msg = "Choose the *gender* of the animal:"
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, parse_mode="Markdown", reply_markup=kb)
        else:
            await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=kb)
        return
    if step == "stage":
        if data.get("sex") == "female":
            choices = ["Calf", "Heifer", "Cow", "Unknown"]
        else:
            choices = ["Calf", "Bull", "Steer", "Unknown"]
        kb_rows = _make_yesno_row([c.lower() for c in choices], "animal:add_stage")
        kb_rows.append([InlineKeyboardButton("Skip", callback_data="animal:add_skip"),
                        InlineKeyboardButton("Save & finish", callback_data="animal:add_save")])
        kb_rows.append([InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")])
        kb = InlineKeyboardMarkup(kb_rows)
        msg = "Choose stage:"
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return
    if step == "lact":
        kb_rows = _make_yesno_row(["1", "2", "3", "dry", "unknown"], "animal:add_lact")
        kb_rows.append([InlineKeyboardButton("Skip", callback_data="animal:add_skip"),
                        InlineKeyboardButton("Save & finish", callback_data="animal:add_save")])
        kb_rows.append([InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")])
        kb = InlineKeyboardMarkup(kb_rows)
        msg = "Choose lactation phase (1 / 2 / 3 / dry) or skip:"
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return
    if step == "repro":
        txt = "Choose initial reproductive phase:"
        repro_choices = [
            "🤰 Pregnant", "🛌 Dry Off", "🍼 Lactating", "🔄 Estrus",
            "🚫 Immature", "📅 Inseminated", "🩹 Postpartum",
            "❌ Aborted", "❓ Unknown"
        ]
        repro_values = ["pregnant", "dry_off", "lactating", "estrus", "immature", "inseminated", "postpartum", "aborted", "unknown"]
        kb_rows = []
        for i in range(0, len(repro_choices), 2):
            row = []
            for j in range(2):
                if i + j < len(repro_choices):
                    row.append(InlineKeyboardButton(repro_choices[i + j], callback_data=f"animal:add_repro:{repro_values[i + j]}"))
            if row:
                kb_rows.append(row)
        kb_rows.append([InlineKeyboardButton("Skip", callback_data="animal:add_skip"),
                        InlineKeyboardButton("Save & finish", callback_data="animal:add_save")])
        kb_rows.append([InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")])
        kb = InlineKeyboardMarkup(kb_rows)
        if update.callback_query:
            await update.callback_query.edit_message_text(txt, reply_markup=kb)
        else:
            await update.message.reply_text(txt, reply_markup=kb)
        return
    if step == "birth_date":
        kb = _footer_kb()
        msg = "Send birth date YYYY-MM-DD or send `-` to skip."
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return
    if step == "weight":
        kb = _footer_kb()
        msg = "Send weight in kg (number) or send `-` to skip."
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return
    if step == "breed":
        kb = _footer_kb()
        msg = "Send the breed (e.g. Jersey) or `-` to skip."
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return
    if step == "sire":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Skip", callback_data="animal:add_skip"),
             InlineKeyboardButton("Save & finish", callback_data="animal:add_save")],
            [InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")]
        ])
        msg = "Send sire (father) tag to link this animal or `-` to skip."
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return
    if step == "notes":
        kb = _footer_kb()
        msg = "Send optional notes (or `-` to skip). After this the animal will be saved."
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return

# -----------------------
# Save helper
# -----------------------
async def _save_current_animal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get(_ADD_PREFIX + "data", {})
    combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
    if not combined or not combined.get("farm"):
        _clear_add_flow(context.user_data)
        if update.callback_query:
            await update.callback_query.edit_message_text("⚠️ Farm not found. Please register a farm first with /start.")
        else:
            await update.message.reply_text("⚠️ Farm not found. Please register a farm first with /start.")
        return
    farm_id = combined["farm"]["id"]

    tag = data.get("tag") or ""
    name = data.get("name")
    breed = data.get("breed")
    sex = data.get("sex", "female")
    birth_date = data.get("birth_date")
    weight = data.get("weight")
    initial_phase = data.get("initial_phase")
    meta = {}
    if data.get("lactation_stage"):
        meta["lactation_stage"] = data.get("lactation_stage")
    if data.get("notes"):
        meta["notes"] = data.get("notes")
    if data.get("sire_tag"):
        meta["sire_tag"] = data.get("sire_tag")
    if data.get("stage"):
        meta["stage"] = data.get("stage")

    created = await async_create_animal(farm_id=farm_id, tag=tag, name=name, breed=breed, sex=sex, birth_date=birth_date, meta=meta, weight=weight, initial_phase=initial_phase)
    if not created:
        txt = "❌ Failed to create animal (maybe tag duplicate)."
    else:
        display = name or tag or created.get("id")
        txt = f"✅ Animal saved: *{display}*"
    _clear_add_flow(context.user_data)
    try:
        if update.callback_query:
            await update.callback_query.edit_message_text(txt, parse_mode="Markdown")
        else:
            await update.message.reply_text(txt, parse_mode="Markdown")
    except Exception:
        try:
            await update.effective_message.reply_text(txt, parse_mode="Markdown")
        except Exception:
            pass

# -----------------------
# Text handler for typed steps
# -----------------------
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Add flow typed states
    if context.user_data.get("flow") == "animal_add":
        step = context.user_data.get(_ADD_PREFIX + "step", "tag")
        text = (update.effective_message.text or "").strip()
        if text.lower() in ("/cancel", "cancel"):
            _clear_add_flow(context.user_data)
            await update.effective_message.reply_text("❌ Animal registration cancelled.")
            return
        if text == "-":
            await router(update, context, "animal:add_skip")
            return
        try:
            data = context.user_data.get(_ADD_PREFIX + "data", {})
            if step == "tag":
                data["tag"] = text
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "name"
                await _prompt_for_step(update, context, "name")
                return
            if step == "name":
                data["name"] = text
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "gender"
                await _prompt_for_step(update, context, "gender")
                return
            if step == "birth_date":
                try:
                    datetime.datetime.strptime(text, "%Y-%m-%d")
                    data["birth_date"] = text
                except Exception:
                    await update.effective_message.reply_text("Invalid date format. Use YYYY-MM-DD or send `-` to skip.")
                    return
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "weight"
                await _prompt_for_step(update, context, "weight")
                return
            if step == "weight":
                try:
                    data["weight"] = float(text)
                except ValueError:
                    await update.effective_message.reply_text("Invalid weight. Send a number or `-` to skip.")
                    return
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "breed"
                await _prompt_for_step(update, context, "breed")
                return
            if step == "breed":
                data["breed"] = text
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "sire"
                await _prompt_for_step(update, context, "sire")
                return
            if step == "sire":
                data["sire_tag"] = text
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "notes"
                await _prompt_for_step(update, context, "notes")
                return
            if step == "notes":
                data["notes"] = text
                context.user_data[_ADD_PREFIX + "data"] = data
                await _save_current_animal(update, context)
                return
        except Exception:
            logger.exception("Error handling add flow text")
            await update.effective_message.reply_text("❌ Error processing input — registration cancelled.")
            _clear_add_flow(context.user_data)
            return

    # Edit flow
    if context.user_data.get("flow") == "animal_edit":
        step = context.user_data.get("animal_edit_step")
        animal_id = context.user_data.get("animal_edit_id")
        if not animal_id:
            await update.effective_message.reply_text("⚠️ Edit flow lost the animal id. Cancelled.")
            context.user_data.pop("flow", None)
            return
        text = (update.effective_message.text or "").strip()
        try:
            if step == "name":
                new_name = None if text == "-" else text
                context.user_data["animal_edit_name"] = new_name
                context.user_data["animal_edit_step"] = "breed_sex"
                await update.effective_message.reply_text("Send new breed and sex separated by comma (e.g. `Jersey, female`) or `-` to keep:", parse_mode="Markdown")
                return
            if step == "breed_sex":
                breed = None
                sex = None
                if text != "-":
                    parts = [p.strip() for p in text.split(",")]
                    if parts:
                        breed = parts[0] or None
                    if len(parts) > 1 and parts[1]:
                        s = parts[1].lower()
                        sex = s if s in ("female", "male", "unknown") else "unknown"
                context.user_data["animal_edit_breed"] = breed
                context.user_data["animal_edit_sex"] = sex
                context.user_data["animal_edit_step"] = "birth_date"
                await update.effective_message.reply_text("Send new birth date YYYY-MM-DD or `-` to keep:", parse_mode="Markdown")
                return
            if step == "birth_date":
                birth_date = None
                if text != "-":
                    try:
                        datetime.datetime.strptime(text, "%Y-%m-%d")
                        birth_date = text
                    except Exception:
                        await update.effective_message.reply_text("Invalid date. Use YYYY-MM-DD or `-` to keep.")
                        return
                context.user_data["animal_edit_birth_date"] = birth_date
                context.user_data["animal_edit_step"] = "weight"
                await update.effective_message.reply_text("Send new weight (number in kg) or `-` to keep:", parse_mode="Markdown")
                return
            if step == "weight":
                weight = None
                if text != "-":
                    try:
                        weight = float(text)
                    except ValueError:
                        await update.effective_message.reply_text("Invalid weight. Use number or `-` to keep.")
                        return
                context.user_data["animal_edit_weight"] = weight
                context.user_data["animal_edit_step"] = "notes"
                await update.effective_message.reply_text("Send new notes or `-` to keep:", parse_mode="Markdown")
                return
            if step == "notes":
                notes = None if text == "-" else text
                upd: Dict[str, Any] = {}
                if "animal_edit_name" in context.user_data:
                    nm = context.user_data.get("animal_edit_name")
                    if nm is not None:
                        upd["name"] = nm
                if "animal_edit_breed" in context.user_data:
                    br = context.user_data.get("animal_edit_breed")
                    if br is not None:
                        upd["breed"] = br
                if "animal_edit_sex" in context.user_data:
                    sx = context.user_data.get("animal_edit_sex")
                    if sx is not None:
                        upd["sex"] = sx
                if "animal_edit_birth_date" in context.user_data:
                    bd = context.user_data.get("animal_edit_birth_date")
                    if bd is not None:
                        upd["birth_date"] = bd
                if "animal_edit_weight" in context.user_data:
                    wt = context.user_data.get("animal_edit_weight")
                    if wt is not None:
                        upd["weight"] = wt
                meta_update = {}
                if notes is not None:
                    meta_update["notes"] = notes
                if meta_update:
                    upd["meta"] = meta_update
                if not upd:
                    await update.effective_message.reply_text("No changes provided. Cancelled.")
                    _clear_add_flow(context.user_data)
                    return
                updated = await async_update_animal(animal_id, upd)
                if not updated:
                    await update.effective_message.reply_text("❌ Failed to update animal.")
                else:
                    await update.effective_message.reply_text("✅ Animal updated.")
                page = context.user_data.get("animal_edit_return_page", 0)
                for k in list(context.user_data.keys()):
                    if k.startswith("animal_edit"):
                        context.user_data.pop(k, None)
                context.user_data.pop("flow", None)
                await router(update, context, f"animal:list:{page}")
                return
        except Exception:
            logger.exception("Error in edit flow")
            await update.effective_message.reply_text("❌ Error processing edit. Cancelled.")
            for k in list(context.user_data.keys()):
                if k.startswith("animal_edit"):
                    context.user_data.pop(k, None)
            context.user_data.pop("flow", None)
            return

# Expose handlers map
animal_handlers["router"] = router
animal_handlers["handle_text"] = handle_text
animal_handlers["menu"] = menu
'''













'''# aboutanimal.py without role
import asyncio
import logging
import datetime
from typing import Optional, List, Dict, Any
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from farmcore import (
    async_get_user_with_farm_by_telegram,
    async_list_animals,
    async_create_animal,
    async_get_animal,
    async_update_animal,
    async_delete_animal,
)

logger = logging.getLogger(__name__)
animal_handlers: Dict[str, Any] = {}

_PAGE_SIZE = 8
_ADD_PREFIX = "animal_add_"  # prefix used in context.user_data for add-flow keys

# -----------------------
# Utilities
# -----------------------
def _format_animal_full(a: dict) -> str:
    """Return a multiline detailed string for a single animal row."""
    name = a.get("name") or "—"
    tag = a.get("tag") or "—"
    breed = a.get("breed") or "—"
    sex = a.get("sex") or "—"
    stage = a.get("stage") or ( (a.get("meta") or {}).get("stage") if isinstance(a.get("meta"), dict) else "—")
    lact = a.get("lactation_stage") or ( (a.get("meta") or {}).get("lactation_stage") if isinstance(a.get("meta"), dict) else "—")
    birth = a.get("birth_date") or "—"
    status = a.get("status") or "—"
    created = a.get("created_at") or "—"
    updated = a.get("updated_at") or "—"
    notes = (a.get("meta") or {}).get("notes") if isinstance(a.get("meta"), dict) else None
    sire = a.get("sire_id") or ( (a.get("meta") or {}).get("sire_tag") if isinstance(a.get("meta"), dict) else None)
    lines = [
        f"🐄 *{name}*  — tag: `{tag}`",
        f"• Breed: {breed}   • Sex: {sex}   • Stage: {stage}   • Lactation: {lact}",
        f"• Birth date: {birth}   • Status: {status}",
        f"• Sire/father: `{sire or '—'}`",
        f"• Created: {created}   • Updated: {updated}",
        f"• Notes: {notes or '—'}",
    ]
    return "\n".join(lines)

def _clear_add_flow(user_data: dict):
    for k in list(user_data.keys()):
        if k.startswith(_ADD_PREFIX) or k in ("flow",):
            user_data.pop(k, None)

def _footer_kb(skip_label: str = "Skip", save_label: str = "Save & finish", cancel_label: str = "Cancel"):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(skip_label, callback_data="animal:add_skip"),
         InlineKeyboardButton(save_label, callback_data="animal:add_save")],
        [InlineKeyboardButton(cancel_label, callback_data="animal:add_cancel")]
    ])

def _make_yesno_row(values: List[str], cb_prefix: str):
    rows = []
    for i in range(0, len(values), 2):
        pair = values[i:i+2]
        row = [InlineKeyboardButton(v.capitalize(), callback_data=f"{cb_prefix}:{v}") for v in pair]
        rows.append(row)
    return rows

# -----------------------
# Menu
# -----------------------
async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Animal", callback_data="animal:add")],
        [InlineKeyboardButton("📋 List Animals", callback_data="animal:list:0")],
        [InlineKeyboardButton("🔙 Back", callback_data="skip")]
    ])
    try:
        if update.callback_query:
            await update.callback_query.edit_message_text("🐮 Animals — choose an action:", reply_markup=kb)
        else:
            await update.message.reply_text("🐮 Animals — choose an action:", reply_markup=kb)
    except Exception:
        logger.exception("Failed to show animals menu")

animal_handlers["menu"] = menu

# -----------------------
# Router
# -----------------------
async def router(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str):
    """
    Actions:
      - add
      - list:<page>
      - view:<animal_id>:<page>
      - edit:<animal_id>:<page>
      - confirm_delete:<animal_id>:<page>
      - delete:<animal_id>:<page>
      - add_gender:<val>
      - add_stage:<val>
      - add_lact:<val>
      - add_skip
      - add_save
      - add_cancel
    """
    try:
        parts = action.split(":")
        cmd = parts[0] if parts else ""
        arg = parts[1] if len(parts) > 1 else ""
        arg2 = parts[2] if len(parts) > 2 else None

        # ---------- Start add flow ----------
        if cmd == "add":
            context.user_data["flow"] = "animal_add"
            context.user_data[_ADD_PREFIX + "data"] = {}
            context.user_data[_ADD_PREFIX + "step"] = "tag"
            kb = _footer_kb()
            msg = (
                "➕ Adding a new animal — send the *tag* (unique id) or send `-` to skip.\n\n"
                "You can press *Save & finish* at any time to persist what's filled so far."
            )
            if update.callback_query:
                await update.callback_query.edit_message_text(msg, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=kb)
            return

        # ---------- List animals (detailed) ----------
        if cmd == "list":
            page = int(arg) if arg and arg.isdigit() else 0
            combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
            if not combined or not combined.get("farm"):
                text = "⚠️ Farm not found. Please register a farm first with /start."
                if update.callback_query:
                    await update.callback_query.edit_message_text(text)
                else:
                    await update.message.reply_text(text)
                return
            farm = combined["farm"]
            animals = await async_list_animals(farm["id"], limit=1000)
            total = len(animals)
            start = page * _PAGE_SIZE
            end = start + _PAGE_SIZE
            page_animals = animals[start:end]

            # Build detailed text for each animal on page
            header = f"*Animals on your farm* — page {page+1} / {max(1, (total + _PAGE_SIZE -1)//_PAGE_SIZE)}\n\n"
            if page_animals:
                parts = []
                for idx, a in enumerate(page_animals, start=1):
                    parts.append(f"*{idx + start}.* " + _format_animal_full(a))
                    parts.append("")  # blank line
                body = "\n".join(parts).rstrip()
            else:
                body = "No animals on this page."

            text = header + body

            # Build keyboard: one row per animal with actions (view/edit/delete)
            kb_rows = []
            for a in page_animals:
                display = f"{a.get('name') or a.get('tag') or 'Unnamed'}"
                # Keep callback_data short but include id & page
                kb_rows.append([
                    InlineKeyboardButton(f"View {display}", callback_data=f"animal:view:{a.get('id')}:{page}"),
                    InlineKeyboardButton("Edit", callback_data=f"animal:edit:{a.get('id')}:{page}"),
                    InlineKeyboardButton("Delete", callback_data=f"animal:confirm_delete:{a.get('id')}:{page}")
                ])

            # Paging buttons
            nav_row = []
            if start > 0:
                nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"animal:list:{page-1}"))
            if end < total:
                nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"animal:list:{page+1}"))
            if nav_row:
                kb_rows.append(nav_row)

            kb_rows.append([InlineKeyboardButton("➕ Add new", callback_data="animal:add"), InlineKeyboardButton("🔙 Back", callback_data="skip")])
            kb = InlineKeyboardMarkup(kb_rows)

            # Edit message if callback, else send new message
            if update.callback_query:
                # sometimes editing long messages fails; catch and fallback to sending a new message
                try:
                    await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
                except Exception:
                    logger.warning("edit_message_text failed for long animal list; sending new message instead")
                    await update.callback_query.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
            return

        # ---------- View single animal ----------
        if cmd == "view" and arg:
            animal_id = arg
            page = int(arg2) if arg2 and arg2.isdigit() else 0
            a = await async_get_animal(animal_id)
            if not a:
                txt = "⚠️ Animal not found."
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt)
                else:
                    await update.message.reply_text(txt)
                return
            text = (
                f"🐄 *{a.get('name') or 'Unnamed'}*\n\n"
                f"Tag: `{a.get('tag')}`\n"
                f"Breed: {a.get('breed') or '—'}\n"
                f"Sex: {a.get('sex') or '—'}\n"
                f"Stage: {a.get('stage') or ( (a.get('meta') or {}).get('stage') if isinstance(a.get('meta'), dict) else '—')}\n"
                f"Lactation: {a.get('lactation_stage') or ( (a.get('meta') or {}).get('lactation_stage') if isinstance(a.get('meta'), dict) else '—')}\n"
                f"Status: {a.get('status') or '—'}\n"
                f"Birth date: {a.get('birth_date') or '—'}\n"
                f"Created: {a.get('created_at') or '—'}\n"
                f"Updated: {a.get('updated_at') or '—'}\n"
                f"Notes: { (a.get('meta') or {}).get('notes') if isinstance(a.get('meta'), dict) else '—' }\n"
            )
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ Edit", callback_data=f"animal:edit:{animal_id}:{page}"),
                 InlineKeyboardButton("🗑 Delete", callback_data=f"animal:confirm_delete:{animal_id}:{page}")],
                [InlineKeyboardButton("🔙 Back to list", callback_data=f"animal:list:{page}")]
            ])
            if update.callback_query:
                await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
            return

        # ---------- Confirm & Delete ----------
        if cmd == "confirm_delete" and arg:
            animal_id = arg
            page = int(arg2) if arg2 and arg2.isdigit() else 0
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Yes, delete", callback_data=f"animal:delete:{animal_id}:{page}"),
                 InlineKeyboardButton("No, cancel", callback_data=f"animal:view:{animal_id}:{page}")]
            ])
            if update.callback_query:
                await update.callback_query.edit_message_text("⚠️ Are you sure you want to *permanently delete* this animal?", parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text("⚠️ Are you sure you want to *permanently delete* this animal?", parse_mode="Markdown", reply_markup=kb)
            return

        if cmd == "delete" and arg:
            animal_id = arg
            page = int(arg2) if arg2 and arg2.isdigit() else 0
            ok = await async_delete_animal(animal_id)
            if ok:
                txt = "✅ Animal deleted."
            else:
                txt = "❌ Failed to delete animal."
            if update.callback_query:
                await update.callback_query.edit_message_text(txt)
            else:
                await update.message.reply_text(txt)
            # refresh list page
            await router(update, context, f"animal:list:{page}")
            return

        # ---------- Edit flow (simple) ----------
        if cmd == "edit" and arg:
            animal_id = arg
            page = int(arg2) if arg2 and arg2.isdigit() else 0
            a = await async_get_animal(animal_id)
            if not a:
                txt = "⚠️ Animal not found."
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt)
                else:
                    await update.message.reply_text(txt)
                return
            context.user_data["flow"] = "animal_edit"
            context.user_data["animal_edit_step"] = "name"
            context.user_data["animal_edit_id"] = animal_id
            context.user_data["animal_edit_return_page"] = page
            if update.callback_query:
                await update.callback_query.edit_message_text(f"Editing *{a.get('name') or a.get('tag')}* — send new *name* or `-` to keep:", parse_mode="Markdown")
            else:
                await update.message.reply_text(f"Editing *{a.get('name') or a.get('tag')}* — send new *name* or `-` to keep:", parse_mode="Markdown")
            return

        # ---------- Add flow inline choices: gender / stage / lactation ----------
        if cmd == "add_gender" and arg:
            g = arg.lower()
            data = context.user_data.get(_ADD_PREFIX + "data", {})
            data["sex"] = "female" if g in ("female", "f") else ("male" if g in ("male", "m") else "unknown")
            context.user_data[_ADD_PREFIX + "data"] = data
            context.user_data[_ADD_PREFIX + "step"] = "stage"
            if data["sex"] == "female":
                choices = ["calf", "heifer", "cow", "unknown"]
            else:
                choices = ["calf", "bull", "steer", "unknown"]
            kb_rows = _make_yesno_row(choices, "animal:add_stage")
            kb_rows.append([InlineKeyboardButton("Skip", callback_data="animal:add_skip"),
                            InlineKeyboardButton("Save & finish", callback_data="animal:add_save")])
            kb_rows.append([InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")])
            kb = InlineKeyboardMarkup(kb_rows)
            txt = f"Selected gender: *{data['sex']}* — now choose stage:"
            if update.callback_query:
                await update.callback_query.edit_message_text(txt, parse_mode="Markdown", reply_markup=kb)
            else:
                await update.message.reply_text(txt, parse_mode="Markdown", reply_markup=kb)
            return

        if cmd == "add_stage" and arg:
            st = arg.lower()
            data = context.user_data.get(_ADD_PREFIX + "data", {})
            data["stage"] = st
            context.user_data[_ADD_PREFIX + "data"] = data
            if data.get("sex") == "female" and st == "cow":
                context.user_data[_ADD_PREFIX + "step"] = "lact"
                choices = ["1", "2", "3", "dry", "unknown"]
                kb_rows = _make_yesno_row(choices, "animal:add_lact")
                kb_rows.append([InlineKeyboardButton("Skip", callback_data="animal:add_skip"),
                                InlineKeyboardButton("Save & finish", callback_data="animal:add_save")])
                kb_rows.append([InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")])
                kb = InlineKeyboardMarkup(kb_rows)
                txt = "This is a cow — choose lactation phase (1 / 2 / 3 / dry) or skip:"
                if update.callback_query:
                    await update.callback_query.edit_message_text(txt, reply_markup=kb)
                else:
                    await update.message.reply_text(txt, reply_markup=kb)
                return
            context.user_data[_ADD_PREFIX + "step"] = "birth_date"
            kb = _footer_kb()
            txt = "Send birth date YYYY-MM-DD (or `-` to skip)."
            if update.callback_query:
                await update.callback_query.edit_message_text(txt, reply_markup=kb)
            else:
                await update.message.reply_text(txt, reply_markup=kb)
            return

        if cmd == "add_lact" and arg:
            val = arg.lower()
            data = context.user_data.get(_ADD_PREFIX + "data", {})
            data["lactation_stage"] = val
            context.user_data[_ADD_PREFIX + "data"] = data
            context.user_data[_ADD_PREFIX + "step"] = "birth_date"
            kb = _footer_kb()
            txt = "Send birth date YYYY-MM-DD (or `-` to skip)."
            if update.callback_query:
                await update.callback_query.edit_message_text(txt, reply_markup=kb)
            else:
                await update.message.reply_text(txt, reply_markup=kb)
            return

        # ---------- Skip / Save / Cancel ----------
        if cmd == "add_skip":
            step = context.user_data.get(_ADD_PREFIX + "step", "tag")
            order = ["tag", "name", "gender", "stage", "lact", "birth_date", "breed", "sire", "notes"]
            try:
                idx = order.index(step)
            except ValueError:
                idx = 0
            next_idx = idx + 1
            if next_idx >= len(order):
                await _save_current_animal(update, context)
                return
            next_step = order[next_idx]
            context.user_data[_ADD_PREFIX + "step"] = next_step
            await _prompt_for_step(update, context, next_step)
            return

        if cmd == "add_save":
            await _save_current_animal(update, context)
            return

        if cmd == "add_cancel":
            _clear_add_flow(context.user_data)
            if update.callback_query:
                await update.callback_query.edit_message_text("❌ Animal registration cancelled.")
            else:
                await update.message.reply_text("❌ Animal registration cancelled.")
            return

        # Unknown action
        if update.callback_query:
            await update.callback_query.answer("Action not recognized.")
        else:
            await update.message.reply_text("Action not recognized.")

    except Exception:
        logger.exception("Error in animal router")
        try:
            if update.callback_query:
                await update.callback_query.edit_message_text("❌ Error handling animal action.")
            else:
                await update.message.reply_text("❌ Error handling animal action.")
        except Exception:
            pass

animal_handlers["router"] = router

# -----------------------
# Helper: prompt text for a given step
# -----------------------
async def _prompt_for_step(update: Update, context: ContextTypes.DEFAULT_TYPE, step: str):
    if step == "tag":
        kb = _footer_kb()
        msg = "Send the *tag* (unique id) for the animal or send `-` to skip."
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, parse_mode="Markdown", reply_markup=kb)
        else:
            await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=kb)
        return
    if step == "name":
        kb = _footer_kb()
        msg = "Send the *name* of the animal or `-` to skip."
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, parse_mode="Markdown", reply_markup=kb)
        else:
            await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=kb)
        return
    if step == "gender":
        kb_rows = [
            [InlineKeyboardButton("Female", callback_data="animal:add_gender:f"),
             InlineKeyboardButton("Male", callback_data="animal:add_gender:m")],
            [InlineKeyboardButton("Unknown", callback_data="animal:add_gender:unknown")],
            [InlineKeyboardButton("Skip", callback_data="animal:add_skip"),
             InlineKeyboardButton("Save & finish", callback_data="animal:add_save")],
            [InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")]
        ]
        kb = InlineKeyboardMarkup(kb_rows)
        msg = "Choose the *gender* of the animal:"
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, parse_mode="Markdown", reply_markup=kb)
        else:
            await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=kb)
        return
    if step == "stage":
        data = context.user_data.get(_ADD_PREFIX + "data", {})
        if data.get("sex") == "female":
            choices = ["Calf", "Heifer", "Cow", "Unknown"]
        else:
            choices = ["Calf", "Bull", "Steer", "Unknown"]
        kb_rows = _make_yesno_row([c.lower() for c in choices], "animal:add_stage")
        kb_rows.append([InlineKeyboardButton("Skip", callback_data="animal:add_skip"),
                        InlineKeyboardButton("Save & finish", callback_data="animal:add_save")])
        kb_rows.append([InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")])
        kb = InlineKeyboardMarkup(kb_rows)
        msg = "Choose stage:"
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return
    if step == "lact":
        kb_rows = _make_yesno_row(["1", "2", "3", "dry", "unknown"], "animal:add_lact")
        kb_rows.append([InlineKeyboardButton("Skip", callback_data="animal:add_skip"),
                        InlineKeyboardButton("Save & finish", callback_data="animal:add_save")])
        kb_rows.append([InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")])
        kb = InlineKeyboardMarkup(kb_rows)
        msg = "Choose lactation phase (1 / 2 / 3 / dry) or skip:"
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return
    if step == "birth_date":
        kb = _footer_kb()
        msg = "Send birth date YYYY-MM-DD or send `-` to skip."
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return
    if step == "breed":
        kb = _footer_kb()
        msg = "Send the breed (e.g. Jersey) or `-` to skip."
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return
    if step == "sire":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Skip", callback_data="animal:add_skip"),
             InlineKeyboardButton("Save & finish", callback_data="animal:add_save")],
            [InlineKeyboardButton("Cancel", callback_data="animal:add_cancel")]
        ])
        msg = "Send sire (father) tag to link this animal or `-` to skip."
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return
    if step == "notes":
        kb = _footer_kb()
        msg = "Send optional notes (or `-` to skip). After this the animal will be saved."
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=kb)
        else:
            await update.message.reply_text(msg, reply_markup=kb)
        return

# -----------------------
# Save helper
# -----------------------
async def _save_current_animal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get(_ADD_PREFIX + "data", {})
    combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
    if not combined or not combined.get("farm"):
        _clear_add_flow(context.user_data)
        if update.callback_query:
            await update.callback_query.edit_message_text("⚠️ Farm not found. Please register a farm first with /start.")
        else:
            await update.message.reply_text("⚠️ Farm not found. Please register a farm first with /start.")
        return
    farm_id = combined["farm"]["id"]

    tag = data.get("tag") or ""
    name = data.get("name")
    breed = data.get("breed")
    sex = data.get("sex", "female")
    birth_date = data.get("birth_date")
    meta = {}
    if data.get("lactation_stage"):
        meta["lactation_stage"] = data.get("lactation_stage")
    if data.get("notes"):
        meta["notes"] = data.get("notes")
    if data.get("sire_tag"):
        meta["sire_tag"] = data.get("sire_tag")
    if data.get("stage"):
        meta["stage"] = data.get("stage")

    created = await async_create_animal(farm_id=farm_id, tag=tag, name=name, breed=breed, sex=sex, birth_date=birth_date, meta=meta)
    if not created:
        txt = "❌ Failed to create animal (maybe tag duplicate)."
    else:
        display = name or tag or created.get("id")
        txt = f"✅ Animal saved: *{display}*"
    _clear_add_flow(context.user_data)
    try:
        if update.callback_query:
            await update.callback_query.edit_message_text(txt, parse_mode="Markdown")
        else:
            await update.message.reply_text(txt, parse_mode="Markdown")
    except Exception:
        try:
            await update.effective_message.reply_text(txt, parse_mode="Markdown")
        except Exception:
            pass

# -----------------------
# Text handler for typed steps
# -----------------------
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Add flow typed states
    if context.user_data.get("flow") == "animal_add":
        step = context.user_data.get(_ADD_PREFIX + "step", "tag")
        text = (update.effective_message.text or "").strip()
        if text.lower() in ("/cancel", "cancel"):
            _clear_add_flow(context.user_data)
            await update.effective_message.reply_text("❌ Animal registration cancelled.")
            return
        if text == "-":
            await router(update, context, "animal:add_skip")
            return
        try:
            data = context.user_data.get(_ADD_PREFIX + "data", {})
            if step == "tag":
                data["tag"] = text
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "name"
                await _prompt_for_step(update, context, "name")
                return
            if step == "name":
                data["name"] = text
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "gender"
                await _prompt_for_step(update, context, "gender")
                return
            if step == "birth_date":
                try:
                    datetime.datetime.strptime(text, "%Y-%m-%d")
                    data["birth_date"] = text
                except Exception:
                    await update.effective_message.reply_text("Invalid date format. Use YYYY-MM-DD or send `-` to skip.")
                    return
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "breed"
                await _prompt_for_step(update, context, "breed")
                return
            if step == "breed":
                data["breed"] = text
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "sire"
                await _prompt_for_step(update, context, "sire")
                return
            if step == "sire":
                data["sire_tag"] = text
                context.user_data[_ADD_PREFIX + "data"] = data
                context.user_data[_ADD_PREFIX + "step"] = "notes"
                await _prompt_for_step(update, context, "notes")
                return
            if step == "notes":
                data["notes"] = text
                context.user_data[_ADD_PREFIX + "data"] = data
                await _save_current_animal(update, context)
                return
        except Exception:
            logger.exception("Error handling add flow text")
            await update.effective_message.reply_text("❌ Error processing input — registration cancelled.")
            _clear_add_flow(context.user_data)
            return

    # Edit flow
    if context.user_data.get("flow") == "animal_edit":
        step = context.user_data.get("animal_edit_step")
        animal_id = context.user_data.get("animal_edit_id")
        if not animal_id:
            await update.effective_message.reply_text("⚠️ Edit flow lost the animal id. Cancelled.")
            context.user_data.pop("flow", None)
            return
        text = (update.effective_message.text or "").strip()
        try:
            if step == "name":
                new_name = None if text == "-" else text
                context.user_data["animal_edit_name"] = new_name
                context.user_data["animal_edit_step"] = "breed_sex"
                await update.effective_message.reply_text("Send new breed and sex separated by comma (e.g. `Jersey, female`) or `-` to keep:", parse_mode="Markdown")
                return
            if step == "breed_sex":
                breed = None
                sex = None
                if text != "-":
                    parts = [p.strip() for p in text.split(",")]
                    if parts:
                        breed = parts[0] or None
                    if len(parts) > 1 and parts[1]:
                        s = parts[1].lower()
                        sex = s if s in ("female", "male", "unknown") else "unknown"
                context.user_data["animal_edit_breed"] = breed
                context.user_data["animal_edit_sex"] = sex
                context.user_data["animal_edit_step"] = "birth_date"
                await update.effective_message.reply_text("Send new birth date YYYY-MM-DD or `-` to keep:", parse_mode="Markdown")
                return
            if step == "birth_date":
                birth_date = None
                if text != "-":
                    try:
                        datetime.datetime.strptime(text, "%Y-%m-%d")
                        birth_date = text
                    except Exception:
                        await update.effective_message.reply_text("Invalid date. Use YYYY-MM-DD or `-` to keep.")
                        return
                context.user_data["animal_edit_birth_date"] = birth_date
                context.user_data["animal_edit_step"] = "notes"
                await update.effective_message.reply_text("Send new notes or `-` to keep:", parse_mode="Markdown")
                return
            if step == "notes":
                notes = None if text == "-" else text
                upd: Dict[str, Any] = {}
                if "animal_edit_name" in context.user_data:
                    nm = context.user_data.get("animal_edit_name")
                    if nm is not None:
                        upd["name"] = nm
                if "animal_edit_breed" in context.user_data:
                    br = context.user_data.get("animal_edit_breed")
                    if br is not None:
                        upd["breed"] = br
                if "animal_edit_sex" in context.user_data:
                    sx = context.user_data.get("animal_edit_sex")
                    if sx is not None:
                        upd["sex"] = sx
                if "animal_edit_birth_date" in context.user_data:
                    bd = context.user_data.get("animal_edit_birth_date")
                    if bd is not None:
                        upd["birth_date"] = bd
                meta_update = {}
                if notes is not None:
                    meta_update["notes"] = notes
                if meta_update:
                    upd["meta"] = meta_update
                if not upd:
                    await update.effective_message.reply_text("No changes provided. Cancelled.")
                    _clear_add_flow(context.user_data)
                    return
                updated = await async_update_animal(animal_id, upd)
                if not updated:
                    await update.effective_message.reply_text("❌ Failed to update animal.")
                else:
                    await update.effective_message.reply_text("✅ Animal updated.")
                page = context.user_data.get("animal_edit_return_page", 0)
                for k in list(context.user_data.keys()):
                    if k.startswith("animal_edit"):
                        context.user_data.pop(k, None)
                context.user_data.pop("flow", None)
                await router(update, context, f"animal:list:{page}")
                return
        except Exception:
            logger.exception("Error in edit flow")
            await update.effective_message.reply_text("❌ Error processing edit. Cancelled.")
            for k in list(context.user_data.keys()):
                if k.startswith("animal_edit"):
                    context.user_data.pop(k, None)
            context.user_data.pop("flow", None)
            return

# Expose handlers map
animal_handlers["router"] = router
animal_handlers["handle_text"] = handle_text
animal_handlers["menu"] = menu
'''