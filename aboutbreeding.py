import logging
import datetime
from typing import Dict, Any, Optional, List
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from farmcore import (
    async_get_user_with_farm_by_telegram,
    async_list_animals,
    async_get_user_by_telegram,
    async_create_breeding_event,
    async_get_breeding_summary,
    async_compute_current_phase,
)

logger = logging.getLogger(__name__)
breeding_handlers: Dict[str, Any] = {}

# menu definitions: label -> canonical event type (must match DB enum)
MENU = [
    ("🧪 Insemination (AI)", "insemination"),
    ("💕 Mating", "mating"),
    ("🤰 Pregnancy check", "pregnancy_check"),
    ("🐄 Calving", "calving"),
    ("⚠️ Miscarriage", "miscarriage"),
    ("❌ Abortion", "abortion"),
    ("🔁 Other", "other"),
]

# events that should only target female animals
FEMALE_ONLY_EVENTS = {"insemination", "mating", "pregnancy_check", "calving", "miscarriage", "abortion"}

_PAGE_SIZE = 8

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
    if not combined or not combined.get("farm"):
        await (update.callback_query.edit_message_text if update.callback_query else update.message.reply_text)("⚠️ Farm not found.")
        return
    farm_id = combined["farm"]["id"]
    
    # NEW: Get and format summary
    summary = await async_get_breeding_summary(farm_id)
    summary_text = "Breeding Summary:\n"
    summary_text += f"🤰 Pregnant: {summary.get('pregnant', 0)}\n"
    summary_text += f"🛌 Dry Off: {summary.get('dry_off', 0)}\n"
    summary_text += f"🍼 Lactating: {summary.get('lactating', 0)}\n"
    summary_text += f"🔄 Estrus: {summary.get('estrus', 0)}\n"
    summary_text += f"🚫 Immature: {summary.get('immature', 0)}\n"
    summary_text += f"📅 Inseminated: {summary.get('inseminated', 0)}\n"
    summary_text += f"🩹 Postpartum: {summary.get('postpartum', 0)}\n"
    summary_text += f"⚠️ Aborted: {summary.get('aborted', 0)}\n"
    summary_text += f"❓ Unknown: {summary.get('unknown', 0)}\n\n"
    
    text = summary_text + "Breeding — choose event type:"
    
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton(lbl, callback_data=f"breeding:start:{val}")] for lbl, val in MENU]
        + [[InlineKeyboardButton("🔙 Back", callback_data="skip")]]
    )
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=kb)
    else:
        await update.message.reply_text(text, reply_markup=kb)

breeding_handlers["menu"] = menu

# -------------------------
# Render a page of animals (targets). We filter for female-only events.
# Store filtered list in context under 'breeding_animals_filtered'.
# Also store the complete list as 'breeding_animals_all' for generating sire candidates later.
# -------------------------
async def _render_animals_page(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    farm_id: str,
    event_type: str,
    date_str: Optional[str],
    page: int = 0,
):
    animals_all = await async_list_animals(farm_id=farm_id, limit=1000)
    context.user_data["breeding_animals_all"] = animals_all or []

    # Filter target animals depending on event_type
    if event_type in FEMALE_ONLY_EVENTS:
        # treat explicit male as male; any non-'male' records are considered eligible (including unknown)
        filtered = [a for a in animals_all if str(a.get("sex") or "").lower() != "male"]
    else:
        filtered = animals_all[:]  # all animals
    
    # NEW: Additional phase-based filter for insemination/mating
    if event_type in ("insemination", "mating"):
        eligible = []
        for a in filtered:
            phase = await async_compute_current_phase(a["id"], farm_id)
            if phase in ("estrus", "postpartum"):  # Eligible for breeding; adjust as needed
                eligible.append(a)
        filtered = eligible

    context.user_data["breeding_animals_filtered"] = filtered

    total = len(filtered)
    if total == 0:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="skip")]])
        text = "No suitable animals found on your farm for this event. Add animals first or choose another event."
        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=kb)
        else:
            await update.message.reply_text(text, reply_markup=kb)
        return

    start = page * _PAGE_SIZE
    end = start + _PAGE_SIZE
    page_animals = filtered[start:end]

    kb_rows = []
    for i, a in enumerate(page_animals):
        idx = start + i  # index into breeding_animals_filtered
        name = (a.get("name") or a.get("tag") or "Unnamed")
        tag = a.get("tag") or ""
        sex = (a.get("sex") or "unknown")
        stage = (a.get("stage") or "")
        label = f"{name} ({tag}) — {sex}{' / ' + stage if stage else ''}"
        safe_date = date_str or "-"
        cb = f"breeding:select_idx:{idx}:{event_type}:{safe_date}:{page}"
        kb_rows.append([InlineKeyboardButton(label, callback_data=cb)])

    # navigation
    nav = []
    if start > 0:
        nav.append(
            InlineKeyboardButton(
                "⬅️ Prev",
                callback_data=f"breeding:animals_page:{event_type}:{date_str or '-'}:{page-1}",
            )
        )
    if end < total:
        nav.append(
            InlineKeyboardButton(
                "Next ➡️",
                callback_data=f"breeding:animals_page:{event_type}:{date_str or '-'}:{page+1}",
            )
        )
    if nav:
        kb_rows.append(nav)

    kb_rows.append([InlineKeyboardButton("🔙 Cancel", callback_data="skip")])
    kb = InlineKeyboardMarkup(kb_rows)

    human_date = date_str or "—"
    text = f"Select animal for *{event_type}* on {human_date}  — page {page+1}/{max(1, (total + _PAGE_SIZE - 1)//_PAGE_SIZE)}"
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
    else:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)

# -------------------------
# Router for callback actions
# -------------------------
async def router(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str):
    try:
        parts = action.split(":")
        cmd = parts[0] if parts else ""

        # Start flow: choose date (today / pick)
        if cmd == "start":
            event_type = parts[1] if len(parts) > 1 else None
            if not event_type:
                await (update.callback_query.edit_message_text if update.callback_query else update.message.reply_text)(
                    "Invalid event type."
                )
                return
            context.user_data["flow"] = "breeding_add"
            context.user_data["breeding_event_type"] = event_type
            kb = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "📅 Today",
                            callback_data=f"breeding:pick_date:{event_type}:{datetime.date.today().isoformat()}",
                        )
                    ],
                    [InlineKeyboardButton("🖊 Pick date (type)", callback_data=f"breeding:pick_date_type:{event_type}")],
                    [InlineKeyboardButton("🔙 Cancel", callback_data="skip")],
                ]
            )
            await (update.callback_query.edit_message_text if update.callback_query else update.message.reply_text)(
                "Choose date for the event:", reply_markup=kb
            )
            return

        # pick_date: event_type : date_iso
        if cmd == "pick_date" and len(parts) >= 3:
            event_type = parts[1]
            date_str = parts[2]
            combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
            if not combined or not combined.get("farm"):
                await (update.callback_query.edit_message_text if update.callback_query else update.message.reply_text)("⚠️ Farm not found.")
                return
            farm_id = combined["farm"]["id"]
            await _render_animals_page(update, context, farm_id, event_type, date_str, page=0)
            return

        # user chose "type a date" path
        if cmd == "pick_date_type" and len(parts) >= 2:
            event_type = parts[1]
            context.user_data["flow"] = "breeding_add"
            context.user_data["breeding_event_type"] = event_type
            context.user_data["breeding_waiting_date"] = True
            await (update.callback_query.edit_message_text if update.callback_query else update.message.reply_text)(
                "Send date in YYYY-MM-DD format (or /cancel):"
            )
            return

        # paginate animals: breeding:animals_page:<event_type>:<date_str_or_->:<page>
        if cmd == "animals_page" and len(parts) >= 4:
            event_type = parts[1]
            date_str = parts[2] if parts[2] != "-" else None
            page = int(parts[3]) if parts[3].isdigit() else 0
            combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
            if not combined or not combined.get("farm"):
                await (update.callback_query.edit_message_text if update.callback_query else update.message.reply_text)("⚠️ Farm not found.")
                return
            farm_id = combined["farm"]["id"]
            await _render_animals_page(update, context, farm_id, event_type, date_str, page=page)
            return

        # selection by index: breeding:select_idx:<animal_idx>:<event_type>:<date_str_or_->:<page>
        if cmd == "select_idx" and len(parts) >= 5:
            animal_idx = int(parts[1])
            event_type = parts[2]
            date_str = parts[3] if parts[3] != "-" else None
            page = int(parts[4]) if parts[4].isdigit() else 0

            animals: List[Dict[str, Any]] = context.user_data.get("breeding_animals_filtered") or []
            if animal_idx < 0 or animal_idx >= len(animals):
                await update.callback_query.edit_message_text("⚠️ Invalid animal selection or list expired. Please start again.")
                return
            animal = animals[animal_idx]
            animal_id = animal.get("id")

            # Build sire candidates (only males, excluding the selected animal)
            animals_all: List[Dict[str, Any]] = context.user_data.get("breeding_animals_all") or []
            sire_candidates = [a for a in animals_all if str(a.get("sex") or "").lower() == "male" and a.get("id") != animal_id]
            context.user_data["breeding_sire_candidates"] = sire_candidates

            # Ask to choose sire or skip
            kb = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🔎 Choose sire from herd", callback_data=f"breeding:choose_sire:{animal_idx}:{event_type}:{date_str or '-'}:{0}")],
                    [InlineKeyboardButton("➕ Skip sire", callback_data=f"breeding:confirm:{animal_idx}:{event_type}:{date_str or '-'}:{page}:none")],
                    [InlineKeyboardButton("🔙 Back", callback_data=f"breeding:animals_page:{event_type}:{date_str or '-'}:{page}")],
                ]
            )
            text = f"Selected *{(animal.get('name') or animal.get('tag') or animal_id)}* for *{event_type}* on {date_str or '—'}.\nChoose a sire or skip."
            await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
            return

        # choose_sire: breeding:choose_sire:<animal_idx>:<event_type>:<date_str_or_->:<page>
        if cmd == "choose_sire" and len(parts) >= 5:
            animal_idx = int(parts[1])
            event_type = parts[2]
            date_str = parts[3] if parts[3] != "-" else None
            page = int(parts[4]) if parts[4].isdigit() else 0

            sire_candidates: List[Dict[str, Any]] = context.user_data.get("breeding_sire_candidates") or []
            total = len(sire_candidates)
            if total == 0:
                # no male candidates
                kb = InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("➕ Skip sire", callback_data=f"breeding:confirm:{animal_idx}:{event_type}:{date_str or '-'}:{page}:none")],
                        [InlineKeyboardButton("🔙 Back", callback_data=f"breeding:select_idx:{animal_idx}:{event_type}:{date_str or '-'}:{page}")],
                    ]
                )
                await update.callback_query.edit_message_text("No male animals found on the farm to use as sire. You can skip sire.", reply_markup=kb)
                return

            # paginate sire candidates; `page` here is reuse for paging the sire list
            start = page * _PAGE_SIZE
            end = start + _PAGE_SIZE
            page_animals = sire_candidates[start:end]

            kb_rows = []
            for i, a in enumerate(page_animals):
                idx = start + i
                label = f"{a.get('name') or a.get('tag') or 'Unnamed'} ({a.get('tag')})"
                cb = f"breeding:pick_sire:{animal_idx}:{idx}:{event_type}:{date_str or '-'}:{page}"
                kb_rows.append([InlineKeyboardButton(label, callback_data=cb)])

            nav = []
            if start > 0:
                nav.append(
                    InlineKeyboardButton(
                        "⬅️ Prev",
                        callback_data=f"breeding:choose_sire:{animal_idx}:{event_type}:{date_str or '-'}:{page-1}",
                    )
                )
            if end < total:
                nav.append(
                    InlineKeyboardButton(
                        "Next ➡️",
                        callback_data=f"breeding:choose_sire:{animal_idx}:{event_type}:{date_str or '-'}:{page+1}",
                    )
                )
            if nav:
                kb_rows.append(nav)

            kb_rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"breeding:select_idx:{animal_idx}:{event_type}:{date_str or '-'}:{page}")])
            await update.callback_query.edit_message_text("Choose sire from list:", reply_markup=InlineKeyboardMarkup(kb_rows))
            return

        # pick_sire: breeding:pick_sire:<animal_idx>:<sire_idx>:<event_type>:<date_str_or_->:<page>
        if cmd == "pick_sire" and len(parts) >= 6:
            animal_idx = int(parts[1])
            sire_idx = int(parts[2])
            event_type = parts[3]
            date_str = parts[4] if parts[4] != "-" else None
            page = int(parts[5]) if parts[5].isdigit() else 0

            sire_candidates: List[Dict[str, Any]] = context.user_data.get("breeding_sire_candidates") or []
            animals_filtered: List[Dict[str, Any]] = context.user_data.get("breeding_animals_filtered") or []

            if animal_idx < 0 or animal_idx >= len(animals_filtered) or sire_idx < 0 or sire_idx >= len(sire_candidates):
                await update.callback_query.edit_message_text("⚠️ Invalid selection (list expired). Start again.")
                return

            selected_animal = animals_filtered[animal_idx]
            sire = sire_candidates[sire_idx]

            # confirm dialog
            kb = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("✅ Confirm & Save", callback_data=f"breeding:confirm:{animal_idx}:{event_type}:{date_str or '-'}:{page}:{sire_idx}")],
                    [InlineKeyboardButton("🔙 Back", callback_data=f"breeding:choose_sire:{animal_idx}:{event_type}:{date_str or '-'}:{page}")],
                ]
            )
            await update.callback_query.edit_message_text(
                f"Confirm: animal *{(selected_animal.get('name') or selected_animal.get('tag') or selected_animal.get('id'))}* with sire *{(sire.get('name') or sire.get('tag') or sire.get('id'))}* on {date_str or '—'}",
                parse_mode="Markdown",
                reply_markup=kb,
            )
            return

        # confirm: breeding:confirm:<animal_idx>:<event_type>:<date_str_or_->:<page>:<sire_idx_or_none>
        if cmd == "confirm" and len(parts) >= 6:
            animal_idx = int(parts[1])
            event_type = parts[2]
            date_str = parts[3] if parts[3] != "-" else None
            page = int(parts[4]) if parts[4].isdigit() else 0
            sire_token = parts[5]

            animals_filtered: List[Dict[str, Any]] = context.user_data.get("breeding_animals_filtered") or []
            sire_candidates: List[Dict[str, Any]] = context.user_data.get("breeding_sire_candidates") or []

            if animal_idx < 0 or animal_idx >= len(animals_filtered):
                await update.callback_query.edit_message_text("⚠️ Animal list expired or invalid. Start again.")
                return
            animal_id = animals_filtered[animal_idx].get("id")

            sire_id = None
            if sire_token not in ("none", "-", "none"):
                try:
                    sire_idx = int(sire_token)
                    if 0 <= sire_idx < len(sire_candidates):
                        sire_id = sire_candidates[sire_idx].get("id")
                except Exception:
                    sire_id = None

            # check not same animal
            if sire_id and sire_id == animal_id:
                await update.callback_query.edit_message_text("⚠️ Selected sire is same as the animal. Aborted.")
                return

            combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
            if not combined or not combined.get("farm"):
                await update.callback_query.edit_message_text("⚠️ Farm not found.")
                return
            farm_id = combined["farm"]["id"]
            created_by_row = await async_get_user_by_telegram(update.effective_user.id)
            created_by = created_by_row.get("id") if created_by_row else None

            if not date_str or date_str == "-":
                date_str = datetime.date.today().isoformat()

            created = await async_create_breeding_event(
                farm_id=farm_id,
                animal_id=animal_id,
                event_type=event_type,
                date_val=date_str,
                sire_id=sire_id,
                details=None,
                created_by=created_by,
            )
            if not created:
                await update.callback_query.edit_message_text("❌ Failed to create breeding event. Verify event type or check logs.")
            else:
                await update.callback_query.edit_message_text(
                    f"✅ Breeding event recorded for *{(animals_filtered[animal_idx].get('name') or animals_filtered[animal_idx].get('tag') or animal_id)}* on {date_str}.",
                    parse_mode="Markdown",
                )

            # clear animals lists so subsequent flows refresh data
            context.user_data.pop("breeding_animals_all", None)
            context.user_data.pop("breeding_animals_filtered", None)
            context.user_data.pop("breeding_sire_candidates", None)
            context.user_data.pop("flow", None)
            return

        # unknown action
        await (update.callback_query.answer if update.callback_query else update.message.reply_text)("Action not recognized.")
    except Exception:
        logger.exception("Error handling breeding callback")
        try:
            if update.callback_query:
                await update.callback_query.edit_message_text("❌ Error handling breeding action. Try again or reopen the breeding menu.")
            else:
                await update.message.reply_text("❌ Error handling breeding action. Try again.")
        except Exception:
            pass

breeding_handlers["router"] = router

# -------------------------
# Text handler for "type date" or other typed flows
# -------------------------
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # If user was asked to type date
    if context.user_data.get("breeding_waiting_date"):
        text = (update.effective_message.text or "").strip()
        if text.lower() in ("/cancel", "cancel"):
            context.user_data.pop("breeding_waiting_date", None)
            context.user_data.pop("flow", None)
            await update.effective_message.reply_text("Cancelled.")
            return
        try:
            datetime.datetime.strptime(text, "%Y-%m-%d")
            date_str = text
        except Exception:
            await update.effective_message.reply_text("Invalid date format. Use YYYY-MM-DD or /cancel.")
            return
        # clear waiting flag
        context.user_data.pop("breeding_waiting_date", None)
        event_type = context.user_data.get("breeding_event_type")
        combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
        if not combined or not combined.get("farm"):
            await update.effective_message.reply_text("⚠️ Farm not found.")
            context.user_data.pop("flow", None)
            return
        farm_id = combined["farm"]["id"]
        await _render_animals_page(update, context, farm_id, event_type, date_str, page=0)
        return

    # other text in breeding context is not used
    return

breeding_handlers["handle_text"] = handle_text








'''# aboutbreeding.py
import logging
import datetime
from typing import Dict, Any, Optional, List
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from farmcore import (
    async_get_user_with_farm_by_telegram,
    async_list_animals,
    async_get_user_by_telegram,
    async_create_breeding_event,
)

logger = logging.getLogger(__name__)
breeding_handlers: Dict[str, Any] = {}

# menu definitions: label -> canonical event type (must match DB enum)
MENU = [
    ("🧪 Insemination (AI)", "insemination"),
    ("💕 Mating", "mating"),
    ("🤰 Pregnancy check", "pregnancy_check"),
    ("🐄 Calving", "calving"),
    ("⚠️ Miscarriage", "miscarriage"),
    ("❌ Abortion", "abortion"),
    ("🔁 Other", "other"),
]

# events that should only target female animals
FEMALE_ONLY_EVENTS = {"insemination", "mating", "pregnancy_check", "calving", "miscarriage", "abortion"}

_PAGE_SIZE = 8


async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton(lbl, callback_data=f"breeding:start:{val}")] for lbl, val in MENU]
        + [[InlineKeyboardButton("🔙 Back", callback_data="skip")]]
    )
    if update.callback_query:
        await update.callback_query.edit_message_text("Breeding — choose event type:", reply_markup=kb)
    else:
        await update.message.reply_text("Breeding — choose event type:", reply_markup=kb)


breeding_handlers["menu"] = menu


# -------------------------
# Render a page of animals (targets). We filter for female-only events.
# Store filtered list in context under 'breeding_animals_filtered'.
# Also store the complete list as 'breeding_animals_all' for generating sire candidates later.
# -------------------------
async def _render_animals_page(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    farm_id: str,
    event_type: str,
    date_str: Optional[str],
    page: int = 0,
):
    animals_all = await async_list_animals(farm_id=farm_id, limit=1000)
    context.user_data["breeding_animals_all"] = animals_all or []

    # Filter target animals depending on event_type
    if event_type in FEMALE_ONLY_EVENTS:
        # treat explicit male as male; any non-'male' records are considered eligible (including unknown)
        filtered = [a for a in animals_all if str(a.get("sex") or "").lower() != "male"]
    else:
        filtered = animals_all[:]  # all animals

    context.user_data["breeding_animals_filtered"] = filtered

    total = len(filtered)
    if total == 0:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="skip")]])
        text = "No suitable animals found on your farm for this event. Add animals first or choose another event."
        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=kb)
        else:
            await update.message.reply_text(text, reply_markup=kb)
        return

    start = page * _PAGE_SIZE
    end = start + _PAGE_SIZE
    page_animals = filtered[start:end]

    kb_rows = []
    for i, a in enumerate(page_animals):
        idx = start + i  # index into breeding_animals_filtered
        name = (a.get("name") or a.get("tag") or "Unnamed")
        tag = a.get("tag") or ""
        sex = (a.get("sex") or "unknown")
        stage = (a.get("stage") or "")
        label = f"{name} ({tag}) — {sex}{' / ' + stage if stage else ''}"
        safe_date = date_str or "-"
        cb = f"breeding:select_idx:{idx}:{event_type}:{safe_date}:{page}"
        kb_rows.append([InlineKeyboardButton(label, callback_data=cb)])

    # navigation
    nav = []
    if start > 0:
        nav.append(
            InlineKeyboardButton(
                "⬅️ Prev",
                callback_data=f"breeding:animals_page:{event_type}:{date_str or '-'}:{page-1}",
            )
        )
    if end < total:
        nav.append(
            InlineKeyboardButton(
                "Next ➡️",
                callback_data=f"breeding:animals_page:{event_type}:{date_str or '-'}:{page+1}",
            )
        )
    if nav:
        kb_rows.append(nav)

    kb_rows.append([InlineKeyboardButton("🔙 Cancel", callback_data="skip")])
    kb = InlineKeyboardMarkup(kb_rows)

    human_date = date_str or "—"
    text = f"Select animal for *{event_type}* on {human_date}  — page {page+1}/{max(1, (total + _PAGE_SIZE - 1)//_PAGE_SIZE)}"
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
    else:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)


# -------------------------
# Router for callback actions
# -------------------------
async def router(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str):
    try:
        parts = action.split(":")
        cmd = parts[0] if parts else ""

        # Start flow: choose date (today / pick)
        if cmd == "start":
            event_type = parts[1] if len(parts) > 1 else None
            if not event_type:
                await (update.callback_query.edit_message_text if update.callback_query else update.message.reply_text)(
                    "Invalid event type."
                )
                return
            context.user_data["flow"] = "breeding_add"
            context.user_data["breeding_event_type"] = event_type
            kb = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "📅 Today",
                            callback_data=f"breeding:pick_date:{event_type}:{datetime.date.today().isoformat()}",
                        )
                    ],
                    [InlineKeyboardButton("🖊 Pick date (type)", callback_data=f"breeding:pick_date_type:{event_type}")],
                    [InlineKeyboardButton("🔙 Cancel", callback_data="skip")],
                ]
            )
            await (update.callback_query.edit_message_text if update.callback_query else update.message.reply_text)(
                "Choose date for the event:", reply_markup=kb
            )
            return

        # pick_date: event_type : date_iso
        if cmd == "pick_date" and len(parts) >= 3:
            event_type = parts[1]
            date_str = parts[2]
            combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
            if not combined or not combined.get("farm"):
                await (update.callback_query.edit_message_text if update.callback_query else update.message.reply_text)("⚠️ Farm not found.")
                return
            farm_id = combined["farm"]["id"]
            await _render_animals_page(update, context, farm_id, event_type, date_str, page=0)
            return

        # user chose "type a date" path
        if cmd == "pick_date_type" and len(parts) >= 2:
            event_type = parts[1]
            context.user_data["flow"] = "breeding_add"
            context.user_data["breeding_event_type"] = event_type
            context.user_data["breeding_waiting_date"] = True
            await (update.callback_query.edit_message_text if update.callback_query else update.message.reply_text)(
                "Send date in YYYY-MM-DD format (or /cancel):"
            )
            return

        # paginate animals: breeding:animals_page:<event_type>:<date_str_or_->:<page>
        if cmd == "animals_page" and len(parts) >= 4:
            event_type = parts[1]
            date_str = parts[2] if parts[2] != "-" else None
            page = int(parts[3]) if parts[3].isdigit() else 0
            combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
            if not combined or not combined.get("farm"):
                await (update.callback_query.edit_message_text if update.callback_query else update.message.reply_text)("⚠️ Farm not found.")
                return
            farm_id = combined["farm"]["id"]
            await _render_animals_page(update, context, farm_id, event_type, date_str, page=page)
            return

        # selection by index: breeding:select_idx:<animal_idx>:<event_type>:<date_str_or_->:<page>
        if cmd == "select_idx" and len(parts) >= 5:
            animal_idx = int(parts[1])
            event_type = parts[2]
            date_str = parts[3] if parts[3] != "-" else None
            page = int(parts[4]) if parts[4].isdigit() else 0

            animals: List[Dict[str, Any]] = context.user_data.get("breeding_animals_filtered") or []
            if animal_idx < 0 or animal_idx >= len(animals):
                await update.callback_query.edit_message_text("⚠️ Invalid animal selection or list expired. Please start again.")
                return
            animal = animals[animal_idx]
            animal_id = animal.get("id")

            # Build sire candidates (only males, excluding the selected animal)
            animals_all: List[Dict[str, Any]] = context.user_data.get("breeding_animals_all") or []
            sire_candidates = [a for a in animals_all if str(a.get("sex") or "").lower() == "male" and a.get("id") != animal_id]
            context.user_data["breeding_sire_candidates"] = sire_candidates

            # Ask to choose sire or skip
            kb = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🔎 Choose sire from herd", callback_data=f"breeding:choose_sire:{animal_idx}:{event_type}:{date_str or '-'}:{0}")],
                    [InlineKeyboardButton("➕ Skip sire", callback_data=f"breeding:confirm:{animal_idx}:{event_type}:{date_str or '-'}:{page}:none")],
                    [InlineKeyboardButton("🔙 Back", callback_data=f"breeding:animals_page:{event_type}:{date_str or '-'}:{page}")],
                ]
            )
            text = f"Selected *{(animal.get('name') or animal.get('tag') or animal_id)}* for *{event_type}* on {date_str or '—'}.\nChoose a sire or skip."
            await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
            return

        # choose_sire: breeding:choose_sire:<animal_idx>:<event_type>:<date_str_or_->:<page>
        if cmd == "choose_sire" and len(parts) >= 5:
            animal_idx = int(parts[1])
            event_type = parts[2]
            date_str = parts[3] if parts[3] != "-" else None
            page = int(parts[4]) if parts[4].isdigit() else 0

            sire_candidates: List[Dict[str, Any]] = context.user_data.get("breeding_sire_candidates") or []
            total = len(sire_candidates)
            if total == 0:
                # no male candidates
                kb = InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("➕ Skip sire", callback_data=f"breeding:confirm:{animal_idx}:{event_type}:{date_str or '-'}:{page}:none")],
                        [InlineKeyboardButton("🔙 Back", callback_data=f"breeding:select_idx:{animal_idx}:{event_type}:{date_str or '-'}:{page}")],
                    ]
                )
                await update.callback_query.edit_message_text("No male animals found on the farm to use as sire. You can skip sire.", reply_markup=kb)
                return

            # paginate sire candidates; `page` here is reuse for paging the sire list
            start = page * _PAGE_SIZE
            end = start + _PAGE_SIZE
            page_animals = sire_candidates[start:end]

            kb_rows = []
            for i, a in enumerate(page_animals):
                idx = start + i
                label = f"{a.get('name') or a.get('tag') or 'Unnamed'} ({a.get('tag')})"
                cb = f"breeding:pick_sire:{animal_idx}:{idx}:{event_type}:{date_str or '-'}:{page}"
                kb_rows.append([InlineKeyboardButton(label, callback_data=cb)])

            nav = []
            if start > 0:
                nav.append(
                    InlineKeyboardButton(
                        "⬅️ Prev",
                        callback_data=f"breeding:choose_sire:{animal_idx}:{event_type}:{date_str or '-'}:{page-1}",
                    )
                )
            if end < total:
                nav.append(
                    InlineKeyboardButton(
                        "Next ➡️",
                        callback_data=f"breeding:choose_sire:{animal_idx}:{event_type}:{date_str or '-'}:{page+1}",
                    )
                )
            if nav:
                kb_rows.append(nav)

            kb_rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"breeding:select_idx:{animal_idx}:{event_type}:{date_str or '-'}:{page}")])
            await update.callback_query.edit_message_text("Choose sire from list:", reply_markup=InlineKeyboardMarkup(kb_rows))
            return

        # pick_sire: breeding:pick_sire:<animal_idx>:<sire_idx>:<event_type>:<date_str_or_->:<page>
        if cmd == "pick_sire" and len(parts) >= 6:
            animal_idx = int(parts[1])
            sire_idx = int(parts[2])
            event_type = parts[3]
            date_str = parts[4] if parts[4] != "-" else None
            page = int(parts[5]) if parts[5].isdigit() else 0

            sire_candidates: List[Dict[str, Any]] = context.user_data.get("breeding_sire_candidates") or []
            animals_filtered: List[Dict[str, Any]] = context.user_data.get("breeding_animals_filtered") or []

            if animal_idx < 0 or animal_idx >= len(animals_filtered) or sire_idx < 0 or sire_idx >= len(sire_candidates):
                await update.callback_query.edit_message_text("⚠️ Invalid selection (list expired). Start again.")
                return

            selected_animal = animals_filtered[animal_idx]
            sire = sire_candidates[sire_idx]

            # confirm dialog
            kb = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("✅ Confirm & Save", callback_data=f"breeding:confirm:{animal_idx}:{event_type}:{date_str or '-'}:{page}:{sire_idx}")],
                    [InlineKeyboardButton("🔙 Back", callback_data=f"breeding:choose_sire:{animal_idx}:{event_type}:{date_str or '-'}:{page}")],
                ]
            )
            await update.callback_query.edit_message_text(
                f"Confirm: animal *{(selected_animal.get('name') or selected_animal.get('tag') or selected_animal.get('id'))}* with sire *{(sire.get('name') or sire.get('tag') or sire.get('id'))}* on {date_str or '—'}",
                parse_mode="Markdown",
                reply_markup=kb,
            )
            return

        # confirm: breeding:confirm:<animal_idx>:<event_type>:<date_str_or_->:<page>:<sire_idx_or_none>
        if cmd == "confirm" and len(parts) >= 6:
            animal_idx = int(parts[1])
            event_type = parts[2]
            date_str = parts[3] if parts[3] != "-" else None
            page = int(parts[4]) if parts[4].isdigit() else 0
            sire_token = parts[5]

            animals_filtered: List[Dict[str, Any]] = context.user_data.get("breeding_animals_filtered") or []
            sire_candidates: List[Dict[str, Any]] = context.user_data.get("breeding_sire_candidates") or []

            if animal_idx < 0 or animal_idx >= len(animals_filtered):
                await update.callback_query.edit_message_text("⚠️ Animal list expired or invalid. Start again.")
                return
            animal_id = animals_filtered[animal_idx].get("id")

            sire_id = None
            if sire_token not in ("none", "-", "none"):
                try:
                    sire_idx = int(sire_token)
                    if 0 <= sire_idx < len(sire_candidates):
                        sire_id = sire_candidates[sire_idx].get("id")
                except Exception:
                    sire_id = None

            # check not same animal
            if sire_id and sire_id == animal_id:
                await update.callback_query.edit_message_text("⚠️ Selected sire is same as the animal. Aborted.")
                return

            combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
            if not combined or not combined.get("farm"):
                await update.callback_query.edit_message_text("⚠️ Farm not found.")
                return
            farm_id = combined["farm"]["id"]
            created_by_row = await async_get_user_by_telegram(update.effective_user.id)
            created_by = created_by_row.get("id") if created_by_row else None

            if not date_str or date_str == "-":
                date_str = datetime.date.today().isoformat()

            created = await async_create_breeding_event(
                farm_id=farm_id,
                animal_id=animal_id,
                event_type=event_type,
                date_val=date_str,
                sire_id=sire_id,
                details=None,
                created_by=created_by,
            )
            if not created:
                await update.callback_query.edit_message_text("❌ Failed to create breeding event. Verify event type or check logs.")
            else:
                await update.callback_query.edit_message_text(
                    f"✅ Breeding event recorded for *{(animals_filtered[animal_idx].get('name') or animals_filtered[animal_idx].get('tag') or animal_id)}* on {date_str}.",
                    parse_mode="Markdown",
                )

            # clear animals lists so subsequent flows refresh data
            context.user_data.pop("breeding_animals_all", None)
            context.user_data.pop("breeding_animals_filtered", None)
            context.user_data.pop("breeding_sire_candidates", None)
            context.user_data.pop("flow", None)
            return

        # unknown action
        await (update.callback_query.answer if update.callback_query else update.message.reply_text)("Action not recognized.")
    except Exception:
        logger.exception("Error handling breeding callback")
        try:
            if update.callback_query:
                await update.callback_query.edit_message_text("❌ Error handling breeding action. Try again or reopen the breeding menu.")
            else:
                await update.message.reply_text("❌ Error handling breeding action. Try again.")
        except Exception:
            pass


breeding_handlers["router"] = router


# -------------------------
# Text handler for "type date" or other typed flows
# -------------------------
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # If user was asked to type date
    if context.user_data.get("breeding_waiting_date"):
        text = (update.effective_message.text or "").strip()
        if text.lower() in ("/cancel", "cancel"):
            context.user_data.pop("breeding_waiting_date", None)
            context.user_data.pop("flow", None)
            await update.effective_message.reply_text("Cancelled.")
            return
        try:
            datetime.datetime.strptime(text, "%Y-%m-%d")
            date_str = text
        except Exception:
            await update.effective_message.reply_text("Invalid date format. Use YYYY-MM-DD or /cancel.")
            return
        # clear waiting flag
        context.user_data.pop("breeding_waiting_date", None)
        event_type = context.user_data.get("breeding_event_type")
        combined = await async_get_user_with_farm_by_telegram(update.effective_user.id)
        if not combined or not combined.get("farm"):
            await update.effective_message.reply_text("⚠️ Farm not found.")
            context.user_data.pop("flow", None)
            return
        farm_id = combined["farm"]["id"]
        await _render_animals_page(update, context, farm_id, event_type, date_str, page=0)
        return

    # other text in breeding context is not used
    return


breeding_handlers["handle_text"] = handle_text
'''