from __future__ import annotations
import os
import logging
import asyncio
from functools import wraps, partial
from typing import Any, Dict, List, Optional, Tuple, Callable
from datetime import date, datetime as dt, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client
import re
import time

from dateutil.relativedelta import relativedelta  # For age calculations

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("SUPABASE_URL or SUPABASE_KEY missing from environment")
    raise RuntimeError("Supabase configuration missing. Set SUPABASE_URL and SUPABASE_KEY in .env")

# Raw client for backwards compatibility
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -------------------------
# Utilities
# -------------------------
def _retry(max_attempts: int = 3, initial_delay: float = 0.5, backoff: float = 2.0, allowed_exceptions: Tuple[type, ...] = (Exception,)):
    """
    A simple retry decorator with exponential backoff for I/O operations.
    Only re-raises the last exception if attempts exhausted.
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except allowed_exceptions as exc:
                    last_exc = exc
                    logger.warning("Attempt %s/%s failed for %s: %s", attempt, max_attempts, fn.__name__, exc)
                    if attempt == max_attempts:
                        logger.exception("All retry attempts failed for %s", fn.__name__)
                        raise
                    time.sleep(delay)
                    delay *= backoff
            raise last_exc
        return wrapper
    return decorator

def _normalize_response(resp: Any) -> Tuple[Optional[Any], Optional[str]]:
    """
    Convert various supabase response shapes into (data, error) tuple.
    """
    if resp is None:
        return None, "no-response"
    if hasattr(resp, "data") or hasattr(resp, "error"):
        data = getattr(resp, "data", None)
        error = getattr(resp, "error", None)
        return data, error
    if isinstance(resp, dict):
        return resp.get("data"), resp.get("error")
    return resp, None

# -------------------------
# Thread-run helpers (async wrappers)
# -------------------------
def _run_in_thread_sync(fn: Callable, *args, **kwargs):
    """Run a blocking function synchronously (used by sync helpers)."""
    return fn(*args, **kwargs)

async def _run_in_thread(fn: Callable, *args, **kwargs):
    """
    Run blocking fn in a thread to avoid blocking the event loop.
    Returns result or raises exceptions from the thread.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(fn, *args, **kwargs))

# -------------------------
# Low-level safe execute
# -------------------------
def _safe_execute_sync(callable_fn: Callable[[], Any], single_row: bool = False) -> Dict[str, Any]:
    """
    Run a synchronous callable that returns the supabase response-like object.
    Returns dict with either {'data': ...} or {'error': '...'}.
    If single_row is True, extracts the first item from data if it's a list.
    """
    try:
        resp = _retry(max_attempts=3)(callable_fn)()
        data, error = _normalize_response(resp)
        if error:
            logger.debug("Normalized response error: %s", error)
            return {"error": error}
        if single_row and isinstance(data, list):
            data = data[0] if data else None
        return {"data": data}
    except Exception as e:
        logger.exception("Supabase sync call failed")
        return {"error": str(e)}

async def _safe_execute(callable_fn: Callable[[], Any], single_row: bool = False) -> Dict[str, Any]:
    """
    Async wrapper for safe execute for blocking supabase calls.
    If single_row is True, extracts the first item from data if it's a list.
    """
    try:
        result = await _run_in_thread(lambda: _retry(max_attempts=3)(callable_fn)())
        data, error = _normalize_response(result)
        if error:
            logger.debug("Normalized response error (async): %s", error)
            return {"error": error}
        if single_row and isinstance(data, list):
            data = data[0] if data else None
        return {"data": data}
    except Exception as e:
        logger.exception("Supabase async call failed")
        return {"error": str(e)}

# -------------------------
# Helper: safe single-row fetch (use limit(1) to avoid .single() exceptions)
# -------------------------
def _maybe_single_sync(table: str, select_cols: str = "*", eq: Optional[Tuple[str, Any]] = None):
    """
    Safer single-row fetch: use limit(1) and return either a list (handled above by single_row)
    or an error object. Call _safe_execute_sync(..., single_row=True) to extract the single row.
    """
    def _fn():
        q = supabase.table(table).select(select_cols)
        if eq:
            q = q.eq(eq[0], eq[1])
        return q.limit(1).execute()
    return _fn

def _select_sync(table: str, select_cols: str = "*", filters: Optional[List[Tuple[str, str, Any]]] = None, order_by: Optional[Tuple[str, dict]] = None, limit: Optional[int] = None, offset: Optional[int] = None):
    """
    Generic select builder for sync use.
    Updated for supabase-py 2.x.
    filters is a list of tuples: (field, operator, value)
    """
    def _fn():
        q = supabase.table(table).select(select_cols)
        if filters:
            for field, operator, val in filters:
                if operator == "eq":
                    q = q.eq(field, val)
                elif operator == "gte":
                    q = q.gte(field, val)
                elif operator == "lte":
                    q = q.lte(field, val)
                elif operator == "like":
                    q = q.like(field, val)
                elif operator == "ilike":
                    q = q.ilike(field, val)
                else:
                    logger.warning("Unsupported operator %s for field %s", operator, field)
                    raise ValueError(f"Unsupported operator: {operator}")
        if order_by:
            q = q.order(order_by[0], desc=not order_by[1].get("ascending", True))
        if limit:
            q = q.limit(limit)
        if offset:
            q = q.offset(offset)
        return q.execute()
    return _fn

# -------------------------
# Small centralized DB helpers (sync)
# -------------------------
def _db_insert_sync(table: str, payload: Dict[str, Any], returning: str = "representation") -> Dict[str, Any]:
    def _fn():
        return supabase.table(table).insert(payload, returning=returning).execute()
    return _safe_execute_sync(_fn, single_row=True)

def _db_update_sync(table: str, where_field: str, where_value: Any, payload: Dict[str, Any], returning: Optional[str] = "representation") -> Dict[str, Any]:
    """
    Update a single row (or rows) in `table` by matching where_field == where_value.
    - `returning` defaults to "representation" so callers that expect the updated row get it.
    - Returns a dict from _safe_execute_sync, and by default extracts a single row.
    """
    def _fn():
        # Use the `returning` parameter on update (PostgREST style) rather than chaining .select()
        if returning:
            return supabase.table(table).update(payload, returning=returning).eq(where_field, where_value).execute()
        return supabase.table(table).update(payload).eq(where_field, where_value).execute()
    # We expect update-by-id to return a single row (representation). Keep single_row=True for convenience.
    return _safe_execute_sync(_fn, single_row=True)

def _db_delete_sync(table: str, where_field: str, where_value: Any) -> Dict[str, Any]:
    def _fn():
        return supabase.table(table).delete().eq(where_field, where_value).execute()
    return _safe_execute_sync(_fn)

# -------------------------
# High-level operations (sync)
# -------------------------
def get_user_by_telegram(telegram_id: int) -> Optional[Dict[str, Any]]:
    """Sync: Return app_users row for given telegram_id or None."""
    out = _safe_execute_sync(_maybe_single_sync("app_users", "*", ("telegram_id", telegram_id)), single_row=True)
    if out.get("error"):
        return None
    return out.get("data")

def get_user_id(telegram_id: int) -> Optional[str]:
    """Sync: Return the user id (uuid) for the given telegram_id or None if not found."""
    user = get_user_by_telegram(telegram_id)
    if not user:
        return None
    return user.get("id")

def create_app_user(telegram_id: int, name: str, email: Optional[str] = None, phone: Optional[str] = None, role: str = "user") -> Optional[Dict[str, Any]]:
    payload = {"telegram_id": telegram_id, "name": name, "role": role}
    if email:
        payload["email"] = email
    if phone:
        payload["phone"] = phone

    out = _db_insert_sync("app_users", payload, returning="representation")
    if out.get("error") or not out.get("data"):
        logger.error("create_app_user failed: %s", out.get("error"))
        return None
    return out["data"]

def create_farm(owner_id: str, name: str, timezone: str = "UTC") -> Optional[Dict[str, Any]]:
    payload = {"owner_id": owner_id, "name": name, "timezone": timezone}
    out = _db_insert_sync("farms", payload, returning="representation")
    if out.get("error") or not out.get("data"):
        logger.error("create_farm failed: %s", out.get("error"))
        return None
    return out["data"]

def register_user(telegram_id: int, name: str, farm_name: str, timezone: str = "UTC") -> Dict[str, Any]:
    """
    High-level registration (sync). Returns {'user':..., 'farm':...} or {'error':...}
    """
    try:
        existing = get_user_by_telegram(telegram_id)
        if existing:
            user = existing
        else:
            created = create_app_user(telegram_id=telegram_id, name=name)
            if not created:
                return {"error": "Failed to create user"}
            user = created

        created_farm = create_farm(owner_id=user["id"], name=farm_name, timezone=timezone)
        if not created_farm:
            return {"error": "Failed to create farm"}
        return {"user": user, "farm": created_farm}
    except Exception as e:
        logger.exception("register_user failed")
        return {"error": str(e)}

def upsert_app_user_by_telegram(telegram_id: int, update_fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Sync upsert behaviour: attempt update first, then insert if no row changed.
    """
    try:
        def _update():
            return supabase.table("app_users").update(update_fields).eq("telegram_id", telegram_id).select("*").execute()
        out = _safe_execute_sync(_update, single_row=True)
        if out.get("error"):
            logger.warning("upsert update returned error: %s", out.get("error"))
        data = out.get("data")
        if data:
            return data
        payload = {"telegram_id": telegram_id, **update_fields}
        out2 = _db_insert_sync("app_users", payload, returning="representation")
        if out2.get("error") or not out2.get("data"):
            logger.error("upsert insert failed: %s", out2.get("error"))
            return None
        return out2.get("data")
    except Exception:
        logger.exception("upsert_app_user_by_telegram failed")
        return None

# -------------------------
# Breeding events (sync)
# -------------------------
BREEDING_EVENT_TYPES = [
    "mating",
    "insemination",
    "pregnancy_check",
    "calving",
    "miscarriage",
    "abortion",
    "other",
]

BREEDING_LABEL_TO_ENUM = {
    "🤰 Pregnancy check": "pregnancy_check",
    "🧪 Insemination (AI)": "insemination",
    "💕 Mating": "mating",
    "🐄 Calving": "calving",
    "⚠️ Miscarriage": "miscarriage",
    "❌ Abortion": "abortion",
    "🔁 Other": "other",
    "pregnancy_check": "pregnancy_check",
    "insemination": "insemination",
    "mating": "mating",
    "calving": "calving",
    "miscarriage": "miscarriage",
    "abortion": "abortion",
    "other": "other",
}

REPRO_PHASES = [
    "immature",
    "estrus",
    "inseminated",
    "pregnant",
    "dry_off",
    "postpartum",
    "lactating",
    "aborted",
    "unknown"
]

DEFAULT_CONFIG = {
    "gestation_days": 283,
    "postpartum_rest_days": 60,
    "dry_off_days_before_calving": 60,
    "heifer_maturity_months": 15,
    "estrus_cycle_days": 21,
}

def _normalize_event_type_label(label: str) -> str:
    if label in BREEDING_LABEL_TO_ENUM:
        return BREEDING_LABEL_TO_ENUM[label]
    s = (label or "").strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)  # Fixed regex escape sequence
    s = s.replace(" ", "_").replace("-", "_")
    s = re.sub(r"_+", "_", s).strip("_")
    return BREEDING_LABEL_TO_ENUM.get(s, s)

def _get_breeding_config(farm_id: str, key: str) -> Optional[str]:
    """Helper to fetch from breeding_config (sync; add async if needed)."""
    filters = [("farm_id", "eq", farm_id), ("key", "eq", key)]
    config = _safe_execute_sync(_select_sync("breeding_config", "*", filters, limit=1), single_row=True)
    return config.get("data", {}).get("value") if config.get("data") else None

def compute_current_phase(animal_id: str, farm_id: str, current_date: date = date.today()) -> str:
    """Computes repro_phase based on latest events, birth_date, and configs."""
    animal_out = _safe_execute_sync(_maybe_single_sync("animals", "*", ("id", animal_id)), single_row=True)
    animal = animal_out.get("data")
    if not animal or animal.get("sex", "").lower() != "female":
        return "unknown"
    
    birth_date_str = animal.get("birth_date")
    birth_date = dt.fromisoformat(birth_date_str).date() if birth_date_str else None
    maturity_months = int(_get_breeding_config(farm_id, "heifer_maturity_months") or DEFAULT_CONFIG["heifer_maturity_months"])
    if birth_date:
        age = relativedelta(current_date, birth_date)
        age_months = age.months + (age.years * 12)
        if age_months < maturity_months:
            return "immature"
    
    filters = [("animal_id", "eq", animal_id)]
    order_by = ("date", {"ascending": False})
    events_out = _safe_execute_sync(_select_sync("breeding_events", "*", filters, order_by, limit=5))
    events = events_out.get("data", [])
    
    if not events:
        return "estrus" if animal.get("stage") in ("heifer", "cow") else "unknown"
    
    latest = events[0]
    event_type = latest["event_type"]
    event_date_str = latest["date"]
    event_date = dt.fromisoformat(event_date_str).date() if event_date_str else None
    if not event_date:
        return "unknown"
    days_since = (current_date - event_date).days
    
    gestation_days = int(_get_breeding_config(farm_id, "gestation_days") or DEFAULT_CONFIG["gestation_days"])
    postpartum_days = int(_get_breeding_config(farm_id, "postpartum_rest_days") or DEFAULT_CONFIG["postpartum_rest_days"])
    dry_off_days_before_calving = int(_get_breeding_config(farm_id, "dry_off_days_before_calving") or DEFAULT_CONFIG["dry_off_days_before_calving"])
    
    if event_type in ("miscarriage", "abortion"):
        if days_since < postpartum_days:
            return "postpartum"
        return "estrus"
    
    if event_type == "calving":
        if days_since < postpartum_days:
            return "postpartum"
        return "lactating" if animal.get("lactation_stage") else "estrus"
    
    if event_type == "pregnancy_check" and latest.get("outcome") == "successful":
        expected_calving_str = latest.get("expected_calving_date")
        expected_calving = dt.fromisoformat(expected_calving_str).date() if expected_calving_str else None
        if expected_calving:
            days_to_calving = (expected_calving - current_date).days
            if days_to_calving <= dry_off_days_before_calving:
                return "dry_off"
            return "pregnant"
        return "pregnant" if days_since < gestation_days else "unknown"
    
    if event_type in ("insemination", "mating"):
        return "inseminated" if days_since < 30 else "unknown"
    
    return "unknown"

def update_animal_phase(animal_id: str, farm_id: str):
    phase = compute_current_phase(animal_id, farm_id)
    fields = {"repro_phase": phase}
    out = _db_update_sync("animals", "id", animal_id, fields)
    if out.get("error"):
        logger.error("update_animal_phase failed: %s", out.get("error"))

def get_breeding_summary(farm_id: str) -> Dict[str, int]:
    filters = [("farm_id", "eq", farm_id), ("sex", "eq", "female")]
    females_out = _safe_execute_sync(_select_sync("animals", "*", filters, limit=1000))
    females = females_out.get("data", [])
    summary = {phase: 0 for phase in REPRO_PHASES}
    for female in females:
        phase = compute_current_phase(female["id"], farm_id)
        summary[phase] = summary.get(phase, 0) + 1
    return summary

def create_breeding_event(
    farm_id: str,
    animal_id: str,
    event_type: str,
    date_val: Any,
    sire_id: Optional[str] = None,
    details: Optional[str] = None,
    created_by: Optional[str] = None,
    expected_calving_date: Optional[Any] = None,
    outcome: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    candidate = _normalize_event_type_label(event_type)
    if candidate not in BREEDING_EVENT_TYPES:
        logger.error("Invalid breeding event type: %s (normalized %s). allowed=%s", event_type, candidate, BREEDING_EVENT_TYPES)
        return None
    if isinstance(date_val, (date, dt)):
        date_iso = date_val.isoformat() if isinstance(date_val, date) else date_val.date().isoformat()
    else:
        date_iso = None
        try:
            if isinstance(date_val, str) and len(date_val) >= 10:
                date_iso = dt.fromisoformat(date_val).date().isoformat()
        except Exception:
            date_iso = None
    if not date_iso:
        logger.error("Invalid date for breeding_event: %s", date_val)
        return None

    exp_iso = None
    if expected_calving_date:
        if isinstance(expected_calving_date, (date, dt)):
            exp_iso = expected_calving_date.isoformat() if isinstance(expected_calving_date, date) else expected_calving_date.date().isoformat()
        else:
            try:
                exp_iso = dt.fromisoformat(expected_calving_date).date().isoformat()
            except Exception:
                exp_iso = None
    if candidate in ("insemination", "mating") and not exp_iso:
        gestation_days = int(_get_breeding_config(farm_id, "gestation_days") or DEFAULT_CONFIG["gestation_days"])
        d = dt.fromisoformat(date_iso).date()
        exp_iso = (d + timedelta(days=gestation_days)).isoformat()

    payload = {
        "farm_id": farm_id,
        "animal_id": animal_id,
        "event_type": candidate,
        "date": date_iso,
        "sire_id": sire_id,
        "details": details,
        "created_by": created_by,
        "expected_calving_date": exp_iso,
        "outcome": outcome,
        "meta": meta or {}
    }
    out = _db_insert_sync("breeding_events", payload, returning="representation")
    if out.get("error") or not out.get("data"):
        logger.error("create_breeding_event failed: %s", out.get("error"))
        return None

    try:
        update_animal_phase(animal_id, farm_id)
    except Exception:
        logger.exception("Failed to update animal phase after breeding event")

    try:
        rec = out["data"]
        if candidate in ("insemination", "mating"):
            try:
                d = dt.fromisoformat(date_iso).date()
                check_date = (d + timedelta(days=30)).isoformat()
                name = f"Pregnancy check for {animal_id}"
                rem_payload = {"type": "pregnancy_check", "breeding_event_id": rec["id"], "animal_id": animal_id}
                _db_insert_sync("reminders", {"farm_id": farm_id, "name": name, "next_run": check_date, "payload": rem_payload, "enabled": True}, returning="representation")
            except Exception:
                logger.exception("failed creating pregnancy reminder")
            dry_off_days = int(_get_breeding_config(farm_id, "dry_off_days_before_calving") or DEFAULT_CONFIG["dry_off_days_before_calving"])
            if exp_iso:
                exp_d = dt.fromisoformat(exp_iso).date()
                dry_off_date = (exp_d - timedelta(days=dry_off_days)).isoformat()
                name = f"Dry off reminder for {animal_id}"
                rem_payload = {"type": "dry_off", "breeding_event_id": rec["id"], "animal_id": animal_id}
                _db_insert_sync("reminders", {"farm_id": farm_id, "name": name, "next_run": dry_off_date, "payload": rem_payload, "enabled": True})
        if candidate == "calving" and exp_iso:
            try:
                ed = dt.fromisoformat(exp_iso).date()
                remind_date = (ed - timedelta(days=7)).isoformat()
                name = f"Calving reminder for {animal_id}"
                rem_payload = {"type": "calving", "breeding_event_id": rec["id"], "animal_id": animal_id}
                _db_insert_sync("reminders", {"farm_id": farm_id, "name": name, "next_run": remind_date, "payload": rem_payload, "enabled": True}, returning="representation")
            except Exception:
                logger.exception("failed creating calving reminder")
            postpartum_days = int(_get_breeding_config(farm_id, "postpartum_rest_days") or DEFAULT_CONFIG["postpartum_rest_days"])
            d = dt.fromisoformat(date_iso).date()
            next_estrus_date = (d + timedelta(days=postpartum_days)).isoformat()
            name = f"Next estrus reminder for {animal_id} post-calving"
            rem_payload = {"type": "estrus", "breeding_event_id": rec["id"], "animal_id": animal_id}
            _db_insert_sync("reminders", {"farm_id": farm_id, "name": name, "next_run": next_estrus_date, "payload": rem_payload, "enabled": True})
    except Exception:
        logger.exception("post-create breeding reminders failed")

    return out["data"]

def list_breeding_events(farm_id: str, animal_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    filters = [("farm_id", "eq", farm_id)]
    if animal_id:
        filters.append(("animal_id", "eq", animal_id))
    order_by = ("date", {"ascending": False})
    out = _safe_execute_sync(_select_sync("breeding_events", "*", filters, order_by, limit))
    if out.get("error") or not out.get("data"):
        return []
    return out["data"]

def update_breeding_event(event_id: str, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    out = _db_update_sync("breeding_events", "id", event_id, fields)
    if out.get("error") or not out.get("data"):
        logger.error("update_breeding_event failed: %s", out.get("error"))
        return None
    return out["data"]

def delete_breeding_event(event_id: str) -> bool:
    out = _db_delete_sync("breeding_events", "id", event_id)
    if out.get("error"):
        logger.error("delete_breeding_event failed: %s", out.get("error"))
        return False
    return True

# -------------------------
# Inventory items (sync)
# -------------------------
def create_inventory_item(
    farm_id: str,
    name: str,
    category: Optional[str] = None,
    quantity: float = 0,
    unit: str = "unit",
    cost_per_unit: Optional[float] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    payload = {
        "farm_id": farm_id,
        "name": name,
        "category": category,
        "quantity": quantity,
        "unit": unit,
        "cost_per_unit": cost_per_unit,
        "meta": meta or {}
    }
    out = _db_insert_sync("inventory_items", payload, returning="representation")
    if out.get("error") or not out.get("data"):
        logger.error("create_inventory_item failed: %s", out.get("error"))
        return None
    return out["data"]

def list_inventory_items(farm_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    out = _safe_execute_sync(_select_sync("inventory_items", "*", [("farm_id", "eq", farm_id)], ("updated_at", {"ascending": False}), limit))
    if out.get("error") or not out.get("data"):
        return []
    return out["data"]

def update_inventory_item(item_id: str, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    out = _db_update_sync("inventory_items", "id", item_id, fields)
    if out.get("error") or not out.get("data"):
        logger.error("update_inventory_item failed: %s", out.get("error"))
        return None
    return out["data"]

def delete_inventory_item(item_id: str) -> bool:
    out = _db_delete_sync("inventory_items", "id", item_id)
    if out.get("error"):
        logger.error("delete_inventory_item failed: %s", out.get("error"))
        return False
    return True

# -------------------------
# Feed inventory (sync) - upsert behaviour
# -------------------------
def upsert_feed_inventory(
    farm_id: str,
    feed_item_id: str,
    quantity: float,
    unit: str,
    expiry_date: Optional[str] = None,
    quality: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    def _find():
        return supabase.table("feed_inventory").select("*").eq("farm_id", farm_id).eq("feed_item_id", feed_item_id).limit(1).execute()
    found = _safe_execute_sync(_find, single_row=True)
    payload = {
        "farm_id": farm_id,
        "feed_item_id": feed_item_id,
        "quantity": quantity,
        "unit": unit,
        "meta": meta or {},
        "expiry_date": expiry_date,
        "quality": quality
    }
    if found.get("data"):
        existing_id = found["data"].get("id")
        out = _db_update_sync("feed_inventory", "id", existing_id, payload)
        if out.get("error") or not out.get("data"):
            logger.error("upsert_feed_inventory update failed: %s", out.get("error"))
            return None
        return out["data"]
    else:
        out = _db_insert_sync("feed_inventory", payload, returning="representation")
        if out.get("error") or not out.get("data"):
            logger.error("upsert_feed_inventory insert failed: %s", out.get("error"))
            return None
        return out["data"]

def list_feed_inventory(farm_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    out = _safe_execute_sync(_select_sync("feed_inventory", "*", [("farm_id", "eq", farm_id)], ("updated_at", {"ascending": False}), limit))
    if out.get("error") or not out.get("data"):
        return []
    return out["data"]

# -------------------------
# Animals & Milk (sync)
# -------------------------
def list_animals(farm_id: str, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    out = _safe_execute_sync(_select_sync("animals", "*", [("farm_id", "eq", farm_id)], ("created_at", {"ascending": False}), limit, offset))
    if out.get("error") or not out.get("data"):
        return []
    return out["data"]

def get_animal(animal_id: str) -> Optional[Dict[str, Any]]:
    out = _safe_execute_sync(_maybe_single_sync("animals", "*", ("id", animal_id)), single_row=True)
    if out.get("error"):
        return None
    return out.get("data")

def create_animal(
    farm_id: str,
    tag: str,
    name: Optional[str] = None,
    breed: Optional[str] = None,
    sex: str = "female",
    birth_date: Optional[date] = None,
    meta: Optional[Dict] = None,
    weight: Optional[float] = None,
    weight_unit: Optional[str] = "kg",
    initial_phase: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    payload = {
        "farm_id": farm_id,
        "tag": tag,
        "name": name,
        "breed": breed,
        "sex": sex,
        "meta": meta or {},
    }
    if birth_date:
        payload["birth_date"] = birth_date.isoformat() if isinstance(birth_date, (date, dt)) else birth_date
    if weight is not None:
        payload["weight"] = weight
    if weight_unit:
        payload["weight_unit"] = weight_unit
    if initial_phase and initial_phase in REPRO_PHASES:
        payload["repro_phase"] = initial_phase

    out = _db_insert_sync("animals", payload, returning="representation")
    if out.get("error") or not out.get("data"):
        logger.error("create_animal failed: %s", out.get("error"))
        return None
    created = out["data"]
    if not initial_phase:
        try:
            update_animal_phase(created["id"], farm_id)
        except Exception:
            logger.exception("Failed initial phase update for new animal")

    # Non-blocking (best-effort) embedding upsert: lazy import to avoid circular imports.
    try:
        import importlib
        embeddings_mod = importlib.import_module("aiconnection.embeddings")
        aicentral_mod = importlib.import_module("aiconnection.aicentral")
        # Build a short snippet for embedding
        snippet = f"{created.get('tag','') or ''} {created.get('name','') or ''} breed={created.get('breed','') or ''} sex={created.get('sex','') or ''}"
        # Use the sync helper so we don't block long in sync path
        try:
            embeddings_mod.sync_upsert_embedding("animal", created["id"], snippet, created.get("meta", {}), model=None)
        except Exception:
            # swallow embedding errors (non-critical), but log
            logger.exception("Failed to upsert embedding for new animal (sync helper)")
    except Exception:
        # If aiconnection isn't available (dev/test), skip silently
        logger.debug("aiconnection.embeddings not available — skipping embedding upsert (dev/test mode)")

    return created


def update_animal(animal_id: str, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    out = _db_update_sync("animals", "id", animal_id, fields)
    if out.get("error") or not out.get("data"):
        logger.error("update_animal failed: %s", out.get("error"))
        return None
    return out["data"]

def record_milk(farm_id: str, animal_id: Optional[str], quantity: float, recorded_by: Optional[str] = None, entry_type: str = "per_cow", note: Optional[str] = None, date_val: Optional[date] = None) -> Optional[Dict[str, Any]]:
    payload = {
        "farm_id": farm_id,
        "animal_id": animal_id,
        "quantity": quantity,
        "entry_type": entry_type,
        "recorded_by": recorded_by,
        "note": note
    }
    if date_val:
        payload["date"] = date_val.isoformat() if isinstance(date_val, (date, dt)) else date_val
    out = _db_insert_sync("milk_production", payload, returning="representation")
    if out.get("error") or not out.get("data"):
        logger.error("record_milk failed: %s", out.get("error"))
        return None
    return out["data"]

def list_milk(farm_id: str, since: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    def _fn():
        q = supabase.table("milk_production").select("*").eq("farm_id", farm_id).order("date", desc=True).limit(limit)
        if since:
            q = q.gte("date", since)
        return q.execute()
    out = _safe_execute_sync(_fn)
    if out.get("error") or not out.get("data"):
        return []
    return out["data"]

# -------------------------
# Alerts & misc (sync)
# -------------------------
def create_alert_rule(farm_id: str, name: str, condition: Dict[str, Any], cooldown_hours: int = 24, description: Optional[str] = None, enabled: bool = True) -> Optional[Dict[str, Any]]:
    payload = {"farm_id": farm_id, "name": name, "description": description, "condition": condition, "cooldown_hours": cooldown_hours, "enabled": enabled}
    out = _db_insert_sync("alert_rules", payload, returning="representation")
    if out.get("error") or not out.get("data"):
        logger.error("create_alert_rule failed: %s", out.get("error"))
        return None
    return out["data"]

def create_alert(farm_id: str, rule_id: str, animal_id: Optional[str], payload: Dict[str, Any], status: str = "pending") -> Optional[Dict[str, Any]]:
    rec = {"farm_id": farm_id, "rule_id": rule_id, "animal_id": animal_id, "payload": payload, "status": status}
    out = _db_insert_sync("alerts", rec, returning="representation")
    if out.get("error") or not out.get("data"):
        logger.error("create_alert failed: %s", out.get("error"))
        return None
    return out["data"]

# -------------------------
# High-level helpers (sync)
# -------------------------
def get_user_farms(user_id: str) -> List[Dict[str, Any]]:
    out = _safe_execute_sync(_select_sync("farms", "*", [("owner_id", "eq", user_id)], ("created_at", {"ascending": False})))
    if out.get("error") or not out.get("data"):
        return []
    return out["data"]

def get_user_with_farm_by_telegram(telegram_id: int) -> Optional[Dict[str, Any]]:
    """
    Returns combined structure {'user':..., 'farm':...} where farm is the first farm found for the user.
    """
    user = get_user_by_telegram(telegram_id)
    if not user:
        return None
    farms = get_user_farms(user["id"])
    farm = farms[0] if farms else None
    return {"user": user, "farm": farm}

# -------------------------
# Convenience compatibility helpers (sync + async)
# -------------------------
def get_user_by_telegram_id(telegram_id: int) -> Optional[Dict[str, Any]]:
    """Compatibility wrapper for modules expecting get_user_by_telegram_id."""
    return get_user_by_telegram(telegram_id)

def create_user(telegram_id: int, name: str, email: Optional[str] = None, phone: Optional[str] = None, role: str = "user") -> Optional[Dict[str, Any]]:
    """Compatibility wrapper for modules expecting create_user."""
    return create_app_user(telegram_id=telegram_id, name=name, email=email, phone=phone, role=role)

def get_user_farm_id(telegram_id: int) -> Optional[str]:
    """Return the first farm.id for the user identified by telegram_id, or None."""
    combined = get_user_with_farm_by_telegram(telegram_id)
    if not combined or not combined.get("farm"):
        return None
    return combined["farm"].get("id")


def list_health_events(farm_id: str, animal_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Fetch health events for a farm, optionally filtered by animal_id.
    Returns a list of health event records or an empty list on error.
    """
    filters = [("farm_id", "eq", farm_id)]
    if animal_id:
        filters.append(("animal_id", "eq", animal_id))
    order_by = ("date", {"ascending": False})
    out = _safe_execute_sync(_select_sync("health_events", "*", filters, order_by, limit))
    if out.get("error") or not out.get("data"):
        logger.error("list_health_events failed: %s", out.get("error"))
        return []
    return out["data"]

async def async_list_health_events(farm_id: str, animal_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Async wrapper for listing health events.
    """
    return await asyncio.to_thread(list_health_events, farm_id, animal_id, limit)


async def async_get_user_by_telegram_id(telegram_id: int) -> Optional[Dict[str, Any]]:
    return await asyncio.to_thread(get_user_by_telegram, telegram_id)

async def async_create_user(telegram_id: int, name: str, email: Optional[str] = None, phone: Optional[str] = None, role: str = "user") -> Optional[Dict[str, Any]]:
    return await asyncio.to_thread(create_app_user, telegram_id, name, email, phone, role)

async def async_get_user_farm_id(telegram_id: int) -> Optional[str]:
    combined = await asyncio.to_thread(get_user_with_farm_by_telegram, telegram_id)
    if not combined or not combined.get("farm"):
        return None
    return combined["farm"].get("id")

def get_or_create_farm_for_user(user_id: str, farm_name: str, timezone: str = "UTC") -> Optional[Dict[str, Any]]:
    farms = get_user_farms(user_id)
    if farms:
        return farms[0]
    return create_farm(owner_id=user_id, name=farm_name, timezone=timezone)

# --- Async helpers ---
async def async_get_animal(animal_id: str) -> Optional[Dict[str, Any]]:
    return await asyncio.to_thread(get_animal, animal_id)

async def async_list_milk(farm_id: str, since: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    return await asyncio.to_thread(list_milk, farm_id, since, limit)

async def async_update_animal(animal_id: str, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return await asyncio.to_thread(update_animal, animal_id, fields)

async def async_delete_animal(animal_id: str) -> bool:
    def _delete():
        out = _db_delete_sync("animals", "id", animal_id)
        if out.get("error"):
            logger.error("delete animal error: %s", out.get("error"))
            return False
        return True
    return await _run_in_thread(_delete)

# -------------------------
# Async wrappers (non-blocking)
# -------------------------
async def async_get_user_by_telegram(telegram_id: int) -> Optional[Dict[str, Any]]:
    return await asyncio.to_thread(get_user_by_telegram, telegram_id)

async def async_create_app_user(telegram_id: int, name: str, email: Optional[str] = None, phone: Optional[str] = None, role: str = "user") -> Optional[Dict[str, Any]]:
    return await asyncio.to_thread(create_app_user, telegram_id, name, email, phone, role)

async def async_create_farm(owner_id: str, name: str, timezone: str = "UTC") -> Optional[Dict[str, Any]]:
    return await asyncio.to_thread(create_farm, owner_id, name, timezone)

async def async_register_user(telegram_id: int, name: str, farm_name: str, timezone: str = "UTC") -> Dict[str, Any]:
    return await asyncio.to_thread(register_user, telegram_id, name, farm_name, timezone)

async def async_list_animals(farm_id: str, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    return await asyncio.to_thread(list_animals, farm_id, limit, offset)

async def async_create_animal(*args, **kwargs) -> Optional[Dict[str, Any]]:
    return await asyncio.to_thread(create_animal, *args, **kwargs)

async def async_record_milk(*args, **kwargs) -> Optional[Dict[str, Any]]:
    return await asyncio.to_thread(record_milk, *args, **kwargs)

async def async_get_user_with_farm_by_telegram(telegram_id: int) -> Optional[Dict[str, Any]]:
    return await asyncio.to_thread(get_user_with_farm_by_telegram, telegram_id)

async def async_create_breeding_event(*args, **kwargs):
    return await asyncio.to_thread(create_breeding_event, *args, **kwargs)

async def async_list_breeding_events(farm_id: str, animal_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    return await asyncio.to_thread(list_breeding_events, farm_id, animal_id, limit)

async def async_update_breeding_event(*args, **kwargs):
    return await asyncio.to_thread(update_breeding_event, *args, **kwargs)

async def async_delete_breeding_event(event_id: str) -> bool:
    return await asyncio.to_thread(delete_breeding_event, event_id)

async def async_compute_current_phase(animal_id: str, farm_id: str, current_date: date = date.today()) -> str:
    return await asyncio.to_thread(compute_current_phase, animal_id, farm_id, current_date)

async def async_update_animal_phase(animal_id: str, farm_id: str):
    return await asyncio.to_thread(update_animal_phase, animal_id, farm_id)

async def async_get_breeding_summary(farm_id: str) -> Dict[str, int]:
    return await asyncio.to_thread(get_breeding_summary, farm_id)

async def async_create_inventory_item(*args, **kwargs):
    return await asyncio.to_thread(create_inventory_item, *args, **kwargs)

async def async_list_inventory_items(*args, **kwargs):
    return await asyncio.to_thread(list_inventory_items, *args, **kwargs)

async def async_update_inventory_item(*args, **kwargs):
    return await asyncio.to_thread(update_inventory_item, *args, **kwargs)

async def async_delete_inventory_item(item_id: str) -> bool:
    return await asyncio.to_thread(delete_inventory_item, item_id)

async def async_upsert_feed_inventory(*args, **kwargs):
    return await asyncio.to_thread(upsert_feed_inventory, *args, **kwargs)

async def async_list_feed_inventory(*args, **kwargs):
    return await asyncio.to_thread(list_feed_inventory, *args, **kwargs)

# -------------------------
# Permission helper
# -------------------------
def user_can_edit_farm(telegram_id: int, farm_id: str) -> bool:
    user = get_user_by_telegram(telegram_id)
    if not user:
        return False
    user_id = user.get("id")
    farms = get_user_farms(user_id)
    if any(f.get("id") == farm_id for f in farms):
        return True
    out = _safe_execute_sync(_select_sync("farm_members", "*", [("farm_id", "eq", farm_id), ("user_id", "eq", user_id)], None, 1))
    if out.get("error") or not out.get("data"):
        return False
    rows = out.get("data")
    row = rows[0] if isinstance(rows, list) and rows else rows
    if not row:
        return False
    return bool(row.get("can_edit", False))

async def async_user_can_edit_farm(telegram_id: int, farm_id: str) -> bool:
    return await asyncio.to_thread(user_can_edit_farm, telegram_id, farm_id)

# -------------------------
# Small health check & utils
# -------------------------
def check_connection() -> bool:
    try:
        out = supabase.table("app_users").select("id").limit(1).execute()
        data, error = _normalize_response(out)
        if error:
            logger.warning("Supabase health-check returned error: %s", error)
            return False
        return True
    except Exception:
        logger.exception("Supabase health check failed")
        return False

# -------------------------
# Exports
# -------------------------
__all__ = [
    "supabase",
    "get_user_by_telegram",
    "get_user_id",
    "create_app_user",
    "create_farm",
    "register_user",
    "upsert_app_user_by_telegram",
    "list_animals",
    "get_animal",
    "create_animal",
    "update_animal",
    "record_milk",
    "list_milk",
    "create_alert_rule",
    "create_alert",
    "get_user_farms",
    "get_user_with_farm_by_telegram",
    "get_or_create_farm_for_user",
    "check_connection",
    "get_user_by_telegram_id",
    "create_user",
    "get_user_farm_id",
    "create_breeding_event",
    "list_breeding_events",
    "update_breeding_event",
    "delete_breeding_event",
    "compute_current_phase",
    "update_animal_phase",
    "get_breeding_summary",
    "create_inventory_item",
    "list_inventory_items",
    "update_inventory_item",
    "delete_inventory_item",
    "upsert_feed_inventory",
    "list_feed_inventory",
    "async_get_user_by_telegram",
    "async_get_user_id",
    "async_create_app_user",
    "async_create_farm",
    "async_register_user",
    "async_list_animals",
    "async_create_animal",
    "async_record_milk",
    "async_get_user_by_telegram_id",
    "async_create_user",
    "async_get_user_farm_id",
    "async_get_user_with_farm_by_telegram",
    "async_get_animal",
    "async_update_animal",
    "async_delete_animal",
    "user_can_edit_farm",
    "async_user_can_edit_farm",
    "async_create_breeding_event",
    "async_list_breeding_events",
    "async_update_breeding_event",
    "async_delete_breeding_event",
    "async_compute_current_phase",
    "async_update_animal_phase",
    "async_get_breeding_summary",
    "async_create_inventory_item",
    "async_list_inventory_items",
    "async_update_inventory_item",
    "async_delete_inventory_item",
    "async_upsert_feed_inventory",
    "async_list_feed_inventory",
     "list_health_events",
    "async_list_health_events",
]
