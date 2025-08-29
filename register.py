# register.py
"""
Improved registration helpers for FarmBot.

Provides:
  - async_begin_registration / begin_registration (sync fallback)
  - async_complete_registration / complete_registration (sync fallback)
  - flow helpers for use inside handlers: start_flow, handle_name_input, handle_farm_input, cancel_flow

Return shape (dict):
  - Success: {"status": "ok", "user": {...}, "farm": {...}, "message": "..."}
  - Error:   {"status": "error", "error": "...", "code": "optional_code"}

Notes:
  - This module uses async farmcore helpers to avoid blocking the event loop.
  - Keep .env and farmcore configured correctly.
"""

import logging
import re
from typing import Any, Dict, Optional

from farmcore import (
    # async helpers (must exist in your upgraded farmcore)
    async_get_user_by_telegram,
    async_create_app_user,
    async_create_farm,
    async_get_user_with_farm_by_telegram,
    async_register_user,  # present for convenience (creates user + farm)
    # sync fallbacks (kept for scripts/tests)
    get_user_by_telegram,
    register_user,
)

logger = logging.getLogger(__name__)

# Basic validation rules
_NAME_MIN = 2
_NAME_MAX = 120
_FARM_MIN = 2
_FARM_MAX = 120
# Allow letters, numbers, spaces, punctuation common in names/farmnames
_VALID_NAME_RE = re.compile(r"^[\w\-\.\' \u00C0-\u024F]+$", flags=re.UNICODE)


def _sanitize_text(s: str) -> str:
    """Trim and collapse whitespace."""
    return " ".join((s or "").strip().split())


def _validate_name(name: str) -> Optional[str]:
    if not name:
        return "Name is required."
    name = _sanitize_text(name)
    if len(name) < _NAME_MIN:
        return f"Name is too short (min {_NAME_MIN} characters)."
    if len(name) > _NAME_MAX:
        return f"Name is too long (max {_NAME_MAX} characters)."
    if not _VALID_NAME_RE.match(name):
        return "Name contains invalid characters."
    return None


def _validate_farm_name(farm: str) -> Optional[str]:
    if not farm:
        return "Farm name is required."
    farm = _sanitize_text(farm)
    if len(farm) < _FARM_MIN:
        return f"Farm name is too short (min {_FARM_MIN} characters)."
    if len(farm) > _FARM_MAX:
        return f"Farm name is too long (max {_FARM_MAX} characters)."
    # allow a broader set for farm names (including commas/slash)
    return None


# -------------------------
# Async high-level functions
# -------------------------
async def async_begin_registration(telegram_id: int) -> Dict[str, Any]:
    """
    Check if a user exists. Returns:
      {"status":"ok","exists":True,"user":{...}} or {"status":"ok","exists":False}
      or {"status":"error","error":...}
    """
    try:
        user = await async_get_user_by_telegram(telegram_id)
        if user:
            return {"status": "ok", "exists": True, "user": user}
        return {"status": "ok", "exists": False}
    except Exception as e:
        logger.exception("async_begin_registration failed")
        return {"status": "error", "error": str(e)}


async def async_complete_registration(telegram_id: int, name: str, farm_name: str, timezone: str = "UTC") -> Dict[str, Any]:
    """
    High-level registration function (non-blocking).
    Idempotent-ish:
      - If user exists and already has a farm, returns existing resources.
      - If user exists but no farm, creates a farm.
      - If user missing, creates user and farm.
    Returns {"status":"ok","user":...,"farm":...} or {"status":"error","error":...}
    """
    try:
        # sanitize
        name_s = _sanitize_text(name)
        farm_s = _sanitize_text(farm_name)

        # validate
        name_err = _validate_name(name_s)
        if name_err:
            return {"status": "error", "error": name_err, "code": "invalid_name"}

        farm_err = _validate_farm_name(farm_s)
        if farm_err:
            return {"status": "error", "error": farm_err, "code": "invalid_farm_name"}

        # Try convenience async_register_user first (it creates user+farm)
        try:
            result = await async_register_user(telegram_id=telegram_id, name=name_s, farm_name=farm_s, timezone=timezone)
            # async_register_user returns dict like farmcore.register_user
            if isinstance(result, dict) and result.get("error"):
                # fallthrough to more explicit path if register_user indicates failure
                logger.warning("async_register_user returned error, falling back: %s", result.get("error"))
            else:
                # sometimes async_register_user returns {'user':..., 'farm':...}
                if result and isinstance(result, dict) and result.get("user"):
                    return {"status": "ok", "user": result.get("user"), "farm": result.get("farm")}
        except Exception:
            # continue with explicit flow
            logger.info("async_register_user failed, continuing with explicit create flow")

        # Explicit idempotent flow:
        # 1) Check if user exists (and fetch their farm)
        existing = await async_get_user_with_farm_by_telegram(telegram_id)
        if existing:
            user = existing.get("user")
            farm = existing.get("farm")
            if farm:
                return {"status": "ok", "user": user, "farm": farm, "message": "user_and_farm_exists"}
            # create farm for existing user
            created_farm = await async_create_farm(owner_id=user["id"], name=farm_s, timezone=timezone)
            if not created_farm:
                return {"status": "error", "error": "Failed to create farm for existing user"}
            return {"status": "ok", "user": user, "farm": created_farm, "message": "farm_created_for_existing_user"}

        # 2) Create user
        created_user = await async_create_app_user(telegram_id=telegram_id, name=name_s, email=None, phone=None, role="user")
        if not created_user:
            return {"status": "error", "error": "Failed to create user"}
        # 3) Create farm
        created_farm = await async_create_farm(owner_id=created_user["id"], name=farm_s, timezone=timezone)
        if not created_farm:
            return {"status": "error", "error": "Failed to create farm after user creation"}
        return {"status": "ok", "user": created_user, "farm": created_farm, "message": "created_user_and_farm"}

    except Exception as e:
        logger.exception("async_complete_registration failed")
        return {"status": "error", "error": str(e)}


# -------------------------
# Flow helpers for handlers
# -------------------------
def start_flow(context_user_data: dict) -> None:
    """Initialize registration flow in context.user_data (sync)."""
    context_user_data["register_flow"] = "name"
    # optionally pre-clear prior register keys
    context_user_data.pop("register_name", None)


def handle_name_input(context_user_data: dict, text: str) -> Dict[str, Any]:
    """
    Store name and advance the flow. Returns small dict to indicate next step or validation error.
    Synchronous helper intended to be used in an async handler (it only mutates user_data).
    """
    name_s = _sanitize_text(text)
    err = _validate_name(name_s)
    if err:
        return {"status": "error", "error": err}
    context_user_data["register_name"] = name_s
    context_user_data["register_flow"] = "farm_name"
    return {"status": "ok", "next": "farm_name"}


async def handle_farm_input(context_user_data: dict, telegram_id: int, text: str, timezone: str = "UTC") -> Dict[str, Any]:
    """
    Called when the user has entered the farm name. Completes registration (async).
    Will clear register_flow keys on success or leave them for retry on error.
    Returns the same dict shape as async_complete_registration.
    """
    farm_s = _sanitize_text(text)
    err = _validate_farm_name(farm_s)
    if err:
        return {"status": "error", "error": err}

    # require name to be present
    name = context_user_data.get("register_name")
    if not name:
        return {"status": "error", "error": "Missing previously-entered name. Please start again.", "code": "missing_name"}

    res = await async_complete_registration(telegram_id=telegram_id, name=name, farm_name=farm_s, timezone=timezone)
    if res.get("status") == "ok":
        # cleanup flow keys
        context_user_data.pop("register_flow", None)
        context_user_data.pop("register_name", None)
    return res


def cancel_flow(context_user_data: dict) -> Dict[str, Any]:
    """Cancel and clear any registration keys."""
    context_user_data.pop("register_flow", None)
    context_user_data.pop("register_name", None)
    return {"status": "ok", "message": "registration_cancelled"}


# -------------------------
# Sync wrappers (for scripts/tests)
# -------------------------
def begin_registration(telegram_id: int) -> Dict[str, Any]:
    """Sync wrapper — calls farmcore.get_user_by_telegram (blocking)."""
    try:
        existing = get_user_by_telegram(telegram_id)
        if existing:
            return {"status": "ok", "exists": True, "user": existing}
        return {"status": "ok", "exists": False}
    except Exception as e:
        logger.exception("begin_registration (sync) failed")
        return {"status": "error", "error": str(e)}


def complete_registration(telegram_id: int, name: str, farm_name: str, timezone: str = "UTC") -> Dict[str, Any]:
    """
    Sync wrapper around farmcore.register_user (blocking).
    Kept for backward compatibility.
    """
    try:
        result = register_user(telegram_id=telegram_id, name=name, farm_name=farm_name, timezone=timezone)
        if isinstance(result, dict) and result.get("error"):
            return {"status": "error", "error": result.get("error")}
        return {"status": "ok", "user": result.get("user"), "farm": result.get("farm")}
    except Exception as e:
        logger.exception("complete_registration (sync) failed")
        return {"status": "error", "error": str(e)}
