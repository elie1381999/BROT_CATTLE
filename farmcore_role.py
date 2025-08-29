import logging
from typing import Any, Dict, List, Optional
from datetime import datetime as dt, timedelta
import uuid
import asyncio
from datetime import timezone

from farmcore import (
    supabase,
    _safe_execute_sync,
    _db_insert_sync,
    _db_update_sync,
    _db_delete_sync,
    _select_sync,
    _run_in_thread,
)

logger = logging.getLogger(__name__)

# Role definitions and permissions (tweak as needed)
FARM_ROLES = ['owner', 'manager', 'worker', 'vet', 'viewer']
DEFAULT_ROLE = 'worker'

ROLE_PERMISSIONS = {
    'owner':   {'animals': True,  'milk': True,  'breeding': True,  'inventory': True,  'finance': True, 'partners': True, 'profile': True,  'roles': True},
    'manager': {'animals': True,  'milk': True,  'breeding': True,  'inventory': True,  'finance': True, 'partners': False, 'profile': True,  'roles': True},
    'worker':  {'animals': True,  'milk': True,  'breeding': False, 'inventory': True,  'finance': False, 'partners': False, 'profile': True,  'roles': False},
    'vet':     {'animals': True,  'milk': False, 'breeding': True,  'inventory': False, 'finance': False, 'partners': False, 'profile': True,  'roles': False},
    'viewer':  {'animals': True,  'milk': True,  'breeding': True,  'inventory': True,  'finance': False, 'partners': False, 'profile': True,  'roles': False},
}

# -------------------------
# Internal helper: single-row select
# -------------------------
def _single_select(table: str, select: str = "*", filters: Optional[List] = None) -> Dict[str, Any]:
    """
    Convenience wrapper to run a select and return the first row or empty dict.
    Uses _select_sync which your farmcore module already exposes.
    Filters should be a list like: [("user_id","eq","..."), ("farm_id","eq","...")]
    """
    try:
        out = _safe_execute_sync(_select_sync(table, select, filters or [], None, 1))
        data = out.get("data") or []
        return data[0] if data else {}
    except Exception as e:
        logger.exception("_single_select failed for table=%s, filters=%s: %s", table, filters, e)
        return {}

# -------------------------
# Invitation helpers (sync)
# -------------------------
def _generate_code(role: str, base_length: int = 8, max_retries: int = 5) -> str:
    """
    Generate a unique code embedding the first and last letters of the role.
    Format: <first_letter><4 chars><last_letter><4 chars>
    Total length: 10 characters.
    """
    if not role or role not in FARM_ROLES:
        logger.error("Invalid role for code generation: %s", role)
        raise ValueError(f"Invalid role: {role}")
    
    first_letter = role[0].upper()
    last_letter = role[-1].upper()
    
    for attempt in range(max_retries):
        base = str(uuid.uuid4()).replace('-', '')[:base_length].upper()
        code = first_letter + base[:4] + last_letter + base[4:]
        
        # Check for uniqueness
        existing = _get_invitation_by_code(code)
        if not existing:
            logger.info("Generated unique invitation code: %s for role %s", code, role)
            return code
        logger.warning("Code collision on attempt %d: %s", attempt + 1, code)
    
    logger.error("Failed to generate unique code for role %s after %d attempts", role, max_retries)
    raise RuntimeError("Could not generate unique invitation code")

def _get_invitation_by_code(code: str) -> Optional[Dict[str, Any]]:
    if not code or not isinstance(code, str) or len(code.strip()) < 4:
        logger.warning("Invalid or too short invitation code: %s", code)
        return None
    code = code.strip().upper()
    try:
        out = _safe_execute_sync(_select_sync("invitation_codes", "*", [("code", "eq", code)], None, 1))
        data = out.get("data") or []
        return data[0] if data else None
    except Exception as e:
        logger.exception("Failed to fetch invitation by code %s: %s", code, e)
        return None

# New: user-friendly code generator (short, readable)
def _generate_user_friendly_code(role: str, length: int = 10) -> str:
    """Simple readable code generator using UUID pieces (uppercase letters/numbers)."""
    base = str(uuid.uuid4()).replace('-', '').upper()
    first = role[0].upper() if role else 'X'
    last  = role[-1].upper() if role else 'X'
    # Choose slices to get approximate desired length
    left = (length // 2) - 1
    right = length - left - 2
    code = first + base[:left] + last + base[left:left+right]
    return code[:length]

def create_invitation(farm_id: str, role: str = DEFAULT_ROLE, expires_in_days: int = 7, created_by: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Create an invitation code for a farm with role."""
    if not farm_id:
        logger.error("No farm_id provided for invitation creation")
        return None
    # ---- validate farm exists ----
    farm_row = _single_select("farms", "id", [("id", "eq", farm_id)])
    if not farm_row or not farm_row.get("id"):
        logger.error("Invalid farm_id provided to create_invitation: %s", farm_id)
        return None
    # --------------------------------
    if role not in FARM_ROLES:
        logger.error("Invalid role for invitation: %s", role)
        return None

    try:
        # Try a few times to avoid collision
        max_attempts = 5
        code = None
        for _ in range(max_attempts):
            candidate = _generate_user_friendly_code(role)
            if not _get_invitation_by_code(candidate):
                code = candidate
                break
        if not code:
            # fallback to the original generator (more unique by checking)
            code = _generate_code(role)

        expires_at = (dt.utcnow() + timedelta(days=expires_in_days)).isoformat()
        payload = {
            "farm_id": farm_id,
            "code": code,
            "role": role,
            "expires_at": expires_at,
            "created_by": created_by,
            "created_at": dt.utcnow().isoformat(),
            "meta": {}
        }
        out = _db_insert_sync("invitation_codes", payload, returning="representation")
        if out.get("error") or not out.get("data"):
            logger.error("create_invitation failed for farm_id=%s, role=%s: %s", farm_id, role, out.get("error"))
            return None
        logger.info("Created invitation code %s for farm_id=%s, role=%s by %s", code, farm_id, role, created_by)
        return out["data"]
    except Exception as e:
        logger.exception("create_invitation failed for farm_id=%s, role=%s: %s", farm_id, role, e)
        return None

def redeem_invitation(code: str, user_id: str) -> Optional[Dict[str, Any]]:
    """Redeem code: Add user to farm_members if valid, update invitation."""
    if not code or not user_id:
        logger.error("Missing code or user_id for redeem_invitation: code=%s, user_id=%s", code, user_id)
        return None
    code = code.strip().upper()
    try:
        inv_out = _safe_execute_sync(_select_sync("invitation_codes", "*", [("code", "eq", code)], None, 1))
        inv_rows = inv_out.get("data") or []
        if not inv_rows:
            logger.error("Invitation not found: %s for user_id=%s", code, user_id)
            return None
        invite = inv_rows[0]

        # Already redeemed?
        if invite.get("redeemed_by"):
            logger.error("Invitation already redeemed: %s by %s", code, invite.get("redeemed_by"))
            return None

        # Expired?
        expires_at = invite.get("expires_at")
        if expires_at:
            try:
                exp_dt = dt.fromisoformat(expires_at)
                if exp_dt < dt.utcnow():
                    logger.error("Invitation expired: %s for user_id=%s", code, user_id)
                    return None
            except Exception as e:
                logger.warning("Invalid expires_at format for code %s: %s", code, e)
                # allow server-side check to reject later if necessary

        farm_id = invite.get("farm_id")
        role = invite.get("role", DEFAULT_ROLE)

        # Check user already member
        member = _single_select("farm_members", "id,role", [("user_id", "eq", user_id), ("farm_id", "eq", farm_id)])
        if member:
            logger.warning("User %s already a member of farm %s with role %s", user_id, farm_id, member.get("role"))
            return None

        # Insert member
        try:
            member_payload = {
                "farm_id": farm_id,
                "user_id": user_id,
                "role": role,
                "can_edit": True if role in ['manager', 'worker'] else False,
                "assigned_by": invite.get("created_by"),
                "assigned_at": dt.utcnow().isoformat(),
                "created_at": dt.utcnow().isoformat(),
                "meta": {}
            }
            member_out = _db_insert_sync("farm_members", member_payload, returning="representation")
            if member_out.get("error") or not member_out.get("data"):
                logger.error("redeem_invitation add member failed for user_id=%s, farm_id=%s: %s", user_id, farm_id, member_out.get("error"))
                return None

            # Update invitation record to mark redeemed_by and redeemed_at
            try:
                _db_update_sync("invitation_codes", "id", invite["id"], {"redeemed_by": user_id, "redeemed_at": dt.utcnow().isoformat()})
                logger.info("Invitation %s redeemed by user_id=%s for farm_id=%s, role=%s", code, user_id, farm_id, role)
            except Exception as e:
                logger.exception("Failed to update invitation record for code %s (non-fatal): %s", code, e)

            # Audit log
                        # --- set user's current_farm_id so UI picks the newly joined farm by default ---
            try:
                # member_out["data"] might be representation row (dict)
                _db_update_sync("app_users", "id", user_id, {"current_farm_id": farm_id})
                logger.info("Set current_farm_id=%s for user_id=%s after redeeming invite %s", farm_id, user_id, code)
            except Exception as e:
                logger.exception("Failed to update app_users.current_farm_id for user_id=%s (non-fatal): %s", user_id, e)

            return member_out["data"]
        except Exception as e:
            logger.exception("redeem_invitation failed for code=%s, user_id=%s: %s", code, user_id, e)
            return None
    except Exception as e:
        logger.exception("redeem_invitation failed for code=%s, user_id=%s: %s", code, user_id, e)
        return None

def list_invitations(farm_id: str, active_only: bool = True) -> List[Dict[str, Any]]:
    """List invitations for a farm. active_only filters out redeemed/expired invites."""
    if not farm_id:
        logger.error("No farm_id provided for list_invitations")
        return []
    try:
        filters = [("farm_id", "eq", farm_id)]
        if active_only:
            filters.append(("redeemed_by", "is", None))
            filters.append(("expires_at", "gte", dt.utcnow().isoformat()))
        out = _safe_execute_sync(_select_sync("invitation_codes", "*", filters, ("created_at", {"ascending": False})))
        data = out.get("data", [])
        logger.info("Listed %d invitations for farm_id=%s (active_only=%s)", len(data), farm_id, active_only)
        return data
    except Exception as e:
        logger.exception("list_invitations failed for farm_id=%s: %s", farm_id, e)
        return []

# -------------------------
# Member helpers (sync)
# -------------------------
def get_farm_members(farm_id: str) -> List[Dict[str, Any]]:
    """Get all members of a farm."""
    if not farm_id:
        logger.error("No farm_id provided for get_farm_members")
        return []
    try:
        out = _safe_execute_sync(_select_sync("farm_members", "*", [("farm_id", "eq", farm_id)], ("created_at", {"ascending": False})))
        data = out.get("data", [])
        logger.info("Retrieved %d members for farm_id=%s", len(data), farm_id)
        return data
    except Exception as e:
        logger.exception("get_farm_members failed for farm_id=%s: %s", farm_id, e)
        return []

def get_user_role_in_farm(user_id: str, farm_id: str) -> Optional[str]:
    """
    Return user's role in a specific farm_id.
    If farm_id is provided, look up farm_members.role first; if not found check farms.owner_id.
    """
    if not farm_id or not user_id:
        logger.error("Missing user_id=%s or farm_id=%s for get_user_role_in_farm", user_id, farm_id)
        return None
    try:
        # Check farm_members
        member = _single_select("farm_members", "role", [("user_id", "eq", user_id), ("farm_id", "eq", farm_id)])
        role = member.get("role")
        if role:
            logger.info("Found role=%s for user_id=%s in farm_id=%s (farm_members)", role, user_id, farm_id)
            return role
        # Check owner
        farm_row = _single_select("farms", "owner_id", [("id", "eq", farm_id)])
        if farm_row.get("owner_id") == user_id:
            logger.info("User_id=%s is owner of farm_id=%s", user_id, farm_id)
            return 'owner'
        logger.info("No role found for user_id=%s in farm_id=%s", user_id, farm_id)
        return None
    except Exception as e:
        logger.exception("get_user_role_in_farm failed for user_id=%s, farm_id=%s: %s", user_id, farm_id, e)
        return None

def find_user_primary_farm(user_id: str) -> Dict[str, Optional[str]]:
    """
    Try to find a primary farm for the user.
    Prefer app_users.current_farm_id if set and valid, then owner, then membership.
    Returns dict: {"farm_id": ..., "role": ...}
    """
    if not user_id:
        logger.error("No user_id provided for find_user_primary_farm")
        return {"farm_id": None, "role": None}
    try:
        # 0) prefer current_farm_id from app_users if valid
        user_row = _single_select("app_users", "current_farm_id", [("id", "eq", user_id)])
        current_farm = user_row.get("current_farm_id")
        if current_farm:
            # validate ownership or membership quickly
            owner_check = _single_select("farms", "id,owner_id", [("id", "eq", current_farm)])
            if owner_check and owner_check.get("id"):
                if owner_check.get("owner_id") == user_id:
                    return {"farm_id": current_farm, "role": "owner"}
                # check membership
                m = _single_select("farm_members", "role", [("user_id", "eq", user_id), ("farm_id", "eq", current_farm)])
                if m and m.get("role"):
                    return {"farm_id": current_farm, "role": m.get("role")}

        # 1) owner
        farm = _single_select("farms", "id", [("owner_id", "eq", user_id)])
        if farm and farm.get("id"):
            logger.info("Found primary farm_id=%s for user_id=%s as owner", farm["id"], user_id)
            return {"farm_id": farm["id"], "role": "owner"}

        # 2) membership (take first)
        member = _single_select("farm_members", "farm_id,role", [("user_id", "eq", user_id)])
        if member and member.get("farm_id"):
            logger.info("Found primary farm_id=%s for user_id=%s with role=%s", member["farm_id"], user_id, member.get("role"))
            return {"farm_id": member["farm_id"], "role": member.get("role")}

        logger.info("No primary farm found for user_id=%s", user_id)
        return {"farm_id": None, "role": None}
    except Exception as e:
        logger.exception("find_user_primary_farm failed for user_id=%s: %s", user_id, e)
        return {"farm_id": None, "role": None}


def revoke_member(farm_id: str, member_id: str = None, member_user_id: str = None) -> bool:
    """
    Revoke a member.
    - If member_id (farm_members.id) is provided, delete by that id.
    - Else if member_user_id is provided, delete records matching (farm_id, user_id).
    """
    if not farm_id or (not member_id and not member_user_id):
        logger.error("Missing parameters for revoke_member: farm_id=%s, member_id=%s, member_user_id=%s", farm_id, member_id, member_user_id)
        return False
    try:
        if member_id:
            out = _db_delete_sync("farm_members", "id", member_id)
            if out.get("error"):
                logger.error("revoke_member by id=%s failed: %s", member_id, out.get("error"))
                return False
            logger.info("Revoked member_id=%s from farm_id=%s", member_id, farm_id)
            return True

        if member_user_id:
            sel = _safe_execute_sync(_select_sync("farm_members", "*", [("farm_id", "eq", farm_id), ("user_id", "eq", member_user_id)]))
            rows = sel.get("data", []) or []
            if not rows:
                logger.warning("revoke_member: no matching member for user_id=%s in farm_id=%s", member_user_id, farm_id)
                return False
            for r in rows:
                try:
                    _db_delete_sync("farm_members", "id", r["id"])
                    logger.info("Revoked member_id=%s (user_id=%s) from farm_id=%s", r["id"], member_user_id, farm_id)
                except Exception as e:
                    logger.exception("Failed to delete farm_members row id=%s for user_id=%s: %s", r.get("id"), member_user_id, e)
            return True

        logger.error("revoke_member called without member_id or member_user_id")
        return False
    except Exception as e:
        logger.exception("revoke_member failed for farm_id=%s: %s", farm_id, e)
        return False

def update_member_role(member_id: str, new_role: str, changed_by: Optional[str] = None) -> bool:
    """
    Update a farm_members row's role and can_edit flag.
    - member_id: the primary key id from farm_members table
    - new_role: must be in FARM_ROLES
    - changed_by: optional user id performing the change (for audit/notification)
    Returns True on success.
    """
    if not member_id or not new_role:
        logger.error("Missing member_id=%s or new_role=%s for update_member_role", member_id, new_role)
        return False
    try:
        if new_role not in FARM_ROLES:
            logger.error("update_member_role: invalid role %s", new_role)
            return False

        # Fetch existing member to get farm_id and user_id
        sel = _safe_execute_sync(_select_sync("farm_members", "*", [("id", "eq", member_id)], None, 1))
        rows = sel.get("data", []) or []
        if not rows:
            logger.warning("update_member_role: no member found with id %s", member_id)
            return False
        member = rows[0]
        farm_id = member.get("farm_id")
        target_user_id = member.get("user_id")

        can_edit = True if new_role in ['manager', 'worker'] else False

        # Perform update
        out = _db_update_sync("farm_members", "id", member_id, {"role": new_role, "can_edit": can_edit})
        if out.get("error"):
            logger.error("update_member_role db update failed for member_id=%s, new_role=%s: %s", member_id, new_role, out.get("error"))
            return False

        # Audit log
        try:
            detail = {
                "member_id": member_id,
                "user_id": target_user_id,
                "new_role": new_role,
                "changed_by": changed_by
            }
            log_action(farm_id=farm_id, user_id=changed_by or target_user_id or 'unknown', object_type='farm_member', object_id=member_id, action='role_change', detail=detail)
            logger.info("Logged role change for member_id=%s to role=%s by changed_by=%s", member_id, new_role, changed_by)
        except Exception as e:
            logger.exception("update_member_role: audit log failed for member_id=%s (non-fatal): %s", member_id, e)

        return True
    except Exception as e:
        logger.exception("update_member_role failed for member_id=%s, new_role=%s: %s", member_id, new_role, e)
        return False

# -------------------------
# Permission check (sync)
# -------------------------
def user_has_permission(user_id: str, farm_id: str, module: str) -> bool:
    if not user_id or not farm_id or not module:
        logger.error("Missing parameters for user_has_permission: user_id=%s, farm_id=%s, module=%s", user_id, farm_id, module)
        return False
    role = get_user_role_in_farm(user_id, farm_id)
    if not role:
        logger.info("No role found for user_id=%s in farm_id=%s for module=%s", user_id, farm_id, module)
        return False
    perms = ROLE_PERMISSIONS.get(role, {})
    has_perm = perms.get(module, False)
    logger.info("Permission check for user_id=%s, farm_id=%s, module=%s: role=%s, allowed=%s", user_id, farm_id, module, role, has_perm)
    return has_perm

# -------------------------
# Audit log (sync)
# -------------------------
def log_action(farm_id: str, user_id: str, object_type: str, object_id: Optional[str] = None, action: str = 'update', detail: Optional[Dict] = None):
    if not farm_id or not user_id or not object_type:
        logger.error("Missing parameters for log_action: farm_id=%s, user_id=%s, object_type=%s", farm_id, user_id, object_type)
        return
    payload = {
        "farm_id": farm_id,
        "user_id": user_id,
        "object_type": object_type,
        "object_id": object_id,
        "action": action,
        "detail": detail or {}
    }
    try:
        _db_insert_sync("audit_logs", payload)
        logger.info("Logged action: farm_id=%s, user_id=%s, object_type=%s, action=%s", farm_id, user_id, object_type, action)
    except Exception as e:
        logger.exception("Failed to write audit log for farm_id=%s, user_id=%s: %s", farm_id, user_id, e)

def get_audit_logs(farm_id: str, since: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    if not farm_id:
        logger.error("No farm_id provided for get_audit_logs")
        return []
    try:
        filters = [("farm_id", "eq", farm_id)]
        if since:
            filters.append(("created_at", "gte", since))
        out = _safe_execute_sync(_select_sync("audit_logs", "*", filters, ("created_at", {"ascending": False}), limit))
        data = out.get("data", [])
        logger.info("Retrieved %d audit logs for farm_id=%s", len(data), farm_id)
        return data
    except Exception as e:
        logger.exception("get_audit_logs failed for farm_id=%s: %s", farm_id, e)
        return []

# -------------------------
# Notification helper (async)
# -------------------------
async def notify_owner(farm_id: str, message: str, bot):
    """
    Send message to farm owner. Looks up owner id from farms.owner_id -> app_users.telegram_id.
    """
    if not farm_id or not message:
        logger.error("Missing farm_id=%s or message for notify_owner", farm_id)
        return
    try:
        farm_out = await asyncio.to_thread(_safe_execute_sync, _select_sync("farms", "owner_id", [("id", "eq", farm_id)], None, 1))
        farm_data = farm_out.get("data") or []
        owner_user_id = farm_data[0].get("owner_id") if farm_data else None
        if not owner_user_id:
            logger.error("No owner found for farm_id=%s", farm_id)
            return

        user_out = await asyncio.to_thread(_safe_execute_sync, _select_sync("app_users", "*", [("id", "eq", owner_user_id)], None, 1))
        user_data = user_out.get("data") or []
        owner = user_data[0] if user_data else {}
        owner_tg = owner.get("telegram_id")
        if not owner_tg:
            logger.error("No Telegram ID for owner user_id=%s", owner_user_id)
            return

        try:
            await bot.send_message(chat_id=owner_tg, text=message, parse_mode="Markdown")
            logger.info("Notified owner telegram_id=%s for farm_id=%s", owner_tg, farm_id)
        except Exception as e:
            logger.error("Failed to notify owner telegram_id=%s: %s", owner_tg, e)
    except Exception as e:
        logger.exception("notify_owner failed for farm_id=%s: %s", farm_id, e)

# -------------------------
# Async wrappers
# -------------------------
async def async_create_invitation(*args, **kwargs):
    return await _run_in_thread(create_invitation, *args, **kwargs)

async def async_redeem_invitation(*args, **kwargs):
    return await _run_in_thread(redeem_invitation, *args, **kwargs)

async def async_list_invitations(*args, **kwargs):
    return await _run_in_thread(list_invitations, *args, **kwargs)

async def async_get_farm_members(*args, **kwargs):
    return await _run_in_thread(get_farm_members, *args, **kwargs)

async def async_get_user_role_in_farm(*args, **kwargs):
    return await _run_in_thread(get_user_role_in_farm, *args, **kwargs)

async def async_find_user_primary_farm(*args, **kwargs):
    return await _run_in_thread(find_user_primary_farm, *args, **kwargs)

async def async_revoke_member(*args, **kwargs):
    return await _run_in_thread(revoke_member, *args, **kwargs)

async def async_update_member_role(*args, **kwargs):
    return await _run_in_thread(update_member_role, *args, **kwargs)

async def async_user_has_permission(*args, **kwargs):
    return await _run_in_thread(user_has_permission, *args, **kwargs)

async def async_log_action(*args, **kwargs):
    return await _run_in_thread(log_action, *args, **kwargs)

async def async_get_audit_logs(*args, **kwargs):
    return await _run_in_thread(get_audit_logs, *args, **kwargs)

async def async_notify_owner(*args, **kwargs):
    return await notify_owner(*args, **kwargs)

# Public exports
__all__ = [
    "create_invitation", "redeem_invitation", "list_invitations",
    "get_farm_members", "get_user_role_in_farm", "revoke_member",
    "update_member_role",
    "user_has_permission", "log_action", "get_audit_logs", "notify_owner",
    "async_create_invitation", "async_redeem_invitation", "async_list_invitations",
    "async_get_farm_members", "async_get_user_role_in_farm", "async_find_user_primary_farm",
    "async_revoke_member", "async_update_member_role", "async_user_has_permission", 
    "async_log_action", "async_get_audit_logs", "async_notify_owner",
    "ROLE_PERMISSIONS", "FARM_ROLES", "DEFAULT_ROLE"
]









'''import logging
from typing import Any, Dict, List, Optional
from datetime import datetime as dt, timedelta
import uuid
import asyncio
from datetime import timezone

from farmcore import (
    supabase,
    _safe_execute_sync,
    _db_insert_sync,
    _db_update_sync,
    _db_delete_sync,
    _select_sync,
    _run_in_thread,
)

logger = logging.getLogger(__name__)

# Role definitions and permissions (tweak as needed)
FARM_ROLES = ['owner', 'manager', 'worker', 'vet', 'viewer']
DEFAULT_ROLE = 'worker'

ROLE_PERMISSIONS = {
    'owner':   {'animals': True,  'milk': True,  'breeding': True,  'inventory': True,  'finance': True, 'partners': True, 'profile': True,  'roles': True},
    'manager': {'animals': True,  'milk': True,  'breeding': True,  'inventory': True,  'finance': True, 'partners': False, 'profile': True,  'roles': True},
    'worker':  {'animals': True,  'milk': True,  'breeding': False, 'inventory': True,  'finance': False, 'partners': False, 'profile': True,  'roles': False},
    'vet':     {'animals': True,  'milk': False, 'breeding': True,  'inventory': False, 'finance': False, 'partners': False, 'profile': True,  'roles': False},
    'viewer':  {'animals': True,  'milk': True,  'breeding': True,  'inventory': True,  'finance': False, 'partners': False, 'profile': True,  'roles': False},
}

# -------------------------
# Internal helper: single-row select
# -------------------------
def _single_select(table: str, select: str = "*", filters: Optional[List] = None) -> Dict[str, Any]:
    """
    Convenience wrapper to run a select and return the first row or empty dict.
    Uses _select_sync which your farmcore module already exposes.
    Filters should be a list like: [("user_id","eq","..."), ("farm_id","eq","...")]
    """
    try:
        out = _safe_execute_sync(_select_sync(table, select, filters or [], None, 1))
        data = out.get("data") or []
        return data[0] if data else {}
    except Exception as e:
        logger.exception("_single_select failed for table=%s, filters=%s: %s", table, filters, e)
        return {}

# -------------------------
# Invitation helpers (sync)
# -------------------------
def _generate_code(role: str, base_length: int = 8, max_retries: int = 5) -> str:
    """
    Generate a unique code embedding the first and last letters of the role.
    Format: <first_letter><4 chars><last_letter><4 chars>
    Total length: 10 characters.
    """
    if not role or role not in FARM_ROLES:
        logger.error("Invalid role for code generation: %s", role)
        raise ValueError(f"Invalid role: {role}")
    
    first_letter = role[0].upper()
    last_letter = role[-1].upper()
    
    for attempt in range(max_retries):
        base = str(uuid.uuid4()).replace('-', '')[:base_length].upper()
        code = first_letter + base[:4] + last_letter + base[4:]
        
        # Check for uniqueness
        existing = _get_invitation_by_code(code)
        if not existing:
            logger.info("Generated unique invitation code: %s for role %s", code, role)
            return code
        logger.warning("Code collision on attempt %d: %s", attempt + 1, code)
    
    logger.error("Failed to generate unique code for role %s after %d attempts", role, max_retries)
    raise RuntimeError("Could not generate unique invitation code")

def _get_invitation_by_code(code: str) -> Optional[Dict[str, Any]]:
    if not code or not isinstance(code, str) or len(code.strip()) < 4:
        logger.warning("Invalid or too short invitation code: %s", code)
        return None
    code = code.strip().upper()
    try:
        out = _safe_execute_sync(_select_sync("invitation_codes", "*", [("code", "eq", code)], None, 1))
        data = out.get("data") or []
        return data[0] if data else None
    except Exception as e:
        logger.exception("Failed to fetch invitation by code %s: %s", code, e)
        return None

# New: user-friendly code generator (short, readable)
def _generate_user_friendly_code(role: str, length: int = 10) -> str:
    """Simple readable code generator using UUID pieces (uppercase letters/numbers)."""
    base = str(uuid.uuid4()).replace('-', '').upper()
    first = role[0].upper() if role else 'X'
    last  = role[-1].upper() if role else 'X'
    # Choose slices to get approximate desired length
    left = (length // 2) - 1
    right = length - left - 2
    code = first + base[:left] + last + base[left:left+right]
    return code[:length]

def create_invitation(farm_id: str, role: str = DEFAULT_ROLE, expires_in_days: int = 7, created_by: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Create an invitation code for a farm with role."""
    if not farm_id:
        logger.error("No farm_id provided for invitation creation")
        return None
    if role not in FARM_ROLES:
        logger.error("Invalid role for invitation: %s", role)
        return None
    try:
        # Try a few times to avoid collision
        max_attempts = 5
        code = None
        for _ in range(max_attempts):
            candidate = _generate_user_friendly_code(role)
            if not _get_invitation_by_code(candidate):
                code = candidate
                break
        if not code:
            # fallback to the original generator (more unique by checking)
            code = _generate_code(role)

        expires_at = (dt.utcnow() + timedelta(days=expires_in_days)).isoformat()
        payload = {
            "farm_id": farm_id,
            "code": code,
            "role": role,
            "expires_at": expires_at,
            "created_by": created_by,
            "created_at": dt.utcnow().isoformat(),
            "meta": {}
        }
        out = _db_insert_sync("invitation_codes", payload, returning="representation")
        if out.get("error") or not out.get("data"):
            logger.error("create_invitation failed for farm_id=%s, role=%s: %s", farm_id, role, out.get("error"))
            return None
        logger.info("Created invitation code %s for farm_id=%s, role=%s by %s", code, farm_id, role, created_by)
        return out["data"]
    except Exception as e:
        logger.exception("create_invitation failed for farm_id=%s, role=%s: %s", farm_id, role, e)
        return None

def redeem_invitation(code: str, user_id: str) -> Optional[Dict[str, Any]]:
    """Redeem code: Add user to farm_members if valid, update invitation."""
    if not code or not user_id:
        logger.error("Missing code or user_id for redeem_invitation: code=%s, user_id=%s", code, user_id)
        return None
    code = code.strip().upper()
    try:
        inv_out = _safe_execute_sync(_select_sync("invitation_codes", "*", [("code", "eq", code)], None, 1))
        inv_rows = inv_out.get("data") or []
        if not inv_rows:
            logger.error("Invitation not found: %s for user_id=%s", code, user_id)
            return None
        invite = inv_rows[0]

        # Already redeemed?
        if invite.get("redeemed_by"):
            logger.error("Invitation already redeemed: %s by %s", code, invite.get("redeemed_by"))
            return None

        # Expired?
        expires_at = invite.get("expires_at")
        if expires_at:
            try:
                exp_dt = dt.fromisoformat(expires_at)
                if exp_dt < dt.utcnow():
                    logger.error("Invitation expired: %s for user_id=%s", code, user_id)
                    return None
            except Exception as e:
                logger.warning("Invalid expires_at format for code %s: %s", code, e)
                # allow server-side check to reject later if necessary

        farm_id = invite.get("farm_id")
        role = invite.get("role", DEFAULT_ROLE)

        # Check user already member
        member = _single_select("farm_members", "id,role", [("user_id", "eq", user_id), ("farm_id", "eq", farm_id)])
        if member:
            logger.warning("User %s already a member of farm %s with role %s", user_id, farm_id, member.get("role"))
            return None

        # Insert member
        try:
            member_payload = {
                "farm_id": farm_id,
                "user_id": user_id,
                "role": role,
                "can_edit": True if role in ['manager', 'worker'] else False,
                "assigned_by": invite.get("created_by"),
                "assigned_at": dt.utcnow().isoformat(),
                "created_at": dt.utcnow().isoformat(),
                "meta": {}
            }
            member_out = _db_insert_sync("farm_members", member_payload, returning="representation")
            if member_out.get("error") or not member_out.get("data"):
                logger.error("redeem_invitation add member failed for user_id=%s, farm_id=%s: %s", user_id, farm_id, member_out.get("error"))
                return None

            # Update invitation record to mark redeemed_by and redeemed_at
            try:
                _db_update_sync("invitation_codes", "id", invite["id"], {"redeemed_by": user_id, "redeemed_at": dt.utcnow().isoformat()})
                logger.info("Invitation %s redeemed by user_id=%s for farm_id=%s, role=%s", code, user_id, farm_id, role)
            except Exception as e:
                logger.exception("Failed to update invitation record for code %s (non-fatal): %s", code, e)

            # Audit log
            try:
                log_action(farm_id=farm_id, user_id=user_id, object_type='farm_member', object_id=member_out["data"]["id"], action='invite_redeemed', detail={"invite_code": code, "role": role})
            except Exception:
                logger.exception("audit log failed for redeem_invitation (non-fatal)")

            return member_out["data"]
        except Exception as e:
            logger.exception("redeem_invitation failed for code=%s, user_id=%s: %s", code, user_id, e)
            return None
    except Exception as e:
        logger.exception("redeem_invitation failed for code=%s, user_id=%s: %s", code, user_id, e)
        return None

def list_invitations(farm_id: str, active_only: bool = True) -> List[Dict[str, Any]]:
    """List invitations for a farm. active_only filters out redeemed/expired invites."""
    if not farm_id:
        logger.error("No farm_id provided for list_invitations")
        return []
    try:
        filters = [("farm_id", "eq", farm_id)]
        if active_only:
            filters.append(("redeemed_by", "is", None))
            filters.append(("expires_at", "gte", dt.utcnow().isoformat()))
        out = _safe_execute_sync(_select_sync("invitation_codes", "*", filters, ("created_at", {"ascending": False})))
        data = out.get("data", [])
        logger.info("Listed %d invitations for farm_id=%s (active_only=%s)", len(data), farm_id, active_only)
        return data
    except Exception as e:
        logger.exception("list_invitations failed for farm_id=%s: %s", farm_id, e)
        return []

# -------------------------
# Member helpers (sync)
# -------------------------
def get_farm_members(farm_id: str) -> List[Dict[str, Any]]:
    """Get all members of a farm."""
    if not farm_id:
        logger.error("No farm_id provided for get_farm_members")
        return []
    try:
        out = _safe_execute_sync(_select_sync("farm_members", "*", [("farm_id", "eq", farm_id)], ("created_at", {"ascending": False})))
        data = out.get("data", [])
        logger.info("Retrieved %d members for farm_id=%s", len(data), farm_id)
        return data
    except Exception as e:
        logger.exception("get_farm_members failed for farm_id=%s: %s", farm_id, e)
        return []

def get_user_role_in_farm(user_id: str, farm_id: str) -> Optional[str]:
    """
    Return user's role in a specific farm_id.
    If farm_id is provided, look up farm_members.role first; if not found check farms.owner_id.
    """
    if not farm_id or not user_id:
        logger.error("Missing user_id=%s or farm_id=%s for get_user_role_in_farm", user_id, farm_id)
        return None
    try:
        # Check farm_members
        member = _single_select("farm_members", "role", [("user_id", "eq", user_id), ("farm_id", "eq", farm_id)])
        role = member.get("role")
        if role:
            logger.info("Found role=%s for user_id=%s in farm_id=%s (farm_members)", role, user_id, farm_id)
            return role
        # Check owner
        farm_row = _single_select("farms", "owner_id", [("id", "eq", farm_id)])
        if farm_row.get("owner_id") == user_id:
            logger.info("User_id=%s is owner of farm_id=%s", user_id, farm_id)
            return 'owner'
        logger.info("No role found for user_id=%s in farm_id=%s", user_id, farm_id)
        return None
    except Exception as e:
        logger.exception("get_user_role_in_farm failed for user_id=%s, farm_id=%s: %s", user_id, farm_id, e)
        return None

def find_user_primary_farm(user_id: str) -> Dict[str, Optional[str]]:
    """
    Try to find a primary farm for the user.
    Returns dict: {"farm_id": ..., "role": ...} or {"farm_id": None, "role": None}
    Order:
      1) farm where user is owner (first match)
      2) farm_members row where user is a member (first match)
    """
    if not user_id:
        logger.error("No user_id provided for find_user_primary_farm")
        return {"farm_id": None, "role": None}
    try:
        # 1) owner
        farm = _single_select("farms", "id", [("owner_id", "eq", user_id)])
        if farm and farm.get("id"):
            logger.info("Found primary farm_id=%s for user_id=%s as owner", farm["id"], user_id)
            return {"farm_id": farm["id"], "role": "owner"}

        # 2) membership (take first)
        member = _single_select("farm_members", "farm_id,role", [("user_id", "eq", user_id)])
        if member and member.get("farm_id"):
            logger.info("Found primary farm_id=%s for user_id=%s with role=%s", member["farm_id"], user_id, member.get("role"))
            return {"farm_id": member["farm_id"], "role": member.get("role")}

        logger.info("No primary farm found for user_id=%s", user_id)
        return {"farm_id": None, "role": None}
    except Exception as e:
        logger.exception("find_user_primary_farm failed for user_id=%s: %s", user_id, e)
        return {"farm_id": None, "role": None}

def revoke_member(farm_id: str, member_id: str = None, member_user_id: str = None) -> bool:
    """
    Revoke a member.
    - If member_id (farm_members.id) is provided, delete by that id.
    - Else if member_user_id is provided, delete records matching (farm_id, user_id).
    """
    if not farm_id or (not member_id and not member_user_id):
        logger.error("Missing parameters for revoke_member: farm_id=%s, member_id=%s, member_user_id=%s", farm_id, member_id, member_user_id)
        return False
    try:
        if member_id:
            out = _db_delete_sync("farm_members", "id", member_id)
            if out.get("error"):
                logger.error("revoke_member by id=%s failed: %s", member_id, out.get("error"))
                return False
            logger.info("Revoked member_id=%s from farm_id=%s", member_id, farm_id)
            return True

        if member_user_id:
            sel = _safe_execute_sync(_select_sync("farm_members", "*", [("farm_id", "eq", farm_id), ("user_id", "eq", member_user_id)]))
            rows = sel.get("data", []) or []
            if not rows:
                logger.warning("revoke_member: no matching member for user_id=%s in farm_id=%s", member_user_id, farm_id)
                return False
            for r in rows:
                try:
                    _db_delete_sync("farm_members", "id", r["id"])
                    logger.info("Revoked member_id=%s (user_id=%s) from farm_id=%s", r["id"], member_user_id, farm_id)
                except Exception as e:
                    logger.exception("Failed to delete farm_members row id=%s for user_id=%s: %s", r.get("id"), member_user_id, e)
            return True

        logger.error("revoke_member called without member_id or member_user_id")
        return False
    except Exception as e:
        logger.exception("revoke_member failed for farm_id=%s: %s", farm_id, e)
        return False

def update_member_role(member_id: str, new_role: str, changed_by: Optional[str] = None) -> bool:
    """
    Update a farm_members row's role and can_edit flag.
    - member_id: the primary key id from farm_members table
    - new_role: must be in FARM_ROLES
    - changed_by: optional user id performing the change (for audit/notification)
    Returns True on success.
    """
    if not member_id or not new_role:
        logger.error("Missing member_id=%s or new_role=%s for update_member_role", member_id, new_role)
        return False
    try:
        if new_role not in FARM_ROLES:
            logger.error("update_member_role: invalid role %s", new_role)
            return False

        # Fetch existing member to get farm_id and user_id
        sel = _safe_execute_sync(_select_sync("farm_members", "*", [("id", "eq", member_id)], None, 1))
        rows = sel.get("data", []) or []
        if not rows:
            logger.warning("update_member_role: no member found with id %s", member_id)
            return False
        member = rows[0]
        farm_id = member.get("farm_id")
        target_user_id = member.get("user_id")

        can_edit = True if new_role in ['manager', 'worker'] else False

        # Perform update
        out = _db_update_sync("farm_members", "id", member_id, {"role": new_role, "can_edit": can_edit})
        if out.get("error"):
            logger.error("update_member_role db update failed for member_id=%s, new_role=%s: %s", member_id, new_role, out.get("error"))
            return False

        # Audit log
        try:
            detail = {
                "member_id": member_id,
                "user_id": target_user_id,
                "new_role": new_role,
                "changed_by": changed_by
            }
            log_action(farm_id=farm_id, user_id=changed_by or target_user_id or 'unknown', object_type='farm_member', object_id=member_id, action='role_change', detail=detail)
            logger.info("Logged role change for member_id=%s to role=%s by changed_by=%s", member_id, new_role, changed_by)
        except Exception as e:
            logger.exception("update_member_role: audit log failed for member_id=%s (non-fatal): %s", member_id, e)

        return True
    except Exception as e:
        logger.exception("update_member_role failed for member_id=%s, new_role=%s: %s", member_id, new_role, e)
        return False

# -------------------------
# Permission check (sync)
# -------------------------
def user_has_permission(user_id: str, farm_id: str, module: str) -> bool:
    if not user_id or not farm_id or not module:
        logger.error("Missing parameters for user_has_permission: user_id=%s, farm_id=%s, module=%s", user_id, farm_id, module)
        return False
    role = get_user_role_in_farm(user_id, farm_id)
    if not role:
        logger.info("No role found for user_id=%s in farm_id=%s for module=%s", user_id, farm_id, module)
        return False
    perms = ROLE_PERMISSIONS.get(role, {})
    has_perm = perms.get(module, False)
    logger.info("Permission check for user_id=%s, farm_id=%s, module=%s: role=%s, allowed=%s", user_id, farm_id, module, role, has_perm)
    return has_perm

# -------------------------
# Audit log (sync)
# -------------------------
def log_action(farm_id: str, user_id: str, object_type: str, object_id: Optional[str] = None, action: str = 'update', detail: Optional[Dict] = None):
    if not farm_id or not user_id or not object_type:
        logger.error("Missing parameters for log_action: farm_id=%s, user_id=%s, object_type=%s", farm_id, user_id, object_type)
        return
    payload = {
        "farm_id": farm_id,
        "user_id": user_id,
        "object_type": object_type,
        "object_id": object_id,
        "action": action,
        "detail": detail or {}
    }
    try:
        _db_insert_sync("audit_logs", payload)
        logger.info("Logged action: farm_id=%s, user_id=%s, object_type=%s, action=%s", farm_id, user_id, object_type, action)
    except Exception as e:
        logger.exception("Failed to write audit log for farm_id=%s, user_id=%s: %s", farm_id, user_id, e)

def get_audit_logs(farm_id: str, since: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    if not farm_id:
        logger.error("No farm_id provided for get_audit_logs")
        return []
    try:
        filters = [("farm_id", "eq", farm_id)]
        if since:
            filters.append(("created_at", "gte", since))
        out = _safe_execute_sync(_select_sync("audit_logs", "*", filters, ("created_at", {"ascending": False}), limit))
        data = out.get("data", [])
        logger.info("Retrieved %d audit logs for farm_id=%s", len(data), farm_id)
        return data
    except Exception as e:
        logger.exception("get_audit_logs failed for farm_id=%s: %s", farm_id, e)
        return []

# -------------------------
# Notification helper (async)
# -------------------------
async def notify_owner(farm_id: str, message: str, bot):
    """
    Send message to farm owner. Looks up owner id from farms.owner_id -> app_users.telegram_id.
    """
    if not farm_id or not message:
        logger.error("Missing farm_id=%s or message for notify_owner", farm_id)
        return
    try:
        farm_out = await asyncio.to_thread(_safe_execute_sync, _select_sync("farms", "owner_id", [("id", "eq", farm_id)], None, 1))
        farm_data = farm_out.get("data") or []
        owner_user_id = farm_data[0].get("owner_id") if farm_data else None
        if not owner_user_id:
            logger.error("No owner found for farm_id=%s", farm_id)
            return

        user_out = await asyncio.to_thread(_safe_execute_sync, _select_sync("app_users", "*", [("id", "eq", owner_user_id)], None, 1))
        user_data = user_out.get("data") or []
        owner = user_data[0] if user_data else {}
        owner_tg = owner.get("telegram_id")
        if not owner_tg:
            logger.error("No Telegram ID for owner user_id=%s", owner_user_id)
            return

        try:
            await bot.send_message(chat_id=owner_tg, text=message, parse_mode="Markdown")
            logger.info("Notified owner telegram_id=%s for farm_id=%s", owner_tg, farm_id)
        except Exception as e:
            logger.error("Failed to notify owner telegram_id=%s: %s", owner_tg, e)
    except Exception as e:
        logger.exception("notify_owner failed for farm_id=%s: %s", farm_id, e)

# -------------------------
# Async wrappers
# -------------------------
async def async_create_invitation(*args, **kwargs):
    return await _run_in_thread(create_invitation, *args, **kwargs)

async def async_redeem_invitation(*args, **kwargs):
    return await _run_in_thread(redeem_invitation, *args, **kwargs)

async def async_list_invitations(*args, **kwargs):
    return await _run_in_thread(list_invitations, *args, **kwargs)

async def async_get_farm_members(*args, **kwargs):
    return await _run_in_thread(get_farm_members, *args, **kwargs)

async def async_get_user_role_in_farm(*args, **kwargs):
    return await _run_in_thread(get_user_role_in_farm, *args, **kwargs)

async def async_find_user_primary_farm(*args, **kwargs):
    return await _run_in_thread(find_user_primary_farm, *args, **kwargs)

async def async_revoke_member(*args, **kwargs):
    return await _run_in_thread(revoke_member, *args, **kwargs)

async def async_update_member_role(*args, **kwargs):
    return await _run_in_thread(update_member_role, *args, **kwargs)

async def async_user_has_permission(*args, **kwargs):
    return await _run_in_thread(user_has_permission, *args, **kwargs)

async def async_log_action(*args, **kwargs):
    return await _run_in_thread(log_action, *args, **kwargs)

async def async_get_audit_logs(*args, **kwargs):
    return await _run_in_thread(get_audit_logs, *args, **kwargs)

async def async_notify_owner(*args, **kwargs):
    return await notify_owner(*args, **kwargs)

# Public exports
__all__ = [
    "create_invitation", "redeem_invitation", "list_invitations",
    "get_farm_members", "get_user_role_in_farm", "revoke_member",
    "update_member_role",
    "user_has_permission", "log_action", "get_audit_logs", "notify_owner",
    "async_create_invitation", "async_redeem_invitation", "async_list_invitations",
    "async_get_farm_members", "async_get_user_role_in_farm", "async_find_user_primary_farm",
    "async_revoke_member", "async_update_member_role", "async_user_has_permission", 
    "async_log_action", "async_get_audit_logs", "async_notify_owner",
    "ROLE_PERMISSIONS", "FARM_ROLES", "DEFAULT_ROLE"
]'''















'''# farmcore_role.py 99
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime as dt, timedelta
import uuid
import asyncio

from farmcore import (
    supabase,
    _safe_execute_sync,
    _db_insert_sync,
    _db_update_sync,
    _db_delete_sync,
    _select_sync,
    _run_in_thread,
)

logger = logging.getLogger(__name__)

# Role definitions and permissions (tweak as needed)
FARM_ROLES = ['owner', 'manager', 'worker', 'vet', 'viewer']
DEFAULT_ROLE = 'worker'

ROLE_PERMISSIONS = {
    'owner':   {'animals': True,  'milk': True,  'breeding': True,  'inventory': True,  'finance': True, 'partners': True, 'profile': True,  'roles': True},
    'manager': {'animals': True,  'milk': True,  'breeding': True,  'inventory': True,  'finance': True, 'partners': False, 'profile': True,  'roles': True},
    'worker':  {'animals': True,  'milk': True,  'breeding': False, 'inventory': True,  'finance': False, 'partners': False, 'profile': True,  'roles': False},
    'vet':     {'animals': True,  'milk': False, 'breeding': True,  'inventory': False, 'finance': False, 'partners': False, 'profile': True,  'roles': False},
    'viewer':  {'animals': True,  'milk': True,  'breeding': True,  'inventory': True,  'finance': False, 'partners': False, 'profile': True,  'roles': False},
}

# -------------------------
# Internal helper: single-row select
# -------------------------
def _single_select(table: str, select: str = "*", filters: Optional[List] = None) -> Dict[str, Any]:
    """
    Convenience wrapper to run a select and return the first row or empty dict.
    Uses _select_sync which your farmcore module already exposes.
    Filters should be a list like: [("user_id","eq","..."), ("farm_id","eq","...")]
    """
    try:
        out = _safe_execute_sync(_select_sync(table, select, filters or [], None, 1))
        data = out.get("data") or []
        return data[0] if data else {}
    except Exception as e:
        logger.exception("_single_select failed for table=%s, filters=%s: %s", table, filters, e)
        return {}

# -------------------------
# Invitation helpers (sync)
# -------------------------
def _generate_code(role: str, base_length: int = 8, max_retries: int = 5) -> str:
    """
    Generate a unique code embedding the first and last letters of the role.
    Format: <first_letter><4 chars><last_letter><4 chars>
    Total length: 10 characters.
    """
    if not role or role not in FARM_ROLES:
        logger.error("Invalid role for code generation: %s", role)
        raise ValueError(f"Invalid role: {role}")
    
    first_letter = role[0].upper()
    last_letter = role[-1].upper()
    
    for attempt in range(max_retries):
        base = str(uuid.uuid4()).replace('-', '')[:base_length].upper()
        code = first_letter + base[:4] + last_letter + base[4:]
        
        # Check for uniqueness
        existing = _get_invitation_by_code(code)
        if not existing:
            logger.info("Generated unique invitation code: %s for role %s", code, role)
            return code
        logger.warning("Code collision on attempt %d: %s", attempt + 1, code)
    
    logger.error("Failed to generate unique code for role %s after %d attempts", role, max_retries)
    raise RuntimeError("Could not generate unique invitation code")


def _get_invitation_by_code(code: str) -> Optional[Dict[str, Any]]:
    if not code or not isinstance(code, str) or len(code.strip()) < 4:
        logger.warning("Invalid or too short invitation code: %s", code)
        return None
    code = code.strip().upper()
    try:
        out = _safe_execute_sync(_select_sync("invitation_codes", "*", [("code", "eq", code)], None, 1))
        data = out.get("data") or []
        return data[0] if data else None
    except Exception as e:
        logger.exception("Failed to fetch invitation by code %s: %s", code, e)
        return None

# New: user-friendly code generator (short, readable)
def _generate_user_friendly_code(role: str, length: int = 10) -> str:
    """Simple readable code generator using UUID pieces (uppercase letters/numbers)."""
    base = str(uuid.uuid4()).replace('-', '').upper()
    first = role[0].upper() if role else 'X'
    last  = role[-1].upper() if role else 'X'
    # Choose slices to get approximate desired length
    left = (length // 2) - 1
    right = length - left - 2
    code = first + base[:left] + last + base[left:left+right]
    return code[:length]

def create_invitation(farm_id: str, role: str = DEFAULT_ROLE, expires_in_days: int = 7, created_by: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Create an invitation code for a farm with role."""
    if not farm_id:
        logger.error("No farm_id provided for invitation creation")
        return None
    if role not in FARM_ROLES:
        logger.error("Invalid role for invitation: %s", role)
        return None
    try:
        # Try a few times to avoid collision
        max_attempts = 5
        code = None
        for _ in range(max_attempts):
            candidate = _generate_user_friendly_code(role)
            if not _get_invitation_by_code(candidate):
                code = candidate
                break
        if not code:
            # fallback to the original generator (more unique by checking)
            code = _generate_code(role)

        expires_at = (dt.utcnow() + timedelta(days=expires_in_days)).isoformat()
        payload = {
            "farm_id": farm_id,
            "code": code,
            "role": role,
            "expires_at": expires_at,
            "created_by": created_by,
            "created_at": dt.utcnow().isoformat(),
            "meta": {}
        }
        out = _db_insert_sync("invitation_codes", payload, returning="representation")
        if out.get("error") or not out.get("data"):
            logger.error("create_invitation failed for farm_id=%s, role=%s: %s", farm_id, role, out.get("error"))
            return None
        logger.info("Created invitation code %s for farm_id=%s, role=%s by %s", code, farm_id, role, created_by)
        return out["data"]
    except Exception as e:
        logger.exception("create_invitation failed for farm_id=%s, role=%s: %s", farm_id, role, e)
        return None

def redeem_invitation(code: str, user_id: str) -> Optional[Dict[str, Any]]:
    """Redeem code: Add user to farm_members if valid, update invitation."""
    if not code or not user_id:
        logger.error("Missing code or user_id for redeem_invitation: code=%s, user_id=%s", code, user_id)
        return None
    code = code.strip().upper()
    try:
        inv_out = _safe_execute_sync(_select_sync("invitation_codes", "*", [("code", "eq", code)], None, 1))
        inv_rows = inv_out.get("data") or []
        if not inv_rows:
            logger.error("Invitation not found: %s for user_id=%s", code, user_id)
            return None
        invite = inv_rows[0]

        # Already redeemed?
        if invite.get("redeemed_by"):
            logger.error("Invitation already redeemed: %s by %s", code, invite.get("redeemed_by"))
            return None

        # Expired?
        expires_at = invite.get("expires_at")
        if expires_at:
            try:
                exp_dt = dt.fromisoformat(expires_at)
                if exp_dt < dt.utcnow():
                    logger.error("Invitation expired: %s for user_id=%s", code, user_id)
                    return None
            except Exception as e:
                logger.warning("Invalid expires_at format for code %s: %s", code, e)
                # allow server-side check to reject later if necessary

        farm_id = invite.get("farm_id")
        role = invite.get("role", DEFAULT_ROLE)

        # Check user already member
        member = _single_select("farm_members", "id,role", [("user_id", "eq", user_id), ("farm_id", "eq", farm_id)])
        if member:
            logger.warning("User %s already a member of farm %s with role %s", user_id, farm_id, member.get("role"))
            return None

        # Insert member
        try:
            member_payload = {
                "farm_id": farm_id,
                "user_id": user_id,
                "role": role,
                "can_edit": True if role in ['manager', 'worker'] else False,
                "assigned_by": invite.get("created_by"),
                "assigned_at": dt.utcnow().isoformat(),
                "created_at": dt.utcnow().isoformat(),
                "meta": {}
            }
            member_out = _db_insert_sync("farm_members", member_payload, returning="representation")
            if member_out.get("error") or not member_out.get("data"):
                logger.error("redeem_invitation add member failed for user_id=%s, farm_id=%s: %s", user_id, farm_id, member_out.get("error"))
                return None

            # Update invitation record to mark redeemed_by and redeemed_at
            try:
                _db_update_sync("invitation_codes", "id", invite["id"], {"redeemed_by": user_id, "redeemed_at": dt.utcnow().isoformat()})
                logger.info("Invitation %s redeemed by user_id=%s for farm_id=%s, role=%s", code, user_id, farm_id, role)
            except Exception as e:
                logger.exception("Failed to update invitation record for code %s (non-fatal): %s", code, e)

            # Audit log
            try:
                log_action(farm_id=farm_id, user_id=user_id, object_type='farm_member', object_id=member_out["data"]["id"], action='invite_redeemed', detail={"invite_code": code, "role": role})
            except Exception:
                logger.exception("audit log failed for redeem_invitation (non-fatal)")

            return member_out["data"]
        except Exception as e:
            logger.exception("redeem_invitation failed for code=%s, user_id=%s: %s", code, user_id, e)
            return None
    except Exception as e:
        logger.exception("redeem_invitation failed for code=%s, user_id=%s: %s", code, user_id, e)
        return None

def list_invitations(farm_id: str, active_only: bool = True) -> List[Dict[str, Any]]:
    """List invitations for a farm. active_only filters out redeemed/expired invites."""
    if not farm_id:
        logger.error("No farm_id provided for list_invitations")
        return []
    try:
        filters = [("farm_id", "eq", farm_id)]
        if active_only:
            filters.append(("redeemed_by", "is", None))
            filters.append(("expires_at", "gte", dt.utcnow().isoformat()))
        out = _safe_execute_sync(_select_sync("invitation_codes", "*", filters, ("created_at", {"ascending": False})))
        data = out.get("data", [])
        logger.info("Listed %d invitations for farm_id=%s (active_only=%s)", len(data), farm_id, active_only)
        return data
    except Exception as e:
        logger.exception("list_invitations failed for farm_id=%s: %s", farm_id, e)
        return []

# -------------------------
# Member helpers (sync)
# -------------------------
def get_farm_members(farm_id: str) -> List[Dict[str, Any]]:
    """Get all members of a farm."""
    if not farm_id:
        logger.error("No farm_id provided for get_farm_members")
        return []
    try:
        out = _safe_execute_sync(_select_sync("farm_members", "*", [("farm_id", "eq", farm_id)], ("created_at", {"ascending": False})))
        data = out.get("data", [])
        logger.info("Retrieved %d members for farm_id=%s", len(data), farm_id)
        return data
    except Exception as e:
        logger.exception("get_farm_members failed for farm_id=%s: %s", farm_id, e)
        return []

def get_user_role_in_farm(user_id: str, farm_id: str) -> Optional[str]:
    """
    Return user's role in a specific farm_id.
    If farm_id is provided, look up farm_members.role first; if not found check farms.owner_id.
    """
    if not farm_id or not user_id:
        logger.error("Missing user_id=%s or farm_id=%s for get_user_role_in_farm", user_id, farm_id)
        return None
    try:
        # Check farm_members
        member = _single_select("farm_members", "role", [("user_id", "eq", user_id), ("farm_id", "eq", farm_id)])
        role = member.get("role")
        if role:
            logger.info("Found role=%s for user_id=%s in farm_id=%s (farm_members)", role, user_id, farm_id)
            return role
        # Check owner
        farm_row = _single_select("farms", "owner_id", [("id", "eq", farm_id)])
        if farm_row.get("owner_id") == user_id:
            logger.info("User_id=%s is owner of farm_id=%s", user_id, farm_id)
            return 'owner'
        logger.info("No role found for user_id=%s in farm_id=%s", user_id, farm_id)
        return None
    except Exception as e:
        logger.exception("get_user_role_in_farm failed for user_id=%s, farm_id=%s: %s", user_id, farm_id, e)
        return None

def find_user_primary_farm(user_id: str) -> Dict[str, Optional[str]]:
    """
    Try to find a primary farm for the user.
    Returns dict: {"farm_id": ..., "role": ...} or {"farm_id": None, "role": None}
    Order:
      1) farm where user is owner (first match)
      2) farm_members row where user is a member (first match)
    """
    if not user_id:
        logger.error("No user_id provided for find_user_primary_farm")
        return {"farm_id": None, "role": None}
    try:
        # 1) owner
        farm = _single_select("farms", "id", [("owner_id", "eq", user_id)])
        if farm and farm.get("id"):
            logger.info("Found primary farm_id=%s for user_id=%s as owner", farm["id"], user_id)
            return {"farm_id": farm["id"], "role": "owner"}

        # 2) membership (take first)
        member = _single_select("farm_members", "farm_id,role", [("user_id", "eq", user_id)])
        if member and member.get("farm_id"):
            logger.info("Found primary farm_id=%s for user_id=%s with role=%s", member["farm_id"], user_id, member.get("role"))
            return {"farm_id": member["farm_id"], "role": member.get("role")}

        logger.info("No primary farm found for user_id=%s", user_id)
        return {"farm_id": None, "role": None}
    except Exception as e:
        logger.exception("find_user_primary_farm failed for user_id=%s: %s", user_id, e)
        return {"farm_id": None, "role": None}

def revoke_member(farm_id: str, member_id: str = None, member_user_id: str = None) -> bool:
    """
    Revoke a member.
    - If member_id (farm_members.id) is provided, delete by that id.
    - Else if member_user_id is provided, delete records matching (farm_id, user_id).
    """
    if not farm_id or (not member_id and not member_user_id):
        logger.error("Missing parameters for revoke_member: farm_id=%s, member_id=%s, member_user_id=%s", farm_id, member_id, member_user_id)
        return False
    try:
        if member_id:
            out = _db_delete_sync("farm_members", "id", member_id)
            if out.get("error"):
                logger.error("revoke_member by id=%s failed: %s", member_id, out.get("error"))
                return False
            logger.info("Revoked member_id=%s from farm_id=%s", member_id, farm_id)
            return True

        if member_user_id:
            sel = _safe_execute_sync(_select_sync("farm_members", "*", [("farm_id", "eq", farm_id), ("user_id", "eq", member_user_id)]))
            rows = sel.get("data", []) or []
            if not rows:
                logger.warning("revoke_member: no matching member for user_id=%s in farm_id=%s", member_user_id, farm_id)
                return False
            for r in rows:
                try:
                    _db_delete_sync("farm_members", "id", r["id"])
                    logger.info("Revoked member_id=%s (user_id=%s) from farm_id=%s", r["id"], member_user_id, farm_id)
                except Exception as e:
                    logger.exception("Failed to delete farm_members row id=%s for user_id=%s: %s", r.get("id"), member_user_id, e)
            return True

        logger.error("revoke_member called without member_id or member_user_id")
        return False
    except Exception as e:
        logger.exception("revoke_member failed for farm_id=%s: %s", farm_id, e)
        return False

def update_member_role(member_id: str, new_role: str, changed_by: Optional[str] = None) -> bool:
    """
    Update a farm_members row's role and can_edit flag.
    - member_id: the primary key id from farm_members table
    - new_role: must be in FARM_ROLES
    - changed_by: optional user id performing the change (for audit/notification)
    Returns True on success.
    """
    if not member_id or not new_role:
        logger.error("Missing member_id=%s or new_role=%s for update_member_role", member_id, new_role)
        return False
    try:
        if new_role not in FARM_ROLES:
            logger.error("update_member_role: invalid role %s", new_role)
            return False

        # Fetch existing member to get farm_id and user_id
        sel = _safe_execute_sync(_select_sync("farm_members", "*", [("id", "eq", member_id)], None, 1))
        rows = sel.get("data", []) or []
        if not rows:
            logger.warning("update_member_role: no member found with id %s", member_id)
            return False
        member = rows[0]
        farm_id = member.get("farm_id")
        target_user_id = member.get("user_id")

        can_edit = True if new_role in ['manager', 'worker'] else False

        # Perform update
        out = _db_update_sync("farm_members", "id", member_id, {"role": new_role, "can_edit": can_edit})
        if out.get("error"):
            logger.error("update_member_role db update failed for member_id=%s, new_role=%s: %s", member_id, new_role, out.get("error"))
            return False

        # Audit log
        try:
            detail = {
                "member_id": member_id,
                "user_id": target_user_id,
                "new_role": new_role,
                "changed_by": changed_by
            }
            log_action(farm_id=farm_id, user_id=changed_by or target_user_id or 'unknown', object_type='farm_member', object_id=member_id, action='role_change', detail=detail)
            logger.info("Logged role change for member_id=%s to role=%s by changed_by=%s", member_id, new_role, changed_by)
        except Exception as e:
            logger.exception("update_member_role: audit log failed for member_id=%s (non-fatal): %s", member_id, e)

        return True
    except Exception as e:
        logger.exception("update_member_role failed for member_id=%s, new_role=%s: %s", member_id, new_role, e)
        return False

# -------------------------
# Permission check (sync)
# -------------------------
def user_has_permission(user_id: str, farm_id: str, module: str) -> bool:
    if not user_id or not farm_id or not module:
        logger.error("Missing parameters for user_has_permission: user_id=%s, farm_id=%s, module=%s", user_id, farm_id, module)
        return False
    role = get_user_role_in_farm(user_id, farm_id)
    if not role:
        logger.info("No role found for user_id=%s in farm_id=%s for module=%s", user_id, farm_id, module)
        return False
    perms = ROLE_PERMISSIONS.get(role, {})
    has_perm = perms.get(module, False)
    logger.info("Permission check for user_id=%s, farm_id=%s, module=%s: role=%s, allowed=%s", user_id, farm_id, module, role, has_perm)
    return has_perm

# -------------------------
# Audit log (sync)
# -------------------------
def log_action(farm_id: str, user_id: str, object_type: str, object_id: Optional[str] = None, action: str = 'update', detail: Optional[Dict] = None):
    if not farm_id or not user_id or not object_type:
        logger.error("Missing parameters for log_action: farm_id=%s, user_id=%s, object_type=%s", farm_id, user_id, object_type)
        return
    payload = {
        "farm_id": farm_id,
        "user_id": user_id,
        "object_type": object_type,
        "object_id": object_id,
        "action": action,
        "detail": detail or {}
    }
    try:
        _db_insert_sync("audit_logs", payload)
        logger.info("Logged action: farm_id=%s, user_id=%s, object_type=%s, action=%s", farm_id, user_id, object_type, action)
    except Exception as e:
        logger.exception("Failed to write audit log for farm_id=%s, user_id=%s: %s", farm_id, user_id, e)

def get_audit_logs(farm_id: str, since: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    if not farm_id:
        logger.error("No farm_id provided for get_audit_logs")
        return []
    try:
        filters = [("farm_id", "eq", farm_id)]
        if since:
            filters.append(("created_at", "gte", since))
        out = _safe_execute_sync(_select_sync("audit_logs", "*", filters, ("created_at", {"ascending": False}), limit))
        data = out.get("data", [])
        logger.info("Retrieved %d audit logs for farm_id=%s", len(data), farm_id)
        return data
    except Exception as e:
        logger.exception("get_audit_logs failed for farm_id=%s: %s", farm_id, e)
        return []

# -------------------------
# Notification helper (async)
# -------------------------
async def notify_owner(farm_id: str, message: str, bot):
    """
    Send message to farm owner. Looks up owner id from farms.owner_id -> app_users.telegram_id.
    """
    if not farm_id or not message:
        logger.error("Missing farm_id=%s or message for notify_owner", farm_id)
        return
    try:
        farm_out = await asyncio.to_thread(_safe_execute_sync, _select_sync("farms", "owner_id", [("id", "eq", farm_id)], None, 1))
        farm_data = farm_out.get("data") or []
        owner_user_id = farm_data[0].get("owner_id") if farm_data else None
        if not owner_user_id:
            logger.error("No owner found for farm_id=%s", farm_id)
            return

        user_out = await asyncio.to_thread(_safe_execute_sync, _select_sync("app_users", "*", [("id", "eq", owner_user_id)], None, 1))
        user_data = user_out.get("data") or []
        owner = user_data[0] if user_data else {}
        owner_tg = owner.get("telegram_id")
        if not owner_tg:
            logger.error("No Telegram ID for owner user_id=%s", owner_user_id)
            return

        try:
            await bot.send_message(chat_id=owner_tg, text=message, parse_mode="Markdown")
            logger.info("Notified owner telegram_id=%s for farm_id=%s", owner_tg, farm_id)
        except Exception as e:
            logger.error("Failed to notify owner telegram_id=%s: %s", owner_tg, e)
    except Exception as e:
        logger.exception("notify_owner failed for farm_id=%s: %s", farm_id, e)

# -------------------------
# Async wrappers
# -------------------------
async def async_create_invitation(*args, **kwargs):
    return await _run_in_thread(create_invitation, *args, **kwargs)

async def async_redeem_invitation(*args, **kwargs):
    return await _run_in_thread(redeem_invitation, *args, **kwargs)

async def async_list_invitations(*args, **kwargs):
    return await _run_in_thread(list_invitations, *args, **kwargs)

async def async_get_farm_members(*args, **kwargs):
    return await _run_in_thread(get_farm_members, *args, **kwargs)

async def async_get_user_role_in_farm(*args, **kwargs):
    return await _run_in_thread(get_user_role_in_farm, *args, **kwargs)

async def async_find_user_primary_farm(*args, **kwargs):
    return await _run_in_thread(find_user_primary_farm, *args, **kwargs)

async def async_revoke_member(*args, **kwargs):
    return await _run_in_thread(revoke_member, *args, **kwargs)

async def async_update_member_role(*args, **kwargs):
    return await _run_in_thread(update_member_role, *args, **kwargs)

async def async_user_has_permission(*args, **kwargs):
    return await _run_in_thread(user_has_permission, *args, **kwargs)

async def async_log_action(*args, **kwargs):
    return await _run_in_thread(log_action, *args, **kwargs)

async def async_get_audit_logs(*args, **kwargs):
    return await _run_in_thread(get_audit_logs, *args, **kwargs)

async def async_notify_owner(*args, **kwargs):
    return await notify_owner(*args, **kwargs)

# Public exports
__all__ = [
    "create_invitation", "redeem_invitation", "list_invitations",
    "get_farm_members", "get_user_role_in_farm", "revoke_member",
    "update_member_role",
    "user_has_permission", "log_action", "get_audit_logs", "notify_owner",
    "async_create_invitation", "async_redeem_invitation", "async_list_invitations",
    "async_get_farm_members", "async_get_user_role_in_farm", "async_find_user_primary_farm",
    "async_revoke_member", "async_update_member_role", "async_user_has_permission", 
    "async_log_action", "async_get_audit_logs", "async_notify_owner",
    "ROLE_PERMISSIONS", "FARM_ROLES", "DEFAULT_ROLE"
]
'''



















'''
# farmcore_role.py
"""
Role / invitation helpers for farm membership management.

This version avoids calling _maybe_single_sync with unexpected argument shapes by
using a small _single_select wrapper around _select_sync, and adds a helper
to find a user's primary farm (owner or member) so older users without a
user->farm mapping still get a role/farm detected.
"""
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime as dt, timedelta
import uuid
import asyncio

from farmcore import (
    supabase,
    _safe_execute_sync,
    _db_insert_sync,
    _db_update_sync,
    _db_delete_sync,
    _select_sync,
    _run_in_thread,
)

logger = logging.getLogger(__name__)

# Role definitions and permissions (tweak as needed)
FARM_ROLES = ['owner', 'manager', 'worker', 'vet', 'viewer']
DEFAULT_ROLE = 'worker'

ROLE_PERMISSIONS = {
    'owner':   {'animals': True,  'milk': True,  'breeding': True,  'inventory': True,  'finance': True, 'partners': True, 'profile': True,  'roles': True},
    'manager': {'animals': True,  'milk': True,  'breeding': True,  'inventory': True,  'finance': True, 'partners': False, 'profile': True,  'roles': True},
    'worker':  {'animals': True,  'milk': True,  'breeding': False, 'inventory': True,  'finance': False, 'partners': False, 'profile': True,  'roles': False},
    'vet':     {'animals': True,  'milk': False, 'breeding': True,  'inventory': False, 'finance': False, 'partners': False, 'profile': True,  'roles': False},
    'viewer':  {'animals': True,  'milk': True,  'breeding': True,  'inventory': True,  'finance': False, 'partners': False, 'profile': True,  'roles': False},
}

# -------------------------
# Internal helper: single-row select
# -------------------------
def _single_select(table: str, select: str = "*", filters: Optional[List] = None) -> Dict[str, Any]:
    """
    Convenience wrapper to run a select and return the first row or empty dict.
    Uses _select_sync which your farmcore module already exposes.
    Filters should be a list like: [("user_id","eq","..."), ("farm_id","eq","...")]
    """
    try:
        # call _select_sync(table, select, filters, order=None, limit=1)
        out = _safe_execute_sync(_select_sync(table, select, filters or [], None, 1))
        data = out.get("data") or []
        return data[0] if data else {}
    except Exception as e:
        logger.exception("_single_select failed for %s: %s", table, e)
        return {}

# -------------------------
# Invitation helpers (sync)
# -------------------------
def _generate_code(length: int = 10) -> str:
    return str(uuid.uuid4()).replace('-', '')[:length].upper()

def create_invitation(farm_id: str, role: str = DEFAULT_ROLE, expires_in_days: int = 7, created_by: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Create an invitation code for a farm with role."""
    if role not in FARM_ROLES:
        logger.error("Invalid role: %s", role)
        return None
    code = _generate_code(10)
    expires_at = (dt.utcnow() + timedelta(days=expires_in_days)).isoformat()
    payload = {
        "farm_id": farm_id,
        "code": code,
        "role": role,
        "expires_at": expires_at,
        "created_by": created_by,
    }
    out = _db_insert_sync("invitation_codes", payload, returning="representation")
    if out.get("error") or not out.get("data"):
        logger.error("create_invitation failed: %s", out.get("error"))
        return None
    return out["data"]

def _get_invitation_by_code(code: str) -> Optional[Dict[str, Any]]:
    if not code:
        return None
    code = code.strip().upper()
    # Use _select_sync directly to avoid signature mismatch
    out = _safe_execute_sync(_select_sync("invitation_codes", "*", [("code", "eq", code)], None, 1))
    data = out.get("data") or []
    return data[0] if data else None

def redeem_invitation(code: str, user_id: str) -> Optional[Dict[str, Any]]:
    """Redeem code: Add user to farm_members if valid, update invitation."""
    code = (code or "").strip().upper()
    invite = _get_invitation_by_code(code)
    if not invite:
        logger.error("Invitation not found: %s", code)
        return None

    # already redeemed?
    if invite.get("redeemed_by"):
        logger.error("Invitation already redeemed: %s", code)
        return None

    # expired?
    expires_at = invite.get("expires_at")
    if expires_at:
        try:
            exp_dt = dt.fromisoformat(expires_at)
            if exp_dt < dt.utcnow():
                logger.error("Invitation expired: %s", code)
                return None
        except Exception:
            # if weird format, allow server-side check to reject later
            pass

    farm_id = invite["farm_id"]
    role = invite.get("role", DEFAULT_ROLE)

    # Check user already member
    member = _single_select("farm_members", "id,role", [("user_id","eq",user_id),("farm_id","eq",farm_id)])
    if member:
        logger.warning("User %s already a member of farm %s", user_id, farm_id)
        return None

    # Insert member
    member_payload = {
        "farm_id": farm_id,
        "user_id": user_id,
        "role": role,
        "can_edit": True if role in ['manager', 'worker'] else False
    }
    member_out = _db_insert_sync("farm_members", member_payload, returning="representation")
    if member_out.get("error") or not member_out.get("data"):
        logger.error("redeem_invitation add member failed: %s", member_out.get("error"))
        return None

    # Update invitation record to mark redeemed_by and redeemed_at
    try:
        _db_update_sync("invitation_codes", "id", invite["id"], {"redeemed_by": user_id, "meta": {"redeemed_at": dt.utcnow().isoformat()}})
    except Exception as e:
        logger.exception("Failed to update invitation record (non-fatal): %s", e)

    return member_out["data"]

def list_invitations(farm_id: str, active_only: bool = True) -> List[Dict[str, Any]]:
    """List invitations for a farm. active_only filters out redeemed/expired invites."""
    filters = [("farm_id", "eq", farm_id)]
    if active_only:
        filters.append(("redeemed_by", "is", None))
        filters.append(("expires_at", "gte", dt.utcnow().isoformat()))
    out = _safe_execute_sync(_select_sync("invitation_codes", "*", filters, ("created_at", {"ascending": False})))
    return out.get("data", []) if out.get("data") else []

# -------------------------
# Member helpers (sync)
# -------------------------
def get_farm_members(farm_id: str) -> List[Dict[str, Any]]:
    """Get all members of a farm."""
    out = _safe_execute_sync(_select_sync("farm_members", "*", [("farm_id", "eq", farm_id)], ("created_at", {"ascending": False})))
    return out.get("data", []) if out.get("data") else []

def get_user_role_in_farm(user_id: str, farm_id: str) -> Optional[str]:
    """
    Return user's role in a specific farm_id.
    If farm_id is provided, look up farm_members.role first; if not found check farms.owner_id.
    """
    if not farm_id:
        return None
    # Check farm_members
    member = _single_select("farm_members", "role", [("user_id","eq",user_id),("farm_id","eq",farm_id)])
    role = member.get("role")
    if role:
        return role
    # Check owner
    farm_row = _single_select("farms", "owner_id", [("id","eq",farm_id)])
    if farm_row.get("owner_id") == user_id:
        return 'owner'
    return None

def find_user_primary_farm(user_id: str) -> Dict[str, Optional[str]]:
    """
    Try to find a primary farm for the user.
    Returns dict: {"farm_id": ..., "role": ...} or {"farm_id": None, "role": None}
    Order:
      1) farm where user is owner (first match)
      2) farm_members row where user is a member (first match)
    """
    try:
        # 1) owner
        farm = _single_select("farms", "id", [("owner_id","eq",user_id)])
        if farm and farm.get("id"):
            return {"farm_id": farm["id"], "role": "owner"}

        # 2) membership (take first)
        member = _single_select("farm_members", "farm_id,role", [("user_id","eq",user_id)])
        if member and member.get("farm_id"):
            return {"farm_id": member["farm_id"], "role": member.get("role")}

        return {"farm_id": None, "role": None}
    except Exception as e:
        logger.exception("find_user_primary_farm failed for %s: %s", user_id, e)
        return {"farm_id": None, "role": None}

def revoke_member(farm_id: str, member_id: str = None, member_user_id: str = None) -> bool:
    """
    Revoke a member.
    - If member_id (farm_members.id) is provided, delete by that id.
    - Else if member_user_id is provided, delete records matching (farm_id, user_id).
    """
    try:
        if member_id:
            out = _db_delete_sync("farm_members", "id", member_id)
            if out.get("error"):
                logger.error("revoke_member by id failed: %s", out.get("error"))
                return False
            return True

        if member_user_id:
            # select matching rows and delete each by id
            sel = _safe_execute_sync(_select_sync("farm_members", "*", [("farm_id", "eq", farm_id), ("user_id", "eq", member_user_id)]))
            rows = sel.get("data", []) or []
            if not rows:
                logger.warning("revoke_member: no matching member for user_id %s in farm %s", member_user_id, farm_id)
                return False
            for r in rows:
                try:
                    _db_delete_sync("farm_members", "id", r["id"])
                except Exception:
                    logger.exception("Failed to delete farm_members row id=%s", r.get("id"))
            return True

        logger.error("revoke_member called without member_id or member_user_id")
        return False
    except Exception as e:
        logger.exception("revoke_member exception: %s", e)
        return False

# -------------------------
# Permission check (sync)
# -------------------------
def user_has_permission(user_id: str, farm_id: str, module: str) -> bool:
    role = get_user_role_in_farm(user_id, farm_id)
    if not role:
        return False
    perms = ROLE_PERMISSIONS.get(role, {})
    return perms.get(module, False)

# -------------------------
# Audit log (sync)
# -------------------------
def log_action(farm_id: str, user_id: str, object_type: str, object_id: Optional[str] = None, action: str = 'update', detail: Optional[Dict] = None):
    payload = {
        "farm_id": farm_id,
        "user_id": user_id,
        "object_type": object_type,
        "object_id": object_id,
        "action": action,
        "detail": detail or {}
    }
    try:
        _db_insert_sync("audit_logs", payload)
    except Exception:
        logger.exception("Failed to write audit log")

def get_audit_logs(farm_id: str, since: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    filters = [("farm_id", "eq", farm_id)]
    if since:
        filters.append(("created_at", "gte", since))
    out = _safe_execute_sync(_select_sync("audit_logs", "*", filters, ("created_at", {"ascending": False}), limit))
    return out.get("data", []) if out.get("data") else []

# -------------------------
# Notification helper (async)
# -------------------------
async def notify_owner(farm_id: str, message: str, bot):
    """
    Send message to farm owner. Looks up owner id from farms.owner_id -> app_users.telegram_id.
    """
    try:
        farm_out = await asyncio.to_thread(_safe_execute_sync, _select_sync("farms", "owner_id", [("id","eq",farm_id)], None, 1))
        farm_data = farm_out.get("data") or []
        owner_user_id = farm_data[0].get("owner_id") if farm_data else None
        if not owner_user_id:
            logger.error("No owner found for farm %s", farm_id)
            return

        user_out = await asyncio.to_thread(_safe_execute_sync, _select_sync("app_users", "*", [("id","eq",owner_user_id)], None, 1))
        user_data = user_out.get("data") or []
        owner = user_data[0] if user_data else {}
        owner_tg = owner.get("telegram_id")
        if not owner_tg:
            logger.error("No Telegram ID for owner %s", owner_user_id)
            return

        try:
            await bot.send_message(chat_id=owner_tg, text=message, parse_mode="Markdown")
        except Exception as e:
            logger.error("Failed to notify owner %s: %s", owner_tg, e)
    except Exception:
        logger.exception("notify_owner failed")

# -------------------------
# Async wrappers
# -------------------------
async def async_create_invitation(*args, **kwargs):
    return await _run_in_thread(create_invitation, *args, **kwargs)

async def async_redeem_invitation(*args, **kwargs):
    return await _run_in_thread(redeem_invitation, *args, **kwargs)

async def async_list_invitations(*args, **kwargs):
    return await _run_in_thread(list_invitations, *args, **kwargs)

async def async_get_farm_members(*args, **kwargs):
    return await _run_in_thread(get_farm_members, *args, **kwargs)

async def async_get_user_role_in_farm(*args, **kwargs):
    return await _run_in_thread(get_user_role_in_farm, *args, **kwargs)

async def async_find_user_primary_farm(*args, **kwargs):
    return await _run_in_thread(find_user_primary_farm, *args, **kwargs)

async def async_revoke_member(*args, **kwargs):
    return await _run_in_thread(revoke_member, *args, **kwargs)

async def async_user_has_permission(*args, **kwargs):
    return await _run_in_thread(user_has_permission, *args, **kwargs)

async def async_log_action(*args, **kwargs):
    return await _run_in_thread(log_action, *args, **kwargs)

async def async_get_audit_logs(*args, **kwargs):
    return await _run_in_thread(get_audit_logs, *args, **kwargs)

async def async_notify_owner(*args, **kwargs):
    return await notify_owner(*args, **kwargs)

# Public exports
__all__ = [
    "create_invitation", "redeem_invitation", "list_invitations",
    "get_farm_members", "get_user_role_in_farm", "revoke_member",
    "user_has_permission", "log_action", "get_audit_logs", "notify_owner",
    "async_create_invitation", "async_redeem_invitation", "async_list_invitations",
    "async_get_farm_members", "async_get_user_role_in_farm", "async_find_user_primary_farm",
    "async_revoke_member", "async_user_has_permission", "async_log_action", "async_get_audit_logs",
    "async_notify_owner", "ROLE_PERMISSIONS", "FARM_ROLES", "DEFAULT_ROLE"
]

'''