# promo_helper.py
import asyncio
import logging
from typing import Optional, Dict, Any

from farmcore import supabase, async_get_user_by_telegram

logger = logging.getLogger("promo_helper")


async def apply_promo_for_user(telegram_id: int, promo_code: str, existing_user: bool = False) -> Dict[str, Any]:
    """
    Best-effort: attach promo_code to app_user and create partner_signups.
    Returns dict with status and debug info.
    """
    if not promo_code:
        return {"ok": False, "error": "no_promo_code"}

    # 1) lookup promo row (sync supabase usage in thread)
    def _get_promo():
        res = supabase.table("promo_codes").select("id,partner_id,uses,max_uses,code").eq("code", promo_code).limit(1).execute()
        return getattr(res, "data", None) or []

    try:
        promo_rows = await asyncio.to_thread(_get_promo)
    except Exception as e:
        logger.exception("promo lookup failed")
        return {"ok": False, "error": "promo_lookup_failed", "detail": str(e)}

    if not promo_rows:
        return {"ok": False, "error": "promo_not_found", "promo_code": promo_code}

    promo = promo_rows[0]
    promo_id = promo.get("id")
    partner_id = promo.get("partner_id")
    current_uses = int(promo.get("uses") or 0)
    max_uses = int(promo.get("max_uses") or 0)

    if max_uses != 0 and current_uses >= max_uses:
        return {"ok": False, "error": "promo_maxed", "promo_id": promo_id}

    # 2) find user / ensure exists
    try:
        user_row = await async_get_user_by_telegram(telegram_id)
    except Exception as e:
        logger.exception("user lookup failed")
        return {"ok": False, "error": "user_lookup_failed", "detail": str(e)}

    if not user_row:
        return {"ok": False, "error": "user_not_found"}

    user_id = user_row["id"]

    # 3) patch app_users.meta and referred_by
    def _patch_app_user():
        # fetch existing meta
        res = supabase.table("app_users").select("meta").eq("id", user_id).limit(1).execute()
        data = getattr(res, "data", None) or []
        current_meta = data[0].get("meta") if data else {}
        if not isinstance(current_meta, dict):
            current_meta = {}
        current_meta["promo_code_id"] = promo_id
        # perform update
        return supabase.table("app_users").update({"referred_by": partner_id, "meta": current_meta}).eq("id", user_id).execute()

    try:
        await asyncio.to_thread(_patch_app_user)
    except Exception:
        logger.exception("Failed to patch app_users (non-fatal)")

    # 4) increment promo_codes.uses (best-effort)
    def _inc_promo_uses():
        # optimistic increment (no compare); you can add DB-side constraint if needed
        return supabase.table("promo_codes").update({"uses": current_uses + 1}).eq("id", promo_id).execute()

    try:
        if max_uses == 0 or (current_uses + 1) <= max_uses:
            await asyncio.to_thread(_inc_promo_uses)
    except Exception:
        logger.exception("Failed to increment promo_codes.uses (non-fatal)")

    # 5) insert partner_signups row (best-effort)
    def _insert_signup():
        payload = {
            "partner_id": partner_id,
            "promo_code_id": promo_id,
            "user_id": user_id,
            "existing_user": bool(existing_user)
        }
        return supabase.table("partner_signups").insert(payload).execute()

    try:
        await asyncio.to_thread(_insert_signup)
    except Exception:
        logger.exception("partner_signups insert failed (non-fatal)")

    logger.info("Applied promo %s for user %s (partner=%s)", promo_code, user_id, partner_id)
    return {"ok": True, "promo_id": promo_id, "partner_id": partner_id, "user_id": user_id}
