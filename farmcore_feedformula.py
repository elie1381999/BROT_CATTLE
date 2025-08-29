# farmcore_feedformula.py
import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Optional

from farmcore import supabase, async_get_user_by_telegram

LOG = logging.getLogger(__name__)

TABLE = "feed_formulas"  # new table we will use

# NOTE: Ensure you create the table in Supabase. Example SQL:
# CREATE TABLE public.feed_formulas (
#   id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
#   name text NOT NULL,
#   created_by uuid, -- app_users.id
#   farm_id uuid,
#   components jsonb NOT NULL, -- list of {feed_item_id, proportion}
#   created_at timestamptz DEFAULT now(),
#   updated_at timestamptz DEFAULT now(),
#   meta jsonb DEFAULT '{}'
# );

# ------------------------
# Helpers (blocking supabase calls wrapped in to_thread)
# ------------------------
async def _supabase_insert(table: str, payload: dict):
    def _fn():
        return supabase.table(table).insert(payload).execute()
    return await asyncio.to_thread(_fn)

async def _supabase_select(table: str, cols: str = "*", eq: Optional[List[tuple]] = None, _in: Optional[tuple] = None, single: bool = False):
    def _fn():
        qry = supabase.table(table).select(cols)
        if eq:
            for col, val in eq:
                qry = qry.eq(col, val)
        if _in:
            col, vals = _in
            qry = qry.in_(col, vals)
        if single:
            return qry.single().execute()
        return qry.execute()
    return await asyncio.to_thread(_fn)

async def _supabase_update(table: str, payload: dict, eq_col: str, eq_val):
    def _fn():
        return supabase.table(table).update(payload).eq(eq_col, eq_val).execute()
    return await asyncio.to_thread(_fn)

# ------------------------
# Public API
# ------------------------
async def async_create_formula(telegram_id: int, name: str, components: List[Dict]) -> Optional[dict]:
    """
    components: list of {"feed_item_id": "<uuid>", "proportion": <percent number 0-100>}
    Returns inserted row or None
    """
    try:
        user = await async_get_user_by_telegram(telegram_id)
        if not user:
            LOG.error("User not found for telegram_id=%s", telegram_id)
            return None
        user_id = user.get("id")
        farm_id = user.get("current_farm_id") or None

        payload = {
            "name": name,
            "created_by": user_id,
            "farm_id": farm_id,
            "components": components,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "meta": {}
        }
        res = await _supabase_insert(TABLE, payload)
        if res and getattr(res, "data", None):
            return res.data[0]
        return None
    except Exception:
        LOG.exception("async_create_formula failed")
        return None

async def async_list_formulas_by_user(telegram_id: int) -> List[Dict]:
    """
    Return list of formulas created by the telegram user.
    """
    try:
        user = await async_get_user_by_telegram(telegram_id)
        if not user:
            return []
        user_id = user.get("id")
        res = await _supabase_select(TABLE, cols="id,name,components,created_at", eq=[("created_by", user_id)])
        if res and getattr(res, "data", None):
            return res.data
        return []
    except Exception:
        LOG.exception("async_list_formulas_by_user failed")
        return []

async def async_get_formula(formula_id: str) -> Optional[Dict]:
    try:
        res = await _supabase_select(TABLE, cols="*", eq=[("id", formula_id)], single=True)
        if res and getattr(res, "data", None):
            return res.data
        return None
    except Exception:
        LOG.exception("async_get_formula failed")
        return None

async def async_find_feed_item_by_name(name: str):
    """
    Try to find a feed_item by partial name (case-insensitive).
    Returns single feed_item row or None.
    """
    try:
        def _fn():
            return supabase.table("feed_items").select("*").ilike("name", f"%{name}%").limit(1).execute()
        res = await asyncio.to_thread(_fn)
        if res and getattr(res, "data", None):
            return res.data[0]
        return None
    except Exception:
        LOG.exception("async_find_feed_item_by_name failed")
        return None

async def async_get_feed_items_by_ids(ids: List[str]):
    """
    Return list of feed_items for given ids.
    """
    if not ids:
        return []
    try:
        res = await _supabase_select("feed_items", cols="id,name,unit,cost_per_unit,nutrients", _in=("id", ids))
        if res and getattr(res, "data", None):
            return res.data
        return []
    except Exception:
        LOG.exception("async_get_feed_items_by_ids failed")
        return []

async def async_calculate_formula(formula_id: str, target_kg: float) -> Optional[Dict]:
    """
    Calculate component weights, cost and aggregated nutrients for a formula for a given total kg.
    Returns dict:
      {
        "components": [{"feed_item_id", "name", "proportion", "weight_kg", "cost", "cost_per_unit", "nutrients": {...}}],
        "total_cost": x,
        "nutrients": {...}
      }
    """
    try:
        formula = await async_get_formula(formula_id)
        if not formula:
            LOG.error("Formula not found %s", formula_id)
            return None

        comps = formula.get("components") or []
        ids = [c.get("feed_item_id") for c in comps if c.get("feed_item_id")]
        items = await async_get_feed_items_by_ids(ids)
        items_map = {it["id"]: it for it in items}

        result_comps = []
        total_cost = 0.0
        nutrients_agg = {}

        for c in comps:
            fid = c.get("feed_item_id")
            prop = float(c.get("proportion") or 0.0)
            if fid not in items_map:
                LOG.warning("Feed item id %s not found in feed_items table", fid)
                continue
            item = items_map[fid]
            weight = target_kg * (prop / 100.0)
            cost_per_unit = float(item.get("cost_per_unit") or 0.0)
            cost = weight * cost_per_unit
            total_cost += cost

            # nutrients is expected to be JSON like {"cp": 18.5, "nde": 2.5} where numbers are percent or units
            nutrients = item.get("nutrients") or {}
            # compute contribution: if nutrient stored as percent, convert to kg contribution = weight * (percent/100)
            contrib = {}
            for k, v in (nutrients.items() if isinstance(nutrients, dict) else []):
                try:
                    val = float(v)
                except Exception:
                    continue
                # contribution in kg equivalent
                kg_contrib = weight * (val / 100.0)
                contrib[k] = kg_contrib
                nutrients_agg[k] = nutrients_agg.get(k, 0.0) + kg_contrib

            result_comps.append({
                "feed_item_id": fid,
                "name": item.get("name"),
                "proportion": prop,
                "weight_kg": round(weight, 4),
                "cost": round(cost, 4),
                "cost_per_unit": cost_per_unit,
                "nutrients": contrib
            })

        return {
            "components": result_comps,
            "total_cost": round(total_cost, 4),
            "nutrients": {k: round(v, 4) for k, v in nutrients_agg.items()}
        }
    except Exception:
        LOG.exception("async_calculate_formula failed")
        return None
