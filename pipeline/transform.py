"""Silver-layer transforms and feature enrichment."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict

LOGGER = logging.getLogger(__name__)
BRONZE_DIR = Path("bronze")


def load_fundamentals(ticker: str, date: str) -> Dict[str, float | None]:
    """Load bronze fundamentals for a ticker/date and map required silver features."""
    feature_defaults: Dict[str, float | None] = {
        "pe_ratio": None,
        "pb_ratio": None,
        "ps_ratio": None,
        "free_cash_flow": None,
        "debt_to_equity": None,
        "net_profit_margin": None,
        "revenue_growth_yoy": None,
        "latest_eps": None,
        "operating_cashflow_latest": None,
    }

    fundamentals_file = BRONZE_DIR / f"{ticker}_fundamentals_{date}.json"

    try:
        payload = json.loads(fundamentals_file.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.warning("Could not load fundamentals file '%s': %s", fundamentals_file, exc)
        return feature_defaults

    valuation = payload.get("valuation", {})
    derived = payload.get("derived_metrics", {})
    income_quarterly = payload.get("income_statement", {}).get("quarterly", [])
    cash_quarterly = payload.get("cash_flow", {}).get("quarterly", [])

    latest_income = _latest_period_record(income_quarterly)
    latest_cash = _latest_period_record(cash_quarterly)

    return {
        "pe_ratio": _to_float(valuation.get("pe_ratio")),
        "pb_ratio": _to_float(valuation.get("pb_ratio")),
        "ps_ratio": _to_float(valuation.get("ps_ratio")),
        "free_cash_flow": _to_float(derived.get("free_cash_flow")),
        "debt_to_equity": _to_float(derived.get("debt_to_equity")),
        "net_profit_margin": _to_float(derived.get("net_profit_margin")),
        "revenue_growth_yoy": _to_float(derived.get("revenue_growth_yoy")),
        "latest_eps": _to_float(latest_income.get("eps") if latest_income else None),
        "operating_cashflow_latest": _to_float(latest_cash.get("operating") if latest_cash else None),
    }


def _latest_period_record(records: list[Dict[str, Any]]) -> Dict[str, Any] | None:
    """Return the latest period record from a list."""
    if not records:
        return None
    return sorted(records, key=lambda item: str(item.get("period", "")), reverse=True)[0]


def _to_float(value: Any) -> float | None:
    """Safely convert values to float."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

