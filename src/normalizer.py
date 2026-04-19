"""Normalization and category grouping utilities."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List


def market_cap_bucket(market_cap: int | float | None) -> str:
    """Map market cap to human-friendly bucket."""
    if market_cap is None:
        return "unknown"
    if market_cap >= 10_000_000_000:
        return "large"
    if market_cap >= 2_000_000_000:
        return "mid"
    return "small"


def build_normalized_record(company: Dict[str, Any], snapshot: Dict[str, Any]) -> Dict[str, Any]:
    info = snapshot.get("info", {})
    market_cap = info.get("market_cap")

    return {
        "ticker": company["ticker"],
        "name": company["name"],
        "sector_family": company["sector_family"],
        "sub_vertical": company["sub_vertical"],
        "size_bucket": market_cap_bucket(market_cap),
        "metrics": {
            "market_cap": market_cap,
            "trailing_pe": info.get("trailing_pe"),
            "forward_pe": info.get("forward_pe"),
            "price_to_sales_ttm": info.get("price_to_sales_ttm"),
            "peg_ratio": info.get("peg_ratio"),
            "gross_margins": info.get("gross_margins"),
            "operating_margins": info.get("operating_margins"),
            "profit_margins": info.get("profit_margins"),
            "revenue_growth": info.get("revenue_growth"),
            "earnings_growth": info.get("earnings_growth"),
            "return_on_equity": info.get("return_on_equity"),
            "return_on_assets": info.get("return_on_assets"),
            "debt_to_equity": info.get("debt_to_equity"),
            "current_ratio": info.get("current_ratio"),
            "quick_ratio": info.get("quick_ratio"),
            "beta": info.get("beta"),
        },
        "metadata": {
            "exchange": info.get("exchange"),
            "currency": info.get("currency"),
            "fifty_two_week_high": info.get("fifty_two_week_high"),
            "fifty_two_week_low": info.get("fifty_two_week_low"),
        },
        "financial_statements": snapshot.get("financial_statements", {}),
    }


def group_by_business_context(records: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    grouped: Dict[str, Any] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for record in records:
        sector = record["sector_family"]
        size = record["size_bucket"]
        vertical = record["sub_vertical"]
        grouped[sector][size][vertical].append(record)

    return _to_plain_dict(grouped)


def _to_plain_dict(obj: Any) -> Any:
    if isinstance(obj, defaultdict):
        return {k: _to_plain_dict(v) for k, v in obj.items()}
    if isinstance(obj, dict):
        return {k: _to_plain_dict(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_plain_dict(item) for item in obj]
    return obj
