"""Bronze fundamentals ingestion from a local consolidated JSON file."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.universe import TARGET_COMPANIES

LOGGER = logging.getLogger(__name__)
DEFAULT_INPUT_FILE = Path("fundamentals_raw.json")
BRONZE_DIR = Path("bronze")
CONFIG_TICKERS = [company.ticker for company in TARGET_COMPANIES]


def detect_structure(filepath: str) -> Dict[str, Dict[str, Any]]:
    """Load fundamentals JSON and normalize supported shapes to {ticker: payload}."""
    normalized: Dict[str, Dict[str, Any]] = {}
    path = Path(filepath)

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.exception("Failed to load fundamentals file '%s': %s", path, exc)
        return normalized

    detected_shape = "unknown"

    try:
        if isinstance(payload, list):
            detected_shape = "A"
            for entry in payload:
                if not isinstance(entry, dict):
                    LOGGER.warning("Skipping non-dict entry in list payload: %s", entry)
                    continue
                raw_ticker = entry.get("ticker")
                if not raw_ticker:
                    LOGGER.warning("Skipping entry without ticker: %s", entry)
                    continue
                ticker = str(raw_ticker).upper()
                sanitized = dict(entry)
                sanitized.pop("ticker", None)
                normalized[ticker] = sanitized
        elif isinstance(payload, dict):
            detected_shape = "B"
            for raw_ticker, entry in payload.items():
                if not isinstance(entry, dict):
                    LOGGER.warning("Skipping non-dict company payload for ticker %s", raw_ticker)
                    continue
                normalized[str(raw_ticker).upper()] = dict(entry)
        else:
            LOGGER.error("Unsupported JSON structure in '%s'. Expected list or dict.", path)
            return {}
    except Exception as exc:
        LOGGER.exception("Error while normalizing structure from '%s': %s", path, exc)
        return {}

    config_tickers = {ticker.upper() for ticker in CONFIG_TICKERS}
    file_tickers = set(normalized.keys())
    extra_tickers = sorted(file_tickers - config_tickers)
    missing_tickers = sorted(config_tickers - file_tickers)

    LOGGER.info("Detected shape %s with %d companies from '%s'", detected_shape, len(normalized), path.name)
    if extra_tickers:
        LOGGER.warning("Tickers found in file but missing in config ticker universe: %s", extra_tickers)
    if missing_tickers:
        LOGGER.warning("Tickers in config ticker universe missing from file: %s", missing_tickers)

    return normalized


def validate_fundamental(ticker: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and clean one company's fundamentals, preserving schema with nulls."""
    warnings: List[str] = []

    def to_float(value: Any, field_name: str) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            warnings.append(f"{field_name} has non-numeric value '{value}' and was set to null")
            return None

    def normalize_records(
        records: Any,
        field_name: str,
        required_keys: Iterable[str],
    ) -> List[Dict[str, Any]]:
        if records is None:
            warnings.append(f"{field_name} missing; defaulted to empty list")
            return []
        if not isinstance(records, list):
            warnings.append(f"{field_name} is not a list; defaulted to empty list")
            return []

        clean_records: List[Dict[str, Any]] = []
        for idx, record in enumerate(records):
            if not isinstance(record, dict):
                warnings.append(f"{field_name}[{idx}] is not an object and was skipped")
                continue

            cleaned = {"period": str(record.get("period")) if record.get("period") is not None else None}
            if cleaned["period"] is None:
                warnings.append(f"{field_name}[{idx}] missing period")
            for key in required_keys:
                value = to_float(record.get(key), f"{field_name}[{idx}].{key}")
                if key not in record:
                    warnings.append(f"{field_name}[{idx}] missing {key}; defaulted to null")
                cleaned[key] = value
            clean_records.append(cleaned)
        return clean_records

    try:
        cleaned = {
            "valuation": {
                "pe_ratio": to_float(data.get("pe_ratio"), "pe_ratio"),
                "ps_ratio": to_float(data.get("ps_ratio"), "ps_ratio"),
                "pb_ratio": to_float(data.get("pb_ratio"), "pb_ratio"),
            },
            "income_statement": {
                "annual": normalize_records(
                    data.get("income_annual"),
                    "income_annual",
                    required_keys=("revenue", "net_income", "eps"),
                ),
                "quarterly": normalize_records(
                    data.get("income_quarterly"),
                    "income_quarterly",
                    required_keys=("revenue", "net_income", "eps"),
                ),
            },
            "cash_flow": {
                "annual": normalize_records(
                    data.get("cashflow_annual"),
                    "cashflow_annual",
                    required_keys=("operating", "investing", "financing"),
                ),
                "quarterly": normalize_records(
                    data.get("cashflow_quarterly"),
                    "cashflow_quarterly",
                    required_keys=("operating", "investing", "financing"),
                ),
            },
            "balance_sheet": {
                "annual": normalize_records(
                    data.get("balancesheet_annual"),
                    "balancesheet_annual",
                    required_keys=("total_assets", "total_liabilities", "total_equity"),
                ),
                "quarterly": normalize_records(
                    data.get("balancesheet_quarterly"),
                    "balancesheet_quarterly",
                    required_keys=("total_assets", "total_liabilities", "total_equity"),
                ),
            },
        }
    except Exception as exc:
        LOGGER.exception("Unexpected error validating ticker %s: %s", ticker, exc)
        cleaned = {
            "valuation": {"pe_ratio": None, "ps_ratio": None, "pb_ratio": None},
            "income_statement": {"annual": [], "quarterly": []},
            "cash_flow": {"annual": [], "quarterly": []},
            "balance_sheet": {"annual": [], "quarterly": []},
        }
        warnings.append(f"validation_error: {exc}")

    derived = _compute_derived_metrics(cleaned, warnings)
    cleaned["derived_metrics"] = derived
    cleaned["validation_warnings"] = warnings
    return cleaned


def save_bronze_fundamentals(normalized: Dict[str, Dict[str, Any]], run_date: str) -> None:
    """Write one bronze fundamentals file per ticker and flagged watchlist for warning-heavy records."""
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    ingested_at = datetime.utcnow().isoformat(timespec="seconds")
    source_filename = DEFAULT_INPUT_FILE.name
    flagged_rows: List[Dict[str, Any]] = []

    for ticker, validated in normalized.items():
        try:
            output_file = BRONZE_DIR / f"{ticker}_fundamentals_{run_date}.json"
            if output_file.exists():
                LOGGER.info("%s already ingested for %s; skipping", ticker, run_date)
                continue

            payload = {
                "ticker": ticker,
                "ingested_at": ingested_at,
                "source": "local_file",
                "source_filename": source_filename,
                "run_date": run_date,
                "validation_warnings": validated.get("validation_warnings", []),
                "valuation": validated.get("valuation", {}),
                "income_statement": validated.get("income_statement", {"annual": [], "quarterly": []}),
                "cash_flow": validated.get("cash_flow", {"annual": [], "quarterly": []}),
                "balance_sheet": validated.get("balance_sheet", {"annual": [], "quarterly": []}),
                "derived_metrics": validated.get("derived_metrics", {}),
            }

            output_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            LOGGER.info("Saved bronze fundamentals: %s", output_file)

            if len(payload["validation_warnings"]) > 3:
                flagged_rows.append(
                    {
                        "ticker": ticker,
                        "run_date": run_date,
                        "warning_count": len(payload["validation_warnings"]),
                        "validation_warnings": payload["validation_warnings"],
                    }
                )
        except Exception as exc:
            LOGGER.exception("Failed to save ticker '%s' fundamentals: %s", ticker, exc)

    if flagged_rows:
        flagged_file = BRONZE_DIR / f"flagged_{run_date}.json"
        existing: List[Dict[str, Any]] = []
        try:
            if flagged_file.exists():
                loaded = json.loads(flagged_file.read_text(encoding="utf-8"))
                if isinstance(loaded, list):
                    existing = loaded
        except Exception as exc:
            LOGGER.exception("Could not read existing flagged file '%s': %s", flagged_file, exc)
        flagged_file.write_text(json.dumps(existing + flagged_rows, indent=2), encoding="utf-8")
        LOGGER.warning("Saved %d flagged records to %s", len(flagged_rows), flagged_file)


def _compute_derived_metrics(cleaned: Dict[str, Any], warnings: List[str]) -> Dict[str, float | None]:
    """Compute derived metrics from the latest available annual/quarterly records."""
    latest_cash = _latest_record(cleaned["cash_flow"]["quarterly"]) or _latest_record(cleaned["cash_flow"]["annual"])
    latest_bs = _latest_record(cleaned["balance_sheet"]["quarterly"]) or _latest_record(cleaned["balance_sheet"]["annual"])
    latest_income = _latest_record(cleaned["income_statement"]["quarterly"]) or _latest_record(cleaned["income_statement"]["annual"])
    annual_income_sorted = _sort_records_desc(cleaned["income_statement"]["annual"])

    free_cash_flow = None
    if latest_cash and latest_cash.get("operating") is not None and latest_cash.get("investing") is not None:
        free_cash_flow = latest_cash["operating"] + latest_cash["investing"]
    else:
        warnings.append("Unable to compute free_cash_flow due to missing operating/investing cash flow")

    debt_to_equity = None
    if latest_bs and latest_bs.get("total_liabilities") is not None and latest_bs.get("total_equity") not in (None, 0):
        debt_to_equity = latest_bs["total_liabilities"] / latest_bs["total_equity"]
    else:
        warnings.append("Unable to compute debt_to_equity due to missing or zero total_equity")

    net_profit_margin = None
    if latest_income and latest_income.get("net_income") is not None and latest_income.get("revenue") not in (None, 0):
        net_profit_margin = latest_income["net_income"] / latest_income["revenue"]
    else:
        warnings.append("Unable to compute net_profit_margin due to missing or zero revenue")

    revenue_growth_yoy = None
    if len(annual_income_sorted) >= 2:
        latest_revenue = annual_income_sorted[0].get("revenue")
        prev_revenue = annual_income_sorted[1].get("revenue")
        if latest_revenue is not None and prev_revenue not in (None, 0):
            revenue_growth_yoy = (latest_revenue - prev_revenue) / prev_revenue
        else:
            warnings.append("Unable to compute revenue_growth_yoy due to missing or zero previous annual revenue")
    else:
        warnings.append("Unable to compute revenue_growth_yoy due to insufficient annual records")

    return {
        "free_cash_flow": free_cash_flow,
        "debt_to_equity": debt_to_equity,
        "net_profit_margin": net_profit_margin,
        "revenue_growth_yoy": revenue_growth_yoy,
    }


def _sort_records_desc(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort period records descending by parsed period value."""
    return sorted(records, key=lambda rec: _period_sort_key(rec.get("period")), reverse=True)


def _latest_record(records: List[Dict[str, Any]]) -> Dict[str, Any] | None:
    """Return latest record from a period list."""
    if not records:
        return None
    return _sort_records_desc(records)[0]


def _period_sort_key(period: Any) -> Tuple[int, int]:
    """Convert period strings like 2024 or 2024-Q1 into sortable keys."""
    if period is None:
        return (0, 0)
    text = str(period).upper().replace(" ", "")
    try:
        if "-Q" in text:
            year_text, quarter_text = text.split("-Q", maxsplit=1)
            return (int(year_text), int(quarter_text))
        return (int(text), 0)
    except Exception:
        return (0, 0)


def main() -> None:
    """Orchestrate local-file fundamentals ingestion into bronze ticker files."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run_date = datetime.today().strftime("%Y-%m-%d")
    normalized = detect_structure(str(DEFAULT_INPUT_FILE))

    validated: Dict[str, Dict[str, Any]] = {}
    for ticker, data in normalized.items():
        try:
            validated[ticker] = validate_fundamental(ticker, data)
        except Exception as exc:
            LOGGER.exception("Validation crashed for %s: %s", ticker, exc)
            validated[ticker] = validate_fundamental(ticker, {})
            validated[ticker]["validation_warnings"].append(f"fatal_validation_error: {exc}")

    save_bronze_fundamentals(validated, run_date)

    print(f"[Bronze Fundamentals] {len(validated)} tickers processed")
    print("Files saved to: bronze/")
    total_warnings = sum(len(v.get("validation_warnings", [])) for v in validated.values())
    print(f"Total validation warnings: {total_warnings}")


if __name__ == "__main__":
    main()
