"""Market data retrieval client backed by Yahoo Finance (yfinance)."""

from __future__ import annotations

import json
import logging
import random
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import yfinance as yf

LOGGER = logging.getLogger(__name__)


class DataFetchError(RuntimeError):
    """Raised when a company snapshot cannot be fetched after retries."""

    def __init__(self, ticker: str, attempts: int, last_error: Exception):
        self.ticker = ticker
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(f"{ticker} fetch failed after {attempts} attempts: {last_error}")


class LocalSnapshotStore:
    """Read latest raw snapshots from local archive as a fallback source."""

    def __init__(self, archive_root: Path = Path("data/archive")) -> None:
        self.archive_root = archive_root

    def get_latest_snapshot(self, ticker: str) -> Dict[str, Any] | None:
        if not self.archive_root.exists():
            return None

        candidates = sorted(self.archive_root.glob("*/run_*/raw_snapshots.json"), reverse=True)
        for snapshot_file in candidates:
            try:
                payload = json.loads(snapshot_file.read_text(encoding="utf-8"))
            except Exception:  # best-effort fallback loading
                continue
            if not isinstance(payload, list):
                continue
            for snapshot in payload:
                if snapshot.get("ticker") == ticker:
                    return snapshot
        return None


class FinanceClient:
    """Fetch company fundamentals and statements from Yahoo Finance."""

    def __init__(
        self,
        max_retries: int = 3,
        base_delay_seconds: float = 1.0,
        backoff_multiplier: float = 2.0,
        max_jitter_seconds: float = 0.25,
        request_timeout_seconds: float = 20.0,
        snapshot_store: LocalSnapshotStore | None = None,
    ) -> None:
        self.max_retries = max_retries
        self.base_delay_seconds = base_delay_seconds
        self.backoff_multiplier = backoff_multiplier
        self.max_jitter_seconds = max_jitter_seconds
        self.request_timeout_seconds = request_timeout_seconds
        self.snapshot_store = snapshot_store or LocalSnapshotStore()

    def fetch_company_snapshot(self, ticker: str) -> Dict[str, Any]:
        last_error: Exception | None = None
        start = time.perf_counter()

        for attempt in range(1, self.max_retries + 1):
            try:
                snapshot = self._fetch_with_timeout(ticker)
                snapshot["fetch_metadata"] = {
                    "status": "success",
                    "attempt_count": attempt,
                    "source": "yfinance",
                    "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                    "fetch_duration_seconds": round(time.perf_counter() - start, 3),
                    "as_of_utc": _extract_as_of(snapshot.get("info", {})),
                }
                return snapshot
            except Exception as exc:
                last_error = exc
                LOGGER.warning(
                    json.dumps(
                        {
                            "event": "fetch_attempt_failed",
                            "ticker": ticker,
                            "attempt": attempt,
                            "max_retries": self.max_retries,
                            "error": str(exc),
                        }
                    )
                )
                if attempt == self.max_retries:
                    break
                delay_seconds = (self.base_delay_seconds * (self.backoff_multiplier ** (attempt - 1))) + random.uniform(
                    0, self.max_jitter_seconds
                )
                time.sleep(delay_seconds)

        fallback = self.snapshot_store.get_latest_snapshot(ticker) if self.snapshot_store else None
        if fallback:
            fallback["fetch_metadata"] = {
                "status": "fallback",
                "attempt_count": self.max_retries,
                "source": "local_snapshot_cache",
                "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                "fetch_duration_seconds": round(time.perf_counter() - start, 3),
                "as_of_utc": _extract_as_of(fallback.get("info", {})),
                "fallback_reason": str(last_error) if last_error else "unknown",
            }
            return fallback

        raise DataFetchError(ticker=ticker, attempts=self.max_retries, last_error=last_error or RuntimeError("unknown error"))

    def _fetch_with_timeout(self, ticker: str) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        error: Exception | None = None

        def _runner() -> None:
            nonlocal result, error
            try:
                result = self._fetch_snapshot_once(ticker)
            except Exception as exc:
                error = exc

        thread = threading.Thread(target=_runner, daemon=True)
        thread.start()
        thread.join(timeout=self.request_timeout_seconds)

        if thread.is_alive():
            raise TimeoutError(f"Timeout after {self.request_timeout_seconds}s for ticker {ticker}")
        if error is not None:
            raise error
        return result

    def _fetch_snapshot_once(self, ticker: str) -> Dict[str, Any]:
        stock = yf.Ticker(ticker)
        info = stock.info or {}

        return {
            "ticker": ticker,
            "info": {
                "market_cap": info.get("marketCap"),
                "enterprise_value": info.get("enterpriseValue"),
                "trailing_pe": info.get("trailingPE"),
                "forward_pe": info.get("forwardPE"),
                "price_to_sales_ttm": info.get("priceToSalesTrailing12Months"),
                "peg_ratio": info.get("pegRatio"),
                "gross_margins": info.get("grossMargins"),
                "operating_margins": info.get("operatingMargins"),
                "profit_margins": info.get("profitMargins"),
                "revenue_growth": info.get("revenueGrowth"),
                "earnings_growth": info.get("earningsGrowth"),
                "return_on_equity": info.get("returnOnEquity"),
                "return_on_assets": info.get("returnOnAssets"),
                "debt_to_equity": info.get("debtToEquity"),
                "current_ratio": info.get("currentRatio"),
                "quick_ratio": info.get("quickRatio"),
                "beta": info.get("beta"),
                "fifty_two_week_high": info.get("fiftyTwoWeekHigh"),
                "fifty_two_week_low": info.get("fiftyTwoWeekLow"),
                "currency": info.get("currency"),
                "exchange": info.get("exchange"),
                "long_business_summary": info.get("longBusinessSummary"),
            },
            "financial_statements": {
                "income_statement": _df_to_records(stock.financials),
                "balance_sheet": _df_to_records(stock.balance_sheet),
                "cash_flow": _df_to_records(stock.cashflow),
            },
        }


def _extract_as_of(info: Dict[str, Any]) -> str | None:
    epoch_seconds = info.get("regularMarketTime")
    if epoch_seconds is None:
        return None
    try:
        return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).isoformat()
    except Exception:
        return None


def _df_to_records(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """Convert statement dataframe into JSON-safe nested dictionary."""
    if df is None or df.empty:
        return {}

    safe_df = df.copy()
    safe_df.columns = [str(col) for col in safe_df.columns]
    safe_df.index = [str(idx) for idx in safe_df.index]

    data = safe_df.to_dict()
    normalized: Dict[str, Dict[str, Any]] = {}

    for period, metrics in data.items():
        normalized[period] = {}
        for metric_name, value in metrics.items():
            if pd.isna(value):
                normalized[period][metric_name] = None
            else:
                normalized[period][metric_name] = value.item() if hasattr(value, "item") else value

    return normalized
