"""Stage 1 pipeline: fetch, normalize, group, and store company fundamentals."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from src.finance_client import DataFetchError, FinanceClient
from src.normalizer import build_normalized_record, group_by_business_context
from src.universe import as_dicts

OUTPUT_DIR = Path("data/output")
ARCHIVE_DIR = Path("data/archive")
RAW_OUTPUT_FILE = OUTPUT_DIR / "companies_raw.json"
GROUPED_OUTPUT_FILE = OUTPUT_DIR / "companies_by_category.json"
SUMMARY_OUTPUT_FILE = OUTPUT_DIR / "pipeline_run_summary.json"
LOGGER = logging.getLogger(__name__)


def run_pipeline() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run_started_at = datetime.now(timezone.utc)
    run_date_dir = ARCHIVE_DIR / run_started_at.strftime("%Y-%m-%d")
    run_dir = run_date_dir / f"run_{run_started_at.strftime('%H%M%S')}"
    run_dir.mkdir(parents=True, exist_ok=True)

    companies = as_dicts()
    client = FinanceClient()

    normalized_records: List[dict] = []
    fetch_results: List[dict] = []
    raw_snapshots: List[dict] = []

    for company in companies:
        ticker = company["ticker"]
        print(f"[INFO] Fetching {ticker} ...")
        try:
            snapshot = client.fetch_company_snapshot(ticker)
            normalized_records.append(build_normalized_record(company, snapshot))
            raw_snapshots.append(snapshot)
            metadata = snapshot.get("fetch_metadata", {})
            fetch_results.append(
                {
                    "ticker": ticker,
                    "status": metadata.get("status", "success"),
                    "source": metadata.get("source"),
                    "attempt_count": metadata.get("attempt_count"),
                    "fetch_duration_seconds": metadata.get("fetch_duration_seconds"),
                    "as_of_utc": metadata.get("as_of_utc"),
                    "error": None,
                }
            )
            LOGGER.info(
                json.dumps(
                    {
                        "event": "ticker_fetch_complete",
                        "ticker": ticker,
                        "status": metadata.get("status", "success"),
                        "source": metadata.get("source"),
                        "attempt_count": metadata.get("attempt_count"),
                    }
                )
            )
        except DataFetchError as exc:
            fetch_results.append(
                {
                    "ticker": ticker,
                    "status": "failed",
                    "source": None,
                    "attempt_count": exc.attempts,
                    "fetch_duration_seconds": None,
                    "as_of_utc": None,
                    "error": str(exc.last_error),
                }
            )
            print(f"[WARN] Failed to fetch {ticker}: {exc}")
            LOGGER.error(
                json.dumps(
                    {
                        "event": "ticker_fetch_failed",
                        "ticker": ticker,
                        "attempt_count": exc.attempts,
                        "error": str(exc.last_error),
                    }
                )
            )

    grouped = group_by_business_context(normalized_records)
    failed_count = len([result for result in fetch_results if result["status"] == "failed"])
    run_summary = {
        "run_started_at_utc": run_started_at.isoformat(),
        "run_finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_companies": len(companies),
        "success_count": len(normalized_records),
        "failed_count": failed_count,
        "fetch_results": fetch_results,
    }

    RAW_OUTPUT_FILE.write_text(json.dumps(normalized_records, indent=2), encoding="utf-8")
    GROUPED_OUTPUT_FILE.write_text(json.dumps(grouped, indent=2), encoding="utf-8")
    SUMMARY_OUTPUT_FILE.write_text(json.dumps(run_summary, indent=2), encoding="utf-8")
    (run_dir / "raw_snapshots.json").write_text(json.dumps(raw_snapshots, indent=2), encoding="utf-8")
    (run_dir / "normalized_records.json").write_text(json.dumps(normalized_records, indent=2), encoding="utf-8")
    (run_dir / "grouped_records.json").write_text(json.dumps(grouped, indent=2), encoding="utf-8")
    (run_dir / "run_summary.json").write_text(json.dumps(run_summary, indent=2), encoding="utf-8")

    print(f"[DONE] Wrote {RAW_OUTPUT_FILE}")
    print(f"[DONE] Wrote {GROUPED_OUTPUT_FILE}")
    print(f"[DONE] Wrote {SUMMARY_OUTPUT_FILE}")
    print(f"[DONE] Archived run artifacts in {run_dir}")


if __name__ == "__main__":
    run_pipeline()
