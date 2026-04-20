"""Stage 1 pipeline: fetch, normalize, group, and store company fundamentals."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from src.finance_client import DataFetchError, FinanceClient
from src.normalizer import build_normalized_record, group_by_business_context
from src.universe import as_dicts

OUTPUT_DIR = Path("data/output")
RAW_OUTPUT_FILE = OUTPUT_DIR / "companies_raw.json"
GROUPED_OUTPUT_FILE = OUTPUT_DIR / "companies_by_category.json"
SUMMARY_OUTPUT_FILE = OUTPUT_DIR / "pipeline_run_summary.json"


def run_pipeline() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    run_started_at = datetime.now(timezone.utc)

    companies = as_dicts()
    client = FinanceClient()

    normalized_records: List[dict] = []
    fetch_results: List[dict] = []

    for company in companies:
        ticker = company["ticker"]
        print(f"[INFO] Fetching {ticker} ...")
        try:
            snapshot = client.fetch_company_snapshot(ticker)
            normalized_records.append(build_normalized_record(company, snapshot))
            fetch_results.append(
                {
                    "ticker": ticker,
                    "status": "success",
                    "attempt_count": snapshot.get("fetch_metadata", {}).get("attempt_count"),
                    "error": None,
                }
            )
        except DataFetchError as exc:
            fetch_results.append(
                {
                    "ticker": ticker,
                    "status": "failed",
                    "attempt_count": exc.attempts,
                    "error": str(exc.last_error),
                }
            )
            print(f"[WARN] Failed to fetch {ticker}: {exc}")

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

    print(f"[DONE] Wrote {RAW_OUTPUT_FILE}")
    print(f"[DONE] Wrote {GROUPED_OUTPUT_FILE}")
    print(f"[DONE] Wrote {SUMMARY_OUTPUT_FILE}")


if __name__ == "__main__":
    run_pipeline()
