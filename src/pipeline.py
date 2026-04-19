"""Stage 1 pipeline: fetch, normalize, group, and store company fundamentals."""

from __future__ import annotations

import json
from pathlib import Path
from typing import List

from src.finance_client import FinanceClient
from src.normalizer import build_normalized_record, group_by_business_context
from src.universe import as_dicts

OUTPUT_DIR = Path("data/output")
RAW_OUTPUT_FILE = OUTPUT_DIR / "companies_raw.json"
GROUPED_OUTPUT_FILE = OUTPUT_DIR / "companies_by_category.json"


def run_pipeline() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    companies = as_dicts()
    client = FinanceClient()

    normalized_records: List[dict] = []

    for company in companies:
        ticker = company["ticker"]
        print(f"[INFO] Fetching {ticker} ...")
        try:
            snapshot = client.fetch_company_snapshot(ticker)
            normalized_records.append(build_normalized_record(company, snapshot))
        except Exception as exc:  # pragmatic stage-1 tolerance for API errors
            print(f"[WARN] Failed to fetch {ticker}: {exc}")

    grouped = group_by_business_context(normalized_records)

    RAW_OUTPUT_FILE.write_text(json.dumps(normalized_records, indent=2), encoding="utf-8")
    GROUPED_OUTPUT_FILE.write_text(json.dumps(grouped, indent=2), encoding="utf-8")

    print(f"[DONE] Wrote {RAW_OUTPUT_FILE}")
    print(f"[DONE] Wrote {GROUPED_OUTPUT_FILE}")


if __name__ == "__main__":
    run_pipeline()
