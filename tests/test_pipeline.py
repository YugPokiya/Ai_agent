import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src import pipeline


class _FakeFinanceClient:
    def fetch_company_snapshot(self, ticker):
        return {
            "ticker": ticker,
            "info": {"market_cap": 15_000_000_000, "exchange": "NASDAQ", "currency": "USD"},
            "financial_statements": {},
            "fetch_metadata": {
                "status": "success",
                "source": "yfinance",
                "attempt_count": 1,
                "fetch_duration_seconds": 0.1,
                "as_of_utc": "2026-04-20T00:00:00+00:00",
            },
        }


class PipelineTests(unittest.TestCase):
    def test_pipeline_writes_outputs_and_archive_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            output_dir = root / "output"
            archive_dir = root / "archive"

            with (
                patch.object(pipeline, "OUTPUT_DIR", output_dir),
                patch.object(pipeline, "ARCHIVE_DIR", archive_dir),
                patch.object(pipeline, "RAW_OUTPUT_FILE", output_dir / "companies_raw.json"),
                patch.object(pipeline, "GROUPED_OUTPUT_FILE", output_dir / "companies_by_category.json"),
                patch.object(pipeline, "SUMMARY_OUTPUT_FILE", output_dir / "pipeline_run_summary.json"),
                patch.object(pipeline, "FinanceClient", _FakeFinanceClient),
            ):
                pipeline.run_pipeline()

            self.assertTrue((output_dir / "companies_raw.json").exists())
            self.assertTrue((output_dir / "companies_by_category.json").exists())
            self.assertTrue((output_dir / "pipeline_run_summary.json").exists())

            run_dirs = list(archive_dir.glob("*/run_*"))
            self.assertEqual(len(run_dirs), 1)
            run_dir = run_dirs[0]
            self.assertTrue((run_dir / "raw_snapshots.json").exists())
            self.assertTrue((run_dir / "normalized_records.json").exists())
            self.assertTrue((run_dir / "grouped_records.json").exists())
            self.assertTrue((run_dir / "run_summary.json").exists())

            summary = json.loads((run_dir / "run_summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["failed_count"], 0)
            self.assertGreater(summary["success_count"], 0)


if __name__ == "__main__":
    unittest.main()
