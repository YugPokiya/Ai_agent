import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline import ingest_fundamentals


class IngestFundamentalsTests(unittest.TestCase):
    def test_detect_structure_shape_a(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "fundamentals_raw.json"
            input_path.write_text(
                json.dumps(
                    [
                        {"ticker": "MSFT", "pe_ratio": 30},
                        {"ticker": "GOOGL", "pe_ratio": 25},
                    ]
                ),
                encoding="utf-8",
            )
            with patch("pipeline.ingest_fundamentals.CONFIG_TICKERS", ["MSFT", "GOOGL"]):
                normalized = ingest_fundamentals.detect_structure(str(input_path))
        self.assertEqual(set(normalized.keys()), {"MSFT", "GOOGL"})

    def test_detect_structure_shape_b(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "fundamentals_raw.json"
            input_path.write_text(json.dumps({"MSFT": {"pe_ratio": 30}}), encoding="utf-8")
            with patch("pipeline.ingest_fundamentals.CONFIG_TICKERS", ["MSFT"]):
                normalized = ingest_fundamentals.detect_structure(str(input_path))
        self.assertIn("MSFT", normalized)

    def test_validate_fundamental_computes_derived_metrics(self):
        payload = {
            "pe_ratio": 22,
            "ps_ratio": 5,
            "pb_ratio": 10,
            "income_annual": [
                {"period": "2024", "revenue": 120, "net_income": 12, "eps": 6},
                {"period": "2023", "revenue": 100, "net_income": 10, "eps": 5},
            ],
            "income_quarterly": [{"period": "2024-Q4", "revenue": 35, "net_income": 4, "eps": 1}],
            "cashflow_annual": [{"period": "2024", "operating": 60, "investing": -20, "financing": -10}],
            "cashflow_quarterly": [{"period": "2024-Q4", "operating": 20, "investing": -5, "financing": -3}],
            "balancesheet_annual": [
                {"period": "2024", "total_assets": 300, "total_liabilities": 120, "total_equity": 180}
            ],
            "balancesheet_quarterly": [
                {"period": "2024-Q4", "total_assets": 305, "total_liabilities": 125, "total_equity": 180}
            ],
        }

        cleaned = ingest_fundamentals.validate_fundamental("MSFT", payload)
        self.assertEqual(cleaned["derived_metrics"]["free_cash_flow"], 15)
        self.assertAlmostEqual(cleaned["derived_metrics"]["debt_to_equity"], 125 / 180)
        self.assertAlmostEqual(cleaned["derived_metrics"]["net_profit_margin"], 4 / 35)
        self.assertAlmostEqual(cleaned["derived_metrics"]["revenue_growth_yoy"], 0.2)

    def test_save_bronze_fundamentals_writes_flagged_watchlist(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            bronze_dir = Path(tmp_dir) / "bronze"
            validated = {
                "MSFT": {
                    "valuation": {"pe_ratio": 20, "ps_ratio": 5, "pb_ratio": 4},
                    "income_statement": {"annual": [], "quarterly": []},
                    "cash_flow": {"annual": [], "quarterly": []},
                    "balance_sheet": {"annual": [], "quarterly": []},
                    "derived_metrics": {},
                    "validation_warnings": ["a", "b", "c", "d"],
                }
            }
            with patch.object(ingest_fundamentals, "BRONZE_DIR", bronze_dir):
                ingest_fundamentals.save_bronze_fundamentals(validated, "2026-04-20")

            self.assertTrue((bronze_dir / "MSFT_fundamentals_2026-04-20.json").exists())
            self.assertTrue((bronze_dir / "flagged_2026-04-20.json").exists())


if __name__ == "__main__":
    unittest.main()
