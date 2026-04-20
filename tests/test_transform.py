import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline import transform


class TransformTests(unittest.TestCase):
    def test_load_fundamentals_maps_expected_feature_fields(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            bronze_dir = Path(tmp_dir)
            fundamentals_file = bronze_dir / "MSFT_fundamentals_2026-04-20.json"
            fundamentals_file.write_text(
                json.dumps(
                    {
                        "valuation": {"pe_ratio": 30, "pb_ratio": 12, "ps_ratio": 8},
                        "derived_metrics": {
                            "free_cash_flow": 100,
                            "debt_to_equity": 1.2,
                            "net_profit_margin": 0.22,
                            "revenue_growth_yoy": 0.1,
                        },
                        "income_statement": {"quarterly": [{"period": "2026-Q1", "eps": 3.5}]},
                        "cash_flow": {"quarterly": [{"period": "2026-Q1", "operating": 40}]},
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(transform, "BRONZE_DIR", bronze_dir):
                features = transform.load_fundamentals("MSFT", "2026-04-20")

        self.assertEqual(features["pe_ratio"], 30.0)
        self.assertEqual(features["latest_eps"], 3.5)
        self.assertEqual(features["operating_cashflow_latest"], 40.0)


if __name__ == "__main__":
    unittest.main()

