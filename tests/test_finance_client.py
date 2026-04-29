import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.finance_client import DataFetchError, FinanceClient, LocalSnapshotStore


class _FakeSnapshotStore:
    def __init__(self, snapshot):
        self.snapshot = snapshot

    def get_latest_snapshot(self, ticker):
        if self.snapshot and self.snapshot.get("ticker") == ticker:
            return self.snapshot
        return None


class FinanceClientTests(unittest.TestCase):
    def test_retry_then_success_records_attempt_count(self):
        client = FinanceClient(max_retries=3, base_delay_seconds=0, max_jitter_seconds=0)
        attempts = {"count": 0}

        def fake_fetch_once(_ticker):
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise RuntimeError("transient failure")
            return {"ticker": "MSFT", "info": {}, "financial_statements": {}}

        with patch.object(client, "_fetch_with_timeout", side_effect=fake_fetch_once):
            snapshot = client.fetch_company_snapshot("MSFT")

        self.assertEqual(snapshot["fetch_metadata"]["attempt_count"], 3)
        self.assertEqual(snapshot["fetch_metadata"]["status"], "success")

    def test_raises_after_max_retries_without_fallback(self):
        client = FinanceClient(max_retries=2, base_delay_seconds=0, max_jitter_seconds=0, snapshot_store=_FakeSnapshotStore(None))

        with patch.object(client, "_fetch_with_timeout", side_effect=RuntimeError("down")):
            with self.assertRaises(DataFetchError):
                client.fetch_company_snapshot("MSFT")

    def test_uses_local_fallback_after_retries(self):
        fallback_snapshot = {"ticker": "MSFT", "info": {"regularMarketTime": 1710000000}, "financial_statements": {}}
        client = FinanceClient(
            max_retries=2,
            base_delay_seconds=0,
            max_jitter_seconds=0,
            snapshot_store=_FakeSnapshotStore(fallback_snapshot),
        )

        with patch.object(client, "_fetch_with_timeout", side_effect=RuntimeError("provider down")):
            snapshot = client.fetch_company_snapshot("MSFT")

        self.assertEqual(snapshot["fetch_metadata"]["status"], "fallback")
        self.assertEqual(snapshot["fetch_metadata"]["source"], "local_snapshot_cache")

    def test_local_snapshot_store_loads_latest_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            archive_root = Path(tmp_dir)
            old_file = archive_root / "2026-04-19" / "run_100000" / "raw_snapshots.json"
            new_file = archive_root / "2026-04-20" / "run_120000" / "raw_snapshots.json"
            old_file.parent.mkdir(parents=True, exist_ok=True)
            new_file.parent.mkdir(parents=True, exist_ok=True)
            old_file.write_text('[{"ticker":"MSFT","info":{"currency":"USD"}}]', encoding="utf-8")
            new_file.write_text('[{"ticker":"MSFT","info":{"currency":"EUR"}}]', encoding="utf-8")

            store = LocalSnapshotStore(archive_root=archive_root)
            snapshot = store.get_latest_snapshot("MSFT")

        self.assertEqual(snapshot["info"]["currency"], "EUR")


if __name__ == "__main__":
    unittest.main()
