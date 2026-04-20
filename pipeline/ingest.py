"""Bronze ingestion orchestrator."""

from __future__ import annotations

import logging


def main() -> None:
    """Run bronze ingestion tasks in order."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.info("Starting bronze ingestion pipeline")

    # Step 1 and 2 placeholders are intentionally left as integration hooks.
    logging.info("Step 1 placeholder: fetch OHLCV prices from yfinance")
    logging.info("Step 2 placeholder: fetch news headlines from Serper")

    from ingest_fundamentals import main as ingest_fundamentals

    ingest_fundamentals()
    logging.info("Bronze ingestion pipeline complete")


if __name__ == "__main__":
    main()
