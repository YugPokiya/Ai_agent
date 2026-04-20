# Autonomous Multi-Agent Market Analyst (Stage 1)

This repository now contains a **Stage 1 foundation** for your market analyst project.

Stage 1 focuses on:

1. Building a configurable universe of companies (large-cap + emerging tech small-caps).
2. Pulling core valuation + financial metrics (`PE`, `PS`, margins, debt, growth, etc.).
3. Capturing basic report data (income statement, balance sheet, cash flow).
4. Organizing companies by business context:
   - Sector family (`tech`, `mining`, etc.)
   - Size bucket (`large`, `mid`, `small`)
   - Sub-vertical (`cybersec`, `ml`, `database_as_service`, `paas`, `iaas`, `lidar`, `quantum_computing`, `computer_vision`)
5. Persisting normalized outputs into separate machine-readable JSON files.
6. Applying retry + backoff fetch resilience and producing a per-run reliability summary.
7. Falling back to the latest local daily snapshot cache when primary source retrieval fails repeatedly.

---

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src.pipeline
```

Generated outputs:

- `data/output/companies_raw.json`
- `data/output/companies_by_category.json`
- `data/output/pipeline_run_summary.json`

Daily versioned run artifacts are also stored under:

- `data/archive/YYYY-MM-DD/run_HHMMSS/raw_snapshots.json`
- `data/archive/YYYY-MM-DD/run_HHMMSS/normalized_records.json`
- `data/archive/YYYY-MM-DD/run_HHMMSS/grouped_records.json`
- `data/archive/YYYY-MM-DD/run_HHMMSS/run_summary.json`

---

## Testing (real-world oriented)

### Automated tests

```bash
python -m unittest discover -s tests -v
```

Current tests cover:

- retry/backoff behavior and max-retry failures,
- local snapshot fallback behavior,
- pipeline output + archive artifact generation.

### Practical reliability test cases

Use these scenarios to validate production-like behavior:

1. **Transient source outage**  
   Force temporary request failures and confirm retries recover before max attempts.
2. **Hard source outage**  
   Block outbound Yahoo access and verify fallback loads latest local snapshot.
3. **No fallback available**  
   Start with empty `data/archive` and confirm failed tickers are reported in `pipeline_run_summary.json`.
4. **Freshness monitoring**  
   Verify `fetch_metadata.as_of_utc`, `fetch_duration_seconds`, and per-run timestamps are present for observability.
5. **Daily archival continuity**  
   Run pipeline across multiple days and validate folder-per-day/run versioning under `data/archive/`.

---

## Current architecture (Stage 1)

- `src/universe.py` -> Company definitions and category metadata.
- `src/finance_client.py` -> Yahoo Finance data retrieval wrapper.
- `src/normalizer.py` -> Metrics normalization and classification logic.
- `src/pipeline.py` -> End-to-end runner to fetch + organize + save outputs.

---

## Next stage ideas

For Stage 2 (multi-agent workflow with CrewAI + LangGraph), add:

- LangGraph state machine with cyclical verification loops.
- Specialized agents: `Researcher`, `Analyst`, `Writer`.
- Human-in-the-loop approval checkpoints for high-stakes conclusions.
- Optional Serper enrichment for real-time news/context.
