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
