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

## Python Backend Portfolio Project

This repo now includes a runnable backend portfolio project that demonstrates the stack requested for a Python developer role:

- **FastAPI** public API in `backend_project/fastapi_app.py` with `/health`, `/profile`, `/skills`, and `/projects` endpoints.
- **Flask** admin-style API in `backend_project/flask_app.py` for lightweight profile and project access.
- **Django** adapter in `backend_project/django_resume/` for teams that prefer Django settings, URLs, and JSON views.
- Shared domain and repository code in `backend_project/portfolio/` so all frameworks expose the same profile, skills, and projects.
- **PostgreSQL** schema in `postgres/001_create_portfolio_schema.sql`.
- **Redis** configuration through `REDIS_URL` for cache/queue integration.
- **Docker** and Docker Compose files for local API, Flask admin, PostgreSQL, and Redis services.
- **Kubernetes** manifests in `k8s/` and an **AWS ECS Fargate** task definition in `aws/`.
- **Resume OCR classifier** in `backend_project/resume_ocr/` with FastAPI endpoints for classifying raw resume text or base64 encoded text/PDF/image resumes by content.

### Run the FastAPI service locally

```bash
pip install -r requirements.txt
uvicorn backend_project.fastapi_app:app --reload
```

### Run the full Docker stack

```bash
docker compose up --build
```

FastAPI is available at `http://localhost:8000`, and the Flask adapter is available at `http://localhost:5000`.

### Run the Django adapter

```bash
python manage.py runserver
```

Django exposes `/health` and `/profile` using the shared backend portfolio repository.

### Resume OCR classification API

The FastAPI service includes a resume parser/classifier that can classify resumes into categories such as software engineering, data science/ML, cloud/DevOps, cybersecurity, product management, finance/accounting, sales/marketing, and healthcare.

Classify raw resume text:

```bash
curl -X POST http://localhost:8000/resume/classify \
  -H "Content-Type: application/json" \
  -d '{"text":"Python backend engineer with FastAPI, Django, PostgreSQL, Docker, and Kubernetes experience"}'
```

Classify a base64 encoded resume file:

```bash
python - <<'PY'
import base64
from pathlib import Path

path = Path("resume.pdf")
print({"filename": path.name, "file_base64": base64.b64encode(path.read_bytes()).decode("ascii")})
PY
```

Supported file inputs are `.txt`, `.md`, `.csv`, `.pdf`, `.png`, `.jpg`, `.jpeg`, `.tif`, `.tiff`, and `.bmp`. PDF parsing uses `pypdf`; image OCR uses `Pillow`, `pytesseract`, and the Tesseract system binary.


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
- fundamentals bronze ingestion shape detection/validation/persistence.
- silver fundamentals feature loading for downstream model features.

---

## Bronze fundamentals ingestion

Run standalone:

```bash
python pipeline/ingest_fundamentals.py
```

This script:

- auto-detects combined input shape from `fundamentals_raw.json` (list or dict),
- validates required valuation/income/cashflow/balance sheet fields,
- computes derived metrics (FCF, D/E, margin, YoY revenue growth),
- writes per-ticker files under `bronze/{TICKER}_fundamentals_{YYYY-MM-DD}.json`,
- appends warning-heavy tickers to `bronze/flagged_{YYYY-MM-DD}.json`.

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
