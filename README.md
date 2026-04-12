# GAN-Based Synthetic Test Data Generation System

A pipeline that ingests production SQL schemas, detects PII, learns schema semantics via LLM, and generates statistically faithful synthetic data using CTGAN/TVAE/rule-based methods — with validation, real production defect detection, and an interactive web dashboard.

**Demo dataset:** 22-table telecom schema (~200k rows) served from three backends (SQLite OLTP, DuckDB warehouse, DuckDB parquet lake) to prove the pipeline is backend-agnostic.

---

## Prerequisites

| Tool       | Version  | Check command        |
|------------|----------|----------------------|
| Python     | 3.10+    | `python --version`   |
| pip        | 21+      | `pip --version`      |
| Node.js    | 18+      | `node --version`     |
| npm        | 8+       | `npm --version`      |
| Git        | any      | `git --version`      |

**Optional:**
- **Neo4j 5.x** — for knowledge graph storage. If not installed, the pipeline falls back to heuristic domain partitioning automatically.
- **Ollama** — for local/on-prem LLM instead of Gemini API.

---

## Quick Start

All commands below assume you are in the project root directory and using **Git Bash** (on Windows) or a Unix-compatible shell.

### 1. Clone the repository

```bash
git clone <repo-url>
cd GAN-SYNTHETIC-DATA-V4
```

### 2. Create and activate a Python virtual environment

```bash
python -m venv venv

# Windows (Git Bash):
source venv/Scripts/activate

# Windows (CMD):
venv\Scripts\activate

# macOS / Linux:
source venv/bin/activate
```

### 3. Install Python dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

This installs FastAPI, SQLAlchemy, SDV (CTGAN/TVAE), Presidio, LangChain, Google Generative AI, spaCy, and ~50 other packages. The full list is in `requirements.txt`.

### 4. Download the spaCy NLP model

```bash
python -m spacy download en_core_web_lg
```

Required by Presidio for PII detection. This is ~560 MB. If bandwidth is a concern, you can use `en_core_web_sm` instead and update `config/config.yaml` under `presidio.spacy_model`.

### 5. Install DuckDB engine (if not pulled in automatically)

```bash
pip install duckdb duckdb_engine
```

These are needed for the DuckDB warehouse and parquet lake data sources. They may already be installed as transitive dependencies, but install explicitly to be sure.

### 6. Generate seed databases

```bash
python datasets/generate_seed_data.py
```

This creates three telecom source databases:
- `datasets/telecom_source.db` — SQLite OLTP (22 tables, ~200k rows)
- `datasets/telecom_dw.duckdb` — DuckDB analytical warehouse
- `datasets/telecom_lake.duckdb` — DuckDB views over Parquet files in `datasets/telecom_lake_parquet/`

> **Note:** If these files already exist in the repo from a previous commit, this step regenerates them with fresh Faker data. You can skip it if you want to use the existing data.

### 7. Inject production defects

```bash
python datasets/inject_production_defects.py
```

Plants realistic bad-data rows (malformed emails, future DOBs, negative balances, dangling FKs, etc.) into all three source databases. The production defect detector in the pipeline will find and report these in the dashboard.

### 8. Configure the LLM provider

Edit `config/config.yaml` under the `llm` section:

**Option A — Google Gemini (default):**

1. Obtain a Google Cloud service account JSON key with Gemini API access.
2. Place the file at the path specified in `config.yaml` (default: `../service-account.json`, i.e., one directory above the project root).
3. Verify the config:
   ```yaml
   llm:
     provider: "gemini"
     model: "gemini-2.5-flash"
     service_account_path: "../service-account.json"
   ```

**Option B — Ollama (local, no API key needed):**

1. Install [Ollama](https://ollama.com/) and pull a model:
   ```bash
   ollama pull gemma3:4b
   ```
2. Update `config/config.yaml`:
   ```yaml
   llm:
     provider: "ollama"
     model: "gemma3:4b"
     ollama_base_url: "http://localhost:11434"
   ```

### 9. (Optional) Start Neo4j

If you have Neo4j Desktop or Community Edition installed:
```bash
neo4j console
# Default bolt://localhost:7687, user: neo4j, password: synthetic_data_poc
```

The credentials are configured in `config/config.yaml` under `neo4j`. If Neo4j is not running, the pipeline uses NetworkX + Louvain community detection as a fallback — no action needed.

### 10. Start the backend

```bash
python -m uvicorn api.main:app --host 127.0.0.1 --port 8001
```

- API: http://localhost:8001
- Swagger docs: http://localhost:8001/docs

> **Important:** The backend must run on port **8001**. The frontend is hardcoded to connect to `http://localhost:8001/api`.

### 11. Start the frontend (new terminal)

```bash
cd frontend
npm install
npm run dev
```

- Dashboard: http://localhost:5173

### 12. Run the pipeline

1. Open http://localhost:5173 in your browser.
2. Select a data source from the dropdown (default: "Telecom OLTP (SQLite)").
3. Click **"Execute Pipeline"**.
4. Watch the progress bar move through 5 phases:
   - **Schema Ingestion** (2-18%) — discovers tables, columns, FKs, statistics
   - **Intelligence & Semantic Reasoning** (25-65%) — PII detection, LLM classification, domain partitioning
   - **Synthetic Data Generation** (65-82%) — CTGAN/TVAE/rule-based generation per table
   - **Validation Gate** (82-95%) — KS test, JSD, FK integrity, PII leakage checks
   - **Delivery Packaging** (95-100%) — Parquet/CSV export with manifest
5. Review flagged columns in the **Human Review Queue** panel.
6. View production defects in the **Edge Cases** panel.

---

## Project Structure

```
GAN-SYNTHETIC-DATA-V4/
├── api/                        # FastAPI REST API (main.py)
├── config/                     # config.yaml, domain overrides
├── db/                         # SQLAlchemy ORM models + DB client
├── models/                     # Pydantic request/response schemas
├── datasets/                   # Seed data scripts + DDL + source DBs
│   ├── ddl/                    #   SQL DDL for 22 tables (4 domain files)
│   ├── query_logs/             #   Historical query logs for FK inference
│   ├── telecom_lake_parquet/   #   Parquet files for DuckDB lake
│   ├── generate_seed_data.py   #   Creates all 3 source databases
│   └── inject_production_defects.py  # Plants realistic bad data
├── ingestion/                  # Schema discovery (SQLAlchemy reflection, DuckDB introspection)
├── graph/                      # Knowledge graph (NetworkX + optional Neo4j)
├── intelligence/               # PII detection (Presidio), LLM classification, strategy planning
├── synthesis/                  # CTGAN/TVAE/rule-based generation, validation, edge cases
├── pipeline/                   # Master orchestrator (5-phase pipeline)
├── delivery/                   # Output packaging (Parquet/CSV + manifest)
├── llm/                        # LLM abstraction (Gemini / Ollama)
├── frontend/                   # React 19 + Vite + Tailwind CSS dashboard
├── scripts/                    # Utilities (pretrain_models.py)
├── output/synthetic/           # Generated synthetic data (per run)
├── models/trained/             # Cached CTGAN/TVAE model files
├── logs/                       # Runtime logs
├── requirements.txt            # Python dependencies
├── config/config.yaml          # All configuration (LLM, DB, sources, thresholds)
├── CHANGES.md                  # Latest fixes and change log
├── SETUP_GUIDE.md              # Original setup walkthrough
└── IMPLEMENTATION_CHANGES.md   # Recent feature enhancements
```

---

## Configuration Reference

All configuration lives in `config/config.yaml`. Key sections:

| Section | What it controls |
|---------|-----------------|
| `llm` | Provider (gemini/ollama), model name, temperature, service account path |
| `database` | Operational DB URL (default: SQLite at `./synthetic_data.db`) |
| `neo4j` | Neo4j connection (optional) |
| `data_sources` | List of source databases the pipeline can ingest |
| `ingestion` | Sample row count, DDL directory path |
| `presidio` | spaCy model, PII confidence threshold, enabled recognizers |
| `generation` | CTGAN/TVAE epochs, row count thresholds for tier routing |
| `validation` | KS test alpha, JSD threshold, re-identification risk threshold |
| `delivery` | Output format (parquet/csv), output directory, compression |
| `pipeline` | Human review mode (wait/skip), crash recovery, parallelism |
| `abbreviations` | Seeded dictionary for expanding column name abbreviations |

---

## Output

After each pipeline run, results are written to:

```
output/synthetic/{run_id}/
├── CUST_MSTR.parquet          # Synthetic data per table
├── SUBSCR_ACCT.parquet
├── ... (all tables)
├── manifest.json              # Row counts, validation scores, strategies used
└── production_defects.json    # Real defects found in source database
```

---

## Running the Pipeline via API (without the UI)

```bash
# Start the pipeline
curl -X POST http://localhost:8001/api/pipeline/start

# Check pipeline status
curl http://localhost:8001/api/pipeline/status/{run_id}

# View the human review queue
curl http://localhost:8001/api/review/queue

# Approve a review item
curl -X POST http://localhost:8001/api/review/{id}/approve \
  -H "Content-Type: application/json" \
  -d '{"reviewer_notes": "Approved"}'
```

Or via Python:

```python
from pipeline.orchestrator import PipelineOrchestrator

o = PipelineOrchestrator()
run_id = o.initialize_run(source_name="telecom_sqlite")
o.execute_pipeline(run_id=run_id, source_name="telecom_sqlite")
```

Available source names: `telecom_sqlite`, `telecom_duckdb_dw`, `telecom_parquet_lake`, `demo_showcase`.

---

## Verification

Quick smoke test (no UI needed):

```bash
python -c "
from sqlalchemy import create_engine
from synthesis.production_defect_detector import ProductionDefectDetector

for url, label in [
    ('sqlite:///./datasets/telecom_source.db', 'sqlite'),
    ('duckdb:///./datasets/telecom_dw.duckdb', 'duckdb_dw'),
]:
    reports = ProductionDefectDetector().detect(create_engine(url))
    total = sum(len(r.defect_rows) for r in reports.values())
    print(f'{label}: tables={len(reports)} defects={total}')
"
```

Expected: each source reports ~7 tables / ~62 defects.

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError: No module named 'community'` | `pip install python-louvain` |
| `ModuleNotFoundError: No module named 'duckdb_engine'` | `pip install duckdb duckdb_engine` |
| spaCy model load fails | `python -m spacy download en_core_web_lg` |
| Neo4j connection refused | Pipeline works without Neo4j (heuristic fallback) |
| Gemini API auth fails | Check service account JSON path in `config/config.yaml` |
| CTGAN training is slow | Reduce epochs in `config.yaml`: `ctgan_epochs: 50` |
| SQLite locked errors | Ensure only one process accesses the DB at a time |
| `UNIQUE constraint failed: table_metadata.table_name` | Delete `synthetic_data.db` and restart — schema constraints were updated |
| DuckDB `pg_catalog.pg_collation` error | This is fixed in the codebase; ensure you have the latest code |
| Frontend can't connect to backend | Backend must be running on port **8001** |
| Wrong Python interpreter | Use `venv/Scripts/python.exe` (Windows) or `venv/bin/python` (Linux/Mac) |

---

## Tech Stack

**Backend:** Python 3.10+, FastAPI, SQLAlchemy, Pandas, SDV (CTGAN/TVAE), Presidio, spaCy, LangChain, Google Generative AI / Ollama, NetworkX, SciPy

**Frontend:** React 19, Vite, Tailwind CSS, Axios, React Force Graph 2D

**Databases:** SQLite (operational), DuckDB (warehouse/lake sources), Neo4j (optional graph)

**LLM Providers:** Google Gemini API or Ollama (local)
