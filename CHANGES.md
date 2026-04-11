# Changes Log — GAN Synthetic Data POC

This document captures everything that changed in the current pass of work on
the POC, plus the scripts and dependencies needed to run it end-to-end. It is
intended to be read alongside `SETUP_GUIDE.md` (which describes the original
happy path). If anything in `SETUP_GUIDE.md` contradicts this file, this file
is the authoritative version for the current state of the repo.

---

## 1. What was broken and how it was fixed

### 1.1 Pipeline IntegrityError when running a second source

- **Symptom:** after running the pipeline once against any telecom source,
  running it again against a different source failed with
  `sqlite3.IntegrityError: UNIQUE constraint failed: table_metadata.table_name`.
- **Root cause:** `TableMetadataRecord.table_name` (and a couple of sibling
  models) were declared with `unique=True`, but the three telecom sources
  share the same table names (`BLNG_ACCT`, `CUST_MSTR`, etc). The moment a
  second source tried to upsert, the global uniqueness check blocked it.
- **Fix:** in `db/schema.py` the uniqueness is now scoped to
  `(source_name, table_name)` (or `(source_name, table_name, column_name)` for
  `ColumnPolicy`). Column-level `unique=True` was removed.

Affected models:

| Model                 | New constraint                                        |
|-----------------------|-------------------------------------------------------|
| `TableMetadataRecord` | `uq_metadata_source_table(source_name, table_name)`   |
| `GenerationStrategy`  | `uq_strategy_source_table(source_name, table_name)`   |
| `ColumnPolicy`        | `uq_source_table_column(source_name, table_name, column_name)` |

> **IMPORTANT:** because SQLAlchemy does not migrate live schemas, you must
> delete the existing operational DB once (`synthetic_data.db` / whichever
> file your config points at) so it gets recreated with the new constraints.
> See §4.

### 1.2 DuckDB reflection failed on `pg_catalog.pg_collation`

- **Symptom:** pipeline against a DuckDB source crashed during schema
  ingestion with
  `Catalog Error: Table with name pg_collation does not exist! LINE 6: FROM pg_catalog.pg_collation`.
- **Root cause:** `duckdb_engine 0.17.0`'s `get_multi_columns()` still emits
  Postgres-style reflection queries that DuckDB 1.5+ has dropped from the
  system catalog, so `MetaData.reflect()` blows up.
- **Fix:** `ingestion/schema_connector.py` now bypasses SQLAlchemy reflection
  entirely for DuckDB and walks `information_schema.columns` instead. Two
  lightweight dataclasses, `_SimpleColumn` / `_SimpleTable`, stand in for the
  SQLAlchemy objects so the rest of the pipeline can consume the same
  `(name, type)` shape without caring about dialect.

Key pieces:
- New helper `_introspect_duckdb()` reads
  `information_schema.columns WHERE table_schema IN ('main', 'public')`.
- New helper `_iter_tables()` yields `(name, table)` for both paths.
- `_compute_column_stats()` now uses quoted identifiers (`"{col}"`) so both
  SQLite and DuckDB accept them.

### 1.3 Fake production-defect rows in the UI

- **Symptom:** the Edge Cases panel was showing synthetic rows with
  fabricated “Original value” / “Defect value” pairs that did not actually
  exist in the source database. The original simulator was mutating valid
  generated rows with recipe-based patterns, not scanning real data.
- **Root cause:** `synthesis/production_defect_simulator.py` ran *after*
  generation, against the synthetic DataFrames, and manufactured defects.
- **Fix:** replaced with `synthesis/production_defect_detector.py`, a
  SQL-level detector that scans the live source database and reports only
  rows that actually exist there. Every value in the UI now comes straight
  from a `SELECT` against the source DB. Cross-table impact is also real: it
  runs `JOIN`s against declared child tables to count and sample downstream
  rows linked to each broken parent.

---

## 2. New / modified files

### 2.1 New files

- **`synthesis/production_defect_detector.py`** — Real defect detector. Runs a
  catalog of `Validator` predicates against the source engine. Produces the
  same `TableDefectReport` / API-payload shape the frontend already consumes,
  so no API contract change was needed. Validators cover:

  | Table         | Column              | Check                                             |
  |---------------|---------------------|----------------------------------------------------|
  | `CUST_CNTCT`  | `CNTCT_VAL`         | Email missing `@` / domain, or phone with letters  |
  | `CUST_MSTR`   | `CUST_SSN`          | Length not equal to 11 (`NNN-NN-NNNN`)             |
  | `CUST_MSTR`   | `CUST_DOB`          | Date of birth in the future                        |
  | `CUST_MSTR`   | `CUST_FRST_NM`      | SQL-injection payload / dangerous chars            |
  | `BLNG_ACCT`   | `BLNG_CURR_BAL_AMT` | Negative current balance                           |
  | `BLNG_ACCT`   | `BLNG_CRED_LMT_AMT` | Credit limit > DECIMAL(12,2) bounds                |
  | `INVC`        | `INVC_DUE_DT`       | Due date earlier than cycle date                   |
  | `INVC`        | `INVC_PAID_AMT`     | Paid amount greater than total                     |
  | `CDR_REC`     | `CDR_DUR_SEC`       | Negative duration                                  |
  | `CDR_REC`     | `CDR_END_DT`        | End time earlier than start time                   |
  | `SUBSCR_ACCT` | `CUST_ID`           | Dangling FK (anti-join against `CUST_MSTR`)        |
  | `PYMT`        | `BLNG_ACCT_ID`      | Dangling FK (anti-join against `BLNG_ACCT`)        |

  The detector has a tiny SQLite → DuckDB SQL translator for the handful of
  functions that differ (`INSTR` → `position`, `GLOB` → `regexp_matches`,
  `DATE('now')` is sidestepped with a literal ISO-8601 date).

- **`datasets/inject_production_defects.py`** — Plants the realistic bad-data
  rows that the detector above is designed to find. Runs against every
  configured telecom source, records every injection in a sibling
  `_defect_ledger` table, and is idempotent (uses `MIN(pk)` / `MAX(pk)`
  subselects, so repeated runs hit the same rows).

  CLI:

  ```bash
  python datasets/inject_production_defects.py
  python datasets/inject_production_defects.py --targets telecom_sqlite
  python datasets/inject_production_defects.py --dry-run
  ```

- **`CHANGES.md`** — this file.

### 2.2 Modified files

- **`db/schema.py`** — composite unique constraints scoped to `source_name`
  (§1.1).
- **`ingestion/schema_connector.py`** — DuckDB reflection bypass (§1.2).
- **`pipeline/orchestrator.py`** — imports switched from
  `production_defect_simulator` to `production_defect_detector`, and the
  phase 7.5 call site now hands the detector the live `connector.engine`
  plus the FK relationship list and `table_filter` instead of generated
  DataFrames.
- **`frontend/src/components/EdgeCasePanel.jsx`** — removed the two-column
  “Original value / Defect value (would fail in prod)” layout. It now shows
  a single “Detected value (from source database)” card wired to the real
  value, and the header copy / status chip now say **real defects detected**
  instead of *simulated*.

### 2.3 Deprecated

- **`synthesis/production_defect_simulator.py`** — no longer imported. It is
  left in the repo temporarily so you can diff it against the new detector if
  you want to see the change; the orchestrator no longer references it. Safe
  to delete in a follow-up cleanup.

---

## 3. Dependencies

No new Python dependencies were added — everything needed was already pinned
in `requirements.txt`. Two dependencies that matter for the fixes above, and
their expected versions on a working machine, are:

| Package         | Version | Notes |
|-----------------|---------|-------|
| `sqlalchemy`    | `>=2.0` | Needs 2.x API for the composite `UniqueConstraint` syntax used in `db/schema.py`. |
| `duckdb`        | `1.5.1` | Required for the DuckDB warehouse / lake sources. |
| `duckdb_engine` | `0.17.0`| The SQLAlchemy driver. Reflection is bypassed (see §1.2), but `create_engine("duckdb:///...")` still needs this package installed. |

> **If `python -c "import duckdb_engine"` fails from your shell** but the
> pipeline previously worked from the venv, you are almost certainly running
> the wrong Python. Use `venv/Scripts/python.exe` (Windows) or
> `venv/bin/python` (macOS/Linux) for every command in this doc.

---

## 4. How to bring a fresh clone up to a working state

```bash
# 0. Create and activate a venv (skip if you already have one)
cd D:/GAN-SYNTHETIC-DATA/GAN-Based-Synthetic-Test-Data-Generation
python -m venv venv
source venv/Scripts/activate           # Windows Git Bash
# OR:  venv\Scripts\activate            # Windows CMD

pip install --upgrade pip
pip install -r requirements.txt
python -m spacy download en_core_web_lg

# 1. Build the telecom source databases (SQLite + the two DuckDB sources)
python datasets/generate_seed_data.py

# 2. Drop the operational DB so the new composite unique constraints
#    get applied cleanly. (One-time step after pulling §1.1 fixes.)
rm -f synthetic_data.db                  # path comes from config/config.yaml

# 3. Plant realistic production defects in the source DBs so the
#    detector in Phase 7.5 has something meaningful to find.
python datasets/inject_production_defects.py

# 4. Start the backend on port 8001
venv/Scripts/python.exe -m uvicorn api.main:app --host 127.0.0.1 --port 8001

# 5. In a second terminal, start the frontend on port 5173
cd frontend && npm install && npm run dev
```

Then either click **Execute Pipeline** in the dashboard or run it manually:

```bash
venv/Scripts/python.exe -c "
from pipeline.orchestrator import PipelineOrchestrator
o = PipelineOrchestrator()
run_id = o.initialize_run(source_name='telecom_sqlite')
o.execute_pipeline(run_id=run_id, source_name='telecom_sqlite')
"
```

When the run finishes, `output/synthetic/{run_id}/production_defects.json`
will contain the real defect report and the **Production Edge Cases** panel
in the UI will render every row with its actual source value, actual primary
key, and actual cross-table impact.

---

## 5. How to verify everything works

Quick smoke test without spinning up the UI:

```bash
venv/Scripts/python.exe -c "
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

On a freshly-seeded + freshly-injected set of databases you should see each
source report **7 tables / ~62 defects** (14 planted plus whatever Faker
happened to emit that already violates the validators). The exact number
will vary slightly between runs of `generate_seed_data.py`, but it should
never be zero.

---

## 6. Architectural notes that are easy to miss

- **Parallel column classification.** The orchestrator already runs the per-
  column LLM classifier in a `ThreadPoolExecutor`
  (`_get_intelligence_parallel_workers`, default 6 workers). Tables are still
  processed sequentially so FK ordering stays deterministic; only the
  per-column calls within a table fan out in parallel.

- **The detector vs the EdgeCaseEngine.** They are *not* the same thing. The
  `EdgeCaseEngine` (used during synthesis) injects statistical edge cases
  (zero, min, max, nulls, duplicates) into the synthetic output so
  downstream models are robust. The `ProductionDefectDetector` (Phase 7.5)
  reads only the source DB and reports rows that real production code would
  reject. Both ship in every run and they complement each other.

- **The `_defect_ledger` table** created by `inject_production_defects.py` is
  a sibling table in each source DB. It is there purely for verification —
  downstream code can query it to compute detector precision / recall if you
  want to measure how well the validator catalog covers the injection recipe
  catalog. The pipeline itself does not depend on it.
