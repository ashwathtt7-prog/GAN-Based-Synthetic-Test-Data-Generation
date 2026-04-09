# GAN-Based Synthetic Data Generation — Setup & Run Guide

> This guide walks through every step to get the POC running locally on Windows.

---

## Prerequisites

| Tool | Version Required | Check Command |
|------|-----------------|---------------|
| Python | 3.10+ | `python --version` |
| pip | 21+ | `pip --version` |
| Node.js | 18+ | `node --version` |
| npm | 8+ | `npm --version` |
| Neo4j | 5.x (optional) | Neo4j Desktop or Community Edition |

---

## Step 1: Create Python Virtual Environment

```bash
cd D:/GAN-SYNTHETIC-DATA/GAN-Based-Synthetic-Test-Data-Generation
python -m venv venv
source venv/Scripts/activate   # Windows Git Bash
# OR: venv\Scripts\activate    # Windows CMD
```

## Step 2: Install Python Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

Key packages installed:
- **FastAPI + Uvicorn** — Backend REST API
- **SQLAlchemy** — ORM for operational memory (SQLite for POC)
- **google-generativeai** — Gemini LLM API client
- **LangChain** — LLM orchestration and tool-calling
- **neo4j** — Knowledge graph driver
- **Presidio** — PII detection engine
- **SDV (CTGAN/TVAE)** — Synthetic data generation
- **SciPy** — Statistical validation (KS test, JSD)
- **Faker** — Realistic seed data generation
- **spaCy** — NLP model for Presidio NER

## Step 3: Download spaCy NLP Model

```bash
python -m spacy download en_core_web_lg
```

This is required by Presidio for named entity recognition (PII detection).
If `en_core_web_lg` is too large (~560MB), you can use `en_core_web_sm` as a lighter alternative — update `config/config.yaml` accordingly.

## Step 4: Generate Seed Data

```bash
cd D:/GAN-SYNTHETIC-DATA/GAN-Based-Synthetic-Test-Data-Generation
python datasets/generate_seed_data.py
```

This creates `datasets/telecom_source.db` — a SQLite database with **22 tables** across 3 domains:
- **Customer Management** (8 tables): CUST_MSTR, SUBSCR_ACCT, SUBSCR_PLAN_ASSGN, SVC_PLAN_REF, CUST_ADDR, CUST_CNTCT, IDENT_DOC, CUST_STAT_HIST
- **Billing & Revenue** (7 tables): BLNG_ACCT, INVC, INVC_LN_ITEM, PYMT, PYMT_MTHD, USAGE_REC, CDR_REC
- **Network Operations** (7 tables): NTWK_ELEM, CELL_TWR, SVC_ORD, SVC_ORD_ITEM, WRK_ORD_ASSGN, NTWK_INCDT, FIELD_AGT

Total: ~200,000+ records of realistic telecom data.

## Step 5: (Optional) Start Neo4j

If you have Neo4j installed:
```bash
neo4j console
# Default: bolt://localhost:7687, user: neo4j, password: synthetic_data_poc
```

If Neo4j is **not** available, the pipeline will automatically use a heuristic fallback for domain partitioning. The core pipeline still runs.

## Step 6: Configure LLM (Gemini)

The service account JSON is at `D:/GAN-SYNTHETIC-DATA/service-account.json`.
The config already points to it via `config/config.yaml`:

```yaml
llm:
  provider: "gemini"
  model: "gemini-2.5-flash-preview-04-17"
  service_account_path: "../service-account.json"
```

No changes needed if the service account file is in place.

## Step 7: Start FastAPI Backend

```bash
cd D:/GAN-SYNTHETIC-DATA/GAN-Based-Synthetic-Test-Data-Generation
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at: **http://localhost:8000**
- Swagger docs: http://localhost:8000/docs
- Dashboard stats: http://localhost:8000/api/dashboard/stats

## Step 8: Start React Frontend

In a new terminal:
```bash
cd D:/GAN-SYNTHETIC-DATA/GAN-Based-Synthetic-Test-Data-Generation/frontend
npm install
npm run dev
```

The dashboard will be available at: **http://localhost:5173**

## Step 9: Run the Pipeline

1. Open the dashboard at http://localhost:5173
2. Click **"Execute Pipeline"**
3. Watch the progress bar move through phases:
   - Schema Ingestion (2-18%)
   - Intelligence & Semantic Reasoning (25-65%)
   - Synthetic Data Generation (65-82%)
   - Validation Gate (82-95%)
   - Delivery Packaging (95-100%)
4. Review flagged columns in the Human Review Queue panel
5. Click "View Policies" to see all LLM classification decisions

## Alternatively: Run Pipeline via API

```bash
# Start pipeline
curl -X POST http://localhost:8000/api/pipeline/start

# Check status (replace RUN_ID with the returned run_id)
curl http://localhost:8000/api/pipeline/status/RUN_ID

# View review queue
curl http://localhost:8000/api/review/queue

# Approve a review item
curl -X POST http://localhost:8000/api/review/1/approve -H "Content-Type: application/json" -d '{"reviewer_notes": "Looks good"}'
```

---

## Output

After the pipeline completes, synthetic data is exported to:
```
output/synthetic/{run_id}/
  ├── CUST_MSTR.parquet
  ├── SUBSCR_ACCT.parquet
  ├── ... (all 22 tables)
  └── manifest.json
```

The `manifest.json` contains:
- Row counts per table
- Validation results (KS test, JSD, PII leakage, FK integrity)
- Edge case coverage percentages
- Generation strategies used
- Timestamp and run ID

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError: No module named 'community'` | `pip install python-louvain` |
| `spacy.load("en_core_web_lg") fails` | `python -m spacy download en_core_web_lg` |
| Neo4j connection refused | Pipeline works without Neo4j (heuristic fallback) |
| Gemini API auth fails | Check service account JSON path in config.yaml |
| CTGAN training slow | Reduce epochs in config.yaml (`ctgan_epochs: 50`) |
| SQLite locked errors | Ensure only one process accesses the DB at a time |

---

## Architecture Reference

```
Source DB → [SchemaConnector] → [DDLParser] → [QueryLogMiner]
                                       ↓
                              [Neo4jBuilder] → Knowledge Graph
                                       ↓
                           [DomainPartitioner] → Domain Clusters
                                       ↓
              [PresidioScanner] → PII columns → auto-masking (bypass LLM)
              [AbbreviationResolver] → expand column names
              [LLMAgent] → ColumnPolicySchema per column
              [StrategyPlanner] → GenerationStrategySchema per table
                                       ↓
              [TierRouter] → CTGAN / TVAE / RuleBased
              [MaskingEngine] → mask real data before training
              [CTGANModel/TVAEModel/RuleBasedGenerator] → synthetic data
              [EdgeCaseEngine] → inject edge cases
              [DedupEngine] → mode-aware deduplication
                                       ↓
              [DataValidator] → KS/JSD/chi-sq + PII scan + FK check + rules
              [FailureDiagnosisAgent] → LLM retry on failure
                                       ↓
              [DeliveryPackager] → Parquet + manifest + gzip
```
