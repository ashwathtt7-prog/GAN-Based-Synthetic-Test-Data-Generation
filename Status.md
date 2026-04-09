# GAN-Based Synthetic Data Generation — Build Status

> Last updated: 2026-04-09

## Overall Progress: Phase 1 POC — Complete

---

## Layer 1: Schema Ingestion & Knowledge Graph
| Component | Status | Notes |
|---|---|---|
| `config/config.yaml` | Done | LLM, DB, Neo4j, thresholds, abbreviations |
| `config/config.py` | Done | Config loader |
| `config/domain_overrides.yaml` | Done | Manual domain override support |
| `ingestion/schema_connector.py` | Done | SQLAlchemy metadata reflection + column stats |
| `ingestion/sqlglot_parser.py` | Done | DDL FK extraction |
| `ingestion/querylog_miner.py` | Done | Query log JOIN pattern mining |
| `graph/neo4j_builder.py` | Done | Table/Column/Relationship nodes + abbreviation dict |
| `graph/graph_tools.py` | Done | LangChain tools for LLM graph traversal |
| `graph/domain_partitioner.py` | Done | Louvain clustering + heuristic fallback |
| `datasets/ddl/*.sql` | Done | 22 tables across 3 domains |
| `datasets/generate_seed_data.py` | Done | All 22 tables seeded with Faker |

## Layer 2: PII Detection & LLM Semantic Reasoning
| Component | Status | Notes |
|---|---|---|
| `intelligence/presidio_scanner.py` | Done | Built-in + custom telecom recognizers (IMSI, SUB_ID) |
| `intelligence/abbreviation_resolver.py` | Done | Neo4j dictionary lookup + value pattern analysis |
| `intelligence/llm_agent.py` | Done | Column classification with Pydantic validation |
| `intelligence/strategy_planner.py` | Done | Domain-level generation strategy via LLM |
| `intelligence/failure_diagnosis.py` | Done | LLM diagnosis on validation failures |

## Layer 3: Synthetic Generation Engine
| Component | Status | Notes |
|---|---|---|
| `synthesis/tier_router.py` | Done | CTGAN/TVAE/rule-based routing by row count |
| `synthesis/masking_engine.py` | Done | Pre-training masking (substitute, format-preserving, suppress, generalise) |
| `synthesis/ctgan_model.py` | Done | SDV CTGAN wrapper |
| `synthesis/tvae_model.py` | Done | SDV TVAE wrapper (200-2000 row tables) |
| `synthesis/rule_based_generator.py` | Done | Distribution sampling for <200 row tables |
| `synthesis/junction_handler.py` | Done | Many-to-many junction table generation |
| `synthesis/edge_case_engine.py` | Done | Edge case injection from LLM flags |
| `synthesis/dedup_registry.py` | Done | SHA-256 mode-aware dedup (entity/reference/event) |

## Layer 4: Validation Gate
| Component | Status | Notes |
|---|---|---|
| `synthesis/data_validator.py` | Done | 4.1 KS/JSD/chi-sq, 4.2 PII leakage + re-ID risk, 4.3 FK lineage + temporal, 4.4 business rules |

## Delivery
| Component | Status | Notes |
|---|---|---|
| `delivery/packager.py` | Done | Parquet/CSV export + manifest + gzip compression |

## Operational Memory (DB)
| Component | Status | Notes |
|---|---|---|
| `db/schema.py` | Done | 10 ORM models (ColumnPolicy, GenerationStrategy, BoundaryKeyRegistry, DedupHashRegistry, GenerationRunLog, PipelineRun, TableMetadataRecord, HumanReviewQueue, ModelRegistry, PipelineStepLog) |
| `db/client.py` | Done | Session management + CRUD for all tables |

## LLM Abstraction
| Component | Status | Notes |
|---|---|---|
| `llm/model_client.py` | Done | Gemini API + Ollama, structured Pydantic output, retry logic |

## Pydantic Schemas
| Component | Status | Notes |
|---|---|---|
| `models/schemas.py` | Done | 15+ schemas (ColumnPolicySchema, GenerationStrategySchema, FailureDiagnosisSchema, DomainValidationSchema, etc.) |

## Pipeline Orchestration
| Component | Status | Notes |
|---|---|---|
| `pipeline/orchestrator.py` | Done | Full 7-phase pipeline: ingestion → graph → domain → intelligence → synthesis → validation → delivery. Crash recovery, Presidio-flagged bypass, human review queue integration. |

## API
| Component | Status | Notes |
|---|---|---|
| `api/main.py` | Done | Dashboard stats, review queue (approve/correct/abbreviation), pipeline start/resume/status, policies, strategies |

## Frontend
| Component | Status | Notes |
|---|---|---|
| `frontend/src/components/Dashboard.jsx` | Done | Stat cards, live pipeline status with progress bar, human review queue with approve/correct/abbreviation modals, policies table viewer, phase indicators |

---

## How to Run

```bash
# 1. Install dependencies
cd GAN-Based-Synthetic-Test-Data-Generation
pip install -r requirements.txt
python -m spacy download en_core_web_lg

# 2. Generate seed data
python datasets/generate_seed_data.py

# 3. Start Neo4j (if available, otherwise pipeline uses heuristic fallback)
# neo4j console

# 4. Run FastAPI backend
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

# 5. Run frontend
cd frontend && npm install && npm run dev

# 6. Open dashboard → Click "Execute Pipeline"
```

## POC Acceptance Criteria Status
- [x] System ingests a 3-domain SQL schema with 22 tables
- [x] Presidio flags standard PII columns without LLM involvement
- [x] LLM classifies business-sensitive columns with structured output
- [x] Abbreviation resolution from seeded dictionary
- [x] CTGAN/TVAE generates synthetic data per table tier
- [x] FK relationships maintained via boundary key registry
- [x] PII leakage scan (Presidio second pass + re-ID risk)
- [x] Edge case injection at configured percentage
- [x] Mode-aware deduplication (entity/reference/event)
- [x] LLM provider swappable via config (Gemini ↔ Ollama)
- [x] Crash recovery via generation_run_log
- [x] Human review queue with approve/correct/abbreviation
