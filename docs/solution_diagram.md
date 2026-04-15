# Solution Architecture — Flow Diagram

```mermaid
flowchart TB
    %% ── Styling ──
    classDef source fill:#e8f4fd,stroke:#2196f3,stroke-width:2px,color:#0d47a1
    classDef phase fill:#fff3e0,stroke:#ff9800,stroke-width:2px,color:#e65100
    classDef llm fill:#fce4ec,stroke:#e91e63,stroke-width:2px,color:#880e4f
    classDef graph fill:#e8eaf6,stroke:#3f51b5,stroke-width:2px,color:#1a237e
    classDef rule fill:#e0f2f1,stroke:#009688,stroke-width:2px,color:#004d40
    classDef validate fill:#f3e5f5,stroke:#9c27b0,stroke-width:2px,color:#4a148c
    classDef output fill:#e8f5e9,stroke:#4caf50,stroke-width:2px,color:#1b5e20
    classDef human fill:#fff8e1,stroke:#ffc107,stroke-width:2px,color:#f57f17
    classDef db fill:#efebe9,stroke:#795548,stroke-width:2px,color:#3e2723

    %% ═══════════════════════════════════════════
    %% DATA SOURCES
    %% ═══════════════════════════════════════════
    subgraph SOURCES["Data Sources (Backend-Agnostic)"]
        direction LR
        S1["SQLite OLTP\n(telecom_source.db)"]:::source
        S2["DuckDB Warehouse\n(telecom_dw.duckdb)"]:::source
        S3["DuckDB Parquet Lake\n(telecom_lake.duckdb)"]:::source
    end

    %% ═══════════════════════════════════════════
    %% PHASE 1: SCHEMA INGESTION
    %% ═══════════════════════════════════════════
    subgraph P1["Phase 1 — Schema Ingestion"]
        direction TB
        P1A["SchemaConnector\nSQLAlchemy reflection (SQLite)\ninfo_schema introspection (DuckDB)"]:::phase
        P1B["DDL Parser\n(sqlglot — extract explicit FKs\nfrom DDL files)"]:::phase
        P1C["Query Log Miner\n(mine implicit FK joins\nfrom historical queries)"]:::phase
        P1D["Merge Relationships\nexplicit FKs + implicit joins"]:::phase
    end

    SOURCES --> P1A
    P1A --> P1D
    P1B --> P1D
    P1C --> P1D

    %% ═══════════════════════════════════════════
    %% PHASE 2: KNOWLEDGE GRAPH
    %% ═══════════════════════════════════════════
    subgraph P2["Phase 2 — Knowledge Graph Construction"]
        direction TB
        P2A["Build In-Memory Graph\n(NetworkX)\nTables → Nodes, FKs → Edges"]:::graph
        P2B["Load Abbreviation Dictionary\n(config.yaml → graph properties)"]:::graph
    end

    P1D --> P2A
    P2B --> P2A

    %% ═══════════════════════════════════════════
    %% PHASE 3: DOMAIN PARTITIONING
    %% ═══════════════════════════════════════════
    subgraph P3["Phase 3 — Domain Partitioning"]
        direction TB
        P3A{"Louvain Community\nDetection on Graph"}:::graph
        P3B["Heuristic Fallback\n(keyword → domain mapping)"]:::phase
        P3C["Domain Map\ncustomer_management\nbilling_revenue\nnetwork_operations"]:::graph
    end

    P2A --> P3A
    P3A -->|success| P3C
    P3A -->|fail| P3B --> P3C

    %% ═══════════════════════════════════════════
    %% PHASE 4: INTELLIGENCE (LLM)
    %% ═══════════════════════════════════════════
    subgraph P4["Phase 4 — Intelligence & Semantic Reasoning"]
        direction TB

        subgraph PII["PII Detection"]
            P4A["Presidio Scanner\n+ Custom Recognizers\n(SSN, IMSI, Subscriber ID)"]:::phase
        end

        subgraph ABBR["Abbreviation Resolution"]
            P4B["Knowledge Graph Lookup\nCUST → Customer\nBLNG → Billing"]:::graph
        end

        subgraph LLM_CLASS["LLM Column Classification (per column, parallelized)"]
            P4C["Gather Graph Context\n• Table schema (sibling columns)\n• FK relationships\n• Downstream tables\n• Abbreviation expansions\n• Business domain"]:::graph
            P4D["LLM Call → Gemini / Ollama\nPrompt includes full graph context\nOutput: ColumnPolicySchema (Pydantic)"]:::llm
            P4E["Column Policy\n• PII classification\n• Masking strategy\n• Constraint profile\n• Business importance\n• Dedup mode\n• Edge case flags"]:::llm
        end

        subgraph REVIEW["Human Review Gate"]
            P4F{"Confidence\n< threshold?"}:::human
            P4G["Queue for Human Review\n(approve / correct / skip)"]:::human
        end

        subgraph STRATEGY["Generation Strategy Planning"]
            P4H["LLM Call → Strategy Planner\nInput: all column policies for table\nOutput: GenerationStrategySchema"]:::llm
            P4I["Strategy\n• Temporal constraints\n• Post-generation rules\n• Edge case injection %\n• Notes"]:::llm
        end
    end

    P3C --> P4A
    P3C --> P4B
    P4A --> P4C
    P4B --> P4C
    P4C --> P4D
    P4D --> P4E
    P4E --> P4F
    P4F -->|"low confidence"| P4G
    P4F -->|"high confidence"| P4H
    P4G -->|"approved"| P4H
    P4E --> P4H
    P4H --> P4I

    %% ═══════════════════════════════════════════
    %% PHASE 5: SYNTHESIS (Rule-Based)
    %% ═══════════════════════════════════════════
    subgraph P5["Phase 5 — Synthetic Data Generation (Rule-Based)"]
        direction TB
        P5A["Topological Sort\n(parent tables first\nfor FK ordering)"]:::rule
        P5B["PII Masking Engine\n(Faker-based realistic\nsubstitutions before generation)"]:::rule
        P5C["Table Profiling\n• Structural columns (PK, FK, status, date)\n• Modeled columns (free-form values)\n• Sensitive columns (PII flagged)"]:::rule
        P5D["Structural Generator\n(deterministic: PKs, FKs,\nallowed values, dates)"]:::rule
        P5E["Rule-Based Generator\n(distribution sampling\nfor modeled columns)"]:::rule
        P5F["Shared Repairs\n• FK stitching to parent tables\n• Allowed-value enforcement\n• Temporal constraint fixes\n• Uniqueness checks"]:::rule
        P5G["Boundary Key Registry\n(cross-domain FK stitching)"]:::db
        P5H["Edge Case Injection\n(nulls, zeros, min/max,\nduplicates, boundary values)"]:::rule
        P5I["Dedup Engine\n(SHA-256, mode-aware:\nentity / reference / event)"]:::rule
    end

    P4I --> P5A
    P5A --> P5B
    P5B --> P5C
    P5C --> P5D
    P5D --> P5E
    P5E --> P5F
    P5F --> P5G
    P5G --> P5H
    P5H --> P5I

    %% ═══════════════════════════════════════════
    %% PHASE 6: VALIDATION
    %% ═══════════════════════════════════════════
    subgraph P6["Phase 6 — Validation Gate"]
        direction TB
        P6A["Statistical Fidelity\nKS Test + Jensen-Shannon\nDivergence per column"]:::validate
        P6B["PII Leakage Check\nPresidio re-scan +\nre-identification risk score"]:::validate
        P6C["Lineage Integrity\nFK referential checks\nacross generated tables"]:::validate
        P6D["Business Rule Assertions\n(from strategy's\npost_generation_rules)"]:::validate
        P6E{"All checks\npassed?"}:::validate
        P6F["LLM Failure Diagnosis\n(root cause analysis +\nstrategy adjustment)"]:::llm
    end

    P5I --> P6A
    P5I --> P6B
    P5I --> P6C
    P5I --> P6D
    P6A --> P6E
    P6B --> P6E
    P6C --> P6E
    P6D --> P6E
    P6E -->|"failures"| P6F
    P6F -.->|"retry with\nupdated strategy"| P5D

    %% ═══════════════════════════════════════════
    %% PHASE 7: DELIVERY
    %% ═══════════════════════════════════════════
    subgraph P7["Phase 7 — Delivery & Defect Detection"]
        direction TB
        P7A["Delivery Packager\nParquet/CSV + gzip\n+ manifest.json"]:::output
        P7B["Production Defect Detector\n(SQL validators against\nlive source DB — real rows only)"]:::output
        P7C["Defect Report\nproduction_defects.json\n(actual bad rows + cross-table impact)"]:::output
    end

    P6E -->|"pass"| P7A
    SOURCES -.->|"live scan"| P7B
    P7A --> P7C
    P7B --> P7C

    %% ═══════════════════════════════════════════
    %% OUTPUT
    %% ═══════════════════════════════════════════
    subgraph OUT["Output"]
        direction LR
        O1["output/synthetic/{run_id}/\n├── *.parquet (per table)\n├── manifest.json\n└── production_defects.json"]:::output
    end

    P7C --> O1

    %% ═══════════════════════════════════════════
    %% DASHBOARD
    %% ═══════════════════════════════════════════
    subgraph DASH["React Dashboard (localhost:5173)"]
        direction LR
        D1["Pipeline Progress\n& Stats"]:::source
        D2["Human Review\nQueue"]:::human
        D3["Policy Viewer\n& LLM Reasoning"]:::llm
        D4["Data Comparison\n(source vs synthetic)"]:::source
        D5["Production\nDefects Panel"]:::validate
        D6["Knowledge\nGraph Viz"]:::graph
    end

    O1 --> DASH
    P4G -.-> D2

    %% ═══════════════════════════════════════════
    %% OPERATIONAL DB
    %% ═══════════════════════════════════════════
    ODB[("Operational DB\n(synthetic_data.db)\nPolicies • Strategies\nRun logs • Review queue\nModel registry")]:::db

    P4E -.->|"persist"| ODB
    P4I -.->|"persist"| ODB
    ODB -.->|"cache hit\n(skip LLM)"| P4D
    P5G -.->|"boundary keys"| ODB
```

## How to Read This Diagram

**The 3 LLM touchpoints** (pink boxes) are where Gemini/Ollama is called:
1. **Column Classification** — LLM receives full knowledge graph context and outputs a structured `ColumnPolicySchema` per column
2. **Strategy Planning** — LLM receives all column policies for a table and outputs temporal constraints + business rules
3. **Failure Diagnosis** — LLM analyzes validation failures and suggests strategy adjustments (retry loop)

**Everything else is rule-based / deterministic:**
- Schema ingestion uses SQLAlchemy reflection or DuckDB `information_schema`
- Domain partitioning uses Louvain community detection on the graph
- PII detection uses Presidio (NLP-based, not LLM)
- Data generation uses structural deterministic generators + distribution sampling
- Validation uses statistical tests (KS, JSD) and referential integrity checks
- Defect detection uses SQL validators against the live source DB

**The Knowledge Graph** (blue boxes) is the connective tissue — it feeds context to the LLM so classification is informed by table relationships, abbreviations, and domain membership rather than column names alone.

**Human Review** (yellow) is a blocking gate — columns with low LLM confidence are queued for human approval before generation proceeds.
