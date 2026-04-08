# GAN-Based Synthetic Test Data Generation — Master Build Prompt

## Project Overview

You are building a **GAN-Based Synthetic Test Data Generation System** for a telecom enterprise. The system ingests production SQL database schemas, understands them intelligently using an LLM, and generates statistically faithful synthetic test data that can safely be used in lower test environments (PLE) without exposing real customer PII.

This is a **POC build**. Scope is limited to SQL relational databases only. NoSQL, Flink, and anomaly replay are Phase 2.

---

## Tech Stack — Non-Negotiable Choices

| Component | Tool | Reason |
|---|---|---|
| Data ingestion | Apache Spark + JDBC | Multi-source SQL connector |
| SQL parsing | SQLGlot | Multi-dialect DDL parser |
| Knowledge graph | Neo4j + LangChain Neo4j toolkit | Graph traversal as LLM context |
| PII detection | Microsoft Presidio + custom recognisers | Fast, deterministic, open source |
| LLM (POC) | Gemini API | Configurable — swappable to local |
| LLM (production) | Gemma 4B via Ollama | On-premise, no data egress |
| LLM orchestration | LangChain tool-calling agent | Graph traversal tools for context |
| Structured output | Pydantic v2 | Enforced JSON schema on every LLM call |
| Operational memory | PostgreSQL | All LLM decisions persisted |
| GAN generation | CTGAN + SDV (HMA) | Multi-table tabular synthesis |
| Validation | SciPy + Great Expectations + Presidio | Statistical + business rule checks |
| Deduplication | SHA-256 hash registry in PostgreSQL | Mode-aware per table |

---

## System Architecture — Layer by Layer

### Layer 1 — Schema Ingestion & Knowledge Graph Construction

**Step 1.1 — Data Ingestion**
- Connect to SQL sources (Oracle, PostgreSQL, MySQL) via Apache Spark JDBC
- Read DDL statements for all tables
- Extract statistical profiles per column: row count, null rate, unique count, min, max, mean, std dev, top 10 values with frequencies
- Store raw DDL and statistical profiles in staging (Delta Lake or local Parquet)
- Nothing is written back to source databases — read-only access

**Step 1.2 — SQLGlot Parsing**
- Parse all DDL statements using SQLGlot
- Extract: table names, column names, data types, NOT NULL constraints, CHECK constraints, PRIMARY KEY definitions, FOREIGN KEY definitions with referenced table and column
- Build a structured relationship map: {source_table, source_column, target_table, target_column, relationship_type: "FK_DECLARED", confidence: 1.0}
- Handle all SQL dialects SQLGlot supports — no dialect-specific code

**Step 1.3 — Query Log Mining**
- Parse historical SQL query logs using SQLGlot's query parser (not DDL parser)
- Extract all JOIN conditions: ON clause patterns linking table.column = table.column
- Aggregate by table pair — count how many distinct queries contain each JOIN pattern
- Assign confidence scores: >1000 queries = 0.95, 100-1000 = 0.80, 10-100 = 0.60, <10 = 0.30
- Add to relationship map with relationship_type: "FK_INFERRED"
- If query logs are unavailable, skip this step and note it in the run log

**Step 1.4 — Neo4j Knowledge Graph Construction**
- Create Neo4j nodes for every table with properties: {table_name, row_count, column_count, domain: null, abbreviation_dict: {}}
- Create Neo4j nodes for every column with properties: {column_name, data_type, nullable, statistical_profile: {}, pii_classification: null, masking_strategy: null, constraint_profile: null, edge_case_flags: [], dedup_mode: null, llm_confidence: null}
- Create Neo4j edges for every relationship with properties: {relationship_type, confidence, cardinality}
- Create Neo4j edge type BELONGS_TO linking column nodes to table nodes
- Seed the abbreviation dictionary as a global Neo4j node with properties for common telecom abbreviations: CUST→Customer, TEN→Tenure, MNT→Month, SVC→Service, ORD→Order, AGT→Agent, ASSGN→Assignment, BLNG→Billing, CYC→Cycle, TYP→Type, DT→Date, CD→Code, ID→Identifier, NO→Number, AMT→Amount, QTY→Quantity, STAT→Status, REF→Reference, SEQ→Sequence, PRI→Primary, SEC→Secondary, ACT→Active, SUS→Suspended, TRM→Terminated, PDG→Pending, CLS→Closed

**Step 1.5 — Domain Partitioning**
- Run Louvain community detection on the Neo4j graph to produce initial domain clusters
- Store suggested clusters as temporary node properties
- Invoke LLM domain validation agent (see LLM Loop 1 below)
- Store finalised domain assignments as permanent node properties on table nodes
- Allow manual override via config file: {table_name: domain_override}

---

### Layer 2 — PII Detection & LLM Semantic Reasoning

**Step 2.1 — Presidio PII Scan (runs first, before any LLM involvement)**
- For every column, pass sample values (up to 100 rows) to Presidio structured data analyser
- Use en_core_web_lg spaCy model for NER
- Enable all built-in recognisers: PERSON, EMAIL_ADDRESS, PHONE_NUMBER, CREDIT_CARD, IBAN_CODE, IP_ADDRESS, NRP, LOCATION, DATE_TIME, MEDICAL_LICENSE, URL
- Add custom recognisers for telecom (see Step 2.2)
- For each flagged column: store {pii_type, confidence, presidio_masked: true} in PostgreSQL column_policy table
- Presidio-flagged columns are routed directly to masking strategy — LLM does not process them
- Columns Presidio passes OR marks uncertain (confidence < 0.7) go to the LLM queue

**Step 2.2 — Custom Presidio Recognisers (built during onboarding)**
Build and register these custom pattern recognisers in Presidio:
- SUBSCRIBER_ID: regex pattern for subscriber/account identifiers specific to client
- IMSI: 15-digit numeric IMSI pattern
- NETWORK_ELEMENT_ID: client-specific network node ID patterns
- INTERNAL_ACCOUNT_CODE: client-specific internal account reference patterns
Each recogniser should include context words that increase confidence when found nearby (e.g. "subscriber", "account", "IMSI")

**Step 2.3 — Abbreviation Resolution (runs before LLM reasoning)**
For every column name that reaches the LLM queue:
- Tokenise the column name by underscore delimiter
- For each token, query Neo4j abbreviation dictionary node
- For tokens not found in dictionary, attempt contextual inference: look at other columns in the same table for context clues
- If token remains unresolved: flag column as ABBREVIATION_UNKNOWN alongside normal processing
- Run value pattern analysis regardless of abbreviation resolution: unique value count, frequency distribution, value length distribution, regex pattern matching on top values
- Pass expanded column name + value pattern summary to LLM

**Step 2.4 — LLM Semantic Reasoning Agent**

This is the core intelligence component. Build a LangChain tool-calling agent with the following tools:

```
Tool: get_table_schema(table_name: str) -> dict
  Cypher: MATCH (t:Table {name: table_name})-[:HAS_COLUMN]->(c:Column) RETURN t, collect(c)
  Returns: table metadata + all column names, types, statistical profiles

Tool: get_relationships(table_name: str) -> list
  Cypher: MATCH (t:Table {name: table_name})-[r:RELATES_TO]-(other:Table) RETURN r, other
  Returns: all relationships (incoming and outgoing) with confidence scores

Tool: get_downstream_tables(table_name: str) -> list
  Cypher: MATCH path=(t:Table {name: table_name})-[:RELATES_TO*1..3]->(downstream:Table) RETURN downstream
  Returns: all tables downstream up to 3 hops

Tool: get_domain(table_name: str) -> str
  Cypher: MATCH (t:Table {name: table_name}) RETURN t.domain
  Returns: domain assignment for context

Tool: get_abbreviation(token: str) -> str | null
  Cypher: MATCH (a:AbbreviationDict) RETURN a[token]
  Returns: expanded form or null if unknown

Tool: write_column_policy(column_id: str, policy: ColumnPolicySchema) -> bool
  Writes validated LLM output to PostgreSQL column_policy table
  Validates against Pydantic ColumnPolicySchema before writing
```

The LLM agent receives per column:
- Expanded column name (post abbreviation resolution)
- Data type
- Statistical profile summary
- Top 10 sample values with frequencies
- Presidio result (passed or uncertain)
- Flag: ABBREVIATION_UNKNOWN if applicable

The LLM must output structured JSON conforming to this Pydantic schema:

```python
class ColumnPolicySchema(BaseModel):
    column_name: str
    table_name: str
    pii_classification: Literal["none", "sensitive_business", "uncertain"]
    sensitivity_reason: str  # plain English explanation
    masking_strategy: Literal[
        "passthrough",           # not sensitive, use as-is for GAN training
        "substitute_realistic",  # replace with realistic fake values
        "format_preserving",     # replace preserving format/structure
        "suppress",              # omit entirely from synthetic output
        "generalise"             # replace with range or category
    ]
    constraint_profile: dict  # {min, max, regex, allowed_values, distribution_hint}
    business_importance: Literal["critical", "important", "low"]
    # critical = downstream AI pipelines depend on this column's distribution
    # important = part of business logic but not pipeline-critical
    # low = identifiers, labels, non-impactful columns
    edge_case_flags: list[str]  # describe rare-but-important scenarios
    dedup_mode: Literal["entity", "reference", "event"]
    # entity = unique records required (customers, accounts)
    # reference = repeats valid and expected (status codes, plan types)
    # event = FK columns repeat, full records unique (transactions, calls)
    llm_confidence: float  # 0.0 to 1.0
    abbreviation_resolved: bool
    notes: str  # any reasoning the LLM wants to record
```

Retry logic:
- If Pydantic validation fails: retry up to 3 times with the validation error included in the prompt
- If confidence < 0.6 after 3 retries: route to human_review_queue table in PostgreSQL, do not block pipeline
- If ABBREVIATION_UNKNOWN was flagged: route to human_review_queue regardless of confidence

**Step 2.5 — Human Review Queue**
PostgreSQL table: human_review_queue
Columns: id, table_name, column_name, llm_best_guess (JSON), flag_reason, status (pending/approved/corrected), reviewer_notes, reviewed_at
Build a simple FastAPI endpoint that:
- GET /review-queue: returns all pending items
- POST /review-queue/{id}/approve: approves LLM output, writes to column_policy
- POST /review-queue/{id}/correct: accepts corrected JSON, writes to column_policy
- POST /review-queue/{id}/abbreviation: accepts abbreviation expansion, writes to Neo4j abbreviation dictionary node

**Step 2.6 — Generation Strategy Planner**
After all columns in a domain are classified, invoke LLM one more time with domain-level context:
- Read all column policies for the domain from PostgreSQL
- LLM identifies: temporal dependencies between columns, columns with bimodal/multimodal distributions, tables below 200 rows with complex patterns, tables needing post-generation constraint injection
- Output: GenerationStrategySchema per table stored in PostgreSQL generation_strategy table

```python
class GenerationStrategySchema(BaseModel):
    table_name: str
    domain: str
    tier_override: Literal["ctgan", "tvae", "rule_based", "hybrid"] | None
    # None = use default row-count-based tier
    temporal_constraints: list[dict]
    # [{earlier_column: "ORDER_DATE", later_column: "SHIP_DATE"}]
    post_generation_rules: list[str]
    # plain English rules enforced after generation
    edge_case_injection_pct: float
    # % of generated records that should be edge cases (0.0 to 0.3)
    notes: str
```

---

### PostgreSQL Schema — Operational Memory

Create these tables:

```sql
CREATE TABLE column_policy (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR NOT NULL,
    column_name VARCHAR NOT NULL,
    pii_classification VARCHAR,
    pii_source VARCHAR, -- "presidio" or "llm"
    sensitivity_reason TEXT,
    masking_strategy VARCHAR,
    constraint_profile JSONB,
    business_importance VARCHAR,
    edge_case_flags JSONB,
    dedup_mode VARCHAR,
    llm_confidence FLOAT,
    abbreviation_resolved BOOLEAN,
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(table_name, column_name)
);

CREATE TABLE generation_strategy (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR NOT NULL UNIQUE,
    domain VARCHAR,
    tier_override VARCHAR,
    temporal_constraints JSONB,
    post_generation_rules JSONB,
    edge_case_injection_pct FLOAT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE boundary_key_registry (
    id SERIAL PRIMARY KEY,
    domain VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    primary_key_column VARCHAR NOT NULL,
    generated_key_value VARCHAR NOT NULL,
    generation_run_id VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE dedup_hash_registry (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR NOT NULL,
    record_hash VARCHAR NOT NULL,
    generation_run_id VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(table_name, record_hash)
);

CREATE TABLE generation_run_log (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR NOT NULL UNIQUE,
    status VARCHAR, -- running / completed / failed / partial
    domains_completed JSONB,
    domains_pending JSONB,
    validation_results JSONB,
    started_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP
);

CREATE TABLE human_review_queue (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR,
    column_name VARCHAR,
    llm_best_guess JSONB,
    flag_reason VARCHAR,
    status VARCHAR DEFAULT 'pending',
    reviewer_notes TEXT,
    reviewed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE model_registry (
    id SERIAL PRIMARY KEY,
    domain VARCHAR NOT NULL,
    model_path VARCHAR NOT NULL,
    trained_on_run_id VARCHAR,
    row_count_at_training INT,
    trained_at TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);
```

---

### Layer 3 — Synthetic Generation Engine

**Step 3.1 — Table Tier Router**
For each table in the domain being generated:
1. Check PostgreSQL generation_strategy for tier_override
2. If override exists: use it
3. If no override: use row count — >2000 = CTGAN, 200-2000 = TVAE, <200 = rule_based
4. Log tier assignment to generation_run_log

**Step 3.2 — Pre-generation Masking**
Before passing any data to CTGAN/TVAE training:
- For every column with masking_strategy != "passthrough":
  - Apply the masking strategy to the real data sample
  - substitute_realistic: use Faker with locale='en_IN' (or client locale) for names, emails, phones etc.
  - format_preserving: replace values maintaining regex pattern from constraint_profile
  - suppress: drop column from training data
  - generalise: replace with range bucket or category label
- CTGAN/TVAE trains ONLY on masked data — never on raw PII

**Step 3.3 — CTGAN/TVAE Training**
For tables routed to CTGAN:
- Use SDV's SingleTableSynthesiser with CTGANSynthesiser
- Set epochs=300 for POC (tune based on dataset size)
- Pass column metadata: specify which columns are categorical, which are numerical
- Apply temporal constraints from generation_strategy as SDV constraints
- Train on masked data sample
- Save trained model to disk: models/{domain}/{table_name}_ctgan_{run_id}.pkl
- Register in model_registry table

For tables routed to TVAE:
- Use SDV's SingleTableSynthesiser with TVAESynthesiser
- Same process as CTGAN

For tables routed to rule_based:
- Generate records by sampling from constraint_profile (min, max, allowed_values, regex)
- No neural network involved

For multi-table generation within a domain:
- Use SDV's HMASynthesiser for the full domain
- Provide the table hierarchy derived from Neo4j relationships
- Generate in parent-to-child order

**Step 3.4 — Junction Table Generation**
After parent tables in a domain are generated:
- Identify junction tables: tables with exactly 2 FK columns and minimal own attributes
- Analyse real junction table: compute distribution of relationship multiplicity (how many child records per parent)
- Generate junction records procedurally: for each generated parent record, sample a multiplicity count from the real distribution, then sample that many records from the other parent table's generated keys
- Apply dedup_mode from column_policy — junction tables are typically "event" mode

**Step 3.5 — Boundary Key Registry Update**
After each domain completes generation:
- Extract all generated primary key values from entity tables
- Write to boundary_key_registry with domain, table_name, pk_column, pk_value, run_id
- Next domain reads valid FK values from this registry before generating

**Step 3.6 — Edge Case Injection**
After baseline generation for each table:
- Read edge_case_flags from column_policy for all columns in the table
- Read edge_case_injection_pct from generation_strategy
- Generate additional records specifically targeting the flagged combinations
- Use conditional sampling: force CTGAN to generate records where specified column conditions are met
- Append edge case records to the main generated dataset
- Tag edge case records with edge_case: true in a metadata column for tracking

**Step 3.7 — Deduplication**
After generation, before validation:
- Read dedup_mode from column_policy for each table
- entity mode: hash every record, check against dedup_hash_registry, discard and regenerate duplicates until unique
- reference mode: skip deduplication entirely, allow all repeats
- event mode: hash full record, allow FK column repeats, deduplicate only full record matches
- Write new hashes to dedup_hash_registry with run_id

---

### Layer 4 — Validation Gate

Run all four checks in sequence. Any failure triggers LLM diagnosis before retry.

**Check 4.1 — Statistical Fidelity**
For every column where business_importance is "critical" or "important":
- Run two-sample KS test (real masked vs synthetic): reject if p-value < 0.05
- Compute Jensen-Shannon Divergence: reject if JSD > 0.15
- For categorical columns: run chi-squared test on value frequency distributions
- Produce per-column pass/fail report

On failure:
- Write failure report to generation_run_log
- Invoke LLM diagnosis agent with failure report
- LLM outputs corrected GenerationStrategySchema
- Update PostgreSQL generation_strategy
- Trigger re-generation of affected domain only
- Max 3 retries before marking as manual_review_required

**Check 4.2 — PII Leakage Scan**
- Run full Presidio scan (same config as Layer 2) over entire generated dataset
- Flag any record where Presidio detects PII in a column that should be masked
- Remove flagged records
- Compute re-identification risk score: for each synthetic record, measure similarity to nearest real record using Hamming distance on categorical columns and normalised distance on numerical columns
- Records with risk score > 0.85: remove and regenerate

**Check 4.3 — Lineage Integrity**
- For every FK relationship in Neo4j:
  - Verify every child column value exists in the parent column of generated data
  - Verify junction tables have valid records on both sides
- For every temporal constraint in generation_strategy:
  - Verify column ordering holds (ORDER_DATE < SHIP_DATE for all records)
- Report any violations with table and row counts

**Check 4.4 — Business Rule Assertions**
- Read post_generation_rules from generation_strategy for each table
- Translate rules to Great Expectations expectations
- Run expectation suite against generated data
- Report pass/fail per assertion

**Delivery**
If all four checks pass:
- Export generated data to Parquet per table (or CSV if client requires)
- Generate delivery manifest: {run_id, tables_generated, row_counts, validation_results, edge_case_coverage, generation_strategies_used, timestamp}
- Compress and package for PLE delivery

---

### LLM Agent System Prompts

**System prompt for all LLM calls:**
```
You are a data engineering specialist with deep expertise in telecom business systems and enterprise database schemas. You reason carefully about what data means in business context, not just what it looks like syntactically.

You have access to tools to traverse a Neo4j knowledge graph. Always use these tools to gather context before making decisions. Never assume — always verify by querying the graph.

You output ONLY valid JSON conforming to the schema you are given. No preamble, no explanation outside the JSON. If you are uncertain, express that uncertainty as a low llm_confidence score and a detailed notes field.

Security constraint: You are working with metadata and statistics only. You never see real customer values. Sample values shown to you have been pre-screened by Presidio. Do not attempt to reconstruct or identify real individuals from any data shown to you.
```

**Column reasoning prompt template:**
```
Reason about the following database column and produce a complete ColumnPolicySchema JSON.

Table: {table_name}
Column: {column_name_expanded} (original: {column_name_raw})
Data type: {data_type}
Statistical profile: {statistical_profile}
Top values with frequencies: {top_values}
Presidio result: {presidio_result}
Abbreviation resolution status: {abbreviation_status}

Use your graph traversal tools to understand:
1. What this table's purpose is
2. What tables reference this table and what tables this table references
3. Whether similar columns exist in the schema to inform your reasoning
4. What domain this table belongs to

Then produce the ColumnPolicySchema JSON.
```

---

### Configurable Model Layer

Build a model abstraction layer so the LLM provider is swappable via config:

```python
# config.yaml
llm:
  provider: "gemini"          # options: gemini, ollama
  model: "gemini-1.5-pro"    # or "gemma:4b" for ollama
  ollama_base_url: "http://localhost:11434"  # used if provider=ollama
  temperature: 0.1
  max_tokens: 2048
  structured_output: true

# To switch to local Gemma 4B:
# provider: "ollama"
# model: "gemma:4b"
```

Build a ModelClient class with a single .invoke(prompt, output_schema) method. Internally routes to either Gemini API or Ollama REST endpoint based on config. The rest of the system never imports Gemini or Ollama directly — always uses ModelClient.

---

### Project Structure

```
synthetic-data-gen/
├── config/
│   ├── config.yaml              # LLM provider, DB connections, thresholds
│   └── domain_overrides.yaml    # Manual domain partition overrides
├── ingestion/
│   ├── spark_connector.py       # Spark JDBC connections
│   ├── sqlglot_parser.py        # DDL parsing and relationship extraction
│   └── querylog_miner.py        # Query log JOIN pattern extraction
├── graph/
│   ├── neo4j_builder.py         # Build and update Neo4j graph
│   ├── graph_tools.py           # LangChain Neo4j tool definitions
│   └── domain_partitioner.py   # Louvain + LLM validation
├── intelligence/
│   ├── presidio_scanner.py      # Presidio PII detection + custom recognisers
│   ├── abbreviation_resolver.py # Abbreviation expansion logic
│   ├── llm_agent.py             # LangChain tool-calling agent
│   ├── human_review_api.py      # FastAPI review queue endpoints
│   └── strategy_planner.py     # Domain-level generation strategy
├── generation/
│   ├── tier_router.py           # Table tier classification and routing
│   ├── masking_engine.py        # Pre-training masking strategies
│   ├── ctgan_trainer.py         # CTGAN/TVAE training and generation
│   ├── junction_handler.py      # Many-to-many procedural generation
│   ├── edge_case_engine.py      # Edge case injection
│   └── dedup_registry.py       # Deduplication with mode awareness
├── validation/
│   ├── statistical_check.py     # KS test + JSD
│   ├── pii_scan.py              # Presidio second pass + re-ID risk
│   ├── lineage_check.py         # Neo4j FK verification
│   └── business_rules.py       # Great Expectations assertions
├── delivery/
│   └── packager.py              # Export + manifest generation
├── db/
│   ├── postgres_schema.sql      # All PostgreSQL table definitions
│   └── postgres_client.py      # DB connection and query helpers
├── models/
│   └── schemas.py               # All Pydantic schemas
├── llm/
│   └── model_client.py          # Configurable LLM abstraction layer
└── main.py                      # Orchestration entry point
```

---

### Orchestration Flow — main.py

```
1. Load config
2. Connect to source SQL databases via Spark
3. Run SQLGlot parser → extract relationships
4. Run query log miner (if logs available) → add inferred relationships
5. Build Neo4j knowledge graph
6. Run domain partitioner (Louvain + LLM validation) → write domain assignments to Neo4j
7. For each domain (in dependency order — domains with no upstream dependencies first):
   a. Run Presidio on all columns in domain → write flagged columns to PostgreSQL
   b. Run abbreviation resolution on non-Presidio columns
   c. Run LLM semantic reasoning agent on non-Presidio columns → write to PostgreSQL
   d. Check human_review_queue — pause if items pending (configurable: wait or skip)
   e. Run generation strategy planner → write to PostgreSQL
   f. Run table tier router → assign generation method per table
   g. Apply pre-generation masking to training data
   h. Train CTGAN/TVAE per table (or load saved model if exists and no drift)
   i. Generate synthetic records
   j. Generate junction tables procedurally
   k. Update boundary key registry
   l. Inject edge cases
   m. Apply deduplication per table mode
8. Run validation gate (all 4 checks):
   a. Statistical fidelity check
   b. PII leakage scan
   c. Lineage integrity check
   d. Business rule assertions
   e. If any fail: invoke LLM diagnosis → update strategy → retry (max 3)
9. Package and deliver to PLE
10. Write final generation_run_log entry
```

---

### POC Acceptance Criteria

The POC is successful when:
- [ ] System ingests a 3-domain SQL schema with at least 15 tables successfully
- [ ] Presidio correctly flags all standard PII columns without LLM involvement
- [ ] LLM correctly classifies business-sensitive non-PII columns with >80% accuracy (validated by human review)
- [ ] Abbreviation resolution correctly expands >70% of enterprise abbreviations from the seeded dictionary
- [ ] CTGAN generates synthetic data where KS test p-value > 0.05 for all critical columns
- [ ] All FK relationships hold in generated data (0 orphaned records)
- [ ] Presidio PII leakage scan finds 0 real PII values in generated output
- [ ] Edge case records comprise at least the configured percentage of output
- [ ] Deduplication correctly applies entity/reference/event mode per table
- [ ] LLM model provider switches from Gemini to local with config change only
- [ ] System resumes correctly from last completed domain after simulated crash
- [ ] Human review queue correctly receives and processes flagged columns

---

### Phase 2 — Hardening (Post-POC)

- Apache Flink production anomaly interceptor
- Structural fingerprint capture (zero PII)
- Anomaly replay engine with LLM perturbation design
- Near-duplicate detection via pgvector similarity search
- Schema drift detection with LLM self-healing
- Many-to-many junction table augmentation improvements
- Query log mining at scale (distributed Spark job)

### Phase 3 — Production

- Swap Gemini API → local Gemma 4B via Ollama (config change only)
- QLoRA fine-tuning of Gemma 4B on telecom schema patterns
- Pipeline smoke test integration for AI model validation
- Full legacy schema support (tables with 100+ relationships)
- Real-time generation API (FastAPI endpoint)
- All 6 LLM reasoning loops production-hardened
- Monitoring dashboard for generation run health

---

*End of master prompt. Paste this entire document into your AI IDE as the project specification.*
