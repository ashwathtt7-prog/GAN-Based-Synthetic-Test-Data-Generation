"""
Pipeline Orchestrator
Master coordinator that executes all 4 layers end-to-end:
  1. Schema Ingestion & Knowledge Graph Construction
  2. PII Detection & LLM Semantic Reasoning
  3. Synthetic Generation Engine
  4. Validation Gate & Delivery
Supports crash recovery via generation_run_log.
"""

import ast
import logging
import os
import re

for env_var in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ.setdefault(env_var, "1")

import uuid
import pandas as pd
from datetime import datetime

from config.config import load_config

# Layer 1 — Ingestion
from ingestion.schema_connector import SchemaConnector
from ingestion.sqlglot_parser import DDLParser
from ingestion.querylog_miner import QueryLogMiner

# Layer 1 — Graph (NetworkX in-memory, no external Neo4j server needed)
from graph.knowledge_graph import get_knowledge_graph

# Layer 2 — Intelligence
from intelligence.presidio_scanner import PresidioScanner
from intelligence.abbreviation_resolver import AbbreviationResolver
from intelligence.llm_agent import LLMAgent
from intelligence.strategy_planner import StrategyPlanner
from intelligence.failure_diagnosis import FailureDiagnosisAgent

# Layer 3 — Synthesis
from synthesis.tier_router import TierRouter
from synthesis.masking_engine import MaskingEngine
from synthesis.ctgan_model import CTGANModel
from synthesis.tvae_model import TVAEModel
from synthesis.rule_based_generator import RuleBasedGenerator
from synthesis.junction_handler import JunctionHandler
from synthesis.edge_case_engine import EdgeCaseEngine
from synthesis.dedup_registry import DedupEngine
from synthesis.structural_generator import StructuralColumnGenerator
from synthesis.table_profile import build_generation_profile

# Layer 4 — Validation
from synthesis.data_validator import DataValidator

# Delivery
from delivery.packager import DeliveryPackager

# DB
from db.client import DatabaseClient
import db.schema as db_models
from models.schemas import ColumnPolicySchema

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    def __init__(self):
        self.config = load_config()
        self.db_client = DatabaseClient()
        self.db_client.initialize()
        self.run_id = None

        # Track generated data for cross-table FK stitching
        self.generated_data = {}       # {table_name: DataFrame}
        self.column_policies_cache = {}  # {table_name: [ColumnPolicySchema]}
        self.strategies_cache = {}     # {table_name: GenerationStrategySchema}
        self.table_profiles_cache = {}   # {table_name: TableGenerationProfile}
        self.generated_tiers = {}       # {table_name: str}

    def initialize_run(self) -> str:
        self.run_id = str(uuid.uuid4())
        self.generated_data = {}
        self.column_policies_cache = {}
        self.strategies_cache = {}
        self.table_profiles_cache = {}
        self.generated_tiers = {}
        with self.db_client.session() as session:
            run = db_models.PipelineRun(
                run_id=self.run_id,
                status="initialized",
                current_step="Starting"
            )
            session.add(run)
            session.commit()
        return self.run_id

    def _update_status(self, step: str, progress: float, status: str = "running"):
        with self.db_client.session() as session:
            run = session.query(db_models.PipelineRun).filter_by(run_id=self.run_id).first()
            if run:
                run.current_step = step
                run.progress_pct = progress
                run.status = status
                if status in {"completed", "failed", "cancelled"}:
                    run.ended_at = datetime.utcnow()
                session.commit()

    def _log_step(self, step_name: str, table_name: str = None, domain: str = None,
                  status: str = "completed", details: dict = None, duration: float = None):
        """Log a pipeline step to the step log for frontend visibility."""
        try:
            with self.db_client.session() as session:
                log = db_models.PipelineStepLog(
                    run_id=self.run_id,
                    step_name=step_name,
                    table_name=table_name,
                    domain=domain,
                    status=status,
                    details=details or {},
                    duration_seconds=duration,
                    completed_at=datetime.utcnow() if status in ("completed", "failed") else None,
                )
                session.add(log)
                session.commit()
        except Exception as e:
            logger.debug(f"Failed to log step {step_name}: {e}")

    def execute_pipeline(self, run_id: str, table_filter: list[str] = None, fast_mode: bool = False):
        """
        Execute the full 4-layer pipeline.

        Args:
            run_id: Pipeline run identifier
            table_filter: Optional list of table names to process (for testing).
                          If provided, only these tables and their relationships are processed.
        """
        self.run_id = run_id
        try:
            smoke_test_mode = bool(fast_mode)
            # ================================================================
            # Phase 1: Schema Ingestion (Step 1.1 - 1.3)
            # ================================================================
            self._update_status("Schema Ingestion", 2.0)
            logger.info("=== Phase 1: Schema Ingestion ===")

            self._log_step("schema_ingestion", status="running", details={"phase": "starting"})
            source_url = self.config['data_sources'][0]['connection_string']
            connector = SchemaConnector(source_url)
            tables = connector.extract_schema()

            ddl_dir = self.config['ingestion']['ddl_directory']
            ddl_parser = DDLParser(ddl_dir)
            explicit_rels = ddl_parser.parse_relationships()

            query_log_dir = self.config['ingestion']['query_log_directory']
            miner = QueryLogMiner(query_log_dir)
            implicit_rels = miner.mine_relationships()

            all_rels = explicit_rels + implicit_rels
            logger.info(f"Extracted {len(tables)} tables, {len(all_rels)} relationships")
            self._log_step("schema_ingestion", status="completed",
                          details={"tables": len(tables), "relationships": len(all_rels)})

            # Apply table filter if provided (for small-table testing)
            if table_filter:
                filter_set = {t.upper() for t in table_filter}
                tables = [t for t in tables if t.table_name.upper() in filter_set]
                all_rels = [r for r in all_rels
                            if r.source_table.upper() in filter_set
                            and r.target_table.upper() in filter_set]
                logger.info(f"Table filter applied: {len(tables)} tables, {len(all_rels)} relationships")

            # ================================================================
            # Phase 2: Knowledge Graph Construction (Step 1.4 - 1.5)
            # ================================================================
            self._update_status("Knowledge Graph Construction", 10.0)
            logger.info("=== Phase 2: Knowledge Graph ===")

            kg = get_knowledge_graph()
            kg.build_graph(tables, all_rels)
            self._log_step("knowledge_graph_build", status="completed",
                          details={"tables": len(tables), "relationships": len(all_rels)})

            # ================================================================
            # Phase 3: Domain Partitioning (Louvain on knowledge graph)
            # ================================================================
            self._update_status("Domain Partitioning", 18.0)
            logger.info("=== Phase 3: Domain Partitioning ===")

            try:
                domain_map = kg.partition_domains()
            except Exception as e:
                logger.warning(f"Louvain partitioning failed, using heuristic: {e}")
                domain_map = {}

            self._log_step("domain_partitioning", status="completed",
                          details={"domains": list(set(domain_map.values())) if domain_map else []})
            # Fallback heuristic domain assignment if partitioning returned empty
            if not domain_map:
                for t in tables:
                    name = t.table_name.upper()
                    if any(kw in name for kw in ['CUST', 'SUBSCR', 'SVC_PLAN', 'ADDR', 'CNTCT', 'IDENT', 'STAT_HIST']):
                        domain_map[t.table_name] = "customer_management"
                    elif any(kw in name for kw in ['BLNG', 'INVC', 'PYMT', 'USAGE', 'CDR']):
                        domain_map[t.table_name] = "billing_revenue"
                    elif any(kw in name for kw in ['NTWK', 'CELL', 'SVC_ORD', 'WRK_ORD', 'INCDT', 'FIELD', 'AGT']):
                        domain_map[t.table_name] = "network_operations"
                    else:
                        domain_map[t.table_name] = "general"

            # Cache table metadata in operational DB
            with self.db_client.session() as session:
                for t in tables:
                    t_meta = session.query(db_models.TableMetadataRecord).filter_by(table_name=t.table_name).first()
                    if not t_meta:
                        t_meta = db_models.TableMetadataRecord(table_name=t.table_name)
                        session.add(t_meta)
                    t_meta.row_count = t.row_count
                    t_meta.column_count = t.column_count
                    t_meta.domain = domain_map.get(t.table_name)
                session.commit()

            # Create generation run log
            domains_list = list(set(domain_map.values()))
            with self.db_client.session() as session:
                self.db_client.create_run_log(session, self.run_id, domains_list)

            # ================================================================
            # Phase 4: Intelligence & Semantic Reasoning (Layer 2)
            # ================================================================
            self._update_status("PII Detection & Semantic Reasoning", 25.0)
            logger.info("=== Phase 4: Intelligence ===")

            presidio = PresidioScanner(self.config)
            abbrev = AbbreviationResolver()
            agent = LLMAgent()
            planner = StrategyPlanner()
            confidence_threshold = self.config['llm'].get('confidence_threshold', 0.6)

            total_tables = len(tables)
            for i, table in enumerate(tables):
                progress = 25.0 + (i / total_tables * 35.0)
                self._update_status(f"Classifying {table.table_name}", progress)
                logger.info(f"Processing table {i+1}/{total_tables}: {table.table_name}")

                cached_policies = self._load_existing_policies(table.table_name)
                table_policies = []
                for col in table.columns:
                    cached_policy = cached_policies.get(col.column_name)
                    if cached_policy:
                        table_policies.append(cached_policy)
                        self._log_step(
                            "policy_cache_hit",
                            table_name=table.table_name,
                            status="completed",
                            details={"column": col.column_name},
                        )
                        continue

                    # Step 2.3: Abbreviation Resolution
                    try:
                        exp_name, fully_resolved = abbrev.resolve_column_name(col.column_name)
                    except Exception:
                        exp_name = col.column_name
                        fully_resolved = False

                    # Step 2.1: Presidio PII Scan
                    try:
                        sample_vals = self._get_column_sample(connector, table.table_name, col.column_name)
                    except Exception:
                        sample_vals = []

                    pres_result = presidio.scan_column(table.table_name, col.column_name, sample_vals)

                    # Master prompt rule: Presidio-flagged columns bypass LLM entirely
                    if pres_result.pii_detected and pres_result.confidence >= self.config['presidio'].get('confidence_threshold', 0.7):
                        masking_strategy = presidio.is_pii_passthrough(pres_result.pii_type)
                        policy = ColumnPolicySchema(
                            column_name=col.column_name,
                            table_name=table.table_name,
                            pii_classification="sensitive_business",
                            sensitivity_reason=f"Presidio detected {pres_result.pii_type} (conf={pres_result.confidence})",
                            masking_strategy=masking_strategy,
                            constraint_profile={},
                            business_importance="important",
                            edge_case_flags=[],
                            dedup_mode="entity",
                            llm_confidence=1.0,
                            abbreviation_resolved=fully_resolved,
                            notes=f"Auto-classified by Presidio. PII type: {pres_result.pii_type}"
                        )
                        # Save with pii_source = "presidio"
                        self._save_column_policy(policy, pii_source="presidio")
                        table_policies.append(policy)
                        self._log_step("pii_detection", table_name=table.table_name, status="completed",
                                      details={"column": col.column_name, "source": "presidio",
                                               "pii_type": pres_result.pii_type, "masking": masking_strategy})
                        continue

                    # Step 2.4: LLM Semantic Reasoning (for non-PII or uncertain columns)
                    policy = agent.classify_column(
                        table_name=table.table_name,
                        column_name_raw=col.column_name,
                        column_name_expanded=exp_name,
                        data_type=col.data_type,
                        statistical_profile=str(col.model_dump()),
                        top_values=str(col.top_values),
                        presidio_result=str(pres_result.model_dump()),
                        abbreviation_status=str(fully_resolved)
                    )

                    # Queue low confidence for human review
                    if policy.llm_confidence < confidence_threshold:
                        self._queue_for_review(
                            table.table_name, col.column_name, policy, "Low Confidence Score"
                        )

                    # Queue unresolved abbreviations for human review
                    if not fully_resolved:
                        self._queue_for_review(
                            table.table_name, col.column_name, policy, "ABBREVIATION_UNKNOWN"
                        )

                    self._save_column_policy(policy, pii_source="llm")
                    table_policies.append(policy)
                    self._log_step("llm_reasoning", table_name=table.table_name, status="completed",
                                  details={"column": col.column_name, "pii": policy.pii_classification,
                                           "masking": policy.masking_strategy, "confidence": policy.llm_confidence,
                                           "reason": (policy.sensitivity_reason or "")[:200]})

                # Step 2.6: Generation Strategy
                domain = domain_map.get(table.table_name, "unknown")
                strategy = self._load_existing_strategy(table.table_name)
                if strategy:
                    self._log_step(
                        "strategy_cache_hit",
                        table_name=table.table_name,
                        domain=domain,
                        status="completed",
                        details={"tier_override": strategy.tier_override},
                    )
                else:
                    strategy = planner.generate_strategy(table.table_name, domain, table_policies)
                    self._save_generation_strategy(strategy)

                self.column_policies_cache[table.table_name] = table_policies
                self.strategies_cache[table.table_name] = strategy

            # ================================================================
            # Phase 5: Synthetic Generation Engine (Layer 3)
            # ================================================================
            self._update_status("Synthetic Data Generation", 65.0)
            logger.info("=== Phase 5: Synthesis ===")

            tier_router = TierRouter(self.config)
            masking_engine = MaskingEngine(self.config['generation'].get('faker_locale', 'en_US'))
            structural_generator = StructuralColumnGenerator(random_seed=42)
            junction_handler = JunctionHandler()
            edge_case_engine = EdgeCaseEngine()
            dedup_engine = DedupEngine(self.db_client)
            ctgan_epochs = self.config['generation'].get('ctgan_epochs', 300)
            tvae_epochs = self.config['generation'].get('tvae_epochs', 300)
            model_save_dir = self.config['generation'].get('model_save_dir', 'models/trained')

            # Sort tables by dependency order: parent tables first
            sorted_tables = self._topological_sort(tables, all_rels)

            for idx, table in enumerate(sorted_tables):
                table_name = table.table_name
                progress = 65.0 + (idx / len(sorted_tables) * 15.0)
                self._update_status(f"Generating {table_name}", progress)

                policies = self.column_policies_cache.get(table_name, [])
                strategy = self.strategies_cache.get(table_name)
                domain = domain_map.get(table_name, "unknown")

                if not policies:
                    logger.warning(f"No policies for {table_name}, skipping generation")
                    continue

                # Load source data
                try:
                    source_df = pd.read_sql(f"SELECT * FROM {table_name}", connector.engine)
                except Exception as e:
                    logger.warning(f"Cannot read source data for {table_name}: {e}")
                    continue

                if len(source_df) == 0:
                    logger.info(f"Skipping {table_name} — no source data")
                    continue

                masked_df = masking_engine.mask_dataframe(source_df, policies)
                profile = build_generation_profile(table_name, source_df, masked_df, policies, all_rels)
                self.table_profiles_cache[table_name] = profile
                self._log_step(
                    "table_profile",
                    table_name=table_name,
                    domain=domain,
                    status="completed",
                    details={
                        "fingerprint": profile.fingerprint,
                        "structural_columns": len(profile.structural_columns),
                        "modeled_columns": len(profile.modeled_columns),
                        "sensitive_columns": len(profile.sensitive_columns),
                    },
                )

                # Step 3.1: Tier routing
                tier_override = strategy.tier_override if strategy else None
                tier = tier_router.route(table_name, len(source_df), tier_override, profile=profile)
                if smoke_test_mode and tier != "rule_based":
                    logger.info(
                        "Smoke test mode active for filtered run. Overriding %s tier to rule_based for %s.",
                        tier,
                        table_name,
                    )
                    tier = "rule_based"

                self._log_step("tier_routing", table_name=table_name, domain=domain, status="completed",
                              details={"tier": tier, "row_count": len(source_df),
                                       "tier_override": tier_override, "smoke_test": smoke_test_mode,
                                       "modeled_columns": len(profile.modeled_columns),
                                       "structural_columns": len(profile.structural_columns)})

                # Step 3.3: Train and generate
                num_rows = len(source_df)

                # POC: cap epochs low to avoid blocking — production would use more
                poc_ctgan_epochs = min(ctgan_epochs, 15)
                poc_tvae_epochs = min(tvae_epochs, 15)

                try:
                    synthetic_df, effective_tier, model_reused = self._generate_table_output(
                        table_name=table_name,
                        domain=domain,
                        source_df=source_df,
                        masked_df=masked_df,
                        policies=policies,
                        profile=profile,
                        tier=tier,
                        model_save_dir=model_save_dir,
                        ctgan_epochs=poc_ctgan_epochs,
                        tvae_epochs=poc_tvae_epochs,
                        structural_generator=structural_generator,
                    )

                except Exception as e:
                    logger.error(f"Generation failed for {table_name}: {e}")
                    self._log_step(
                        "generation_failed",
                        table_name=table_name,
                        domain=domain,
                        status="failed",
                        details={"tier": tier, "error": str(e)[:500]},
                    )
                    continue

                synthetic_df = self._apply_shared_repairs(
                    table_name=table_name,
                    synthetic_df=synthetic_df,
                    source_df=source_df,
                    policies=policies,
                    relationships=all_rels,
                    strategy=strategy,
                )

                # Step 3.5: Boundary Key Registry
                self._update_boundary_keys(table_name, domain, synthetic_df, all_rels)

                # Step 3.6: Edge Case Injection
                injection_pct = strategy.edge_case_injection_pct if strategy else 0.05
                synthetic_df = edge_case_engine.inject_edge_cases(
                    table_name, synthetic_df, policies, injection_pct
                )

                # Step 3.7: Deduplication
                dominant_dedup = self._get_dominant_dedup_mode(policies)
                fk_cols = [r.source_column for r in all_rels if r.source_table.upper() == table_name.upper()]
                synthetic_df = dedup_engine.deduplicate(
                    table_name, synthetic_df, dominant_dedup, fk_cols, self.run_id
                )

                # Coerce mixed-type object columns to match source dtypes
                for col in synthetic_df.columns:
                    if col in source_df.columns:
                        try:
                            if pd.api.types.is_numeric_dtype(source_df[col]):
                                synthetic_df[col] = pd.to_numeric(synthetic_df[col], errors='coerce')
                            elif pd.api.types.is_datetime64_any_dtype(source_df[col]):
                                synthetic_df[col] = pd.to_datetime(synthetic_df[col], errors='coerce')
                        except Exception:
                            pass

                self.generated_data[table_name] = synthetic_df
                self.generated_tiers[table_name] = effective_tier
                self._append_completed_table(table_name)
                logger.info(f"Generated {len(synthetic_df)} records for {table_name}")
                self._log_step("generation_complete", table_name=table_name, domain=domain, status="completed",
                              details={"tier": effective_tier, "rows_generated": len(synthetic_df),
                                       "columns": len(synthetic_df.columns), "model_reused": model_reused,
                                       "modeled_columns": len(profile.modeled_columns),
                                       "structural_columns": len(profile.structural_columns)})

            # ================================================================
            # Phase 6: Validation Gate (Layer 4)
            # ================================================================
            self._update_status("Validation Gate", 82.0)
            logger.info("=== Phase 6: Validation ===")

            validation_cfg = self.config.get('validation', {})
            max_retries = validation_cfg.get('max_retry_on_failure', 3)
            retry_diagnosis_enabled = validation_cfg.get('enable_retry_diagnosis', False)
            if smoke_test_mode:
                max_retries = 0
            diagnosis_agent = FailureDiagnosisAgent()

            all_validation_results = {}
            tables_needing_retry = []

            for table_name, synthetic_df in self.generated_data.items():
                self._update_status(f"Validating {table_name}", 82.0)
                policies = self.column_policies_cache.get(table_name, [])
                strategy = self.strategies_cache.get(table_name)

                validator = DataValidator(table_name)

                # Load real (masked) data for comparison
                try:
                    real_df = pd.read_sql(f"SELECT * FROM {table_name}", connector.engine)
                    real_masked = masking_engine.mask_dataframe(real_df, policies)
                except Exception:
                    real_df = pd.DataFrame()
                    real_masked = pd.DataFrame()

                results = []

                # Check 4.1: Statistical Fidelity
                if len(real_masked) > 0:
                    stat_results = validator.validate_statistical_fidelity(
                        real_masked, synthetic_df, policies,
                        ks_alpha=validation_cfg.get('ks_test_alpha', 0.05),
                        jsd_threshold=validation_cfg.get('jsd_threshold', 0.15)
                    )
                    results.extend(stat_results)

                # Check 4.2: PII Leakage
                pii_results = validator.validate_pii_leakage(
                    synthetic_df, real_df if len(real_df) > 0 else synthetic_df,
                    policies, presidio,
                    reid_threshold=validation_cfg.get('reid_risk_threshold', 0.85)
                )
                results.extend(pii_results)

                # Check 4.3: Lineage Integrity
                table_rels = [r for r in all_rels if r.source_table.upper() == table_name.upper()]
                lineage_results = validator.validate_lineage_integrity(
                    synthetic_df, table_rels, self.generated_data, strategy
                )
                results.extend(lineage_results)

                # Check 4.4: Business Rule Assertions
                post_rules = strategy.post_generation_rules if strategy else []
                business_results = validator.validate_business_rules(
                    synthetic_df, post_rules, policies
                )
                results.extend(business_results)

                all_validation_results[table_name] = results

                # Check for failures
                failures = [r for r in results if not r.passed]
                passed_count = len(results) - len(failures)
                if failures:
                    logger.warning(f"Validation failures for {table_name}: {len(failures)}")
                    tables_needing_retry.append(table_name)
                else:
                    logger.info(f"All validations passed for {table_name}")
                self._log_step("validation", table_name=table_name, status="completed",
                              details={"total_checks": len(results), "passed": passed_count,
                                       "failed": len(failures),
                                       "failures": [f.check_name for f in failures][:10]})

            if tables_needing_retry and not retry_diagnosis_enabled:
                self._log_step(
                    "validation_retry_skipped",
                    status="completed",
                    details={
                        "tables": tables_needing_retry,
                        "reason": "retry diagnosis disabled until adaptive regeneration is implemented",
                    },
                )
                tables_needing_retry = []

            # Retry loop for failed tables
            for retry in range(max_retries):
                if not tables_needing_retry:
                    break

                self._update_status(f"Retry {retry + 1}/{max_retries}", 88.0)
                logger.info(f"=== Retry {retry + 1}: {len(tables_needing_retry)} tables ===")

                still_failing = []
                for table_name in tables_needing_retry:
                    # LLM diagnosis
                    strategy = self.strategies_cache.get(table_name)
                    strategy_dict = self._strategy_to_dict(strategy)
                    failed_results = [r for r in all_validation_results.get(table_name, []) if not r.passed]

                    try:
                        diagnosis = diagnosis_agent.diagnose(
                            table_name, domain_map.get(table_name, "unknown"),
                            failed_results, strategy_dict
                        )

                        # If diagnosis includes updated strategy, apply it
                        if diagnosis.updated_strategy:
                            self.strategies_cache[table_name] = diagnosis.updated_strategy
                            self._save_generation_strategy(diagnosis.updated_strategy)

                        logger.info(f"Diagnosis for {table_name}: {diagnosis.root_cause}")
                        self._log_step(
                            "validation_diagnosis",
                            table_name=table_name,
                            status="completed",
                            details={"root_cause": diagnosis.root_cause[:300]},
                        )
                    except Exception as exc:
                        logger.warning("Diagnosis failed for %s: %s", table_name, exc)
                        self._log_step(
                            "validation_diagnosis",
                            table_name=table_name,
                            status="failed",
                            details={"error": str(exc)[:300]},
                        )
                        # Avoid burning all retries on malformed diagnosis output.
                        continue

                    still_failing.append(table_name)

                tables_needing_retry = still_failing

            # ================================================================
            # Phase 7: Delivery
            # ================================================================
            self._update_status("Packaging Delivery", 95.0)
            logger.info("=== Phase 7: Delivery ===")

            packager = DeliveryPackager(self.config)

            edge_case_coverage = {}
            gen_strategies = {}
            for table_name, df in self.generated_data.items():
                if '_edge_case' in df.columns:
                    edge_case_coverage[table_name] = float(df['_edge_case'].mean())
                else:
                    edge_case_coverage[table_name] = 0.0
                gen_strategies[table_name] = self.generated_tiers.get(table_name, "auto")

            manifest = packager.package(
                run_id=self.run_id,
                synthetic_datasets=self.generated_data,
                validation_results=all_validation_results,
                generation_strategies=gen_strategies,
                edge_case_coverage=edge_case_coverage,
                domains=domains_list
            )

            # Update run log
            with self.db_client.session() as session:
                self.db_client.update_run_log(
                    session, self.run_id,
                    status="completed",
                    domains_completed=domains_list,
                    domains_pending=[],
                    tables_completed=list(self.generated_data.keys()),
                    validation_results={
                        t: [r.model_dump() for r in rs]
                        for t, rs in all_validation_results.items()
                    },
                    completed_at=datetime.utcnow()
                )

            self._log_step("delivery", status="completed",
                          details={"output_path": str(manifest.output_path),
                                   "tables_delivered": list(self.generated_data.keys())})

            self._update_status("Completed", 100.0, "completed")
            logger.info(f"Pipeline completed. Manifest: {manifest.output_path}")

        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}", exc_info=True)
            self._update_status(f"Failed: {str(e)[:200]}", 0.0, "failed")

            # Save crash state for recovery
            with self.db_client.session() as session:
                self.db_client.update_run_log(
                    session, self.run_id,
                    status="failed",
                    tables_completed=list(self.generated_data.keys()),
                    completed_at=datetime.utcnow()
                )

    # ====================================================================
    # Helper Methods
    # ====================================================================

    def _get_column_sample(self, connector, table_name: str, col_name: str, limit: int = 100) -> list:
        """Get sample values from a column for Presidio scanning."""
        try:
            df = pd.read_sql(
                f'SELECT "{col_name}" FROM "{table_name}" LIMIT {limit}',
                connector.engine
            )
            return [str(v) for v in df[col_name].dropna().tolist()]
        except Exception:
            return []

    def _save_column_policy(self, policy, pii_source: str = "llm"):
        """Upsert column policy to operational DB."""
        with self.db_client.session() as session:
            data = policy.model_dump()
            data['pii_source'] = pii_source
            self.db_client.upsert_column_policy(session, data)

    def _save_generation_strategy(self, strategy):
        """Upsert generation strategy to operational DB."""
        with self.db_client.session() as session:
            self.db_client.upsert_generation_strategy(session, strategy.model_dump())

    def _load_existing_policies(self, table_name: str) -> dict:
        """Load cached column policies for a table from operational memory."""
        with self.db_client.session() as session:
            records = session.query(db_models.ColumnPolicy).filter_by(table_name=table_name).all()

        policies = {}
        for record in records:
            try:
                payload = {
                    "column_name": record.column_name,
                    "table_name": record.table_name,
                    "pii_classification": record.pii_classification or "uncertain",
                    "sensitivity_reason": record.sensitivity_reason or "",
                    "masking_strategy": record.masking_strategy or "passthrough",
                    "constraint_profile": record.constraint_profile or {},
                    "business_importance": record.business_importance or "low",
                    "edge_case_flags": record.edge_case_flags or [],
                    "dedup_mode": record.dedup_mode or "reference",
                    "llm_confidence": record.llm_confidence or 0.0,
                    "abbreviation_resolved": bool(record.abbreviation_resolved),
                    "notes": record.notes or "",
                }
                policies[record.column_name] = ColumnPolicySchema.model_validate(payload)
            except Exception as exc:
                logger.debug("Failed to hydrate cached policy for %s.%s: %s", table_name, record.column_name, exc)

        return policies

    def _load_existing_strategy(self, table_name: str):
        """Load a cached generation strategy for a table if available."""
        with self.db_client.session() as session:
            return session.query(db_models.GenerationStrategy).filter_by(table_name=table_name).first()

    def _strategy_to_dict(self, strategy):
        """Serialize either a Pydantic or ORM strategy object."""
        if strategy is None:
            return None
        if hasattr(strategy, "model_dump"):
            return strategy.model_dump()
        return {
            "table_name": getattr(strategy, "table_name", None),
            "domain": getattr(strategy, "domain", None),
            "tier_override": getattr(strategy, "tier_override", None),
            "temporal_constraints": getattr(strategy, "temporal_constraints", None) or [],
            "post_generation_rules": getattr(strategy, "post_generation_rules", None) or [],
            "edge_case_injection_pct": getattr(strategy, "edge_case_injection_pct", None),
            "notes": getattr(strategy, "notes", None),
        }

    def _emit_training_metric(self, table_name: str, domain: str, metric: dict):
        """Persist live model training metrics for the dashboard."""
        self._log_step(
            "training_metric",
            table_name=table_name,
            domain=domain,
            status="running",
            details=metric,
        )

    def _append_completed_table(self, table_name: str):
        """Persist generated tables so the dashboard can surface them immediately."""
        with self.db_client.session() as session:
            run_log = session.query(db_models.GenerationRunLog).filter_by(run_id=self.run_id).first()
            if not run_log:
                return

            completed_tables = list(run_log.tables_completed or [])
            if table_name not in completed_tables:
                completed_tables.append(table_name)
                run_log.tables_completed = completed_tables

    def _queue_for_review(self, table_name: str, column_name: str, policy, reason: str):
        """Add a column to the human review queue."""
        with self.db_client.session() as session:
            self.db_client.add_to_review_queue(session, {
                "table_name": table_name,
                "column_name": column_name,
                "llm_best_guess": policy.model_dump(),
                "flag_reason": reason
            })

    def _register_model(self, table_name, domain, path, model_type, row_count, fingerprint=None, profile=None, training_epochs=None):
        """Register a trained model in the model registry."""
        with self.db_client.session() as session:
            column_metadata = {
                "fingerprint": fingerprint,
                "modeled_columns": getattr(profile, "modeled_columns", []),
                "structural_columns": getattr(profile, "structural_columns", []),
            }
            self.db_client.register_model(session, {
                "domain": domain,
                "table_name": table_name,
                "model_type": model_type,
                "model_path": path,
                "trained_on_run_id": self.run_id,
                "row_count_at_training": row_count,
                "column_metadata": column_metadata,
                "training_epochs": training_epochs,
            })

    def _generate_table_output(
        self,
        table_name: str,
        domain: str,
        source_df: pd.DataFrame,
        masked_df: pd.DataFrame,
        policies: list,
        profile,
        tier: str,
        model_save_dir: str,
        ctgan_epochs: int,
        tvae_epochs: int,
        structural_generator: StructuralColumnGenerator,
    ):
        """Generate a table using the shared structural + modeled flow."""
        num_rows = len(source_df)
        structural_df = structural_generator.generate(source_df, profile.structural_columns, num_rows)
        modeled_source_df = self._prepare_modeled_training_frame(source_df, masked_df, profile)

        effective_tier = tier
        model_reused = False

        if tier in {"ctgan", "hybrid"} and not modeled_source_df.empty:
            modeled_synth_df, model_reused = self._generate_ml_columns(
                table_name=table_name,
                domain=domain,
                model_type="ctgan",
                modeled_source_df=modeled_source_df,
                policies=policies,
                profile=profile,
                model_save_dir=model_save_dir,
                epochs=ctgan_epochs,
            )
        elif tier == "tvae" and not modeled_source_df.empty:
            modeled_synth_df, model_reused = self._generate_ml_columns(
                table_name=table_name,
                domain=domain,
                model_type="tvae",
                modeled_source_df=modeled_source_df,
                policies=policies,
                profile=profile,
                model_save_dir=model_save_dir,
                epochs=tvae_epochs,
            )
        else:
            effective_tier = "rule_based"
            modeled_synth_df = self._generate_rule_based_columns(table_name, policies, modeled_source_df, num_rows)

        synthetic_df = self._assemble_generated_table(
            source_df=source_df,
            masked_df=masked_df,
            profile=profile,
            structural_df=structural_df,
            modeled_synth_df=modeled_synth_df,
        )
        return synthetic_df, effective_tier, model_reused

    def _prepare_modeled_training_frame(self, source_df: pd.DataFrame, masked_df: pd.DataFrame, profile) -> pd.DataFrame:
        """Build the ML training slice using source numerics and masked text columns."""
        if not getattr(profile, "modeled_columns", None):
            return pd.DataFrame(index=source_df.index)

        modeled_df = pd.DataFrame(index=source_df.index)
        for column in profile.modeled_columns:
            if column not in source_df.columns and column not in masked_df.columns:
                continue

            source_series = source_df[column] if column in source_df.columns else None
            masked_series = masked_df[column] if column in masked_df.columns else None

            if source_series is not None and (
                pd.api.types.is_numeric_dtype(source_series)
                or pd.api.types.is_datetime64_any_dtype(source_series)
                or pd.api.types.is_bool_dtype(source_series)
            ):
                modeled_df[column] = source_series
            elif masked_series is not None:
                modeled_df[column] = masked_series
            else:
                modeled_df[column] = source_series

        return modeled_df.copy()

    def _generate_ml_columns(
        self,
        table_name: str,
        domain: str,
        model_type: str,
        modeled_source_df: pd.DataFrame,
        policies: list,
        profile,
        model_save_dir: str,
        epochs: int,
    ):
        """Train or reuse a CTGAN/TVAE model for modeled columns only."""
        model_cls = CTGANModel if model_type == "ctgan" else TVAEModel
        model = model_cls(table_name, policies)
        model_match = self._load_compatible_model(table_name, model_type, profile, epochs)
        model_path = os.path.join(model_save_dir, domain, f"{table_name}_{model_type}.pkl")

        if model_match and model_match["match_type"] == "exact":
            cached_model = model_match["model"]
            model.load(cached_model.model_path)
            self._log_step(
                "model_reuse",
                table_name=table_name,
                domain=domain,
                status="completed",
                details={
                    "model_type": model_type,
                    "model_path": cached_model.model_path,
                    "fingerprint": profile.fingerprint,
                    "match_type": "exact",
                },
            )
            reused = True
        else:
            match_type = model_match["match_type"] if model_match else "none"
            planned_epochs = model_match["suggested_epochs"] if model_match else epochs
            training_mode = "near_match_adaptation" if match_type == "near" else "fresh_train"
            self._log_step(
                "training_start",
                table_name=table_name,
                domain=domain,
                status="running",
                details={
                    "model_type": model_type,
                    "epochs": planned_epochs,
                    "match_type": match_type,
                    "training_mode": training_mode,
                    "source_model_path": model_match["model"].model_path if model_match else None,
                },
            )
            try:
                model.train(
                    modeled_source_df,
                    epochs=planned_epochs,
                    emit_metric=lambda metric: self._emit_training_metric(table_name, domain, metric),
                )
            except Exception as exc:
                self._log_step(
                    "training_failed",
                    table_name=table_name,
                    domain=domain,
                    status="failed",
                    details={
                        "model_type": model_type,
                        "epochs": planned_epochs,
                        "match_type": match_type,
                        "training_mode": training_mode,
                        "error": str(exc)[:500],
                    },
                )
                raise
            model.save(os.path.join(model_save_dir, domain))
            self._register_model(
                table_name=table_name,
                domain=domain,
                path=model_path,
                model_type=model_type,
                row_count=len(modeled_source_df),
                fingerprint=profile.fingerprint,
                profile=profile,
                training_epochs=planned_epochs,
            )
            self._log_step(
                "training_complete",
                table_name=table_name,
                domain=domain,
                status="completed",
                details={
                    "model_type": model_type,
                    "epochs": planned_epochs,
                    "model_path": model_path,
                    "match_type": match_type,
                    "training_mode": training_mode,
                },
            )
            reused = False

        return model.generate(len(modeled_source_df)), reused

    def _generate_rule_based_columns(self, table_name: str, policies: list, modeled_source_df: pd.DataFrame, num_rows: int) -> pd.DataFrame:
        """Generate modeled columns through the shared rule-based path."""
        if modeled_source_df.empty:
            return pd.DataFrame(index=range(num_rows))

        generator = RuleBasedGenerator(table_name, policies)
        return generator.generate(modeled_source_df, num_rows)

    def _assemble_generated_table(
        self,
        source_df: pd.DataFrame,
        masked_df: pd.DataFrame,
        profile,
        structural_df: pd.DataFrame,
        modeled_synth_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Merge structural and modeled outputs back into source column order."""
        num_rows = len(source_df)
        assembled = pd.DataFrame(index=range(num_rows))
        fallback_df = masked_df.sample(n=num_rows, replace=True, random_state=42).reset_index(drop=True)

        for column in source_df.columns:
            if column in structural_df.columns:
                assembled[column] = structural_df[column].reset_index(drop=True)
            elif column in modeled_synth_df.columns:
                assembled[column] = modeled_synth_df[column].reset_index(drop=True)
            elif column in fallback_df.columns:
                assembled[column] = fallback_df[column].reset_index(drop=True)
            else:
                assembled[column] = None

        return assembled

    def _load_compatible_model(self, table_name: str, model_type: str, profile, base_epochs: int):
        """Return the best exact or near-match model candidate for a table profile."""
        with self.db_client.session() as session:
            candidates = self.db_client.get_registered_models(session, table_name, model_type=model_type)

        best_match = None
        for model in candidates:
            if not model.model_path or not os.path.exists(model.model_path):
                continue

            metadata = model.column_metadata or {}
            if metadata.get("fingerprint") == profile.fingerprint:
                return {
                    "model": model,
                    "match_type": "exact",
                    "score": 1.0,
                    "suggested_epochs": 0,
                }

            score = self._score_model_compatibility(model, profile)
            if score < 0.75:
                continue

            if not best_match or score > best_match["score"]:
                best_match = {
                    "model": model,
                    "match_type": "near",
                    "score": score,
                    "suggested_epochs": max(5, min(base_epochs, max(5, base_epochs // 3))),
                }

        return best_match

    def _score_model_compatibility(self, model, profile) -> float:
        """Score how reusable an older model is for the current table profile."""
        metadata = model.column_metadata or {}
        modeled_columns = set(metadata.get("modeled_columns") or [])
        structural_columns = set(metadata.get("structural_columns") or [])
        current_modeled = set(getattr(profile, "modeled_columns", []) or [])
        current_structural = set(getattr(profile, "structural_columns", []) or [])

        modeled_overlap = self._jaccard_similarity(modeled_columns, current_modeled)
        structural_overlap = self._jaccard_similarity(structural_columns, current_structural)

        trained_rows = float(getattr(model, "row_count_at_training", 0) or 0)
        current_rows = float(getattr(profile, "row_count", 0) or 0)
        if trained_rows and current_rows:
            row_similarity = 1.0 - min(abs(trained_rows - current_rows) / max(trained_rows, current_rows), 1.0)
        else:
            row_similarity = 0.5

        return round((modeled_overlap * 0.65) + (structural_overlap * 0.25) + (row_similarity * 0.10), 4)

    def _jaccard_similarity(self, left: set[str], right: set[str]) -> float:
        """Compute a simple set similarity for model compatibility checks."""
        if not left and not right:
            return 1.0
        if not left or not right:
            return 0.0
        return len(left & right) / len(left | right)

    def _apply_shared_repairs(self, table_name: str, synthetic_df: pd.DataFrame, source_df: pd.DataFrame, policies: list, relationships: list, strategy) -> pd.DataFrame:
        """Apply cross-tier structural repairs before validation."""
        repaired_df = synthetic_df.copy()
        repaired_df = self._stitch_foreign_keys(table_name, repaired_df, relationships)
        repaired_df = self._enforce_allowed_values(repaired_df, policies)
        repaired_df = self._enforce_temporal_constraints(repaired_df, strategy)
        repaired_df = self._enforce_entity_uniqueness(repaired_df, source_df, policies, relationships)
        return repaired_df

    def _enforce_allowed_values(self, synthetic_df: pd.DataFrame, policies: list) -> pd.DataFrame:
        """Repair columns with explicit allowed values back into the approved set."""
        repaired_df = synthetic_df.copy()
        for policy in policies:
            column = getattr(policy, "column_name", None)
            if not column or column not in repaired_df.columns:
                continue

            constraint = getattr(policy, "constraint_profile", {}) or {}
            allowed_values = self._normalize_allowed_values(constraint.get("allowed_values"))
            if not allowed_values:
                continue

            invalid_mask = repaired_df[column].notna() & ~repaired_df[column].isin(allowed_values)
            if invalid_mask.any():
                replacements = pd.Series(allowed_values).sample(
                    n=int(invalid_mask.sum()),
                    replace=True,
                    random_state=42,
                ).tolist()
                repaired_df.loc[invalid_mask, column] = replacements

        return repaired_df

    def _normalize_allowed_values(self, allowed_values):
        """Normalize stored constraint values into a concrete list."""
        if allowed_values is None:
            return []

        if isinstance(allowed_values, dict):
            return [value for value in allowed_values.values() if value is not None]

        if isinstance(allowed_values, (list, tuple, set, pd.Series)):
            return [value for value in list(allowed_values) if value is not None]

        if isinstance(allowed_values, range):
            return list(allowed_values)

        if isinstance(allowed_values, str):
            text = allowed_values.strip()
            if not text:
                return []

            range_match = re.fullmatch(r"range\(\s*(-?\d+)\s*,\s*(-?\d+)(?:\s*,\s*(-?\d+))?\s*\)", text)
            if range_match:
                start = int(range_match.group(1))
                stop = int(range_match.group(2))
                step = int(range_match.group(3) or 1)
                return list(range(start, stop, step))

            try:
                parsed = ast.literal_eval(text)
            except Exception:
                return [text]

            return self._normalize_allowed_values(parsed)

        return [allowed_values]

    def _enforce_entity_uniqueness(self, synthetic_df: pd.DataFrame, source_df: pd.DataFrame, policies: list, relationships: list) -> pd.DataFrame:
        """Repair duplicate entity identifiers without disturbing FK columns."""
        repaired_df = synthetic_df.copy()
        fk_columns = {rel.source_column for rel in relationships}

        for policy in policies:
            column = getattr(policy, "column_name", None)
            upper = column.upper() if column else ""
            if not column or column not in repaired_df.columns or column in fk_columns:
                continue
            if getattr(policy, "dedup_mode", None) != "entity":
                continue
            if not any(token in upper for token in ("_ID", "UUID", "KEY", "SSN", "IMSI", "MSISDN")):
                continue
            if any(token in upper for token in ("DATE", "_DT", "_TS", "TIME")):
                continue

            series = repaired_df[column]
            if not series.duplicated().any():
                continue

            null_mask = series.isna()
            source_column = source_df[column] if column in source_df.columns else series
            if pd.api.types.is_numeric_dtype(source_column):
                repaired_df.loc[~null_mask, column] = range(1, int((~null_mask).sum()) + 1)
            else:
                repaired_df.loc[~null_mask, column] = [
                    f"{column}_{idx + 1:06d}" for idx in range(int((~null_mask).sum()))
                ]

        return repaired_df

    def _update_boundary_keys(self, table_name, domain, synthetic_df, relationships):
        """Extract PKs from generated data and register in boundary key registry."""
        # Find PK columns (columns that are targets of FK relationships)
        pk_cols = set()
        for rel in relationships:
            if rel.target_table.upper() == table_name.upper():
                pk_cols.add(rel.target_column)

        # Also detect _ID columns as likely PKs
        for col in synthetic_df.columns:
            if col.upper().endswith('_ID') and synthetic_df[col].nunique() == len(synthetic_df):
                pk_cols.add(col)

        for pk_col in pk_cols:
            if pk_col in synthetic_df.columns:
                values = synthetic_df[pk_col].dropna().unique().tolist()
                with self.db_client.session() as session:
                    self.db_client.register_boundary_keys(
                        session, domain, table_name, pk_col,
                        [str(v) for v in values[:10000]],  # Cap for performance
                        self.run_id
                    )

    def _get_dominant_dedup_mode(self, policies) -> str:
        """Determine dominant dedup mode from column policies."""
        modes = {}
        for p in policies:
            mode = p.dedup_mode if hasattr(p, 'dedup_mode') else 'reference'
            modes[mode] = modes.get(mode, 0) + 1

        if not modes:
            return "reference"
        return max(modes, key=modes.get)

    def _stitch_foreign_keys(self, table_name: str, synthetic_df: pd.DataFrame, relationships: list) -> pd.DataFrame:
        """Align child FK columns to already generated parent tables."""
        stitched_df = synthetic_df.copy()

        for rel in relationships:
            if rel.source_table.upper() != table_name.upper():
                continue

            fk_col = rel.source_column
            parent_table = rel.target_table
            parent_col = rel.target_column
            parent_df = self.generated_data.get(parent_table)

            if (
                fk_col not in stitched_df.columns
                or parent_df is None
                or parent_col not in parent_df.columns
            ):
                continue

            parent_values = parent_df[parent_col].dropna().tolist()
            if not parent_values:
                continue

            null_mask = stitched_df[fk_col].isna() if fk_col in stitched_df.columns else None
            replacement = pd.Series(parent_values).sample(
                n=len(stitched_df),
                replace=True,
                random_state=42,
            ).reset_index(drop=True)
            stitched_df[fk_col] = replacement

            if null_mask is not None and null_mask.any():
                stitched_df.loc[null_mask, fk_col] = None

        return stitched_df

    def _enforce_temporal_constraints(self, synthetic_df: pd.DataFrame, strategy) -> pd.DataFrame:
        """Repair simple earlier/later temporal violations before validation."""
        if not strategy or not getattr(strategy, "temporal_constraints", None):
            return synthetic_df

        adjusted_df = synthetic_df.copy()
        for constraint in strategy.temporal_constraints:
            earlier_col = constraint.get("earlier_column")
            later_col = constraint.get("later_column")
            if earlier_col not in adjusted_df.columns or later_col not in adjusted_df.columns:
                continue

            earlier_dt = pd.to_datetime(adjusted_df[earlier_col], errors="coerce")
            later_dt = pd.to_datetime(adjusted_df[later_col], errors="coerce")
            comparable = earlier_dt.notna() & later_dt.notna()
            violations = comparable & (earlier_dt > later_dt)

            if not violations.any():
                continue

            earlier_vals = earlier_dt.loc[violations]
            later_vals = later_dt.loc[violations]
            adjusted_df.loc[violations, earlier_col] = earlier_vals.combine(later_vals, min)
            adjusted_df.loc[violations, later_col] = earlier_vals.combine(later_vals, max)

        return adjusted_df

    def _topological_sort(self, tables, relationships) -> list:
        """Sort tables so parent tables are generated before child tables."""
        table_names = {t.table_name.upper(): t for t in tables}

        # Build adjacency list (parent → children)
        children = {name: set() for name in table_names}
        parents = {name: set() for name in table_names}

        for rel in relationships:
            src = rel.source_table.upper()
            tgt = rel.target_table.upper()
            if src in table_names and tgt in table_names:
                parents[src].add(tgt)
                children[tgt].add(src)

        # Kahn's algorithm
        sorted_list = []
        no_parents = [name for name in table_names if not parents[name]]

        while no_parents:
            node = no_parents.pop(0)
            sorted_list.append(table_names[node])

            for child in list(children.get(node, [])):
                parents[child].discard(node)
                if not parents[child]:
                    no_parents.append(child)

        # Add remaining (cycle handling)
        for name, t in table_names.items():
            if t not in sorted_list:
                sorted_list.append(t)

        return sorted_list

    def resume_from_crash(self, run_id: str):
        """Resume pipeline from the last completed domain after a crash."""
        logger.info(f"Attempting crash recovery for run {run_id}")
        with self.db_client.session() as session:
            run_log = session.query(db_models.GenerationRunLog).filter_by(run_id=run_id).first()
            if not run_log or run_log.status != "failed":
                logger.info("No crashed run found or run already completed.")
                return

            completed_tables = set(run_log.tables_completed or [])
            logger.info(f"Crash recovery: {len(completed_tables)} tables already completed")

            # Update run log status
            run_log.status = "running"
            session.commit()

        # Re-execute with skip logic
        self.run_id = run_id
        # In production, we'd selectively re-run only pending domains/tables
        # For POC, we restart the full pipeline
        self.execute_pipeline(run_id)
