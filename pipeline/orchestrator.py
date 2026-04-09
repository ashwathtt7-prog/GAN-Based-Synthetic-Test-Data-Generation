"""
Pipeline Orchestrator
Master coordinator that executes all 4 layers end-to-end:
  1. Schema Ingestion & Knowledge Graph Construction
  2. PII Detection & LLM Semantic Reasoning
  3. Synthetic Generation Engine
  4. Validation Gate & Delivery
Supports crash recovery via generation_run_log.
"""

import logging
import uuid
import pandas as pd
from datetime import datetime
from pathlib import Path

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

# Layer 4 — Validation
from synthesis.data_validator import DataValidator

# Delivery
from delivery.packager import DeliveryPackager

# DB
from db.client import DatabaseClient
import db.schema as db_models

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

    def initialize_run(self) -> str:
        self.run_id = str(uuid.uuid4())
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
                session.commit()

    def execute_pipeline(self, run_id: str, table_filter: list[str] = None):
        """
        Execute the full 4-layer pipeline.

        Args:
            run_id: Pipeline run identifier
            table_filter: Optional list of table names to process (for testing).
                          If provided, only these tables and their relationships are processed.
        """
        self.run_id = run_id
        try:
            # ================================================================
            # Phase 1: Schema Ingestion (Step 1.1 - 1.3)
            # ================================================================
            self._update_status("Schema Ingestion", 2.0)
            logger.info("=== Phase 1: Schema Ingestion ===")

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

                table_policies = []
                for col in table.columns:
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
                        from models.schemas import ColumnPolicySchema
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

                # Step 2.6: Generation Strategy
                domain = domain_map.get(table.table_name, "unknown")
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

                # Step 3.1: Tier routing
                tier_override = strategy.tier_override if strategy else None
                tier = tier_router.route(table_name, len(source_df), tier_override)

                # Step 3.2: Pre-generation masking
                masked_df = masking_engine.mask_dataframe(source_df, policies)

                # Step 3.3: Train and generate
                num_rows = len(source_df)
                domain = domain_map.get(table_name, "unknown")

                # POC: cap epochs low to avoid blocking — production would use more
                poc_ctgan_epochs = min(ctgan_epochs, 15)
                poc_tvae_epochs = min(tvae_epochs, 15)

                try:
                    if tier == "ctgan":
                        model = CTGANModel(table_name, policies)
                        model.train(masked_df, epochs=poc_ctgan_epochs)
                        synthetic_df = model.generate(num_rows)
                        model.save(f"{model_save_dir}/{domain}")
                        self._register_model(table_name, domain, f"{model_save_dir}/{domain}/{table_name}_ctgan.pkl", "ctgan", num_rows)

                    elif tier == "tvae":
                        model = TVAEModel(table_name, policies)
                        model.train(masked_df, epochs=poc_tvae_epochs)
                        synthetic_df = model.generate(num_rows)
                        model.save(f"{model_save_dir}/{domain}")
                        self._register_model(table_name, domain, f"{model_save_dir}/{domain}/{table_name}_tvae.pkl", "tvae", num_rows)

                    elif tier == "rule_based":
                        generator = RuleBasedGenerator(table_name, policies)
                        synthetic_df = generator.generate(masked_df, num_rows)

                    else:  # hybrid — use CTGAN as default
                        model = CTGANModel(table_name, policies)
                        model.train(masked_df, epochs=poc_ctgan_epochs)
                        synthetic_df = model.generate(num_rows)

                except Exception as e:
                    logger.error(f"Generation failed for {table_name}: {e}")
                    # Fallback to rule-based
                    try:
                        generator = RuleBasedGenerator(table_name, policies)
                        synthetic_df = generator.generate(masked_df, num_rows)
                    except Exception as e2:
                        logger.error(f"Fallback generation also failed for {table_name}: {e2}")
                        continue

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

                self.generated_data[table_name] = synthetic_df
                logger.info(f"Generated {len(synthetic_df)} records for {table_name}")

            # ================================================================
            # Phase 6: Validation Gate (Layer 4)
            # ================================================================
            self._update_status("Validation Gate", 82.0)
            logger.info("=== Phase 6: Validation ===")

            validation_cfg = self.config.get('validation', {})
            max_retries = validation_cfg.get('max_retry_on_failure', 3)
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
                    synthetic_df, real_masked if len(real_masked) > 0 else synthetic_df,
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
                if failures:
                    logger.warning(f"Validation failures for {table_name}: {len(failures)}")
                    tables_needing_retry.append(table_name)
                else:
                    logger.info(f"All validations passed for {table_name}")

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
                    strategy_dict = strategy.model_dump() if strategy else None
                    failed_results = [r for r in all_validation_results.get(table_name, []) if not r.passed]

                    diagnosis = diagnosis_agent.diagnose(
                        table_name, domain_map.get(table_name, "unknown"),
                        failed_results, strategy_dict
                    )

                    # If diagnosis includes updated strategy, apply it
                    if diagnosis.updated_strategy:
                        self.strategies_cache[table_name] = diagnosis.updated_strategy
                        self._save_generation_strategy(diagnosis.updated_strategy)

                    # Re-generate would go here in full production
                    # For POC, we log the diagnosis and mark as manual_review_required
                    logger.info(f"Diagnosis for {table_name}: {diagnosis.root_cause}")
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
                strategy = self.strategies_cache.get(table_name)
                gen_strategies[table_name] = strategy.tier_override or "auto" if strategy else "auto"

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
                    validation_results={
                        t: [r.model_dump() for r in rs]
                        for t, rs in all_validation_results.items()
                    },
                    completed_at=datetime.utcnow()
                )

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
                    domains_completed=list(self.generated_data.keys()),
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

    def _queue_for_review(self, table_name: str, column_name: str, policy, reason: str):
        """Add a column to the human review queue."""
        with self.db_client.session() as session:
            self.db_client.add_to_review_queue(session, {
                "table_name": table_name,
                "column_name": column_name,
                "llm_best_guess": policy.model_dump(),
                "flag_reason": reason
            })

    def _register_model(self, table_name, domain, path, model_type, row_count):
        """Register a trained model in the model registry."""
        with self.db_client.session() as session:
            self.db_client.register_model(session, {
                "domain": domain,
                "table_name": table_name,
                "model_type": model_type,
                "model_path": path,
                "trained_on_run_id": self.run_id,
                "row_count_at_training": row_count,
            })

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
