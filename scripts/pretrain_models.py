"""
Pretrain reusable CTGAN/TVAE baselines for compatible table families.

This script intentionally separates model-bank creation from normal pipeline
execution so demo runs can reuse cached models later without pushing any local
changes first.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

for env_var in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ.setdefault(env_var, "1")

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from graph.knowledge_graph import get_knowledge_graph
from ingestion.querylog_miner import QueryLogMiner
from ingestion.schema_connector import SchemaConnector
from ingestion.sqlglot_parser import DDLParser
from intelligence.abbreviation_resolver import AbbreviationResolver
from intelligence.llm_agent import LLMAgent
from intelligence.presidio_scanner import PresidioScanner
from intelligence.strategy_planner import StrategyPlanner
from models.schemas import ColumnPolicySchema
from pipeline.orchestrator import PipelineOrchestrator
from synthesis.masking_engine import MaskingEngine
from synthesis.table_profile import build_generation_profile
from synthesis.tier_router import TierRouter


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pretrain reusable table generators.")
    parser.add_argument(
        "--tables",
        nargs="*",
        default=None,
        help="Optional list of tables to pretrain. Defaults to all discoverable tables.",
    )
    parser.add_argument(
        "--only-tier",
        choices=["ctgan", "tvae"],
        default=None,
        help="Restrict pretraining to a single ML tier.",
    )
    parser.add_argument(
        "--ctgan-epochs",
        type=int,
        default=15,
        help="Epochs for fresh or adapted CTGAN training.",
    )
    parser.add_argument(
        "--tvae-epochs",
        type=int,
        default=15,
        help="Epochs for fresh or adapted TVAE training.",
    )
    return parser.parse_args()


def assign_domains(tables, relationships):
    """Build domains with the same fallback heuristic as the main pipeline."""
    kg = get_knowledge_graph()
    kg.build_graph(tables, relationships)
    try:
        domain_map = kg.partition_domains()
    except Exception:
        domain_map = {}

    if domain_map:
        return domain_map

    fallback = {}
    for table in tables:
        name = table.table_name.upper()
        if any(kw in name for kw in ["CUST", "SUBSCR", "SVC_PLAN", "ADDR", "CNTCT", "IDENT", "STAT_HIST"]):
            fallback[table.table_name] = "customer_management"
        elif any(kw in name for kw in ["BLNG", "INVC", "PYMT", "USAGE", "CDR"]):
            fallback[table.table_name] = "billing_revenue"
        elif any(kw in name for kw in ["NTWK", "CELL", "SVC_ORD", "WRK_ORD", "INCDT", "FIELD", "AGT"]):
            fallback[table.table_name] = "network_operations"
        else:
            fallback[table.table_name] = "general"
    return fallback


def ensure_policies(orchestrator, connector, table, presidio, abbrev, agent, confidence_threshold):
    """Load cached policies or classify a table live if no cache exists yet."""
    cached = orchestrator._load_existing_policies(table.table_name)
    if cached:
        return list(cached.values())

    policies = []
    for column in table.columns:
        try:
            expanded_name, fully_resolved = abbrev.resolve_column_name(column.column_name)
        except Exception:
            expanded_name = column.column_name
            fully_resolved = False

        try:
            sample_vals = orchestrator._get_column_sample(connector, table.table_name, column.column_name)
        except Exception:
            sample_vals = []

        pres_result = presidio.scan_column(table.table_name, column.column_name, sample_vals)
        if pres_result.pii_detected and pres_result.confidence >= orchestrator.config["presidio"].get("confidence_threshold", 0.7):
            masking_strategy = presidio.is_pii_passthrough(pres_result.pii_type)
            policy = ColumnPolicySchema(
                column_name=column.column_name,
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
                notes=f"Auto-classified by Presidio. PII type: {pres_result.pii_type}",
            )
            orchestrator._save_column_policy(policy, pii_source="presidio")
            policies.append(policy)
            continue

        policy = agent.classify_column(
            table_name=table.table_name,
            column_name_raw=column.column_name,
            column_name_expanded=expanded_name,
            data_type=column.data_type,
            statistical_profile=str(column.model_dump()),
            top_values=str(column.top_values),
            presidio_result=str(pres_result.model_dump()),
            abbreviation_status=str(fully_resolved),
        )
        if policy.llm_confidence < confidence_threshold:
            orchestrator._queue_for_review(table.table_name, column.column_name, policy, "Low Confidence Score")
        if not fully_resolved:
            orchestrator._queue_for_review(table.table_name, column.column_name, policy, "ABBREVIATION_UNKNOWN")

        orchestrator._save_column_policy(policy, pii_source="llm")
        policies.append(policy)

    return policies


def ensure_strategy(orchestrator, table_name: str, domain: str, policies, planner):
    """Load or create the generation strategy for a table."""
    strategy = orchestrator._load_existing_strategy(table_name)
    if strategy:
        return strategy

    strategy = planner.generate_strategy(table_name, domain, policies)
    orchestrator._save_generation_strategy(strategy)
    return strategy


def main() -> int:
    args = parse_args()
    orchestrator = PipelineOrchestrator()
    run_id = orchestrator.initialize_run()
    orchestrator._update_status("Pretraining Models", 2.0)

    source_url = orchestrator.config["data_sources"][0]["connection_string"]
    connector = SchemaConnector(source_url)
    tables = connector.extract_schema()

    ddl_dir = orchestrator.config["ingestion"]["ddl_directory"]
    ddl_parser = DDLParser(ddl_dir)
    explicit_rels = ddl_parser.parse_relationships()

    query_log_dir = orchestrator.config["ingestion"]["query_log_directory"]
    implicit_rels = QueryLogMiner(query_log_dir).mine_relationships()
    relationships = explicit_rels + implicit_rels

    if args.tables:
        requested = {table_name.upper() for table_name in args.tables}
        tables = [table for table in tables if table.table_name.upper() in requested]
        relationships = [
            rel for rel in relationships
            if rel.source_table.upper() in requested and rel.target_table.upper() in requested
        ]

    if not tables:
        print("No matching tables found for pretraining.")
        orchestrator._update_status("Pretraining Complete", 100.0, status="completed")
        return 0

    domain_map = assign_domains(tables, relationships)
    presidio = PresidioScanner(orchestrator.config)
    abbrev = AbbreviationResolver()
    agent = LLMAgent()
    planner = StrategyPlanner()
    confidence_threshold = orchestrator.config["llm"].get("confidence_threshold", 0.6)
    tier_router = TierRouter(orchestrator.config)
    masking_engine = MaskingEngine(orchestrator.config["generation"].get("faker_locale", "en_US"))
    model_save_dir = orchestrator.config["generation"].get("model_save_dir", "models/trained")

    results = []
    failures = []
    total_tables = len(tables)

    for index, table in enumerate(tables, start=1):
        orchestrator._update_status(f"Pretraining {table.table_name}", 2.0 + (index / max(total_tables, 1) * 95.0))
        domain = domain_map.get(table.table_name, "unknown")
        try:
            policies = ensure_policies(
                orchestrator=orchestrator,
                connector=connector,
                table=table,
                presidio=presidio,
                abbrev=abbrev,
                agent=agent,
                confidence_threshold=confidence_threshold,
            )
            strategy = ensure_strategy(orchestrator, table.table_name, domain, policies, planner)

            source_df = pd.read_sql(f"SELECT * FROM {table.table_name}", connector.engine)
            if source_df.empty:
                results.append((table.table_name, "skipped-empty"))
                continue

            masked_df = masking_engine.mask_dataframe(source_df, policies)
            profile = build_generation_profile(table.table_name, source_df, masked_df, policies, relationships)
            tier_override = strategy.tier_override if strategy else None
            tier = tier_router.route(table.table_name, len(source_df), tier_override, profile=profile)

            if args.only_tier and tier != args.only_tier and not (args.only_tier == "ctgan" and tier == "hybrid"):
                results.append((table.table_name, f"skipped-tier-{tier}"))
                continue

            if not profile.modeled_columns:
                results.append((table.table_name, "skipped-no-modeled-columns"))
                continue

            modeled_source_df = orchestrator._prepare_modeled_training_frame(source_df, masked_df, profile)
            if modeled_source_df.empty:
                results.append((table.table_name, "skipped-empty-modeled-frame"))
                continue

            if tier in {"ctgan", "hybrid"}:
                model_type = "ctgan"
                epochs = args.ctgan_epochs
            elif tier == "tvae":
                model_type = "tvae"
                epochs = args.tvae_epochs
            else:
                results.append((table.table_name, f"skipped-tier-{tier}"))
                continue

            _, reused = orchestrator._generate_ml_columns(
                table_name=table.table_name,
                domain=domain,
                model_type=model_type,
                modeled_source_df=modeled_source_df,
                policies=policies,
                profile=profile,
                model_save_dir=model_save_dir,
                epochs=epochs,
            )
            results.append((table.table_name, "reused" if reused else f"trained-{model_type}"))
        except Exception as exc:
            failures.append((table.table_name, str(exc)))

    final_status = "completed" if not failures else "failed"
    orchestrator._update_status("Pretraining Complete", 100.0, status=final_status)

    print(f"Pretraining run: {run_id}")
    for table_name, status in results:
        print(f"{table_name}: {status}")

    if failures:
        print("\nFailures:")
        for table_name, error in failures:
            print(f"{table_name}: {error}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
