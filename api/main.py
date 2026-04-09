"""
FastAPI Backend
Hosts the REST API for the GUI Dashboard and exposes the Pipeline Orchestrator.
"""

import json
import logging
import math
import os
import threading
from datetime import datetime
from pathlib import Path

for env_var in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ.setdefault(env_var, "1")

import pandas as pd
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, inspect as sqlalchemy_inspect

from config.config import load_config

from db.client import DatabaseClient
import db.schema as db_models
from models import schemas
from pipeline.orchestrator import PipelineOrchestrator

logger = logging.getLogger(__name__)

app = FastAPI(title="GAN Synthetic Data Controller")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global clients
db_client = DatabaseClient()
orchestrator = PipelineOrchestrator()


@app.on_event("startup")
def startup_event():
    db_client.initialize()


# =====================================================
# Dashboard Stats
# =====================================================

@app.get("/api/dashboard/stats", response_model=schemas.DashboardStats)
def get_dashboard_stats():
    """Aggregate statistics for the dashboard."""
    with db_client.session() as session:
        total_tables = session.query(db_models.TableMetadataRecord).count()
        domains = [
            d[0] for d in
            session.query(db_models.TableMetadataRecord.domain).distinct().all()
            if d[0]
        ]

        total_columns = session.query(db_models.ColumnPolicy).count()
        pii_cols = session.query(db_models.ColumnPolicy).filter(
            db_models.ColumnPolicy.pii_classification != 'none'
        ).count()
        pending_review = session.query(db_models.HumanReviewQueue).filter(
            db_models.HumanReviewQueue.status == 'pending'
        ).count()

        # Latest run
        last_run = session.query(db_models.PipelineRun).order_by(
            db_models.PipelineRun.started_at.desc()
        ).first()
        run_status = last_run.status if last_run else "idle"

        # Validation pass rate
        run_log = session.query(db_models.GenerationRunLog).order_by(
            db_models.GenerationRunLog.started_at.desc()
        ).first()
        pass_rate = 0.0
        if run_log and run_log.validation_results:
            total_checks = 0
            passed_checks = 0
            for table_results in run_log.validation_results.values():
                for r in table_results:
                    total_checks += 1
                    if r.get("passed"):
                        passed_checks += 1
            pass_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0.0

        return schemas.DashboardStats(
            total_tables=total_tables,
            total_columns=total_columns,
            columns_classified=total_columns,
            pii_columns_detected=pii_cols,
            columns_pending_review=pending_review,
            domains=domains,
            latest_run_status=run_status,
            validation_pass_rate=round(pass_rate, 1)
        )


# =====================================================
# Human Review Queue
# =====================================================

@app.get("/api/review/queue", response_model=list[schemas.ReviewQueueItem])
def get_review_queue():
    """Get all pending flags."""
    with db_client.session() as session:
        items = session.query(db_models.HumanReviewQueue).filter(
            db_models.HumanReviewQueue.status == 'pending'
        ).all()
        return [schemas.ReviewQueueItem(
            id=i.id,
            table_name=i.table_name or "",
            column_name=i.column_name or "",
            llm_best_guess=i.llm_best_guess,
            flag_reason=i.flag_reason or "",
            status=i.status or "pending",
            reviewer_notes=i.reviewer_notes,
            reviewed_at=i.reviewed_at.isoformat() if i.reviewed_at else None,
            created_at=i.created_at.isoformat() if i.created_at else ""
        ) for i in items]


@app.post("/api/review/{item_id}/approve")
def approve_review_item(item_id: int, approval: schemas.ReviewApproval):
    """Approve LLM's best guess policy."""
    with db_client.session() as session:
        item = session.query(db_models.HumanReviewQueue).filter_by(id=item_id).first()
        if not item:
            raise HTTPException(status_code=404, detail="Review item not found")

        item.status = "approved"
        item.reviewer_notes = approval.reviewer_notes
        item.reviewed_at = datetime.utcnow()
        session.flush()

        # Write policy to ColumnPolicy
        if item.llm_best_guess:
            policy_data = dict(item.llm_best_guess)
            policy_data['pii_source'] = 'human_review'
            db_client.upsert_column_policy(session, policy_data)

    return {"message": "Approved", "item_id": item_id}


@app.post("/api/review/{item_id}/correct")
def correct_review_item(item_id: int, correction: schemas.ReviewCorrection):
    """Override LLM policy with human correction."""
    with db_client.session() as session:
        item = session.query(db_models.HumanReviewQueue).filter_by(id=item_id).first()
        if not item:
            raise HTTPException(status_code=404, detail="Review item not found")

        item.status = "corrected"
        item.reviewer_notes = correction.reviewer_notes
        item.reviewed_at = datetime.utcnow()
        session.flush()

        # Write corrected policy
        policy_data = correction.corrected_policy.model_dump()
        policy_data['pii_source'] = 'human_correction'
        db_client.upsert_column_policy(session, policy_data)

    return {"message": "Corrected", "item_id": item_id}


@app.post("/api/review/{item_id}/abbreviation")
def submit_abbreviation(item_id: int, submission: schemas.AbbreviationSubmission):
    """Submit an abbreviation expansion, writes to knowledge graph dictionary."""
    # Update in-memory knowledge graph abbreviation dictionary
    try:
        from graph.knowledge_graph import get_knowledge_graph
        kg = get_knowledge_graph()
        kg.add_abbreviation(submission.token, submission.expansion)
    except Exception as e:
        logger.warning(f"Could not update abbreviation dict: {e}")

    # Update review item
    with db_client.session() as session:
        item = session.query(db_models.HumanReviewQueue).filter_by(id=item_id).first()
        if item:
            item.status = "corrected"
            item.reviewer_notes = f"Abbreviation: {submission.token} → {submission.expansion}. {submission.reviewer_notes or ''}"
            item.reviewed_at = datetime.utcnow()

    return {
        "message": "Abbreviation saved",
        "token": submission.token,
        "expansion": submission.expansion
    }


# =====================================================
# Pipeline Orchestration
# =====================================================

class PipelineStartRequest(schemas.BaseModel):
    table_filter: list[str] | None = None
    fast_mode: bool = False

@app.post("/api/pipeline/start")
def start_pipeline(request: PipelineStartRequest = None):
    """
    Kick off the pipeline in a separate daemon thread.
    Optional table_filter in request body: list of table names to process (for testing).
    """
    table_filter = request.table_filter if request else None
    fast_mode = request.fast_mode if request else False
    run_id = orchestrator.initialize_run()
    t = threading.Thread(target=orchestrator.execute_pipeline, args=(run_id, table_filter, fast_mode), daemon=True)
    t.start()
    return {"message": "Pipeline started", "run_id": run_id, "table_filter": table_filter, "fast_mode": fast_mode}


@app.post("/api/pipeline/resume/{run_id}")
def resume_pipeline(run_id: str):
    """Resume a crashed pipeline run."""
    t = threading.Thread(target=orchestrator.resume_from_crash, args=(run_id,), daemon=True)
    t.start()
    return {"message": "Pipeline resume initiated", "run_id": run_id}


@app.get("/api/pipeline/status/{run_id}", response_model=schemas.PipelineRunStatus)
def get_pipeline_status(run_id: str):
    """Get status of an active or recent pipeline run."""
    with db_client.session() as session:
        run = session.query(db_models.PipelineRun).filter_by(run_id=run_id).first()
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")

        elapsed = 0.0
        if run.ended_at and run.started_at:
            elapsed = (run.ended_at - run.started_at).total_seconds()
        elif run.started_at:
            elapsed = (datetime.utcnow() - run.started_at).total_seconds()

        # Get domain info from run log
        run_log = session.query(db_models.GenerationRunLog).filter_by(run_id=run_id).first()

        return schemas.PipelineRunStatus(
            run_id=run.run_id,
            status=run.status or "unknown",
            current_step=run.current_step,
            progress_pct=run.progress_pct or 0.0,
            started_at=run.started_at.isoformat() if run.started_at else "",
            elapsed_seconds=elapsed,
            domains_completed=run_log.domains_completed or [] if run_log else [],
            domains_pending=run_log.domains_pending or [] if run_log else []
        )


@app.get("/api/pipeline/runs", response_model=list[schemas.PipelineRunStatus])
def list_pipeline_runs():
    """List recent pipeline runs."""
    with db_client.session() as session:
        runs = session.query(db_models.PipelineRun).order_by(
            db_models.PipelineRun.started_at.desc()
        ).limit(10).all()

        return [schemas.PipelineRunStatus(
            run_id=r.run_id,
            status=r.status or "unknown",
            current_step=r.current_step,
            progress_pct=r.progress_pct or 0.0,
            started_at=r.started_at.isoformat() if r.started_at else "",
            elapsed_seconds=0.0
        ) for r in runs]


# =====================================================
# Column Policies (read-only for dashboard)
# =====================================================

@app.get("/api/policies")
def get_all_policies():
    """Get all column policies for display."""
    with db_client.session() as session:
        policies = session.query(db_models.ColumnPolicy).all()
        return [{
            "id": p.id,
            "table_name": p.table_name,
            "column_name": p.column_name,
            "pii_classification": p.pii_classification,
            "pii_source": p.pii_source,
            "sensitivity_reason": p.sensitivity_reason,
            "masking_strategy": p.masking_strategy,
            "constraint_profile": p.constraint_profile,
            "business_importance": p.business_importance,
            "edge_case_flags": p.edge_case_flags,
            "llm_confidence": p.llm_confidence,
            "dedup_mode": p.dedup_mode,
            "notes": p.notes,
        } for p in policies]


@app.get("/api/strategies")
def get_all_strategies():
    """Get all generation strategies."""
    with db_client.session() as session:
        strategies = session.query(db_models.GenerationStrategy).all()
        return [{
            "id": s.id,
            "table_name": s.table_name,
            "domain": s.domain,
            "tier_override": s.tier_override,
            "temporal_constraints": s.temporal_constraints,
            "edge_case_injection_pct": s.edge_case_injection_pct,
        } for s in strategies]


# =====================================================
# Generation Run Log & Synthetic Data Preview
# =====================================================

def _get_output_root() -> Path:
    config = load_config()
    return Path(config.get("delivery", {}).get("output_directory", "output/synthetic"))


def _list_run_manifests() -> list[dict]:
    output_root = _get_output_root()
    if not output_root.exists():
        return []

    manifests = []
    run_dirs = sorted(
        [d for d in output_root.iterdir() if d.is_dir()],
        key=lambda d: d.stat().st_mtime,
        reverse=True,
    )
    for run_dir in run_dirs:
        manifest_path = run_dir / "manifest.json"
        if not manifest_path.exists():
            continue
        try:
            manifest = json.loads(manifest_path.read_text())
            manifests.append({"run_dir": run_dir, "manifest": manifest})
        except Exception as exc:
            logger.warning(f"Failed to parse manifest {manifest_path}: {exc}")
    return manifests


def _read_generated_dataset(table_name: str, run_id: str | None = None):
    manifests = _list_run_manifests()
    if run_id:
        manifests = [m for m in manifests if m["run_dir"].name == run_id]

    for item in manifests:
        run_dir = item["run_dir"]
        parquet_file = run_dir / f"{table_name}.parquet"
        csv_file = run_dir / f"{table_name}.csv"

        try:
            if parquet_file.exists():
                return run_dir.name, pd.read_parquet(parquet_file)
            if csv_file.exists():
                return run_dir.name, pd.read_csv(csv_file)
        except Exception as exc:
            logger.warning(f"Failed to read generated data for {table_name} from {run_dir}: {exc}")

    return None, None


def _json_safe(value):
    """Recursively normalize NaN/inf values so API responses remain JSON-safe."""
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, dict):
        return {key: _json_safe(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def _get_source_engine():
    config = load_config()
    source_url = config["data_sources"][0]["connection_string"]
    return create_engine(source_url)


def _get_source_table_names() -> list[str]:
    try:
        engine = _get_source_engine()
        return sorted(sqlalchemy_inspect(engine).get_table_names())
    except Exception as exc:
        logger.warning(f"Failed to inspect source database tables: {exc}")
        return []


def _build_manifest_generation_entries() -> list[dict]:
    entries = []
    for item in _list_run_manifests():
        run_dir = item["run_dir"]
        manifest = item["manifest"]
        row_counts = manifest.get("row_counts", {})
        strategies = manifest.get("generation_strategies", {})
        for table_name in manifest.get("tables_generated", []):
            entries.append({
                "run_id": manifest.get("run_id", run_dir.name),
                "table_name": table_name,
                "tier": strategies.get(table_name, "unknown"),
                "rows_generated": row_counts.get(table_name, 0),
                "domain": "unknown",
                "status": "completed",
                "started_at": None,
                "completed_at": manifest.get("timestamp"),
            })
    return entries


def _resolve_latest_run_id(session, requested_run_id: str | None = None) -> str | None:
    """Resolve an explicit run id or fall back to the most recent pipeline run."""
    if requested_run_id:
        return requested_run_id

    latest_run = session.query(db_models.PipelineRun).order_by(
        db_models.PipelineRun.started_at.desc()
    ).first()
    return latest_run.run_id if latest_run else None

@app.get("/api/generation/log")
def get_generation_log():
    """
    Return tier routing history for generated tables.
    Prefers live step logs and falls back to delivery manifests for older runs.
    """
    with db_client.session() as session:
        strategies = session.query(db_models.GenerationStrategy).all()
        step_logs = session.query(db_models.PipelineStepLog).filter(
            db_models.PipelineStepLog.step_name.in_(["tier_routing", "generation_complete"])
        ).order_by(db_models.PipelineStepLog.started_at.desc()).all()
        pipeline_runs = session.query(db_models.PipelineRun).all()

        tier_map = {s.table_name: s.tier_override or "auto" for s in strategies}
        domain_map = {s.table_name: s.domain for s in strategies}
        run_map = {r.run_id: r for r in pipeline_runs}

        entries = {}
        for step in reversed(step_logs):
            if not step.table_name:
                continue
            key = (step.run_id, step.table_name)
            run = run_map.get(step.run_id)
            entry = entries.get(key, {
                "run_id": step.run_id,
                "table_name": step.table_name,
                "tier": tier_map.get(step.table_name, "unknown"),
                "rows_generated": 0,
                "domain": step.domain or domain_map.get(step.table_name, "unknown"),
                "status": run.status if run else step.status,
                "started_at": run.started_at.isoformat() if run and run.started_at else None,
                "completed_at": run.ended_at.isoformat() if run and run.ended_at else None,
            })

            details = step.details or {}
            if step.step_name == "tier_routing":
                entry["tier"] = details.get("tier", entry["tier"])
                entry["rows_generated"] = entry["rows_generated"] or details.get("row_count", 0)
            elif step.step_name == "generation_complete":
                entry["tier"] = details.get("tier", entry["tier"])
                entry["rows_generated"] = details.get("rows_generated", entry["rows_generated"])
                entry["completed_at"] = step.completed_at.isoformat() if step.completed_at else entry["completed_at"]

            entries[key] = entry

    for item in _build_manifest_generation_entries():
        key = (item["run_id"], item["table_name"])
        existing = entries.get(key)
        if not existing:
            item["domain"] = domain_map.get(item["table_name"], "unknown")
            entries[key] = item
            continue
        if existing.get("tier") in (None, "unknown", "auto"):
            existing["tier"] = item["tier"]
        if not existing.get("rows_generated"):
            existing["rows_generated"] = item["rows_generated"]
        if not existing.get("completed_at"):
            existing["completed_at"] = item["completed_at"]

    return sorted(
        entries.values(),
        key=lambda item: (
            item.get("completed_at") or item.get("started_at") or "",
            item["run_id"],
            item["table_name"],
        ),
        reverse=True,
    )


@app.get("/api/training-metrics")
def get_training_metrics(run_id: str | None = None):
    """Return live or historical model training metrics grouped by table."""
    with db_client.session() as session:
        resolved_run_id = _resolve_latest_run_id(session, run_id)
        if not resolved_run_id:
            return {"run_id": None, "tables": []}

        steps = session.query(db_models.PipelineStepLog).filter(
            db_models.PipelineStepLog.run_id == resolved_run_id,
            db_models.PipelineStepLog.step_name.in_([
                "table_profile",
                "training_start",
                "training_metric",
                "training_complete",
                "training_failed",
                "model_reuse",
                "generation_complete",
                "generation_failed",
            ]),
        ).order_by(db_models.PipelineStepLog.started_at.asc(), db_models.PipelineStepLog.id.asc()).all()

        tables = {}
        for step in steps:
            if not step.table_name:
                continue

            details = step.details or {}
            entry = tables.setdefault(step.table_name, {
                "table_name": step.table_name,
                "domain": step.domain or "unknown",
                "tier": None,
                "status": "pending",
                "model_type": None,
                "model_reused": False,
                "epochs_planned": None,
                "epochs_completed": 0,
                "metrics": [],
                "profile": {},
                "model_path": None,
                "training_mode": None,
                "match_type": None,
                "error": None,
            })

            if step.step_name == "table_profile":
                entry["profile"] = {
                    "fingerprint": details.get("fingerprint"),
                    "modeled_columns": details.get("modeled_columns", 0),
                    "structural_columns": details.get("structural_columns", 0),
                    "sensitive_columns": details.get("sensitive_columns", 0),
                }
            elif step.step_name == "training_start":
                entry["status"] = "training"
                entry["model_type"] = details.get("model_type")
                entry["epochs_planned"] = details.get("epochs")
                entry["training_mode"] = details.get("training_mode")
                entry["match_type"] = details.get("match_type")
            elif step.step_name == "training_metric":
                entry["status"] = "training"
                entry["model_type"] = details.get("model_type", entry["model_type"])
                entry["metrics"].append(_json_safe(details))
                entry["epochs_completed"] = max(entry["epochs_completed"], int(details.get("epoch", 0) or 0))
            elif step.step_name == "training_complete":
                entry["status"] = "completed"
                entry["model_type"] = details.get("model_type", entry["model_type"])
                entry["epochs_planned"] = details.get("epochs", entry["epochs_planned"])
                entry["model_path"] = details.get("model_path")
                entry["training_mode"] = details.get("training_mode", entry["training_mode"])
                entry["match_type"] = details.get("match_type", entry["match_type"])
            elif step.step_name == "training_failed":
                entry["status"] = "failed"
                entry["model_type"] = details.get("model_type", entry["model_type"])
                entry["epochs_planned"] = details.get("epochs", entry["epochs_planned"])
                entry["training_mode"] = details.get("training_mode", entry["training_mode"])
                entry["match_type"] = details.get("match_type", entry["match_type"])
                entry["error"] = details.get("error")
            elif step.step_name == "model_reuse":
                entry["status"] = "reused"
                entry["model_type"] = details.get("model_type", entry["model_type"])
                entry["model_reused"] = True
                entry["model_path"] = details.get("model_path")
                entry["match_type"] = details.get("match_type", entry["match_type"])
                entry["profile"] = {
                    **entry["profile"],
                    "fingerprint": details.get("fingerprint", entry["profile"].get("fingerprint")),
                }
            elif step.step_name == "generation_complete":
                entry["tier"] = details.get("tier", entry["tier"])
                if entry["status"] == "pending":
                    entry["status"] = "completed"
                entry["model_reused"] = bool(details.get("model_reused", entry["model_reused"]))
            elif step.step_name == "generation_failed":
                entry["tier"] = details.get("tier", entry["tier"])
                entry["status"] = "failed"
                entry["error"] = details.get("error")

        response_tables = []
        for entry in tables.values():
            metrics = entry["metrics"]
            latest_metric = metrics[-1] if metrics else {}
            response_tables.append({
                **_json_safe(entry),
                "latest_metric": _json_safe(latest_metric),
                "metric_count": len(metrics),
            })

        return {
            "run_id": resolved_run_id,
            "tables": sorted(response_tables, key=lambda item: item["table_name"]),
        }


@app.get("/api/data/tables")
def get_data_tables():
    """Return all source tables plus generated-data availability for the viewer."""
    source_tables = _get_source_table_names()
    manifest_entries = _build_manifest_generation_entries()
    latest_generated = {}
    for entry in manifest_entries:
        latest_generated.setdefault(entry["table_name"], entry)

    with db_client.session() as session:
        table_meta = session.query(db_models.TableMetadataRecord).all()
        meta_map = {t.table_name: t for t in table_meta}

    all_tables = sorted(set(source_tables) | set(latest_generated.keys()) | set(meta_map.keys()))
    return [{
        "table_name": table_name,
        "domain": getattr(meta_map.get(table_name), "domain", None) or latest_generated.get(table_name, {}).get("domain", "unknown"),
        "source_row_count": getattr(meta_map.get(table_name), "row_count", None),
        "generated_row_count": latest_generated.get(table_name, {}).get("rows_generated"),
        "generated_run_id": latest_generated.get(table_name, {}).get("run_id"),
        "has_source": table_name in source_tables or table_name in meta_map,
        "has_generated": table_name in latest_generated,
        "tier": latest_generated.get(table_name, {}).get("tier"),
    } for table_name in all_tables]


@app.get("/api/generated-data/{table_name}")
def get_generated_data(table_name: str, run_id: str | None = None):
    """
    Return up to 50 sample rows of generated synthetic data for a given table.
    Reads from the most recent run's output parquet/csv file.
    """
    resolved_run_id, df = _read_generated_dataset(table_name, run_id=run_id)
    if df is None:
        return {
            "table_name": table_name,
            "rows": [],
            "message": f"No generated data found for table '{table_name}'.",
        }

    rows = df.head(50).to_dict(orient="records")
    return {
        "table_name": table_name,
        "run_id": resolved_run_id,
        "total_rows": len(df),
        "sample_size": len(rows),
        "columns": list(df.columns),
        "rows": rows,
    }


@app.get("/api/source-data/{table_name}")
def get_source_data(table_name: str):
    """Return up to 50 sample rows of source data for comparison."""
    try:
        engine = _get_source_engine()
        total_rows = pd.read_sql(f'SELECT COUNT(*) AS total_rows FROM "{table_name}"', engine).iloc[0]["total_rows"]
        df = pd.read_sql(f'SELECT * FROM "{table_name}" LIMIT 50', engine)
        rows = df.head(50).to_dict(orient="records")
        return {
            "table_name": table_name,
            "total_rows": int(total_rows),
            "sample_size": len(rows),
            "columns": list(df.columns),
            "rows": rows,
        }
    except Exception as e:
        return {"table_name": table_name, "rows": [], "message": str(e)}


# =====================================================
# Pipeline Activity Log
# =====================================================

@app.get("/api/pipeline/activity-log")
def get_pipeline_activity_log():
    """
    Return step-by-step activity log from the pipeline.
    Reads from the pipeline_step_log table.
    """
    with db_client.session() as session:
        steps = session.query(db_models.PipelineStepLog).order_by(
            db_models.PipelineStepLog.started_at.desc()
        ).limit(200).all()

        return [{
            "id": s.id,
            "run_id": s.run_id,
            "step_name": s.step_name,
            "domain": s.domain,
            "table_name": s.table_name,
            "status": s.status,
            "details": s.details,
            "started_at": s.started_at.isoformat() if s.started_at else None,
            "completed_at": s.completed_at.isoformat() if s.completed_at else None,
            "duration_seconds": s.duration_seconds,
        } for s in steps]


# =====================================================
# Knowledge Graph Visualization
# =====================================================

@app.get("/api/graph")
def get_graph_data():
    """
    Return the knowledge graph as nodes + edges for frontend visualization.
    Nodes are tables (with domain coloring), edges are FK relationships.
    """
    try:
        from graph.knowledge_graph import get_knowledge_graph
        kg = get_knowledge_graph()

        nodes = []
        edges = []

        for node_id, data in kg.G.nodes(data=True):
            if data.get("node_type") == "table":
                # Count columns for this table
                col_count = sum(1 for _, _, ed in kg.G.out_edges(node_id, data=True)
                                if ed.get("edge_type") == "HAS_COLUMN")
                # Count PII columns
                pii_count = sum(1 for _, tgt, ed in kg.G.out_edges(node_id, data=True)
                                if ed.get("edge_type") == "HAS_COLUMN"
                                and kg.G.nodes[tgt].get("pii_classification") not in (None, "none", "not_pii"))

                nodes.append({
                    "id": data["name"],
                    "label": data["name"],
                    "domain": data.get("domain", "unknown"),
                    "row_count": data.get("row_count", 0),
                    "column_count": col_count,
                    "pii_columns": pii_count,
                })

        for src, tgt, data in kg.G.edges(data=True):
            if data.get("edge_type") == "RELATES_TO":
                src_name = kg.G.nodes[src].get("name")
                tgt_name = kg.G.nodes[tgt].get("name")
                edges.append({
                    "source": src_name,
                    "target": tgt_name,
                    "source_column": data.get("source_column"),
                    "target_column": data.get("target_column"),
                    "relationship_type": data.get("relationship_type", "FK"),
                })

        return {"nodes": nodes, "edges": edges}
    except Exception as e:
        return {"nodes": [], "edges": [], "error": str(e)}


@app.get("/api/graph/table/{table_name}")
def get_table_graph_detail(table_name: str):
    """Return detailed column-level graph data for a specific table."""
    try:
        from graph.knowledge_graph import get_knowledge_graph
        kg = get_knowledge_graph()
        schema_json = kg.get_table_schema(table_name)
        relationships_json = kg.get_relationships(table_name)
        downstream_json = kg.get_downstream_tables(table_name)
        domain = kg.get_domain(table_name)

        return {
            "table_name": table_name,
            "domain": domain,
            "schema": json.loads(schema_json) if schema_json and "not found" not in schema_json else {},
            "relationships": json.loads(relationships_json),
            "downstream_tables": json.loads(downstream_json),
        }
    except Exception as e:
        return {"error": str(e)}
