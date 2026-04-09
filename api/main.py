"""
FastAPI Backend
Hosts the REST API for the GUI Dashboard and exposes the Pipeline Orchestrator.
"""

import json
import logging
import threading
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware

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

@app.post("/api/pipeline/start")
def start_pipeline(request: PipelineStartRequest = None):
    """
    Kick off the pipeline in a separate daemon thread.
    Optional table_filter in request body: list of table names to process (for testing).
    """
    table_filter = request.table_filter if request else None
    run_id = orchestrator.initialize_run()
    t = threading.Thread(target=orchestrator.execute_pipeline, args=(run_id, table_filter), daemon=True)
    t.start()
    return {"message": "Pipeline started", "run_id": run_id, "table_filter": table_filter}


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
            "masking_strategy": p.masking_strategy,
            "business_importance": p.business_importance,
            "llm_confidence": p.llm_confidence,
            "dedup_mode": p.dedup_mode,
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
