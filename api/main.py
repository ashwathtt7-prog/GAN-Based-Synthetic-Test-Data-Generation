"""
FastAPI Backend
Hosts the REST API for the GUI Dashboard and exposes the Pipeline Orchestrator.
"""

from fastapi import FastAPI, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pathlib import Path

from db.client import DatabaseClient
import db.schema as db_models
from models import schemas
from pipeline.orchestrator import PipelineOrchestrator

app = FastAPI(title="GAN Synthetic Data Controller")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # For React frontend POC
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global clients
db_client = DatabaseClient()
orchestrator = PipelineOrchestrator()

@app.on_event("startup")
def startup_event():
    # Setup DB on startup
    db_client.initialize()

# --- Dashboard Stats ---

@app.get("/api/dashboard/stats", response_model=schemas.DashboardStats)
def get_dashboard_stats():
    """Aggregate statistics for the dashboard."""
    with db_client.get_session() as session:
        # Example metrics gathering
        total_tables = session.query(db_models.TableMetadata).count()
        domains = [d[0] for d in session.query(db_models.TableMetadata.domain).distinct().all() if d[0]]
        
        total_classified = session.query(db_models.ColumnPolicy).count()
        pii_cols = session.query(db_models.ColumnPolicy).filter(db_models.ColumnPolicy.pii_classification != 'none').count()
        
        pending_review = session.query(db_models.HumanReviewQueue).filter(db_models.HumanReviewQueue.status == 'pending').count()
        
        # Get latest run
        last_run = session.query(db_models.PipelineRun).order_by(db_models.PipelineRun.started_at.desc()).first()
        run_status = last_run.status if last_run else "idle"
        
        return schemas.DashboardStats(
            total_tables=total_tables,
            total_columns=0, # Simplified for POC
            columns_classified=total_classified,
            pii_columns_detected=pii_cols,
            columns_pending_review=pending_review,
            domains=domains,
            latest_run_status=run_status,
            validation_pass_rate=95.0 # Mock metric for POC
        )

# --- Human Review Queue ---

@app.get("/api/review/queue", response_model=list[schemas.ReviewQueueItem])
def get_review_queue():
    """Get all pending flags."""
    with db_client.get_session() as session:
        items = session.query(db_models.HumanReviewQueue).filter(db_models.HumanReviewQueue.status == 'pending').all()
        return [schemas.ReviewQueueItem(
            id=i.id,
            table_name=i.table_name,
            column_name=i.column_name,
            llm_best_guess=i.llm_best_guess,
            flag_reason=i.flag_reason,
            status=i.status,
            created_at=i.created_at.isoformat()
        ) for i in items]

@app.post("/api/review/{item_id}/approve")
def approve_review_item(item_id: int, approval: schemas.ReviewApproval):
    """Approve LLM's best guess policy."""
    with db_client.get_session() as session:
        item = session.query(db_models.HumanReviewQueue).filter_by(id=item_id).first()
        if item:
            item.status = "approved"
            item.reviewer_notes = approval.reviewer_notes
            session.commit()
            
            # Commit policy to ColumnPolicy
            if item.llm_best_guess:
               policy = db_models.ColumnPolicy(**item.llm_best_guess)
               session.merge(policy)
               session.commit()
               
    return {"message": "Approved"}

@app.post("/api/review/{item_id}/correct")
def correct_review_item(item_id: int, correction: schemas.ReviewCorrection):
    """Override LLM policy with human correction."""
    with db_client.get_session() as session:
        item = session.query(db_models.HumanReviewQueue).filter_by(id=item_id).first()
        if item:
            item.status = "corrected"
            item.reviewer_notes = correction.reviewer_notes
            session.commit()
            
            policy = db_models.ColumnPolicy(**correction.corrected_policy.model_dump())
            session.merge(policy)
            session.commit()
            
    return {"message": "Corrected"}

# --- Pipeline Orchestration ---

@app.post("/api/pipeline/start")
def start_pipeline(background_tasks: BackgroundTasks):
    """Kick off the end-to-end pipeline."""
    run_id = orchestrator.initialize_run()
    background_tasks.add_task(orchestrator.execute_pipeline, run_id)
    return {"message": "Pipeline started", "run_id": run_id}

@app.get("/api/pipeline/status/{run_id}", response_model=schemas.PipelineRunStatus)
def get_pipeline_status(run_id: str):
    """Get status of an active or recent pipeline run."""
    with db_client.get_session() as session:
        run = session.query(db_models.PipelineRun).filter_by(run_id=run_id).first()
        if not run:
            return {"status": "not_found", "run_id": run_id}
            
        elap = (run.ended_at - run.started_at).total_seconds() if run.ended_at else 0
        return schemas.PipelineRunStatus(
            run_id=run.run_id,
            status=run.status,
            current_step=run.current_step,
            progress_pct=run.progress_pct,
            started_at=run.started_at.isoformat(),
            elapsed_seconds=elap
        )
