"""
SQLAlchemy ORM models for operational memory tables.
These tables store all LLM decisions, pipeline state, and audit trails.
Using SQLite for POC — swap to PostgreSQL by changing DATABASE_URL in config.yaml.
"""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, Boolean, Text, DateTime,
    JSON, UniqueConstraint, create_engine
)
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class ColumnPolicy(Base):
    """Stores LLM/Presidio classification decisions for every column."""
    __tablename__ = "column_policy"

    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String, nullable=False)
    column_name = Column(String, nullable=False)
    pii_classification = Column(String)  # none, sensitive_business, uncertain
    pii_source = Column(String)  # "presidio" or "llm"
    sensitivity_reason = Column(Text)
    masking_strategy = Column(String)  # passthrough, substitute_realistic, format_preserving, suppress, generalise
    constraint_profile = Column(JSON)  # {min, max, regex, allowed_values, distribution_hint}
    business_importance = Column(String)  # critical, important, low
    edge_case_flags = Column(JSON)  # list of edge case descriptions
    dedup_mode = Column(String)  # entity, reference, event
    llm_confidence = Column(Float)
    abbreviation_resolved = Column(Boolean, default=False)
    notes = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("table_name", "column_name", name="uq_table_column"),
    )


class GenerationStrategy(Base):
    """Domain-level generation strategy decided by LLM."""
    __tablename__ = "generation_strategy"

    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String, nullable=False, unique=True)
    domain = Column(String)
    tier_override = Column(String)  # ctgan, tvae, rule_based, hybrid, or None
    temporal_constraints = Column(JSON)  # [{earlier_column, later_column}]
    post_generation_rules = Column(JSON)  # list of plain English rules
    edge_case_injection_pct = Column(Float, default=0.05)
    notes = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


class BoundaryKeyRegistry(Base):
    """Cross-domain FK stitching — stores generated PKs for downstream domains."""
    __tablename__ = "boundary_key_registry"

    id = Column(Integer, primary_key=True, autoincrement=True)
    domain = Column(String, nullable=False)
    table_name = Column(String, nullable=False)
    primary_key_column = Column(String, nullable=False)
    generated_key_value = Column(String, nullable=False)
    generation_run_id = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class DedupHashRegistry(Base):
    """SHA-256 hash registry for mode-aware deduplication."""
    __tablename__ = "dedup_hash_registry"

    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String, nullable=False)
    record_hash = Column(String, nullable=False)
    generation_run_id = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("table_name", "record_hash", name="uq_table_hash"),
    )


class GenerationRunLog(Base):
    """Tracks pipeline run state — supports crash recovery and resume."""
    __tablename__ = "generation_run_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, nullable=False, unique=True)
    status = Column(String)  # running, completed, failed, partial
    domains_completed = Column(JSON)
    domains_pending = Column(JSON)
    tables_completed = Column(JSON)
    validation_results = Column(JSON)
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)


class TableMetadataRecord(Base):
    """Stores table-level metadata extracted during ingestion."""
    __tablename__ = "table_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String, nullable=False, unique=True)
    row_count = Column(Integer, default=0)
    column_count = Column(Integer, default=0)
    domain = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PipelineRun(Base):
    """Tracks simple pipeline run meta status for UI"""
    __tablename__ = "pipeline_run"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, nullable=False, unique=True)
    status = Column(String)
    current_step = Column(String)
    progress_pct = Column(Float, default=0.0)
    table_filter = Column(JSON)
    fast_mode = Column(Boolean, default=False)
    started_at = Column(DateTime, default=datetime.utcnow)
    ended_at = Column(DateTime)



class HumanReviewQueue(Base):
    """Columns flagged for human review — low confidence or unknown abbreviations."""
    __tablename__ = "human_review_queue"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String)
    table_name = Column(String)
    column_name = Column(String)
    llm_best_guess = Column(JSON)  # JSON of LLM's best attempt
    flag_reason = Column(String)  # low_confidence, abbreviation_unknown, validation_failed
    is_blocking = Column(Boolean, default=False)
    status = Column(String, default="pending")  # pending, approved, corrected
    reviewer_notes = Column(Text)
    reviewed_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)


class ModelRegistry(Base):
    """Registry of trained CTGAN/TVAE models for reuse."""
    __tablename__ = "model_registry"

    id = Column(Integer, primary_key=True, autoincrement=True)
    domain = Column(String, nullable=False)
    table_name = Column(String, nullable=False)
    model_type = Column(String)  # ctgan, tvae
    model_path = Column(String, nullable=False)
    trained_on_run_id = Column(String)
    row_count_at_training = Column(Integer)
    column_metadata = Column(JSON)  # column types used during training
    training_epochs = Column(Integer)
    trained_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)


class PipelineStepLog(Base):
    """Granular tracking of each pipeline step for the dashboard."""
    __tablename__ = "pipeline_step_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, nullable=False)
    step_name = Column(String, nullable=False)  # e.g., "presidio_scan", "llm_reasoning"
    domain = Column(String)
    table_name = Column(String)
    status = Column(String)  # running, completed, failed, skipped
    details = Column(JSON)
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)
    duration_seconds = Column(Float)
