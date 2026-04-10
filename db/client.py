"""
Database client — session management and query helpers.
Handles SQLite for POC, swappable to PostgreSQL via config.
"""

import yaml
from pathlib import Path
from contextlib import contextmanager
from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime
from db.schema import Base


def load_config() -> dict:
    """Load configuration from config.yaml."""
    config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_engine(config: dict = None):
    """Create SQLAlchemy engine from config."""
    if config is None:
        config = load_config()

    db_url = config["database"]["url"]
    echo = config["database"].get("echo", False)

    engine = create_engine(db_url, echo=echo)

    # Enable WAL mode and foreign keys for SQLite
    if "sqlite" in db_url:
        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    return engine


def init_db(engine=None):
    """Create all tables if they don't exist."""
    if engine is None:
        engine = get_engine()
    Base.metadata.create_all(engine)
    return engine


def get_session_factory(engine=None) -> sessionmaker:
    """Get a session factory bound to the engine."""
    if engine is None:
        engine = get_engine()
    return sessionmaker(bind=engine, expire_on_commit=False)


@contextmanager
def get_session(engine=None):
    """Context manager for database sessions with auto-commit/rollback."""
    factory = get_session_factory(engine)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


class DatabaseClient:
    """
    High-level database client for the pipeline.
    Provides CRUD operations for all operational tables.
    """

    def __init__(self, config: dict = None):
        self.config = config or load_config()
        self.engine = get_engine(self.config)
        self.SessionFactory = get_session_factory(self.engine)

    def initialize(self):
        """Create all tables."""
        init_db(self.engine)
        self.ensure_schema_compatibility()
        self.mark_incomplete_runs_failed()

    def ensure_schema_compatibility(self):
        """Add newly introduced columns for existing SQLite databases."""
        additions = {
            "pipeline_run": {
                "table_filter": "TEXT",
                "fast_mode": "BOOLEAN DEFAULT 0",
            },
            "human_review_queue": {
                "run_id": "VARCHAR",
                "is_blocking": "BOOLEAN DEFAULT 0",
            },
        }

        inspector = inspect(self.engine)
        with self.engine.begin() as connection:
            for table_name, columns in additions.items():
                existing = {column["name"] for column in inspector.get_columns(table_name)}
                for column_name, sql_type in columns.items():
                    if column_name in existing:
                        continue
                    connection.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {sql_type}"))

    @contextmanager
    def session(self):
        """Get a managed session."""
        session = self.SessionFactory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # Alias for backward compatibility
    get_session = session

    def mark_incomplete_runs_failed(self):
        """Mark stale in-progress runs as failed when the app starts fresh."""
        from db.schema import PipelineRun, GenerationRunLog

        with self.session() as session:
            now = datetime.utcnow()
            stale_runs = session.query(PipelineRun).filter(
                PipelineRun.status.in_(["initialized", "running", "waiting_review"])
            ).all()
            for run in stale_runs:
                run.status = "failed"
                run.current_step = (run.current_step or "Unknown") + " (interrupted)"
                run.ended_at = now

            stale_logs = session.query(GenerationRunLog).filter(
                GenerationRunLog.status.in_(["running", "waiting_review"])
            ).all()
            for log in stale_logs:
                log.status = "failed"
                log.completed_at = now

    # === Column Policy Operations ===
    def upsert_column_policy(self, session: Session, policy_data: dict):
        """Insert or update a column policy."""
        from db.schema import ColumnPolicy
        existing = session.query(ColumnPolicy).filter_by(
            table_name=policy_data["table_name"],
            column_name=policy_data["column_name"]
        ).first()

        if existing:
            for key, value in policy_data.items():
                setattr(existing, key, value)
            return existing
        else:
            policy = ColumnPolicy(**policy_data)
            session.add(policy)
            return policy

    def get_column_policies(self, session: Session, table_name: str = None):
        """Get column policies, optionally filtered by table."""
        from db.schema import ColumnPolicy
        query = session.query(ColumnPolicy)
        if table_name:
            query = query.filter_by(table_name=table_name)
        return query.all()

    def get_domain_column_policies(self, session: Session, domain: str):
        """Get all column policies for tables in a domain."""
        from db.schema import ColumnPolicy, GenerationStrategy
        table_names = [
            gs.table_name for gs in
            session.query(GenerationStrategy).filter_by(domain=domain).all()
        ]
        return session.query(ColumnPolicy).filter(
            ColumnPolicy.table_name.in_(table_names)
        ).all()

    # === Generation Strategy Operations ===
    def upsert_generation_strategy(self, session: Session, strategy_data: dict):
        """Insert or update a generation strategy."""
        from db.schema import GenerationStrategy
        existing = session.query(GenerationStrategy).filter_by(
            table_name=strategy_data["table_name"]
        ).first()

        if existing:
            for key, value in strategy_data.items():
                setattr(existing, key, value)
            return existing
        else:
            strategy = GenerationStrategy(**strategy_data)
            session.add(strategy)
            return strategy

    # === Boundary Key Registry Operations ===
    def register_boundary_keys(self, session: Session, domain: str,
                                table_name: str, pk_column: str,
                                key_values: list, run_id: str):
        """Register generated primary key values for cross-domain FK stitching."""
        from db.schema import BoundaryKeyRegistry
        for value in key_values:
            entry = BoundaryKeyRegistry(
                domain=domain,
                table_name=table_name,
                primary_key_column=pk_column,
                generated_key_value=str(value),
                generation_run_id=run_id
            )
            session.add(entry)

    def get_boundary_keys(self, session: Session, table_name: str, run_id: str = None):
        """Get registered boundary keys for a table."""
        from db.schema import BoundaryKeyRegistry
        query = session.query(BoundaryKeyRegistry).filter_by(table_name=table_name)
        if run_id:
            query = query.filter_by(generation_run_id=run_id)
        return [r.generated_key_value for r in query.all()]

    # === Human Review Queue Operations ===
    def add_to_review_queue(self, session: Session, review_data: dict):
        """Add an item to the human review queue."""
        from db.schema import HumanReviewQueue
        item = HumanReviewQueue(**review_data)
        session.add(item)
        return item

    def get_pending_reviews(self, session: Session, run_id: str = None, blocking_only: bool = False):
        """Get all pending review items."""
        from db.schema import HumanReviewQueue
        query = session.query(HumanReviewQueue).filter_by(status="pending")
        if run_id:
            query = query.filter_by(run_id=run_id)
        if blocking_only:
            query = query.filter_by(is_blocking=True)
        return query.order_by(HumanReviewQueue.created_at.asc()).all()

    # === Run Log Operations ===
    def create_run_log(self, session: Session, run_id: str, domains: list):
        """Create a new generation run log entry."""
        from db.schema import GenerationRunLog
        log = GenerationRunLog(
            run_id=run_id,
            status="running",
            domains_completed=[],
            domains_pending=domains,
            tables_completed=[]
        )
        session.add(log)
        return log

    def update_run_log(self, session: Session, run_id: str, **kwargs):
        """Update a run log entry."""
        from db.schema import GenerationRunLog
        log = session.query(GenerationRunLog).filter_by(run_id=run_id).first()
        if log:
            for key, value in kwargs.items():
                setattr(log, key, value)
        return log

    # === Pipeline Step Log Operations ===
    def log_pipeline_step(self, session: Session, run_id: str, step_name: str,
                           domain: str = None, table_name: str = None,
                           status: str = "running", details: dict = None):
        """Log a pipeline step for dashboard tracking."""
        from db.schema import PipelineStepLog
        step = PipelineStepLog(
            run_id=run_id,
            step_name=step_name,
            domain=domain,
            table_name=table_name,
            status=status,
            details=details
        )
        session.add(step)
        return step

    # === Model Registry Operations ===
    def register_model(self, session: Session, model_data: dict):
        """Register a trained model."""
        from db.schema import ModelRegistry
        table_name = model_data.get("table_name")
        model_type = model_data.get("model_type")
        if table_name and model_type:
            session.query(ModelRegistry).filter_by(
                table_name=table_name,
                model_type=model_type,
                is_active=True,
            ).update({"is_active": False})

        model = ModelRegistry(**model_data)
        session.add(model)
        return model

    def get_active_model(self, session: Session, table_name: str, model_type: str = None):
        """Get the active model for a table."""
        from db.schema import ModelRegistry
        query = session.query(ModelRegistry).filter_by(
            table_name=table_name, is_active=True
        )
        if model_type:
            query = query.filter_by(model_type=model_type)
        return query.order_by(ModelRegistry.trained_at.desc()).first()

    def get_registered_models(self, session: Session, table_name: str, model_type: str = None, limit: int = 10):
        """Return recent registered models for compatibility matching."""
        from db.schema import ModelRegistry
        query = session.query(ModelRegistry).filter_by(table_name=table_name)
        if model_type:
            query = query.filter_by(model_type=model_type)
        return query.order_by(ModelRegistry.is_active.desc(), ModelRegistry.trained_at.desc()).limit(limit).all()
