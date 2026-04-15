"""
Database client — session management and query helpers.
Handles SQLite for POC, swappable to PostgreSQL via config.
"""

import yaml
import sqlite3
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
        self.backfill_legacy_source_names()
        self.prune_invalid_cached_policies()
        self.prune_non_override_defect_configs()
        self.mark_incomplete_runs_failed()

    def ensure_schema_compatibility(self):
        """Add newly introduced columns for existing SQLite databases."""
        additions = {
            "column_policy": {
                "source_name": "VARCHAR",
            },
            "generation_strategy": {
                "source_name": "VARCHAR",
            },
            "generation_run_log": {
                "source_name": "VARCHAR",
            },
            "table_metadata": {
                "source_name": "VARCHAR",
            },
            "pipeline_run": {
                "source_name": "VARCHAR",
                "table_filter": "TEXT",
                "fast_mode": "BOOLEAN DEFAULT 0",
            },
            "human_review_queue": {
                "run_id": "VARCHAR",
                "source_name": "VARCHAR",
                "is_blocking": "BOOLEAN DEFAULT 0",
            },
            "pipeline_step_log": {
                "source_name": "VARCHAR",
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

    def backfill_legacy_source_names(self):
        """Map pre-source-selector rows onto the default source so unique keys don't collide."""
        sources = list(self.config.get("data_sources", []) or [])
        if not sources:
            return

        default_source = next((source for source in sources if source.get("default")), None) or sources[0]
        default_source_name = default_source.get("name")
        if not default_source_name:
            return

        source_tables = [
            "column_policy",
            "generation_strategy",
            "generation_run_log",
            "table_metadata",
            "pipeline_run",
            "human_review_queue",
            "pipeline_step_log",
        ]

        db_url = self.config.get("database", {}).get("url", "")
        if db_url.startswith("sqlite:///"):
            db_path = Path(db_url.replace("sqlite:///", "", 1))
            if not db_path.is_absolute():
                db_path = Path(__file__).parent.parent / db_path
            conn = sqlite3.connect(db_path)
            try:
                cur = conn.cursor()
                existing_tables = {
                    row[0] for row in cur.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
                for table_name in source_tables:
                    if table_name not in existing_tables:
                        continue
                    columns = {
                        row[1] for row in cur.execute(f"PRAGMA table_info({table_name})").fetchall()
                    }
                    if "source_name" not in columns:
                        continue
                    cur.execute(
                        f"UPDATE {table_name} SET source_name = ? WHERE source_name IS NULL",
                        (default_source_name,),
                    )
                conn.commit()
            finally:
                conn.close()
            return

        inspector = inspect(self.engine)
        with self.engine.begin() as connection:
            for table_name in source_tables:
                if table_name not in inspector.get_table_names():
                    continue
                columns = {column["name"] for column in inspector.get_columns(table_name)}
                if "source_name" not in columns:
                    continue
                connection.execute(
                    text(f"UPDATE {table_name} SET source_name = :source_name WHERE source_name IS NULL"),
                    {"source_name": default_source_name},
                )

    def prune_invalid_cached_policies(self):
        """Drop cached policies that no longer match the real source schema."""
        from db.schema import ColumnPolicy

        source_schema: dict[str, dict[str, set[str]]] = {}
        for source in self.config.get("data_sources", []) or []:
            source_name = source.get("name")
            conn_str = source.get("connection_string")
            if not source_name or not conn_str:
                continue
            try:
                inspector = inspect(create_engine(conn_str))
                table_map: dict[str, set[str]] = {}
                for table_name in inspector.get_table_names():
                    if str(table_name).startswith("_"):
                        continue
                    try:
                        table_map[table_name] = {
                            col["name"] for col in inspector.get_columns(table_name)
                        }
                    except Exception:
                        continue
                source_schema[source_name] = table_map
            except Exception:
                continue

        with self.session() as session:
            policies = session.query(ColumnPolicy).all()
            for policy in policies:
                if str(policy.table_name or "").startswith("_"):
                    session.delete(policy)
                    continue

                source_tables = source_schema.get(policy.source_name or "")
                if not source_tables:
                    continue

                valid_columns = source_tables.get(policy.table_name)
                if not valid_columns or policy.column_name not in valid_columns:
                    session.delete(policy)

    def prune_non_override_defect_configs(self):
        """Drop stale human-review rows that should not persist as overrides."""
        from db.schema import DefectRuleConfig

        with self.session() as session:
            stale_configs = session.query(DefectRuleConfig).filter(
                DefectRuleConfig.review_status != "approved"
            ).all()
            for config in stale_configs:
                session.delete(config)

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
            source_name=policy_data.get("source_name"),
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

    def get_column_policies(self, session: Session, table_name: str = None, source_name: str = None):
        """Get column policies, optionally filtered by table."""
        from db.schema import ColumnPolicy
        query = session.query(ColumnPolicy)
        if source_name is not None:
            query = query.filter_by(source_name=source_name)
        if table_name:
            query = query.filter_by(table_name=table_name)
        return query.all()

    def get_domain_column_policies(self, session: Session, domain: str, source_name: str = None):
        """Get all column policies for tables in a domain."""
        from db.schema import ColumnPolicy, GenerationStrategy
        strategy_query = session.query(GenerationStrategy).filter_by(domain=domain)
        if source_name is not None:
            strategy_query = strategy_query.filter_by(source_name=source_name)
        table_names = [gs.table_name for gs in strategy_query.all()]
        policy_query = session.query(ColumnPolicy).filter(ColumnPolicy.table_name.in_(table_names))
        if source_name is not None:
            policy_query = policy_query.filter_by(source_name=source_name)
        return policy_query.all()

    # === Generation Strategy Operations ===
    def upsert_generation_strategy(self, session: Session, strategy_data: dict):
        """Insert or update a generation strategy."""
        from db.schema import GenerationStrategy
        existing = session.query(GenerationStrategy).filter_by(
            source_name=strategy_data.get("source_name"),
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
    def create_run_log(self, session: Session, run_id: str, domains: list, source_name: str = None):
        """Create a new generation run log entry."""
        from db.schema import GenerationRunLog
        log = GenerationRunLog(
            run_id=run_id,
            source_name=source_name,
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
                           source_name: str = None,
                           status: str = "running", details: dict = None):
        """Log a pipeline step for dashboard tracking."""
        from db.schema import PipelineStepLog
        step = PipelineStepLog(
            run_id=run_id,
            source_name=source_name,
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

    # === Defect Rule Config Operations ===
    def upsert_defect_rule_config(self, session: Session, config_data: dict):
        """Insert or update a source-specific production-defect rule config."""
        from db.schema import DefectRuleConfig

        existing = session.query(DefectRuleConfig).filter_by(
            source_name=config_data["source_name"],
            rule_key=config_data["rule_key"],
        ).first()

        if existing:
            for key, value in config_data.items():
                setattr(existing, key, value)
            return existing

        record = DefectRuleConfig(**config_data)
        session.add(record)
        return record

    def get_defect_rule_configs(self, session: Session, source_name: str):
        """Return all production-defect rule configs for a source."""
        from db.schema import DefectRuleConfig

        return session.query(DefectRuleConfig).filter_by(source_name=source_name).all()

    def delete_defect_rule_config(self, session: Session, source_name: str, rule_key: str):
        """Delete a production-defect rule config override."""
        from db.schema import DefectRuleConfig

        record = session.query(DefectRuleConfig).filter_by(
            source_name=source_name,
            rule_key=rule_key,
        ).first()
        if record:
            session.delete(record)
        return record

    # === Failed Case Scenario Operations ===
    def upsert_failed_case_scenario(self, session: Session, scenario_data: dict):
        """Insert or update a stored failed-case scenario payload."""
        from db.schema import FailedCaseScenario

        existing = session.query(FailedCaseScenario).filter_by(
            scenario_id=scenario_data["scenario_id"]
        ).first()
        if existing:
            for key, value in scenario_data.items():
                setattr(existing, key, value)
            return existing

        record = FailedCaseScenario(**scenario_data)
        session.add(record)
        return record
