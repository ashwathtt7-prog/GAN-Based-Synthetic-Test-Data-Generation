"""
Schema Connector
Replaces Spark JDBC for POC. Uses SQLAlchemy to connect to the source database
and extract table architectures and statistical profiles.

Dialect support: SQLite (via SQLAlchemy reflection) and DuckDB (via native
`information_schema` queries — SQLAlchemy's ``duckdb_engine`` emits
Postgres-specific ``pg_catalog`` queries that fail on DuckDB 1.5+, so we
bypass reflection for DuckDB).
"""

import logging
from dataclasses import dataclass, field
from sqlalchemy import create_engine, MetaData, select, func, text
from sqlalchemy.engine import Engine
import pandas as pd
from models.schemas import TableMetadata, StatisticalProfile
logger = logging.getLogger(__name__)


def _is_internal_table(table_name: str) -> bool:
    """Hide backend-only helper tables from the live schema pipeline."""
    return str(table_name).startswith("_")


@dataclass
class _SimpleColumn:
    """Lightweight stand-in for SQLAlchemy ``Column`` used by the DuckDB path."""
    name: str
    type: str


@dataclass
class _SimpleTable:
    """Lightweight stand-in for SQLAlchemy ``Table`` used by the DuckDB path."""
    name: str
    columns: list = field(default_factory=list)


class SchemaConnector:
    def __init__(self, connection_string: str, sample_size: int = 100):
        self.engine = create_engine(connection_string)
        self.sample_size = sample_size
        self.dialect = self.engine.dialect.name
        self.metadata = MetaData()

        if self.dialect == "duckdb":
            # Skip SQLAlchemy reflection for DuckDB — duckdb_engine's column
            # introspection still emits ``pg_catalog.pg_collation`` joins
            # which do not exist in DuckDB's catalog. Build a native table map
            # directly from ``information_schema``.
            self._duck_tables = self._introspect_duckdb()
        else:
            self.metadata.reflect(bind=self.engine)
            self._duck_tables = None

    # ------------------------------------------------------------------ #
    def _introspect_duckdb(self) -> dict[str, _SimpleTable]:
        """Use DuckDB's ``information_schema`` to build a Table/Column map."""
        tables: dict[str, _SimpleTable] = {}
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT table_name, column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema IN ('main', 'public')
                    ORDER BY table_name, ordinal_position
                    """
                )
            ).fetchall()
        for table_name, column_name, data_type in rows:
            if _is_internal_table(table_name):
                continue
            tables.setdefault(table_name, _SimpleTable(name=table_name)).columns.append(
                _SimpleColumn(name=column_name, type=data_type)
            )
        logger.info(
            "DuckDB introspection found %d tables (skipped SQLAlchemy reflection)",
            len(tables),
        )
        return tables

    # ------------------------------------------------------------------ #
    def _iter_tables(self):
        """Yield (table_name, table_obj) pairs regardless of dialect."""
        if self._duck_tables is not None:
            for name, table in self._duck_tables.items():
                yield name, table
        else:
            for name, table in self.metadata.tables.items():
                if _is_internal_table(name):
                    continue
                yield name, table

    # ------------------------------------------------------------------ #
    def extract_schema(self) -> list[TableMetadata]:
        """Extract schema and statistical profile for all tables."""
        tables_meta = []

        for table_name, table in self._iter_tables():
            logger.info(f"Extracting schema for table: {table_name}")

            # 1. Get Basic Table Stats
            with self.engine.connect() as conn:
                try:
                    row_count = conn.execute(
                        text(f'SELECT COUNT(*) FROM "{table_name}"')
                    ).scalar()
                except Exception as e:
                    logger.warning(f"Could not count rows for {table_name}: {e}")
                    row_count = 0

            table_meta = TableMetadata(
                table_name=table_name,
                row_count=int(row_count or 0),
                column_count=len(table.columns),
            )

            # 2. Extract column profiles
            for col in table.columns:
                col_type = str(col.type)
                profile = StatisticalProfile(
                    column_name=col.name,
                    data_type=col_type,
                )

                if table_meta.row_count > 0:
                    profile = self._compute_column_stats(
                        table_name, col.name, col_type, table_meta.row_count, profile
                    )

                table_meta.columns.append(profile)

            tables_meta.append(table_meta)

        return tables_meta

    def _compute_column_stats(self, table_name: str, col_name: str, col_type: str, total_rows: int, profile: StatisticalProfile) -> StatisticalProfile:
        """Compute statistics using pandas for simplicity in POC."""
        query = f'SELECT "{col_name}" FROM "{table_name}" LIMIT {self.sample_size}'
        try:
            df = pd.read_sql(query, self.engine)
            
            # Basic stats
            null_count = int(df[col_name].isna().sum())
            profile.null_count = null_count
            profile.null_rate = null_count / total_rows if total_rows > 0 else 0
            profile.row_count = total_rows
            
            non_null_df = df.dropna()
            profile.unique_count = int(non_null_df[col_name].nunique())
            
            # Top values
            if profile.unique_count > 0:
                value_counts = non_null_df[col_name].value_counts().head(10)
                profile.top_values = [{"value": str(k), "frequency": int(v)} for k, v in value_counts.items()]
            
            # Num stats
            if pd.api.types.is_numeric_dtype(non_null_df[col_name]):
                profile.min_value = str(non_null_df[col_name].min())
                profile.max_value = str(non_null_df[col_name].max())
                profile.mean_value = float(non_null_df[col_name].mean())
                profile.std_dev = float(non_null_df[col_name].std())
                
        except Exception as e:
            logger.warning(f"Error computing stats for {table_name}.{col_name}: {e}")
            
        return profile
