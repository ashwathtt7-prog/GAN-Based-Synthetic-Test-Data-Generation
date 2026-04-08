"""
Schema Connector
Replaces Spark JDBC for POC. Uses SQLAlchemy to connect to the source database
and extract table architectures and statistical profiles.
"""

import logging
from sqlalchemy import create_engine, MetaData, select, func, text
from sqlalchemy.engine import Engine
import pandas as pd
from models.schemas import TableMetadata, StatisticalProfile
logger = logging.getLogger(__name__)

class SchemaConnector:
    def __init__(self, connection_string: str, sample_size: int = 100):
        self.engine = create_engine(connection_string)
        self.sample_size = sample_size
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        
    def extract_schema(self) -> list[TableMetadata]:
        """Extract schema and statistical profile for all tables."""
        tables_meta = []
        
        for table_name, table in self.metadata.tables.items():
            logger.info(f"Extracting schema for table: {table_name}")
            
            # 1. Get Basic Table Stats
            with self.engine.connect() as conn:
                try:
                    row_count = conn.execute(select(func.count()).select_from(table)).scalar()
                except Exception as e:
                    logger.warning(f"Could not count rows for {table_name}: {e}")
                    row_count = 0
            
            table_meta = TableMetadata(
                table_name=table_name,
                row_count=row_count,
                column_count=len(table.columns)
            )
            
            # 2. Extract column profiles
            for col in table.columns:
                col_type = str(col.type)
                profile = StatisticalProfile(
                    column_name=col.name,
                    data_type=col_type
                )
                
                # If table has data, get samples and stats
                if row_count > 0:
                    profile = self._compute_column_stats(table_name, col.name, col_type, row_count, profile)
                    
                table_meta.columns.append(profile)
                
            tables_meta.append(table_meta)
            
        return tables_meta

    def _compute_column_stats(self, table_name: str, col_name: str, col_type: str, total_rows: int, profile: StatisticalProfile) -> StatisticalProfile:
        """Compute statistics using pandas for simplicity in POC."""
        query = f"SELECT {col_name} FROM {table_name} LIMIT {self.sample_size}"
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
