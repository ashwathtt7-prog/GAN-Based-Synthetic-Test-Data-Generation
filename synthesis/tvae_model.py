"""
TVAE Model Adapter
Wraps SDV's TVAE for synthetic data generation of medium-sized tables (200-2000 rows).
"""

import logging
import os
import pandas as pd
from sdv.single_table import TVAESynthesizer
from sdv.metadata import SingleTableMetadata

logger = logging.getLogger(__name__)


class TVAEModel:
    def __init__(self, table_name: str, policies: list):
        self.table_name = table_name
        self.policies = {p.column_name: p for p in policies}
        self.metadata = SingleTableMetadata()
        self.synthesizer = None

    def _build_sdv_metadata(self, df: pd.DataFrame):
        """Construct SDV Metadata from dataframe and LLM policy context."""
        self.metadata.detect_from_dataframe(data=df)

        # Apply policy-driven overrides
        for col in df.columns:
            policy = self.policies.get(col)
            if not policy:
                continue
            col_upper = col.upper()

            try:
                if "DATE" in str(df[col].dtype).upper() or "DATE" in col_upper or "DT" in col_upper:
                    self.metadata.update_column(col, sdtype="datetime", datetime_format="%Y-%m-%d")
                elif "ID" in col_upper and (not policy or getattr(policy, 'dedup_mode', '') == 'entity'):
                    self.metadata.update_column(col, sdtype="id")
            except Exception:
                pass

        logger.info(f"SDV Metadata built for TVAE on {self.table_name}")

    def train(self, df: pd.DataFrame, epochs: int = 300):
        """Train the TVAE synthesizer."""
        logger.info(f"Training TVAE on {self.table_name} with {len(df)} records for {epochs} epochs...")

        # Drop suppressed columns
        drop_cols = [c for c, p in self.policies.items() if p.masking_strategy == "suppress"]
        train_df = df.drop(columns=[c for c in drop_cols if c in df.columns])

        self._build_sdv_metadata(train_df)

        self.synthesizer = TVAESynthesizer(self.metadata, epochs=epochs)
        self.synthesizer.fit(train_df)
        logger.info(f"TVAE Model trained for {self.table_name}.")

    def generate(self, num_rows: int) -> pd.DataFrame:
        """Sample synthetic row data."""
        if not self.synthesizer:
            raise ValueError("Synthesizer not trained yet.")

        logger.info(f"Generating {num_rows} synthetic records (TVAE) for {self.table_name}...")
        return self.synthesizer.sample(num_rows=num_rows)

    def save(self, output_dir: str):
        """Persist model to disk."""
        if self.synthesizer:
            os.makedirs(output_dir, exist_ok=True)
            path = f"{output_dir}/{self.table_name}_tvae.pkl"
            self.synthesizer.save(path)
            logger.info(f"TVAE model saved: {path}")
