"""
TVAE Model Adapter
Wraps SDV's TVAE for synthetic data generation of medium-sized tables (200-2000 rows).
"""

import os

from synthesis.sdv_runtime import configure_sdv_runtime

configure_sdv_runtime()

import logging

import pandas as pd
from sdv.metadata import SingleTableMetadata
from sdv.single_table import TVAESynthesizer
from synthesis.training_monitor import LossPollingMonitor

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
            dtype_upper = str(df[col].dtype).upper()

            try:
                if "DATE" in dtype_upper or "DATE" in col_upper or "DT" in col_upper or "TIME" in col_upper:
                    self.metadata.update_column(col, sdtype="datetime", datetime_format="%Y-%m-%d")
                elif "ID" in col_upper and (not policy or getattr(policy, 'dedup_mode', '') == 'entity'):
                    self.metadata.update_column(col, sdtype="id")
                elif any(token in dtype_upper for token in ("INT", "DEC", "NUM", "FLOAT", "DOUBLE")):
                    self.metadata.update_column(col, sdtype="numerical")
            except Exception as exc:
                logger.debug("Skipping TVAE metadata hint for %s.%s: %s", self.table_name, col, exc)

        self._sanitize_primary_key()
        logger.info(f"SDV Metadata built for TVAE on {self.table_name}")

    def _sanitize_primary_key(self):
        """Remove auto-detected primary keys that are not true identifier columns."""
        primary_key = getattr(self.metadata, "primary_key", None)
        if not primary_key:
            return

        column_meta = getattr(self.metadata, "columns", {}).get(primary_key, {})
        if column_meta.get("sdtype") != "id":
            try:
                self.metadata.remove_primary_key()
            except Exception:
                self.metadata.primary_key = None

    def _select_batch_size(self, row_count: int) -> int:
        if row_count >= 5000:
            return 256
        if row_count >= 1000:
            return 128
        return 64

    def train(self, df: pd.DataFrame, epochs: int = 300, emit_metric=None):
        """Train the TVAE synthesizer."""
        logger.info(f"Training TVAE on {self.table_name} with {len(df)} records for {epochs} epochs...")

        # Drop suppressed columns
        drop_cols = [c for c, p in self.policies.items() if p.masking_strategy == "suppress"]
        train_df = df.drop(columns=[c for c in drop_cols if c in df.columns])

        self._build_sdv_metadata(train_df)

        self.synthesizer = TVAESynthesizer(
            self.metadata,
            epochs=epochs,
            batch_size=self._select_batch_size(len(train_df)),
            compress_dims=(128, 128),
            decompress_dims=(128, 128),
            embedding_dim=64,
            enable_gpu=False,
            verbose=False,
        )

        monitor = LossPollingMonitor(self.synthesizer, "tvae", emit_metric) if emit_metric else None
        if monitor:
            monitor.start()

        try:
            self.synthesizer.fit(train_df)
        finally:
            if monitor:
                monitor.stop()

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

    def load(self, model_path: str):
        """Load a persisted TVAE synthesizer from disk."""
        self.synthesizer = TVAESynthesizer.load(model_path)
        logger.info(f"Loaded cached TVAE model for {self.table_name}: {model_path}")
