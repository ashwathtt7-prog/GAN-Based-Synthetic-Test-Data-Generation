"""
CTGAN Model Adapter
Wraps SDV's CTGAN for synthetic data generation based on LLM policies.
"""

import os

from synthesis.sdv_runtime import configure_sdv_runtime

configure_sdv_runtime()

import logging

import pandas as pd
from sdv.metadata import SingleTableMetadata
from sdv.single_table import CTGANSynthesizer
from synthesis.training_monitor import LossPollingMonitor

logger = logging.getLogger(__name__)


class CTGANModel:
    def __init__(self, table_name: str, policies: list):
        self.table_name = table_name
        self.policies = {p.column_name: p for p in policies}
        self.metadata = SingleTableMetadata()
        self.synthesizer = None

    def _detect_sdv_type(self, col_name: str, data_type: str, policy) -> dict:
        """Map generic SQL types and LLM policy to SDV metadata."""
        col_upper = col_name.upper()
        data_upper = data_type.upper()
        sdv_meta = {"sdtype": "categorical"}

        # Override with Presidio/LLM masking hints
        if policy and policy.masking_strategy == "format_preserving":
            if policy.pii_classification == "SSN" or "SSN" in col_upper:
                return {"sdtype": "id"}

        if any(token in data_upper for token in ("DATE", "DATETIME")) or any(
            token in col_upper for token in ("DATE", "_DT", "_TS", "TIME")
        ):
            return {"sdtype": "datetime", "datetime_format": "%Y-%m-%d %H:%M:%S"}

        if any(token in data_upper for token in ("INT", "DEC", "NUM", "FLOAT", "DOUBLE")):
            constraint_profile = getattr(policy, "constraint_profile", {}) or {}
            if constraint_profile.get("pattern_type") != "finite_categorical":
                return {"sdtype": "numerical"}

        if "ID" in col_upper and (not policy or getattr(policy, "dedup_mode", None) == "entity"):
            return {"sdtype": "id"}

        return sdv_meta

    def _build_sdv_metadata(self, df: pd.DataFrame):
        """Construct SDV Metadata dynamically from dataframe and LLM policy context."""
        meta_dict = {"columns": {}}

        for col in df.columns:
            # We skip generating completely suppressed columns
            policy = self.policies.get(col)
            if policy and policy.masking_strategy == "suppress":
                continue

            # Infer Type
            meta_dict["columns"][col] = self._detect_sdv_type(
                col_name=col,
                data_type=str(df[col].dtype),
                policy=policy,
            )

        self.metadata.detect_from_dataframe(data=df)

        for column_name, column_hint in meta_dict["columns"].items():
            try:
                self.metadata.update_column(column_name, **column_hint)
            except Exception as exc:
                logger.debug("Skipping SDV metadata hint for %s.%s: %s", self.table_name, column_name, exc)

        self._sanitize_primary_key()
        logger.info(f"SDV Metadata build complete for {self.table_name}")

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

    def _select_batch_size(self, row_count: int) -> tuple[int, int]:
        """Pick a smaller CTGAN batch shape for local stability."""
        pac = 2
        if row_count >= 20000:
            batch_size = 200
        elif row_count >= 5000:
            batch_size = 100
        else:
            batch_size = 50

        batch_size = max(batch_size, pac * 2)
        if batch_size % pac:
            batch_size += pac - (batch_size % pac)
        if batch_size % 2:
            batch_size += pac
        return batch_size, pac

    def train(self, df: pd.DataFrame, epochs: int = 150, emit_metric=None):
        """Train the CTGAN synthesizer."""
        logger.info(f"Training CTGAN on {self.table_name} with {len(df)} records for {epochs} epochs...")

        # Drop suppressed columns before training
        drop_cols = [c for c, p in self.policies.items() if p.masking_strategy == "suppress"]
        train_df = df.drop(columns=[c for c in drop_cols if c in df.columns])

        self._build_sdv_metadata(train_df)

        batch_size, pac = self._select_batch_size(len(train_df))
        self.synthesizer = CTGANSynthesizer(
            self.metadata,
            epochs=epochs,
            batch_size=batch_size,
            pac=pac,
            embedding_dim=64,
            generator_dim=(128, 128),
            discriminator_dim=(128, 128),
            enable_gpu=False,
            verbose=False,
        )

        monitor = LossPollingMonitor(self.synthesizer, "ctgan", emit_metric) if emit_metric else None
        if monitor:
            monitor.start()

        try:
            self.synthesizer.fit(train_df)
        finally:
            if monitor:
                monitor.stop()

        logger.info(f"CTGAN Model trained for {self.table_name}.")

    def generate(self, num_rows: int) -> pd.DataFrame:
        """Sample synthetic row data."""
        if not self.synthesizer:
            raise ValueError("Synthesizer not trained yet.")

        logger.info(f"Generating {num_rows} synthetic records for {self.table_name}...")
        synthetic_data = self.synthesizer.sample(num_rows=num_rows)
        return synthetic_data

    def save(self, output_dir: str):
        """Persist model to disk."""
        if self.synthesizer:
            os.makedirs(output_dir, exist_ok=True)
            self.synthesizer.save(f"{output_dir}/{self.table_name}_ctgan.pkl")

    def load(self, model_path: str):
        """Load a persisted CTGAN synthesizer from disk."""
        self.synthesizer = CTGANSynthesizer.load(model_path)
        logger.info(f"Loaded cached CTGAN model for {self.table_name}: {model_path}")
