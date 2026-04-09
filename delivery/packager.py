"""
Delivery Packager
Exports generated data to Parquet/CSV, generates delivery manifest,
and packages for PLE delivery.
"""

import gzip
import importlib.util
import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
from models.schemas import DeliveryManifest
from config.config import load_config

logger = logging.getLogger(__name__)


class DeliveryPackager:
    def __init__(self, config: dict = None):
        self.config = config or load_config()
        delivery_cfg = self.config.get("delivery", {})
        self.output_format = delivery_cfg.get("output_format", "parquet")
        self.output_dir = delivery_cfg.get("output_directory", "output/synthetic")
        self.compress = delivery_cfg.get("compress", True)

    def package(
        self,
        run_id: str,
        synthetic_datasets: dict,
        validation_results: dict,
        generation_strategies: dict,
        edge_case_coverage: dict,
        domains: list[str]
    ) -> DeliveryManifest:
        """
        Export all synthetic data and generate delivery manifest.

        Args:
            run_id: Pipeline run ID
            synthetic_datasets: {table_name: DataFrame}
            validation_results: {table_name: [ValidationResult dicts]}
            generation_strategies: {table_name: strategy_tier}
            edge_case_coverage: {table_name: float pct}
            domains: List of domain names

        Returns:
            DeliveryManifest
        """
        run_dir = Path(self.output_dir) / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        row_counts = {}
        actual_output_format = self.output_format
        parquet_available = self.output_format != "parquet" or self._parquet_engine_available()
        if self.output_format == "parquet" and not parquet_available:
            actual_output_format = "csv"
            logger.warning(
                "[Delivery] Parquet engine not available. Falling back to CSV export for run %s.",
                run_id,
            )

        for table_name, df in synthetic_datasets.items():
            # Remove internal metadata columns
            export_df = df.drop(columns=['_edge_case'], errors='ignore')
            export_df = self._normalize_for_export(export_df)

            if actual_output_format == "parquet":
                path = run_dir / f"{table_name}.parquet"
                export_df.to_parquet(path, index=False)
            else:
                path = run_dir / f"{table_name}.csv"
                export_df.to_csv(path, index=False)

            row_counts[table_name] = len(export_df)
            logger.info(f"[Delivery] Exported {table_name}: {len(export_df)} rows → {path}")

        # Serialize validation results
        serialized_results = {}
        for table_name, results in validation_results.items():
            serialized_results[table_name] = [
                r.model_dump() if hasattr(r, 'model_dump') else r
                for r in results
            ]

        # Generate manifest
        manifest = DeliveryManifest(
            run_id=run_id,
            tables_generated=list(synthetic_datasets.keys()),
            row_counts=row_counts,
            validation_results=serialized_results,
            edge_case_coverage=edge_case_coverage,
            generation_strategies=generation_strategies,
            domains=domains,
            timestamp=datetime.utcnow().isoformat(),
            output_format=actual_output_format,
            output_path=str(run_dir)
        )

        # Write manifest
        manifest_path = run_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest.model_dump(), f, indent=2, default=str)

        logger.info(f"[Delivery] Manifest written to {manifest_path}")

        # Compress if configured
        if self.compress:
            archive_path = Path(self.output_dir) / f"{run_id}.tar.gz"
            shutil.make_archive(
                str(run_dir),
                'gztar',
                root_dir=str(Path(self.output_dir)),
                base_dir=run_id
            )
            logger.info(f"[Delivery] Compressed archive: {archive_path}")

        return manifest

    def _normalize_for_export(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize mixed pandas object columns before parquet/CSV export."""
        export_df = df.copy()
        for column in export_df.columns:
            series = export_df[column]
            if pd.api.types.is_object_dtype(series):
                non_null = series.dropna()
                if non_null.empty:
                    continue

                if non_null.map(lambda value: isinstance(value, (pd.Timestamp, datetime))).any():
                    export_df[column] = series.map(
                        lambda value: value.isoformat() if isinstance(value, (pd.Timestamp, datetime)) else value
                    )

        return export_df

    def _parquet_engine_available(self) -> bool:
        """Return True when pandas has an installed parquet backend to use."""
        return any(
            importlib.util.find_spec(engine) is not None
            for engine in ("pyarrow", "fastparquet")
        )
