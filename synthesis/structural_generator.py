"""
Deterministic structural column generation shared by all tiers.
"""

from __future__ import annotations

import random
import re

import pandas as pd


class StructuralColumnGenerator:
    def __init__(self, random_seed: int = 42):
        self.random = random.Random(random_seed)

    def generate(self, source_df: pd.DataFrame, columns: list[str], num_rows: int) -> pd.DataFrame:
        """Generate structural columns using deterministic/statistical sampling rules."""
        generated = {}
        for column in columns:
            if column not in source_df.columns:
                continue
            generated[column] = self._generate_column(column, source_df[column], num_rows)

        return pd.DataFrame(generated)

    def _generate_column(self, column: str, series: pd.Series, num_rows: int) -> list:
        non_null = series.dropna()
        null_rate = float(series.isna().mean()) if len(series) else 0.0
        upper = column.upper()

        if len(non_null) == 0:
            return [None] * num_rows

        if upper.endswith("_ID") and pd.api.types.is_numeric_dtype(series):
            values = list(range(1, num_rows + 1))
            return self._apply_nulls(values, null_rate)

        if (upper.endswith("_ID") or "UUID" in upper or "KEY" in upper) and not pd.api.types.is_numeric_dtype(series):
            values = [self._synthesize_identifier(non_null, idx + 1) for idx in range(num_rows)]
            return self._apply_nulls(values, null_rate)

        if pd.api.types.is_datetime64_any_dtype(series) or any(token in upper for token in ("DATE", "_DT", "_TS", "TIME")):
            sampled = non_null.sample(n=num_rows, replace=True, random_state=42).tolist()
            return self._apply_nulls(sampled, null_rate)

        if non_null.nunique(dropna=True) <= max(12, int(len(series) * 0.02)) or any(
            token in upper for token in ("_STAT", "_FLG", "_CD", "_TYPE")
        ):
            value_counts = non_null.astype(str).value_counts(normalize=True)
            categories = value_counts.index.tolist()
            weights = value_counts.values.tolist()
            sampled = self.random.choices(categories, weights=weights, k=num_rows)
            return self._apply_nulls(sampled, null_rate)

        sampled = non_null.sample(n=num_rows, replace=True, random_state=42).tolist()
        return self._apply_nulls(sampled, null_rate)

    def _synthesize_identifier(self, non_null: pd.Series, sequence_number: int) -> str:
        exemplar = str(non_null.iloc[(sequence_number - 1) % len(non_null)])
        match = re.match(r"^([A-Za-z_-]*)(\d+)([A-Za-z_-]*)$", exemplar)
        if match:
            prefix, digits, suffix = match.groups()
            return f"{prefix}{sequence_number:0{len(digits)}d}{suffix}"

        return f"{exemplar}_{sequence_number:05d}"

    def _apply_nulls(self, values: list, null_rate: float) -> list:
        adjusted = list(values)
        for index in range(len(adjusted)):
            if self.random.random() < null_rate:
                adjusted[index] = None
        return adjusted
