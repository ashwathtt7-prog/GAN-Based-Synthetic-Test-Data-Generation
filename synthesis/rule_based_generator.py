"""
Rule-Based Generator (Step 3.3 — for tables with <200 rows)
Generates records by sampling from constraint_profile (min, max, allowed_values, regex).
No neural network involved.
"""

import logging
import random
import re
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class RuleBasedGenerator:
    def __init__(self, table_name: str, policies: list, locale: str = "en_US"):
        self.table_name = table_name
        self.policies = {p.column_name: p for p in policies}
        self.fake = Faker(locale)

    def generate(self, source_df: pd.DataFrame, num_rows: int) -> pd.DataFrame:
        """
        Generate synthetic data using rule-based sampling from real data distributions.

        Args:
            source_df: The (masked) source data to derive distributions from
            num_rows: Number of synthetic rows to generate

        Returns:
            Synthetic DataFrame
        """
        logger.info(f"[RuleBased] Generating {num_rows} rows for {self.table_name}")
        synthetic_data = {}

        for col in source_df.columns:
            policy = self.policies.get(col)
            constraint = {}
            if policy:
                constraint = policy.constraint_profile if hasattr(policy, 'constraint_profile') else {}
                if constraint is None:
                    constraint = {}

            synthetic_data[col] = self._generate_column(
                col, source_df[col], constraint, num_rows
            )

        return pd.DataFrame(synthetic_data)

    def _generate_column(self, col_name: str, series: pd.Series, constraint: dict, n: int) -> list:
        """Generate values for a single column based on its distribution and constraints."""
        allowed_values = constraint.get("allowed_values")
        if allowed_values and len(allowed_values) > 0:
            # Sample from allowed values with real distribution
            return [random.choice(allowed_values) for _ in range(n)]

        # Determine column behavior from real data
        non_null = series.dropna()
        null_rate = series.isna().mean()

        if len(non_null) == 0:
            return [None] * n

        # Try to coerce object columns that are actually numeric
        is_numeric = pd.api.types.is_numeric_dtype(non_null)
        if not is_numeric and non_null.dtype == object:
            try:
                non_null = pd.to_numeric(non_null, errors='coerce').dropna()
                if len(non_null) > 0:
                    is_numeric = True
            except Exception:
                pass

        # Numeric column
        if is_numeric:
            min_val = constraint.get("min", float(non_null.min()))
            max_val = constraint.get("max", float(non_null.max()))
            mean_val = float(non_null.mean())
            std_val = float(non_null.std()) if len(non_null) > 1 else 0

            if non_null.dtype in (np.int64, np.int32, int):
                values = [int(np.clip(random.gauss(mean_val, max(std_val, 1)), min_val, max_val)) for _ in range(n)]
            else:
                values = [round(np.clip(random.gauss(mean_val, max(std_val, 0.01)), min_val, max_val), 2) for _ in range(n)]

            # Inject nulls at source rate
            for i in range(n):
                if random.random() < null_rate:
                    values[i] = None
            return values

        # Categorical / string column
        unique_count = non_null.nunique()
        if unique_count <= 50:
            # Categorical: sample from real distribution
            value_counts = non_null.value_counts(normalize=True)
            categories = value_counts.index.tolist()
            weights = value_counts.values.tolist()
            values = random.choices(categories, weights=weights, k=n)
        else:
            # High cardinality: sample with replacement from observed values
            values = non_null.sample(n=n, replace=True).tolist()

        # Inject nulls
        for i in range(n):
            if random.random() < null_rate:
                values[i] = None

        return values
