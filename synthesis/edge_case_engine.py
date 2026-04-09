"""
Edge Case Injection Engine (Step 3.6)
After baseline generation, injects additional records specifically targeting
flagged edge case combinations.
"""

import logging
import random
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class EdgeCaseEngine:
    def __init__(self):
        pass

    def inject_edge_cases(
        self,
        table_name: str,
        baseline_df: pd.DataFrame,
        column_policies: list,
        injection_pct: float = 0.05
    ) -> pd.DataFrame:
        """
        Generate edge case records and append to the baseline synthetic data.

        Args:
            table_name: Name of the table
            baseline_df: Baseline synthetic data from CTGAN/TVAE/rule-based
            column_policies: List of ColumnPolicySchema with edge_case_flags
            injection_pct: Percentage of edge case records (0.0 to 0.3)

        Returns:
            DataFrame with edge case records appended, tagged with _edge_case column
        """
        if injection_pct <= 0:
            baseline_df['_edge_case'] = False
            return baseline_df

        num_edge = max(1, int(len(baseline_df) * injection_pct))
        logger.info(f"[EdgeCase] Injecting {num_edge} edge case records into {table_name}")

        # Collect all edge case flags
        edge_specs = []
        policy_map = {p.column_name: p for p in column_policies if hasattr(p, 'column_name')}

        for col, policy in policy_map.items():
            flags = policy.edge_case_flags if hasattr(policy, 'edge_case_flags') else []
            if flags and col in baseline_df.columns:
                for flag in flags:
                    edge_specs.append({"column": col, "flag": flag, "policy": policy})

        if not edge_specs:
            logger.info(f"[EdgeCase] No edge case flags found for {table_name}, skipping.")
            baseline_df['_edge_case'] = False
            return baseline_df

        # Generate edge case records
        result_df = baseline_df.copy().reset_index(drop=True)
        result_df['_edge_case'] = False
        replace_indices = random.sample(range(len(result_df)), k=min(num_edge, len(result_df)))

        for row_index in replace_indices:
            template = result_df.iloc[row_index].to_dict()

            # Apply a random edge case spec
            spec = random.choice(edge_specs)
            col = spec["column"]
            flag = spec["flag"].lower()

            template = self._apply_edge_case(template, col, flag, baseline_df)
            template['_edge_case'] = True
            for key, value in template.items():
                result_df.at[row_index, key] = value

        logger.info(
            f"[EdgeCase] {table_name}: replaced {len(replace_indices)} baseline rows with edge cases; total remains {len(result_df)}"
        )
        return result_df

    def _apply_edge_case(self, record: dict, column: str, flag: str, baseline_df: pd.DataFrame) -> dict:
        """Apply a specific edge case modification to a record."""
        # Interpret common edge case flag patterns
        if "null" in flag or "missing" in flag:
            record[column] = None
        elif "zero" in flag:
            record[column] = 0
        elif "negative" in flag:
            if isinstance(record.get(column), (int, float)):
                record[column] = -abs(record[column]) if record[column] else -1
        elif "max" in flag or "extreme" in flag or "boundary" in flag:
            if column in baseline_df.columns and pd.api.types.is_numeric_dtype(baseline_df[column]):
                col_max = baseline_df[column].max()
                record[column] = col_max * 1.5 if col_max else 999999
        elif "min" in flag:
            if column in baseline_df.columns and pd.api.types.is_numeric_dtype(baseline_df[column]):
                col_min = baseline_df[column].min()
                record[column] = col_min * 0.5 if col_min else 0
        elif "duplicate" in flag:
            # Force a duplicate value from existing data
            if column in baseline_df.columns:
                record[column] = baseline_df[column].mode().iloc[0] if len(baseline_df[column].mode()) > 0 else record[column]
        elif "future" in flag and "date" in column.lower():
            record[column] = "2099-12-31"
        elif "past" in flag and "date" in column.lower():
            record[column] = "1900-01-01"
        elif "empty" in flag:
            record[column] = ""
        elif "special" in flag or "character" in flag:
            if isinstance(record.get(column), str):
                record[column] = record[column] + "!@#$%" if record[column] else "!@#$%"
        else:
            # Generic: set to an outlier value
            if column in baseline_df.columns and pd.api.types.is_numeric_dtype(baseline_df[column]):
                std = baseline_df[column].std()
                mean = baseline_df[column].mean()
                record[column] = mean + 3 * std if std else mean * 2

        return record
