"""
Junction Table Generator (Step 3.4)
Generates many-to-many junction table records procedurally after parent tables
have been generated, preserving real multiplicity distributions.
"""

import logging
import random
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class JunctionHandler:
    def __init__(self):
        pass

    def is_junction_table(self, table_name: str, relationships: list, column_count: int) -> bool:
        """
        Identify junction tables: tables with exactly 2 FK columns and minimal own attributes.
        """
        fk_count = sum(1 for r in relationships if r.source_table.upper() == table_name.upper())
        own_cols = column_count - fk_count
        return fk_count == 2 and own_cols <= 5

    def analyze_multiplicity(self, real_junction_df: pd.DataFrame, fk_columns: list[str]) -> dict:
        """
        Analyze real junction data to compute distribution of relationship multiplicity.
        For each parent FK, how many child records per parent key?
        """
        distributions = {}
        for fk_col in fk_columns:
            if fk_col not in real_junction_df.columns:
                continue
            counts = real_junction_df.groupby(fk_col).size()
            distributions[fk_col] = {
                "mean": float(counts.mean()),
                "std": float(counts.std()) if len(counts) > 1 else 0,
                "min": int(counts.min()),
                "max": int(counts.max()),
                "values": counts.values.tolist()
            }
        return distributions

    def generate_junction(
        self,
        table_name: str,
        fk_columns: list[str],
        parent_keys: dict,
        real_junction_df: pd.DataFrame,
        other_columns: list[str] = None,
        source_df: pd.DataFrame = None
    ) -> pd.DataFrame:
        """
        Generate junction records procedurally.

        Args:
            table_name: Junction table name
            fk_columns: List of 2 FK column names
            parent_keys: {fk_col: list_of_generated_parent_key_values}
            real_junction_df: Real junction data for multiplicity distribution
            other_columns: Non-FK columns that need values
            source_df: Real source data for sampling other column values

        Returns:
            Synthetic junction DataFrame
        """
        logger.info(f"[Junction] Generating junction records for {table_name}")

        if len(fk_columns) < 2:
            logger.warning(f"Expected 2 FK columns for junction table {table_name}, got {len(fk_columns)}")
            return pd.DataFrame()

        primary_fk = fk_columns[0]
        secondary_fk = fk_columns[1]

        primary_keys = parent_keys.get(primary_fk, [])
        secondary_keys = parent_keys.get(secondary_fk, [])

        if not primary_keys or not secondary_keys:
            logger.warning(f"Missing parent keys for junction {table_name}")
            return pd.DataFrame()

        # Analyze real multiplicity
        distributions = self.analyze_multiplicity(real_junction_df, fk_columns)

        # Generate records
        records = []
        primary_dist = distributions.get(primary_fk, {"mean": 2, "std": 1, "min": 1, "max": 5})

        for pk_val in primary_keys:
            # Sample multiplicity from real distribution
            count = max(1, int(random.gauss(primary_dist["mean"], max(primary_dist["std"], 0.5))))
            count = min(count, primary_dist["max"], len(secondary_keys))

            sampled_secondary = random.sample(secondary_keys, min(count, len(secondary_keys)))

            for sk_val in sampled_secondary:
                record = {primary_fk: pk_val, secondary_fk: sk_val}

                # Fill other columns by sampling from real data
                if other_columns and source_df is not None:
                    for oc in other_columns:
                        if oc in source_df.columns:
                            record[oc] = source_df[oc].dropna().sample(1).values[0] if len(source_df[oc].dropna()) > 0 else None
                        else:
                            record[oc] = None

                records.append(record)

        result = pd.DataFrame(records)
        logger.info(f"[Junction] Generated {len(result)} records for {table_name}")
        return result
