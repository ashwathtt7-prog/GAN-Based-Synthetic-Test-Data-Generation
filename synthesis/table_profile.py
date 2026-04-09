"""
Shared table profiling for all generation tiers.

Splits columns into:
- structural: IDs, FKs, dates, enums, flags/status columns that should be generated
  deterministically and repaired consistently across every tier
- modeled: business columns that benefit from CTGAN / TVAE / rule-based distribution modeling
- sensitive: columns that require masking-aware handling
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

import pandas as pd


@dataclass
class TableGenerationProfile:
    table_name: str
    structural_columns: list[str]
    modeled_columns: list[str]
    sensitive_columns: list[str]
    fingerprint: str
    source_columns: list[str]
    row_count: int


def build_generation_profile(
    table_name: str,
    source_df: pd.DataFrame,
    masked_df: pd.DataFrame,
    policies: list,
    relationships: list,
) -> TableGenerationProfile:
    """Classify columns into structural/modeled/sensitive roles for shared generation."""
    policy_map = {p.column_name: p for p in policies if hasattr(p, "column_name")}
    fk_columns = {
        rel.source_column
        for rel in relationships
        if rel.source_table.upper() == table_name.upper()
    }
    pk_like_columns = {
        rel.target_column
        for rel in relationships
        if rel.target_table.upper() == table_name.upper()
    }

    structural_columns = []
    modeled_columns = []
    sensitive_columns = []

    for column in masked_df.columns:
        policy = policy_map.get(column)
        upper = column.upper()
        source_series = source_df[column] if column in source_df.columns else masked_df[column]

        if policy and getattr(policy, "masking_strategy", "passthrough") != "passthrough":
            sensitive_columns.append(column)

        if _is_structural_column(column, upper, source_series, policy, fk_columns, pk_like_columns):
            structural_columns.append(column)
        else:
            modeled_columns.append(column)

    fingerprint_payload = {
        "table_name": table_name,
        "structural_columns": structural_columns,
        "modeled_columns": modeled_columns,
        "sensitive_columns": sensitive_columns,
        "dtypes": {col: str(dtype) for col, dtype in masked_df.dtypes.items()},
        "policies": {
            col: {
                "masking_strategy": getattr(policy_map.get(col), "masking_strategy", None),
                "business_importance": getattr(policy_map.get(col), "business_importance", None),
                "dedup_mode": getattr(policy_map.get(col), "dedup_mode", None),
            }
            for col in masked_df.columns
        },
    }
    fingerprint = hashlib.sha256(
        json.dumps(fingerprint_payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()

    return TableGenerationProfile(
        table_name=table_name,
        structural_columns=structural_columns,
        modeled_columns=modeled_columns,
        sensitive_columns=sensitive_columns,
        fingerprint=fingerprint,
        source_columns=list(source_df.columns),
        row_count=len(source_df),
    )


def _is_structural_column(
    column: str,
    upper: str,
    source_series: pd.Series,
    policy,
    fk_columns: set[str],
    pk_like_columns: set[str],
) -> bool:
    non_null = source_series.dropna()
    unique_ratio = non_null.nunique(dropna=True) / max(len(non_null), 1) if len(non_null) else 0.0
    masking_strategy = getattr(policy, "masking_strategy", "passthrough") if policy else "passthrough"
    dedup_mode = getattr(policy, "dedup_mode", None)
    is_numeric = pd.api.types.is_numeric_dtype(source_series)
    is_boolish = _is_boolean_like(non_null)
    is_datetime = pd.api.types.is_datetime64_any_dtype(source_series) or any(
        token in upper for token in ("DATE", "_DT", "_TS", "TIME")
    )
    is_reference = dedup_mode == "reference"
    is_status_like = any(token in upper for token in ("_STAT_", "_STAT", "_FLG", "_CD", "_TYPE"))
    is_id_like = upper.endswith("_ID") or column in fk_columns or column in pk_like_columns
    is_unique_identifier = unique_ratio >= 0.95 and any(token in upper for token in ("ID", "KEY", "UUID"))
    low_cardinality = non_null.nunique(dropna=True) <= max(12, int(len(source_series) * 0.02))
    high_cardinality_text = (
        not is_numeric
        and not is_datetime
        and unique_ratio >= 0.35
        and non_null.nunique(dropna=True) >= 50
    )
    metric_like = is_numeric or any(token in upper for token in ("AMT", "QTY", "DUR", "PCT", "SCR", "CNT", "KB", "VOL", "BAL", "RATE"))
    is_masked_identifier = masking_strategy != "passthrough" and any([
        dedup_mode == "entity",
        is_datetime,
        is_id_like,
        is_unique_identifier,
        high_cardinality_text,
    ]) and not metric_like

    return any([
        is_masked_identifier,
        is_boolish,
        is_datetime,
        is_reference,
        is_status_like,
        is_id_like,
        is_unique_identifier,
        low_cardinality,
    ])


def _is_boolean_like(series: pd.Series) -> bool:
    if len(series) == 0:
        return False

    values = {str(value).upper() for value in series.dropna().unique()}
    return values.issubset({"Y", "N", "TRUE", "FALSE", "0", "1"})
