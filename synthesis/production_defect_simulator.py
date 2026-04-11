"""
Production Defect Simulator
===========================

Purpose
-------
The ``EdgeCaseEngine`` already handles "statistical" edge cases (nulls, zero,
min/max, duplicates). Those are useful for model robustness but they are *not*
the edge cases that actually take down production pipelines.

This module captures a different, higher-value class of edge case: rows that
look well-formed to a relaxed synthesiser but that real production code would
reject or mis-handle. Typical examples:

    - email strings containing a comma instead of '@', whitespace, or a
      missing domain
    - phone numbers with embedded letters or wrong country code length
    - numeric amounts past what a downstream DECIMAL(10,2) column accepts
    - date strings in ambiguous locale formats
    - primary keys with trailing whitespace / mixed case
    - foreign keys pointing at rows that do not exist
    - unicode right-to-left / zero-width characters hiding inside text

For each defect we *also* compute **cross-table impact**: if a defective row
sits on a parent table, every child row that references it via a declared
foreign key is recorded so the UI can surface the full blast radius.

The output is purely additive — the base synthetic dataset is untouched. A
defect report is produced so the pipeline, API and frontend can surface the
same information without modifying the core generation pipeline.
"""

from __future__ import annotations

import logging
import random
import re
from dataclasses import dataclass, field
from typing import Any, Iterable

import pandas as pd

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Data containers
# --------------------------------------------------------------------------- #

@dataclass
class DefectRow:
    """A single production-defect row injected for a table."""
    defect_id: str
    table_name: str
    row_index: int            # index of the defective row inside the defect table
    column: str
    defect_type: str          # e.g. "email_missing_at"
    original_value: Any
    defect_value: Any
    prod_failure_reason: str
    severity: str             # "critical" | "high" | "medium"
    example_row: dict         # full defective row (small, JSON-safe)
    impacted_tables: list[dict] = field(default_factory=list)
    # impacted_tables entries: {"table": str, "via_column": str,
    #                           "parent_key": str, "rows": [dict, ...]}

    def to_dict(self) -> dict:
        return {
            "defect_id": self.defect_id,
            "table_name": self.table_name,
            "row_index": self.row_index,
            "column": self.column,
            "defect_type": self.defect_type,
            "original_value": _json_safe(self.original_value),
            "defect_value": _json_safe(self.defect_value),
            "prod_failure_reason": self.prod_failure_reason,
            "severity": self.severity,
            "example_row": {k: _json_safe(v) for k, v in self.example_row.items()},
            "impacted_tables": self.impacted_tables,
        }


@dataclass
class TableDefectReport:
    """Summary of all defects injected into a single table."""
    table_name: str
    defect_rows: list[DefectRow] = field(default_factory=list)
    total_rows_considered: int = 0
    defect_dataframe: pd.DataFrame | None = None  # only the defect rows

    def to_dict(self) -> dict:
        return {
            "table_name": self.table_name,
            "total_rows_considered": self.total_rows_considered,
            "defect_count": len(self.defect_rows),
            "defects": [d.to_dict() for d in self.defect_rows],
        }


# --------------------------------------------------------------------------- #
# Defect recipes
# --------------------------------------------------------------------------- #

# Each recipe returns (defect_value, defect_type, reason, severity) OR None
# if it does not apply to the given value.

def _defect_email(value: Any):
    if not isinstance(value, str) or not value:
        return None
    # classic: comma instead of @
    if "@" in value:
        return (
            value.replace("@", ",", 1),
            "email_missing_at_symbol",
            "Email validator regex requires exactly one '@'; row will be rejected by the notification service.",
            "critical",
        )
    return (
        value + ".localhost",
        "email_missing_domain",
        "Email has no top-level domain; SMTP pre-flight check fails.",
        "high",
    )


def _defect_phone(value: Any):
    if not isinstance(value, str) or not value:
        return None
    return (
        value + "X1",
        "phone_non_numeric",
        "Phone number column expects E.164 digits; SMS gateway throws NumberFormatException.",
        "high",
    )


def _defect_amount(value: Any):
    if not isinstance(value, (int, float)):
        return None
    # overflow DECIMAL(10,2) upper bound
    return (
        99999999.99 * 10,
        "numeric_overflow",
        "Value exceeds DECIMAL(10,2) bounds; billing aggregator raises ArithmeticException.",
        "critical",
    )


def _defect_date(value: Any):
    if not isinstance(value, str) or not value:
        return None
    return (
        "31/02/2099",
        "date_invalid_calendar",
        "Feb 31 does not exist; parser throws ValueError on downstream ETL.",
        "high",
    )


def _defect_identifier(value: Any):
    if value is None:
        return None
    return (
        f" {value} ",
        "identifier_whitespace",
        "Primary key has leading/trailing whitespace; JOINs against trimmed keys miss the row silently.",
        "medium",
    )


def _defect_text_special(value: Any):
    if not isinstance(value, str) or not value:
        return None
    # zero-width-joiner hidden inside a name
    return (
        value + "\u200d' OR '1'='1",
        "text_sql_injection_payload",
        "Unicode + SQL fragment would be rejected by the input sanitiser and trigger WAF alerts.",
        "critical",
    )


def _defect_foreign_key_dangling(value: Any):
    if value is None:
        return None
    return (
        str(value) + "_GHOST",
        "fk_dangling_reference",
        "Foreign key points at a row that does not exist in the parent table; INSERT fails on FK constraint.",
        "critical",
    )


# Column-name heuristics → defect recipes
_RECIPE_MATCHERS: list[tuple[re.Pattern, Any]] = [
    (re.compile(r"email", re.IGNORECASE), _defect_email),
    (re.compile(r"phone|msisdn|mobile", re.IGNORECASE), _defect_phone),
    (re.compile(r"(amount|amt|price|balance|charge|fee|total)", re.IGNORECASE), _defect_amount),
    (re.compile(r"(date|dt|timestamp|time)$", re.IGNORECASE), _defect_date),
    (re.compile(r"(name|description|address|label)", re.IGNORECASE), _defect_text_special),
]


# --------------------------------------------------------------------------- #
# Simulator
# --------------------------------------------------------------------------- #

class ProductionDefectSimulator:
    """
    Scans generated synthetic data for columns whose names / policies match
    production-defect recipes, mutates a small number of rows with those
    defects, and reports the result together with cross-table impact.
    """

    def __init__(self, max_defects_per_table: int = 6, seed: int | None = 42):
        self.max_defects_per_table = max_defects_per_table
        self._rng = random.Random(seed)

    # ------------------------------------------------------------------ #
    def simulate(
        self,
        generated_data: dict[str, pd.DataFrame],
        relationships: Iterable[Any],
        column_policies: dict[str, list] | None = None,
    ) -> dict[str, TableDefectReport]:
        """
        Produce defect reports for every table in ``generated_data``.

        Parameters
        ----------
        generated_data:
            Dict of synthetic DataFrames produced by the pipeline.
        relationships:
            Iterable of RelationshipInfo-like objects (attributes:
            source_table, source_column, target_table, target_column).
        column_policies:
            Optional dict of table_name -> list[ColumnPolicySchema]. Used to
            refine recipe selection (e.g. honour masking_strategy hints).
        """
        reports: dict[str, TableDefectReport] = {}
        relationships = list(relationships or [])

        # Build quick FK lookup: parent_table -> [(child_table, child_col,
        # parent_col)]
        child_index: dict[str, list[tuple[str, str, str]]] = {}
        for rel in relationships:
            parent = getattr(rel, "target_table", None)
            if not parent:
                continue
            child_index.setdefault(parent.upper(), []).append(
                (
                    getattr(rel, "source_table", ""),
                    getattr(rel, "source_column", ""),
                    getattr(rel, "target_column", ""),
                )
            )

        for table_name, df in generated_data.items():
            if df is None or df.empty:
                continue
            report = self._simulate_table(
                table_name=table_name,
                df=df,
                child_index=child_index,
                generated_data=generated_data,
                policies=(column_policies or {}).get(table_name, []),
            )
            reports[table_name] = report

        return reports

    # ------------------------------------------------------------------ #
    def _simulate_table(
        self,
        table_name: str,
        df: pd.DataFrame,
        child_index: dict[str, list[tuple[str, str, str]]],
        generated_data: dict[str, pd.DataFrame],
        policies: list,
    ) -> TableDefectReport:
        report = TableDefectReport(
            table_name=table_name, total_rows_considered=len(df)
        )

        candidate_columns = self._pick_candidate_columns(df, policies)
        if not candidate_columns:
            return report

        # Deterministic row sample to mutate.
        sample_size = min(self.max_defects_per_table, len(df))
        row_indices = self._rng.sample(range(len(df)), k=sample_size)

        defect_rows: list[dict] = []

        for i, row_idx in enumerate(row_indices):
            col = self._rng.choice(candidate_columns)
            recipe = self._lookup_recipe(col, table_name, df)
            if recipe is None:
                continue

            original_value = df.iat[row_idx, df.columns.get_loc(col)]
            result = recipe(original_value)
            if result is None:
                # Fall back to dangling FK if the column is a declared FK
                if self._is_foreign_key(col, table_name, child_index):
                    result = _defect_foreign_key_dangling(original_value)
                if result is None:
                    continue

            defect_value, defect_type, reason, severity = result

            # Build a copy of the row and apply the defect.
            row_dict = df.iloc[row_idx].to_dict()
            row_dict[col] = defect_value

            defect_id = f"{table_name}-DEF-{i+1:03d}"
            defect = DefectRow(
                defect_id=defect_id,
                table_name=table_name,
                row_index=row_idx,
                column=col,
                defect_type=defect_type,
                original_value=original_value,
                defect_value=defect_value,
                prod_failure_reason=reason,
                severity=severity,
                example_row=row_dict,
            )

            # Cross-table impact: if this row's primary identifier is
            # referenced by child tables, capture those rows.
            defect.impacted_tables = self._collect_impact(
                table_name=table_name,
                parent_row=row_dict,
                child_index=child_index,
                generated_data=generated_data,
            )

            report.defect_rows.append(defect)
            defect_rows.append(row_dict)

        if defect_rows:
            report.defect_dataframe = pd.DataFrame(defect_rows)
        return report

    # ------------------------------------------------------------------ #
    def _pick_candidate_columns(
        self, df: pd.DataFrame, policies: list
    ) -> list[str]:
        """Favour columns that matter: identifiers, emails, amounts, names."""
        priority: list[str] = []
        for col in df.columns:
            if col == "_edge_case":
                continue
            if any(p.search(col) for p, _ in _RECIPE_MATCHERS):
                priority.append(col)

        # Always allow the first column (typically a PK) so identifier
        # defects get a chance even on opaque schemas.
        if df.columns[0] not in priority:
            priority.append(df.columns[0])
        return priority

    # ------------------------------------------------------------------ #
    def _lookup_recipe(self, column: str, table_name: str, df: pd.DataFrame):
        for pattern, fn in _RECIPE_MATCHERS:
            if pattern.search(column):
                return fn
        # identifier fallback
        lowered = column.lower()
        if lowered.endswith("_id") or lowered == "id" or "code" in lowered:
            return _defect_identifier
        return None

    # ------------------------------------------------------------------ #
    def _is_foreign_key(
        self,
        column: str,
        table_name: str,
        child_index: dict[str, list[tuple[str, str, str]]],
    ) -> bool:
        table_upper = table_name.upper()
        for entries in child_index.values():
            for child_table, child_col, _ in entries:
                if child_table.upper() == table_upper and child_col.upper() == column.upper():
                    return True
        return False

    # ------------------------------------------------------------------ #
    def _collect_impact(
        self,
        table_name: str,
        parent_row: dict,
        child_index: dict[str, list[tuple[str, str, str]]],
        generated_data: dict[str, pd.DataFrame],
        max_rows_per_table: int = 5,
    ) -> list[dict]:
        """For a defective parent row, list child rows that reference it."""
        impacts: list[dict] = []
        entries = child_index.get(table_name.upper(), [])
        for child_table, child_col, parent_col in entries:
            child_df = generated_data.get(child_table)
            if child_df is None or child_df.empty:
                continue
            if child_col not in child_df.columns:
                continue
            if parent_col not in parent_row:
                continue

            parent_value = parent_row[parent_col]
            try:
                matches = child_df[child_df[child_col] == parent_value]
            except Exception:
                continue
            if matches.empty:
                continue

            sliced = matches.head(max_rows_per_table)
            impacts.append(
                {
                    "table": child_table,
                    "via_column": child_col,
                    "parent_key": parent_col,
                    "parent_value": _json_safe(parent_value),
                    "row_count": int(len(matches)),
                    "rows": [
                        {k: _json_safe(v) for k, v in r.items()}
                        for r in sliced.to_dict(orient="records")
                    ],
                }
            )
        return impacts


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _json_safe(value: Any) -> Any:
    """Coerce pandas/numpy scalars into JSON-serialisable primitives."""
    if value is None:
        return None
    if isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if pd.isna(value):
            return None
        return value
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def reports_to_api_payload(
    reports: dict[str, TableDefectReport],
    run_id: str | None = None,
    source_name: str | None = None,
) -> dict:
    """Flatten reports into a JSON payload suitable for /api/edge-cases."""
    tables_payload = []
    total_defects = 0
    for name, report in reports.items():
        rd = report.to_dict()
        total_defects += rd["defect_count"]
        tables_payload.append(rd)

    # Sort most defective tables first so the UI surfaces them immediately.
    tables_payload.sort(key=lambda entry: entry["defect_count"], reverse=True)
    return {
        "run_id": run_id,
        "source_name": source_name,
        "total_defects": total_defects,
        "tables": tables_payload,
    }
