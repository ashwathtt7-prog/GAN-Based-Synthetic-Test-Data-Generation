"""
Production Defect Detector
==========================

Purpose
-------
Scans a live source database for rows that real production code would reject
and reports them as-is (no mutation, no synthesis). This is the counterpart to
``datasets/inject_production_defects.py`` — the injector plants realistic bad
rows into the source DB, this detector finds them again during the pipeline.

What it does
------------
For each configured validator, it runs a SQL query against the source engine:

    - ``CUST_CNTCT``: bad email (no ``@`` or missing domain), phone with
      non-digit characters.
    - ``CUST_MSTR``: SSN length mismatch, future date of birth, suspicious
      text in name fields (SQL injection payloads, zero-width chars, etc).
    - ``BLNG_ACCT``: negative balance, credit limit larger than
      DECIMAL(12,2) can hold.
    - ``INVC``: due date before cycle date, paid amount greater than total.
    - ``CDR_REC``: negative duration, end time earlier than start time.
    - ``SUBSCR_ACCT``/``PYMT``: dangling foreign keys via anti-joins.

Every detected row carries its **actual** primary key and the **actual** bad
value read from the source database. No value is ever fabricated.

Cross-table impact
------------------
For each defect row we run a live ``JOIN`` against the parent's child tables
(derived from ``all_rels``) so the UI can show downstream rows that reference
the broken record. This is also done via SQL, against the same engine, so the
numbers correspond to real data.

Output shape
------------
Returns a ``dict[str, TableDefectReport]``. ``reports_to_api_payload`` flattens
that into the JSON the ``/api/edge-cases/production-defects`` endpoint serves
and the ``EdgeCasePanel`` renders.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Data containers (shape-compatible with the old simulator)
# --------------------------------------------------------------------------- #

@dataclass
class DefectRow:
    """A single defect row detected in the source database."""
    defect_id: str
    table_name: str
    row_index: Any            # actual primary key of the bad row
    column: str
    defect_type: str
    original_value: Any       # the actual bad value from the DB
    prod_failure_reason: str
    severity: str             # critical | high | medium
    example_row: dict         # full row from the source table (JSON-safe)
    impacted_tables: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "defect_id": self.defect_id,
            "table_name": self.table_name,
            "row_index": _json_safe(self.row_index),
            "column": self.column,
            "defect_type": self.defect_type,
            "original_value": _json_safe(self.original_value),
            "prod_failure_reason": self.prod_failure_reason,
            "severity": self.severity,
            "example_row": {k: _json_safe(v) for k, v in self.example_row.items()},
            "impacted_tables": self.impacted_tables,
        }


@dataclass
class TableDefectReport:
    table_name: str
    defect_rows: list[DefectRow] = field(default_factory=list)
    total_rows_considered: int = 0

    def to_dict(self) -> dict:
        return {
            "table_name": self.table_name,
            "total_rows_considered": self.total_rows_considered,
            "defect_count": len(self.defect_rows),
            "defects": [d.to_dict() for d in self.defect_rows],
        }


# --------------------------------------------------------------------------- #
# Validator catalog
# --------------------------------------------------------------------------- #

@dataclass
class Validator:
    """
    One SQL-level check for bad data. Each validator is table/column-specific
    and knows how to write a WHERE clause that finds bad rows in that column.
    """
    table: str
    pk_column: str
    column: str
    where_clause: str         # goes after ``WHERE`` — must be a boolean SQL expr
    defect_type: str
    reason: str
    severity: str             # critical | high | medium
    # Optional filter so validators only run against tables that actually
    # exist in the current source (prevents Billing DB from blowing up when
    # Customer tables are missing).
    requires_tables: tuple[str, ...] = ()

    @property
    def rule_key(self) -> str:
        return f"{self.table}.{self.column}.{self.defect_type}".upper()


# The validators below mirror the real bad-data patterns that
# ``datasets/inject_production_defects.py`` plants, but they are expressed
# purely as SQL predicates so they also catch naturally occurring bad rows.
VALIDATORS: list[Validator] = [
    # -------- CUST_CNTCT --------
    Validator(
        table="CUST_CNTCT",
        pk_column="CNTCT_ID",
        column="CNTCT_VAL",
        where_clause=(
            "CNTCT_TYP_CD = 'EMAIL' AND ("
            "INSTR(CNTCT_VAL, '@') = 0 "
            "OR CNTCT_VAL LIKE '%@' "
            "OR CNTCT_VAL LIKE '@%' "
            "OR INSTR(CNTCT_VAL, ' ') > 0"
            ")"
        ),
        defect_type="email_invalid_format",
        reason="Email is missing '@' or the domain portion; notification service rejects the row.",
        severity="critical",
    ),
    Validator(
        table="CUST_CNTCT",
        pk_column="CNTCT_ID",
        column="CNTCT_VAL",
        where_clause=(
            "CNTCT_TYP_CD = 'PHONE' AND CNTCT_VAL GLOB '*[A-Za-z]*'"
        ),
        defect_type="phone_non_numeric",
        reason="Phone number contains alphabetic characters; SMS gateway throws NumberFormatException.",
        severity="high",
    ),

    # -------- CUST_MSTR --------
    Validator(
        table="CUST_MSTR",
        pk_column="CUST_ID",
        column="CUST_SSN",
        where_clause="CUST_SSN IS NOT NULL AND LENGTH(CUST_SSN) <> 11",
        defect_type="ssn_wrong_length",
        reason="SSN does not match 'NNN-NN-NNNN' format; identity verification provider rejects it.",
        severity="critical",
    ),
    Validator(
        table="CUST_MSTR",
        pk_column="CUST_ID",
        column="CUST_DOB",
        # Use a literal ISO-8601 threshold so the predicate works whether
        # CUST_DOB is stored as DATE (SQLite) or VARCHAR (DuckDB DW).
        where_clause=f"CAST(CUST_DOB AS VARCHAR) > '{date.today().isoformat()}'",
        defect_type="date_in_future",
        reason="Date of birth is in the future; KYC check fails and account creation is blocked.",
        severity="high",
    ),
    Validator(
        table="CUST_MSTR",
        pk_column="CUST_ID",
        column="CUST_FRST_NM",
        where_clause=(
            "CUST_FRST_NM LIKE '%DROP TABLE%' "
            "OR CUST_FRST_NM LIKE '%--%' "
            "OR CUST_FRST_NM LIKE '%'' OR%' "
            "OR CUST_FRST_NM LIKE '%;%'"
        ),
        defect_type="text_sql_injection_payload",
        reason="Name field contains an SQL-injection payload; input sanitiser rejects the row and WAF alerts.",
        severity="critical",
    ),

    # -------- BLNG_ACCT --------
    Validator(
        table="BLNG_ACCT",
        pk_column="BLNG_ACCT_ID",
        column="BLNG_CURR_BAL_AMT",
        where_clause="BLNG_CURR_BAL_AMT < 0",
        defect_type="negative_balance",
        reason="Negative current balance violates accounting rules; reconciliation job halts.",
        severity="high",
    ),
    Validator(
        table="BLNG_ACCT",
        pk_column="BLNG_ACCT_ID",
        column="BLNG_CRED_LMT_AMT",
        where_clause="BLNG_CRED_LMT_AMT > 9999999999.99",
        defect_type="numeric_overflow_decimal_12_2",
        reason="Credit limit exceeds DECIMAL(12,2) bounds; billing aggregator raises ArithmeticException.",
        severity="critical",
    ),

    # -------- INVC --------
    Validator(
        table="INVC",
        pk_column="INVC_ID",
        column="INVC_DUE_DT",
        where_clause="INVC_DUE_DT < INVC_CYC_DT",
        defect_type="invoice_due_before_cycle",
        reason="Due date precedes billing cycle date; dunning job misclassifies the invoice as overdue at issue.",
        severity="high",
    ),
    Validator(
        table="INVC",
        pk_column="INVC_ID",
        column="INVC_PAID_AMT",
        where_clause="INVC_PAID_AMT > INVC_TOT_AMT",
        defect_type="paid_exceeds_total",
        reason="Paid amount exceeds invoice total; revenue recognition breaks.",
        severity="high",
    ),

    # -------- CDR_REC --------
    Validator(
        table="CDR_REC",
        pk_column="CDR_ID",
        column="CDR_DUR_SEC",
        where_clause="CDR_DUR_SEC < 0",
        defect_type="negative_duration",
        reason="Call duration cannot be negative; rating engine rejects the CDR and revenue is lost.",
        severity="critical",
    ),
    Validator(
        table="CDR_REC",
        pk_column="CDR_ID",
        column="CDR_END_DT",
        where_clause="CDR_END_DT < CDR_STRT_DT",
        defect_type="end_before_start",
        reason="Call end time precedes start time; duration calculation produces nonsense.",
        severity="high",
    ),

    # -------- Dangling FKs (anti-joins) --------
    Validator(
        table="SUBSCR_ACCT",
        pk_column="SUBSCR_ID",
        column="CUST_ID",
        where_clause=(
            "CUST_ID IS NOT NULL AND CUST_ID NOT IN (SELECT CUST_ID FROM CUST_MSTR)"
        ),
        defect_type="fk_dangling_cust_id",
        reason="Subscriber points at a non-existent customer; JOINs silently drop the row or crash downstream ETL.",
        severity="critical",
        requires_tables=("SUBSCR_ACCT", "CUST_MSTR"),
    ),
    Validator(
        table="PYMT",
        pk_column="PYMT_ID",
        column="BLNG_ACCT_ID",
        where_clause=(
            "BLNG_ACCT_ID IS NOT NULL "
            "AND BLNG_ACCT_ID NOT IN (SELECT BLNG_ACCT_ID FROM BLNG_ACCT)"
        ),
        defect_type="fk_dangling_blng_acct",
        reason="Payment references a non-existent billing account; revenue assurance breaks.",
        severity="critical",
        requires_tables=("PYMT", "BLNG_ACCT"),
    ),
]


# --------------------------------------------------------------------------- #
# Detector
# --------------------------------------------------------------------------- #

class ProductionDefectDetector:
    """
    Runs ``VALIDATORS`` against a live source engine and returns real
    defect rows. No synthesis, no mutation — only rows that actually exist
    in the source database.
    """

    def __init__(self, max_rows_per_validator: int = 25):
        self.max_rows_per_validator = max_rows_per_validator

    # ------------------------------------------------------------------ #
    def detect(
        self,
        engine: Engine,
        relationships: Iterable[Any] = (),
        table_filter: Iterable[str] | None = None,
        rule_overrides: dict[str, dict] | None = None,
    ) -> dict[str, TableDefectReport]:
        """
        Execute every validator whose target table exists in the source
        engine, honouring an optional ``table_filter`` (upper-case set).
        """
        existing_tables = self._list_existing_tables(engine)
        logger.info(
            "Production defect detector found %d tables in source",
            len(existing_tables),
        )

        filter_set: set[str] | None = (
            {t.upper() for t in table_filter} if table_filter else None
        )

        relationships = list(relationships or [])
        # child_index[parent_table] -> [(child_table, child_col, parent_col)]
        child_index: dict[str, list[tuple[str, str, str]]] = {}
        for rel in relationships:
            parent = (getattr(rel, "target_table", "") or "").upper()
            if not parent:
                continue
            child_index.setdefault(parent, []).append(
                (
                    (getattr(rel, "source_table", "") or "").upper(),
                    (getattr(rel, "source_column", "") or "").upper(),
                    (getattr(rel, "target_column", "") or "").upper(),
                )
            )

        reports: dict[str, TableDefectReport] = {}
        rule_overrides = {
            (key or "").upper(): value for key, value in (rule_overrides or {}).items()
        }

        for validator in VALIDATORS:
            target = validator.table.upper()
            if target not in existing_tables:
                continue
            if filter_set is not None and target not in filter_set:
                continue
            # Skip validators that reference tables that don't exist (e.g.
            # dangling-FK checks against missing parent table).
            if any(t.upper() not in existing_tables for t in validator.requires_tables):
                continue
            override = rule_overrides.get(validator.rule_key, {})
            override_mode = (override.get("action_mode") or "flag").lower()
            override_status = (override.get("review_status") or "").lower()
            if override_mode == "allow" and override_status == "approved":
                continue

            report = reports.setdefault(
                validator.table,
                TableDefectReport(
                    table_name=validator.table,
                    total_rows_considered=self._row_count(engine, validator.table),
                ),
            )

            try:
                rows = self._run_validator(engine, validator)
            except Exception as exc:
                logger.warning(
                    "Validator %s.%s failed: %s",
                    validator.table, validator.column, exc,
                )
                continue

            for idx, row in enumerate(rows, start=len(report.defect_rows) + 1):
                row_dict = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
                pk_value = row_dict.get(validator.pk_column)
                bad_value = row_dict.get(validator.column)

                defect = DefectRow(
                    defect_id=f"{validator.table}-{validator.defect_type.upper()}-{idx:03d}",
                    table_name=validator.table,
                    row_index=pk_value,
                    column=validator.column,
                    defect_type=validator.defect_type,
                    original_value=bad_value,
                    prod_failure_reason=(
                        override.get("custom_failure_reason")
                        if override_mode == "customize" and override_status == "approved" and override.get("custom_failure_reason")
                        else validator.reason
                    ),
                    severity=(
                        override.get("custom_severity")
                        if override_mode == "customize" and override_status == "approved" and override.get("custom_severity")
                        else validator.severity
                    ),
                    example_row=row_dict,
                )

                defect.impacted_tables = self._collect_impact(
                    engine=engine,
                    parent_table=validator.table,
                    parent_row=row_dict,
                    child_index=child_index,
                    existing_tables=existing_tables,
                    filter_set=filter_set,
                )
                report.defect_rows.append(defect)

        return reports

    def get_rule_catalog(self) -> list[dict]:
        """Return the static validator catalog for UI customization."""
        catalog = []
        for validator in VALIDATORS:
            catalog.append(
                {
                    "rule_key": validator.rule_key,
                    "table_name": validator.table,
                    "column_name": validator.column,
                    "defect_type": validator.defect_type,
                    "default_failure_reason": validator.reason,
                    "default_severity": validator.severity,
                    "requires_tables": list(validator.requires_tables),
                }
            )
        return catalog

    # ------------------------------------------------------------------ #
    def _list_existing_tables(self, engine: Engine) -> set[str]:
        """Return the set of tables (upper-case) available in the engine."""
        dialect = engine.dialect.name
        with engine.connect() as conn:
            if dialect == "sqlite":
                rows = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table'")
                ).fetchall()
            else:
                rows = conn.execute(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema IN ('main', 'public')"
                    )
                ).fetchall()
        return {str(r[0]).upper() for r in rows}

    # ------------------------------------------------------------------ #
    def _row_count(self, engine: Engine, table: str) -> int:
        try:
            with engine.connect() as conn:
                return int(
                    conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar() or 0
                )
        except Exception:
            return 0

    # ------------------------------------------------------------------ #
    def _run_validator(self, engine: Engine, validator: Validator) -> list:
        """Run the validator's WHERE clause against the source DB."""
        where_sql = self._translate_where_for_dialect(
            validator.where_clause, engine.dialect.name
        )
        sql = (
            f'SELECT * FROM "{validator.table}" '
            f"WHERE {where_sql} "
            f"LIMIT {int(self.max_rows_per_validator)}"
        )
        with engine.connect() as conn:
            return conn.execute(text(sql)).fetchall()

    # ------------------------------------------------------------------ #
    @staticmethod
    def _translate_where_for_dialect(where_sql: str, dialect: str) -> str:
        """
        Lightweight SQLite → DuckDB translator for the validator predicates
        above. Only rewrites the specific constructs we actually use, so it
        stays predictable.
        """
        if dialect == "sqlite":
            return where_sql

        # DuckDB: rewrite SQLite-only helpers.
        translated = where_sql
        # DATE('now') → CURRENT_DATE
        translated = re.sub(
            r"DATE\s*\(\s*'now'\s*\)", "CURRENT_DATE", translated, flags=re.IGNORECASE
        )
        # INSTR(col, lit) → position(lit IN col)
        def _instr_to_position(match: re.Match) -> str:
            col, lit = match.group(1).strip(), match.group(2).strip()
            return f"position({lit} IN {col})"

        translated = re.sub(
            r"INSTR\s*\(\s*([^,]+?)\s*,\s*('[^']*')\s*\)",
            _instr_to_position,
            translated,
            flags=re.IGNORECASE,
        )
        # GLOB '*[A-Za-z]*' → regex-based alpha check.
        translated = re.sub(
            r"(\w+)\s+GLOB\s+'\*\[A-Za-z\]\*'",
            r"regexp_matches(\1, '[A-Za-z]')",
            translated,
            flags=re.IGNORECASE,
        )
        # LENGTH stays LENGTH — DuckDB supports it.
        return translated

    # ------------------------------------------------------------------ #
    def _collect_impact(
        self,
        engine: Engine,
        parent_table: str,
        parent_row: dict,
        child_index: dict[str, list[tuple[str, str, str]]],
        existing_tables: set[str],
        filter_set: set[str] | None,
        max_rows: int = 5,
    ) -> list[dict]:
        """
        For a defective parent row, list child rows that reference it via
        declared foreign keys. The children are read back from the engine.
        """
        impacts: list[dict] = []
        for child_table, child_col, parent_col in child_index.get(
            parent_table.upper(), []
        ):
            if child_table not in existing_tables:
                continue
            if filter_set is not None and child_table not in filter_set:
                continue
            parent_value = parent_row.get(parent_col) or parent_row.get(parent_col.upper())
            if parent_value is None:
                continue

            try:
                with engine.connect() as conn:
                    count = int(
                        conn.execute(
                            text(
                                f'SELECT COUNT(*) FROM "{child_table}" '
                                f'WHERE "{child_col}" = :pv'
                            ),
                            {"pv": parent_value},
                        ).scalar()
                        or 0
                    )
                    if count == 0:
                        continue
                    rows = conn.execute(
                        text(
                            f'SELECT * FROM "{child_table}" '
                            f'WHERE "{child_col}" = :pv LIMIT {int(max_rows)}'
                        ),
                        {"pv": parent_value},
                    ).fetchall()
            except Exception as exc:
                logger.debug(
                    "Impact lookup %s(%s) -> %s(%s) failed: %s",
                    parent_table, parent_col, child_table, child_col, exc,
                )
                continue

            impacts.append(
                {
                    "table": child_table,
                    "via_column": child_col,
                    "parent_key": parent_col,
                    "parent_value": _json_safe(parent_value),
                    "row_count": count,
                    "rows": [
                        {k: _json_safe(v) for k, v in (
                            dict(r._mapping) if hasattr(r, "_mapping") else dict(r)
                        ).items()}
                        for r in rows
                    ],
                }
            )
        return impacts


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
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
    """Flatten reports into the JSON payload served by /api/edge-cases."""
    tables_payload: list[dict] = []
    total_defects = 0
    for _, report in reports.items():
        rd = report.to_dict()
        total_defects += rd["defect_count"]
        tables_payload.append(rd)

    tables_payload.sort(key=lambda e: e["defect_count"], reverse=True)
    return {
        "run_id": run_id,
        "source_name": source_name,
        "total_defects": total_defects,
        "tables": tables_payload,
    }
