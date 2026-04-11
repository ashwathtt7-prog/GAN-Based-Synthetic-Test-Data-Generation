"""
Inject realistic production defects into the telecom source databases.

Purpose
-------
The production-defect *detector* (synthesis/production_defect_detector.py)
scans real source rows for bad data. For it to find anything meaningful in a
freshly seeded SQLite/DuckDB file, we need a small number of deliberately-bad
rows planted alongside the otherwise-clean data.

What this script does
---------------------
For each configured telecom source, it applies a curated set of UPDATE
statements that corrupt specific rows in specific columns. The corruptions are
realistic patterns a detector would find in production:

    - CUST_CNTCT.CNTCT_VAL        → email with ',' instead of '@'
    - CUST_CNTCT.CNTCT_VAL        → phone with letters
    - CUST_MSTR.CUST_FRST_NM      → unicode/SQL-injection payload
    - CUST_MSTR.CUST_SSN          → wrong length (would fail SSN regex)
    - CUST_MSTR.CUST_DOB          → date in the future
    - BLNG_ACCT.BLNG_CURR_BAL_AMT → negative balance (violates business rule)
    - BLNG_ACCT.BLNG_CRED_LMT_AMT → overflow vs DECIMAL(12,2)
    - INVC.INVC_TOT_AMT           → INVC_DUE_DT earlier than INVC_CYC_DT
    - INVC.INVC_PAID_AMT          → paid_amt > tot_amt (impossible)
    - CDR_REC.CDR_DUR_SEC         → negative duration
    - CDR_REC.CDR_END_DT          → end earlier than start
    - SUBSCR_ACCT.CUST_ID         → dangling FK (orphaned subscriber)
    - PYMT.BLNG_ACCT_ID           → dangling FK (orphaned payment)

Each corrupted row is recorded in a sibling table ``_defect_ledger`` so
downstream code can verify detection precision (optional).

Usage
-----
    python datasets/inject_production_defects.py
    python datasets/inject_production_defects.py --targets telecom_sqlite,telecom_duckdb_dw
    python datasets/inject_production_defects.py --dry-run    # show planned edits

The script is *idempotent* — it looks at the lowest row IDs it can find and
overwrites them. Running it twice is safe; the rows just get written again.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Allow running from the repo root: ``python datasets/inject_production_defects.py``
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.config import load_config, get_data_sources  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("inject_defects")


# --------------------------------------------------------------------------- #
# Defect plan
# --------------------------------------------------------------------------- #

@dataclass
class DefectPlan:
    """A single row-level corruption to apply."""
    table: str
    pk_column: str
    column: str
    pk_value_sql: str           # SQL expression returning the row to touch
    bad_value_sql: str          # SQL literal of the bad value
    defect_type: str
    reason: str
    severity: str               # critical | high | medium

    def update_sql(self) -> str:
        return (
            f'UPDATE "{self.table}" SET "{self.column}" = {self.bad_value_sql} '
            f'WHERE "{self.pk_column}" = ({self.pk_value_sql})'
        )


# NOTE: All PK sub-selects use ``LIMIT 1`` so the script is deterministic and
# only corrupts a single row per plan. Row choice deliberately picks the
# smallest matching ID so repeated runs hit the same rows.
DEFECT_PLANS: list[DefectPlan] = [
    # -------- CUST_CNTCT: bad emails and phones --------
    DefectPlan(
        table="CUST_CNTCT",
        pk_column="CNTCT_ID",
        column="CNTCT_VAL",
        pk_value_sql=(
            "SELECT MIN(CNTCT_ID) FROM CUST_CNTCT "
            "WHERE CNTCT_TYP_CD = 'EMAIL'"
        ),
        bad_value_sql="'alice,example.com'",
        defect_type="email_missing_at_symbol",
        reason="Email validator regex requires exactly one '@'; notification service rejects the row.",
        severity="critical",
    ),
    DefectPlan(
        table="CUST_CNTCT",
        pk_column="CNTCT_ID",
        column="CNTCT_VAL",
        pk_value_sql=(
            "SELECT MIN(CNTCT_ID) FROM CUST_CNTCT "
            "WHERE CNTCT_TYP_CD = 'EMAIL' AND CNTCT_ID NOT IN "
            "(SELECT MIN(CNTCT_ID) FROM CUST_CNTCT WHERE CNTCT_TYP_CD = 'EMAIL')"
        ),
        bad_value_sql="'bob@'",
        defect_type="email_missing_domain",
        reason="Email has no domain after '@'; SMTP pre-flight check fails.",
        severity="high",
    ),
    DefectPlan(
        table="CUST_CNTCT",
        pk_column="CNTCT_ID",
        column="CNTCT_VAL",
        pk_value_sql=(
            "SELECT MIN(CNTCT_ID) FROM CUST_CNTCT "
            "WHERE CNTCT_TYP_CD = 'PHONE'"
        ),
        bad_value_sql="'555-HELLO-99'",
        defect_type="phone_non_numeric",
        reason="Phone column expects digits only; SMS gateway throws NumberFormatException.",
        severity="high",
    ),

    # -------- CUST_MSTR: name/ssn/dob defects --------
    DefectPlan(
        table="CUST_MSTR",
        pk_column="CUST_ID",
        column="CUST_FRST_NM",
        pk_value_sql="SELECT MIN(CUST_ID) FROM CUST_MSTR",
        bad_value_sql="'Robert''); DROP TABLE CUST_MSTR;--'",
        defect_type="text_sql_injection_payload",
        reason="Name field contains an SQL-injection payload; input sanitiser rejects the row and WAF alerts.",
        severity="critical",
    ),
    DefectPlan(
        table="CUST_MSTR",
        pk_column="CUST_ID",
        column="CUST_SSN",
        pk_value_sql="SELECT MIN(CUST_ID) + 1 FROM CUST_MSTR",
        bad_value_sql="'12-34'",
        defect_type="ssn_wrong_length",
        reason="SSN must be 9 digits; identity verification provider rejects format.",
        severity="critical",
    ),
    DefectPlan(
        table="CUST_MSTR",
        pk_column="CUST_ID",
        column="CUST_DOB",
        pk_value_sql="SELECT MIN(CUST_ID) + 2 FROM CUST_MSTR",
        bad_value_sql="'2099-05-14'",
        defect_type="date_in_future",
        reason="Date of birth is in the future; KYC check fails and account creation is blocked.",
        severity="high",
    ),

    # -------- BLNG_ACCT: financial defects --------
    DefectPlan(
        table="BLNG_ACCT",
        pk_column="BLNG_ACCT_ID",
        column="BLNG_CURR_BAL_AMT",
        pk_value_sql="SELECT MIN(BLNG_ACCT_ID) FROM BLNG_ACCT",
        bad_value_sql="-450.00",
        defect_type="negative_balance",
        reason="Negative current balance violates accounting rules; reconciliation job halts.",
        severity="high",
    ),
    DefectPlan(
        table="BLNG_ACCT",
        pk_column="BLNG_ACCT_ID",
        column="BLNG_CRED_LMT_AMT",
        pk_value_sql="SELECT MIN(BLNG_ACCT_ID) + 1 FROM BLNG_ACCT",
        bad_value_sql="99999999999.99",
        defect_type="numeric_overflow_decimal_12_2",
        reason="Credit limit exceeds DECIMAL(12,2) bounds; billing aggregator raises ArithmeticException.",
        severity="critical",
    ),

    # -------- INVC: referential business rules --------
    DefectPlan(
        table="INVC",
        pk_column="INVC_ID",
        column="INVC_DUE_DT",
        pk_value_sql="SELECT MIN(INVC_ID) FROM INVC",
        bad_value_sql="'1999-01-01'",
        defect_type="invoice_due_before_cycle",
        reason="Due date precedes billing cycle date; dunning job misclassifies this as overdue at issue.",
        severity="high",
    ),
    DefectPlan(
        table="INVC",
        pk_column="INVC_ID",
        column="INVC_PAID_AMT",
        pk_value_sql="SELECT MIN(INVC_ID) + 1 FROM INVC",
        bad_value_sql="999999.99",
        defect_type="paid_exceeds_total",
        reason="Paid amount exceeds invoice total; revenue recognition breaks.",
        severity="high",
    ),

    # -------- CDR_REC: call-record defects --------
    DefectPlan(
        table="CDR_REC",
        pk_column="CDR_ID",
        column="CDR_DUR_SEC",
        pk_value_sql="SELECT MIN(CDR_ID) FROM CDR_REC",
        bad_value_sql="-12",
        defect_type="negative_duration",
        reason="Duration cannot be negative; rating engine rejects the CDR and revenue is lost.",
        severity="critical",
    ),
    DefectPlan(
        table="CDR_REC",
        pk_column="CDR_ID",
        column="CDR_END_DT",
        pk_value_sql="SELECT MIN(CDR_ID) + 1 FROM CDR_REC",
        bad_value_sql="'1999-01-01 00:00:00'",
        defect_type="end_before_start",
        reason="Call end time precedes start time; duration calculation produces nonsense.",
        severity="high",
    ),

    # -------- Dangling FKs --------
    DefectPlan(
        table="SUBSCR_ACCT",
        pk_column="SUBSCR_ID",
        column="CUST_ID",
        pk_value_sql="SELECT MAX(SUBSCR_ID) FROM SUBSCR_ACCT",
        bad_value_sql="999999999",
        defect_type="fk_dangling_cust_id",
        reason="Subscriber points at a non-existent customer; JOINs silently drop the row or crash downstream ETL.",
        severity="critical",
    ),
    DefectPlan(
        table="PYMT",
        pk_column="PYMT_ID",
        column="BLNG_ACCT_ID",
        pk_value_sql="SELECT MAX(PYMT_ID) FROM PYMT",
        bad_value_sql="999999999",
        defect_type="fk_dangling_blng_acct",
        reason="Payment references a non-existent billing account; revenue assurance breaks.",
        severity="critical",
    ),
]


# --------------------------------------------------------------------------- #
# Execution
# --------------------------------------------------------------------------- #

def iter_source_targets(config) -> Iterable[dict]:
    """Yield telecom source database configs that we can write to."""
    for source in get_data_sources(config):
        dialect = source.get("dialect", "")
        if dialect not in ("sqlite", "duckdb"):
            continue
        # Skip the retail demo — different schema.
        if source.get("name") == "demo_showcase":
            continue
        yield source


def _ensure_ledger(engine: Engine, dialect: str) -> None:
    # Note: no autoincrement PK — DuckDB does not auto-assign INTEGER PRIMARY
    # KEY like SQLite does, so the ledger just records the (table, column,
    # defect_type) tuple without a surrogate id.
    with engine.begin() as conn:
        # Drop any stale ledger from a previous schema revision so both
        # SQLite and DuckDB start from the same shape.
        conn.execute(text("DROP TABLE IF EXISTS _defect_ledger"))
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS _defect_ledger (
                    table_name      VARCHAR(100),
                    pk_column       VARCHAR(100),
                    pk_value        VARCHAR(200),
                    column_name     VARCHAR(100),
                    defect_type     VARCHAR(100),
                    reason          VARCHAR(400),
                    severity        VARCHAR(20),
                    injected_at     VARCHAR(40)
                )
                """
            )
        )
        conn.execute(text("DELETE FROM _defect_ledger"))


def _apply_plan(engine: Engine, plan: DefectPlan, dry_run: bool) -> bool:
    """Apply one defect plan. Returns True on success."""
    with engine.connect() as conn:
        try:
            pk_value = conn.execute(text(plan.pk_value_sql)).scalar()
        except Exception as exc:
            logger.warning(
                "[%s.%s] could not resolve target row: %s",
                plan.table, plan.column, exc,
            )
            return False

    if pk_value is None:
        logger.warning(
            "[%s.%s] no candidate row found for defect '%s'",
            plan.table, plan.column, plan.defect_type,
        )
        return False

    sql = plan.update_sql()
    logger.info(
        "  → %-12s %-18s row %-8s %s",
        plan.table, plan.column, pk_value, plan.defect_type,
    )
    if dry_run:
        return True

    with engine.begin() as conn:
        conn.execute(text(sql))
        conn.execute(
            text(
                """
                INSERT INTO _defect_ledger
                  (table_name, pk_column, pk_value, column_name,
                   defect_type, reason, severity, injected_at)
                VALUES
                  (:t, :pkc, :pkv, :col, :dt, :r, :s, :ia)
                """
            ),
            {
                "t": plan.table,
                "pkc": plan.pk_column,
                "pkv": str(pk_value),
                "col": plan.column,
                "dt": plan.defect_type,
                "r": plan.reason,
                "s": plan.severity,
                "ia": "injected",
            },
        )
    return True


def inject_defects_into_source(source: dict, dry_run: bool = False) -> int:
    """Run every defect plan against one configured source. Returns success count."""
    name = source.get("name")
    conn_str = source.get("connection_string")
    logger.info("")
    logger.info("=" * 72)
    logger.info("Injecting defects into source: %s (%s)", name, conn_str)
    logger.info("=" * 72)

    engine = create_engine(conn_str)
    _ensure_ledger(engine, source.get("dialect", ""))

    successes = 0
    for plan in DEFECT_PLANS:
        if _apply_plan(engine, plan, dry_run=dry_run):
            successes += 1

    logger.info(
        "[%s] injected %d/%d defects", name, successes, len(DEFECT_PLANS)
    )
    return successes


def main() -> int:
    parser = argparse.ArgumentParser(description="Plant realistic production defects in source DBs.")
    parser.add_argument(
        "--targets",
        default=None,
        help="Comma-separated list of source names. Defaults to every writable telecom source.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the planned edits without touching the database.",
    )
    args = parser.parse_args()

    config = load_config()
    all_sources = list(iter_source_targets(config))

    if args.targets:
        requested = {n.strip() for n in args.targets.split(",") if n.strip()}
        sources = [s for s in all_sources if s.get("name") in requested]
        if not sources:
            logger.error("No matching telecom sources found for --targets=%s", args.targets)
            return 1
    else:
        sources = all_sources

    total = 0
    for source in sources:
        total += inject_defects_into_source(source, dry_run=args.dry_run)

    logger.info("")
    logger.info("Done. Applied %d defect-plan updates across %d source(s).", total, len(sources))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
