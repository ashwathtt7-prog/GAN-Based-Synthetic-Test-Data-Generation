"""
Telecom multi-source generator.

Given an already-seeded SQLite telecom database (datasets/telecom_source.db),
produces two additional "source databases" that mirror the exact same telecom
schema but sit on *different backends*, so the demo can point the pipeline at
three completely distinct inputs:

  1. sqlite:///datasets/telecom_source.db              (SQLite OLTP)
  2. duckdb:///datasets/telecom_dw.duckdb              (DuckDB warehouse)
  3. duckdb:///datasets/telecom_lake.duckdb            (DuckDB views over
                                                         Parquet files in
                                                         datasets/telecom_lake_parquet/)

All three sources expose the same table names, columns, and foreign-key
relationships. The goal is to show the synthesis pipeline handling the same
logical dataset delivered through different storage backends.
"""

import logging
import shutil
from pathlib import Path

import duckdb
import pandas as pd
import sqlalchemy as sa

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

ROOT = Path(__file__).resolve().parent
SQLITE_PATH = ROOT / "telecom_source.db"
DUCKDB_DW_PATH = ROOT / "telecom_dw.duckdb"
DUCKDB_LAKE_PATH = ROOT / "telecom_lake.duckdb"
PARQUET_DIR = ROOT / "telecom_lake_parquet"


def _read_sqlite_tables() -> dict[str, pd.DataFrame]:
    if not SQLITE_PATH.exists():
        raise FileNotFoundError(
            f"Expected {SQLITE_PATH} to exist. Run datasets/generate_seed_data.py first."
        )
    engine = sa.create_engine(f"sqlite:///{SQLITE_PATH}")
    insp = sa.inspect(engine)
    tables = insp.get_table_names()
    data: dict[str, pd.DataFrame] = {}
    for t in tables:
        data[t] = pd.read_sql(f'SELECT * FROM "{t}"', engine)
        logger.info("Read %s (%d rows) from SQLite", t, len(data[t]))
    return data


def _write_duckdb_warehouse(data: dict[str, pd.DataFrame]) -> None:
    if DUCKDB_DW_PATH.exists():
        DUCKDB_DW_PATH.unlink()

    con = duckdb.connect(str(DUCKDB_DW_PATH))
    try:
        for table_name, df in data.items():
            safe_df = df.copy()  # noqa: F841  (registered below)
            con.register("_tmp_df", safe_df)
            con.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM _tmp_df')
            con.unregister("_tmp_df")
            logger.info("Wrote %s (%d rows) into DuckDB warehouse", table_name, len(df))
    finally:
        con.close()


def _write_parquet_lake(data: dict[str, pd.DataFrame]) -> None:
    if PARQUET_DIR.exists():
        shutil.rmtree(PARQUET_DIR)
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    for table_name, df in data.items():
        path = PARQUET_DIR / f"{table_name}.parquet"
        df.to_parquet(path, index=False)
        logger.info("Wrote %s parquet file (%d rows)", table_name, len(df))

    # Build a DuckDB file that exposes the parquet files as tables so
    # SQLAlchemy reflection sees them exactly like any other table.
    if DUCKDB_LAKE_PATH.exists():
        DUCKDB_LAKE_PATH.unlink()

    con = duckdb.connect(str(DUCKDB_LAKE_PATH))
    try:
        for table_name in data.keys():
            parquet_path = (PARQUET_DIR / f"{table_name}.parquet").resolve().as_posix()
            con.execute(
                f'CREATE TABLE "{table_name}" AS SELECT * FROM read_parquet(\'{parquet_path}\')'
            )
            logger.info("Registered %s in parquet lake", table_name)
    finally:
        con.close()


def main() -> None:
    data = _read_sqlite_tables()
    _write_duckdb_warehouse(data)
    _write_parquet_lake(data)
    logger.info("All three telecom sources are ready:")
    logger.info(" - SQLite:   %s", SQLITE_PATH)
    logger.info(" - DuckDB:   %s", DUCKDB_DW_PATH)
    logger.info(" - Parquet:  %s (via %s)", PARQUET_DIR, DUCKDB_LAKE_PATH)


if __name__ == "__main__":
    main()
