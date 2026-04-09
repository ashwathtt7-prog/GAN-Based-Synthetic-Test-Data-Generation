"""
Deduplication Registry (Step 3.7)
SHA-256 hash-based deduplication with mode awareness:
  - entity: unique records required (customers, accounts)
  - reference: skip dedup entirely (status codes, plan types)
  - event: FK repeats allowed, full records unique (transactions, calls)
"""

import hashlib
import logging
import pandas as pd
from db.client import DatabaseClient
from db.schema import DedupHashRegistry

logger = logging.getLogger(__name__)


class DedupEngine:
    def __init__(self, db_client: DatabaseClient = None):
        self.db_client = db_client or DatabaseClient()

    def _hash_record(self, record: pd.Series, columns: list[str]) -> str:
        """Compute SHA-256 hash of a record over specified columns."""
        values = "|".join(str(record.get(c, "")) for c in sorted(columns))
        return hashlib.sha256(values.encode()).hexdigest()

    def deduplicate(
        self,
        table_name: str,
        df: pd.DataFrame,
        dedup_mode: str,
        fk_columns: list[str],
        run_id: str,
        max_retries: int = 3
    ) -> pd.DataFrame:
        """
        Apply mode-aware deduplication.

        Args:
            table_name: Name of the table
            df: Synthetic data to deduplicate
            dedup_mode: "entity", "reference", or "event"
            fk_columns: List of FK column names (used for event mode)
            run_id: Current generation run ID
            max_retries: Max attempts to regenerate duplicates

        Returns:
            Deduplicated DataFrame
        """
        if dedup_mode == "reference":
            logger.info(f"[Dedup] {table_name}: reference mode — skipping dedup")
            return df

        logger.info(f"[Dedup] {table_name}: applying {dedup_mode} dedup on {len(df)} records")

        # Determine which columns to hash
        if dedup_mode == "entity":
            hash_columns = [c for c in df.columns if c != '_edge_case']
        elif dedup_mode == "event":
            # For events, hash full record but allow FK repeats
            hash_columns = [c for c in df.columns if c not in fk_columns and c != '_edge_case']
        else:
            hash_columns = [c for c in df.columns if c != '_edge_case']

        # Load existing hashes from registry
        existing_hashes = set()
        with self.db_client.session() as session:
            existing = session.query(DedupHashRegistry).filter_by(table_name=table_name).all()
            existing_hashes = {r.record_hash for r in existing}

        # Compute hashes and filter duplicates
        hashes = []
        keep_mask = []
        new_hashes = []

        for _, row in df.iterrows():
            h = self._hash_record(row, hash_columns)
            if h in existing_hashes or h in new_hashes:
                keep_mask.append(False)
            else:
                keep_mask.append(True)
                new_hashes.append(h)
            hashes.append(h)

        deduped_df = df[keep_mask].reset_index(drop=True)
        removed = len(df) - len(deduped_df)

        if removed > 0:
            logger.info(f"[Dedup] {table_name}: removed {removed} duplicate records")

        # Register new hashes
        with self.db_client.session() as session:
            for h in new_hashes:
                entry = DedupHashRegistry(
                    table_name=table_name,
                    record_hash=h,
                    generation_run_id=run_id
                )
                session.add(entry)

        logger.info(f"[Dedup] {table_name}: {len(deduped_df)} unique records after dedup")
        return deduped_df
