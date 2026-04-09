"""
Table Tier Router (Step 3.1)
Routes tables to CTGAN, TVAE, or rule-based generation based on
generation_strategy tier_override or row count thresholds.
"""

import logging
from config.config import load_config

logger = logging.getLogger(__name__)


class TierRouter:
    def __init__(self, config: dict = None):
        self.config = config or load_config()
        gen_cfg = self.config.get("generation", {})
        thresholds = gen_cfg.get("row_count_thresholds", {})
        configured_ctgan_min = thresholds.get("ctgan_min", 2000)
        # Keep medium-sized POC tables on TVAE to avoid very slow CTGAN runs on
        # high-cardinality schemas during local end-to-end execution.
        self.ctgan_min = max(configured_ctgan_min, 5000)
        self.tvae_min = thresholds.get("tvae_min", 200)

    def route(self, table_name: str, row_count: int, tier_override: str = None) -> str:
        """
        Determine the generation tier for a table.

        Args:
            table_name: Name of the table
            row_count: Number of rows in the source table
            tier_override: Override from generation_strategy (ctgan/tvae/rule_based/hybrid)

        Returns:
            One of: "ctgan", "tvae", "rule_based", "hybrid"
        """
        if tier_override:
            logger.info(f"[TierRouter] {table_name} → {tier_override} (LLM override)")
            return tier_override

        if row_count >= self.ctgan_min:
            tier = "ctgan"
        elif row_count >= self.tvae_min:
            tier = "tvae"
        else:
            tier = "rule_based"

        logger.info(f"[TierRouter] {table_name} → {tier} (row_count={row_count})")
        return tier
