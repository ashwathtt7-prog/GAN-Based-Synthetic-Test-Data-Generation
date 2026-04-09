"""
Table Tier Router (Step 3.1)
Routes tables to CTGAN, TVAE, or rule-based generation based on
generation_strategy tier_override, shared table profiles, and row count thresholds.
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

    def route(self, table_name: str, row_count: int, tier_override: str = None, profile=None) -> str:
        """
        Determine the generation tier for a table.

        Args:
            table_name: Name of the table
            row_count: Number of rows in the source table
            tier_override: Override from generation_strategy (ctgan/tvae/rule_based/hybrid)
            profile: Optional shared table profile with structural/modeled column splits

        Returns:
            One of: "ctgan", "tvae", "rule_based", "hybrid"
        """
        if profile and not getattr(profile, "modeled_columns", []):
            logger.info("[TierRouter] %s -> rule_based (no modeled columns after profiling)", table_name)
            return "rule_based"

        if profile:
            modeled_count = len(getattr(profile, "modeled_columns", []))
            structural_count = len(getattr(profile, "structural_columns", []))
            if modeled_count == 0:
                logger.info(
                    "[TierRouter] %s -> rule_based (profile favors deterministic generation: %s modeled / %s structural)",
                    table_name,
                    modeled_count,
                    structural_count,
                )
                return "rule_based"
            if modeled_count == 1 and structural_count >= 8:
                logger.info(
                    "[TierRouter] %s -> rule_based (single modeled column is not enough for ML tier: %s modeled / %s structural)",
                    table_name,
                    modeled_count,
                    structural_count,
                )
                return "rule_based"
            if row_count >= 6000 and modeled_count <= 3 and structural_count >= 10:
                logger.info(
                    "[TierRouter] %s -> rule_based (large structural-heavy table is cheaper and safer to generate deterministically: %s modeled / %s structural / %s rows)",
                    table_name,
                    modeled_count,
                    structural_count,
                    row_count,
                )
                return "rule_based"

        if tier_override:
            logger.info("[TierRouter] %s -> %s (LLM override)", table_name, tier_override)
            return tier_override

        if row_count >= self.ctgan_min:
            tier = "ctgan"
        elif row_count >= self.tvae_min:
            tier = "tvae"
        else:
            tier = "rule_based"

        logger.info("[TierRouter] %s -> %s (row_count=%s)", table_name, tier, row_count)
        return tier
