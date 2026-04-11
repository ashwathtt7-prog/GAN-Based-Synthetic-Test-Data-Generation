"""
Generation Strategy Planner
LLM considers all column policies in a domain and formulates the Generation Strategy.
"""

import logging
import json
from llm.model_client import get_model_client
from models.schemas import GenerationStrategySchema
from db.client import DatabaseClient

logger = logging.getLogger(__name__)

class StrategyPlanner:
    def __init__(self):
        self.model_client = get_model_client()
        
    def generate_strategy(self, table_name: str, domain: str, column_policies: list) -> GenerationStrategySchema:
        """Use LLM to determine table-level generation strategy."""
        
        # Serialize policies for context
        policies_json = json.dumps([p.model_dump() for p in column_policies], indent=2)
        
        prompt = f"""
Analyze the column-level policies for the table '{table_name}' in the '{domain}' domain,
and formulate a macro-level GenerationStrategySchema for a deterministic, rule-based pipeline.

Column Policies:
{policies_json}

Identify any temporal dependencies (e.g., START_DATE < END_DATE).
Identify business rules for post-generation validation.
Keep the plan rule-based only; do not propose CTGAN, TVAE, training, or tier-routing behavior.
Use tier_override only if you need a descriptive label for the rule plan, otherwise leave it null.
Decide edge case injection percentage.
        """
        
        try:
            strategy = self.model_client.invoke(
                prompt=prompt,
                output_schema=GenerationStrategySchema,
                retry_on_failure=True
            )
            return strategy
            
        except Exception as e:
            logger.error(f"Strategy Planning failed for {table_name}: {e}")
            return GenerationStrategySchema(
                table_name=table_name,
                domain=domain,
                tier_override=None,
                temporal_constraints=[],
                post_generation_rules=[],
                edge_case_injection_pct=0.05,
                notes="Fallback strategy due to LLM failure"
            )
