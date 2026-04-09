"""
LLM Failure Diagnosis Agent
When validation checks fail, invokes the LLM to diagnose root cause
and produce a corrected GenerationStrategySchema.
"""

import json
import logging
from llm.model_client import get_model_client
from models.schemas import FailureDiagnosisSchema, ValidationResult

logger = logging.getLogger(__name__)


class FailureDiagnosisAgent:
    def __init__(self):
        self.model_client = get_model_client()

    def diagnose(
        self,
        table_name: str,
        domain: str,
        validation_results: list[ValidationResult],
        current_strategy: dict = None
    ) -> FailureDiagnosisSchema:
        """
        Invoke LLM to diagnose validation failures and suggest corrective action.

        Args:
            table_name: The failed table
            domain: Business domain
            validation_results: List of failed ValidationResult objects
            current_strategy: Current GenerationStrategySchema as dict

        Returns:
            FailureDiagnosisSchema with root cause and corrective action
        """
        failures = [r.model_dump() for r in validation_results if not r.passed]

        if not failures:
            return FailureDiagnosisSchema(
                affected_table=table_name,
                failure_type="none",
                root_cause="No failures detected",
                corrective_action="No action needed",
                confidence=1.0
            )

        prompt = f"""
Analyze the following validation failures for table '{table_name}' in domain '{domain}'
and diagnose the root cause. Then suggest corrective actions.

Validation Failures:
{json.dumps(failures, indent=2)}

Current Generation Strategy:
{json.dumps(current_strategy, indent=2) if current_strategy else "Not available"}

Produce a FailureDiagnosisSchema JSON with:
- failure_type: one of "statistical", "pii_leakage", "lineage", "business_rule"
- root_cause: Explain why the generation failed these checks
- corrective_action: Specific steps to fix the issue
- If you can improve the generation strategy, include an updated_strategy
"""

        try:
            diagnosis = self.model_client.invoke(
                prompt=prompt,
                output_schema=FailureDiagnosisSchema,
                retry_on_failure=True
            )
            return diagnosis

        except Exception as e:
            logger.error(f"LLM Failure Diagnosis failed for {table_name}: {e}")
            # Determine failure type from results
            failure_types = set()
            for f in failures:
                name = f.get("check_name", "").lower()
                if "ks" in name or "jsd" in name or "chi" in name:
                    failure_types.add("statistical")
                elif "pii" in name or "leak" in name or "re-id" in name:
                    failure_types.add("pii_leakage")
                elif "fk" in name or "lineage" in name or "temporal" in name:
                    failure_types.add("lineage")
                else:
                    failure_types.add("business_rule")

            return FailureDiagnosisSchema(
                affected_table=table_name,
                failure_type=", ".join(failure_types) if failure_types else "unknown",
                root_cause=f"LLM diagnosis failed: {str(e)[:100]}. Manual review recommended.",
                corrective_action="Increase CTGAN epochs, review masking strategies, check FK generation order.",
                confidence=0.3
            )
