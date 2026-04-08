"""
Data Validator
Validates generated synthetic data against original data and LLM constraints.
"""

import logging
import pandas as pd
from sdv.evaluation.single_table import evaluate_quality
from models.schemas import ValidationResult

logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self, table_name: str):
        self.table_name = table_name

    def validate_statistical_similarity(self, real_data: pd.DataFrame, synthetic_data: pd.DataFrame, metadata) -> list[ValidationResult]:
        """SDV built-in statistical quality evaluation."""
        logger.info(f"Running SDV Quality Report for {self.table_name}")
        
        results = []
        try:
             quality_report = evaluate_quality(
                 real_data,
                 synthetic_data,
                 metadata
             )
             
             score = quality_report.get_score()
             results.append(ValidationResult(
                 check_name="Overall SDV Quality Score",
                 table_name=self.table_name,
                 passed=(score >= 0.8),
                 metric_value=score,
                 threshold=0.8,
                 details="Aggregate of Column Shapes and Column Pair Trends"
             ))
        except Exception as e:
             logger.error(f"SDV Quality eval failed: {e}")
             
        return results

    def validate_llm_constraints(self, synthetic_data: pd.DataFrame, generation_strategy) -> list[ValidationResult]:
        """Verify hard boundaries determined by the LLM Strategy."""
        results = []
        
        if not generation_strategy:
             return results
             
        # Example validation: Temporal Ordering (Start Date < End Date)
        for constraint in generation_strategy.temporal_constraints:
             col_start = constraint.get('earlier_column')
             col_end = constraint.get('later_column')
             
             if col_start in synthetic_data.columns and col_end in synthetic_data.columns:
                 # Check violations
                 synth_valid = synthetic_data.dropna(subset=[col_start, col_end])
                 violations = synth_valid[synth_valid[col_start] > synth_valid[col_end]]
                 
                 violation_count = len(violations)
                 violation_pct = violation_count / len(synth_valid) if len(synth_valid) > 0 else 0
                 
                 results.append(ValidationResult(
                     check_name=f"Temporal Rule: {col_start} <= {col_end}",
                     table_name=self.table_name,
                     passed=(violation_count == 0),
                     metric_value=violation_pct,
                     threshold=0.0,
                     details=f"{violation_count} records violated."
                 ))
                 
        return results

    def validate_pii_leakage(self, synthetic_data: pd.DataFrame, column_policies: list) -> list[ValidationResult]:
        """Ensure no 'suppressed' columns exist, and check deterministic formatting for PII."""
        results = []
        for policy in column_policies:
            col = policy.column_name
            
            if policy.masking_strategy == "suppress":
                passed = col not in synthetic_data.columns
                results.append(ValidationResult(
                    check_name=f"PII Suppression Leak Check: {col}",
                    table_name=self.table_name,
                    column_name=col,
                    passed=passed,
                    details="Column was found in output!" if not passed else "Column successfully suppressed."
                ))
                
            elif policy.masking_strategy == "format_preserving" and col in synthetic_data.columns:
                # E.g., if it's SSN we expect 9 digits. SDV should have made it ID type.
                 if policy.pii_classification == "SSN" or "SSN" in col:
                     # Basic heuristic: Check length
                     invalid_lens = synthetic_data[col].astype(str).str.len() != 9
                     bad_count = invalid_lens.sum()
                     results.append(ValidationResult(
                         check_name=f"Format Check: {col} SSN Format",
                         table_name=self.table_name,
                         column_name=col,
                         passed=(bad_count == 0),
                         details=f"{bad_count} values do not have length 9."
                     ))
                     
        return results
