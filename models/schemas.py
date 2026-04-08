"""
All Pydantic v2 schemas for structured LLM output and API contracts.
Every LLM call outputs JSON validated against these schemas.
"""

from typing import Literal, Optional
from pydantic import BaseModel, Field


class ColumnPolicySchema(BaseModel):
    """Schema for LLM column classification output."""
    column_name: str
    table_name: str
    pii_classification: Literal["none", "sensitive_business", "uncertain"]
    sensitivity_reason: str = Field(description="Plain English explanation of classification")
    masking_strategy: Literal[
        "passthrough",           # not sensitive, use as-is for GAN training
        "substitute_realistic",  # replace with realistic fake values
        "format_preserving",     # replace preserving format/structure
        "suppress",              # omit entirely from synthetic output
        "generalise"             # replace with range or category
    ]
    constraint_profile: dict = Field(
        default_factory=dict,
        description="Constraints: {min, max, regex, allowed_values, distribution_hint}"
    )
    business_importance: Literal["critical", "important", "low"] = Field(
        description=(
            "critical = downstream AI pipelines depend on this column's distribution, "
            "important = part of business logic but not pipeline-critical, "
            "low = identifiers, labels, non-impactful columns"
        )
    )
    edge_case_flags: list[str] = Field(
        default_factory=list,
        description="Describe rare-but-important scenarios for this column"
    )
    dedup_mode: Literal["entity", "reference", "event"] = Field(
        description=(
            "entity = unique records required (customers, accounts), "
            "reference = repeats valid and expected (status codes, plan types), "
            "event = FK columns repeat, full records unique (transactions, calls)"
        )
    )
    llm_confidence: float = Field(ge=0.0, le=1.0, description="Confidence score 0.0-1.0")
    abbreviation_resolved: bool = Field(default=True)
    notes: str = Field(default="", description="Any reasoning the LLM wants to record")


class GenerationStrategySchema(BaseModel):
    """Schema for domain-level generation strategy output."""
    table_name: str
    domain: str
    tier_override: Optional[Literal["ctgan", "tvae", "rule_based", "hybrid"]] = Field(
        default=None,
        description="Override for default row-count-based tier selection"
    )
    temporal_constraints: list[dict] = Field(
        default_factory=list,
        description="List of {earlier_column, later_column} temporal ordering constraints"
    )
    post_generation_rules: list[str] = Field(
        default_factory=list,
        description="Plain English rules enforced after generation"
    )
    edge_case_injection_pct: float = Field(
        default=0.05, ge=0.0, le=0.3,
        description="Percentage of generated records that should be edge cases"
    )
    notes: str = Field(default="")


class DomainValidationSchema(BaseModel):
    """Schema for LLM domain partition validation output."""
    table_name: str
    suggested_domain: str
    validated_domain: str
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str


class FailureDiagnosisSchema(BaseModel):
    """Schema for LLM failure diagnosis output."""
    affected_table: str
    failure_type: str  # statistical, pii_leakage, lineage, business_rule
    root_cause: str
    corrective_action: str
    updated_strategy: Optional[GenerationStrategySchema] = None
    confidence: float = Field(ge=0.0, le=1.0)


class StatisticalProfile(BaseModel):
    """Statistical profile for a database column."""
    column_name: str
    data_type: str
    row_count: int = 0
    null_count: int = 0
    null_rate: float = 0.0
    unique_count: int = 0
    min_value: Optional[str] = None
    max_value: Optional[str] = None
    mean_value: Optional[float] = None
    std_dev: Optional[float] = None
    top_values: list[dict] = Field(default_factory=list)  # [{value, frequency}]
    value_lengths: Optional[dict] = None  # {min, max, avg}
    regex_patterns: list[str] = Field(default_factory=list)


class TableMetadata(BaseModel):
    """Metadata for a database table."""
    table_name: str
    row_count: int = 0
    column_count: int = 0
    domain: Optional[str] = None
    columns: list[StatisticalProfile] = Field(default_factory=list)


class RelationshipInfo(BaseModel):
    """Relationship between two tables."""
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    relationship_type: Literal["FK_DECLARED", "FK_INFERRED"] = "FK_DECLARED"
    confidence: float = 1.0
    cardinality: Optional[str] = None  # "1:N", "N:1", "N:M"


class PresidioResult(BaseModel):
    """Result from Presidio PII scan for a column."""
    column_name: str
    table_name: str
    pii_detected: bool = False
    pii_type: Optional[str] = None
    confidence: float = 0.0
    sample_matches: list[str] = Field(default_factory=list)


class ValidationResult(BaseModel):
    """Result from a validation check."""
    check_name: str
    table_name: str
    column_name: Optional[str] = None
    passed: bool
    metric_value: Optional[float] = None
    threshold: Optional[float] = None
    details: str = ""


class DeliveryManifest(BaseModel):
    """Manifest for delivered synthetic dataset."""
    run_id: str
    tables_generated: list[str]
    row_counts: dict[str, int]
    validation_results: dict[str, list[dict]]
    edge_case_coverage: dict[str, float]
    generation_strategies: dict[str, str]
    domains: list[str]
    timestamp: str
    output_format: str
    output_path: str


# === API Request/Response Models ===

class ReviewQueueItem(BaseModel):
    """API model for human review queue items."""
    id: int
    table_name: str
    column_name: str
    llm_best_guess: Optional[dict] = None
    flag_reason: str
    status: str = "pending"
    reviewer_notes: Optional[str] = None
    reviewed_at: Optional[str] = None
    created_at: str


class ReviewApproval(BaseModel):
    """API model for approving a review item."""
    reviewer_notes: Optional[str] = None


class ReviewCorrection(BaseModel):
    """API model for correcting a review item."""
    corrected_policy: ColumnPolicySchema
    reviewer_notes: Optional[str] = None


class AbbreviationSubmission(BaseModel):
    """API model for submitting an abbreviation expansion."""
    token: str
    expansion: str
    reviewer_notes: Optional[str] = None


class PipelineRunStatus(BaseModel):
    """API model for pipeline run status."""
    run_id: str
    status: str
    domains_completed: list[str] = []
    domains_pending: list[str] = []
    current_step: Optional[str] = None
    progress_pct: float = 0.0
    started_at: str
    elapsed_seconds: float = 0.0


class DashboardStats(BaseModel):
    """API model for dashboard statistics."""
    total_tables: int = 0
    total_columns: int = 0
    columns_classified: int = 0
    pii_columns_detected: int = 0
    columns_pending_review: int = 0
    domains: list[str] = []
    latest_run_status: Optional[str] = None
    validation_pass_rate: float = 0.0
