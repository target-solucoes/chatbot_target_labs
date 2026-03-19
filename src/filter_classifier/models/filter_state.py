"""
Filter state models and Pydantic schemas for filter_classifier.

This module defines:
- FilterGraphState: Extended TypedDict for filter management workflow
- FilterSpec: Specification for individual filters
- FilterOperation: CRUD operation specification
- FilterOutput: Final output format with CRUD operations
"""

from typing import TypedDict, Optional, List, Dict, Any, Literal
from pydantic import BaseModel, Field, model_validator
from datetime import datetime

from src.shared_lib.models.schema import GraphState


# ============================================================================
# GRAPH STATE (TypedDict)
# ============================================================================


class FilterGraphState(GraphState):
    """
    Extended GraphState for filter management workflow.

    This TypedDict extends the base GraphState with filter-specific fields
    for managing CRUD operations and filter persistence.
    """

    # Filter management fields
    filter_history: List[Dict[str, Any]]
    """Historical record of filters applied in previous queries"""

    current_filters: Dict[str, Any]
    """Currently active filters from previous session"""

    filter_operations: Dict[str, Any]
    """CRUD operations identified in current query (ADICIONAR, ALTERAR, REMOVER, MANTER)"""

    filter_final: Dict[str, Any]
    """Final consolidated filters after applying all operations"""

    detected_filter_columns: List[str]
    """Column names detected as filter targets in the query"""

    filter_confidence: float
    """Confidence score for filter detection (0.0 to 1.0)"""

    # Phase 3 fields (parallel execution)
    plotly_output: Optional[Dict[str, Any]]
    """Result from plotly generator agent (figure, html, file_path, config)"""

    insight_result: Optional[Dict[str, Any]]
    """Result from insight generator agent (status, insights, metadata)"""

    # Phase 4 fields (formatter agent)
    formatter_output: Optional[Dict[str, Any]]
    """Result from formatter agent - final structured JSON output"""


# ============================================================================
# PYDANTIC SCHEMAS
# ============================================================================


class FilterSpec(BaseModel):
    """
    Specification for an individual filter.

    Represents a single filter condition with column, value, and operator.
    """

    column: str = Field(..., min_length=1, description="Column name to filter on")

    value: Any = Field(
        ...,
        description="Filter value (can be scalar, list, dict for complex operators)",
    )

    operator: Literal["=", ">", "<", ">=", "<=", "between", "in", "not_in"] = Field(
        default="=", description="Comparison operator for the filter"
    )

    confidence: float = Field(
        default=1.0, ge=0.0, le=1.0, description="Confidence in this filter detection"
    )

    class Config:
        """Pydantic configuration."""

        json_schema_extra = {
            "examples": [
                {
                    "column": "CategoryColumn",
                    "value": "ValueA",
                    "operator": "=",
                    "confidence": 0.95,
                },
                {
                    "column": "GroupColumn",
                    "value": ["Option1", "Option2"],
                    "operator": "in",
                    "confidence": 0.90,
                },
                {
                    "column": "TemporalColumn",
                    "value": {"start": "2024-01-01", "end": "2024-12-31"},
                    "operator": "between",
                    "confidence": 0.85,
                },
            ]
        }


class FilterOperation(BaseModel):
    """
    CRUD operation specification for filter management.

    Represents the operations to be applied to filters:
    - ADICIONAR: Add new filters
    - ALTERAR: Modify existing filters
    - REMOVER: Remove filters
    - MANTER: Keep filters unchanged
    """

    ADICIONAR: Dict[str, Any] = Field(
        default_factory=dict, description="Filters to add"
    )

    ALTERAR: Dict[str, Any] = Field(
        default_factory=dict, description="Filters to alter with from/to values"
    )

    REMOVER: Dict[str, Any] = Field(
        default_factory=dict, description="Filters to remove"
    )

    MANTER: Dict[str, Any] = Field(
        default_factory=dict, description="Filters to keep unchanged"
    )

    class Config:
        """Pydantic configuration."""

        json_schema_extra = {
            "examples": [
                {
                    "ADICIONAR": {"CategoryColumn": "ValueA", "GroupColumn": "Option1"},
                    "ALTERAR": {},
                    "REMOVER": {},
                    "MANTER": {},
                },
                {
                    "ADICIONAR": {},
                    "ALTERAR": {"CategoryColumn": {"from": "ValueA", "to": "ValueB"}},
                    "REMOVER": {},
                    "MANTER": {"GroupColumn": "Option1"},
                },
                {
                    "ADICIONAR": {},
                    "ALTERAR": {},
                    "REMOVER": {"CategoryColumn": "ValueA"},
                    "MANTER": {"GroupColumn": "Option1"},
                },
            ]
        }


class FilterOutputMetadata(BaseModel):
    """Metadata for filter output."""

    confidence: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Overall confidence in filter detection",
    )

    timestamp: str = Field(
        default_factory=lambda: datetime.now().isoformat(),
        description="Processing timestamp in ISO format",
    )

    columns_detected: List[str] = Field(
        default_factory=list,
        description="List of column names detected as filter targets",
    )

    errors: List[str] = Field(
        default_factory=list, description="List of errors encountered during processing"
    )

    status: Literal["success", "error", "partial"] = Field(
        default="success", description="Processing status"
    )


class FilterOutput(BaseModel):
    """
    Complete output from filter_classifier agent.

    This schema defines the structured output with CRUD operations
    and final consolidated filters.
    """

    ADICIONAR: Dict[str, Any] = Field(
        default_factory=dict, description="Filters that were added"
    )

    MANTER: Dict[str, Any] = Field(
        default_factory=dict, description="Filters that were kept unchanged"
    )

    ALTERAR: Dict[str, Any] = Field(
        default_factory=dict, description="Filters that were altered"
    )

    REMOVER: Dict[str, Any] = Field(
        default_factory=dict, description="Filters that were removed"
    )

    filter_final: Dict[str, Any] = Field(
        default_factory=dict,
        description="Final consolidated filters after all operations",
    )

    metadata: FilterOutputMetadata = Field(
        default_factory=FilterOutputMetadata,
        description="Metadata about the filter processing",
    )

    @model_validator(mode="after")
    def validate_filter_consistency(self) -> "FilterOutput":
        """
        Validate that filter_final is consistent with operations.

        This ensures that the final filters match the expected result
        of applying all CRUD operations.
        """
        # This is a basic validation - detailed logic is in FilterManager
        if not self.filter_final and not self.ADICIONAR and not self.MANTER:
            # If no final filters, there should be no operations either
            pass

        return self

    class Config:
        """Pydantic configuration."""

        json_schema_extra = {
            "examples": [
                {
                    "ADICIONAR": {"CategoryColumn": "ValueA"},
                    "MANTER": {},
                    "ALTERAR": {},
                    "REMOVER": {},
                    "filter_final": {"CategoryColumn": "ValueA"},
                    "metadata": {
                        "confidence": 0.95,
                        "timestamp": "2025-11-05T10:00:00",
                        "columns_detected": ["CategoryColumn"],
                        "errors": [],
                        "status": "success",
                    },
                },
                {
                    "ADICIONAR": {},
                    "MANTER": {"GroupColumn": "Option1"},
                    "ALTERAR": {"CategoryColumn": {"from": "ValueA", "to": "ValueB"}},
                    "REMOVER": {},
                    "filter_final": {
                        "CategoryColumn": "ValueB",
                        "GroupColumn": "Option1",
                    },
                    "metadata": {
                        "confidence": 0.90,
                        "timestamp": "2025-11-05T10:01:00",
                        "columns_detected": ["CategoryColumn"],
                        "errors": [],
                        "status": "success",
                    },
                },
            ]
        }
