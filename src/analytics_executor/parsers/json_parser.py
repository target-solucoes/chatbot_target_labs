"""
JSON Parser for Analytics Executor Agent.

This module provides parsing and validation functionality for JSON specifications
received from the graphic_classifier_agent. It converts raw JSON dictionaries into
validated Pydantic models ready for execution.
"""

import logging
from typing import Dict, Any, List, Optional

from src.shared_lib.models.schema import (
    AnalyticsInputSpec,
    MetricSpec,
    DimensionSpec,
    SortSpec,
    ChartOutput
)

logger = logging.getLogger(__name__)


class JSONParsingError(Exception):
    """Exception raised when JSON parsing fails."""
    
    def __init__(self, message: str, field: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        """
        Initialize parsing error.
        
        Args:
            message: Error message
            field: Field that caused the error (if applicable)
            details: Additional error details
        """
        self.message = message
        self.field = field
        self.details = details or {}
        super().__init__(self._format_message())
    
    def _format_message(self) -> str:
        """Format error message with context."""
        msg = self.message
        if self.field:
            msg = f"[{self.field}] {msg}"
        if self.details:
            details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            msg = f"{msg} ({details_str})"
        return msg


class JSONParser:
    """
    Parser for JSON specifications from graphic_classifier_agent.
    
    This parser validates and converts raw JSON dictionaries into validated
    AnalyticsInputSpec instances, ensuring all required fields are present
    and properly formatted.
    """
    
    # Required fields for a valid analytics specification
    REQUIRED_FIELDS = ["chart_type", "metrics", "dimensions", "data_source"]
    
    def __init__(self):
        """Initialize the JSON parser."""
        logger.debug("JSONParser initialized")
    
    def parse(self, json_input: Dict[str, Any]) -> AnalyticsInputSpec:
        """
        Parse and validate JSON specification.
        
        This method performs the following steps:
        1. Validates that all required fields are present
        2. Validates field types and structures
        3. Converts to Pydantic models with automatic validation
        4. Returns validated AnalyticsInputSpec instance
        
        Args:
            json_input: Raw JSON dictionary from graphic_classifier_agent
            
        Returns:
            Validated AnalyticsInputSpec instance
            
        Raises:
            JSONParsingError: If parsing fails due to missing/invalid fields
            
        Example:
            >>> parser = JSONParser()
            >>> spec = parser.parse({
            ...     "chart_type": "bar_horizontal",
            ...     "metrics": [{"name": "Valor_Vendido", "aggregation": "sum"}],
            ...     "dimensions": [{"name": "Cod_Cliente"}],
            ...     "data_source": "test_data"
            ... })
        """
        try:
            logger.info("Parsing JSON specification")
            
            # Step 1: Validate required fields
            self._validate_required_fields(json_input)
            
            # Step 2: Validate field types
            self._validate_field_types(json_input)
            
            # Step 3: Parse and validate using Pydantic
            spec = AnalyticsInputSpec(**json_input)
            
            logger.info(
                f"Successfully parsed specification: chart_type={spec.chart_type}, "
                f"metrics={len(spec.metrics)}, dimensions={len(spec.dimensions)}"
            )
            
            return spec
            
        except JSONParsingError:
            # Re-raise our custom errors
            raise
        except ValueError as e:
            # Pydantic validation error
            logger.error(f"Validation error: {e}")
            raise JSONParsingError(
                f"Invalid field value: {str(e)}",
                details={"validation_error": str(e)}
            )
        except Exception as e:
            # Unexpected error
            logger.error(f"Unexpected parsing error: {e}")
            raise JSONParsingError(
                f"Failed to parse JSON specification: {str(e)}",
                details={"error_type": type(e).__name__}
            )
    
    def parse_from_chart_output(self, chart_output: ChartOutput) -> AnalyticsInputSpec:
        """
        Parse from ChartOutput (Phase 1 output).
        
        This is a convenience method that converts ChartOutput from the
        graphic_classifier_agent directly into AnalyticsInputSpec.
        
        Args:
            chart_output: ChartOutput instance from Phase 1
            
        Returns:
            Validated AnalyticsInputSpec instance
            
        Raises:
            JSONParsingError: If conversion fails
            
        Example:
            >>> parser = JSONParser()
            >>> chart_out = ChartOutput(...)  # From Phase 1
            >>> spec = parser.parse_from_chart_output(chart_out)
        """
        try:
            logger.info("Converting ChartOutput to AnalyticsInputSpec")
            spec = AnalyticsInputSpec.from_chart_output(chart_output)
            logger.info("Successfully converted ChartOutput")
            return spec
        except ValueError as e:
            logger.error(f"Conversion error: {e}")
            raise JSONParsingError(
                f"Failed to convert ChartOutput: {str(e)}",
                details={"conversion_error": str(e)}
            )
    
    def _validate_required_fields(self, json_input: Dict[str, Any]) -> None:
        """
        Validate that all required fields are present.
        
        Args:
            json_input: Raw JSON dictionary
            
        Raises:
            JSONParsingError: If any required field is missing
        """
        missing_fields = []
        
        for field in self.REQUIRED_FIELDS:
            if field not in json_input:
                missing_fields.append(field)
        
        if missing_fields:
            logger.error(f"Missing required fields: {missing_fields}")
            raise JSONParsingError(
                f"Missing required field(s): {', '.join(missing_fields)}",
                details={
                    "missing_fields": missing_fields,
                    "required_fields": self.REQUIRED_FIELDS
                }
            )
    
    def _validate_field_types(self, json_input: Dict[str, Any]) -> None:
        """
        Validate field types and structures.
        
        Args:
            json_input: Raw JSON dictionary
            
        Raises:
            JSONParsingError: If field types are invalid
        """
        # Validate chart_type is string
        if not isinstance(json_input.get("chart_type"), str):
            raise JSONParsingError(
                "chart_type must be a string",
                field="chart_type",
                details={"received_type": type(json_input.get("chart_type")).__name__}
            )
        
        # Validate metrics is list and not empty
        metrics = json_input.get("metrics")
        if not isinstance(metrics, list):
            raise JSONParsingError(
                "metrics must be a list",
                field="metrics",
                details={"received_type": type(metrics).__name__}
            )
        
        if len(metrics) == 0:
            raise JSONParsingError(
                "metrics list cannot be empty (at least 1 metric required)",
                field="metrics"
            )
        
        # Validate each metric
        for idx, metric in enumerate(metrics):
            self._validate_metric(metric, idx)
        
        # Validate dimensions is list
        dimensions = json_input.get("dimensions")
        if not isinstance(dimensions, list):
            raise JSONParsingError(
                "dimensions must be a list",
                field="dimensions",
                details={"received_type": type(dimensions).__name__}
            )
        
        # Validate each dimension
        for idx, dimension in enumerate(dimensions):
            self._validate_dimension(dimension, idx)
        
        # Validate filters is dict (if present)
        filters = json_input.get("filters")
        if filters is not None and not isinstance(filters, dict):
            raise JSONParsingError(
                "filters must be a dictionary",
                field="filters",
                details={"received_type": type(filters).__name__}
            )
        
        # Validate sort (if present)
        sort = json_input.get("sort")
        if sort is not None:
            self._validate_sort(sort)
        
        # Validate top_n (if present)
        top_n = json_input.get("top_n")
        if top_n is not None:
            if not isinstance(top_n, int):
                raise JSONParsingError(
                    "top_n must be an integer",
                    field="top_n",
                    details={"received_type": type(top_n).__name__}
                )
            if top_n < 1:
                raise JSONParsingError(
                    "top_n must be at least 1",
                    field="top_n",
                    details={"received_value": top_n}
                )
        
        # Validate data_source is string
        if not isinstance(json_input.get("data_source"), str):
            raise JSONParsingError(
                "data_source must be a string",
                field="data_source",
                details={"received_type": type(json_input.get("data_source")).__name__}
            )
    
    def _validate_metric(self, metric: Any, index: int) -> None:
        """
        Validate a single metric specification.
        
        Args:
            metric: Metric specification
            index: Index of metric in list
            
        Raises:
            JSONParsingError: If metric is invalid
        """
        if not isinstance(metric, dict):
            raise JSONParsingError(
                f"Metric at index {index} must be a dictionary",
                field=f"metrics[{index}]",
                details={"received_type": type(metric).__name__}
            )
        
        # Validate required metric fields
        if "name" not in metric:
            raise JSONParsingError(
                f"Metric at index {index} missing required field 'name'",
                field=f"metrics[{index}].name"
            )
        
        if not isinstance(metric["name"], str) or not metric["name"].strip():
            raise JSONParsingError(
                f"Metric 'name' at index {index} must be a non-empty string",
                field=f"metrics[{index}].name",
                details={"received_value": metric.get("name")}
            )
        
        # Validate aggregation if present (Pydantic will validate allowed values)
        if "aggregation" in metric:
            agg = metric["aggregation"]
            if not isinstance(agg, str):
                raise JSONParsingError(
                    f"Metric 'aggregation' at index {index} must be a string",
                    field=f"metrics[{index}].aggregation",
                    details={"received_type": type(agg).__name__}
                )
    
    def _validate_dimension(self, dimension: Any, index: int) -> None:
        """
        Validate a single dimension specification.
        
        Args:
            dimension: Dimension specification
            index: Index of dimension in list
            
        Raises:
            JSONParsingError: If dimension is invalid
        """
        if not isinstance(dimension, dict):
            raise JSONParsingError(
                f"Dimension at index {index} must be a dictionary",
                field=f"dimensions[{index}]",
                details={"received_type": type(dimension).__name__}
            )
        
        # Validate required dimension fields
        if "name" not in dimension:
            raise JSONParsingError(
                f"Dimension at index {index} missing required field 'name'",
                field=f"dimensions[{index}].name"
            )
        
        if not isinstance(dimension["name"], str) or not dimension["name"].strip():
            raise JSONParsingError(
                f"Dimension 'name' at index {index} must be a non-empty string",
                field=f"dimensions[{index}].name",
                details={"received_value": dimension.get("name")}
            )
    
    def _validate_sort(self, sort: Any) -> None:
        """
        Validate sort specification.
        
        Args:
            sort: Sort specification
            
        Raises:
            JSONParsingError: If sort is invalid
        """
        if not isinstance(sort, dict):
            raise JSONParsingError(
                "sort must be a dictionary",
                field="sort",
                details={"received_type": type(sort).__name__}
            )
        
        # Validate 'by' field if present
        if "by" in sort:
            if not isinstance(sort["by"], str):
                raise JSONParsingError(
                    "sort.by must be a string",
                    field="sort.by",
                    details={"received_type": type(sort["by"]).__name__}
                )
        
        # Validate 'order' field if present (Pydantic will validate values)
        # Note: order is optional and can be None
        if "order" in sort:
            order = sort["order"]
            # Allow None (optional field)
            if order is not None:
                if not isinstance(order, str):
                    raise JSONParsingError(
                        "sort.order must be a string or None",
                        field="sort.order",
                        details={"received_type": type(order).__name__}
                    )
                if order not in ["asc", "desc"]:
                    raise JSONParsingError(
                        "sort.order must be 'asc' or 'desc'",
                        field="sort.order",
                        details={"received_value": order, "allowed_values": ["asc", "desc"]}
                    )


def parse_json_specification(json_input: Dict[str, Any]) -> AnalyticsInputSpec:
    """
    Convenience function to parse JSON specification.
    
    This is a shortcut that creates a JSONParser instance and parses the input.
    
    Args:
        json_input: Raw JSON dictionary
        
    Returns:
        Validated AnalyticsInputSpec instance
        
    Raises:
        JSONParsingError: If parsing fails
    """
    parser = JSONParser()
    return parser.parse(json_input)

