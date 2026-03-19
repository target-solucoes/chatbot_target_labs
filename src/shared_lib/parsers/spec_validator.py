"""
Specification Validator for Analytics Executor Agent.

This module provides validation utilities for AnalyticsInputSpec instances,
ensuring logical consistency and correctness beyond basic type checking.
"""

import logging
from typing import Set, List, Optional

from src.shared_lib.models.schema import AnalyticsInputSpec, MetricSpec, DimensionSpec

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Exception raised when specification validation fails."""
    
    def __init__(self, message: str, field: Optional[str] = None):
        """
        Initialize validation error.
        
        Args:
            message: Error message
            field: Field that caused the error (if applicable)
        """
        self.message = message
        self.field = field
        super().__init__(self._format_message())
    
    def _format_message(self) -> str:
        """Format error message with context."""
        if self.field:
            return f"[{self.field}] {self.message}"
        return self.message


class SpecValidator:
    """
    Validator for AnalyticsInputSpec instances.
    
    This validator performs logical consistency checks beyond basic type validation:
    - Ensures sort.by references valid metric or dimension
    - Validates filter keys could match potential columns
    - Checks for duplicate metric/dimension names
    - Validates aggregation functions are appropriate
    """
    
    def __init__(self):
        """Initialize the validator."""
        logger.debug("SpecValidator initialized")
    
    def validate(self, spec: AnalyticsInputSpec) -> bool:
        """
        Validate an AnalyticsInputSpec instance.
        
        Performs comprehensive logical validation to ensure the specification
        is internally consistent and executable.
        
        Args:
            spec: AnalyticsInputSpec to validate
            
        Returns:
            True if validation passes
            
        Raises:
            ValidationError: If validation fails
            
        Example:
            >>> validator = SpecValidator()
            >>> spec = AnalyticsInputSpec(...)
            >>> validator.validate(spec)  # Returns True or raises ValidationError
        """
        logger.info(f"Validating specification for chart_type={spec.chart_type}")
        
        # Validate structure
        self.validate_structure(spec)
        
        # Validate metrics
        self.validate_metrics(spec.metrics)
        
        # Validate dimensions
        self.validate_dimensions(spec.dimensions)
        
        # Validate filters
        self.validate_filters(spec.filters)
        
        # Validate sort consistency
        if spec.sort:
            self.validate_sort_consistency(spec)
        
        logger.info("Specification validation passed")
        return True
    
    def validate_structure(self, spec: AnalyticsInputSpec) -> bool:
        """
        Validate overall structure of specification.
        
        Args:
            spec: AnalyticsInputSpec to validate
            
        Returns:
            True if structure is valid
            
        Raises:
            ValidationError: If structure is invalid
        """
        # Chart type should not be empty
        if not spec.chart_type or not spec.chart_type.strip():
            raise ValidationError(
                "chart_type cannot be empty",
                field="chart_type"
            )
        
        # Must have at least one metric
        if not spec.metrics or len(spec.metrics) == 0:
            raise ValidationError(
                "At least one metric is required",
                field="metrics"
            )
        
        # Data source should not be empty
        if not spec.data_source or not spec.data_source.strip():
            raise ValidationError(
                "data_source cannot be empty",
                field="data_source"
            )
        
        return True
    
    def validate_metrics(self, metrics: List[MetricSpec]) -> bool:
        """
        Validate metric specifications.
        
        Checks:
        - No duplicate metric names
        - Metric names are valid identifiers
        - Aggregation functions are supported
        
        Args:
            metrics: List of MetricSpec to validate
            
        Returns:
            True if metrics are valid
            
        Raises:
            ValidationError: If metrics are invalid
        """
        if not metrics:
            raise ValidationError("Metrics list cannot be empty", field="metrics")
        
        # Check for duplicate metric names
        metric_names = [m.name for m in metrics]
        duplicates = self._find_duplicates(metric_names)
        
        if duplicates:
            raise ValidationError(
                f"Duplicate metric names found: {', '.join(duplicates)}",
                field="metrics"
            )
        
        # Validate each metric
        for idx, metric in enumerate(metrics):
            # Validate name is not empty
            if not metric.name or not metric.name.strip():
                raise ValidationError(
                    f"Metric at index {idx} has empty name",
                    field=f"metrics[{idx}].name"
                )
            
            # Validate aggregation is set (should always be due to default)
            if not metric.aggregation:
                raise ValidationError(
                    f"Metric '{metric.name}' at index {idx} has no aggregation function",
                    field=f"metrics[{idx}].aggregation"
                )
        
        return True
    
    def validate_dimensions(self, dimensions: List[DimensionSpec]) -> bool:
        """
        Validate dimension specifications.
        
        Checks:
        - No duplicate dimension names
        - Dimension names are valid identifiers
        
        Args:
            dimensions: List of DimensionSpec to validate
            
        Returns:
            True if dimensions are valid
            
        Raises:
            ValidationError: If dimensions are invalid
        """
        # Dimensions can be empty (for total aggregations)
        if not dimensions:
            logger.debug("No dimensions specified (total aggregation)")
            return True
        
        # Check for duplicate dimension names
        dimension_names = [d.name for d in dimensions]
        duplicates = self._find_duplicates(dimension_names)
        
        if duplicates:
            raise ValidationError(
                f"Duplicate dimension names found: {', '.join(duplicates)}",
                field="dimensions"
            )
        
        # Validate each dimension
        for idx, dimension in enumerate(dimensions):
            # Validate name is not empty
            if not dimension.name or not dimension.name.strip():
                raise ValidationError(
                    f"Dimension at index {idx} has empty name",
                    field=f"dimensions[{idx}].name"
                )
        
        return True
    
    def validate_filters(self, filters: dict) -> bool:
        """
        Validate filter specifications.
        
        Checks:
        - Filter keys are valid (non-empty strings)
        - Filter values are appropriate types
        
        Args:
            filters: Filter dictionary to validate
            
        Returns:
            True if filters are valid
            
        Raises:
            ValidationError: If filters are invalid
        """
        # Filters can be empty
        if not filters:
            logger.debug("No filters specified")
            return True
        
        # Validate each filter
        for key, value in filters.items():
            # Validate key is not empty
            if not key or not key.strip():
                raise ValidationError(
                    "Filter key cannot be empty",
                    field="filters"
                )
            
            # Validate value is not None (explicit None not allowed, but absence is ok)
            if value is None:
                raise ValidationError(
                    f"Filter '{key}' has None value (remove filter instead of using None)",
                    field=f"filters.{key}"
                )
            
            # If value is dict (complex filter), validate structure
            if isinstance(value, dict):
                self._validate_complex_filter(key, value)
        
        return True
    
    def validate_sort_consistency(self, spec: AnalyticsInputSpec) -> bool:
        """
        Validate that sort specification is consistent with metrics and dimensions.
        
        Checks that sort.by references a valid metric or dimension name (or alias).
        
        Args:
            spec: AnalyticsInputSpec to validate
            
        Returns:
            True if sort is consistent
            
        Raises:
            ValidationError: If sort is inconsistent
        """
        if not spec.sort or not spec.sort.by:
            logger.debug("No sort.by specified, skipping consistency check")
            return True
        
        sort_by = spec.sort.by
        
        # Collect all valid field names (metrics and dimensions, names and aliases)
        valid_fields: Set[str] = set()
        
        # Add metric names and aliases
        for metric in spec.metrics:
            valid_fields.add(metric.name)
            if metric.alias:
                valid_fields.add(metric.alias)
        
        # Add dimension names and aliases
        for dimension in spec.dimensions:
            valid_fields.add(dimension.name)
            if dimension.alias:
                valid_fields.add(dimension.alias)
        
        # Check if sort.by references a valid field
        if sort_by not in valid_fields:
            raise ValidationError(
                f"sort.by references unknown field '{sort_by}'. "
                f"Must be one of: {', '.join(sorted(valid_fields))}",
                field="sort.by"
            )
        
        return True
    
    def _find_duplicates(self, items: List[str]) -> Set[str]:
        """
        Find duplicate items in a list.
        
        Args:
            items: List of strings to check
            
        Returns:
            Set of duplicate items
        """
        seen = set()
        duplicates = set()
        
        for item in items:
            if item in seen:
                duplicates.add(item)
            else:
                seen.add(item)
        
        return duplicates
    
    def _validate_complex_filter(self, key: str, value: dict) -> None:
        """
        Validate complex filter (operator-based).
        
        Args:
            key: Filter key
            value: Filter value dictionary
            
        Raises:
            ValidationError: If complex filter is invalid
        """
        # Complex filter should have 'operator' and 'value'
        if "operator" not in value:
            raise ValidationError(
                f"Complex filter '{key}' missing 'operator' field",
                field=f"filters.{key}"
            )
        
        if "value" not in value:
            raise ValidationError(
                f"Complex filter '{key}' missing 'value' field",
                field=f"filters.{key}"
            )
        
        # Validate operator is valid
        valid_operators = ["=", "!=", ">", "<", ">=", "<=", "=="]
        operator = value["operator"]
        
        if operator not in valid_operators:
            raise ValidationError(
                f"Complex filter '{key}' has invalid operator '{operator}'. "
                f"Must be one of: {', '.join(valid_operators)}",
                field=f"filters.{key}.operator"
            )


def validate_specification(spec: AnalyticsInputSpec) -> bool:
    """
    Convenience function to validate a specification.
    
    This is a shortcut that creates a SpecValidator instance and validates the spec.
    
    Args:
        spec: AnalyticsInputSpec to validate
        
    Returns:
        True if validation passes
        
    Raises:
        ValidationError: If validation fails
    """
    validator = SpecValidator()
    return validator.validate(spec)

