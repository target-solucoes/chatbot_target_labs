"""
Granularity Validator Module

Validates query results against chart type requirements, including:
- Row count validation
- Temporal dimension validation
- Chronological ordering
- Temporal gap detection
- Multi-series validation
"""

from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple
import logging
import pandas as pd

from src.shared_lib.models.schema import AnalyticsInputSpec, DimensionSpec

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """
    Result of validation check.

    Attributes:
        is_valid: Whether validation passed
        errors: List of validation errors (blocking issues)
        warnings: List of validation warnings (non-blocking issues)
    """

    is_valid: bool
    errors: List[str]
    warnings: List[str]

    def __str__(self) -> str:
        """String representation of validation result."""
        if self.is_valid and not self.warnings:
            return "Validation passed"

        parts = []
        if not self.is_valid:
            parts.append(f"Errors: {'; '.join(self.errors)}")
        if self.warnings:
            parts.append(f"Warnings: {'; '.join(self.warnings)}")

        return " | ".join(parts)


class GranularityValidator:
    """
    Validates query results meet chart type requirements.

    This validator ensures that the data returned from queries is structurally
    compatible with the requested chart type, checking:
    - Sufficient data points for visualization
    - Presence of required temporal dimensions
    - Chronological ordering
    - Temporal continuity
    - Multi-series structure
    """

    # Chart type validation requirements
    REQUIREMENTS = {
        "line": {
            "min_rows": 2,
            "requires_temporal_dimension": True,
            "requires_chronological_order": True,
            "validates_continuity": True,
        },
        "line_composed": {
            "min_rows": 2,
            "requires_temporal_dimension": True,
            "requires_chronological_order": True,
            "validates_multi_series": True,
        },
        "area": {
            "min_rows": 2,
            "requires_temporal_dimension": True,
            "requires_chronological_order": True,
            "validates_continuity": True,
        },
        "area_stacked": {
            "min_rows": 2,
            "requires_temporal_dimension": True,
        },
        "bar_horizontal": {
            "min_rows": 1,
            "validates_aggregation": False,  # Can be detail or aggregated
        },
        "bar_vertical": {
            "min_rows": 1,
            "validates_aggregation": False,
        },
        # REMOVED: bar_vertical_composed (migrated to line_composed)
        "bar_vertical_stacked": {
            "min_rows": 2,
        },
        "pie": {
            "min_rows": 2,
            "max_rows": 10,  # Pie charts get crowded with too many slices
        },
        "donut": {
            "min_rows": 2,
            "max_rows": 10,
        },
    }

    # Temporal column indicators
    TEMPORAL_KEYWORDS = {
        "data",
        "date",
        "datetime",
        "timestamp",
        "ano",
        "year",
        "mes",
        "month",
        "dia",
        "day",
        "trimestre",
        "quarter",
        "semestre",
        "semester",
        "semana",
        "week",
        "hora",
        "hour",
        "periodo",
        "period",
    }

    def validate(
        self, result_df: pd.DataFrame, spec: AnalyticsInputSpec
    ) -> ValidationResult:
        """
        Main validation entry point.

        Args:
            result_df: Query result DataFrame
            spec: Analytics input specification

        Returns:
            ValidationResult with errors and warnings
        """
        errors = []
        warnings = []

        chart_type = spec.chart_type.lower() if spec.chart_type else "bar_horizontal"
        requirements = self.REQUIREMENTS.get(chart_type, {})

        logger.debug(f"Validating result for chart_type='{chart_type}'")

        # Validate minimum rows
        if "min_rows" in requirements:
            min_rows = requirements["min_rows"]
            if len(result_df) < min_rows:
                errors.append(
                    f"Chart type '{chart_type}' requires at least {min_rows} rows, "
                    f"got {len(result_df)}"
                )

        # Validate maximum rows (for pie/donut)
        if "max_rows" in requirements:
            max_rows = requirements["max_rows"]
            if len(result_df) > max_rows:
                warnings.append(
                    f"Chart type '{chart_type}' works best with {max_rows} or fewer rows, "
                    f"got {len(result_df)} (chart may be crowded)"
                )

        # Validate temporal dimension
        if requirements.get("requires_temporal_dimension"):
            if not self._has_temporal_dimension(result_df, spec):
                errors.append(
                    f"Chart type '{chart_type}' requires temporal dimension, "
                    f"but none found"
                )

        # Validate chronological order
        if requirements.get("requires_chronological_order"):
            temporal_col = self._get_temporal_column(result_df, spec)
            if temporal_col:
                if not self._is_chronologically_ordered(result_df, temporal_col):
                    warnings.append(
                        f"Data should be in chronological order for '{chart_type}' charts"
                    )

        # Validate continuity (detect gaps)
        if requirements.get("validates_continuity"):
            temporal_col = self._get_temporal_column(result_df, spec)
            if temporal_col:
                gaps = self._detect_temporal_gaps(result_df, temporal_col)
                if gaps:
                    gap_summary = gaps[:3]  # Show first 3 gaps
                    warnings.append(
                        f"Detected {len(gaps)} temporal gap(s) in data: {gap_summary}"
                    )

        # Validate multi-series structure
        if requirements.get("validates_multi_series"):
            validation_msg = self._validate_multi_series_structure(result_df, spec)
            if validation_msg:
                warnings.append(validation_msg)

        # Validate top_n limit
        if spec.top_n and len(result_df) > spec.top_n:
            warnings.append(
                f"Result has {len(result_df)} rows but top_n={spec.top_n} was specified"
            )

        # Determine overall validity
        is_valid = len(errors) == 0

        result = ValidationResult(is_valid=is_valid, errors=errors, warnings=warnings)

        if not is_valid:
            logger.warning(f"Validation failed: {result}")
        elif warnings:
            logger.info(f"Validation passed with warnings: {result}")
        else:
            logger.debug("Validation passed")

        return result

    def _has_temporal_dimension(
        self, df: pd.DataFrame, spec: AnalyticsInputSpec
    ) -> bool:
        """
        Check if result has temporal dimension.

        Args:
            df: Result DataFrame
            spec: Analytics specification

        Returns:
            True if temporal dimension exists
        """
        # Check spec dimensions
        for dim in spec.dimensions:
            if self._is_temporal_column(dim.name):
                return True

        # Check DataFrame columns for temporal types
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                return True
            if self._is_temporal_column(col):
                return True

        return False

    def _get_temporal_column(
        self, df: pd.DataFrame, spec: AnalyticsInputSpec
    ) -> Optional[str]:
        """
        Get temporal column name from result.

        Args:
            df: Result DataFrame
            spec: Analytics specification

        Returns:
            Temporal column name or None
        """
        # Check spec dimensions first
        for dim in spec.dimensions:
            if self._is_temporal_column(dim.name) and dim.name in df.columns:
                return dim.name

        # Check DataFrame columns
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                return col
            if self._is_temporal_column(col):
                return col

        return None

    def _is_temporal_column(self, col_name: str) -> bool:
        """
        Check if column name indicates temporal data.

        Args:
            col_name: Column name

        Returns:
            True if column is temporal
        """
        col_lower = col_name.lower()
        return any(keyword in col_lower for keyword in self.TEMPORAL_KEYWORDS)

    def _is_chronologically_ordered(self, df: pd.DataFrame, temporal_col: str) -> bool:
        """
        Check if data is in chronological order.

        Args:
            df: Result DataFrame
            temporal_col: Name of temporal column

        Returns:
            True if data is chronologically ordered
        """
        if temporal_col not in df.columns:
            return False

        col_data = df[temporal_col]

        # Convert to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(col_data):
            try:
                col_data = pd.to_datetime(col_data)
            except Exception as e:
                logger.warning(f"Could not convert '{temporal_col}' to datetime: {e}")
                return False

        # Check if sorted
        is_sorted = col_data.is_monotonic_increasing

        if not is_sorted:
            logger.debug(
                f"Temporal column '{temporal_col}' is not chronologically ordered"
            )

        return is_sorted

    def _detect_temporal_gaps(self, df: pd.DataFrame, temporal_col: str) -> List[str]:
        """
        Detect gaps in temporal series.

        Args:
            df: Result DataFrame
            temporal_col: Name of temporal column

        Returns:
            List of gap descriptions
        """
        if temporal_col not in df.columns or len(df) < 2:
            return []

        col_data = df[temporal_col]

        # Convert to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(col_data):
            try:
                col_data = pd.to_datetime(col_data)
            except Exception:
                return []

        # Sort data
        col_data = col_data.sort_values()

        # Calculate differences between consecutive timestamps
        diffs = col_data.diff()[1:]  # Skip first NaT

        # Determine expected frequency
        median_diff = diffs.median()

        # Find gaps (differences significantly larger than median)
        threshold = median_diff * 1.5  # 50% larger than expected
        gaps = []

        for idx, diff in diffs.items():
            if diff > threshold:
                # Found a gap
                prev_idx = idx - 1 if idx > 0 else 0
                if prev_idx in col_data.index:
                    gap_desc = f"{col_data.iloc[prev_idx]} to {col_data.iloc[idx]}"
                    gaps.append(gap_desc)

                # Limit number of gaps reported
                if len(gaps) >= 10:
                    break

        return gaps

    def _validate_multi_series_structure(
        self, df: pd.DataFrame, spec: AnalyticsInputSpec
    ) -> Optional[str]:
        """
        Validate multi-series chart structure.

        For composed/stacked charts, we expect multiple series
        (e.g., multiple cities across time).

        Args:
            df: Result DataFrame
            spec: Analytics specification

        Returns:
            Warning message if structure is suboptimal, None otherwise
        """
        if not spec.dimensions or len(spec.dimensions) < 2:
            return "Multi-series chart should have at least 2 dimensions"

        # Check if we have multiple values per series
        # For example: multiple months per city
        temporal_dim = None
        categorical_dim = None

        for dim in spec.dimensions:
            if self._is_temporal_column(dim.name):
                temporal_dim = dim.name
            else:
                categorical_dim = dim.name

        if not temporal_dim or not categorical_dim:
            return None

        if categorical_dim not in df.columns:
            return None

        # Check number of series
        num_series = df[categorical_dim].nunique()
        if num_series < 2:
            return (
                f"Multi-series chart should have multiple series, "
                f"found only {num_series}"
            )

        return None
