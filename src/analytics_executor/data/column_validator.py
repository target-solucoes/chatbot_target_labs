"""
Column Validator module for Phase 4 - Analytics Executor Agent.

This module provides functionality for validating that required columns
exist in datasets before executing analytical operations.
"""

import logging
from typing import List, Set, Optional
import pandas as pd

from src.shared_lib.models.schema import AnalyticsInputSpec
from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class ColumnValidationError(Exception):
    """
    Exception raised when column validation fails.
    
    Attributes:
        message: Error message
        missing_columns: List of columns that are missing
        available_columns: List of columns available in the dataset
    """
    
    def __init__(
        self,
        message: str,
        missing_columns: List[str],
        available_columns: List[str]
    ):
        self.message = message
        self.missing_columns = missing_columns
        self.available_columns = available_columns
        super().__init__(self.message)
    
    def __str__(self) -> str:
        """Format error message with column details."""
        return (
            f"{self.message}\n"
            f"Missing columns: {', '.join(self.missing_columns)}\n"
            f"Available columns: {', '.join(self.available_columns)}"
        )


class ColumnValidator:
    """
    Validator for checking column existence in DataFrames.
    
    This class validates that all columns referenced in an AnalyticsInputSpec
    (metrics, dimensions, filters, sort) exist in the provided DataFrame.
    
    Features:
    - Validates metric columns
    - Validates dimension columns
    - Validates filter columns
    - Validates sort columns
    - Detailed error messages with available columns
    - Comprehensive logging
    
    Example:
        >>> validator = ColumnValidator()
        >>> df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        >>> spec = AnalyticsInputSpec(
        ...     chart_type="bar",
        ...     metrics=[MetricSpec(name="A", aggregation="sum")],
        ...     dimensions=[DimensionSpec(name="B")],
        ...     data_source="test"
        ... )
        >>> validator.validate_columns_exist(df, spec)  # No error
    """
    
    def __init__(self, case_sensitive: bool = True):
        """
        Initialize the ColumnValidator.
        
        Args:
            case_sensitive: Whether column name matching should be case-sensitive.
                          Default is True.
        """
        self.case_sensitive = case_sensitive
        logger.info(f"ColumnValidator initialized (case_sensitive={case_sensitive})")
    
    def validate_columns_exist(
        self,
        df: pd.DataFrame,
        spec: AnalyticsInputSpec
    ) -> None:
        """
        Validate that all columns referenced in the spec exist in the DataFrame.
        
        This method checks:
        1. All metric columns exist
        2. All dimension columns exist
        3. All filter columns exist
        4. Sort column exists (if specified)
        
        Args:
            df: DataFrame to validate against
            spec: Analytics specification containing column references
        
        Raises:
            ColumnValidationError: If any required columns are missing
            ValueError: If DataFrame is None or empty
        
        Example:
            >>> validator = ColumnValidator()
            >>> df = pd.DataFrame({'sales': [100, 200], 'region': ['A', 'B']})
            >>> spec = AnalyticsInputSpec(
            ...     chart_type="bar",
            ...     metrics=[MetricSpec(name="sales", aggregation="sum")],
            ...     dimensions=[DimensionSpec(name="region")],
            ...     data_source="test"
            ... )
            >>> validator.validate_columns_exist(df, spec)
        """
        # Validate inputs
        if df is None:
            raise ValueError("DataFrame cannot be None")
        
        if df.empty:
            logger.warning("DataFrame is empty - validation may not be meaningful")
        
        # Get available columns
        available_columns = set(df.columns)
        if not self.case_sensitive:
            available_columns = {col.lower() for col in available_columns}
        
        logger.debug(f"Validating columns against DataFrame with {len(available_columns)} columns")
        
        # Collect all missing columns
        missing_columns = []
        
        # Validate metric columns
        missing_metrics = self._validate_metrics(spec, available_columns)
        missing_columns.extend(missing_metrics)
        
        # Validate dimension columns
        missing_dimensions = self._validate_dimensions(spec, available_columns)
        missing_columns.extend(missing_dimensions)
        
        # Validate filter columns
        missing_filters = self._validate_filters(spec, available_columns)
        missing_columns.extend(missing_filters)
        
        # Validate sort column
        missing_sort = self._validate_sort(spec, available_columns)
        if missing_sort:
            missing_columns.append(missing_sort)
        
        # Raise error if any columns are missing
        if missing_columns:
            error_msg = f"Column validation failed: {len(missing_columns)} required column(s) not found in dataset"
            logger.error(error_msg)
            raise ColumnValidationError(
                message=error_msg,
                missing_columns=missing_columns,
                available_columns=sorted(df.columns.tolist())
            )
        
        logger.info("Column validation successful - all required columns exist")
    
    def _validate_metrics(
        self,
        spec: AnalyticsInputSpec,
        available_columns: Set[str]
    ) -> List[str]:
        """
        Validate that all metric columns exist.
        
        Args:
            spec: Analytics specification
            available_columns: Set of available column names
        
        Returns:
            List of missing metric column names
        """
        missing = []
        
        for metric in spec.metrics:
            col_name = metric.name if self.case_sensitive else metric.name.lower()
            
            if col_name not in available_columns:
                logger.warning(f"Metric column not found: {metric.name}")
                missing.append(metric.name)
        
        if missing:
            logger.error(f"Missing metric columns: {missing}")
        else:
            logger.debug(f"All {len(spec.metrics)} metric columns validated")
        
        return missing
    
    def _validate_dimensions(
        self,
        spec: AnalyticsInputSpec,
        available_columns: Set[str]
    ) -> List[str]:
        """
        Validate that all dimension columns exist.
        
        Args:
            spec: Analytics specification
            available_columns: Set of available column names
        
        Returns:
            List of missing dimension column names
        """
        missing = []
        
        for dimension in spec.dimensions:
            col_name = dimension.name if self.case_sensitive else dimension.name.lower()
            
            if col_name not in available_columns:
                logger.warning(f"Dimension column not found: {dimension.name}")
                missing.append(dimension.name)
        
        if missing:
            logger.error(f"Missing dimension columns: {missing}")
        else:
            logger.debug(f"All {len(spec.dimensions)} dimension columns validated")
        
        return missing
    
    def _validate_filters(
        self,
        spec: AnalyticsInputSpec,
        available_columns: Set[str]
    ) -> List[str]:
        """
        Validate that all filter columns exist.
        
        Args:
            spec: Analytics specification
            available_columns: Set of available column names
        
        Returns:
            List of missing filter column names
        """
        missing = []
        
        for filter_col in spec.filters.keys():
            col_name = filter_col if self.case_sensitive else filter_col.lower()
            
            if col_name not in available_columns:
                logger.warning(f"Filter column not found: {filter_col}")
                missing.append(filter_col)
        
        if missing:
            logger.error(f"Missing filter columns: {missing}")
        else:
            logger.debug(f"All {len(spec.filters)} filter columns validated")
        
        return missing
    
    def _validate_sort(
        self,
        spec: AnalyticsInputSpec,
        available_columns: Set[str]
    ) -> Optional[str]:
        """
        Validate that sort column exists (if specified).
        
        Args:
            spec: Analytics specification
            available_columns: Set of available column names
        
        Returns:
            Missing sort column name, or None if valid
        """
        if not spec.sort or not spec.sort.by:
            logger.debug("No sort column specified")
            return None
        
        sort_col = spec.sort.by if self.case_sensitive else spec.sort.by.lower()
        
        if sort_col not in available_columns:
            logger.warning(f"Sort column not found: {spec.sort.by}")
            return spec.sort.by
        
        logger.debug(f"Sort column validated: {spec.sort.by}")
        return None
    
    def get_missing_columns(
        self,
        df: pd.DataFrame,
        spec: AnalyticsInputSpec
    ) -> List[str]:
        """
        Get list of missing columns without raising an error.
        
        This is useful for checking column availability without triggering
        exception handling.
        
        Args:
            df: DataFrame to check
            spec: Analytics specification
        
        Returns:
            List of missing column names (empty if all columns exist)
        
        Example:
            >>> validator = ColumnValidator()
            >>> df = pd.DataFrame({'A': [1, 2]})
            >>> spec = AnalyticsInputSpec(
            ...     chart_type="bar",
            ...     metrics=[MetricSpec(name="B", aggregation="sum")],
            ...     dimensions=[],
            ...     data_source="test"
            ... )
            >>> missing = validator.get_missing_columns(df, spec)
            >>> print(missing)
            ['B']
        """
        try:
            self.validate_columns_exist(df, spec)
            return []
        except ColumnValidationError as e:
            return e.missing_columns
    
    def validate_column_list(
        self,
        df: pd.DataFrame,
        columns: List[str],
        context: str = "columns"
    ) -> None:
        """
        Validate that a list of columns exists in the DataFrame.
        
        This is a simpler validation method for checking arbitrary column lists
        without requiring a full AnalyticsInputSpec.
        
        Args:
            df: DataFrame to validate against
            columns: List of column names to check
            context: Description of what these columns are for (for error messages)
        
        Raises:
            ColumnValidationError: If any columns are missing
        
        Example:
            >>> validator = ColumnValidator()
            >>> df = pd.DataFrame({'A': [1], 'B': [2]})
            >>> validator.validate_column_list(df, ['A', 'B'], context="required")
        """
        available_columns = set(df.columns)
        if not self.case_sensitive:
            available_columns = {col.lower() for col in available_columns}
            columns = [col.lower() for col in columns]
        
        missing = [col for col in columns if col not in available_columns]
        
        if missing:
            error_msg = f"Validation failed for {context}: {len(missing)} column(s) not found"
            logger.error(error_msg)
            raise ColumnValidationError(
                message=error_msg,
                missing_columns=missing,
                available_columns=sorted(df.columns.tolist())
            )
        
        logger.debug(f"All {len(columns)} {context} validated successfully")



