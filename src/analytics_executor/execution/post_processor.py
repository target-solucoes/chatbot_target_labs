"""
Post-Processor for Analytics Executor Agent.

This module provides post-processing capabilities for query results, including:
- Column aliasing
- Numeric formatting
- Null value handling
- Result validation
"""

import logging
from typing import Optional, Dict, Any, List
import pandas as pd
import numpy as np

from src.shared_lib.models.schema import AnalyticsInputSpec, MetricSpec, DimensionSpec
from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class PostProcessorError(Exception):
    """Exception raised when post-processing fails."""
    
    def __init__(self, message: str, operation: Optional[str] = None, original_error: Optional[Exception] = None):
        """
        Initialize post-processor error.
        
        Args:
            message: Error message
            operation: Operation that failed
            original_error: Original exception
        """
        self.message = message
        self.operation = operation
        self.original_error = original_error
        super().__init__(self._format_message())
    
    def _format_message(self) -> str:
        """Format error message with context."""
        msg = f"Post-processing failed: {self.message}"
        if self.operation:
            msg = f"[{self.operation}] {msg}"
        if self.original_error:
            msg += f"\nOriginal error: {str(self.original_error)}"
        return msg


class PostProcessor:
    """
    Post-processor for query execution results.
    
    This class provides comprehensive post-processing of query results:
    - Applies column aliases from specification
    - Formats numeric values for display
    - Handles null/missing values
    - Validates result structure
    - Ensures data type consistency
    
    Post-processing is applied after query execution and before result formatting.
    
    Example:
        >>> processor = PostProcessor()
        >>> result_df = pd.DataFrame({'sales': [100, 200], 'region': ['A', 'B']})
        >>> spec = AnalyticsInputSpec(
        ...     metrics=[MetricSpec(name='sales', aggregation='sum', alias='Total Sales')],
        ...     dimensions=[DimensionSpec(name='region', alias='Region')],
        ...     ...
        ... )
        >>> processed_df = processor.process(result_df, spec)
        >>> print(processed_df.columns)
        Index(['Region', 'Total Sales'], dtype='object')
    """
    
    def __init__(
        self,
        decimal_places: int = 2,
        handle_nulls: bool = True,
        validate_output: bool = True
    ):
        """
        Initialize the post-processor.
        
        Args:
            decimal_places: Number of decimal places for numeric formatting (default: 2)
            handle_nulls: Whether to handle null values (default: True)
            validate_output: Whether to validate output structure (default: True)
        """
        self.decimal_places = decimal_places
        self.handle_nulls = handle_nulls
        self.validate_output = validate_output
        
        logger.info(
            f"PostProcessor initialized: decimal_places={decimal_places}, "
            f"handle_nulls={handle_nulls}, validate_output={validate_output}"
        )
    
    def process(self, df: pd.DataFrame, spec: AnalyticsInputSpec) -> pd.DataFrame:
        """
        Process DataFrame with all post-processing operations.
        
        Processing steps:
        1. Validate input DataFrame
        2. Apply column aliases
        3. Handle null values
        4. Format numeric columns
        5. Validate output structure
        
        Args:
            df: Result DataFrame from query execution
            spec: Analytics specification with metadata
            
        Returns:
            Processed DataFrame ready for output formatting
            
        Raises:
            PostProcessorError: If processing fails
            ValueError: If inputs are invalid
            
        Example:
            >>> processor = PostProcessor()
            >>> df = pd.DataFrame({
            ...     'Valor_Vendido': [1000.5, 2000.75],
            ...     'Estado': ['SP', 'RJ']
            ... })
            >>> spec = AnalyticsInputSpec(
            ...     metrics=[MetricSpec(name='Valor_Vendido', aggregation='sum', alias='Total')],
            ...     dimensions=[DimensionSpec(name='Estado', alias='State')],
            ...     ...
            ... )
            >>> result = processor.process(df, spec)
        """
        # Validate inputs
        if df is None:
            raise ValueError("DataFrame cannot be None")
        
        if spec is None:
            raise ValueError("Specification cannot be None")
        
        logger.debug(f"Post-processing DataFrame: {len(df)} rows, {len(df.columns)} columns")
        
        try:
            # Start with a copy to avoid modifying original
            result_df = df.copy()
            
            # Step 1: Validate input
            if self.validate_output:
                self._validate_input(result_df)
            
            # Step 2: Apply column aliases
            result_df = self._apply_aliases(result_df, spec)
            logger.debug("Column aliases applied")
            
            # Step 3: Handle null values
            if self.handle_nulls:
                result_df = self._handle_nulls(result_df)
                logger.debug("Null values handled")
            
            # Step 4: Format numeric columns
            # Note: Skip formatting to preserve numeric types for downstream processing
            # Formatting should be done at presentation layer if needed
            logger.debug("Numeric formatting skipped (preserved for downstream)")
            
            # Step 5: Validate output
            if self.validate_output:
                self._validate_output(result_df)
            
            logger.info(f"Post-processing completed: {len(result_df)} rows, {len(result_df.columns)} columns")
            
            return result_df
            
        except PostProcessorError:
            # Re-raise our custom errors
            raise
            
        except Exception as e:
            error_msg = f"Unexpected error during post-processing: {str(e)}"
            logger.error(error_msg)
            raise PostProcessorError(
                error_msg,
                operation="process",
                original_error=e
            )
    
    def _apply_aliases(self, df: pd.DataFrame, spec: AnalyticsInputSpec) -> pd.DataFrame:
        """
        Apply column aliases from specification.
        
        This method renames columns based on aliases defined in metrics and dimensions.
        Only applies aliases that are meaningfully different from original names.
        
        Args:
            df: DataFrame to process
            spec: Specification with alias definitions
            
        Returns:
            DataFrame with aliased columns
            
        Raises:
            PostProcessorError: If aliasing fails
        """
        try:
            rename_dict = {}
            
            # Build rename dictionary from metrics
            for metric in spec.metrics:
                if metric.alias and metric.name in df.columns:
                    # Only rename if alias is meaningfully different
                    if metric.alias != metric.name and metric.alias.lower() != metric.name.lower():
                        rename_dict[metric.name] = metric.alias
            
            # Build rename dictionary from dimensions
            for dim in spec.dimensions:
                if dim.alias and dim.name in df.columns:
                    # Only rename if alias is meaningfully different
                    if dim.alias != dim.name and dim.alias.lower() != dim.name.lower():
                        rename_dict[dim.name] = dim.alias
            
            # Apply renaming
            if rename_dict:
                logger.debug(f"Applying column aliases: {rename_dict}")
                df = df.rename(columns=rename_dict)
            else:
                logger.debug("No column aliases to apply")
            
            return df
            
        except Exception as e:
            error_msg = f"Error applying aliases: {str(e)}"
            logger.error(error_msg)
            raise PostProcessorError(
                error_msg,
                operation="apply_aliases",
                original_error=e
            )
    
    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle null/missing values in DataFrame.
        
        Strategy:
        - For numeric columns: Keep NaN as is (Plotly handles them well)
        - For string columns: Replace None/NaN with empty string
        - Log warning if many nulls are present
        
        Args:
            df: DataFrame to process
            
        Returns:
            DataFrame with null values handled
        """
        try:
            # Check for nulls
            null_counts = df.isnull().sum()
            total_nulls = null_counts.sum()
            
            if total_nulls == 0:
                logger.debug("No null values found")
                return df
            
            # Log null statistics
            null_columns = null_counts[null_counts > 0]
            logger.debug(f"Null values found in columns: {null_columns.to_dict()}")
            
            # Warn if many nulls
            null_percentage = (total_nulls / (len(df) * len(df.columns))) * 100
            if null_percentage > 10:
                logger.warning(
                    f"High percentage of null values: {null_percentage:.1f}% "
                    f"({total_nulls} nulls in {len(df) * len(df.columns)} cells)"
                )
            
            # Handle string columns
            for col in df.columns:
                if df[col].dtype == object:
                    # Replace None/NaN with empty string for object columns
                    df[col] = df[col].fillna("")
            
            # For numeric columns, keep NaN as is (Plotly handles them)
            logger.debug("Null values handled")
            
            return df
            
        except Exception as e:
            error_msg = f"Error handling null values: {str(e)}"
            logger.error(error_msg)
            raise PostProcessorError(
                error_msg,
                operation="handle_nulls",
                original_error=e
            )
    
    def _format_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Format numeric columns to specified decimal places.
        
        Note: This method is currently not used to preserve numeric types.
        If needed, it can be called separately for display formatting.
        
        Args:
            df: DataFrame to format
            
        Returns:
            DataFrame with formatted numeric columns
        """
        try:
            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    # Round to specified decimal places
                    df[col] = df[col].round(self.decimal_places)
            
            logger.debug(f"Numeric columns formatted to {self.decimal_places} decimal places")
            return df
            
        except Exception as e:
            error_msg = f"Error formatting numeric columns: {str(e)}"
            logger.error(error_msg)
            raise PostProcessorError(
                error_msg,
                operation="format_numeric",
                original_error=e
            )
    
    def _validate_input(self, df: pd.DataFrame) -> None:
        """
        Validate input DataFrame structure.
        
        Args:
            df: DataFrame to validate
            
        Raises:
            PostProcessorError: If validation fails
        """
        try:
            # Check not empty
            if df.empty:
                logger.warning("DataFrame is empty")
            
            # Check has columns
            if len(df.columns) == 0:
                raise PostProcessorError(
                    "DataFrame has no columns",
                    operation="validate_input"
                )
            
            # Check for duplicate column names
            duplicate_cols = df.columns[df.columns.duplicated()].tolist()
            if duplicate_cols:
                raise PostProcessorError(
                    f"DataFrame has duplicate column names: {duplicate_cols}",
                    operation="validate_input"
                )
            
            logger.debug("Input validation passed")
            
        except PostProcessorError:
            raise
            
        except Exception as e:
            error_msg = f"Error during input validation: {str(e)}"
            logger.error(error_msg)
            raise PostProcessorError(
                error_msg,
                operation="validate_input",
                original_error=e
            )
    
    def _validate_output(self, df: pd.DataFrame) -> None:
        """
        Validate output DataFrame structure.
        
        Args:
            df: DataFrame to validate
            
        Raises:
            PostProcessorError: If validation fails
        """
        try:
            # Check not None
            if df is None:
                raise PostProcessorError(
                    "Output DataFrame is None",
                    operation="validate_output"
                )
            
            # Check is DataFrame
            if not isinstance(df, pd.DataFrame):
                raise PostProcessorError(
                    f"Output is not a DataFrame: {type(df)}",
                    operation="validate_output"
                )
            
            # Check has columns
            if len(df.columns) == 0:
                raise PostProcessorError(
                    "Output DataFrame has no columns",
                    operation="validate_output"
                )
            
            # Warn if empty
            if df.empty:
                logger.warning("Output DataFrame is empty - query returned no results")
            
            logger.debug("Output validation passed")
            
        except PostProcessorError:
            raise
            
        except Exception as e:
            error_msg = f"Error during output validation: {str(e)}"
            logger.error(error_msg)
            raise PostProcessorError(
                error_msg,
                operation="validate_output",
                original_error=e
            )
    
    def get_column_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get information about DataFrame columns.
        
        This can be useful for debugging and understanding the result structure.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with column information
            
        Example:
            >>> processor = PostProcessor()
            >>> df = pd.DataFrame({'A': [1, 2], 'B': ['x', 'y']})
            >>> info = processor.get_column_info(df)
            >>> print(info['columns'])
            ['A', 'B']
        """
        try:
            return {
                "columns": df.columns.tolist(),
                "dtypes": df.dtypes.to_dict(),
                "row_count": len(df),
                "column_count": len(df.columns),
                "null_counts": df.isnull().sum().to_dict(),
                "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024
            }
        except Exception as e:
            logger.warning(f"Error getting column info: {str(e)}")
            return {
                "columns": df.columns.tolist() if hasattr(df, 'columns') else [],
                "error": str(e)
            }


def post_process_result(df: pd.DataFrame, spec: AnalyticsInputSpec) -> pd.DataFrame:
    """
    Convenience function to post-process query results.
    
    This function creates a PostProcessor instance and processes the DataFrame,
    providing a simple one-line interface for post-processing.
    
    Args:
        df: Result DataFrame to process
        spec: Analytics specification
        
    Returns:
        Processed DataFrame
        
    Raises:
        PostProcessorError: If processing fails
        
    Example:
        >>> from src.analytics_executor.execution.post_processor import post_process_result
        >>> df = pd.DataFrame(...)
        >>> spec = AnalyticsInputSpec(...)
        >>> processed = post_process_result(df, spec)
    """
    processor = PostProcessor()
    return processor.process(df, spec)

