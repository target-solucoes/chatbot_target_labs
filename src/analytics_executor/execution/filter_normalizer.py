"""
Filter Normalizer - Scalable and Dynamic Filter Value Normalization

This module provides intelligent normalization of filter values to ensure compatibility
with actual dataset values, handling:
- Case sensitivity mismatches (e.g., 'Joinville' vs 'JOINVILLE')
- Type conversions (strings to datetime, etc.)
- List filters (IN clauses)
- Range filters (BETWEEN clauses)

All normalization is done dynamically based on the actual data, with NO hardcoding.
"""

import logging
import pandas as pd
from typing import Dict, Any, List, Optional, Union
from datetime import datetime

logger = logging.getLogger(__name__)


class FilterNormalizer:
    """
    Normalizes filter values to match actual dataset values.

    Features:
    - Automatic case detection and normalization for string columns
    - Type conversion (string to datetime, int, etc.)
    - Preserves performance with caching
    - No hardcoded column names
    """

    def __init__(self, df: pd.DataFrame, case_sensitive: bool = False):
        """
        Initialize normalizer with dataset.

        Args:
            df: DataFrame to normalize filters against
            case_sensitive: If True, preserves original case (default: False for auto-detection)
        """
        self.df = df
        self.case_sensitive = case_sensitive
        self._column_metadata = {}  # Cache for column metadata

        logger.debug(f"FilterNormalizer initialized: case_sensitive={case_sensitive}")

    def normalize_filters(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize all filter values.

        Args:
            filters: Dictionary of column_name -> filter_value

        Returns:
            Dictionary with normalized filter values
        """
        if not filters:
            return filters

        normalized = {}

        for column, value in filters.items():
            if column not in self.df.columns:
                logger.warning(
                    f"[FilterNormalizer] Column '{column}' not in dataset, "
                    f"skipping normalization"
                )
                normalized[column] = value
                continue

            try:
                normalized_value = self._normalize_column_value(column, value)

                # Log if value was changed
                if normalized_value != value:
                    logger.info(
                        f"[FilterNormalizer] Normalized filter: '{column}' "
                        f"'{value}' -> '{normalized_value}'"
                    )

                normalized[column] = normalized_value

            except Exception as e:
                logger.warning(
                    f"[FilterNormalizer] Failed to normalize '{column}': {e}, "
                    f"using original value"
                )
                normalized[column] = value

        return normalized

    def _normalize_column_value(self, column: str, value: Any) -> Any:
        """
        Normalize a single filter value for a column.

        Args:
            column: Column name
            value: Filter value (can be single value, list, or dict for operators)

        Returns:
            Normalized value
        """
        # Get column metadata (cached)
        if column not in self._column_metadata:
            self._column_metadata[column] = self._analyze_column(column)

        metadata = self._column_metadata[column]

        # Handle different value types
        if isinstance(value, list):
            # Check if this is a temporal range (2 dates representing start/end)
            if self._is_temporal_range(value, metadata):
                logger.info(
                    f"[FilterNormalizer] Detected temporal range for '{column}': "
                    f"{value[0]} to {value[1]}"
                )
                # Convert to range format
                return {
                    'between': [
                        self._normalize_single_value(column, value[0], metadata),
                        self._normalize_single_value(column, value[1], metadata)
                    ]
                }
            else:
                # List filter (IN clause)
                return [self._normalize_single_value(column, v, metadata) for v in value]

        elif isinstance(value, dict):
            # Operator filter (e.g., {'operator': '>=', 'value': 100})
            if 'operator' in value and 'value' in value:
                return {
                    'operator': value['operator'],
                    'value': self._normalize_single_value(column, value['value'], metadata)
                }
            # Range filter (e.g., {'between': [start, end]})
            elif 'between' in value:
                return {
                    'between': [
                        self._normalize_single_value(column, value['between'][0], metadata),
                        self._normalize_single_value(column, value['between'][1], metadata)
                    ]
                }
            else:
                # Unknown dict structure, return as is
                return value

        else:
            # Single value filter
            return self._normalize_single_value(column, value, metadata)

    def _normalize_single_value(self, column: str, value: Any, metadata: Dict[str, Any]) -> Any:
        """
        Normalize a single atomic value.

        Args:
            column: Column name
            value: Single value to normalize
            metadata: Column metadata from _analyze_column

        Returns:
            Normalized value
        """
        dtype = metadata['dtype']

        # Datetime conversion
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return self._to_datetime(value)

        # String normalization (case handling)
        elif pd.api.types.is_string_dtype(dtype) or pd.api.types.is_categorical_dtype(dtype):
            return self._normalize_string(column, value, metadata)

        # Numeric types - try conversion
        elif pd.api.types.is_numeric_dtype(dtype):
            return self._to_numeric(value, dtype)

        # Default: return as is
        else:
            return value

    def _normalize_string(self, column: str, value: Any, metadata: Dict[str, Any]) -> str:
        """
        Normalize string value based on column's dominant case.

        Args:
            column: Column name
            value: String value to normalize
            metadata: Column metadata

        Returns:
            Normalized string
        """
        if not isinstance(value, str):
            return value

        if self.case_sensitive:
            return value

        # Use detected dominant case
        dominant_case = metadata.get('dominant_case', 'original')

        if dominant_case == 'upper':
            return value.upper()
        elif dominant_case == 'lower':
            return value.lower()
        elif dominant_case == 'title':
            return value.title()
        else:
            # Check if exact value exists in column (case-insensitive search)
            return self._find_exact_match(column, value)

    def _find_exact_match(self, column: str, value: str) -> str:
        """
        Find exact case match in column values.

        Args:
            column: Column name
            value: Value to find

        Returns:
            Exact match from dataset, or uppercased value as fallback
        """
        try:
            # Get unique values (limited to avoid memory issues)
            unique_values = self.df[column].unique()

            # Case-insensitive search
            value_lower = value.lower()
            for unique_val in unique_values:
                if isinstance(unique_val, str) and unique_val.lower() == value_lower:
                    return unique_val

            # No match found - use uppercase as most common fallback
            logger.debug(
                f"[FilterNormalizer] No exact match for '{value}' in '{column}', "
                f"using uppercase"
            )
            return value.upper()

        except Exception as e:
            logger.warning(
                f"[FilterNormalizer] Error finding exact match for '{column}': {e}"
            )
            return value.upper()

    def _to_datetime(self, value: Any) -> pd.Timestamp:
        """
        Convert value to datetime.

        Args:
            value: Value to convert (string, datetime, or timestamp)

        Returns:
            pandas Timestamp
        """
        if isinstance(value, pd.Timestamp):
            return value

        if isinstance(value, datetime):
            return pd.Timestamp(value)

        if isinstance(value, str):
            try:
                return pd.to_datetime(value)
            except Exception as e:
                logger.warning(f"[FilterNormalizer] Failed to parse datetime '{value}': {e}")
                return value

        return value

    def _to_numeric(self, value: Any, target_dtype) -> Union[int, float, Any]:
        """
        Convert value to numeric type.

        Args:
            value: Value to convert
            target_dtype: Target numeric dtype

        Returns:
            Converted numeric value
        """
        if pd.isna(value):
            return value

        try:
            if pd.api.types.is_integer_dtype(target_dtype):
                return int(value)
            elif pd.api.types.is_float_dtype(target_dtype):
                return float(value)
            else:
                return value
        except (ValueError, TypeError):
            return value

    def _analyze_column(self, column: str) -> Dict[str, Any]:
        """
        Analyze column to determine normalization strategy.

        Args:
            column: Column name

        Returns:
            Dictionary with column metadata:
            - dtype: Column data type
            - dominant_case: Dominant case for string columns ('upper', 'lower', 'title', 'mixed')
        """
        col_data = self.df[column]
        dtype = col_data.dtype

        metadata = {'dtype': dtype}

        # For string/categorical columns, detect dominant case
        if pd.api.types.is_string_dtype(dtype) or pd.api.types.is_categorical_dtype(dtype):
            metadata['dominant_case'] = self._detect_dominant_case(col_data)

        return metadata

    def _detect_dominant_case(self, series: pd.Series) -> str:
        """
        Detect dominant case in a string series.

        Args:
            series: Pandas Series of strings

        Returns:
            'upper', 'lower', 'title', or 'mixed'
        """
        # Sample to avoid processing huge datasets
        sample = series.dropna().head(100)

        if len(sample) == 0:
            return 'original'

        # Count case patterns
        upper_count = 0
        lower_count = 0
        title_count = 0

        for val in sample:
            if not isinstance(val, str):
                continue

            if val.isupper():
                upper_count += 1
            elif val.islower():
                lower_count += 1
            elif val.istitle():
                title_count += 1

        total = upper_count + lower_count + title_count

        if total == 0:
            return 'original'

        # Determine dominant case (>70% threshold)
        threshold = 0.7

        if upper_count / total > threshold:
            return 'upper'
        elif lower_count / total > threshold:
            return 'lower'
        elif title_count / total > threshold:
            return 'title'
        else:
            return 'mixed'

    def _is_temporal_range(self, value_list: List[Any], metadata: Dict[str, Any]) -> bool:
        """
        Detect if a list represents a temporal range (start/end dates).

        A list is considered a temporal range if:
        1. It has exactly 2 elements
        2. The column is datetime type
        3. The values can be converted to dates
        4. The second value is greater than the first (valid range)

        Args:
            value_list: List of values to check
            metadata: Column metadata with dtype info

        Returns:
            True if this is a temporal range, False otherwise
        """
        # Must have exactly 2 elements
        if not isinstance(value_list, list) or len(value_list) != 2:
            return False

        # Column must be datetime type
        dtype = metadata.get('dtype')
        if not pd.api.types.is_datetime64_any_dtype(dtype):
            return False

        try:
            # Try to convert both values to datetime
            start = self._to_datetime(value_list[0])
            end = self._to_datetime(value_list[1])

            # Valid range: end > start
            if end > start:
                logger.debug(
                    f"[FilterNormalizer] Detected valid temporal range: "
                    f"{start} to {end}"
                )
                return True
            else:
                logger.debug(
                    f"[FilterNormalizer] Invalid range order: {start} >= {end}, "
                    f"treating as IN clause"
                )
                return False

        except Exception as e:
            # If conversion fails, not a temporal range
            logger.debug(
                f"[FilterNormalizer] Failed to parse as temporal range: {e}, "
                f"treating as IN clause"
            )
            return False


def normalize_filters(df: pd.DataFrame, filters: Dict[str, Any], case_sensitive: bool = False) -> Dict[str, Any]:
    """
    Convenience function to normalize filters.

    Args:
        df: DataFrame to normalize against
        filters: Dictionary of filters
        case_sensitive: If True, preserves original case

    Returns:
        Normalized filters dictionary
    """
    normalizer = FilterNormalizer(df, case_sensitive=case_sensitive)
    return normalizer.normalize_filters(filters)
