"""
Filter Validator for validating filter columns and values.

This module provides the FilterValidator class responsible for validating
filter specifications against dataset schemas and aliases.
"""

import logging
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path
import pandas as pd
from cachetools import TTLCache
from rapidfuzz import fuzz, process

from src.graphic_classifier.tools.alias_mapper import AliasMapper
from src.shared_lib.data.dataset_column_extractor import DatasetColumnExtractor
from src.filter_classifier.core.settings import FUZZY_THRESHOLD, DATASET_SAMPLE_SIZE

logger = logging.getLogger(__name__)


class FilterValidator:
    """
    Validates filter specifications against dataset schema.

    This class provides validation for:
    - Column existence (with alias resolution)
    - Value validity for categorical columns
    - Type compatibility
    """

    def __init__(
        self,
        alias_mapper: AliasMapper,
        dataset_path: str,
        fuzzy_threshold: float = FUZZY_THRESHOLD
    ):
        """
        Initialize the FilterValidator.

        Args:
            alias_mapper: AliasMapper instance for column resolution
            dataset_path: Path to the dataset file
            fuzzy_threshold: Threshold for fuzzy matching (0.0 to 1.0)
        """
        self.alias_mapper = alias_mapper
        self.dataset_path = Path(dataset_path)
        self.fuzzy_threshold = fuzzy_threshold

        # Initialize dataset extractor
        self.dataset_extractor = DatasetColumnExtractor()

        # Cache for column metadata
        self._column_metadata: Optional[Dict[str, Any]] = None
        self._available_columns: Optional[List[str]] = None

        # TTL Cache for value validation (5 minutes)
        self._value_cache: TTLCache = TTLCache(maxsize=1000, ttl=300)

        logger.info(f"[FilterValidator] Initialized with dataset: {self.dataset_path}")

    def validate_columns_exist(
        self,
        filter_columns: List[str]
    ) -> Tuple[List[str], List[str]]:
        """
        Validate that filter columns exist in the dataset.

        Uses AliasMapper to resolve column names and fuzzy matching
        to find the best matches.

        Args:
            filter_columns: List of column names from filter specification

        Returns:
            Tuple of (valid_columns, invalid_columns)

        Examples:
            >>> validator = FilterValidator(mapper, "data.parquet")
            >>> valid, invalid = validator.validate_columns_exist(["UF", "Ano"])
            >>> print(valid)
            ['UF_Cliente', 'Ano']
        """
        logger.info(f"[FilterValidator] Validating {len(filter_columns)} filter columns")

        # Get available columns
        available = self._get_available_columns()

        valid_columns = []
        invalid_columns = []

        for column in filter_columns:
            # Try exact match first
            if column in available:
                valid_columns.append(column)
                logger.debug(f"[FilterValidator] Column '{column}' found (exact match)")
                continue

            # Try alias resolution
            resolved = self.alias_mapper.resolve(column)
            if resolved and resolved in available:
                valid_columns.append(resolved)
                logger.debug(f"[FilterValidator] Column '{column}' resolved to '{resolved}'")
                continue

            # Column not found
            invalid_columns.append(column)
            logger.warning(f"[FilterValidator] Column '{column}' not found in dataset")

        logger.info(
            f"[FilterValidator] Validation result: {len(valid_columns)} valid, "
            f"{len(invalid_columns)} invalid"
        )

        return valid_columns, invalid_columns

    def validate_value_exists(
        self,
        column: str,
        value: Any,
        use_cache: bool = True
    ) -> bool:
        """
        Validate that a single value exists in the column.

        NEW METHOD: Optimized for single value validation with caching.

        Args:
            column: Column name
            value: Single value to validate
            use_cache: Whether to use TTL cache (default: True)

        Returns:
            True if value exists in column, False otherwise

        Examples:
            >>> validator.validate_value_exists("Des_Grupo_Produto", "ADESIVOS")
            True
            >>> validator.validate_value_exists("Des_Linha_Produto", "PRODUTOS")
            False
        """
        # Check cache first
        cache_key = f"{column}:{value}"
        if use_cache and cache_key in self._value_cache:
            logger.debug(f"[FilterValidator] Cache hit for '{column}':'{value}'")
            return self._value_cache[cache_key]

        # Validate value
        df = self._load_dataset_sample()
        result = self._validate_single_value(column, value, df)

        # Store in cache
        if use_cache:
            self._value_cache[cache_key] = result
            logger.debug(f"[FilterValidator] Cached result for '{column}':'{value}' = {result}")

        return result

    def validate_column_values(
        self,
        column: str,
        values: Any,
        df: Optional[pd.DataFrame] = None
    ) -> bool:
        """
        Validate that filter values exist in the column.

        For categorical columns, checks if values exist in the dataset.
        For numeric columns, performs basic type checking.

        Args:
            column: Column name
            values: Value or list of values to validate
            df: Optional DataFrame (if not provided, loads from dataset_path)

        Returns:
            True if values are valid, False otherwise

        Examples:
            >>> validator.validate_column_values("UF_Cliente", "SP")
            True
            >>> validator.validate_column_values("UF_Cliente", "XX")
            False
        """
        logger.debug(f"[FilterValidator] Validating values for column '{column}'")

        # Load DataFrame if not provided
        if df is None:
            df = self._load_dataset_sample()

        if column not in df.columns:
            logger.warning(f"[FilterValidator] Column '{column}' not in DataFrame")
            return False

        # Handle list of values
        if isinstance(values, list):
            return all(self._validate_single_value(column, v, df) for v in values)

        # Handle dict (complex operators like 'between')
        if isinstance(values, dict):
            if "between" in values:
                start, end = values["between"]
                return self._validate_single_value(column, start, df) and \
                       self._validate_single_value(column, end, df)
            elif "in" in values:
                return all(self._validate_single_value(column, v, df) for v in values["in"])
            else:
                logger.warning(f"[FilterValidator] Unknown operator in values: {values}")
                return True  # Assume valid for unknown operators

        # Handle single value
        return self._validate_single_value(column, values, df)

    def _validate_single_value(self, column: str, value: Any, df: pd.DataFrame) -> bool:
        """
        Validate a single value against column data.

        Args:
            column: Column name
            value: Single value to validate
            df: DataFrame

        Returns:
            True if value is valid
        """
        column_type = self.infer_column_type(column, df)

        if column_type == "categorical":
            # For categorical, check if value exists
            unique_values = df[column].unique()

            # Case-insensitive comparison for string columns
            if pd.api.types.is_string_dtype(df[column]):
                # Normalize both filter value and unique values to uppercase
                value_upper = str(value).upper()
                unique_values_upper = [str(v).upper() for v in unique_values]
                is_valid = value_upper in unique_values_upper

                if not is_valid:
                    logger.debug(
                        f"[FilterValidator] Value '{value}' (normalized: '{value_upper}') "
                        f"not found in column '{column}' (has {len(unique_values)} unique values)"
                    )
                else:
                    logger.debug(
                        f"[FilterValidator] Value '{value}' matched in column '{column}' "
                        f"(case-insensitive comparison)"
                    )
            else:
                # Non-string categorical: exact match
                is_valid = value in unique_values

                if not is_valid:
                    logger.debug(
                        f"[FilterValidator] Value '{value}' not found in column '{column}' "
                        f"(has {len(unique_values)} unique values)"
                    )

            return is_valid

        elif column_type in ["numeric", "date"]:
            # For numeric/date, just check type compatibility
            try:
                if column_type == "numeric":
                    float(value)
                return True
            except (ValueError, TypeError):
                logger.warning(f"[FilterValidator] Value '{value}' is not compatible with {column_type}")
                return False

        return True

    def resolve_column_aliases(
        self,
        mentioned_columns: List[str]
    ) -> Dict[str, str]:
        """
        Resolve column aliases to actual column names.

        Args:
            mentioned_columns: List of column references from query

        Returns:
            Dictionary mapping query terms to resolved column names

        Examples:
            >>> validator.resolve_column_aliases(["estado", "ano"])
            {'estado': 'UF_Cliente', 'ano': 'Ano'}
        """
        logger.info(f"[FilterValidator] Resolving aliases for {len(mentioned_columns)} columns")

        resolved = {}
        for column in mentioned_columns:
            resolved_name = self.alias_mapper.resolve(column)
            if resolved_name:
                resolved[column] = resolved_name
                logger.debug(f"[FilterValidator] Resolved '{column}' → '{resolved_name}'")
            else:
                # Try exact match with available columns
                available = self._get_available_columns()
                if column in available:
                    resolved[column] = column
                    logger.debug(f"[FilterValidator] '{column}' matched exactly")
                else:
                    logger.warning(f"[FilterValidator] Could not resolve '{column}'")

        return resolved

    def infer_column_type(self, column: str, df: Optional[pd.DataFrame] = None) -> str:
        """
        Infer the type of a column: numeric, date, or categorical.

        Args:
            column: Column name
            df: Optional DataFrame (if not provided, loads from dataset_path)

        Returns:
            Column type: "numeric", "date", or "categorical"

        Examples:
            >>> validator.infer_column_type("Valor_Vendido")
            'numeric'
            >>> validator.infer_column_type("UF_Cliente")
            'categorical'
        """
        if df is None:
            df = self._load_dataset_sample()

        if column not in df.columns:
            logger.warning(f"[FilterValidator] Column '{column}' not found, assuming categorical")
            return "categorical"

        dtype = df[column].dtype

        if pd.api.types.is_numeric_dtype(dtype):
            return "numeric"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "date"
        else:
            return "categorical"

    def _get_available_columns(self) -> List[str]:
        """
        Get list of available columns from dataset.

        Returns:
            List of column names

        Raises:
            FileNotFoundError: If dataset file not found
        """
        if self._available_columns is None:
            if not self.dataset_path.exists():
                raise FileNotFoundError(f"Dataset not found: {self.dataset_path}")

            logger.debug(f"[FilterValidator] Loading column list from {self.dataset_path}")
            self._available_columns = self.dataset_extractor.get_columns(str(self.dataset_path))
            logger.debug(f"[FilterValidator] Found {len(self._available_columns)} columns")

        return self._available_columns

    def _load_dataset_sample(self) -> pd.DataFrame:
        """
        Load a sample of the dataset for validation.

        Returns:
            DataFrame sample

        Raises:
            FileNotFoundError: If dataset file not found
        """
        if not self.dataset_path.exists():
            raise FileNotFoundError(f"Dataset not found: {self.dataset_path}")

        logger.debug(f"[FilterValidator] Loading dataset sample from {self.dataset_path}")

        # Determine file type and load accordingly
        if self.dataset_path.suffix == ".parquet":
            df = pd.read_parquet(self.dataset_path)
        elif self.dataset_path.suffix == ".csv":
            df = pd.read_csv(self.dataset_path)
        else:
            raise ValueError(f"Unsupported file type: {self.dataset_path.suffix}")

        # Sample if dataset is too large
        if DATASET_SAMPLE_SIZE and len(df) > DATASET_SAMPLE_SIZE:
            logger.debug(f"[FilterValidator] Sampling {DATASET_SAMPLE_SIZE} rows from {len(df)}")
            df = df.sample(n=DATASET_SAMPLE_SIZE, random_state=42)

        return df

    def get_column_info(self, column: str) -> Dict[str, Any]:
        """
        Get metadata information about a column.

        Args:
            column: Column name

        Returns:
            Dictionary with column metadata (type, sample values, etc.)
        """
        df = self._load_dataset_sample()

        if column not in df.columns:
            return {"error": f"Column '{column}' not found"}

        col_type = self.infer_column_type(column, df)
        info = {
            "name": column,
            "type": col_type,
            "dtype": str(df[column].dtype),
            "null_count": int(df[column].isnull().sum()),
            "unique_count": int(df[column].nunique())
        }

        # Add type-specific info
        if col_type == "categorical":
            unique_values = df[column].unique()
            info["unique_values"] = list(unique_values[:20])  # Limit to first 20
            info["total_unique"] = len(unique_values)
        elif col_type == "numeric":
            info["min"] = float(df[column].min())
            info["max"] = float(df[column].max())
            info["mean"] = float(df[column].mean())

        return info

    def get_unique_values(self, column: str, limit: int = 100) -> List[Any]:
        """
        Get unique values from a column.

        NEW METHOD: Extracted from get_column_info for reusability.

        Args:
            column: Column name
            limit: Maximum number of unique values to return (default: 100)

        Returns:
            List of unique values (up to limit)

        Examples:
            >>> validator.get_unique_values("Des_Grupo_Produto", limit=10)
            ['ADESIVOS', 'COMPONENTES', 'ESQUADRIAS', ...]
        """
        logger.debug(f"[FilterValidator] Getting unique values for '{column}' (limit={limit})")

        df = self._load_dataset_sample()

        if column not in df.columns:
            logger.warning(f"[FilterValidator] Column '{column}' not found")
            return []

        unique_values = df[column].dropna().unique()

        # Convert to list and limit
        unique_list = list(unique_values[:limit])

        logger.debug(
            f"[FilterValidator] Found {len(unique_values)} unique values, "
            f"returning {len(unique_list)}"
        )

        return unique_list

    def suggest_valid_values(
        self,
        column: str,
        invalid_value: str,
        max_suggestions: int = 5,
        score_cutoff: float = 60.0
    ) -> List[str]:
        """
        Suggest valid values using fuzzy matching.

        NEW METHOD: Helps users when they mistype values like "ADESIVO" instead of "ADESIVOS".

        Args:
            column: Column name
            invalid_value: The invalid value that was not found
            max_suggestions: Maximum number of suggestions to return (default: 5)
            score_cutoff: Minimum similarity score (0-100, default: 60.0)

        Returns:
            List of suggested valid values, sorted by similarity score

        Examples:
            >>> validator.suggest_valid_values("Des_Grupo_Produto", "ADESIVO")
            ['ADESIVOS', 'ADESIVOS ESPECIAIS']
            >>> validator.suggest_valid_values("Des_Linha_Produto", "PRODUTOS")
            ['PRODUTOS REVENDA', 'PRODUTOS ACABADOS']
        """
        logger.debug(
            f"[FilterValidator] Finding suggestions for '{invalid_value}' in column '{column}'"
        )

        # Get unique values from column
        unique_values = self.get_unique_values(column, limit=200)

        if not unique_values:
            logger.warning(f"[FilterValidator] No unique values found for column '{column}'")
            return []

        # Convert to strings for fuzzy matching
        choices = [str(v) for v in unique_values]

        # Use RapidFuzz for fuzzy matching
        # token_sort_ratio handles word order differences
        matches = process.extract(
            str(invalid_value),
            choices,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=score_cutoff,
            limit=max_suggestions
        )

        # Extract just the matched strings (not scores)
        suggestions = [match[0] for match in matches]

        logger.info(
            f"[FilterValidator] Found {len(suggestions)} suggestions for '{invalid_value}': "
            f"{suggestions[:3]}{'...' if len(suggestions) > 3 else ''}"
        )

        return suggestions
