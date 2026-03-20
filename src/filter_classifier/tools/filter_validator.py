"""
Filter Validator for validating filter columns and values.

Refactored in Phase 3 to use ValueCatalog instead of loading dataset samples.
All value lookups are now O(1) hash lookups against the pre-computed catalog.
"""

import logging
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path

from rapidfuzz import fuzz, process

from src.graphic_classifier.tools.alias_mapper import AliasMapper
from src.shared_lib.data.dataset_column_extractor import DatasetColumnExtractor
from src.shared_lib.data.value_catalog import ValueCatalog, normalize_text
from src.filter_classifier.core.settings import FUZZY_THRESHOLD

logger = logging.getLogger(__name__)


class FilterValidator:
    """
    Validates filter specifications against dataset schema.

    Uses ValueCatalog (pre-computed, singleton) for O(1) value lookups
    instead of loading dataset samples on every validation call.
    """

    def __init__(
        self,
        alias_mapper: AliasMapper,
        dataset_path: str,
        fuzzy_threshold: float = FUZZY_THRESHOLD
    ):
        self.alias_mapper = alias_mapper
        self.dataset_path = Path(dataset_path)
        self.fuzzy_threshold = fuzzy_threshold
        self.dataset_extractor = DatasetColumnExtractor()
        self._available_columns: Optional[List[str]] = None

        # ValueCatalog singleton (loaded once at startup)
        self._catalog = ValueCatalog.get_instance()

        logger.info(f"[FilterValidator] Initialized with ValueCatalog (dataset: {self.dataset_path.name})")

    def validate_columns_exist(
        self, filter_columns: List[str]
    ) -> Tuple[List[str], List[str]]:
        """
        Validate that filter columns exist in the dataset.

        Uses AliasMapper for alias resolution.

        Returns:
            Tuple of (valid_columns, invalid_columns)
        """
        logger.info(f"[FilterValidator] Validating {len(filter_columns)} filter columns")
        available = self._get_available_columns()

        valid_columns = []
        invalid_columns = []

        for column in filter_columns:
            if column in available:
                valid_columns.append(column)
                continue

            resolved = self.alias_mapper.resolve(column)
            if resolved and resolved in available:
                valid_columns.append(resolved)
                continue

            invalid_columns.append(column)
            logger.warning(f"[FilterValidator] Column '{column}' not found in dataset")

        logger.info(
            f"[FilterValidator] Validation result: {len(valid_columns)} valid, "
            f"{len(invalid_columns)} invalid"
        )
        return valid_columns, invalid_columns

    def validate_value_exists(self, column: str, value: Any) -> bool:
        """
        Validate that a value exists in the column using ValueCatalog.

        O(1) lookup against the pre-computed catalog (no DataFrame loading).

        Args:
            column: Column name
            value: Single value to validate

        Returns:
            True if value exists in column
        """
        catalog_values = self._catalog.get_values(column)
        if not catalog_values:
            # Column not in catalog (might be numeric/temporal) — accept
            logger.debug(f"[FilterValidator] Column '{column}' not in ValueCatalog, accepting")
            return True

        value_str = str(value)
        # Exact match (case-insensitive via normalized comparison)
        value_norm = normalize_text(value_str)
        norm_map = self._catalog.normalized_to_original.get(column, {})

        if value_norm in norm_map:
            return True

        # Also check exact string match (case-sensitive)
        if value_str in catalog_values:
            return True

        logger.debug(
            f"[FilterValidator] Value '{value}' not found in column '{column}' "
            f"({self._catalog.get_cardinality(column)} unique values)"
        )
        return False

    def validate_column_values(self, column: str, values: Any) -> bool:
        """
        Validate that filter values exist in the column via ValueCatalog.

        Args:
            column: Column name
            values: Value or list of values to validate

        Returns:
            True if values are valid
        """
        if isinstance(values, list):
            return all(self.validate_value_exists(column, v) for v in values)

        if isinstance(values, dict):
            if "between" in values:
                return True  # Range operators don't need exact value match
            if "in" in values:
                return all(self.validate_value_exists(column, v) for v in values["in"])
            return True

        return self.validate_value_exists(column, values)

    def resolve_column_aliases(
        self, mentioned_columns: List[str]
    ) -> Dict[str, str]:
        """
        Resolve column aliases to actual column names.

        Returns:
            Dictionary mapping query terms to resolved column names
        """
        logger.info(f"[FilterValidator] Resolving aliases for {len(mentioned_columns)} columns")
        available = self._get_available_columns()
        resolved = {}

        for column in mentioned_columns:
            resolved_name = self.alias_mapper.resolve(column)
            if resolved_name:
                resolved[column] = resolved_name
            elif column in available:
                resolved[column] = column
            else:
                logger.warning(f"[FilterValidator] Could not resolve '{column}'")

        return resolved

    def get_unique_values(self, column: str, limit: int = 100) -> List[Any]:
        """
        Get unique values from a column via ValueCatalog.

        Args:
            column: Column name
            limit: Maximum number of unique values to return

        Returns:
            List of unique values (up to limit)
        """
        values = self._catalog.get_values(column)
        return list(values)[:limit]

    def suggest_valid_values(
        self,
        column: str,
        invalid_value: str,
        max_suggestions: int = 5,
        score_cutoff: float = 60.0
    ) -> List[str]:
        """
        Suggest valid values using fuzzy matching against ValueCatalog.

        Args:
            column: Column name
            invalid_value: The invalid value that was not found
            max_suggestions: Maximum number of suggestions
            score_cutoff: Minimum similarity score (0-100)

        Returns:
            List of suggested valid values, sorted by similarity
        """
        unique_values = self.get_unique_values(column, limit=500)
        if not unique_values:
            return []

        choices = [str(v) for v in unique_values]
        matches = process.extract(
            str(invalid_value),
            choices,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=score_cutoff,
            limit=max_suggestions
        )

        suggestions = [match[0] for match in matches]
        logger.info(
            f"[FilterValidator] Found {len(suggestions)} suggestions for '{invalid_value}': "
            f"{suggestions[:3]}{'...' if len(suggestions) > 3 else ''}"
        )
        return suggestions

    def _get_available_columns(self) -> List[str]:
        """Get list of available columns from dataset."""
        if self._available_columns is None:
            if not self.dataset_path.exists():
                raise FileNotFoundError(f"Dataset not found: {self.dataset_path}")
            self._available_columns = self.dataset_extractor.get_columns(str(self.dataset_path))
        return self._available_columns
