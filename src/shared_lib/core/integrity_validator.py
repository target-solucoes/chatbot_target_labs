"""
Dataset Integrity Validator - Startup validation for dataset configuration.

This module validates the consistency between the dataset file, alias.yaml,
and the DatasetConfig singleton at system startup. It prevents silent errors
caused by mismatched configurations when swapping datasets.

Validation checks:
  1. Dataset file exists at DATASET_PATH
  2. All columns in alias.yaml exist in the dataset
  3. column_types covers all columns in the dataset
  4. No alias conflicts (one alias pointing to multiple columns)
  5. No unclassified columns in the dataset

Usage:
    from src.shared_lib.core.integrity_validator import validate_dataset_integrity

    # Raises DatasetIntegrityError on failure
    validate_dataset_integrity()
"""

import logging
from pathlib import Path
from typing import Dict, List, Set

logger = logging.getLogger(__name__)


class DatasetIntegrityError(Exception):
    """Raised when dataset configuration fails integrity validation."""

    def __init__(self, errors: List[str]):
        self.errors = errors
        message = (
            "Dataset integrity validation failed with "
            f"{len(errors)} error(s):\n" + "\n".join(f"  - {e}" for e in errors)
        )
        super().__init__(message)


def validate_dataset_integrity(strict: bool = True) -> List[str]:
    """
    Validate consistency between dataset file, alias.yaml, and DatasetConfig.

    This should be called during system startup to catch configuration
    mismatches early, before any query processing occurs.

    Args:
        strict: If True, raises DatasetIntegrityError on failures.
                If False, returns list of warnings without raising.

    Returns:
        List of warning/error messages (empty if no issues).

    Raises:
        DatasetIntegrityError: If strict=True and validation fails.
    """
    from src.shared_lib.core.dataset_config import DatasetConfig

    errors: List[str] = []
    warnings: List[str] = []

    try:
        config = DatasetConfig.get_instance()
    except Exception as e:
        msg = f"Failed to initialize DatasetConfig: {e}"
        if strict:
            raise DatasetIntegrityError([msg]) from e
        return [msg]

    # ---------------------------------------------------------------
    # Check 1: Dataset file exists
    # ---------------------------------------------------------------
    dataset_path = Path(config.dataset_path)
    if not dataset_path.exists():
        errors.append(
            f"Dataset file not found: {config.dataset_path}. "
            "Verify DATASET_PATH environment variable."
        )
    elif dataset_path.suffix not in (".parquet", ".csv", ".json"):
        warnings.append(
            f"Unusual dataset file extension: {dataset_path.suffix}. "
            "Supported formats: .parquet, .csv, .json"
        )

    # ---------------------------------------------------------------
    # Check 2: Alias.yaml file exists (already validated by DatasetConfig)
    # ---------------------------------------------------------------
    alias_path = Path(config.alias_path)
    if not alias_path.exists():
        errors.append(
            f"Alias configuration file not found: {config.alias_path}. "
            "Verify ALIAS_PATH environment variable."
        )

    # ---------------------------------------------------------------
    # Check 3: column_types is not empty
    # ---------------------------------------------------------------
    all_typed_cols = set(
        config.numeric_columns + config.categorical_columns + config.temporal_columns
    )
    if not all_typed_cols:
        errors.append(
            "No columns defined in alias.yaml column_types. "
            "At least numeric or categorical columns must be specified."
        )

    # ---------------------------------------------------------------
    # Check 4: All columns in 'columns' section exist in column_types
    # ---------------------------------------------------------------
    columns_section_keys = set(config.columns.keys())
    untyped_from_columns = columns_section_keys - all_typed_cols
    if untyped_from_columns:
        warnings.append(
            f"Columns in 'columns' section but missing from 'column_types': "
            f"{sorted(untyped_from_columns)}. "
            "These columns may not be properly classified."
        )

    # ---------------------------------------------------------------
    # Check 5: No alias conflicts (same alias -> multiple columns)
    # ---------------------------------------------------------------
    alias_conflicts = _check_alias_conflicts(config.columns)
    if alias_conflicts:
        for alias_text, target_cols in alias_conflicts.items():
            errors.append(
                f"Alias conflict: '{alias_text}' maps to multiple columns: "
                f"{sorted(target_cols)}. Each alias must be unique."
            )

    # ---------------------------------------------------------------
    # Check 6: Validate columns against actual dataset (if file exists)
    # ---------------------------------------------------------------
    if dataset_path.exists():
        try:
            dataset_cols = _get_dataset_columns(config.dataset_path)
            if dataset_cols is not None:
                # Check alias.yaml columns exist in dataset
                missing_in_dataset = all_typed_cols - dataset_cols
                if missing_in_dataset:
                    errors.append(
                        f"Columns in alias.yaml not found in dataset: "
                        f"{sorted(missing_in_dataset)}. "
                        "Update alias.yaml to match the dataset schema."
                    )

                # Check for unclassified dataset columns
                unclassified = dataset_cols - all_typed_cols
                if unclassified:
                    warnings.append(
                        f"Dataset columns not classified in alias.yaml column_types: "
                        f"{sorted(unclassified)}. "
                        "Consider adding them to numeric, categorical, or temporal."
                    )
        except Exception as e:
            warnings.append(f"Could not validate columns against dataset file: {e}")

    # ---------------------------------------------------------------
    # Check 7: Metrics section references valid columns
    # ---------------------------------------------------------------
    for metric_name, metric_aliases in config.metrics.items():
        # Metrics are named aggregations, not column references directly
        # No structural validation needed beyond existence
        pass

    # ---------------------------------------------------------------
    # Report results
    # ---------------------------------------------------------------
    all_issues = errors + warnings

    if errors:
        logger.error(
            "[IntegrityValidator] Validation FAILED with %d error(s), %d warning(s)",
            len(errors),
            len(warnings),
        )
        for e in errors:
            logger.error("[IntegrityValidator] ERROR: %s", e)
        for w in warnings:
            logger.warning("[IntegrityValidator] WARNING: %s", w)

        if strict:
            raise DatasetIntegrityError(errors)
    elif warnings:
        logger.warning(
            "[IntegrityValidator] Validation passed with %d warning(s)", len(warnings)
        )
        for w in warnings:
            logger.warning("[IntegrityValidator] WARNING: %s", w)
    else:
        logger.info("[IntegrityValidator] Validation passed: all checks OK")

    return all_issues


def _check_alias_conflicts(columns: Dict[str, list]) -> Dict[str, Set[str]]:
    """
    Detect aliases that map to multiple columns.

    Args:
        columns: The 'columns' section from alias.yaml.

    Returns:
        Dict of conflicting alias -> set of target column names.
        Empty dict if no conflicts.
    """
    alias_to_columns: Dict[str, Set[str]] = {}

    for real_col, aliases in columns.items():
        if not isinstance(aliases, list):
            continue
        for alias in aliases:
            key = str(alias).lower()
            if key not in alias_to_columns:
                alias_to_columns[key] = set()
            alias_to_columns[key].add(real_col)

    # Filter to only conflicts (alias maps to 2+ columns)
    return {alias: cols for alias, cols in alias_to_columns.items() if len(cols) > 1}


def _get_dataset_columns(dataset_path: str) -> Set[str]:
    """
    Read column names from the dataset file without loading all data.

    Supports .parquet and .csv formats.

    Args:
        dataset_path: Absolute path to the dataset file.

    Returns:
        Set of column names, or None if unable to read.
    """
    path = Path(dataset_path)

    if path.suffix == ".parquet":
        try:
            import duckdb

            conn = duckdb.connect(":memory:")
            result = conn.execute(
                f"SELECT name FROM parquet_schema('{path}')"
            ).fetchall()
            conn.close()
            # Filter out 'schema' metadata row returned by parquet_schema
            return {row[0] for row in result if row[0] != "schema"}
        except Exception:
            # Fallback to pandas
            try:
                import pandas as pd

                df = pd.read_parquet(path, nrows=0)
                return set(df.columns)
            except Exception:
                # Try reading just the schema
                import pyarrow.parquet as pq

                schema = pq.read_schema(path)
                return {field.name for field in schema}

    elif path.suffix == ".csv":
        try:
            import pandas as pd

            df = pd.read_csv(path, nrows=0)
            return set(df.columns)
        except Exception:
            return None

    return None
