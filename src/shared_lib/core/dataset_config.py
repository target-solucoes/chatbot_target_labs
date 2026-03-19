"""
DatasetConfig - Centralized Dataset Configuration Singleton.

This module provides a single point of access for ALL dataset-related
configuration, eliminating distributed hardcoded references and ensuring
that swapping datasets requires only:
  1. Setting DATASET_PATH environment variable
  2. Updating data/mappings/alias.yaml

No other module should directly access:
  - The DATASET_PATH environment variable
  - The alias.yaml file
  - Hardcoded column names, types, or dataset paths

Usage:
    from src.shared_lib.core.dataset_config import DatasetConfig

    config = DatasetConfig.get_instance()
    print(config.dataset_path)
    print(config.numeric_columns)
    print(config.has_temporal)
"""

import logging
import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import yaml

logger = logging.getLogger(__name__)

# Project root: 4 levels up from this file
# dataset_config.py -> core/ -> shared_lib/ -> src/ -> PROJECT_ROOT
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent


class DatasetConfig:
    """
    Singleton providing centralized, read-only access to all dataset metadata.

    Loads configuration from:
      - DATASET_PATH environment variable (required)
      - ALIAS_PATH environment variable (optional, defaults to data/mappings/alias.yaml)

    Thread-safe singleton pattern ensures a single instance across all modules.

    Properties exposed:
      - dataset_path: Absolute path to the dataset file
      - alias_path: Absolute path to the alias.yaml file
      - alias_data: Full alias.yaml content as dict
      - columns: Dict mapping real column names to their alias lists
      - column_types: Dict with 'numeric', 'categorical', 'temporal' lists
      - numeric_columns: List of numeric column names
      - categorical_columns: List of categorical column names
      - temporal_columns: List of temporal column names (may be empty)
      - has_temporal: Whether the dataset has temporal columns
      - temporal_column_name: Name of the primary temporal column (or None)
      - metrics: Dict of named metric definitions from alias.yaml
      - categories: Dict of category groupings from alias.yaml
      - conventions: Dict of display conventions from alias.yaml
      - filter_hints: Dict with valid_years, known_entities, important_values
      - column_aliases: Reverse index mapping each alias to its real column
      - all_real_columns: Set of all real column names in alias.yaml
    """

    _instance: Optional["DatasetConfig"] = None
    _lock = threading.Lock()
    _initialized: bool = False

    def __new__(cls) -> "DatasetConfig":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        """Initialize on first creation only (singleton guard)."""
        if DatasetConfig._initialized:
            return

        with DatasetConfig._lock:
            if DatasetConfig._initialized:
                return

            self._dataset_path: str = ""
            self._alias_path: str = ""
            self._alias_data: Dict[str, Any] = {}
            self._column_types: Dict[str, List[str]] = {
                "numeric": [],
                "categorical": [],
                "temporal": [],
            }
            self._columns: Dict[str, List[str]] = {}
            self._metrics: Dict[str, List[str]] = {}
            self._categories: Dict[str, List[str]] = {}
            self._conventions: Dict[str, str] = {}
            self._filter_hints: Dict[str, Any] = {}
            self._column_aliases: Dict[str, str] = {}
            self._all_real_columns: Set[str] = set()

            self._load()
            DatasetConfig._initialized = True

    # ------------------------------------------------------------------
    # Public class methods
    # ------------------------------------------------------------------

    @classmethod
    def get_instance(cls) -> "DatasetConfig":
        """Return the singleton instance, creating it if necessary."""
        return cls()

    @classmethod
    def reset(cls) -> None:
        """
        Reset the singleton (for testing or hot-reload scenarios only).

        After calling reset(), the next get_instance() will reload everything.
        """
        with cls._lock:
            cls._instance = None
            cls._initialized = False

    # ------------------------------------------------------------------
    # Properties (read-only)
    # ------------------------------------------------------------------

    @property
    def dataset_path(self) -> str:
        """Absolute path to the dataset file."""
        return self._dataset_path

    @property
    def alias_path(self) -> str:
        """Absolute path to the alias.yaml file."""
        return self._alias_path

    @property
    def alias_data(self) -> Dict[str, Any]:
        """Full alias.yaml content as a dictionary."""
        return self._alias_data

    @property
    def columns(self) -> Dict[str, List[str]]:
        """Mapping of real column names to their alias lists."""
        return self._columns

    @property
    def column_types(self) -> Dict[str, List[str]]:
        """Dict with keys 'numeric', 'categorical', 'temporal'."""
        return self._column_types

    @property
    def numeric_columns(self) -> List[str]:
        """List of numeric (metric) column names."""
        return self._column_types.get("numeric", [])

    @property
    def categorical_columns(self) -> List[str]:
        """List of categorical (dimension) column names."""
        return self._column_types.get("categorical", [])

    @property
    def temporal_columns(self) -> List[str]:
        """List of temporal column names (may be empty)."""
        return self._column_types.get("temporal", [])

    @property
    def has_temporal(self) -> bool:
        """Whether the dataset has temporal columns."""
        return len(self.temporal_columns) > 0

    @property
    def temporal_column_name(self) -> Optional[str]:
        """
        Name of the primary temporal column, or None if no temporal columns.

        This replaces all hardcoded references to 'Data'.
        """
        cols = self.temporal_columns
        return cols[0] if cols else None

    @property
    def metrics(self) -> Dict[str, List[str]]:
        """Named metric definitions from alias.yaml."""
        return self._metrics

    @property
    def categories(self) -> Dict[str, List[str]]:
        """Category groupings from alias.yaml."""
        return self._categories

    @property
    def conventions(self) -> Dict[str, str]:
        """Display conventions from alias.yaml."""
        return self._conventions

    @property
    def filter_hints(self) -> Dict[str, Any]:
        """
        Filter hints for heuristic detection.

        Returns dict with keys:
          - valid_years: List[str]  (may be empty)
          - known_entities: Dict with 'states', 'cities' (may be empty)
          - important_values: Dict[str, List[str]] (optional category values)
        """
        return self._filter_hints

    @property
    def column_aliases(self) -> Dict[str, str]:
        """Reverse index: lowercase alias -> real column name."""
        return self._column_aliases

    @property
    def all_real_columns(self) -> Set[str]:
        """Set of all real column names defined in alias.yaml."""
        return self._all_real_columns

    # ------------------------------------------------------------------
    # Derived helpers
    # ------------------------------------------------------------------

    def get_valid_years(self) -> Set[str]:
        """Return set of valid year strings from filter_hints."""
        years = self._filter_hints.get("valid_years", [])
        return {str(y) for y in years}

    def get_known_states(self) -> Set[str]:
        """Return set of known geographic states from filter_hints."""
        entities = self._filter_hints.get("known_entities", {})
        return set(entities.get("states", []))

    def get_known_cities(self) -> Set[str]:
        """Return set of known cities from filter_hints."""
        entities = self._filter_hints.get("known_entities", {})
        return {c.lower() for c in entities.get("cities", [])}

    def get_important_values(self) -> Dict[str, List[str]]:
        """Return important category values per column from filter_hints."""
        return self._filter_hints.get("important_values", {})

    def is_numeric_column(self, col_name: str) -> bool:
        """Check if a column is classified as numeric."""
        return col_name in self.numeric_columns

    def is_categorical_column(self, col_name: str) -> bool:
        """Check if a column is classified as categorical."""
        return col_name in self.categorical_columns

    def is_temporal_column(self, col_name: str) -> bool:
        """Check if a column is classified as temporal."""
        return col_name in self.temporal_columns

    def get_default_metric(self) -> Optional[str]:
        """Return the first numeric column as default metric, or None."""
        return self.numeric_columns[0] if self.numeric_columns else None

    def build_keyword_to_column_map(self) -> Dict[str, str]:
        """
        Build reverse mapping: lowercase keyword -> real column name.

        Includes all aliases from the 'columns' section of alias.yaml.
        """
        return dict(self._column_aliases)

    def build_metric_keyword_map(self) -> Dict[str, str]:
        """
        Build reverse mapping filtered to numeric columns only.

        Returns: keyword (lowercase) -> real numeric column name.
        """
        numeric_set = set(self.numeric_columns)
        return {k: v for k, v in self._column_aliases.items() if v in numeric_set}

    # ------------------------------------------------------------------
    # Internal loading
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """Load dataset path and alias.yaml configuration."""
        self._load_dataset_path()
        self._load_alias_path()
        self._load_alias_data()
        self._build_reverse_index()

        logger.info(
            "[DatasetConfig] Initialized: "
            f"dataset={Path(self._dataset_path).name}, "
            f"numeric={len(self.numeric_columns)}, "
            f"categorical={len(self.categorical_columns)}, "
            f"temporal={len(self.temporal_columns)}, "
            f"aliases={len(self._column_aliases)}, "
            f"has_temporal={self.has_temporal}"
        )

    def _load_dataset_path(self) -> None:
        """Resolve DATASET_PATH from environment."""
        raw = os.getenv("DATASET_PATH", "")
        if not raw:
            raise ValueError(
                "DATASET_PATH environment variable is not set. "
                "Please define it in your .env file or environment variables. "
                "Example: DATASET_PATH=data/datasets/telco_customer_churn.parquet"
            )
        path = Path(raw)
        if not path.is_absolute():
            path = _PROJECT_ROOT / path
        self._dataset_path = str(path)

    def _load_alias_path(self) -> None:
        """Resolve ALIAS_PATH from environment (with sensible default)."""
        raw = os.getenv(
            "ALIAS_PATH",
            str(_PROJECT_ROOT / "data" / "mappings" / "alias.yaml"),
        )
        path = Path(raw)
        if not path.is_absolute():
            path = _PROJECT_ROOT / path
        self._alias_path = str(path)

    def _load_alias_data(self) -> None:
        """Parse alias.yaml and populate internal structures."""
        alias_file = Path(self._alias_path)
        if not alias_file.exists():
            raise FileNotFoundError(
                f"Alias configuration file not found: {self._alias_path}. "
                "Ensure alias.yaml exists at the configured ALIAS_PATH."
            )

        try:
            with open(alias_file, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise ValueError(f"Failed to parse alias.yaml: {e}") from e

        self._alias_data = data

        # column_types
        ct = data.get("column_types", {})
        self._column_types = {
            "numeric": ct.get("numeric", []),
            "categorical": ct.get("categorical", []),
            "temporal": ct.get("temporal", []),
        }

        # columns (alias mappings)
        self._columns = data.get("columns", {})

        # metrics, categories, conventions
        self._metrics = data.get("metrics", {})
        self._categories = data.get("categories", {})
        self._conventions = data.get("conventions", {})

        # filter_hints (with safe defaults)
        fh = data.get("filter_hints", {})
        self._filter_hints = {
            "valid_years": fh.get("valid_years", []),
            "known_entities": fh.get("known_entities", {"states": [], "cities": []}),
            "important_values": fh.get("important_values", {}),
        }

    def _build_reverse_index(self) -> None:
        """Build reverse alias index and all_real_columns set."""
        reverse: Dict[str, str] = {}
        real_cols: Set[str] = set()

        # From column_types
        for type_list in self._column_types.values():
            real_cols.update(type_list)

        # From columns (alias mappings)
        for real_col, aliases in self._columns.items():
            real_cols.add(real_col)
            # Map the column name itself (lowercase)
            reverse[real_col.lower()] = real_col
            if isinstance(aliases, list):
                for alias in aliases:
                    reverse[str(alias).lower()] = real_col

        self._column_aliases = reverse
        self._all_real_columns = real_cols

    # ------------------------------------------------------------------
    # String representation
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        ds_name = Path(self._dataset_path).name if self._dataset_path else "<unset>"
        return (
            f"DatasetConfig(dataset='{ds_name}', "
            f"numeric={len(self.numeric_columns)}, "
            f"categorical={len(self.categorical_columns)}, "
            f"temporal={len(self.temporal_columns)})"
        )
