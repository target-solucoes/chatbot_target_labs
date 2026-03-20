"""
ValueCatalog - Pre-computed index of unique categorical values from dataset.

Loaded once at startup and cached in memory. Provides fast lookup
for the PreMatchEngine to resolve query tokens to real dataset values
before the LLM call.

Key features:
- Singleton pattern (loaded once, reused across queries)
- Reads ALL rows (not a sample) to capture every unique value
- Builds normalized inverted index for fast fuzzy matching
- Encoding normalization for corrupted characters (PEDICOS, Concorrencia, etc.)
- Cardinality tracking per column for scoring
"""

import unicodedata
import re
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from functools import lru_cache

import pandas as pd

from src.shared_lib.utils.logger import get_logger
from src.shared_lib.core.config import (
    load_alias_data,
    get_dataset_path,
    get_column_types,
)

logger = get_logger(__name__)


def normalize_text(text: str) -> str:
    """
    Normalize text for matching: lowercase, remove accents, fix encoding artifacts.

    Handles corrupted encoding from latin1->utf8 conversion literais:
    - '\\xc9' -> E, '\\xe3' -> a, '\\xe7' -> c, etc.
    """
    if not isinstance(text, str):
        text = str(text)

    # Fast-path uppercase exact literals common in this dataset's mojibake
    # (e.g., pandas loaded latin-1 as raw utf-8 strings)
    mojibake_map = {
        r"\xc9": "E", r"Ã©": "e",
        r"\xcd": "I", r"Ã­": "i",
        r"\xe1": "a", r"Ã¡": "a",
        r"\xe3": "a", r"Ã£": "a",
        r"\xe7": "c", r"Ã§": "c",
        r"\xe9": "e", r"Ã©": "e",
        r"\xea": "e", r"Ãª": "e",
        r"\xf3": "o", r"Ã³": "o",
        r"\xf4": "o", r"Ã´": "o",
        r"\xfa": "u", r"Ãº": "u",
        r"\xe2": "a", r"Ã¢": "a",
    }
    
    for corrupted, fixed in mojibake_map.items():
        if corrupted in text:
            text = text.replace(corrupted, fixed)

    # Also handle literal backslash-x escapes that might be interpreted by Python
    # This is a safe fallback if the string contains actual byte escapes
    try:
        # Try to encode to latin-1 and decode to utf-8 if it's a badly loaded string
        # This catches "P\xc9DICOS" if it's actually `P\xc9DICOS` in memory
        fixed_text = text.encode('latin1').decode('utf8')
        # Only use it if it didn't raise UnicodeDecodeError and actually changed something
        if fixed_text != text:
             text = fixed_text
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass

    # Standard normalization
    # NFD decomposition to split base chars from combining marks
    normalized = unicodedata.normalize("NFD", text)
    # Remove combining marks (accents)
    normalized = "".join(
        ch for ch in normalized if unicodedata.category(ch) != "Mn"
    )
    # Lowercase and strip
    normalized = normalized.lower().strip()
    # Collapse multiple spaces
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


class ValueCatalog:
    """
    Pre-computed index of all unique categorical values from the dataset.

    Structure:
    - values_by_column: {column -> set of original values}
    - normalized_to_original: {column -> {normalized_value -> original_value}}
    - inverted_index: {normalized_value -> [(column, original_value)]}
    - cardinality: {column -> int}
    """

    _instance: Optional["ValueCatalog"] = None

    def __init__(self):
        self.values_by_column: Dict[str, Set[str]] = {}
        self.normalized_to_original: Dict[str, Dict[str, str]] = {}
        self.inverted_index: Dict[str, List[Tuple[str, str]]] = {}
        self.cardinality: Dict[str, int] = {}
        self._loaded = False

    @classmethod
    def get_instance(cls) -> "ValueCatalog":
        """Get or create the singleton ValueCatalog instance."""
        if cls._instance is None:
            cls._instance = cls()
            cls._instance.load()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton (for testing)."""
        cls._instance = None

    def load(self) -> None:
        """Load all unique categorical values from the dataset."""
        if self._loaded:
            return

        try:
            dataset_path = get_dataset_path()
            column_types = get_column_types()
            categorical_columns = column_types.get("categorical", [])

            if not categorical_columns:
                logger.warning("[ValueCatalog] No categorical columns defined in alias.yaml")
                self._loaded = True
                return

            logger.info(
                f"[ValueCatalog] Loading unique values for {len(categorical_columns)} "
                f"categorical columns from {Path(dataset_path).name}"
            )

            # Read only categorical columns from parquet
            path = Path(dataset_path)
            if path.suffix == ".parquet":
                import pyarrow.parquet as pq

                schema_cols = pq.ParquetFile(dataset_path).schema_arrow.names
                cols_to_read = [c for c in categorical_columns if c in schema_cols]
                df = pd.read_parquet(dataset_path, columns=cols_to_read)
            else:
                df = pd.read_csv(dataset_path, nrows=0)
                cols_to_read = [c for c in categorical_columns if c in df.columns]
                df = pd.read_csv(dataset_path, usecols=cols_to_read)

            total_values = 0
            for col in cols_to_read:
                unique_vals = df[col].dropna().unique()
                str_vals = {str(v) for v in unique_vals}

                self.values_by_column[col] = str_vals
                self.cardinality[col] = len(str_vals)

                # Build normalized mapping
                norm_map = {}
                for val in str_vals:
                    norm = normalize_text(val)
                    norm_map[norm] = val
                    # Add to inverted index
                    if norm not in self.inverted_index:
                        self.inverted_index[norm] = []
                    self.inverted_index[norm].append((col, val))

                self.normalized_to_original[col] = norm_map
                total_values += len(str_vals)

            self._loaded = True
            logger.info(
                f"[ValueCatalog] Loaded {total_values} unique values across "
                f"{len(cols_to_read)} columns. "
                f"Inverted index: {len(self.inverted_index)} entries"
            )

            # Log cardinality summary
            for col in sorted(self.cardinality, key=self.cardinality.get):
                logger.debug(
                    f"[ValueCatalog] {col}: {self.cardinality[col]} unique values"
                )

        except Exception as e:
            logger.error(f"[ValueCatalog] Error loading catalog: {e}")
            self._loaded = True  # Prevent infinite retry

    def get_values(self, column: str) -> Set[str]:
        """Get all unique values for a column."""
        return self.values_by_column.get(column, set())

    def get_cardinality(self, column: str) -> int:
        """Get the number of unique values for a column."""
        return self.cardinality.get(column, 0)

    def get_cardinality_tier(self, column: str) -> str:
        """
        Get cardinality tier for scoring boosts.

        Returns: 'very_low' (1-2), 'low' (3-5), 'medium' (6-30), 'high' (30+)
        """
        card = self.get_cardinality(column)
        if card <= 2:
            return "very_low"
        elif card <= 5:
            return "low"
        elif card <= 30:
            return "medium"
        else:
            return "high"

    def lookup_exact(self, normalized_term: str) -> List[Tuple[str, str]]:
        """
        Exact lookup in inverted index.

        Returns: List of (column, original_value) tuples.
        """
        return self.inverted_index.get(normalized_term, [])

    def get_all_normalized_values(self) -> List[str]:
        """Get all normalized values for fuzzy matching."""
        return list(self.inverted_index.keys())

    def is_loaded(self) -> bool:
        return self._loaded

    def get_stats(self) -> Dict:
        """Get catalog statistics."""
        return {
            "columns": len(self.values_by_column),
            "total_values": sum(self.cardinality.values()),
            "inverted_index_size": len(self.inverted_index),
            "cardinality": dict(self.cardinality),
        }
