"""
Dataset Column Extractor - Phase 1 Column Validation.

This module provides lightweight column extraction from datasets without
loading the full data into memory. It's used in Phase 1 to validate that
detected columns actually exist in the target dataset.

Key features:
- Fast metadata-only extraction using DuckDB (primary)
- PyArrow fallback for parquet files
- Pandas fallback for CSV files
- Minimal memory footprint
- Caching support for repeated queries
- Integration with Phase 1 workflow

Performance:
- DuckDB: Queries only schema, zero data loaded
- PyArrow: Reads only parquet metadata
- Pandas: Reads only CSV header (nrows=0)
"""

import logging
from pathlib import Path
from typing import List, Optional, Union, Set, Dict
from functools import lru_cache
import pandas as pd
import pyarrow.parquet as pq

from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class DatasetColumnExtractor:
    """
    Extracts column names from datasets with minimal overhead.

    This class is designed for Phase 1 validation, allowing the
    graphic_classifier_agent to validate column references against
    the actual dataset before passing to Phase 2.

    Features:
    - DuckDB-based schema extraction (primary, fastest)
    - PyArrow metadata-only extraction for parquet (fallback)
    - Pandas header-only extraction for CSV (fallback)
    - LRU caching for performance
    - Support for multiple file formats

    Performance Characteristics:
    - DuckDB: ~5-10ms for parquet schema query (no data loaded)
    - PyArrow: ~10-20ms for metadata extraction
    - Pandas CSV: ~20-50ms for header-only read

    Example:
        >>> extractor = DatasetColumnExtractor()
        >>> columns = extractor.get_columns("data/sales.parquet")
        >>> print(columns)
        ['Empresa', 'Cod_Cliente', 'Valor_Vendido', ...]
    """

    def __init__(self, cache_size: int = 10, use_duckdb: bool = True):
        """
        Initialize the DatasetColumnExtractor.

        Args:
            cache_size: Maximum number of datasets to cache metadata for
            use_duckdb: Whether to use DuckDB for schema extraction (recommended)
        """
        self.cache_size = cache_size
        self.use_duckdb = use_duckdb
        self._column_cache: Dict[str, List[str]] = {}
        self._setup_cache()

        # Try to import DuckDB if enabled
        self._duckdb_available = False
        if self.use_duckdb:
            try:
                import duckdb

                self._duckdb = duckdb
                self._duckdb_available = True
                logger.info("DatasetColumnExtractor initialized with DuckDB support")
            except ImportError:
                logger.warning(
                    "DuckDB not available, falling back to PyArrow/Pandas. "
                    "Install with: pip install duckdb"
                )
        else:
            logger.info("DatasetColumnExtractor initialized (DuckDB disabled)")

        logger.info(f"Cache size: {cache_size}")

    def _setup_cache(self) -> None:
        """Configure the LRU cache."""
        if self.cache_size > 0:
            self._cached_get_columns = lru_cache(maxsize=self.cache_size)(
                self._get_columns_uncached
            )
        else:
            self._cached_get_columns = self._get_columns_uncached

    def get_columns(self, data_source: Union[str, Path]) -> List[str]:
        """
        Get column names from a data source.

        This method extracts only the column names without loading
        the full dataset into memory, making it very fast.

        Args:
            data_source: Path to the data file (parquet, csv)

        Returns:
            List of column names

        Raises:
            FileNotFoundError: If the data source file does not exist
            ValueError: If the file format is not supported

        Example:
            >>> extractor = DatasetColumnExtractor()
            >>> columns = extractor.get_columns("data/sales.parquet")
            >>> print(f"Found {len(columns)} columns")
            Found 15 columns
        """
        path = Path(data_source)

        # Validate file exists
        if not path.exists():
            error_msg = f"Data source not found: {data_source}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        # Get columns with caching
        return self._cached_get_columns(str(path.resolve()))

    def _get_columns_uncached(self, file_path: str) -> List[str]:
        """
        Internal method to extract columns (used by cache).

        Extraction Strategy:
        1. Try DuckDB (fastest, works for parquet and CSV)
        2. Fall back to PyArrow for parquet
        3. Fall back to Pandas for CSV

        Args:
            file_path: Absolute path to the file

        Returns:
            List of column names

        Raises:
            ValueError: If file format is not supported
        """
        path = Path(file_path)

        try:
            # Strategy 1: Try DuckDB first (fastest, most scalable)
            if self._duckdb_available:
                try:
                    return self._get_columns_duckdb(path)
                except Exception as e:
                    logger.debug(f"DuckDB extraction failed, trying fallback: {e}")

            # Strategy 2: File-specific fallbacks
            if path.suffix == ".parquet":
                return self._get_parquet_columns(path)
            elif path.suffix == ".csv":
                return self._get_csv_columns(path)
            else:
                raise ValueError(f"Unsupported file format: {path.suffix}")

        except Exception as e:
            logger.error(f"Error extracting columns from {file_path}: {str(e)}")
            raise

    def _get_columns_duckdb(self, file_path: Path) -> List[str]:
        """
        Extract columns using DuckDB (schema query only).

        DuckDB can query just the schema without loading data,
        making it extremely fast and memory-efficient.

        Args:
            file_path: Path to data file

        Returns:
            List of column names

        Example Query:
            DESCRIBE SELECT * FROM 'data.parquet' LIMIT 0
        """
        try:
            # Query only schema, no data loaded
            # LIMIT 0 ensures no rows are read
            query = f"DESCRIBE SELECT * FROM '{file_path}' LIMIT 0"

            logger.debug(f"Extracting columns with DuckDB: {file_path.name}")

            # Execute schema query
            result = self._duckdb.query(query).to_df()

            # Extract column names from DESCRIBE output
            columns = result["column_name"].tolist()

            logger.debug(
                f"DuckDB extracted {len(columns)} columns from {file_path.name} "
                f"(format: {file_path.suffix})"
            )

            return columns

        except Exception as e:
            logger.debug(f"DuckDB extraction failed for {file_path.name}: {e}")
            raise

    def get_columns_lazy(self, data_source: Union[str, Path]) -> List[str]:
        """
        Alias for get_columns() - emphasizes lazy loading behavior.

        This method name makes it explicit that only metadata is loaded,
        not the full dataset.

        Args:
            data_source: Path to the data file

        Returns:
            List of column names (metadata only)

        Example:
            >>> extractor = DatasetColumnExtractor()
            >>> # Only schema loaded, NOT the full dataset
            >>> columns = extractor.get_columns_lazy("huge_dataset.parquet")
            >>> print(f"Dataset has {len(columns)} columns")
        """
        return self.get_columns(data_source)

    def _get_parquet_columns(self, file_path: Path) -> List[str]:
        """
        Extract columns from parquet file (metadata only).

        Uses PyArrow to read only the schema without loading data.
        This is the fallback method when DuckDB is not available.

        Args:
            file_path: Path to parquet file

        Returns:
            List of column names
        """
        try:
            logger.debug(f"Extracting columns with PyArrow: {file_path.name}")

            # Read only the schema (no data loaded)
            parquet_file = pq.ParquetFile(file_path)
            schema = parquet_file.schema_arrow
            columns = schema.names

            logger.debug(
                f"PyArrow extracted {len(columns)} columns from {file_path.name}"
            )
            return columns

        except Exception as e:
            # Fallback to pandas if PyArrow fails
            logger.warning(f"PyArrow extraction failed, using pandas fallback: {e}")
            df = pd.read_parquet(file_path, engine="pyarrow")
            return df.columns.tolist()

    def _get_csv_columns(self, file_path: Path) -> List[str]:
        """
        Extract columns from CSV file (header only).

        Reads only the first row to extract column names.
        This is the fallback method when DuckDB is not available.

        Args:
            file_path: Path to CSV file

        Returns:
            List of column names
        """
        try:
            logger.debug(f"Extracting columns with Pandas: {file_path.name}")

            # Read only the header (nrows=0)
            df = pd.read_csv(file_path, nrows=0)
            columns = df.columns.tolist()

            logger.debug(
                f"Pandas extracted {len(columns)} columns from {file_path.name}"
            )
            return columns

        except Exception as e:
            logger.error(f"Failed to extract columns from CSV: {e}")
            raise

    def validate_columns(
        self,
        data_source: Union[str, Path],
        columns_to_check: List[str],
        case_sensitive: bool = True,
    ) -> tuple[List[str], List[str]]:
        """
        Validate which columns exist in the dataset.

        Args:
            data_source: Path to the data file
            columns_to_check: List of column names to validate
            case_sensitive: Whether to use case-sensitive matching

        Returns:
            Tuple of (valid_columns, invalid_columns)

        Example:
            >>> extractor = DatasetColumnExtractor()
            >>> valid, invalid = extractor.validate_columns(
            ...     "data/sales.parquet",
            ...     ["Cod_Cliente", "month", "Valor_Vendido", "year"]
            ... )
            >>> print(f"Valid: {valid}")
            Valid: ['Cod_Cliente', 'Valor_Vendido']
            >>> print(f"Invalid: {invalid}")
            Invalid: ['month', 'year']
        """
        available_columns = self.get_columns(data_source)

        if not case_sensitive:
            available_columns_lower = {col.lower() for col in available_columns}

            valid = []
            invalid = []

            for col in columns_to_check:
                if col.lower() in available_columns_lower:
                    # Find the original case column name
                    original = next(
                        c for c in available_columns if c.lower() == col.lower()
                    )
                    valid.append(original)
                else:
                    invalid.append(col)
        else:
            available_set = set(available_columns)
            valid = [col for col in columns_to_check if col in available_set]
            invalid = [col for col in columns_to_check if col not in available_set]

        logger.debug(f"Column validation: {len(valid)} valid, {len(invalid)} invalid")

        return valid, invalid

    def get_column_set(self, data_source: Union[str, Path]) -> Set[str]:
        """
        Get column names as a set for fast membership testing.

        Args:
            data_source: Path to the data file

        Returns:
            Set of column names

        Example:
            >>> extractor = DatasetColumnExtractor()
            >>> columns = extractor.get_column_set("data/sales.parquet")
            >>> if "Cod_Cliente" in columns:
            ...     print("Column exists")
        """
        return set(self.get_columns(data_source))

    def clear_cache(self) -> None:
        """
        Clear the internal cache.

        Example:
            >>> extractor = DatasetColumnExtractor()
            >>> columns1 = extractor.get_columns("data.parquet")  # From file
            >>> columns2 = extractor.get_columns("data.parquet")  # From cache
            >>> extractor.clear_cache()
            >>> columns3 = extractor.get_columns("data.parquet")  # From file again
        """
        if hasattr(self._cached_get_columns, "cache_clear"):
            self._cached_get_columns.cache_clear()
            logger.info("Cache cleared")

    def get_cache_info(self) -> dict:
        """
        Get information about the current cache state.

        Returns:
            Dictionary with cache statistics

        Example:
            >>> extractor = DatasetColumnExtractor()
            >>> info = extractor.get_cache_info()
            >>> print(f"Cache hits: {info['hits']}")
            >>> print(f"Cache efficiency: {info['hit_rate']:.1%}")
        """
        if hasattr(self._cached_get_columns, "cache_info"):
            cache_info = self._cached_get_columns.cache_info()
            total_requests = cache_info.hits + cache_info.misses
            hit_rate = cache_info.hits / total_requests if total_requests > 0 else 0.0

            return {
                "hits": cache_info.hits,
                "misses": cache_info.misses,
                "size": cache_info.currsize,
                "maxsize": cache_info.maxsize,
                "hit_rate": hit_rate,
                "duckdb_available": self._duckdb_available,
            }
        return {
            "hits": 0,
            "misses": 0,
            "size": 0,
            "maxsize": 0,
            "hit_rate": 0.0,
            "duckdb_available": self._duckdb_available,
        }


# Global singleton instance
_extractor: Optional[DatasetColumnExtractor] = None


def get_dataset_columns(data_source: Union[str, Path]) -> List[str]:
    """
    Convenience function to get dataset columns using global instance.

    Args:
        data_source: Path to the data file

    Returns:
        List of column names

    Example:
        >>> from src.shared_lib.data.dataset_column_extractor import get_dataset_columns
        >>> columns = get_dataset_columns("data/sales.parquet")
        >>> print(columns)
    """
    global _extractor

    if _extractor is None:
        _extractor = DatasetColumnExtractor()

    return _extractor.get_columns(data_source)


def validate_columns_exist(
    data_source: Union[str, Path], columns_to_check: List[str]
) -> tuple[List[str], List[str]]:
    """
    Convenience function to validate columns using global instance.

    Args:
        data_source: Path to the data file
        columns_to_check: List of column names to validate

    Returns:
        Tuple of (valid_columns, invalid_columns)

    Example:
        >>> from src.shared_lib.data.dataset_column_extractor import validate_columns_exist
        >>> valid, invalid = validate_columns_exist(
        ...     "data/sales.parquet",
        ...     ["Cod_Cliente", "month"]
        ... )
    """
    global _extractor

    if _extractor is None:
        _extractor = DatasetColumnExtractor()

    return _extractor.validate_columns(data_source, columns_to_check)
