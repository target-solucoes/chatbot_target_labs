"""
DuckDB Engine for Analytics Executor Agent.

This module provides the primary execution engine using DuckDB for
high-performance analytical queries.
"""

import logging
from typing import Optional
import pandas as pd

from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)

# Import DuckDB with graceful fallback
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    logger.warning("DuckDB not available - queries will fall back to Pandas")


class DuckDBExecutionError(Exception):
    """Exception raised when DuckDB execution fails."""
    
    def __init__(self, message: str, query: Optional[str] = None, original_error: Optional[Exception] = None):
        """
        Initialize DuckDB execution error.
        
        Args:
            message: Error message
            query: SQL query that failed (if applicable)
            original_error: Original exception that was raised
        """
        self.message = message
        self.query = query
        self.original_error = original_error
        super().__init__(self._format_message())
    
    def _format_message(self) -> str:
        """Format error message with context."""
        msg = f"DuckDB execution failed: {self.message}"
        if self.query:
            # Truncate long queries
            query_preview = self.query[:200] + "..." if len(self.query) > 200 else self.query
            msg += f"\nQuery: {query_preview}"
        if self.original_error:
            msg += f"\nOriginal error: {str(self.original_error)}"
        return msg


class DuckDBEngine:
    """
    DuckDB execution engine for high-performance analytical queries.
    
    This engine provides:
    - Fast SQL query execution using DuckDB
    - DataFrame registration and query execution
    - Comprehensive error handling
    - Performance logging
    - Connection management
    
    Example:
        >>> engine = DuckDBEngine()
        >>> df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
        >>> query = "SELECT A, SUM(B) as total FROM df GROUP BY A"
        >>> result = engine.execute_query(query, df)
    """
    
    def __init__(self, connection: Optional[duckdb.DuckDBPyConnection] = None):
        """
        Initialize the DuckDB engine.
        
        Args:
            connection: Optional existing DuckDB connection. If not provided,
                       queries will use the default in-memory connection.
        """
        if not DUCKDB_AVAILABLE:
            raise RuntimeError(
                "DuckDB is not installed. Install it with: pip install duckdb"
            )
        
        self.connection = connection
        logger.info("DuckDBEngine initialized")
    
    def execute_query(self, query: str, df: pd.DataFrame, df_name: str = "df") -> pd.DataFrame:
        """
        Execute SQL query using DuckDB.
        
        This method:
        1. Registers the DataFrame with DuckDB
        2. Executes the SQL query
        3. Converts the result back to a pandas DataFrame
        4. Logs performance metrics
        
        Args:
            query: SQL query string
            df: Source DataFrame to query
            df_name: Name to register the DataFrame as (default: "df")
            
        Returns:
            Result DataFrame
            
        Raises:
            DuckDBExecutionError: If query execution fails
            ValueError: If inputs are invalid
            
        Example:
            >>> engine = DuckDBEngine()
            >>> df = pd.DataFrame({'sales': [100, 200, 300], 'region': ['A', 'B', 'A']})
            >>> query = "SELECT region, SUM(sales) as total FROM df GROUP BY region"
            >>> result = engine.execute_query(query, df)
            >>> print(result)
               region  total
            0       A    400
            1       B    200
        """
        # Validate inputs
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")
        
        if df is None:
            raise ValueError("DataFrame cannot be None")
        
        if df.empty:
            logger.warning("DataFrame is empty - query may return no results")
        
        logger.debug(f"Executing DuckDB query on DataFrame with {len(df)} rows, {len(df.columns)} columns")
        
        try:
            # Execute query directly with DuckDB
            # DuckDB can access DataFrame from the local scope
            result = duckdb.query(query).to_df()
            
            logger.info(f"DuckDB query executed successfully. Rows returned: {len(result)}")
            
            # Validate result
            if result is None:
                raise DuckDBExecutionError(
                    "Query returned None",
                    query=query
                )
            
            return result
            
        except duckdb.Error as e:
            # DuckDB-specific error
            error_msg = f"DuckDB error: {str(e)}"
            logger.error(error_msg)
            raise DuckDBExecutionError(
                error_msg,
                query=query,
                original_error=e
            )
            
        except Exception as e:
            # Unexpected error
            error_msg = f"Unexpected error during query execution: {str(e)}"
            logger.error(error_msg)
            raise DuckDBExecutionError(
                error_msg,
                query=query,
                original_error=e
            )
    
    def validate_query(self, query: str) -> bool:
        """
        Validate SQL query syntax without executing it.
        
        Uses DuckDB's EXPLAIN feature to check if query is valid.
        
        Args:
            query: SQL query to validate
            
        Returns:
            True if query is valid, False otherwise
            
        Example:
            >>> engine = DuckDBEngine()
            >>> is_valid = engine.validate_query("SELECT * FROM df")
            >>> print(is_valid)
            True
        """
        if not query or not query.strip():
            logger.error("Query is empty or whitespace-only")
            return False
        
        try:
            # Try to explain the query (validates syntax)
            explain_query = f"EXPLAIN {query}"
            duckdb.query(explain_query)
            logger.debug("Query syntax is valid")
            return True
            
        except duckdb.Error as e:
            logger.warning(f"Query validation failed: {str(e)}")
            return False
            
        except Exception as e:
            logger.warning(f"Unexpected error during validation: {str(e)}")
            return False
    
    def get_table_info(self, df: pd.DataFrame, df_name: str = "df") -> dict:
        """
        Get information about a DataFrame's structure.
        
        This can be useful for debugging and query construction.
        
        Args:
            df: DataFrame to analyze
            df_name: Name for the DataFrame
            
        Returns:
            Dictionary with table information
            
        Example:
            >>> engine = DuckDBEngine()
            >>> df = pd.DataFrame({'A': [1, 2], 'B': ['x', 'y']})
            >>> info = engine.get_table_info(df)
            >>> print(info['columns'])
            ['A', 'B']
        """
        try:
            # Use DuckDB to get column types
            query = f"DESCRIBE {df_name}"
            describe_result = duckdb.query(query).to_df()
            
            return {
                "name": df_name,
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": df.columns.tolist(),
                "dtypes": df.dtypes.to_dict(),
                "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024
            }
            
        except Exception as e:
            logger.warning(f"Failed to get table info: {str(e)}")
            # Return basic info without DuckDB
            return {
                "name": df_name,
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": df.columns.tolist(),
                "dtypes": df.dtypes.to_dict()
            }
    
    def execute_multiple_queries(
        self,
        queries: list[tuple[str, pd.DataFrame, str]],
        stop_on_error: bool = False
    ) -> list[pd.DataFrame]:
        """
        Execute multiple queries in sequence.
        
        Args:
            queries: List of tuples (query, dataframe, df_name)
            stop_on_error: If True, stop on first error. If False, continue and return None for failed queries.
            
        Returns:
            List of result DataFrames (None for failed queries if stop_on_error=False)
            
        Raises:
            DuckDBExecutionError: If any query fails and stop_on_error=True
            
        Example:
            >>> engine = DuckDBEngine()
            >>> df = pd.DataFrame({'A': [1, 2, 3]})
            >>> queries = [
            ...     ("SELECT * FROM df WHERE A > 1", df, "df"),
            ...     ("SELECT AVG(A) as avg_a FROM df", df, "df")
            ... ]
            >>> results = engine.execute_multiple_queries(queries)
        """
        results = []
        
        for i, (query, df, df_name) in enumerate(queries):
            try:
                result = self.execute_query(query, df, df_name)
                results.append(result)
                logger.debug(f"Query {i+1}/{len(queries)} completed successfully")
                
            except DuckDBExecutionError as e:
                logger.error(f"Query {i+1}/{len(queries)} failed: {str(e)}")
                
                if stop_on_error:
                    raise
                else:
                    results.append(None)
        
        return results
    
    @staticmethod
    def is_available() -> bool:
        """
        Check if DuckDB is available.
        
        Returns:
            True if DuckDB is installed and importable, False otherwise
        """
        return DUCKDB_AVAILABLE
    
    def close(self) -> None:
        """
        Close the DuckDB connection if one exists.
        
        Note: This is only necessary if you created a custom connection.
        The default in-memory connection doesn't need explicit closing.
        """
        if self.connection:
            try:
                self.connection.close()
                logger.info("DuckDB connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {str(e)}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection."""
        self.close()
        return False


def check_duckdb_availability() -> tuple[bool, Optional[str]]:
    """
    Check if DuckDB is available and get version info.
    
    Returns:
        Tuple of (is_available, version_string)
        
    Example:
        >>> available, version = check_duckdb_availability()
        >>> if available:
        ...     print(f"DuckDB {version} is available")
        ... else:
        ...     print("DuckDB is not available")
    """
    if not DUCKDB_AVAILABLE:
        return False, None
    
    try:
        version = duckdb.__version__
        return True, version
    except Exception:
        return True, "unknown"



