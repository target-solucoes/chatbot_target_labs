"""
Execution module for Analytics Executor Agent - Refactored.

This module contains components for executing queries using DuckDB engine
and post-processing results. Legacy query_executor, query_builder,
query_strategy and pandas_engine have been removed in Fase 5.

Components:
- duckdb_engine: DuckDB execution (used by tool handlers)
- post_processor: Result post-processing and validation
- filter_normalizer: Filter normalization
- temporal_analyzer: Temporal analysis utilities
"""

from src.analytics_executor.execution.duckdb_engine import (
    DuckDBEngine,
    DuckDBExecutionError,
    check_duckdb_availability
)

from src.analytics_executor.execution.post_processor import (
    PostProcessor,
    PostProcessorError,
    post_process_result
)

from src.analytics_executor.execution.filter_normalizer import (
    FilterNormalizer,
    normalize_filters
)

from src.analytics_executor.execution.temporal_analyzer import (
    TemporalAnalyzer,
    get_temporal_analyzer
)

__all__ = [
    # DuckDB Engine
    "DuckDBEngine",
    "DuckDBExecutionError",
    "check_duckdb_availability",

    # Post Processor
    "PostProcessor",
    "PostProcessorError",
    "post_process_result",

    # Filter Normalizer
    "FilterNormalizer",
    "normalize_filters",

    # Temporal Analyzer
    "TemporalAnalyzer",
    "get_temporal_analyzer",
]
