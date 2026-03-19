"""
Formatters module for Analytics Executor Agent - Refactored.

This module contains components for formatting execution results into
structured JSON output. Legacy plotly_config_builder has been removed
in Fase 5 (logic moved to individual tool handlers).

Components:
- result_formatter: Format analytics results for output
"""

from src.analytics_executor.formatters.result_formatter import (
    ResultFormatter,
    ResultFormatterError,
    format_analytics_result,
    format_error_result
)

__all__ = [
    # Result Formatter
    "ResultFormatter",
    "ResultFormatterError",
    "format_analytics_result",
    "format_error_result",
]
