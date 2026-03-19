"""
Validation Module for Analytics Executor

This module provides validation functionality for query results,
ensuring compatibility between data output and chart type requirements.
"""

from src.analytics_executor.validation.granularity_validator import (
    GranularityValidator,
    ValidationResult
)

__all__ = [
    'GranularityValidator',
    'ValidationResult'
]
