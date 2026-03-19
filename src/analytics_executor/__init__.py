"""
Analytics Executor Agent Module.

This module contains the AnalyticsExecutorAgent responsible for:
- Parsing and validating JSON specifications
- Loading and validating data
- Building and executing queries (DuckDB/Pandas)
- Post-processing results
- Formatting output for Plotly visualization
"""

from .agent import AnalyticsExecutorAgent, execute_analytics

__all__ = [
    "AnalyticsExecutorAgent",
    "execute_analytics"
]
