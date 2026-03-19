"""
Core module for non_graph_executor.

This module provides settings validation and configuration
for the non-graph executor agent.
"""

from src.non_graph_executor.core.settings import (
    DATA_PATH,
    ALIAS_PATH,
    validate_settings,
)

__all__ = [
    "DATA_PATH",
    "ALIAS_PATH",
    "validate_settings",
]
