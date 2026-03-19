"""
Non-graph executor agent for processing non-graphical queries.

This module provides the NonGraphExecutorAgent for handling queries that
don't require visualizations, such as metadata requests, aggregations,
lookups, and tabular data requests.
"""

from src.non_graph_executor.agent import NonGraphExecutorAgent

__all__ = [
    "NonGraphExecutorAgent",
]
