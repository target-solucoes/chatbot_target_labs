"""
Graph state definitions.

This module provides the GraphState TypedDict that is passed between
nodes in the LangGraph workflow.
"""

# Import GraphState from schema for easy access
from src.shared_lib.models.schema import GraphState

# Re-export for convenience
__all__ = ["GraphState"]


