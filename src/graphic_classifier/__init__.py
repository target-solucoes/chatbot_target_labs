"""
Graphic Classifier Agent Module.

This module contains the GraphicClassifierAgent responsible for:
- Processing natural language queries
- Classifying chart types
- Extracting metrics, dimensions, and filters
- Generating structured chart specifications
"""

from .agent import GraphicClassifierAgent, get_agent, classify_query

__all__ = [
    "GraphicClassifierAgent",
    "get_agent",
    "classify_query"
]
