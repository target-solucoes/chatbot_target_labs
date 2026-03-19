"""
Graph Classifier Agent - LangGraph-based intelligent chart classification system.

This package provides a modular, multi-agent system for:
- Natural language query classification (graphic_classifier)
- Analytics execution and visualization (analytics_executor)
- Shared utilities and schemas (shared_lib)

Architecture:
    src/
    ├── shared_lib/          # Common components used across agents
    ├── graphic_classifier/  # Chart classification agent
    ├── analytics_executor/  # Analytics execution agent
    └── pipeline_orchestrator.py  # Full pipeline integration
"""

__version__ = "0.0.1"

# Export main agent classes for convenience
from src.graphic_classifier import GraphicClassifierAgent
from src.analytics_executor import AnalyticsExecutorAgent

__all__ = [
    "GraphicClassifierAgent",
    "AnalyticsExecutorAgent",
]

