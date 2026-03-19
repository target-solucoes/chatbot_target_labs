"""
Insight Generator Agent.

This agent generates strategic insights from analytics results using
LLM-powered analysis and chart-type-specific metric calculations.

Main Components:
    - InsightState: TypedDict for workflow state management
    - BaseCalculator: Abstract base for metric calculators
    - create_insight_generator_workflow: Main workflow factory
    - execute_workflow: Convenience function for execution

Usage:
    >>> from src.insight_generator import execute_workflow
    >>> result = execute_workflow(chart_spec, analytics_result)
    >>> for insight in result["insights"]:
    ...     print(f"{insight['title']}: {insight['content']}")

FASE 1: Base structure with placeholder implementations.
"""

# Core configuration
from .core import (
    OPENAI_API_KEY,
    OPENAI_MODEL,
    REASONING_EFFORT,
    MAX_COMPLETION_TOKENS,
    MAX_INSIGHTS,
    TRANSPARENCY_THRESHOLD,
    DEFAULT_TOP_N,
    VALID_CHART_TYPES,
    validate_settings,
)

# Graph components
from .graph import (
    InsightState,
    create_insight_generator_workflow,
    create_workflow,
    visualize_workflow,
    execute_workflow,
    initialize_state,
)

# Calculators
from .calculators import BaseCalculator

# Version info
__version__ = "0.1.0"
__phase__ = "FASE 1 - Base Structure"

__all__ = [
    # Core
    "OPENAI_API_KEY",
    "OPENAI_MODEL",
    "REASONING_EFFORT",
    "MAX_COMPLETION_TOKENS",
    "MAX_INSIGHTS",
    "TRANSPARENCY_THRESHOLD",
    "DEFAULT_TOP_N",
    "VALID_CHART_TYPES",
    "validate_settings",
    # Graph
    "InsightState",
    "create_insight_generator_workflow",
    "create_workflow",
    "visualize_workflow",
    "execute_workflow",
    "initialize_state",
    # Calculators
    "BaseCalculator",
    # Metadata
    "__version__",
    "__phase__",
]
