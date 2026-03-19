"""
Calculators module for metric calculations.

This module provides:
1. Legacy calculator classes for each chart type (backward compatibility)
2. FASE 2: Atomic metric modules and MetricComposer for intent-based composition
"""

from typing import Dict, Type
from .base import BaseCalculator
from .ranking_calculator import RankingCalculator
from .temporal_calculator import TemporalCalculator
from .distribution_calculator import DistributionCalculator
from .comparison_calculator import ComparisonCalculator
from .composed_calculator import ComposedCalculator
from .stacked_calculator import StackedCalculator
from .temporal_multi_calculator import TemporalMultiCalculator
from .histogram_calculator import HistogramCalculator

# FASE 2 - New composable metric system
from .metric_modules import (
    MetricModule,
    VariationModule,
    ConcentrationModule,
    GapModule,
    TemporalModule,
    DistributionModule,
    ComparativeModule,
)
from .metric_composer import (
    MetricComposer,
    MODULE_REGISTRY,
    INTENT_MODULE_MAPPING,
    compose_metrics,
    get_modules_for_intent,
)


# Legacy Calculator Registry: Maps chart_type to Calculator class
# Maintained for backward compatibility
CALCULATOR_REGISTRY: Dict[str, Type[BaseCalculator]] = {
    "bar_horizontal": RankingCalculator,
    "bar_vertical": ComparisonCalculator,
    "bar_vertical_composed": ComposedCalculator,
    "bar_vertical_stacked": StackedCalculator,
    "line": TemporalCalculator,
    "line_composed": TemporalMultiCalculator,
    "pie": DistributionCalculator,
    "histogram": HistogramCalculator,
}


def get_calculator(chart_type: str) -> BaseCalculator:
    """
    Factory function to get calculator instance for a chart type

    Args:
        chart_type: The chart type identifier

    Returns:
        Calculator instance for the chart type

    Raises:
        ValueError: If chart_type is not supported

    Example:
        >>> calculator = get_calculator("bar_horizontal")
        >>> isinstance(calculator, RankingCalculator)
        True
    """
    calculator_class = CALCULATOR_REGISTRY.get(chart_type)

    if calculator_class is None:
        supported = ", ".join(CALCULATOR_REGISTRY.keys())
        raise ValueError(
            f"Unsupported chart_type: '{chart_type}'. Supported types: {supported}"
        )

    return calculator_class()


__all__ = [
    # Legacy calculators (backward compatibility)
    "BaseCalculator",
    "RankingCalculator",
    "TemporalCalculator",
    "DistributionCalculator",
    "ComparisonCalculator",
    "ComposedCalculator",
    "StackedCalculator",
    "TemporalMultiCalculator",
    "HistogramCalculator",
    "CALCULATOR_REGISTRY",
    "get_calculator",
    # FASE 2 - New composable metric system
    "MetricModule",
    "VariationModule",
    "ConcentrationModule",
    "GapModule",
    "TemporalModule",
    "DistributionModule",
    "ComparativeModule",
    "MetricComposer",
    "MODULE_REGISTRY",
    "INTENT_MODULE_MAPPING",
    "compose_metrics",
    "get_modules_for_intent",
]
