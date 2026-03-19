"""
Handler Registry - Central registry for all chart type handlers
================================================================

Provides a centralized, extensible registry for mapping chart types to their
corresponding handler implementations.

Usage:
    handler = get_handler("bar_horizontal")
    context = handler.get_context_for_llm(parsed_inputs)
"""

import logging
from typing import Type
from .base import BaseChartHandler
from .bar_horizontal_handler import BarHorizontalHandler
from .bar_vertical_handler import BarVerticalHandler
from .bar_vertical_composed_handler import BarVerticalComposedHandler
from .bar_vertical_stacked_handler import BarVerticalStackedHandler
from .line_handler import LineHandler
from .line_composed_handler import LineComposedHandler
from .pie_handler import PieHandler
from .histogram_handler import HistogramHandler

logger = logging.getLogger(__name__)


# Registry mapping chart types to handler classes
HANDLER_REGISTRY: dict[str, Type[BaseChartHandler]] = {
    "bar_horizontal": BarHorizontalHandler,
    "bar_vertical": BarVerticalHandler,
    "bar_vertical_composed": BarVerticalComposedHandler,
    "bar_vertical_stacked": BarVerticalStackedHandler,
    "line": LineHandler,
    "line_composed": LineComposedHandler,
    "pie": PieHandler,
    "histogram": HistogramHandler,
}


def get_handler(chart_type: str) -> BaseChartHandler:
    """
    Get handler instance for specified chart type.

    This is the main entry point for obtaining chart-specific handlers.
    The registry is extensible - new chart types can be added by:
    1. Creating a new handler class inheriting from BaseChartHandler
    2. Implementing all abstract methods
    3. Adding to HANDLER_REGISTRY

    Args:
        chart_type: Chart type identifier (e.g., "bar_horizontal", "line", "pie")

    Returns:
        Instance of the appropriate handler class

    Raises:
        ValueError: If chart_type is not registered

    Example:
        >>> handler = get_handler("bar_horizontal")
        >>> handler.get_chart_description()
        'gráfico de barras horizontais apresentando ranking de categorias por métrica'
    """
    handler_class = HANDLER_REGISTRY.get(chart_type)

    if not handler_class:
        available_types = ", ".join(HANDLER_REGISTRY.keys())
        error_msg = (
            f"No handler registered for chart_type: '{chart_type}'. "
            f"Available types: {available_types}"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.debug(
        f"Retrieved handler for chart_type='{chart_type}': {handler_class.__name__}"
    )
    return handler_class()


def is_chart_type_supported(chart_type: str) -> bool:
    """
    Check if a chart type is supported (has a registered handler).

    Args:
        chart_type: Chart type identifier to check

    Returns:
        True if chart type is supported, False otherwise

    Example:
        >>> is_chart_type_supported("bar_horizontal")
        True
        >>> is_chart_type_supported("unknown_type")
        False
    """
    return chart_type in HANDLER_REGISTRY


def get_supported_chart_types() -> list[str]:
    """
    Get list of all supported chart types.

    Returns:
        List of chart type identifiers that have registered handlers

    Example:
        >>> get_supported_chart_types()
        ['bar_horizontal', 'bar_vertical', 'line', 'pie', ...]
    """
    return list(HANDLER_REGISTRY.keys())


# Cache for handler instances (optional optimization)
_handler_cache: dict[str, BaseChartHandler] = {}


def get_handler_cached(chart_type: str) -> BaseChartHandler:
    """
    Get handler with caching for repeated requests.

    This cached version reuses handler instances to avoid repeated
    instantiation. Use this if handlers are called multiple times
    for the same chart type within a session.

    Args:
        chart_type: Chart type identifier

    Returns:
        Cached instance of the appropriate handler class

    Raises:
        ValueError: If chart_type is not registered
    """
    if chart_type not in _handler_cache:
        _handler_cache[chart_type] = get_handler(chart_type)
        logger.debug(f"Cached new handler for chart_type='{chart_type}'")
    else:
        logger.debug(f"Retrieved cached handler for chart_type='{chart_type}'")

    return _handler_cache[chart_type]
