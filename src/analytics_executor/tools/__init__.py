"""
Tool handlers for different chart types.

Each tool handler is responsible for:
- Building SQL queries specific to its chart type
- Executing queries via DuckDB
- Building Plotly configuration
- Validating chart specifications
"""

from .base import BaseToolHandler, AnalyticsExecutionError
from .bar_horizontal import ToolHandlerBarHorizontal, tool_handle_bar_horizontal
from .bar_vertical import ToolHandlerBarVertical, tool_handle_bar_vertical

# REMOVED: bar_vertical_composed (migrated to line_composed)
from .bar_vertical_stacked import (
    ToolHandlerBarVerticalStacked,
    tool_handle_bar_vertical_stacked,
)
from .pie import ToolHandlerPie, tool_handle_pie
from .line import ToolHandlerLine, tool_handle_line
from .line_composed import ToolHandlerLineComposed, tool_handle_line_composed
from .histogram import ToolHandlerHistogram, tool_handle_histogram
from .null_chart import ToolHandlerNull, tool_handle_null

__all__ = [
    # Base
    "BaseToolHandler",
    "AnalyticsExecutionError",
    # Bar Horizontal
    "ToolHandlerBarHorizontal",
    "tool_handle_bar_horizontal",
    # Bar Vertical
    "ToolHandlerBarVertical",
    "tool_handle_bar_vertical",
    # REMOVED: Bar Vertical Composed (migrated to line_composed)
    # Bar Vertical Stacked
    "ToolHandlerBarVerticalStacked",
    "tool_handle_bar_vertical_stacked",
    # Pie
    "ToolHandlerPie",
    "tool_handle_pie",
    # Line
    "ToolHandlerLine",
    "tool_handle_line",
    # Line Composed
    "ToolHandlerLineComposed",
    "tool_handle_line_composed",
    # Histogram
    "ToolHandlerHistogram",
    "tool_handle_histogram",
    # Null
    "ToolHandlerNull",
    "tool_handle_null",
]
