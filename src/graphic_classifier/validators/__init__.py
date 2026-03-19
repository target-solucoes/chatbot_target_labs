"""
Chart Type Validators - FASE 5

This module provides cross-field validation for chart type classification,
detecting inconsistencies between chart_type and data structure (dimensions,
filters, temporal_granularity).

The validator acts as a safety net after classification, catching:
- bar_vertical_composed without 2 dimensions
- bar_horizontal with multi-value dimension
- line/line_composed without temporal dimension
- bar_vertical_stacked without composition

Reference: graph_classifier_diagnosis.md - FASE 5
"""

from src.graphic_classifier.validators.chart_validator import ChartTypeValidator

__all__ = ["ChartTypeValidator"]
