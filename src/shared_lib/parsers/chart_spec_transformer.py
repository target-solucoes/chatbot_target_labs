"""Chart Specification Transformer - FASE 3.3 Refactored"""

import logging
from typing import Dict, Any, List
from .transformation_pipeline import ChartSpecTransformationPipeline
from .transformation_functions import (
    infer_missing_metrics,
    infer_temporal_dimensions,
    normalize_aggregations,
    adjust_dimensions_by_chart_type,
    apply_chart_specific_fixes,
    CHART_TYPE_REQUIREMENTS,
    _get_temporal_columns,
)

logger = logging.getLogger(__name__)


def create_default_transformation_pipeline():
    pipeline = ChartSpecTransformationPipeline(name="chart_spec_default")
    pipeline.add_step(
        "infer_missing_metrics", infer_missing_metrics, "Ensure sufficient metrics"
    )
    pipeline.add_step(
        "infer_temporal_dimensions",
        infer_temporal_dimensions,
        "Add temporal dimensions",
    )
    pipeline.add_step(
        "normalize_aggregations", normalize_aggregations, "Normalize aggregations"
    )
    pipeline.add_step(
        "adjust_dimensions_by_chart_type",
        adjust_dimensions_by_chart_type,
        "Adjust dimensions",
    )
    pipeline.add_step(
        "apply_chart_specific_fixes", apply_chart_specific_fixes, "Apply fixes"
    )
    return pipeline


class ChartSpecTransformer:
    def __init__(self, available_columns=None):
        self.available_columns = set(available_columns) if available_columns else set()
        self.pipeline = create_default_transformation_pipeline()

    def transform(self, spec):
        chart_type = spec.get("chart_type")
        if not chart_type or chart_type == "null":
            return spec
        if chart_type not in CHART_TYPE_REQUIREMENTS:
            return spec
        return self.pipeline.transform(spec, stop_on_error=False)

    def get_last_execution_summary(self):
        return self.pipeline.get_execution_summary()


def validate_spec(spec):
    errors = []
    chart_type = spec.get("chart_type")
    if not chart_type or chart_type == "null":
        return True, []
    if chart_type not in CHART_TYPE_REQUIREMENTS:
        errors.append(f"Unknown chart type: {chart_type}")
        return False, errors
    requirements = CHART_TYPE_REQUIREMENTS[chart_type]
    metrics = spec.get("metrics", [])
    dimensions = spec.get("dimensions", [])
    if len(metrics) < requirements["min_metrics"]:
        errors.append(
            f"{chart_type} requires at least {requirements['min_metrics']} metric(s)"
        )
    min_dims = requirements["min_dimensions"]
    max_dims = requirements["max_dimensions"]

    # LAYER 6: Check for single_line variant in line_composed
    intent_config = spec.get("_intent_config") or spec.get("intent_config")
    if chart_type == "line_composed" and intent_config:
        dim_structure = intent_config.get("dimension_structure", {})
        if isinstance(dim_structure, dict) and dim_structure.get("series") is None:
            # single_line variant: only 1 dimension required
            min_dims = 1
            max_dims = 1

    if len(dimensions) < min_dims:
        errors.append(f"{chart_type} requires at least {min_dims} dimension(s)")
    if len(dimensions) > max_dims:
        errors.append(f"{chart_type} allows at most {max_dims} dimension(s)")
    if requirements["requires_temporal"]:
        temporal_cols = _get_temporal_columns()
        has_temporal = any(
            dim.get("name") in temporal_cols
            if isinstance(dim, dict)
            else dim in temporal_cols
            for dim in dimensions
        )
        if not has_temporal and temporal_cols:
            errors.append(f"{chart_type} requires a temporal dimension")
    return len(errors) == 0, errors
