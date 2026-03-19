"""
Router functions for the Insight Generator workflow.

This module contains conditional edge functions that route workflow
execution based on chart_type or other state conditions.
"""

import logging
from .state import InsightState
from ..core.settings import VALID_CHART_TYPES

logger = logging.getLogger(__name__)


def route_by_chart_type(state: InsightState) -> str:
    """
    Route to different calculators based on chart_type.

    This function can be used in conditional edges if the workflow
    needs different paths for different chart types. Currently, all
    chart types follow the same pipeline since calculators are invoked
    dynamically via factory pattern.

    Args:
        state: Current workflow state (must contain chart_type)

    Returns:
        String indicating the next node to execute

    Example usage in workflow:
        workflow.add_conditional_edges(
            "parse_input",
            route_by_chart_type,
            {
                "bar_horizontal": "calculate_metrics",
                "bar_vertical": "calculate_metrics",
                "line": "calculate_metrics",
                # ... other routes
                "default": "calculate_metrics"
            }
        )
    """
    chart_type = state.get("chart_type", "unknown")

    logger.debug(f"[route_by_chart_type] Routing for chart_type: {chart_type}")

    # Validate chart_type
    if chart_type in VALID_CHART_TYPES:
        logger.debug(f"[route_by_chart_type] Valid chart_type, using standard route")
        return "calculate_metrics"
    else:
        logger.warning(
            f"[route_by_chart_type] Unknown chart_type: {chart_type}, "
            f"using default route"
        )
        return "calculate_metrics"


def should_continue(state: InsightState) -> str:
    """
    Determine if workflow should continue or end based on errors.

    This can be used as a conditional edge to short-circuit the workflow
    if critical errors are encountered. Currently configured to always
    continue to ensure graceful degradation.

    Args:
        state: Current workflow state

    Returns:
        "continue" if workflow should proceed, "end" if it should stop

    Example usage in workflow:
        workflow.add_conditional_edges(
            "parse_input",
            should_continue,
            {
                "continue": "calculate_metrics",
                "end": END
            }
        )
    """
    errors = state.get("errors", [])

    if errors:
        logger.warning(f"[should_continue] Errors detected: {len(errors)} error(s)")

        # Check if errors are critical (parse_input failures are critical)
        critical_errors = [e for e in errors if "parse_input_node" in e]

        if critical_errors:
            logger.error(f"[should_continue] Critical errors found, stopping workflow")
            return "end"

        # Non-critical errors - continue with graceful degradation
        logger.info(f"[should_continue] Non-critical errors, continuing workflow")
        return "continue"

    logger.debug("[should_continue] No errors, continuing workflow")
    return "continue"


def should_invoke_llm(state: InsightState) -> str:
    """
    Determine if LLM should be invoked based on state validation.

    This can be used to skip LLM invocation if metrics calculation failed
    or if prompt building encountered issues.

    Args:
        state: Current workflow state

    Returns:
        "invoke" if LLM should be called, "skip" to bypass LLM

    Example usage in workflow:
        workflow.add_conditional_edges(
            "build_prompt",
            should_invoke_llm,
            {
                "invoke": "invoke_llm",
                "skip": "format_output"
            }
        )
    """
    errors = state.get("errors", [])
    llm_prompt = state.get("llm_prompt")

    # Check for errors that would prevent LLM invocation
    if errors:
        logger.warning(f"[should_invoke_llm] Errors present, skipping LLM invocation")
        return "skip"

    # Check if prompt was successfully built
    if not llm_prompt or not llm_prompt.strip():
        logger.warning(f"[should_invoke_llm] No valid prompt, skipping LLM invocation")
        return "skip"

    logger.debug("[should_invoke_llm] Conditions met, invoking LLM")
    return "invoke"


def route_by_data_quality(state: InsightState) -> str:
    """
    Route based on data quality and completeness.

    This can be used to apply different processing strategies based on
    the quality of input data (e.g., missing values, small datasets).

    Args:
        state: Current workflow state

    Returns:
        String indicating data quality tier: "high", "medium", "low"

    Example usage in workflow:
        workflow.add_conditional_edges(
            "parse_input",
            route_by_data_quality,
            {
                "high": "calculate_metrics",
                "medium": "calculate_metrics",
                "low": "format_output"  # Skip processing for low quality
            }
        )
    """
    data = state.get("data")

    if data is None:
        logger.warning("[route_by_data_quality] No data available")
        return "low"

    # Check data size
    row_count = len(data)
    col_count = len(data.columns) if hasattr(data, "columns") else 0

    if row_count == 0 or col_count == 0:
        logger.warning(
            f"[route_by_data_quality] Empty data (rows={row_count}, cols={col_count})"
        )
        return "low"

    # Check for minimum viable data
    MIN_ROWS = 2
    if row_count < MIN_ROWS:
        logger.warning(
            f"[route_by_data_quality] Insufficient rows ({row_count} < {MIN_ROWS})"
        )
        return "low"

    # Check for missing values
    if hasattr(data, "isnull"):
        missing_pct = (data.isnull().sum().sum() / (row_count * col_count)) * 100

        if missing_pct > 50:
            logger.warning(
                f"[route_by_data_quality] High missing values ({missing_pct:.1f}%)"
            )
            return "medium"

    logger.debug("[route_by_data_quality] High quality data detected")
    return "high"
