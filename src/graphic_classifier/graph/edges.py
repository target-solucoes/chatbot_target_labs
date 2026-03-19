"""
LangGraph workflow edges and conditional routing logic.

This module implements the conditional routing functions that determine
the flow of execution through the graph based on the current state.
"""

import logging
from typing import Literal

from src.graphic_classifier.graph.state import GraphState

logger = logging.getLogger(__name__)


def should_map_columns(state: GraphState) -> Literal["map", "no_chart"]:
    """
    Determine whether column mapping is needed based on classification result.

    This conditional edge function routes the workflow after intent classification:
    - If chart_type is not None: proceed to column mapping
    - If chart_type is None: skip to output generation (no visualization needed)

    Args:
        state: Current graph state after classification

    Returns:
        "map": Proceed to map_columns_node
        "no_chart": Skip to generate_output_node (no chart needed)

    Example:
        When chart_type is "bar_horizontal" -> returns "map"
        When chart_type is None (factual query) -> returns "no_chart"
    """
    chart_type = state.get("chart_type")

    if chart_type is None:
        logger.info(
            "[should_map_columns] No chart type identified, skipping column mapping"
        )
        return "no_chart"

    logger.info(
        f"[should_map_columns] Chart type '{chart_type}' identified, "
        "proceeding to column mapping"
    )
    return "map"


def should_retry_classification(state: GraphState) -> Literal["retry", "continue"]:
    """
    Determine whether to retry classification based on confidence level.

    This optional conditional edge can be used to retry classification
    if confidence is too low. Currently not used in the main workflow,
    but available for future enhancements.

    Args:
        state: Current graph state

    Returns:
        "retry": Retry classification with adjusted parameters
        "continue": Proceed with current classification
    """
    confidence = state.get("confidence", 0.0)
    retry_threshold = 0.3

    if confidence < retry_threshold:
        errors = state.get("errors", [])
        # Only retry once (check if we already have a retry error)
        if not any("retry" in str(e).lower() for e in errors):
            logger.warning(
                f"[should_retry_classification] Low confidence ({confidence}), "
                "considering retry"
            )
            return "retry"

    return "continue"


def has_errors(state: GraphState) -> Literal["error_path", "success_path"]:
    """
    Check if critical errors occurred during processing.

    This conditional edge can be used to route to error handling
    based on the presence of errors in the state.

    Args:
        state: Current graph state

    Returns:
        "error_path": Critical errors present, route to error handler
        "success_path": No critical errors, continue normal flow
    """
    errors = state.get("errors", [])

    # Define critical error patterns
    critical_patterns = [
        "llm initialization failed",
        "alias mapper failed to load",
        "unable to generate output",
    ]

    has_critical_error = any(
        any(pattern in str(error).lower() for pattern in critical_patterns)
        for error in errors
    )

    if has_critical_error:
        logger.error(f"[has_errors] Critical errors detected: {len(errors)} errors")
        return "error_path"

    if errors:
        logger.warning(
            f"[has_errors] Non-critical errors present: {len(errors)} errors"
        )

    return "success_path"


def requires_additional_context(
    state: GraphState,
) -> Literal["need_context", "no_context"]:
    """
    Determine if additional context is needed for accurate classification.

    This edge can be used to route to a context gathering node when
    the query is ambiguous or lacks sufficient information.

    Args:
        state: Current graph state

    Returns:
        "need_context": Additional context would improve classification
        "no_context": Sufficient information available
    """
    confidence = state.get("confidence", 0.0)
    chart_type = state.get("chart_type")
    columns_mentioned = state.get("columns_mentioned", [])

    # Need context if:
    # 1. Confidence is low-to-medium AND
    # 2. Chart type was determined BUT
    # 3. No columns were mentioned (might be too vague)

    if (0.3 < confidence < 0.7) and chart_type and not columns_mentioned:
        logger.info(
            "[requires_additional_context] Query may benefit from additional context"
        )
        return "need_context"

    return "no_context"


# ============================================================================
# EDGE VALIDATION HELPERS
# ============================================================================


def validate_state_for_routing(state: GraphState) -> bool:
    """
    Validate that the state has minimum required fields for routing decisions.

    Args:
        state: Current graph state

    Returns:
        True if state is valid for routing, False otherwise
    """
    required_fields = ["query", "chart_type", "confidence"]

    for field in required_fields:
        if field not in state:
            logger.error(
                f"[validate_state_for_routing] Missing required field: {field}"
            )
            return False

    return True


def should_attempt_fallback(state: GraphState) -> Literal["fallback", "success"]:
    """
    FASE 6: Determine if fallback is needed based on execution results.

    This conditional edge checks if visualization failed and fallback should be attempted:
    - If chart_type is null -> attempt fallback
    - If executor returned empty dataset -> attempt fallback
    - If critical validation failed -> attempt fallback
    - Otherwise -> success (proceed to final output)

    CRITICAL FIX (2025-12-22): Temporal chart types (line, line_composed) should NOT
    fallback to non_graph_executor without actual execution. These must always attempt
    graphical visualization first.

    Args:
        state: Current graph state after execution/validation

    Returns:
        "fallback": Visualization failed, attempt fallback/routing
        "success": Visualization succeeded, proceed normally
    """
    chart_type = state.get("chart_type")
    executor_output = state.get("executor_output", {})
    errors = state.get("errors", [])

    # Check 1: Null chart type
    if chart_type is None or chart_type == "null":
        logger.warning(
            "[should_attempt_fallback] Chart type is null, attempting fallback"
        )
        return "fallback"

    # CRITICAL FIX: Temporal charts must execute before fallback decision
    # Do not fallback for temporal chart types without actual execution results
    if chart_type in ["line", "line_composed"]:
        # If executor hasn't run yet, proceed to execution (don't fallback prematurely)
        if not executor_output:
            logger.info(
                f"[should_attempt_fallback] Temporal chart type '{chart_type}' detected. "
                "Proceeding to execution before considering fallback."
            )
            return "success"

        # If executor ran successfully with data, no fallback needed
        if executor_output.get("status") == "success":
            summary = executor_output.get("summary_table", {})
            total_rows = summary.get("total_rows", 0)

            if total_rows > 0:
                logger.info(
                    f"[should_attempt_fallback] Temporal chart has data ({total_rows} rows). "
                    "No fallback needed."
                )
                return "success"

    # Check 2: Empty dataset from executor
    if executor_output:
        summary = executor_output.get("summary_table", {})
        total_rows = summary.get("total_rows", 0)

        if total_rows == 0:
            logger.warning(
                "[should_attempt_fallback] Executor returned empty dataset, "
                "attempting fallback"
            )
            return "fallback"

    # Check 3: Critical validation errors
    critical_error_patterns = [
        "insufficient data",
        "dimensional mismatch",
        "negative values in pie",
        "too many categories",
    ]

    has_critical_error = any(
        any(
            pattern.lower() in str(error).lower() for pattern in critical_error_patterns
        )
        for error in errors
    )

    if has_critical_error:
        logger.warning(
            f"[should_attempt_fallback] Critical validation error detected, "
            "attempting fallback"
        )
        return "fallback"

    # Success - proceed normally
    logger.info("[should_attempt_fallback] Visualization succeeded, no fallback needed")
    return "success"


def should_route_after_fallback(
    state: GraphState,
) -> Literal["retry", "route_to_text", "final"]:
    """
    FASE 6: Determine routing after fallback attempt.

    After fallback manager runs, this edge decides next step:
    - If fallback found viable degraded chart -> retry with new chart type
    - If no visualization possible -> route to non_graph_executor
    - If already at final state -> end

    Args:
        state: Current graph state after fallback attempt

    Returns:
        "retry": Retry with degraded chart type (e.g., line -> bar)
        "route_to_text": Route to non_graph_executor (no viable visualization)
        "final": Proceed to final output
    """
    fallback_result = state.get("fallback_result", {})
    redirect_to = state.get("redirect_to")

    # Check 1: Should retry with degraded chart type
    if fallback_result.get("should_retry"):
        logger.info(
            f"[should_route_after_fallback] Retrying with degraded chart type: "
            f"{fallback_result.get('fallback_chart_type')}"
        )
        return "retry"

    # Check 2: Should route to text agent
    if (
        fallback_result.get("should_route_to_text")
        or redirect_to == "non_graph_executor"
    ):
        logger.warning(
            "[should_route_after_fallback] Routing to non_graph_executor "
            "(no viable visualization)"
        )
        return "route_to_text"

    # Default: proceed to final output
    logger.info("[should_route_after_fallback] Proceeding to final output")
    return "final"
