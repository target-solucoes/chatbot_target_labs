"""
Router - Conditional routing logic for formatter workflow
==========================================================

Defines conditional routing functions for the formatter agent workflow.
Routes execution based on state conditions (errors, status, data availability).
"""

import logging
from typing import Literal

from ..graph.state import FormatterState

logger = logging.getLogger(__name__)


# ============================================================================
# CONDITIONAL ROUTERS
# ============================================================================


def should_continue_or_error(
    state: FormatterState,
) -> Literal["continue", "handle_error"]:
    """
    Determine if workflow should continue or handle error.

    This router is used after parse_inputs_node to check if
    inputs were successfully validated.

    Args:
        state: Current FormatterState

    Returns:
        "continue": If parsing was successful, proceed to handler selection
        "handle_error": If parsing failed, route to error handling
    """
    status = state.get("status", "")
    error = state.get("error")

    if status == "error" or error:
        logger.warning(
            f"Routing to error handler due to status='{status}' or error='{error}'"
        )
        return "handle_error"

    logger.debug("Parsing successful, continuing to handler selection")
    return "continue"


def check_handler_selection(
    state: FormatterState,
) -> Literal["generate", "handle_error"]:
    """
    Check if handler selection was successful.

    This router is used after select_handler_node to verify
    that a valid handler was selected.

    Args:
        state: Current FormatterState

    Returns:
        "generate": If handler selected successfully, proceed to generation
        "handle_error": If handler selection failed, route to error handling
    """
    status = state.get("status", "")
    error = state.get("error")
    chart_handler = state.get("chart_handler")

    if status == "error" or error or not chart_handler:
        logger.warning(
            f"Routing to error handler - handler selection failed: "
            f"status='{status}', error='{error}', handler='{chart_handler}'"
        )
        return "handle_error"

    logger.debug(f"Handler '{chart_handler}' selected successfully, continuing")
    return "generate"


def check_generation_status(
    state: FormatterState,
) -> Literal["assemble", "handle_error"]:
    """
    Check if all generation steps completed successfully.

    This router is used after all generation nodes (executive summary,
    insights, next steps, data table) to verify completion before assembly.

    Args:
        state: Current FormatterState

    Returns:
        "assemble": If all generations successful, proceed to assembly
        "handle_error": If any critical failure occurred, route to error handling
    """
    status = state.get("status", "")
    error = state.get("error")

    # Check for critical errors
    if status == "error" or (error and "critical" in error.lower()):
        logger.warning(
            f"Routing to error handler - critical error in generation: "
            f"status='{status}', error='{error}'"
        )
        return "handle_error"

    # Non-critical errors (like LLM fallbacks) are acceptable
    # We can still assemble output with fallback data
    logger.debug(
        "Generation complete (with or without fallbacks), proceeding to assembly"
    )
    return "assemble"


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def has_critical_error(state: FormatterState) -> bool:
    """
    Check if state contains a critical error that prevents continuation.

    Args:
        state: Current FormatterState

    Returns:
        True if critical error detected, False otherwise
    """
    error = state.get("error")
    if not error:
        return False

    # Critical error indicators
    critical_keywords = [
        "critical",
        "fatal",
        "missing required field",
        "cannot select handler",
        "unsupported chart_type",
    ]

    return any(keyword in error.lower() for keyword in critical_keywords)


def is_status_successful(state: FormatterState) -> bool:
    """
    Check if current status indicates successful processing.

    Args:
        state: Current FormatterState

    Returns:
        True if status is successful, False otherwise
    """
    status = state.get("status", "")
    successful_statuses = [
        "parsing_complete",
        "handler_selected",
        "executive_summary_complete",
        "insights_synthesized",
        "next_steps_complete",
        "table_formatted",
        "success",
    ]

    return status in successful_statuses
