"""
Conditional edge functions for filter_classifier workflow.

This module implements conditional routing logic for the LangGraph workflow.
"""

import logging
from typing import Literal

from src.filter_classifier.graph.state import FilterGraphState

logger = logging.getLogger(__name__)


def should_validate_filters(state: FilterGraphState) -> Literal["validate", "skip"]:
    """
    Edge 1: Determine if filter validation should be performed.

    Checks if any filters were detected in the query. If no filters were
    detected, skip validation to improve performance.

    Args:
        state: Current filter graph state

    Returns:
        "validate" if filters were detected, "skip" otherwise
    """
    detected_columns = state.get("detected_filter_columns", [])

    if detected_columns:
        logger.info(
            f"[should_validate_filters] {len(detected_columns)} filters detected, "
            "proceeding to validation"
        )
        return "validate"
    else:
        logger.info("[should_validate_filters] No filters detected, skipping validation")
        return "skip"


def has_validation_errors(state: FilterGraphState) -> Literal["error", "continue"]:
    """
    Edge 2: Check for critical validation errors.

    Examines the errors list to determine if there are critical errors that
    should prevent further processing. Non-critical warnings are allowed to
    continue.

    Args:
        state: Current filter graph state

    Returns:
        "error" if critical errors exist, "continue" otherwise
    """
    errors = state.get("errors", [])

    if not errors:
        logger.debug("[has_validation_errors] No errors, continuing")
        return "continue"

    # Define critical error patterns
    critical_patterns = [
        "Invalid columns:",
        "Parse error:",
        "Validation error:"
    ]

    # Check if any error is critical
    critical_errors = [
        err for err in errors
        if any(pattern in err for pattern in critical_patterns)
    ]

    if critical_errors:
        logger.warning(
            f"[has_validation_errors] Critical errors detected: {critical_errors}, "
            "skipping to output formatting"
        )
        return "error"
    else:
        logger.info(
            f"[has_validation_errors] Non-critical warnings detected: {errors}, "
            "continuing workflow"
        )
        return "continue"
