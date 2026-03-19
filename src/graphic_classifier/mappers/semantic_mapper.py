"""
Semantic Mapper - Deterministic mapping from semantic anchors to chart families.

This module implements the THIRD LAYER of the semantic-first architecture.
It maps semantic goals to chart families using HARD RULES (no AI, no heuristics).

CRITICAL RULES:
- Mapping is 100% DETERMINISTIC (no probabilities, no LLM)
- Based on canonical mapping table from documentation
- CANNOT be contradicted by heuristics downstream
- Raises errors if semantic anchor doesn't meet requirements

References:
- graph_classifier_diagnosis.md (Section 1.3 on invariants)
- graph_classifier_correction.md (Canonical mapping table)
"""

import logging
from typing import Literal
from dataclasses import dataclass

from src.graphic_classifier.llm.semantic_anchor import SemanticAnchor

logger = logging.getLogger(__name__)


# ============================================================================
# CHART FAMILY TYPE (SEMANTIC TYPES)
# ============================================================================


ChartFamily = Literal[
    "line_composed",  # Temporal variation/trend (semantic type)
    "bar_horizontal",  # Ranking (categorical comparison)
    "pie",  # Distribution/composition (proportional)
    "bar_vertical_stacked",  # Hierarchical composition
    "bar_vertical",  # Simple categorical comparison
    "histogram",  # Value distribution
    None,  # Factual/textual response
]


# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================


class SemanticMappingError(Exception):
    """Raised when semantic anchor cannot be mapped to a chart family."""

    pass


class InvariantViolationError(SemanticMappingError):
    """Raised when a semantic invariant is violated."""

    pass


# ============================================================================
# MAPPING RESULT
# ============================================================================


@dataclass
class SemanticMappingResult:
    """
    Result of semantic mapping.

    Fields:
        chart_family: Mapped chart family (semantic type)
        requires_temporal_dimension: Whether a temporal dimension is required
        requires_categorical_dimension: Whether a categorical dimension is required
        sort_order: Required sort order (based on polarity)
        sort_by: What to sort by ("value" for standard, "variation" for temporal comparison)
        reasoning: Explanation of the mapping decision
    """

    chart_family: ChartFamily
    requires_temporal_dimension: bool
    requires_categorical_dimension: bool
    sort_order: Literal["asc", "desc", None]
    sort_by: Literal["value", "variation", None]
    reasoning: str


# ============================================================================
# SEMANTIC MAPPER
# ============================================================================


class SemanticMapper:
    """
    Maps semantic anchors to chart families using deterministic rules.

    This mapper implements the canonical mapping table that translates
    semantic goals into chart families. The mapping is 100% deterministic
    and based on hard invariants.

    CANONICAL MAPPING TABLE:

    | semantic_goal     | comparison_axis | Chart Family          | Temporal Dim? | Sort Order        |
    |-------------------|----------------|-----------------------|---------------|-------------------|
    | compare_variation | temporal       | line_composed         | REQUIRED      | Based on polarity |
    | compare_variation | categorical    | bar_vertical          | NO            | Based on polarity |
    | ranking           | temporal       | line_composed         | REQUIRED      | Based on polarity |
    | ranking           | categorical    | bar_horizontal        | NO            | Based on polarity |
    | trend             | temporal       | line_composed         | REQUIRED      | None              |
    | distribution      | categorical    | pie                   | NO            | None              |
    | distribution      | none           | pie                   | NO            | None              |
    | composition       | categorical    | bar_vertical_stacked  | NO            | None              |
    | factual           | *              | None                  | NO            | None              |

    INVARIANTS (HARD RULES):
    - I1: Temporal comparison ALWAYS -> line_composed
    - I2: compare_variation NEVER -> pie
    - I3: ranking NEVER -> line (only bar_horizontal or line_composed)
    - I4: Negative polarity ALWAYS -> sort_order = "asc"
    - I5: Positive polarity ALWAYS -> sort_order = "desc"

    Usage:
        mapper = SemanticMapper()
        result = mapper.map(semantic_anchor)
        print(result.chart_family)  # "line_composed"
        print(result.sort_order)  # "asc"
    """

    def __init__(self):
        """Initialize the mapper."""
        logger.info("[SemanticMapper] Initialized with canonical mapping table")

    def map(self, anchor: SemanticAnchor) -> SemanticMappingResult:
        """
        Map semantic anchor to chart family using deterministic rules.

        Args:
            anchor: Semantic anchor to map

        Returns:
            SemanticMappingResult: Mapping result with chart family and requirements

        Raises:
            SemanticMappingError: If anchor cannot be mapped
            InvariantViolationError: If a semantic invariant is violated

        Example:
            >>> mapper = SemanticMapper()
            >>> anchor = SemanticAnchor(
            ...     semantic_goal="compare_variation",
            ...     comparison_axis="temporal",
            ...     polarity="negative",
            ...     requires_time_series=True,
            ...     entity_scope="vendas",
            ...     confidence=0.95,
            ...     reasoning="Temporal comparison"
            ... )
            >>> result = mapper.map(anchor)
            >>> assert result.chart_family == "line_composed"
            >>> assert result.sort_order == "asc"
        """
        logger.info(
            f"[SemanticMapper] Mapping anchor: goal={anchor.semantic_goal}, "
            f"axis={anchor.comparison_axis}, polarity={anchor.polarity}"
        )

        # Initialize result fields
        chart_family = None
        requires_temporal_dimension = False
        requires_categorical_dimension = False
        sort_order = None
        sort_by = None  # "value" for standard sorting, "variation" for delta
        reasoning = ""

        # ====================================================================
        # INVARIANT CHECKS (HARD RULES)
        # ====================================================================

        # I1: Temporal comparison ALWAYS -> line_composed
        if anchor.comparison_axis == "temporal":
            if anchor.semantic_goal in ["compare_variation", "trend", "ranking"]:
                chart_family = "line_composed"
                requires_temporal_dimension = True
                reasoning = f"I1: Temporal {anchor.semantic_goal} maps to line_composed"

                # CRITICAL: compare_variation needs to sort by delta (variation)
                # This ensures SQL orders by (last_period - first_period)
                if anchor.semantic_goal == "compare_variation":
                    sort_by = "variation"
                    logger.info(
                        "[SemanticMapper] compare_variation detected: sort_by='variation' "
                        "(will order by delta between periods)"
                    )
                else:
                    sort_by = "value"
            else:
                raise InvariantViolationError(
                    f"INVARIANT I1 VIOLATED: temporal comparison with semantic_goal="
                    f"'{anchor.semantic_goal}' is not supported. "
                    f"Expected: compare_variation, trend, or ranking."
                )

        # ====================================================================
        # SEMANTIC GOAL MAPPING
        # ====================================================================

        elif anchor.semantic_goal == "compare_variation":
            # I2: compare_variation NEVER -> pie
            if anchor.comparison_axis == "categorical":
                chart_family = "bar_vertical"
                requires_categorical_dimension = True
                reasoning = "Categorical variation maps to bar_vertical"
            elif anchor.comparison_axis == "none":
                raise SemanticMappingError(
                    "compare_variation requires a comparison_axis (temporal or categorical), "
                    "but got 'none'"
                )
            # Temporal case already handled above

        elif anchor.semantic_goal == "ranking":
            # I3: ranking NEVER -> line (only bar_horizontal or line_composed)
            if anchor.comparison_axis == "categorical":
                chart_family = "bar_horizontal"
                requires_categorical_dimension = True
                reasoning = "Categorical ranking maps to bar_horizontal"
            elif anchor.comparison_axis == "none":
                # Default to bar_horizontal for ranking without explicit axis
                chart_family = "bar_horizontal"
                requires_categorical_dimension = True
                reasoning = "Ranking without axis defaults to bar_horizontal"
            # Temporal case already handled above

        elif anchor.semantic_goal == "trend":
            # INVARIANT I3: Trend ALWAYS maps to line_composed (temporal series)
            # This is a semantic type, not a visual variant
            # Visual decision (single_line vs multi_line) happens in RenderSelector
            if anchor.comparison_axis != "temporal":
                logger.warning(
                    f"[SemanticMapper] Trend with axis={anchor.comparison_axis} is unusual, "
                    f"forcing temporal axis"
                )
            chart_family = "line_composed"
            requires_temporal_dimension = True
            reasoning = "I3: Trend analysis maps to line_composed (semantic type, render variant decided later)"

        elif anchor.semantic_goal == "distribution":
            chart_family = "pie"
            requires_categorical_dimension = True
            reasoning = "Distribution maps to pie chart"

        elif anchor.semantic_goal == "composition":
            chart_family = "bar_vertical_stacked"
            requires_categorical_dimension = True
            reasoning = "Hierarchical composition maps to bar_vertical_stacked"

        elif anchor.semantic_goal == "factual":
            chart_family = None
            reasoning = "Factual query requires textual response, no chart"

        else:
            raise SemanticMappingError(
                f"Unknown semantic_goal: '{anchor.semantic_goal}'"
            )

        # ====================================================================
        # POLARITY -> SORT ORDER MAPPING (INVARIANTS I4 & I5)
        # ====================================================================

        if anchor.polarity == "negative":
            # I4: Negative polarity ALWAYS -> sort_order = "asc" (Bottom N)
            sort_order = "asc"
            logger.debug("[SemanticMapper] I4: Negative polarity -> sort_order='asc'")

        elif anchor.polarity == "positive":
            # I5: Positive polarity ALWAYS -> sort_order = "desc" (Top N)
            sort_order = "desc"
            logger.debug("[SemanticMapper] I5: Positive polarity -> sort_order='desc'")

        else:
            # Neutral polarity -> no sort order preference
            sort_order = None

        # ====================================================================
        # FINAL VALIDATION
        # ====================================================================

        # Validate that chart_family was determined
        if chart_family is None and anchor.semantic_goal != "factual":
            raise SemanticMappingError(
                f"Failed to determine chart_family for semantic_goal={anchor.semantic_goal}, "
                f"comparison_axis={anchor.comparison_axis}"
            )

        # Create result
        result = SemanticMappingResult(
            chart_family=chart_family,
            requires_temporal_dimension=requires_temporal_dimension,
            requires_categorical_dimension=requires_categorical_dimension,
            sort_order=sort_order,
            sort_by=sort_by
            if sort_by
            else "value",  # Default to "value" if not specified
            reasoning=reasoning,
        )

        logger.info(
            f"[SemanticMapper] Mapped to: chart_family={result.chart_family}, "
            f"sort_order={result.sort_order}"
        )
        logger.debug(f"[SemanticMapper] Reasoning: {result.reasoning}")

        return result

    def validate_invariants(self, anchor: SemanticAnchor, chart_family: str) -> None:
        """
        Validate that a proposed chart_family doesn't violate semantic invariants.

        This method is used to check if a heuristic or other downstream process
        is trying to contradict the semantic anchor.

        Args:
            anchor: Semantic anchor
            chart_family: Proposed chart family to validate

        Raises:
            InvariantViolationError: If the proposed chart_family violates invariants

        Example:
            >>> mapper = SemanticMapper()
            >>> anchor = SemanticAnchor(..., semantic_goal="compare_variation", ...)
            >>> mapper.validate_invariants(anchor, "pie")  # Raises InvariantViolationError
        """
        logger.debug(
            f"[SemanticMapper] Validating invariants: "
            f"goal={anchor.semantic_goal}, proposed_chart={chart_family}"
        )

        # I1: Temporal comparison ALWAYS -> line_composed
        if anchor.comparison_axis == "temporal":
            if chart_family != "line_composed":
                raise InvariantViolationError(
                    f"INVARIANT I1 VIOLATED: temporal comparison must use line_composed, "
                    f"but '{chart_family}' was proposed"
                )

        # I2: compare_variation NEVER -> pie
        if anchor.semantic_goal == "compare_variation":
            if chart_family == "pie":
                raise InvariantViolationError(
                    f"INVARIANT I2 VIOLATED: compare_variation cannot use pie chart"
                )

        # I3: ranking NEVER -> line
        if anchor.semantic_goal == "ranking":
            if chart_family == "line":
                raise InvariantViolationError(
                    f"INVARIANT I3 VIOLATED: ranking cannot use 'line' chart "
                    f"(deprecated type). Use 'bar_horizontal' or 'line_composed' instead."
                )

        # FASE 2: line is DEPRECATED - only line_composed is valid for temporal series
        if chart_family == "line":
            raise InvariantViolationError(
                f"INVARIANT VIOLATED: 'line' is deprecated as a chart_family. "
                f"Use 'line_composed' (semantic type) instead. "
                f"Visual variant (single_line vs multi_line) is decided by RenderSelector."
            )

        # FASE 3: bar_vertical_composed is DEPRECATED - migrated to line_composed
        if chart_family == "bar_vertical_composed":
            raise InvariantViolationError(
                f"INVARIANT VIOLATED: 'bar_vertical_composed' is deprecated and REMOVED. "
                f"Temporal comparisons must use 'line_composed' (semantic type). "
                f"This type should NEVER be generated by the system."
            )

        logger.debug("[SemanticMapper] Invariants validated successfully")
