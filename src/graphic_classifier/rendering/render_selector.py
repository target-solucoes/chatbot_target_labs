"""
Render Selector - Visual variant selection for chart families.

This module implements the VISUAL LAYER of the semantic-first architecture.
It decides HOW to render a semantic chart family based on data characteristics.

CRITICAL SEPARATION:
- SemanticMapper decides WHAT the chart is (semantic type)
- RenderSelector decides HOW it looks (visual variant)

This separation ensures that:
1. Semantic decisions are independent of visual rendering
2. Same semantic type can have different visual representations
3. Visual decisions are made AFTER semantic classification

Example:
    "histórico de vendas em Joinville" → line_composed (semantic) → single_line (visual)
    "histórico de vendas em 5 cidades" → line_composed (semantic) → multi_line (visual)

References:
- graph_classifier_correction.md (Section 1.5: Render Selector)
- graph_classifier_diagnosis.md (Section: Família Line Mal Definida)
"""

import logging
from typing import Literal, Optional, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# ============================================================================
# RENDER VARIANT TYPE (VISUAL VARIANTS)
# ============================================================================


RenderVariant = Literal[
    "single_line",  # Line chart with single series
    "multi_line",  # Line chart with multiple series
    "single_bar",  # Bar chart with single category
    "multi_bar",  # Bar chart with multiple categories
    "simple_pie",  # Pie chart with few slices (<= 5)
    "detailed_pie",  # Pie chart with many slices (> 5)
    "single_stack",  # Stacked bar with single stack
    "multi_stack",  # Stacked bar with multiple stacks
    None,  # No rendering (textual response)
]


# ============================================================================
# RENDER DECISION
# ============================================================================


@dataclass
class RenderDecision:
    """
    Result of render variant selection.

    Fields:
        render_variant: Selected visual variant
        limit_applied: Whether data was limited (e.g., top 5)
        category_count: Number of categories in the data
        reasoning: Explanation of the rendering decision
    """

    render_variant: RenderVariant
    limit_applied: bool
    category_count: int
    reasoning: str


# ============================================================================
# RENDER SELECTOR
# ============================================================================


class RenderSelector:
    """
    Selects visual rendering variants based on chart family and data characteristics.

    This selector implements the visual layer that decides how to render
    a semantic chart family. It operates AFTER semantic classification.

    RENDERING RULES:

    For line_composed (Temporal Series):
    - 1 category → single_line
    - 2-5 categories → multi_line
    - > 5 categories → multi_line (limited to top 5)

    For bar_vertical (Categorical Comparison):
    - 1 category → single_bar
    - 2-10 categories → multi_bar
    - > 10 categories → multi_bar (limited to top 10)

    For bar_horizontal (Ranking):
    - Always limited to top/bottom N (default: 10)

    For pie (Distribution):
    - <= 5 slices → simple_pie
    - > 5 slices → detailed_pie (limited to top 5 + "Others")

    Usage:
        selector = RenderSelector()
        decision = selector.select("line_composed", categories=["Joinville"])
        print(decision.render_variant)  # "single_line"
        print(decision.limit_applied)  # False
    """

    # Default limits for different chart families
    DEFAULT_LIMITS = {
        "line_composed": 5,
        "bar_vertical": 10,
        "bar_horizontal": 10,
        "pie": 5,
        "bar_vertical_stacked": 10,
    }

    def __init__(self, custom_limits: Optional[dict] = None):
        """
        Initialize the render selector.

        Args:
            custom_limits: Optional custom limits for chart families
        """
        self.limits = self.DEFAULT_LIMITS.copy()
        if custom_limits:
            self.limits.update(custom_limits)

        logger.info(f"[RenderSelector] Initialized with limits: {self.limits}")

    def select(
        self,
        chart_family: str,
        categories: Optional[List[str]] = None,
        data_size: Optional[int] = None,
    ) -> RenderDecision:
        """
        Select rendering variant based on chart family and data characteristics.

        Args:
            chart_family: Semantic chart family (from SemanticMapper)
            categories: List of category values (e.g., cities, products)
            data_size: Alternative to categories - number of data points

        Returns:
            RenderDecision: Rendering decision with variant and metadata

        Example:
            >>> selector = RenderSelector()
            >>> decision = selector.select("line_composed", categories=["Joinville"])
            >>> assert decision.render_variant == "single_line"
            >>> assert decision.category_count == 1
            >>> assert not decision.limit_applied
        """
        # Determine category count
        if categories is not None:
            category_count = len(categories)
        elif data_size is not None:
            category_count = data_size
        else:
            category_count = 0
            logger.warning(
                "[RenderSelector] No categories or data_size provided, "
                "assuming 0 categories"
            )

        logger.info(
            f"[RenderSelector] Selecting variant: "
            f"chart_family={chart_family}, categories={category_count}"
        )

        # Route to specific selection method
        if chart_family == "line_composed":
            return self._select_line_variant(category_count)
        elif chart_family == "bar_vertical":
            return self._select_bar_vertical_variant(category_count)
        elif chart_family == "bar_horizontal":
            return self._select_bar_horizontal_variant(category_count)
        elif chart_family == "pie":
            return self._select_pie_variant(category_count)
        elif chart_family == "bar_vertical_stacked":
            return self._select_stacked_variant(category_count)
        elif chart_family is None:
            # Textual response
            return RenderDecision(
                render_variant=None,
                limit_applied=False,
                category_count=0,
                reasoning="No rendering for textual response",
            )
        else:
            logger.warning(
                f"[RenderSelector] Unknown chart_family: {chart_family}, "
                f"defaulting to None"
            )
            return RenderDecision(
                render_variant=None,
                limit_applied=False,
                category_count=category_count,
                reasoning=f"Unknown chart family: {chart_family}",
            )

    def _select_line_variant(self, category_count: int) -> RenderDecision:
        """
        Select rendering variant for line_composed (temporal series).

        Rules:
        - 1 category → single_line
        - 2-5 categories → multi_line
        - > 5 categories → multi_line (limited to top 5)

        Args:
            category_count: Number of categories/series

        Returns:
            RenderDecision: Rendering decision
        """
        limit = self.limits["line_composed"]

        if category_count == 0:
            logger.warning("[RenderSelector] line_composed with 0 categories")
            return RenderDecision(
                render_variant="single_line",
                limit_applied=False,
                category_count=0,
                reasoning="No categories, defaulting to single_line",
            )

        if category_count == 1:
            logger.debug("[RenderSelector] Categories: 1 → Variant: single_line")
            return RenderDecision(
                render_variant="single_line",
                limit_applied=False,
                category_count=1,
                reasoning="Single category maps to single_line",
            )

        if category_count <= limit:
            logger.debug(
                f"[RenderSelector] Categories: {category_count} → Variant: multi_line"
            )
            return RenderDecision(
                render_variant="multi_line",
                limit_applied=False,
                category_count=category_count,
                reasoning=f"{category_count} categories fit within limit ({limit})",
            )

        # Exceeds limit
        logger.debug(
            f"[RenderSelector] Categories: {category_count} (limit: {limit}) "
            f"→ Variant: multi_line (limited)"
        )
        return RenderDecision(
            render_variant="multi_line",
            limit_applied=True,
            category_count=category_count,
            reasoning=f"{category_count} categories exceed limit ({limit}), will be truncated",
        )

    def _select_bar_vertical_variant(self, category_count: int) -> RenderDecision:
        """
        Select rendering variant for bar_vertical (categorical comparison).

        Rules:
        - 1 category → single_bar
        - 2-10 categories → multi_bar
        - > 10 categories → multi_bar (limited to top 10)

        Args:
            category_count: Number of categories

        Returns:
            RenderDecision: Rendering decision
        """
        limit = self.limits["bar_vertical"]

        if category_count == 1:
            return RenderDecision(
                render_variant="single_bar",
                limit_applied=False,
                category_count=1,
                reasoning="Single category maps to single_bar",
            )

        if category_count <= limit:
            return RenderDecision(
                render_variant="multi_bar",
                limit_applied=False,
                category_count=category_count,
                reasoning=f"{category_count} categories fit within limit ({limit})",
            )

        return RenderDecision(
            render_variant="multi_bar",
            limit_applied=True,
            category_count=category_count,
            reasoning=f"{category_count} categories exceed limit ({limit}), will be truncated",
        )

    def _select_bar_horizontal_variant(self, category_count: int) -> RenderDecision:
        """
        Select rendering variant for bar_horizontal (ranking).

        Bar horizontal is always limited to top/bottom N.

        Args:
            category_count: Number of categories

        Returns:
            RenderDecision: Rendering decision
        """
        limit = self.limits["bar_horizontal"]
        limit_applied = category_count > limit

        return RenderDecision(
            render_variant="multi_bar",  # Always multi for rankings
            limit_applied=limit_applied,
            category_count=category_count,
            reasoning=f"Ranking limited to top/bottom {limit}",
        )

    def _select_pie_variant(self, category_count: int) -> RenderDecision:
        """
        Select rendering variant for pie (distribution).

        Rules:
        - <= 5 slices → simple_pie
        - > 5 slices → detailed_pie (with "Others" aggregation)

        Args:
            category_count: Number of slices

        Returns:
            RenderDecision: Rendering decision
        """
        limit = self.limits["pie"]

        if category_count <= limit:
            return RenderDecision(
                render_variant="simple_pie",
                limit_applied=False,
                category_count=category_count,
                reasoning=f"{category_count} slices fit simple pie",
            )

        return RenderDecision(
            render_variant="detailed_pie",
            limit_applied=True,
            category_count=category_count,
            reasoning=f"{category_count} slices require grouping (top {limit} + Others)",
        )

    def _select_stacked_variant(self, category_count: int) -> RenderDecision:
        """
        Select rendering variant for bar_vertical_stacked (composition).

        Args:
            category_count: Number of stacks

        Returns:
            RenderDecision: Rendering decision
        """
        limit = self.limits["bar_vertical_stacked"]

        if category_count == 1:
            return RenderDecision(
                render_variant="single_stack",
                limit_applied=False,
                category_count=1,
                reasoning="Single stack",
            )

        if category_count <= limit:
            return RenderDecision(
                render_variant="multi_stack",
                limit_applied=False,
                category_count=category_count,
                reasoning=f"{category_count} stacks fit within limit",
            )

        return RenderDecision(
            render_variant="multi_stack",
            limit_applied=True,
            category_count=category_count,
            reasoning=f"{category_count} stacks exceed limit ({limit})",
        )
