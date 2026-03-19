"""
FASE 5: Category Limiter - Data Slicing for Readability

This module implements automatic category limitation to ensure chart readability
WITHOUT changing the semantic chart family.

CRITICAL PRINCIPLE (Anti-Regression):
    The CategoryLimiter ONLY affects HOW MUCH data is displayed,
    NEVER WHAT chart type is used.

    If Phase 5 changes chart_family due to data volume, the implementation is WRONG.
    Data Slicing is COSMETIC, not SEMANTIC.

Architecture Position:
    Semantic Anchor (Phase 1) -> Chart Classification (Phase 2-4) ->
    Data Slicing (Phase 5 - THIS MODULE) -> Executor (Phase 6+)

Invariants:
    I1: User-specified limits ALWAYS override automatic limits
    I2: Limits are chart-family-specific (different limits for different chart types)
    I3: When cutting data, always assume Top N by primary metric (most relevant first)
    I4: Metadata must indicate when limiting was applied (transparency)
"""

import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class LimitConfig:
    """Configuration for automatic category limits per chart family."""

    chart_family: str
    max_categories: int
    reason: str
    applies_to: str  # "series" | "categories" | "slices" | "groups"


# Default limit configurations by chart family
DEFAULT_LIMITS: Dict[str, LimitConfig] = {
    "line_composed": LimitConfig(
        chart_family="line_composed",
        max_categories=5,
        reason="Maximum 5 series for readability in temporal trend visualization",
        applies_to="series",
    ),
    "bar_horizontal": LimitConfig(
        chart_family="bar_horizontal",
        max_categories=15,
        reason="Maximum 15 categories for ranking visualization",
        applies_to="categories",
    ),
    "pie": LimitConfig(
        chart_family="pie",
        max_categories=10,
        reason="Maximum 10 slices for distribution visualization (others can be grouped)",
        applies_to="slices",
    ),
    "bar_vertical_stacked": LimitConfig(
        chart_family="bar_vertical_stacked",
        max_categories=10,
        reason="Maximum 10 groups for composition visualization",
        applies_to="groups",
    ),
    "bar_vertical": LimitConfig(
        chart_family="bar_vertical",
        max_categories=12,
        reason="Maximum 12 categories for comparison visualization",
        applies_to="categories",
    ),
    "line": LimitConfig(
        chart_family="line",
        max_categories=1,
        reason="Single series temporal visualization (use line_composed for multiple)",
        applies_to="series",
    ),
}


@dataclass
class LimitResult:
    """Result of applying category limitation."""

    limit_applied: bool
    original_count: int
    display_count: int
    limit_reason: str
    limit_source: str  # "user" | "automatic" | "none"
    chart_family: str

    def to_metadata(self) -> Dict[str, Any]:
        """Convert to metadata dict for output."""
        return {
            "limit_applied": self.limit_applied,
            "original_count": self.original_count,
            "display_count": self.display_count,
            "limit_reason": self.limit_reason,
            "limit_source": self.limit_source,
        }


class CategoryLimiter:
    """
    Applies intelligent category limitation for chart readability.

    Responsibilities:
        - Determine appropriate category limits per chart family
        - Respect user-specified limits (overrides defaults)
        - Provide metadata for transparency

    NOT Responsible For:
        - Changing chart type based on data volume (PROHIBITED)
        - Executing queries or manipulating data
        - Validating semantic correctness

    Usage:
        ```python
        limiter = CategoryLimiter()

        # Get recommended limit for chart spec
        limit = limiter.get_limit_for_chart(
            chart_type="line_composed",
            user_limit=None,  # No user override
            current_count=50  # 50 products in data
        )

        # limit.display_count = 5 (automatic)
        # limit.limit_applied = True
        # limit.limit_reason = "Auto-limited to top 5 series for readability..."
        ```
    """

    def __init__(self, custom_limits: Optional[Dict[str, LimitConfig]] = None):
        """
        Initialize the CategoryLimiter.

        Args:
            custom_limits: Optional custom limit configurations to override defaults
        """
        self.limits = DEFAULT_LIMITS.copy()
        if custom_limits:
            self.limits.update(custom_limits)

        logger.debug(
            f"[CategoryLimiter] Initialized with {len(self.limits)} chart family limits"
        )

    def get_limit_for_chart(
        self,
        chart_type: str,
        user_limit: Optional[int] = None,
        current_count: Optional[int] = None,
    ) -> LimitResult:
        """
        Determine the appropriate limit for a chart.

        INVARIANT I1: User-specified limits ALWAYS override automatic limits.

        Args:
            chart_type: Chart family (e.g., "line_composed", "bar_horizontal")
            user_limit: User-specified limit (e.g., from "top 3" query)
            current_count: Current number of categories in data (optional, for metadata)

        Returns:
            LimitResult with limit decision and metadata

        Examples:
            >>> limiter = CategoryLimiter()

            # User specified "top 3" - user limit wins
            >>> result = limiter.get_limit_for_chart("bar_horizontal", user_limit=3)
            >>> result.display_count
            3
            >>> result.limit_source
            'user'

            # No user limit, apply automatic
            >>> result = limiter.get_limit_for_chart("line_composed", current_count=50)
            >>> result.display_count
            5
            >>> result.limit_source
            'automatic'

            # Unknown chart type - no limit
            >>> result = limiter.get_limit_for_chart("unknown_chart")
            >>> result.limit_applied
            False
        """
        # Get limit config for this chart family
        limit_config = self.limits.get(chart_type)

        # Unknown chart type - no limit
        if not limit_config:
            logger.warning(
                f"[CategoryLimiter] No limit configuration for chart_type='{chart_type}', "
                f"skipping limitation"
            )
            return LimitResult(
                limit_applied=False,
                original_count=current_count or 0,
                display_count=current_count or 0,
                limit_reason="No automatic limit for this chart type",
                limit_source="none",
                chart_family=chart_type,
            )

        # INVARIANT I1: User limit overrides automatic
        if user_limit is not None:
            logger.info(
                f"[CategoryLimiter] Using user-specified limit: {user_limit} "
                f"(chart_type={chart_type})"
            )
            return LimitResult(
                limit_applied=True,
                original_count=current_count or user_limit,
                display_count=user_limit,
                limit_reason=f"User-specified limit: top {user_limit} {limit_config.applies_to}",
                limit_source="user",
                chart_family=chart_type,
            )

        # Apply automatic limit
        automatic_limit = limit_config.max_categories

        # If current_count provided, check if limit is needed
        if current_count is not None:
            if current_count <= automatic_limit:
                # No limit needed - data already within acceptable range
                logger.debug(
                    f"[CategoryLimiter] No limit needed: current_count={current_count} "
                    f"<= automatic_limit={automatic_limit} (chart_type={chart_type})"
                )
                return LimitResult(
                    limit_applied=False,
                    original_count=current_count,
                    display_count=current_count,
                    limit_reason="Data count within acceptable range",
                    limit_source="none",
                    chart_family=chart_type,
                )
            else:
                # Limit needed
                logger.info(
                    f"[CategoryLimiter] Applying automatic limit: {current_count} -> {automatic_limit} "
                    f"{limit_config.applies_to} (chart_type={chart_type})"
                )
                return LimitResult(
                    limit_applied=True,
                    original_count=current_count,
                    display_count=automatic_limit,
                    limit_reason=f"Auto-limited to top {automatic_limit} {limit_config.applies_to} "
                    f"for readability ({chart_type} constraint)",
                    limit_source="automatic",
                    chart_family=chart_type,
                )

        # current_count not provided - return automatic limit as recommendation
        logger.debug(
            f"[CategoryLimiter] Returning automatic limit recommendation: {automatic_limit} "
            f"(chart_type={chart_type})"
        )
        return LimitResult(
            limit_applied=True,
            original_count=0,  # Unknown
            display_count=automatic_limit,
            limit_reason=limit_config.reason,
            limit_source="automatic",
            chart_family=chart_type,
        )

    def should_apply_limit(self, chart_spec: Dict[str, Any]) -> bool:
        """
        Determine if limiting should be applied to this chart spec.

        Args:
            chart_spec: Chart specification from classifier

        Returns:
            True if limit should be applied, False otherwise
        """
        chart_type = chart_spec.get("chart_type")

        # No chart type - no limit
        if not chart_type or chart_type == "null":
            return False

        # Check if this chart family has a limit config
        return chart_type in self.limits

    def apply_limit_to_spec(self, chart_spec: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply category limit to a chart specification.

        This modifies the chart_spec to include top_n limit if appropriate.

        CRITICAL: This does NOT change chart_type. It only adds/modifies top_n.

        Args:
            chart_spec: Chart specification from classifier

        Returns:
            Modified chart_spec with limit applied (or unchanged if no limit needed)

        Example:
            >>> chart_spec = {
            ...     "chart_type": "line_composed",
            ...     "dimensions": [...],  # 50 products
            ...     "metrics": [...]
            ... }
            >>>
            >>> limiter = CategoryLimiter()
            >>> limited_spec = limiter.apply_limit_to_spec(chart_spec)
            >>>
            >>> limited_spec["top_n"]
            5
            >>> limited_spec["chart_type"]  # UNCHANGED!
            'line_composed'
        """
        chart_type = chart_spec.get("chart_type")

        # Extract existing top_n (user-specified)
        user_top_n = chart_spec.get("top_n")

        # Get limit decision
        limit_result = self.get_limit_for_chart(
            chart_type=chart_type,
            user_limit=user_top_n,
            current_count=None,  # Will be determined at execution time
        )

        # Apply limit to spec if needed
        if limit_result.limit_applied and limit_result.limit_source == "automatic":
            # Add top_n to spec
            chart_spec["top_n"] = limit_result.display_count

            logger.info(
                f"[CategoryLimiter] Added automatic top_n={limit_result.display_count} "
                f"to chart_spec (chart_type={chart_type})"
            )

        # Add metadata for transparency
        if limit_result.limit_applied:
            if "limit_metadata" not in chart_spec:
                chart_spec["limit_metadata"] = limit_result.to_metadata()

            logger.debug(
                f"[CategoryLimiter] Added limit_metadata to chart_spec: "
                f"{limit_result.to_metadata()}"
            )

        return chart_spec

    def get_limit_config(self, chart_type: str) -> Optional[LimitConfig]:
        """
        Get the limit configuration for a chart type.

        Args:
            chart_type: Chart family

        Returns:
            LimitConfig if exists, None otherwise
        """
        return self.limits.get(chart_type)

    def update_limit_config(
        self,
        chart_type: str,
        max_categories: int,
        reason: Optional[str] = None,
        applies_to: Optional[str] = None,
    ) -> None:
        """
        Update or add a limit configuration.

        Args:
            chart_type: Chart family to configure
            max_categories: Maximum number of categories
            reason: Optional custom reason message
            applies_to: Optional descriptor (e.g., "series", "categories")
        """
        existing = self.limits.get(chart_type)

        if existing:
            # Update existing
            self.limits[chart_type] = LimitConfig(
                chart_family=chart_type,
                max_categories=max_categories,
                reason=reason or existing.reason,
                applies_to=applies_to or existing.applies_to,
            )
            logger.info(
                f"[CategoryLimiter] Updated limit for {chart_type}: {max_categories}"
            )
        else:
            # Add new
            self.limits[chart_type] = LimitConfig(
                chart_family=chart_type,
                max_categories=max_categories,
                reason=reason
                or f"Maximum {max_categories} categories for {chart_type}",
                applies_to=applies_to or "categories",
            )
            logger.info(
                f"[CategoryLimiter] Added new limit for {chart_type}: {max_categories}"
            )
