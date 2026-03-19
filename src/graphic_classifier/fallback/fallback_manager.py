"""
FallbackManager: Intelligent Chart Type Degradation
Implements deterministic fallback chains to salvage visualizations.

Architecture:
- Each chart family has a predefined fallback chain
- Fallback changes VISUALIZATION, not SEMANTIC intent
- Chain is linear and finite (anti-loop guarantee)
"""

from typing import Dict, Optional, List, Any
from datetime import datetime


class FallbackManager:
    """
    Manages fallback chains for chart types when structural validation fails.

    Core Principle:
    - Preserve semantic intent (temporal, ranking, distribution, etc.)
    - Degrade visualization complexity gracefully
    - Ensure transparency (user knows fallback occurred)

    Fallback Chains (Deterministic):

    line_composed:
      1. line_composed (ideal for temporal variation)
      2. bar_vertical (fallback: static comparison if < 2 periods)
      3. null (no viable visualization)

    pie:
      1. pie (ideal for composition)
      2. bar_horizontal (fallback: when too many categories or negatives)
      3. null

    bar_horizontal:
      1. bar_horizontal (ranking is already simplest)
      2. null (no simpler alternative)

    bar_vertical:
      1. bar_vertical
      2. null (already simple comparison)
    """

    # Fallback chain definitions (chart_type -> next_option)
    FALLBACK_CHAINS: Dict[str, List[str]] = {
        "line_composed": ["bar_vertical", "null"],
        "pie": ["bar_horizontal", "null"],
        "bar_horizontal": ["null"],
        "bar_vertical": ["null"],
        "bar_vertical_stacked": ["bar_vertical", "null"],
        "histogram": ["null"],
        "table": ["null"],  # Table is already non-visual
    }

    # Reasons for fallback (technical diagnostics)
    FALLBACK_REASONS = {
        "line_composed->bar_vertical": "Insufficient temporal periods (requires 2+)",
        "pie->bar_horizontal": "Too many categories or negative values detected",
        "bar_vertical_stacked->bar_vertical": "Insufficient data for stacking",
        "*->null": "No viable visualization alternative exists",
    }

    def __init__(self):
        self.fallback_history: List[Dict[str, Any]] = []

    def attempt_fallback(
        self,
        current_chart_type: str,
        failure_reason: str,
        chart_spec: Dict[str, Any],
        dataset: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Attempts to find a viable fallback chart type.

        Args:
            current_chart_type: The chart type that failed
            failure_reason: Technical reason for failure (e.g., "insufficient_periods")
            chart_spec: Current chart specification
            dataset: Optional dataset for structural validation

        Returns:
            {
                "fallback_chart_type": str | None,
                "fallback_triggered": bool,
                "fallback_reason": str,
                "should_retry": bool,  # True if new chart type should be attempted
                "should_route_to_text": bool  # True if should route to non_graph_executor
            }
        """

        # Get fallback chain for current chart type
        chain = self.FALLBACK_CHAINS.get(current_chart_type, ["null"])

        if not chain or chain[0] == "null":
            # No fallback available - route to text agent
            return self._create_null_response(
                current_chart_type, failure_reason, chart_spec
            )

        # Get next chart type in chain
        next_chart_type = chain[0]

        # Log fallback attempt
        self._log_fallback(
            from_type=current_chart_type, to_type=next_chart_type, reason=failure_reason
        )

        # Validate if fallback is structurally viable
        if self._is_fallback_viable(next_chart_type, chart_spec, dataset):
            return {
                "fallback_chart_type": next_chart_type,
                "fallback_triggered": True,
                "fallback_reason": self._get_fallback_reason(
                    current_chart_type, next_chart_type
                ),
                "original_chart_type": current_chart_type,
                "should_retry": True,
                "should_route_to_text": False,
                "user_message": self._generate_user_notification(
                    current_chart_type, next_chart_type, failure_reason
                ),
            }
        else:
            # Fallback also not viable - go deeper in chain or null
            if len(chain) > 1:
                # Try next in chain recursively
                return self.attempt_fallback(
                    current_chart_type=next_chart_type,
                    failure_reason=f"Fallback {next_chart_type} also failed: {failure_reason}",
                    chart_spec=chart_spec,
                    dataset=dataset,
                )
            else:
                return self._create_null_response(
                    current_chart_type, failure_reason, chart_spec
                )

    def _is_fallback_viable(
        self,
        fallback_type: str,
        chart_spec: Dict[str, Any],
        dataset: Optional[Dict[str, Any]],
    ) -> bool:
        """
        Validates if the fallback chart type is structurally feasible.

        Quick structural checks (detailed validation happens in executor):
        - bar_vertical: needs at least 1 dimension
        - bar_horizontal: needs at least 1 dimension
        - pie: needs exactly 1 dimension, no negatives
        """

        dimensions = chart_spec.get("dimensions", [])

        # Basic structural requirements
        if fallback_type == "bar_vertical":
            return len(dimensions) >= 1

        elif fallback_type == "bar_horizontal":
            return len(dimensions) >= 1

        elif fallback_type == "pie":
            # Pie needs exactly 1 dimension
            if len(dimensions) != 1:
                return False

            # Check for negative values if dataset provided
            if dataset and "data" in dataset:
                data = dataset["data"]
                if isinstance(data, list) and data:
                    # Check if any values are negative
                    for row in data:
                        if isinstance(row, dict):
                            for key, value in row.items():
                                if isinstance(value, (int, float)) and value < 0:
                                    return False

            return True

        # Default: assume viable (detailed check in executor)
        return True

    def _create_null_response(
        self, failed_chart_type: str, failure_reason: str, chart_spec: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Creates response when no fallback is viable - routes to text agent.
        """
        return {
            "fallback_chart_type": None,
            "fallback_triggered": True,
            "fallback_reason": failure_reason,
            "original_chart_type": failed_chart_type,
            "should_retry": False,
            "should_route_to_text": True,  # CRITICAL: Route to non_graph_executor
            "redirect_to": "non_graph_executor",
            "technical_detail": f"Chart type '{failed_chart_type}' failed: {failure_reason}. No viable visualization fallback exists.",
        }

    def _get_fallback_reason(self, from_type: str, to_type: str) -> str:
        """Gets human-readable reason for fallback."""
        key = f"{from_type}->{to_type}"
        return self.FALLBACK_REASONS.get(
            key,
            self.FALLBACK_REASONS.get(
                "*->null", "Visualization degraded due to data constraints"
            ),
        )

    def _generate_user_notification(
        self, from_type: str, to_type: str, technical_reason: str
    ) -> str:
        """
        Generates user-friendly notification when fallback occurs.

        Transparency principle: User should know visualization was adjusted.
        """

        TYPE_LABELS = {
            "line_composed": "gráfico de evolução temporal",
            "bar_vertical": "gráfico de barras verticais",
            "bar_horizontal": "gráfico de barras horizontais (ranking)",
            "pie": "gráfico de pizza",
        }

        from_label = TYPE_LABELS.get(from_type, from_type)
        to_label = TYPE_LABELS.get(to_type, to_type)

        return (
            f"ℹ️ Ajuste de visualização: O {from_label} foi substituído por "
            f"{to_label} para melhor adequação aos dados disponíveis."
        )

    def _log_fallback(self, from_type: str, to_type: str, reason: str):
        """Logs fallback attempt for debugging and monitoring."""
        self.fallback_history.append(
            {
                "timestamp": datetime.now().isoformat(),
                "from_type": from_type,
                "to_type": to_type,
                "reason": reason,
            }
        )

    def get_fallback_chain(self, chart_type: str) -> List[str]:
        """Returns the complete fallback chain for a chart type."""
        return self.FALLBACK_CHAINS.get(chart_type, ["null"])

    def get_history(self) -> List[Dict[str, Any]]:
        """Returns fallback history for monitoring."""
        return self.fallback_history.copy()

    def reset_history(self):
        """Clears fallback history (useful for testing)."""
        self.fallback_history.clear()


# Factory function for easy instantiation
def create_fallback_manager() -> FallbackManager:
    """Creates and returns a new FallbackManager instance."""
    return FallbackManager()
