"""
BarHorizontalHandler - Handler for horizontal bar charts (rankings)
====================================================================

Specialized handler for bar_horizontal chart type, typically used for:
- Top N rankings
- Category comparisons where order matters
- Highlighting leader/follower dynamics
"""

from typing import Dict, Any, List
from .base import BaseChartHandler


class BarHorizontalHandler(BaseChartHandler):
    """
    Handler for horizontal bar charts showing rankings.

    Focus areas:
    - Concentration analysis (top N vs total)
    - Competitive gaps (leader vs followers)
    - Distribution of power/value across categories
    """

    def __init__(self):
        super().__init__("bar_horizontal")

    def get_context_for_llm(self, parsed_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Extract ranking-specific context for LLM."""
        metric_info = self.extract_metric_info(parsed_inputs)
        dimension_info = self.extract_dimension_info(parsed_inputs)

        chart_spec = parsed_inputs.get("chart_spec", {})
        top_n = chart_spec.get("top_n", 5)

        return {
            "chart_type_description": "Ranking (top N categorias)",
            "analysis_focus": "concentração, gaps competitivos, distribuição de poder",
            "expected_insights": [
                "Concentração (top 3 vs total)",
                "Gap entre líder e segundo colocado",
                "Distribuição de poder entre categorias",
            ],
            "dimension_type": "categórica",
            "metric_aggregation": metric_info["aggregation"],
            "metric_alias": metric_info["alias"],
            "dimension_alias": dimension_info["alias"],
            "top_n": top_n,
        }

    def get_chart_description(self) -> str:
        """Get description for horizontal bar chart."""
        return "gráfico de barras horizontais apresentando ranking de categorias por métrica"

    def format_data_preview(self, data: List[Dict[str, Any]], top_n: int = 3) -> str:
        """Format top N rows as preview."""
        if not data:
            return "(sem dados disponíveis)"

        preview = data[:top_n]
        lines = []

        for row in preview:
            # Get first two columns (typically dimension and metric)
            values = list(row.values())
            if len(values) >= 2:
                dim_val = values[0]
                metric_val = values[1]

                # Format metric as number with thousands separator
                if isinstance(metric_val, (int, float)):
                    lines.append(f"- {dim_val}: {metric_val:,.2f}")
                else:
                    lines.append(f"- {dim_val}: {metric_val}")
            else:
                # Fallback for unexpected structure
                lines.append(f"- {', '.join(map(str, values))}")

        return "\n".join(lines)
