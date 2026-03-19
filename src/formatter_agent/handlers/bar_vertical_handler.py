"""
BarVerticalHandler - Handler for vertical bar charts (comparisons)
===================================================================

Specialized handler for bar_vertical chart type, typically used for:
- Category comparisons
- Side-by-side analysis
- Performance comparisons across categories
"""

from typing import Dict, Any, List
from .base import BaseChartHandler


class BarVerticalHandler(BaseChartHandler):
    """
    Handler for vertical bar charts showing comparisons.

    Focus areas:
    - Cross-category comparisons
    - Performance disparities
    - Category-level insights
    """

    def __init__(self):
        super().__init__("bar_vertical")

    def get_context_for_llm(self, parsed_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Extract comparison-specific context for LLM."""
        metric_info = self.extract_metric_info(parsed_inputs)
        dimension_info = self.extract_dimension_info(parsed_inputs)

        return {
            "chart_type_description": "Comparação entre categorias (barras verticais)",
            "analysis_focus": "comparações entre categorias, diferenças de performance",
            "expected_insights": [
                "Categorias com melhor/pior performance",
                "Magnitude das diferenças entre categorias",
                "Padrões de distribuição",
            ],
            "dimension_type": "categórica",
            "metric_aggregation": metric_info["aggregation"],
            "metric_alias": metric_info["alias"],
            "dimension_alias": dimension_info["alias"],
        }

    def get_chart_description(self) -> str:
        """Get description for vertical bar chart."""
        return "gráfico de barras verticais comparando métricas entre categorias"

    def format_data_preview(self, data: List[Dict[str, Any]], top_n: int = 3) -> str:
        """Format preview of comparison data."""
        if not data:
            return "(sem dados disponíveis)"

        preview = data[:top_n]
        lines = []

        for row in preview:
            values = list(row.values())
            if len(values) >= 2:
                dim_val = values[0]
                metric_val = values[1]

                if isinstance(metric_val, (int, float)):
                    lines.append(f"- {dim_val}: {metric_val:,.2f}")
                else:
                    lines.append(f"- {dim_val}: {metric_val}")
            else:
                lines.append(f"- {', '.join(map(str, values))}")

        return "\n".join(lines)
