"""
BarVerticalComposedHandler - Handler for composed vertical bar charts
======================================================================

Specialized handler for bar_vertical_composed chart type, typically used for:
- Multi-metric comparisons
- Grouped bar analysis
- Side-by-side metric comparisons per category
"""

from typing import Dict, Any, List
from .base import BaseChartHandler


class BarVerticalComposedHandler(BaseChartHandler):
    """
    Handler for composed vertical bar charts (multiple metrics).

    Focus areas:
    - Multi-metric comparisons
    - Relative performance across metrics
    - Correlation between metrics
    """

    def __init__(self):
        super().__init__("bar_vertical_composed")

    def get_context_for_llm(self, parsed_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Extract multi-metric context for LLM."""
        dimension_info = self.extract_dimension_info(parsed_inputs)

        chart_spec = parsed_inputs.get("chart_spec", {})
        metrics = chart_spec.get("metrics", [])
        metric_names = [m.get("alias", m.get("name", "Métrica")) for m in metrics]

        return {
            "chart_type_description": "Comparação multi-métrica (barras agrupadas)",
            "analysis_focus": "comparação entre múltiplas métricas, correlações, trade-offs",
            "expected_insights": [
                "Comparação entre métricas",
                "Categorias que se destacam em diferentes métricas",
                "Correlações ou divergências entre métricas",
            ],
            "dimension_type": "categórica",
            "dimension_alias": dimension_info["alias"],
            "metrics": metric_names,
            "metric_count": len(metrics),
        }

    def get_chart_description(self) -> str:
        """Get description for composed bar chart."""
        return "gráfico de barras verticais agrupadas comparando múltiplas métricas por categoria"

    def format_data_preview(self, data: List[Dict[str, Any]], top_n: int = 3) -> str:
        """Format preview showing multiple metrics."""
        if not data:
            return "(sem dados disponíveis)"

        preview = data[:top_n]
        lines = []

        for row in preview:
            values_str = ", ".join(
                [
                    f"{k}: {v:,.2f}" if isinstance(v, (int, float)) else f"{k}: {v}"
                    for k, v in row.items()
                ]
            )
            lines.append(f"- {values_str}")

        return "\n".join(lines)
