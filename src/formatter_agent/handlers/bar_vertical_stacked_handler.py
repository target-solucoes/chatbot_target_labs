"""
BarVerticalStackedHandler - Handler for stacked vertical bar charts
====================================================================

Specialized handler for bar_vertical_stacked chart type, typically used for:
- Part-to-whole analysis
- Composition breakdown
- Stacked contribution analysis
"""

from typing import Dict, Any, List
from .base import BaseChartHandler


class BarVerticalStackedHandler(BaseChartHandler):
    """
    Handler for stacked vertical bar charts (part-to-whole).

    Focus areas:
    - Composition analysis
    - Contribution of each part to total
    - Evolution of composition across categories
    """

    def __init__(self):
        super().__init__("bar_vertical_stacked")

    def get_context_for_llm(self, parsed_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Extract composition-specific context for LLM."""
        dimension_info = self.extract_dimension_info(parsed_inputs)

        chart_spec = parsed_inputs.get("chart_spec", {})
        metrics = chart_spec.get("metrics", [])
        metric_names = [m.get("alias", m.get("name", "Métrica")) for m in metrics]

        return {
            "chart_type_description": "Composição empilhada (barras verticais empilhadas)",
            "analysis_focus": "composição, contribuição relativa, evolução da estrutura",
            "expected_insights": [
                "Componentes dominantes em cada categoria",
                "Variação de composição entre categorias",
                "Contribuição percentual de cada componente",
            ],
            "dimension_type": "categórica",
            "dimension_alias": dimension_info["alias"],
            "metrics": metric_names,
            "metric_count": len(metrics),
        }

    def get_chart_description(self) -> str:
        """Get description for stacked bar chart."""
        return (
            "gráfico de barras verticais empilhadas mostrando composição por categoria"
        )

    def format_data_preview(self, data: List[Dict[str, Any]], top_n: int = 3) -> str:
        """Format preview showing composition."""
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
