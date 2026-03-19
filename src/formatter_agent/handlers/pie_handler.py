"""
PieHandler - Handler for pie charts (distribution analysis)
============================================================

Specialized handler for pie chart type, typically used for:
- Proportional distribution
- Market share analysis
- Portfolio composition
"""

from typing import Dict, Any, List
from .base import BaseChartHandler


class PieHandler(BaseChartHandler):
    """
    Handler for pie charts showing proportional distribution.

    Focus areas:
    - Proportion/percentage distribution
    - Concentration indices (HHI)
    - Diversification assessment
    - Dominant segments
    """

    def __init__(self):
        super().__init__("pie")

    def get_context_for_llm(self, parsed_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Extract distribution-specific context for LLM."""
        metric_info = self.extract_metric_info(parsed_inputs)
        dimension_info = self.extract_dimension_info(parsed_inputs)

        return {
            "chart_type_description": "Distribuição proporcional (pizza)",
            "analysis_focus": "proporções, concentração, diversificação, segmentos dominantes",
            "expected_insights": [
                "Distribuição de participação entre categorias",
                "Índice de concentração (HHI)",
                "Diversidade do portfólio",
                "Segmentos dominantes vs minoritários",
            ],
            "dimension_type": "categórica",
            "metric_aggregation": metric_info["aggregation"],
            "metric_alias": metric_info["alias"],
            "dimension_alias": dimension_info["alias"],
        }

    def get_chart_description(self) -> str:
        """Get description for pie chart."""
        return "gráfico de pizza mostrando distribuição proporcional de métrica por categoria"

    def format_data_preview(self, data: List[Dict[str, Any]], top_n: int = 3) -> str:
        """Format preview with percentages if possible."""
        if not data:
            return "(sem dados disponíveis)"

        preview = data[:top_n]
        lines = []

        # Calculate total for percentage calculation
        total = sum(
            row[list(row.keys())[1]]
            for row in data
            if len(row) >= 2 and isinstance(list(row.values())[1], (int, float))
        )

        for row in preview:
            values = list(row.values())
            if len(values) >= 2:
                dim_val = values[0]
                metric_val = values[1]

                if isinstance(metric_val, (int, float)) and total > 0:
                    percentage = (metric_val / total) * 100
                    lines.append(f"- {dim_val}: {metric_val:,.2f} ({percentage:.1f}%)")
                else:
                    lines.append(f"- {dim_val}: {metric_val}")
            else:
                lines.append(f"- {', '.join(map(str, values))}")

        return "\n".join(lines)
