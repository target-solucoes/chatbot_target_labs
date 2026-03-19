"""
LineHandler - Handler for line charts (temporal analysis)
==========================================================

Specialized handler for line chart type, typically used for:
- Time series analysis
- Trend identification
- Temporal evolution tracking
"""

from typing import Dict, Any, List
from .base import BaseChartHandler


class LineHandler(BaseChartHandler):
    """
    Handler for line charts showing temporal evolution.

    Focus areas:
    - Trends (growth/decline)
    - Variation patterns
    - Inflection points
    - Acceleration/deceleration
    """

    def __init__(self):
        super().__init__("line")

    def get_context_for_llm(self, parsed_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Extract temporal analysis context for LLM."""
        metric_info = self.extract_metric_info(parsed_inputs)
        dimension_info = self.extract_dimension_info(parsed_inputs)

        chart_spec = parsed_inputs.get("chart_spec", {})
        temporal_granularity = chart_spec.get("temporal_granularity", "month")

        return {
            "chart_type_description": "Série temporal (linha única)",
            "analysis_focus": "tendências, variações, pontos de inflexão, sazonalidade",
            "expected_insights": [
                "Tendência geral (crescimento/queda)",
                "Taxa de variação percentual",
                "Aceleração ou desaceleração",
                "Pontos de inflexão significativos",
            ],
            "dimension_type": "temporal",
            "metric_aggregation": metric_info["aggregation"],
            "metric_alias": metric_info["alias"],
            "dimension_alias": dimension_info["alias"],
            "temporal_granularity": temporal_granularity,
        }

    def get_chart_description(self) -> str:
        """Get description for line chart."""
        return "gráfico de linha mostrando evolução temporal de métrica"

    def format_data_preview(self, data: List[Dict[str, Any]], top_n: int = 3) -> str:
        """Format preview of temporal data."""
        if not data:
            return "(sem dados disponíveis)"

        # For time series, show first, middle, and last points
        if len(data) <= top_n:
            preview = data
        else:
            # Show first, middle, and last for better temporal context
            preview = [data[0], data[len(data) // 2], data[-1]]

        lines = []
        for row in preview:
            values = list(row.values())
            if len(values) >= 2:
                time_val = values[0]
                metric_val = values[1]

                if isinstance(metric_val, (int, float)):
                    lines.append(f"- {time_val}: {metric_val:,.2f}")
                else:
                    lines.append(f"- {time_val}: {metric_val}")
            else:
                lines.append(f"- {', '.join(map(str, values))}")

        return "\n".join(lines)
