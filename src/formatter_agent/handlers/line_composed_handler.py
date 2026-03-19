"""
LineComposedHandler - Handler for composed line charts (multi-series)
======================================================================

Specialized handler for line_composed chart type, typically used for:
- Multi-metric temporal comparison
- Trend comparison across metrics
- Correlation analysis over time
"""

from typing import Dict, Any, List
from .base import BaseChartHandler


class LineComposedHandler(BaseChartHandler):
    """
    Handler for composed line charts (multiple series).

    Focus areas:
    - Multi-metric trend comparison
    - Divergence/convergence patterns
    - Correlation over time
    - Relative performance across time
    """

    def __init__(self):
        super().__init__("line_composed")

    def get_context_for_llm(self, parsed_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Extract multi-series temporal context for LLM."""
        dimension_info = self.extract_dimension_info(parsed_inputs)

        chart_spec = parsed_inputs.get("chart_spec", {})
        metrics = chart_spec.get("metrics", [])
        metric_names = [m.get("alias", m.get("name", "Métrica")) for m in metrics]
        temporal_granularity = chart_spec.get("temporal_granularity", "month")

        return {
            "chart_type_description": "Série temporal multi-métrica (linhas compostas)",
            "analysis_focus": "comparação de tendências, correlações temporais, divergências",
            "expected_insights": [
                "Comparação de tendências entre métricas",
                "Momentos de divergência ou convergência",
                "Correlação ou anti-correlação temporal",
                "Métricas com maior variabilidade",
            ],
            "dimension_type": "temporal",
            "dimension_alias": dimension_info["alias"],
            "metrics": metric_names,
            "metric_count": len(metrics),
            "temporal_granularity": temporal_granularity,
        }

    def get_chart_description(self) -> str:
        """Get description for composed line chart."""
        return "gráfico de linhas compostas comparando evolução temporal de múltiplas métricas"

    def format_data_preview(self, data: List[Dict[str, Any]], top_n: int = 3) -> str:
        """Format preview showing multiple series."""
        if not data:
            return "(sem dados disponíveis)"

        # For time series, show first, middle, and last points
        if len(data) <= top_n:
            preview = data
        else:
            preview = [data[0], data[len(data) // 2], data[-1]]

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
