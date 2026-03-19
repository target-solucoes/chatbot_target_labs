"""
HistogramHandler - Handler for histogram charts (frequency distribution)
=========================================================================

Specialized handler for histogram chart type, typically used for:
- Frequency distribution
- Data distribution shape analysis
- Outlier detection
"""

from typing import Dict, Any, List
from .base import BaseChartHandler


class HistogramHandler(BaseChartHandler):
    """
    Handler for histograms showing frequency distribution.

    Focus areas:
    - Distribution shape (normal, skewed, bimodal)
    - Central tendency
    - Spread and variability
    - Outliers
    """

    def __init__(self):
        super().__init__("histogram")

    def get_context_for_llm(self, parsed_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Extract distribution-specific context for LLM."""
        metric_info = self.extract_metric_info(parsed_inputs)

        chart_spec = parsed_inputs.get("chart_spec", {})
        bin_count = chart_spec.get("bin_count", 10)

        return {
            "chart_type_description": "Distribuição de frequência (histograma)",
            "analysis_focus": "forma da distribuição, tendência central, dispersão, outliers",
            "expected_insights": [
                "Tipo de distribuição (normal, assimétrica, bimodal)",
                "Concentração de valores",
                "Presença de outliers",
                "Dispersão dos dados",
            ],
            "dimension_type": "numérica (bins)",
            "metric_aggregation": metric_info["aggregation"],
            "metric_alias": metric_info["alias"],
            "bin_count": bin_count,
        }

    def get_chart_description(self) -> str:
        """Get description for histogram."""
        return "histograma mostrando distribuição de frequência de valores"

    def format_data_preview(self, data: List[Dict[str, Any]], top_n: int = 3) -> str:
        """Format preview of frequency distribution."""
        if not data:
            return "(sem dados disponíveis)"

        # For histograms, show beginning, middle, and end bins
        if len(data) <= top_n:
            preview = data
        else:
            preview = [data[0], data[len(data) // 2], data[-1]]

        lines = []
        for row in preview:
            values = list(row.values())
            if len(values) >= 2:
                bin_val = values[0]
                freq_val = values[1]

                if isinstance(freq_val, (int, float)):
                    lines.append(f"- {bin_val}: {freq_val:,.0f} ocorrências")
                else:
                    lines.append(f"- {bin_val}: {freq_val}")
            else:
                lines.append(f"- {', '.join(map(str, values))}")

        return "\n".join(lines)
