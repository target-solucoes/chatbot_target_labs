"""
Null Message Generator: User-Facing Explanations
Generates detailed, actionable messages when visualization fails.

Critical UX Principle:
- Never show blank screen without explanation
- Provide technical context for debugging
- Suggest next steps (text agent will try)
"""

from typing import Dict, Any, Optional
from datetime import datetime


class NullMessageGenerator:
    """
    Generates explanatory messages when chart generation fails.

    Enforces Invariante I4: Every null chart MUST have an explanation.

    Message Structure:
    {
        "title": "User-friendly headline",
        "reason": "Why visualization failed (user terms)",
        "technical_detail": "Technical diagnostic (for debugging)",
        "suggestion": "What happens next",
        "timestamp": "ISO timestamp"
    }
    """

    # Categorized failure reasons
    FAILURE_CATEGORIES = {
        "insufficient_periods": {
            "title": "Dados temporais insuficientes",
            "reason": "A visualização de evolução requer dados de pelo menos dois períodos distintos.",
            "suggestion": "O agente textual tentará fornecer uma resposta alternativa.",
        },
        "insufficient_data": {
            "title": "Dados insuficientes para visualização",
            "reason": "Não há dados suficientes para gerar o gráfico solicitado.",
            "suggestion": "Verifique os filtros aplicados ou tente ampliar o período de análise.",
        },
        "negative_values_in_pie": {
            "title": "Valores negativos incompatíveis com gráfico de pizza",
            "reason": "Gráficos de pizza requerem valores positivos para representar proporções.",
            "suggestion": "Um gráfico de barras seria mais adequado para estes dados.",
        },
        "too_many_categories": {
            "title": "Excesso de categorias para visualização",
            "reason": "A visualização solicitada funciona melhor com um número limitado de categorias.",
            "suggestion": "Considere aplicar filtros (ex: Top 10) ou usar uma visualização diferente.",
        },
        "dimensional_mismatch": {
            "title": "Estrutura de dados incompatível",
            "reason": "A estrutura dos dados não é compatível com o tipo de gráfico solicitado.",
            "suggestion": "O sistema tentará uma visualização alternativa.",
        },
        "no_data_returned": {
            "title": "Nenhum dado encontrado",
            "reason": "A consulta não retornou dados para o período/filtros especificados.",
            "suggestion": "Verifique se os critérios de busca estão corretos.",
        },
        "semantic_ambiguity": {
            "title": "Consulta ambígua",
            "reason": "Não foi possível determinar claramente qual visualização seria mais adequada.",
            "suggestion": "Tente reformular a pergunta de forma mais específica.",
        },
        "generic_error": {
            "title": "Não foi possível gerar a visualização",
            "reason": "Ocorreu um erro durante a geração do gráfico.",
            "suggestion": "O agente textual tentará responder sua pergunta.",
        },
    }

    def generate_message(
        self,
        failure_category: str,
        chart_spec: Dict[str, Any],
        technical_detail: Optional[str] = None,
        dataset_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generates a comprehensive error message for null chart.

        Args:
            failure_category: Category key (e.g., "insufficient_periods")
            chart_spec: Original chart specification that failed
            technical_detail: Additional technical context
            dataset_info: Information about the dataset (e.g., row count)

        Returns:
            Message object with title, reason, technical_detail, suggestion
        """

        # Get base message template
        template = self.FAILURE_CATEGORIES.get(
            failure_category, self.FAILURE_CATEGORIES["generic_error"]
        )

        # Build technical detail
        tech_detail = self._build_technical_detail(
            failure_category, chart_spec, technical_detail, dataset_info
        )

        # Construct complete message
        message = {
            "title": template["title"],
            "reason": template["reason"],
            "technical_detail": tech_detail,
            "suggestion": template["suggestion"],
            "timestamp": datetime.now().isoformat(),
            "failure_category": failure_category,
            "chart_type_attempted": chart_spec.get("chart_family", "unknown"),
        }

        return message

    def generate_redirect_payload(
        self,
        failure_category: str,
        chart_spec: Dict[str, Any],
        technical_detail: Optional[str] = None,
        dataset_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generates complete payload for routing to non_graph_executor.

        This is the critical integration point: when graph classifier fails,
        this payload tells the text agent WHY and provides context.

        Returns:
            {
                "chart_family": "null",
                "redirect_to": "non_graph_executor",
                "message": {...},
                "original_query": str,
                "original_intent": str,
                "failure_context": {...}
            }
        """

        message = self.generate_message(
            failure_category, chart_spec, technical_detail, dataset_info
        )

        return {
            "chart_family": "null",
            "redirect_to": "non_graph_executor",
            "message": message,
            "original_query": chart_spec.get("query", ""),
            "original_intent": chart_spec.get("intent", ""),
            "failure_context": {
                "attempted_chart_type": chart_spec.get("chart_family"),
                "dimensions": chart_spec.get("dimensions", []),
                "metrics": chart_spec.get("metrics", []),
                "filters": chart_spec.get("filters", {}),
                "dataset_rows": dataset_info.get("total_rows", 0)
                if dataset_info
                else 0,
            },
            "timestamp": datetime.now().isoformat(),
        }

    def _build_technical_detail(
        self,
        failure_category: str,
        chart_spec: Dict[str, Any],
        additional_detail: Optional[str],
        dataset_info: Optional[Dict[str, Any]],
    ) -> str:
        """Builds detailed technical diagnostic message."""

        details = []

        # Chart type attempted
        chart_type = chart_spec.get("chart_family", "unknown")
        details.append(f"Chart type attempted: {chart_type}")

        # Dataset information
        if dataset_info:
            rows = dataset_info.get("total_rows", 0)
            details.append(f"Dataset rows: {rows}")

            if "unique_periods" in dataset_info:
                periods = dataset_info["unique_periods"]
                details.append(f"Unique periods: {periods}")

            if "unique_categories" in dataset_info:
                categories = dataset_info["unique_categories"]
                details.append(f"Unique categories: {categories}")

        # Dimensions and metrics
        dimensions = chart_spec.get("dimensions", [])
        if dimensions:
            details.append(f"Dimensions: {', '.join(dimensions)}")

        metrics = chart_spec.get("metrics", [])
        if metrics:
            # Metrics can be list of dicts or list of strings
            metric_names = [m.get("name", m) if isinstance(m, dict) else m for m in metrics]
            details.append(f"Metrics: {', '.join(metric_names)}")

        # Additional technical context
        if additional_detail:
            details.append(f"Detail: {additional_detail}")

        return " | ".join(details)

    def create_fallback_notification(
        self, from_chart_type: str, to_chart_type: str, reason: str
    ) -> Dict[str, str]:
        """
        Creates a notification when fallback occurs (degradation, not failure).

        Different from null messages - this is for successful degradation.
        """

        TYPE_NAMES = {
            "line_composed": "gráfico de evolução temporal",
            "bar_vertical": "gráfico de barras verticais",
            "bar_horizontal": "gráfico de barras horizontais",
            "pie": "gráfico de pizza",
            "bar_vertical_stacked": "gráfico de barras empilhadas",
        }

        from_name = TYPE_NAMES.get(from_chart_type, from_chart_type)
        to_name = TYPE_NAMES.get(to_chart_type, to_chart_type)

        return {
            "type": "fallback_notification",
            "severity": "info",
            "message": f"A visualização foi ajustada de {from_name} para {to_name} para melhor representar os dados disponíveis.",
            "reason": reason,
            "from_chart_type": from_chart_type,
            "to_chart_type": to_chart_type,
        }

    def get_failure_categories(self) -> Dict[str, Dict[str, str]]:
        """Returns all available failure categories (for reference/testing)."""
        return self.FAILURE_CATEGORIES.copy()


# Factory function
def create_message_generator() -> NullMessageGenerator:
    """Creates and returns a new NullMessageGenerator instance."""
    return NullMessageGenerator()


# Convenience function for common use case
def generate_null_redirect(
    failure_category: str,
    chart_spec: Dict[str, Any],
    technical_detail: Optional[str] = None,
    dataset_info: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Convenience function to generate redirect payload in one call.

    Usage:
        redirect = generate_null_redirect(
            "insufficient_periods",
            chart_spec,
            technical_detail="Only 1 month of data available",
            dataset_info={"total_rows": 10, "unique_periods": 1}
        )
    """
    generator = NullMessageGenerator()
    return generator.generate_redirect_payload(
        failure_category, chart_spec, technical_detail, dataset_info
    )
