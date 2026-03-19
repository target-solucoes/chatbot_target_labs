"""
Router for LangGraph conditional routing based on chart type.

This module contains the routing logic that determines which
tool handler should process a given chart specification.
"""

from typing import Literal
import logging

logger = logging.getLogger(__name__)


# Valid chart types supported by the system
VALID_CHART_TYPES = {
    "bar_horizontal",
    "bar_vertical",
    "bar_vertical_composed",
    "bar_vertical_stacked",
    "line",
    "line_composed",
    "pie",
    "histogram",
    "null",
}


def route_by_chart_type(
    state: dict,
) -> Literal[
    "bar_horizontal",
    "bar_vertical",
    "bar_vertical_composed",
    "bar_vertical_stacked",
    "line",
    "line_composed",
    "pie",
    "histogram",
    "null",
    "format_output",
]:
    """
    Router function para LangGraph ConditionalEdge.

    Esta função determina qual tool handler deve processar o chart
    baseado no chart_type presente no chart_spec.

    O retorno desta função mapeia diretamente para o nome do node
    que será executado no workflow do LangGraph.

    Args:
        state: AnalyticsState contendo chart_spec

    Returns:
        str: Nome do chart_type que mapeia para o tool node correspondente.
            Valores possíveis: "bar_horizontal", "bar_vertical",
            "bar_vertical_composed", "bar_vertical_stacked", "line",
            "line_composed", "pie", "histogram", "null", "format_output"

    Raises:
        ValueError: Se chart_spec não encontrado no state

    Examples:
        >>> state = {"chart_spec": {"chart_type": "bar_horizontal"}}
        >>> route_by_chart_type(state)
        'bar_horizontal'

        >>> state = {"chart_spec": {"chart_type": "invalid"}}
        >>> route_by_chart_type(state)
        'null'  # Fallback para chart_type inválido

    Notes:
        - A função é determinística: mesmo input sempre produz mesmo output
        - Não tem side effects: não modifica o state
        - Se chart_type desconhecido, faz fallback para "null" com warning
        - Se parse_input_node falhou, pula para format_output diretamente
        - Execução extremamente rápida (< 1ms)
    """
    # Verificar se parse_input_node falhou
    # Se falhou, pular handlers e ir direto para format_output
    if state.get("error_message"):
        logger.warning(
            f"parse_input_node failed with error: {state.get('error_message')}. "
            f"Skipping tool handlers and routing to format_output."
        )
        return "format_output"

    # Validar que chart_spec existe
    chart_spec = state.get("chart_spec")
    if chart_spec is None:
        raise ValueError(
            "ChartSpec not found in state. "
            "Ensure parse_input_node ran successfully before routing."
        )

    # Tratar chart_spec vazio como válido (rota para null)
    if not chart_spec or not isinstance(chart_spec, dict):
        logger.warning("Empty or invalid chart_spec, routing to 'null'")
        return "null"

    # Extrair chart_type com fallback para "null"
    chart_type = chart_spec.get("chart_type", "null")

    # Normalizar para lowercase (defensivo)
    if isinstance(chart_type, str):
        chart_type = chart_type.lower().strip()
    else:
        logger.warning(
            f"chart_type is not a string: {chart_type} ({type(chart_type)}), "
            f"defaulting to 'null'"
        )
        chart_type = "null"

    # Validar chart_type
    if chart_type not in VALID_CHART_TYPES:
        logger.warning(
            f"Unknown chart_type '{chart_type}', routing to 'null'. "
            f"Valid types: {sorted(VALID_CHART_TYPES)}"
        )
        return "null"

    logger.debug(f"Routing to chart_type: {chart_type}")
    return chart_type


def get_valid_chart_types() -> set:
    """
    Retorna o conjunto de chart types válidos.

    Útil para validação e documentação.

    Returns:
        set: Conjunto de chart types válidos

    Examples:
        >>> chart_types = get_valid_chart_types()
        >>> "bar_horizontal" in chart_types
        True
    """
    return VALID_CHART_TYPES.copy()
