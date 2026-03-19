"""
Dimension vs Filter Classifier - Desambiguação Modular

Este módulo implementa a classificação de campos multi-valor como
dimension ou filter, extraindo a lógica que estava embedded em _build_dimensions.

Conforme especificado em planning_graphical_correction.md - Fase 3.2:
- Regras claras e documentadas
- Lógica isolada e testável
- Extensível para novos casos

Referência: GRAPHICAL_CLASSIFIER_DIAGNOSIS.md - Issue #5
"""

import logging
from typing import Any, List, Literal, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


# Intents que indicam comparação (provável dimension)
COMPARISON_INTENTS = [
    "comparison",
    "period_comparison",
    "temporal_comparison",
    "category_comparison",
    "ranking",  # Rankings também comparam entre valores
]

# Chart types que requerem dimensions para comparação
COMPARISON_CHART_TYPES = [
    "bar_vertical",
    "bar_vertical_composed",
    "bar_horizontal",
    "line_composed",
    "pie",
]

# Keywords que indicam comparação
COMPARISON_KEYWORDS = [
    "comparar",
    "comparação",
    "comparacao",
    "entre",
    "versus",
    "vs",
    "diferença",
    "diferenca",
    "contra",
]

# Keywords que indicam filtro
FILTER_KEYWORDS = [
    "em",
    "no",
    "na",
    "apenas",
    "somente",
    "filtrar",
    "filtro",
    "onde",
    "com",
]


def classify_multi_value_field(
    column: str,
    values: List[Any],
    intent: Optional[str] = None,
    chart_type: Optional[str] = None,
    query_keywords: Optional[List[str]] = None,
) -> Literal["dimension", "filter"]:
    """
    Determina se campo com múltiplos valores deve ser dimension ou filter.

    Aplica regras hierárquicas para classificação:
    1. Intent-based: Intent de comparação → dimension
    2. Chart-type-based: Chart types de comparação → dimension
    3. Keyword-based: Keywords de comparação → dimension
    4. Value-count-based: 3+ valores → dimension (provável comparação)
    5. Date-range detection: 2 valores date-like → filter (range)
    6. Default: filtro

    Args:
        column: Nome da coluna
        values: Lista de valores
        intent: Intent classificado pelo LLM (opcional)
        chart_type: Tipo de gráfico (opcional)
        query_keywords: Keywords extraídas da query (opcional)

    Returns:
        "dimension" ou "filter"

    Examples:
        >>> classify_multi_value_field(
        ...     "UF_Cliente", ["SP", "RJ"],
        ...     intent="comparison",
        ...     chart_type="bar_vertical"
        ... )
        'dimension'

        >>> classify_multi_value_field(
        ...     "UF_Cliente", ["SP", "RJ", "MG"],
        ...     intent="ranking",
        ...     chart_type="bar_horizontal"
        ... )
        'dimension'

        >>> classify_multi_value_field(
        ...     "Data", ["2015-01-01", "2015-12-31"]
        ... )
        'filter'  # Date range

        >>> classify_multi_value_field(
        ...     "Ano", [2023, 2015]
        ... )
        'filter'  # Year range
    """

    if not values or not isinstance(values, list):
        logger.debug(
            f"[classify] {column}={values} -> filter (not a valid multi-value list)"
        )
        return "filter"

    # Regra 0: Date range detection (prioridade alta)
    if len(values) == 2 and is_date_range(values):
        logger.debug(
            f"[classify] {column}={values} -> filter (detected date/year range)"
        )
        return "filter"

    # Regra 1: Intent-based
    if intent and intent.lower() in [i.lower() for i in COMPARISON_INTENTS]:
        logger.debug(
            f"[classify] {column}={values} -> dimension "
            f"(intent={intent} indicates comparison)"
        )
        return "dimension"

    # Regra 2: Chart-type-based
    if chart_type and chart_type in COMPARISON_CHART_TYPES:
        logger.debug(
            f"[classify] {column}={values} -> dimension "
            f"(chart_type={chart_type} requires comparison dimension)"
        )
        return "dimension"

    # Regra 3: Keyword-based
    if query_keywords:
        # Normalizar keywords para lowercase
        normalized_keywords = [kw.lower() for kw in query_keywords]

        # Verificar comparison keywords
        has_comparison_keywords = any(
            comp_kw in normalized_keywords for comp_kw in COMPARISON_KEYWORDS
        )

        if has_comparison_keywords:
            logger.debug(
                f"[classify] {column}={values} -> dimension "
                f"(comparison keywords detected: {query_keywords})"
            )
            return "dimension"

        # Verificar filter keywords
        has_filter_keywords = any(
            filter_kw in normalized_keywords for filter_kw in FILTER_KEYWORDS
        )

        if has_filter_keywords:
            logger.debug(
                f"[classify] {column}={values} -> filter "
                f"(filter keywords detected: {query_keywords})"
            )
            return "filter"

    # Regra 4: Number of values
    if len(values) >= 3:
        # 3+ valores geralmente indicam dimension para comparação
        logger.debug(
            f"[classify] {column}={values} -> dimension "
            f"(3+ values suggest dimension for comparison)"
        )
        return "dimension"

    # Regra 5: Exactly 2 values (ambíguo)
    # Se não é date range e não temos outros indicadores, depende do contexto
    if len(values) == 2:
        # Se temos chart_type ou intent, já foi classificado acima
        # Se não temos, assumir dimension (comparação entre 2 valores é comum)
        if chart_type or intent:
            # Já foi avaliado acima, usar fallback
            pass
        else:
            # Sem contexto, assumir dimension para 2 valores
            logger.debug(
                f"[classify] {column}={values} -> dimension "
                f"(2 values likely comparison, no contradicting context)"
            )
            return "dimension"

    # Default: filtro
    logger.debug(
        f"[classify] {column}={values} -> filter (no strong indicators for dimension)"
    )
    return "filter"


def is_date_range(values: List[Any]) -> bool:
    """
    Detecta se valores representam range de datas/anos.

    Verifica se:
    - Exatamente 2 valores
    - Ambos são date-like (datetime, string ISO, ou anos inteiros)

    Args:
        values: Lista de valores a verificar

    Returns:
        True se valores representam date range, False caso contrário

    Examples:
        >>> is_date_range(["2015-01-01", "2015-12-31"])
        True

        >>> is_date_range([2023, 2015])
        True

        >>> is_date_range(["SP", "RJ"])
        False

        >>> is_date_range([2023, 2015, 2025])
        False  # 3 valores não é range
    """
    if not isinstance(values, list) or len(values) != 2:
        return False

    # Verificar se ambos valores são anos (inteiros entre 1900 e 2100)
    if all(isinstance(v, int) and 1900 <= v <= 2100 for v in values):
        logger.debug(f"[is_date_range] Detected year range: {values}")
        return True

    # Verificar se ambos valores são date-like strings
    date_formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ]

    date_values_count = 0

    for value in values:
        if isinstance(value, datetime):
            date_values_count += 1
            continue

        if isinstance(value, str):
            # Tentar parsear como data
            for date_format in date_formats:
                try:
                    datetime.strptime(value, date_format)
                    date_values_count += 1
                    break
                except ValueError:
                    continue

    is_range = date_values_count == 2

    if is_range:
        logger.debug(f"[is_date_range] Detected date range: {values}")

    return is_range


def should_be_dimension(
    column: str, values: Any, context: Optional[dict] = None
) -> bool:
    """
    Helper simplificado que determina se campo deve ser dimension.

    Wrapper conveniente para classify_multi_value_field que retorna boolean.

    Args:
        column: Nome da coluna
        values: Valor(es) da coluna
        context: Contexto opcional com intent, chart_type, keywords

    Returns:
        True se deve ser dimension, False se deve ser filter

    Examples:
        >>> should_be_dimension("UF_Cliente", ["SP", "RJ"],
        ...                     {"intent": "comparison"})
        True

        >>> should_be_dimension("Ano", [2015])
        False
    """
    # Se não é lista, não é dimension
    if not isinstance(values, list):
        return False

    # Se lista vazia ou com 1 item, não é dimension
    if len(values) <= 1:
        return False

    # Extrair contexto
    context = context or {}
    intent = context.get("intent")
    chart_type = context.get("chart_type")
    query_keywords = context.get("query_keywords")

    # Classificar
    classification = classify_multi_value_field(
        column=column,
        values=values,
        intent=intent,
        chart_type=chart_type,
        query_keywords=query_keywords,
    )

    return classification == "dimension"


def get_dimension_filter_hints(query: str) -> dict:
    """
    Extrai hints da query que ajudam na classificação dimension vs filter.

    Retorna keywords detectadas e sugestões de classificação.

    Args:
        query: Query do usuário

    Returns:
        Dict com:
            - comparison_keywords: Keywords de comparação encontradas
            - filter_keywords: Keywords de filtro encontradas
            - suggestion: "dimension" ou "filter" ou "ambiguous"

    Examples:
        >>> get_dimension_filter_hints("comparar vendas entre SP e RJ")
        {'comparison_keywords': ['comparar', 'entre'],
         'filter_keywords': [],
         'suggestion': 'dimension'}
    """
    if not query:
        return {
            "comparison_keywords": [],
            "filter_keywords": [],
            "suggestion": "ambiguous",
        }

    query_lower = query.lower()

    # Detectar comparison keywords
    found_comparison = [kw for kw in COMPARISON_KEYWORDS if kw in query_lower]

    # Detectar filter keywords
    found_filter = [kw for kw in FILTER_KEYWORDS if kw in query_lower]

    # Sugestão baseada em keywords
    suggestion = "ambiguous"
    if found_comparison and not found_filter:
        suggestion = "dimension"
    elif found_filter and not found_comparison:
        suggestion = "filter"
    elif found_comparison and found_filter:
        # Ambos presentes - comparação tem prioridade
        suggestion = "dimension"

    return {
        "comparison_keywords": found_comparison,
        "filter_keywords": found_filter,
        "suggestion": suggestion,
    }
