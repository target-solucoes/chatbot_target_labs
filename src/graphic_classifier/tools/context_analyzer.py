"""
Context Analyzer for Chart Type Classification.

This module extracts semantic context from user queries to enable
context-aware keyword weighting. This is a key component of Phase 2
of the disambiguation improvement strategy.

The context extracted includes:
- Temporal comparison indicators
- Comparison keywords
- Dimension patterns (single vs multiple)
- Nested ranking patterns
- Period comparison patterns

This context is used to dynamically adjust keyword weights in the
keyword detector, resolving ambiguities like:
- "top 5 produtos" vs "top 5 produtos com crescimento entre maio e junho"
- "vendas em SP" vs "vendas em SP e RJ"
"""

import re
import logging
from typing import Dict, List, Optional

from src.graphic_classifier.utils.text_cleaner import normalize_text
from src.graphic_classifier.utils.ranking_detector import extract_nested_ranking


logger = logging.getLogger(__name__)


def extract_query_context(query: str, parsed_entities: Optional[dict] = None) -> dict:
    """
    Extrai contexto semantico da query para ponderacao de keywords.

    Esta funcao e o nucleo da FASE 2: Context-Aware Keywords. Ela detecta
    patterns e contextos que permitem diferenciar queries ambiguas.

    Args:
        query: User query original
        parsed_entities: Entidades extraidas do parse_query (opcional)

    Returns:
        Dicionario com contexto extraido:
        {
            # Temporal comparison context
            "has_temporal_comparison": bool,
            "temporal_operators": list[str],
            "between_periods_pattern": bool,
            "two_temporal_values": bool,

            # Comparison context
            "has_comparison_keywords": bool,
            "comparison_operators": list[str],

            # Dimension context
            "multi_dimension": bool,
            "single_dimension": bool,
            "has_temporal_dimension": bool,
            "multi_value_temporal": bool,

            # Nested patterns
            "nested_ranking": bool,
            "nested_ranking_pattern": bool,

            # Negations (for boost conditions)
            "no_temporal_comparison": bool,
            "no_comparison_keywords": bool,

            # Additional patterns
            "has_top_n": bool,
            "followed_by_dimension": bool,
            "has_temporal_operator": bool
        }

    Examples:
        >>> extract_query_context("top 5 produtos")
        {
            "has_temporal_comparison": False,
            "no_temporal_comparison": True,
            "has_top_n": True,
            "single_dimension": True,
            ...
        }

        >>> extract_query_context("top 5 produtos com crescimento entre maio e junho")
        {
            "has_temporal_comparison": True,
            "temporal_operators": ["crescimento"],
            "between_periods_pattern": True,
            "two_temporal_values": True,
            "no_temporal_comparison": False,
            ...
        }
    """
    context = {}
    normalized = normalize_text(query)

    # ====================
    # 1. TEMPORAL COMPARISON DETECTION
    # ====================
    temporal_operators = [
        "crescimento",
        "variacao",
        "aumento",
        "reducao",
        "mudanca",
        "evolucao",
        "comparacao",  # ADICIONADO
        "comparar",  # ADICIONADO
    ]
    context["temporal_operators"] = [
        op for op in temporal_operators if op in normalized
    ]
    context["has_temporal_comparison"] = len(context["temporal_operators"]) > 0

    # Detectar se ha operador temporal generico
    context["has_temporal_operator"] = context["has_temporal_comparison"]

    # ====================
    # 2. BETWEEN PERIODS PATTERN
    # ====================
    # Pattern: "entre [periodo1] e [periodo2]"
    # CORRIGIDO: Suporta "fev 2015" (normalize_text remove "/"), "mes de maio", etc.
    # Captura: palavra + opcionalmente numeros (ano) + opcionalmente "de ano"
    between_pattern = r"entre\s+([\w]+(?:\s+\d{4})?(?:\s+de\s+\d{4})?)\s+e\s+([\w]+(?:\s+\d{4})?(?:\s+de\s+\d{4})?)"
    match = re.search(between_pattern, normalized)
    if match:
        period1, period2 = match.groups()

        # Verificar se sao periodos temporais (meses, anos)
        temporal_terms = [
            # Meses completos
            "janeiro",
            "fevereiro",
            "marco",
            "abril",
            "maio",
            "junho",
            "julho",
            "agosto",
            "setembro",
            "outubro",
            "novembro",
            "dezembro",
            # ADICIONADO: Abreviações de meses
            "jan",
            "fev",
            "mar",
            "abr",
            "mai",
            "jun",
            "jul",
            "ago",
            "set",
            "out",
            "nov",
            "dez",
            # Anos
            "2015",
            "2016",
            "2017",
            "2018",
            "2019",
            "2020",
            "2021",
            "2022",
            "2023",
            "2024",
            "2025",
            # Trimestres/Semestres
            "q1",
            "q2",
            "q3",
            "q4",
            "trimestre",
            "semestre",
        ]

        # CORRIGIDO: Verificar se QUALQUER PARTE contém termo temporal
        period1_lower = period1.lower()
        period2_lower = period2.lower()

        is_temporal = any(
            term in period1_lower or term in period2_lower for term in temporal_terms
        )

        context["between_periods_pattern"] = is_temporal
        context["two_temporal_values"] = is_temporal

        # Se detectou pattern temporal, forçar has_temporal_comparison = True
        if is_temporal:
            context["has_temporal_comparison"] = True
            context["has_temporal_operator"] = True

        logger.debug(
            f"[extract_query_context] Between pattern detected: '{period1}' e '{period2}' "
            f"(is_temporal={is_temporal})"
        )
    else:
        context["between_periods_pattern"] = False
        context["two_temporal_values"] = False

    # ====================
    # 3. COMPARISON KEYWORDS DETECTION
    # ====================
    comparison_keywords = [
        "comparar",
        "comparando",
        "comparacao",
        "versus",
        "vs",
        "entre",
        "diferenca",
        "contrastar",
    ]
    context["comparison_operators"] = [
        kw for kw in comparison_keywords if kw in normalized
    ]
    context["has_comparison_keywords"] = len(context["comparison_operators"]) > 0

    # ====================
    # 4. NESTED RANKING DETECTION
    # ====================
    # Pattern: "top N [dim1] nos/em [top M] [dim2]"
    nested_info = extract_nested_ranking(query)
    context["nested_ranking"] = nested_info.get("is_nested", False)
    context["nested_ranking_pattern"] = nested_info.get("is_nested", False)

    if context["nested_ranking"]:
        logger.debug(f"[extract_query_context] Nested ranking detected: {nested_info}")

    # ====================
    # 5. DIMENSION ANALYSIS (requires parsed_entities)
    # ====================
    if parsed_entities:
        dimensions = parsed_entities.get("dimensions", [])
        filters = parsed_entities.get("filters", {})

        context["multi_dimension"] = len(dimensions) >= 2
        context["single_dimension"] = len(dimensions) == 1

        # Verificar se tem dimension temporal
        temporal_dims = ["Mes", "Ano", "Data", "Trimestre", "Semestre"]
        context["has_temporal_dimension"] = any(
            dim.get("name") in temporal_dims for dim in dimensions
        )

        # Verificar se tem multi-value temporal filter
        # Exemplo: {"Mes": ["Janeiro", "Fevereiro"]}
        temporal_filters = [
            k
            for k, v in filters.items()
            if k in temporal_dims and isinstance(v, list) and len(v) >= 2
        ]
        context["multi_value_temporal"] = len(temporal_filters) > 0

        logger.debug(
            f"[extract_query_context] Dimension analysis: "
            f"dimensions={len(dimensions)}, "
            f"has_temporal_dimension={context['has_temporal_dimension']}, "
            f"multi_value_temporal={context['multi_value_temporal']}"
        )
    else:
        # Se nao temos parsed_entities, tentar inferir do texto
        context["multi_dimension"] = False
        context["single_dimension"] = False
        context["has_temporal_dimension"] = False
        context["multi_value_temporal"] = False

        # Inferir temporal dimension de keywords
        temporal_keywords = ["mes", "ano", "data", "trimestre", "semestre", "periodo"]
        if any(kw in normalized for kw in temporal_keywords):
            context["has_temporal_dimension"] = True

        logger.debug(
            "[extract_query_context] No parsed_entities, using keyword inference"
        )

    # ====================
    # 6. TOP-N DETECTION
    # ====================
    context["has_top_n"] = bool(
        re.search(r"\btop\s+\d+", normalized)
        or re.search(r"\d+\s+maiore", normalized)
        or re.search(r"\d+\s+menore", normalized)
    )

    # ====================
    # 7. FOLLOWED BY DIMENSION PATTERN
    # ====================
    # Pattern: "dentro de [dimension]", "por [dimension]"
    followed_pattern = r"(?:dentro\s+de|por|em|nos)\s+(?:cada\s+)?(\w+)"
    context["followed_by_dimension"] = bool(re.search(followed_pattern, normalized))

    # ====================
    # 8. NEGATIONS (for boost conditions)
    # ====================
    context["no_temporal_comparison"] = not context["has_temporal_comparison"]
    context["no_comparison_keywords"] = not context["has_comparison_keywords"]

    logger.debug(f"[extract_query_context] Context extracted: {context}")

    return context


def analyze_temporal_context(query: str) -> dict:
    """
    Analisa contexto temporal especifico da query.

    Esta funcao e um helper para detectar patterns temporais mais complexos
    que podem ser usados na desambiguacao.

    Args:
        query: User query

    Returns:
        {
            "has_temporal_trend": bool,
            "has_temporal_comparison": bool,
            "temporal_granularity": str | None,  # "mes", "ano", "trimestre"
            "temporal_range_detected": bool,
            "continuous_time": bool,  # True para line, False para bar_vertical_composed
        }

    Examples:
        >>> analyze_temporal_context("evolucao de vendas por mes")
        {
            "has_temporal_trend": True,
            "has_temporal_comparison": False,
            "temporal_granularity": "mes",
            "continuous_time": True
        }

        >>> analyze_temporal_context("crescimento entre maio e junho")
        {
            "has_temporal_trend": False,
            "has_temporal_comparison": True,
            "temporal_granularity": "mes",
            "continuous_time": False
        }
    """
    normalized = normalize_text(query)
    temporal_context = {}

    # Trend keywords (continuous evolution)
    trend_keywords = ["evolucao", "historico", "tendencia", "ao longo", "timeline"]
    temporal_context["has_temporal_trend"] = any(
        kw in normalized for kw in trend_keywords
    )

    # Comparison keywords (discrete periods)
    comparison_keywords = ["crescimento", "variacao", "aumento", "reducao", "entre"]
    temporal_context["has_temporal_comparison"] = any(
        kw in normalized for kw in comparison_keywords
    )

    # Detect temporal granularity
    if "mes" in normalized or "mensal" in normalized:
        temporal_context["temporal_granularity"] = "mes"
    elif "ano" in normalized or "anual" in normalized:
        temporal_context["temporal_granularity"] = "ano"
    elif "trimestre" in normalized:
        temporal_context["temporal_granularity"] = "trimestre"
    elif "semestre" in normalized:
        temporal_context["temporal_granularity"] = "semestre"
    elif "dia" in normalized or "data" in normalized:
        temporal_context["temporal_granularity"] = "dia"
    else:
        temporal_context["temporal_granularity"] = None

    # Detect temporal range (e.g., "entre maio e junho", "de maio 2016 para junho 2016")
    range_patterns = [
        r"entre\s+\w+(?:\s+\d{4})?\s+e\s+\w+(?:\s+\d{4})?",
        r"de\s+\w+(?:\s+\d{4})?\s+a\s+\w+(?:\s+\d{4})?",
        r"de\s+\w+(?:\s+\d{4})?\s+para\s+\w+(?:\s+\d{4})?",
    ]
    temporal_context["temporal_range_detected"] = any(
        re.search(pattern, normalized) for pattern in range_patterns
    )

    # Determine if continuous time (line) or discrete comparison (bar_vertical_composed)
    if temporal_context["has_temporal_trend"]:
        temporal_context["continuous_time"] = True
    elif (
        temporal_context["has_temporal_comparison"]
        and temporal_context["temporal_range_detected"]
    ):
        temporal_context["continuous_time"] = False
    else:
        temporal_context["continuous_time"] = None

    return temporal_context


def analyze_comparison_context(query: str) -> dict:
    """
    Analisa contexto de comparacao da query.

    Args:
        query: User query

    Returns:
        {
            "is_comparison_query": bool,
            "comparison_type": str | None,  # "categorical", "temporal", "mixed"
            "explicit_categories": list[str],
            "category_count": int
        }

    Examples:
        >>> analyze_comparison_context("comparar vendas entre SP e RJ")
        {
            "is_comparison_query": True,
            "comparison_type": "categorical",
            "explicit_categories": ["SP", "RJ"],
            "category_count": 2
        }

        >>> analyze_comparison_context("comparar vendas entre janeiro e fevereiro")
        {
            "is_comparison_query": True,
            "comparison_type": "temporal",
            "explicit_categories": ["janeiro", "fevereiro"],
            "category_count": 2
        }
    """
    normalized = normalize_text(query)
    comparison_context = {}

    # Detect comparison intent
    comparison_keywords = [
        "comparar",
        "comparacao",
        "versus",
        "vs",
        "diferenca",
        "contrastar",
    ]
    comparison_context["is_comparison_query"] = any(
        kw in normalized for kw in comparison_keywords
    )

    # Extract explicit categories
    # Pattern: "entre X e Y", "X versus Y", "X, Y e Z"
    explicit_categories = []

    # Pattern 1: "entre X e Y"
    match = re.search(r"entre\s+(\w+)\s+e\s+(\w+)", normalized)
    if match:
        explicit_categories.extend([match.group(1), match.group(2)])

    # Pattern 2: "X versus Y" or "X vs Y"
    match = re.search(r"(\w+)\s+(?:versus|vs)\s+(\w+)", normalized)
    if match:
        explicit_categories.extend([match.group(1), match.group(2)])

    # Pattern 3: "X, Y e Z"
    # Mais complexo - capturar lista de items
    list_pattern = r"\b([A-Z]{2}(?:,\s*[A-Z]{2})*(?:\s+e\s+[A-Z]{2})?)\b"
    match = re.search(list_pattern, query)  # Use original query para capturar uppercase
    if match:
        items_str = match.group(1)
        items = re.split(r",\s*|\s+e\s+", items_str)
        explicit_categories.extend(items)

    comparison_context["explicit_categories"] = list(set(explicit_categories))
    comparison_context["category_count"] = len(
        comparison_context["explicit_categories"]
    )

    # Determine comparison type
    if comparison_context["is_comparison_query"]:
        temporal_terms = [
            "janeiro",
            "fevereiro",
            "marco",
            "abril",
            "maio",
            "junho",
            "julho",
            "agosto",
            "setembro",
            "outubro",
            "novembro",
            "dezembro",
            "q1",
            "q2",
            "q3",
            "q4",
        ]

        # Check if any explicit category is temporal
        has_temporal_cat = any(
            cat.lower() in temporal_terms for cat in explicit_categories
        )

        if has_temporal_cat:
            comparison_context["comparison_type"] = "temporal"
        elif len(explicit_categories) >= 2:
            comparison_context["comparison_type"] = "categorical"
        else:
            comparison_context["comparison_type"] = None
    else:
        comparison_context["comparison_type"] = None

    return comparison_context


def detect_filter_vs_dimension_intent(
    query: str, parsed_entities: Optional[dict] = None
) -> dict:
    """
    Detecta se valores mencionados sao filtros ou dimensions para comparacao.

    Esta funcao e critica para resolver o problema:
    - "top 4 clientes de SP" → SP e FILTER
    - "comparar vendas entre SP e RJ" → SP e RJ sao DIMENSION values

    Args:
        query: User query
        parsed_entities: Entidades parseadas (opcional)

    Returns:
        {
            "is_filter": bool,
            "is_dimension": bool,
            "reason": str
        }

    Examples:
        >>> detect_filter_vs_dimension_intent("top 4 clientes de SP")
        {
            "is_filter": True,
            "is_dimension": False,
            "reason": "Single location value with ranking keyword"
        }

        >>> detect_filter_vs_dimension_intent("comparar vendas entre SP e RJ")
        {
            "is_filter": False,
            "is_dimension": True,
            "reason": "Multiple values with comparison keyword"
        }
    """
    normalized = normalize_text(query)
    intent = {}

    # Detectar keywords de comparacao
    comparison_keywords = ["comparar", "versus", "vs", "diferenca", "entre"]
    has_comparison = any(kw in normalized for kw in comparison_keywords)

    # Detectar keywords de ranking
    ranking_keywords = ["top", "ranking", "maiores", "menores"]
    has_ranking = any(kw in normalized for kw in ranking_keywords)

    # Detectar localizacao (pattern: "de [localizacao]", "em [localizacao]")
    location_pattern = r"(?:de|em|do|da|nos|nas)\s+([A-Z]{2})\b"
    location_matches = re.findall(location_pattern, query)  # Use original query

    # Detectar multiplos valores de localizacao
    multi_location_pattern = r"([A-Z]{2}(?:,\s*[A-Z]{2})*(?:\s+e\s+[A-Z]{2})?)"
    multi_matches = re.findall(multi_location_pattern, query)

    num_locations = len(location_matches) + sum(
        len(re.split(r",\s*|\s+e\s+", match)) for match in multi_matches
    )

    # REGRA 1: Ranking + single location → FILTER
    if has_ranking and num_locations == 1:
        intent["is_filter"] = True
        intent["is_dimension"] = False
        intent["reason"] = "Single location value with ranking keyword"
        logger.debug(
            f"[detect_filter_vs_dimension_intent] Detected FILTER: {location_matches}"
        )
        return intent

    # REGRA 2: Comparison + multiple locations → DIMENSION
    if has_comparison and num_locations >= 2:
        intent["is_filter"] = False
        intent["is_dimension"] = True
        intent["reason"] = "Multiple values with comparison keyword"
        logger.debug(
            f"[detect_filter_vs_dimension_intent] Detected DIMENSION: {num_locations} locations"
        )
        return intent

    # REGRA 3: Multiple locations without comparison → DIMENSION (ambiguo, mas assume dimension)
    if num_locations >= 2:
        intent["is_filter"] = False
        intent["is_dimension"] = True
        intent["reason"] = "Multiple location values (likely dimension)"
        logger.debug(
            f"[detect_filter_vs_dimension_intent] Multiple locations detected as DIMENSION"
        )
        return intent

    # REGRA 4: Default para single value → FILTER
    if num_locations == 1:
        intent["is_filter"] = True
        intent["is_dimension"] = False
        intent["reason"] = "Single location value (default to filter)"
        return intent

    # Nenhum pattern detectado
    intent["is_filter"] = False
    intent["is_dimension"] = False
    intent["reason"] = "No clear filter or dimension pattern"
    return intent
