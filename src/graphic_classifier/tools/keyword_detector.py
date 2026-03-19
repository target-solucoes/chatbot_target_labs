"""
Keyword detection for chart type classification.

This module provides functions to detect keywords in queries that
indicate specific chart types or operations.

PHASE 2 ENHANCEMENT: Context-Aware Keywords
- Added weighted keyword scoring based on semantic context
- Keywords now have dynamic weights based on query context
- Resolves ambiguities like "top 5 produtos" vs "top 5 produtos com crescimento"
"""

import re
import logging
from typing import List, Dict, Optional

from src.graphic_classifier.utils.text_cleaner import normalize_text
from src.graphic_classifier.core.settings import VALID_CHART_TYPES
from src.graphic_classifier.tools.context_analyzer import extract_query_context
from src.graphic_classifier.tools.keyword_weights_config import (
    KEYWORD_WEIGHTS,
    get_keyword_config,
)


logger = logging.getLogger(__name__)


# Chart type keyword mappings (from project specs)
CHART_TYPE_KEYWORDS = {
    "bar_horizontal": [
        # Rankings, comparisons of greatest/least values, top-N
        "top",
        "ranking",
        "maiores",
        "menores",
        "mais vendidos",
        "melhores",
        "piores",
        "quem vendeu mais",
        "maior",
        "menor",
        "primeiro",
        "primeiros",
        "ultimo",
        "ultimos",
        "classificacao",
        "ordenar",
        "listar",
    ],
    "bar_vertical": [
        # Direct comparisons between categories
        "comparar",
        "comparacao",
        "diferenca entre",
        "versus",
        "vs",
        "entre",
        "qual e maior entre",
        "como se comparam",
        "diferenca",
        "contrastar",
        "lado a lado",
    ],
    "bar_vertical_composed": [
        # Comparison between periods or conditions within same category
        "crescimento",
        "variacao",
        "mudanca",
        "antes e depois",
        "entre meses",
        "entre anos",
        "por periodo",
        "evolucao por categoria",
        "comparar periodos",
    ],
    "line": [
        # Temporal trends, historical series, continuous evolution
        "historico",
        "ao longo do tempo",
        "tendencia",
        "evolucao",
        "como tem sido",
        "desde",
        "por mes",
        "por ano",
        "timeline",
        "serie temporal",
        "tempo",
        "periodo",
        "cronologico",
        "temporal",
    ],
    "line_composed": [
        # Comparison of multiple categories over time
        "comparar",
        "evolucao de",
        "variacao entre",
        "linha por",
        "trend",
        "evolucao comparada",
        "multiplas linhas",
        "comparar evolucao",
        "trend de multiplos",
    ],
    "pie": [
        # Percentage composition, participation, market share
        "distribuicao",
        "proporcao",
        "percentual",
        "participacao",
        "quota",
        "divisao",
        "representa quanto",
        "concentracao",
        "porcentagem de",
        "fatia",
        "composicao percentual",
        "share",
        "parcela",
    ],
    "bar_vertical_stacked": [
        # Composition of subcategories within main categories
        "composicao",
        "distribuicao dentro de",
        "divisao por",
        "por subcategoria",
        "dentro dos",
        "entre os maiores",
        "como se distribui",
        "empilhado",
        "stacked",
        "composicao por",
        "dentro",
        "por",
    ],
    "histogram": [
        # Distribution of numeric values, dispersion, variability
        "distribuicao de",
        "frequencia",
        "quantos",
        "variabilidade",
        "faixa",
        "intervalo",
        "spread",
        "como se distribuem",
        "valores entre",
        "dispersao",
        "histograma",
    ],
}


# Aggregation keywords
AGGREGATION_KEYWORDS = {
    "sum": ["soma", "somar", "total", "totalizar", "somatorio", "quantidade"],
    "avg": ["media", "medio", "mediana", "average"],
    "count": ["contar", "unicos", "quantos", "contagem"],
    "min": ["minimo", "menor", "min"],
    "max": ["maximo", "maior", "max"],
    "median": ["mediana"],
}


# Sorting keywords
SORT_KEYWORDS = {
    "desc": [
        "maiores",
        "top",
        "ranking",
        "melhores",
        "mais",
        "descendente",
        "decrescente",
        "maior para menor",
    ],
    "asc": [
        "menores",
        "piores",
        "menos",
        "crescente",
        "ascendente",
        "menor para maior",
    ],
}


# Filter keywords
FILTER_KEYWORDS = [
    "em",
    "no",
    "na",
    "nos",
    "nas",
    "onde",
    "quando",
    "para",
    "do",
    "da",
    "dos",
    "das",
    "apenas",
    "somente",
    "filtrar",
    "selecionar",
]


# Grouping keywords
GROUPING_KEYWORDS = [
    "por",
    "agrupado por",
    "grupo",
    "categoria",
    "dividido por",
    "separado por",
    "segmentado por",
]


def detect_keywords(query: str) -> List[str]:
    """
    Detect all relevant keywords in the query.

    Args:
        query: User query

    Returns:
        List of detected keywords

    Examples:
        >>> detect_keywords("top 5 produtos mais vendidos")
        ['top', 'mais vendidos', 'vendidos']
    """
    normalized = normalize_text(query)
    detected = []

    # Check all chart type keywords
    for chart_type, keywords in CHART_TYPE_KEYWORDS.items():
        for keyword in keywords:
            # Normalize keyword for matching
            normalized_keyword = normalize_text(keyword)
            # Skip empty normalized keywords (like '%')
            if not normalized_keyword or not normalized_keyword.strip():
                # Special case: check for '%' directly in original query
                if keyword == "%" and "%" in query:
                    detected.append(keyword)
                continue
            # Use word boundary matching with regex
            pattern = r"\b" + re.escape(normalized_keyword) + r"\b"
            if re.search(pattern, normalized):
                detected.append(keyword)

    # Check aggregation keywords
    for agg_type, keywords in AGGREGATION_KEYWORDS.items():
        for keyword in keywords:
            normalized_keyword = normalize_text(keyword)
            if not normalized_keyword or not normalized_keyword.strip():
                continue
            pattern = r"\b" + re.escape(normalized_keyword) + r"\b"
            if re.search(pattern, normalized):
                detected.append(keyword)

    # Check sort keywords
    for sort_type, keywords in SORT_KEYWORDS.items():
        for keyword in keywords:
            normalized_keyword = normalize_text(keyword)
            if not normalized_keyword or not normalized_keyword.strip():
                continue
            pattern = r"\b" + re.escape(normalized_keyword) + r"\b"
            if re.search(pattern, normalized):
                detected.append(keyword)

    return list(set(detected))


def get_chart_type_hints(
    query: str,
    parsed_entities: Optional[dict] = None,
    use_weighted_scoring: bool = True,
) -> Dict[str, float]:
    """
    Get chart type hints with confidence scores.

    PHASE 2 ENHANCEMENT: Now supports context-aware weighted scoring.

    Analyzes keywords and returns a scored dictionary of
    possible chart types. If use_weighted_scoring=True, uses
    context-aware weights from KEYWORD_WEIGHTS config.

    Args:
        query: User query
        parsed_entities: Optional parsed entities (for dimension analysis)
        use_weighted_scoring: If True, use context-aware weights (default: True)

    Returns:
        Dictionary mapping chart types to confidence scores (0.0 to 1.0)

    Examples:
        >>> get_chart_type_hints("top 10 produtos mais vendidos")
        {'bar_horizontal': 0.95, 'bar_vertical': 0.2}

        >>> get_chart_type_hints("top 5 produtos com crescimento entre maio e junho")
        {'bar_vertical_composed': 0.95, 'bar_horizontal': 0.5}
    """
    normalized = normalize_text(query)
    scores = {chart_type: 0.0 for chart_type in VALID_CHART_TYPES}

    # PHASE 2: Extract context for weighted scoring
    context = None
    if use_weighted_scoring:
        context = extract_query_context(query, parsed_entities)
        logger.debug(f"[get_chart_type_hints] Extracted context: {context}")

    # Count keyword matches for each chart type
    for chart_type, keywords in CHART_TYPE_KEYWORDS.items():
        if use_weighted_scoring:
            # PHASE 2: Use weighted scoring
            matched_keywords = []
            for keyword in keywords:
                if _keyword_matches_in_query(keyword, query, normalized):
                    matched_keywords.append(keyword)

            if matched_keywords:
                weighted_score = calculate_weighted_score(
                    chart_type, matched_keywords, context
                )
                scores[chart_type] = weighted_score
                logger.debug(
                    f"[get_chart_type_hints] {chart_type}: matched={matched_keywords}, "
                    f"weighted_score={weighted_score:.2f}"
                )
        else:
            # Original scoring logic (backward compatibility)
            matches = 0
            for keyword in keywords:
                if _keyword_matches_in_query(keyword, query, normalized):
                    matches += 1

            # Calculate confidence score
            if matches > 0:
                # Base score from keyword matches
                base_score = min(matches / 3.0, 1.0)  # Cap at 3 matches for full score

                # Boost score for very specific keywords
                specific_keywords = {
                    "bar_horizontal": ["top", "ranking"],
                    "line": ["historico", "tendencia", "evolucao"],
                    "pie": ["distribuicao", "proporcao", "percentual"],
                    "histogram": ["frequencia", "histograma"],
                }

                if chart_type in specific_keywords:
                    for specific_kw in specific_keywords[chart_type]:
                        normalized_specific_kw = normalize_text(specific_kw)
                        if (
                            not normalized_specific_kw
                            or not normalized_specific_kw.strip()
                        ):
                            continue
                        pattern = r"\b" + re.escape(normalized_specific_kw) + r"\b"
                        if re.search(pattern, normalized):
                            base_score = min(base_score + 0.2, 1.0)

                scores[chart_type] = base_score

    # Apply heuristics for disambiguation
    scores = apply_disambiguation_heuristics(query, normalized, scores)

    return scores


def _keyword_matches_in_query(keyword: str, query: str, normalized: str) -> bool:
    """
    Helper function to check if a keyword matches in the query.

    Args:
        keyword: Keyword to search for
        query: Original query (for special cases like '%')
        normalized: Normalized query text

    Returns:
        True if keyword matches
    """
    # Special case for '%'
    if keyword == "%":
        return "%" in query

    # Normalize keyword for matching
    normalized_keyword = normalize_text(keyword)

    # Skip empty normalized keywords
    if not normalized_keyword or not normalized_keyword.strip():
        return False

    # Use word boundary matching with regex
    pattern = r"\b" + re.escape(normalized_keyword) + r"\b"
    return bool(re.search(pattern, normalized))


def calculate_weighted_score(
    chart_type: str, matched_keywords: List[str], context: dict
) -> float:
    """
    Calculate weighted score based on matched keywords and context.

    PHASE 2 CORE FUNCTION: This implements context-aware keyword weighting.

    For each matched keyword:
    1. Get base_weight from KEYWORD_WEIGHTS config
    2. Check boost_conditions against context
    3. Add boosts for satisfied conditions
    4. Calculate average across all matched keywords

    Args:
        chart_type: Type of chart
        matched_keywords: List of keywords detected in query
        context: Context dictionary from extract_query_context()

    Returns:
        Weighted score (0.0 to 1.0)

    Examples:
        >>> context = {"has_temporal_comparison": False, "no_temporal_comparison": True}
        >>> calculate_weighted_score("bar_horizontal", ["top"], context)
        0.8  # 0.5 base + 0.3 boost (no_temporal_comparison)

        >>> context = {"has_temporal_comparison": True, "has_temporal_dimension": True}
        >>> calculate_weighted_score("bar_vertical_composed", ["crescimento"], context)
        0.9  # 0.6 base + 0.3 boost (has_temporal_dimension)
    """
    if not matched_keywords:
        return 0.0

    total_score = 0.0
    scored_keywords = 0

    for keyword in matched_keywords:
        keyword_config = get_keyword_config(chart_type, keyword)

        if not keyword_config:
            # Keyword not in weighted config, use default weight
            logger.debug(
                f"[calculate_weighted_score] Keyword '{keyword}' not in config for {chart_type}, "
                f"using default weight 0.3"
            )
            total_score += 0.3
            scored_keywords += 1
            continue

        # Get base weight
        base_weight = keyword_config.get("base_weight", 0.0)

        # Calculate boosts from context
        boost_conditions = keyword_config.get("boost_conditions", {})
        boost_total = 0.0

        for condition_name, boost_value in boost_conditions.items():
            if context.get(condition_name, False):
                boost_total += boost_value
                logger.debug(
                    f"[calculate_weighted_score] {chart_type}.{keyword}: "
                    f"condition '{condition_name}' satisfied, boost +{boost_value}"
                )

        # Calculate final weight for this keyword (capped at 1.0)
        keyword_score = min(base_weight + boost_total, 1.0)
        total_score += keyword_score
        scored_keywords += 1

        logger.debug(
            f"[calculate_weighted_score] {chart_type}.{keyword}: "
            f"base={base_weight}, boosts={boost_total}, final={keyword_score:.2f}"
        )

    # Return average score across all matched keywords
    if scored_keywords > 0:
        avg_score = total_score / scored_keywords
        logger.debug(
            f"[calculate_weighted_score] {chart_type}: "
            f"total={total_score:.2f}, keywords={scored_keywords}, avg={avg_score:.2f}"
        )
        return min(avg_score, 1.0)

    return 0.0


def detect_multiple_dimensions(query: str, normalized: str) -> bool:
    """
    Detect if query involves multiple distinct categorical dimensions semantically.

    This function identifies patterns where a query asks for composition/breakdown
    of one category within another, such as:
    - "top N [dimension1] nos [top M] [dimension2]"
    - "[dimension1] por [dimension2]"
    - "[dimension1] dentro de [dimension2]"

    Args:
        query: Original query
        normalized: Normalized query text

    Returns:
        True if multiple distinct dimensions are detected, False otherwise

    Examples:
        >>> detect_multiple_dimensions("top 3 produtos nos 5 maiores clientes", "top 3 produtos nos 5 maiores clientes")
        True
        >>> detect_multiple_dimensions("vendas por produto dentro de cada regiao", "vendas por produto dentro de cada regiao")
        True
        >>> detect_multiple_dimensions("top 5 produtos mais vendidos", "top 5 produtos mais vendidos")
        False
    """
    # Pattern 1: "top N [dimension1] nos [top M] [dimension2]"
    # Example: "top 3 produtos nos 5 maiores clientes"
    pattern1 = re.search(
        r"(?:top|primeiro[s]?|ranking)\s+\d+\s+(\w+).*?(?:nos|em|dentro\s+de|entre)\s+(?:\d+\s+)?(?:maiores|menores|melhores|piores|top)\s+(\w+)",
        normalized,
        re.IGNORECASE,
    )
    if pattern1:
        dim1 = pattern1.group(1)
        dim2 = pattern1.group(2)
        # Check if dimensions are different (not same word)
        if dim1 != dim2 and len(dim1) > 2 and len(dim2) > 2:
            logger.debug(
                f"[detect_multiple_dimensions] Pattern 1 matched: {dim1} within {dim2}"
            )
            return True

    # Pattern 2: "[dimension1] por [dimension2]" with composition context
    # Example: "vendas por produto dentro de cada regiao"
    pattern2 = re.search(
        r"por\s+(\w+).*?(?:dentro\s+de|em|nos|nas)\s+(?:cada|os|as|as|o|a)\s+(\w+)",
        normalized,
        re.IGNORECASE,
    )
    if pattern2:
        dim1 = pattern2.group(1)
        dim2 = pattern2.group(2)
        if dim1 != dim2 and len(dim1) > 2 and len(dim2) > 2:
            logger.debug(
                f"[detect_multiple_dimensions] Pattern 2 matched: {dim1} within {dim2}"
            )
            return True

    # Pattern 3: "[dimension1] dentro de [dimension2]" or "[dimension1] em [dimension2]"
    # Example: "produtos dentro de cada cliente"
    pattern3 = re.search(
        r"(\w+).*?(?:dentro\s+de|em|nos|nas)\s+(?:cada|os|as|o|a)\s+(\w+)",
        normalized,
        re.IGNORECASE,
    )
    if pattern3:
        dim1 = pattern3.group(1)
        dim2 = pattern3.group(2)
        # Exclude common stopwords and metric words
        stopwords = {
            "vendas",
            "venda",
            "valor",
            "valores",
            "total",
            "quantidade",
            "qtd",
            "numero",
        }
        if (
            dim1 not in stopwords
            and dim2 not in stopwords
            and dim1 != dim2
            and len(dim1) > 2
            and len(dim2) > 2
        ):
            logger.debug(
                f"[detect_multiple_dimensions] Pattern 3 matched: {dim1} within {dim2}"
            )
            return True

    # Pattern 4: Multiple distinct category nouns separated by prepositions
    # Example: "composicao de produtos por cliente"
    category_nouns = [
        "produto",
        "cliente",
        "vendedor",
        "estado",
        "regiao",
        "cidade",
        "pais",
        "setor",
        "categoria",
        "segmento",
        "linha",
        "marca",
    ]
    found_categories = []
    for cat in category_nouns:
        pattern = r"\b" + re.escape(cat) + r"s?\b"
        if re.search(pattern, normalized, re.IGNORECASE):
            found_categories.append(cat)

    # If we found 2+ distinct categories, likely a composition query
    if len(found_categories) >= 2:
        # Check if they're mentioned in composition context
        composition_keywords = [
            "por",
            "dentro",
            "em",
            "nos",
            "nas",
            "composicao",
            "distribuicao",
        ]

        # Helper function for keyword matching
        def keyword_matches_local(kw: str, text: str) -> bool:
            normalized_kw = normalize_text(kw)
            if not normalized_kw or not normalized_kw.strip():
                return False
            pattern = r"\b" + re.escape(normalized_kw) + r"\b"
            return bool(re.search(pattern, text))

        has_composition_context = any(
            keyword_matches_local(kw, normalized) for kw in composition_keywords
        )
        if has_composition_context:
            logger.debug(
                f"[detect_multiple_dimensions] Pattern 4 matched: multiple categories {found_categories}"
            )
            return True

    return False


def apply_disambiguation_heuristics(
    query: str, normalized: str, scores: Dict[str, float]
) -> Dict[str, float]:
    """
    Apply heuristics to disambiguate between similar chart types.

    Args:
        query: Original query
        normalized: Normalized query
        scores: Initial chart type scores

    Returns:
        Updated scores after applying heuristics
    """

    # Helper to check if keyword matches with word boundaries
    def keyword_matches(kw: str, text: str) -> bool:
        # Handle special case for '%'
        if kw == "%":
            return "%" in query
        normalized_kw = normalize_text(kw)
        # Skip empty normalized keywords
        if not normalized_kw or not normalized_kw.strip():
            return False
        pattern = r"\b" + re.escape(normalized_kw) + r"\b"
        return bool(re.search(pattern, text))

    # Heuristic 0: Multiple dimensions detection (NEW - Priority)
    # This detects composition queries like "top N produtos nos top M clientes"
    if detect_multiple_dimensions(query, normalized):
        scores["bar_vertical_stacked"] = max(scores["bar_vertical_stacked"], 0.9)
        # Reduce bar_horizontal score if it was boosted by ranking keywords
        # because composition queries should prioritize stacked bars
        if scores.get("bar_horizontal", 0) > 0.7:
            scores["bar_horizontal"] = min(scores["bar_horizontal"], 0.6)
        logger.debug(
            "[apply_disambiguation_heuristics] Multiple dimensions detected - boosting stacked bar"
        )

    # Heuristic 1: Temporal keywords strongly suggest line charts
    temporal_keywords = [
        "historico",
        "ao longo",
        "tempo",
        "mes",
        "ano",
        "periodo",
        "desde",
        "data",
    ]
    if any(keyword_matches(kw, normalized) for kw in temporal_keywords):
        scores["line"] = max(scores["line"], 0.8)
        scores["line_composed"] = max(scores["line_composed"], 0.6)

    # Heuristic 2: "Top N" or ranking keywords strongly suggest horizontal bar
    # BUT: Only if we haven't detected multiple dimensions (which would be stacked)
    # AND: Only if there's NO temporal comparison context (CRITICAL FIX - Phase 1)
    if not detect_multiple_dimensions(query, normalized):
        # VERIFICAR se ha contexto temporal ANTES de boost (CRITICAL FIX)
        temporal_comparison_keywords = [
            "crescimento",
            "variacao",
            "aumento",
            "reducao",
            "mudanca",
        ]
        has_temporal = any(
            keyword_matches(kw, normalized) for kw in temporal_comparison_keywords
        )

        # Verificar se ha pattern "entre [periodo1] e [periodo2]"
        has_period_comparison = bool(re.search(r"entre\s+\w+\s+e\s+\w+", normalized))

        if (
            re.search(r"\btop\s+\d+", normalized)
            or re.search(r"\d+\s+maiore", normalized)
            or re.search(r"\d+\s+menore", normalized)
            or re.search(r"\d+\s+melhore", normalized)
            or re.search(r"\d+\s+piore", normalized)
        ):
            if not has_temporal and not has_period_comparison:
                # APENAS boost bar_horizontal se NAO ha comparacao temporal
                scores["bar_horizontal"] = max(scores["bar_horizontal"], 0.9)
                logger.debug(
                    "[Heuristic 2] Ranking keyword detected without temporal context - boosting bar_horizontal"
                )
            else:
                # SE tem temporal, boost bar_vertical_composed ao inves
                scores["bar_vertical_composed"] = max(
                    scores["bar_vertical_composed"], 0.95
                )
                logger.debug(
                    "[Heuristic 2] Ranking keyword WITH temporal context - boosting bar_vertical_composed"
                )

        # Also boost for ranking keywords even without explicit numbers
        elif any(
            keyword_matches(kw, normalized)
            for kw in ["maiores", "menores", "melhores", "piores", "ranking"]
        ):
            if not has_temporal and not has_period_comparison:
                scores["bar_horizontal"] = max(scores["bar_horizontal"], 0.85)
                logger.debug(
                    "[Heuristic 2] Ranking keyword (no number) without temporal context - boosting bar_horizontal"
                )
            else:
                scores["bar_vertical_composed"] = max(
                    scores["bar_vertical_composed"], 0.90
                )
                logger.debug(
                    "[Heuristic 2] Ranking keyword (no number) WITH temporal context - boosting bar_vertical_composed"
                )

    # Heuristic 3: Percentage/proportion keywords suggest pie chart
    if any(
        keyword_matches(kw, normalized)
        for kw in ["percentual", "proporcao", "porcentagem", "%", "participacao"]
    ):
        scores["pie"] = max(scores["pie"], 0.85)

    # Heuristic 4: "Comparar" and "versus" with temporal context suggests composed charts
    has_comparison = (
        keyword_matches("comparar", normalized)
        or keyword_matches("comparacao", normalized)
        or keyword_matches("versus", normalized)
        or keyword_matches("vs", normalized)
    )

    if has_comparison:
        if any(keyword_matches(kw, normalized) for kw in temporal_keywords):
            scores["line_composed"] = max(scores["line_composed"], 0.75)
            scores["bar_vertical_composed"] = max(scores["bar_vertical_composed"], 0.7)
        else:
            scores["bar_vertical"] = max(scores["bar_vertical"], 0.8)

    # Heuristic 5: Multiple categories + temporal = composed chart
    has_multiple_categories = normalized.count(" e ") > 1 or normalized.count(",") > 1
    has_temporal = any(keyword_matches(kw, normalized) for kw in temporal_keywords)

    if has_multiple_categories and has_temporal:
        scores["line_composed"] = max(scores["line_composed"], 0.75)
    elif has_multiple_categories:
        scores["bar_vertical_composed"] = max(scores["bar_vertical_composed"], 0.7)

    # Heuristic 6: "Distribuição dentro de" suggests stacked bar
    if keyword_matches("distribuicao", normalized) and keyword_matches(
        "dentro", normalized
    ):
        scores["bar_vertical_stacked"] = max(scores["bar_vertical_stacked"], 0.85)

    # Heuristic 7: Frequency/range keywords suggest histogram
    if any(
        keyword_matches(kw, normalized)
        for kw in ["frequencia", "faixa", "intervalo", "distribuicao de valores"]
    ):
        scores["histogram"] = max(scores["histogram"], 0.8)

    return scores


def get_best_chart_type(
    query_or_keywords,
    query: Optional[str] = None,
    threshold: float = 0.5,
    parsed_entities: Optional[dict] = None,
    use_weighted_scoring: bool = True,
) -> Optional[str]:
    """
    Get the best matching chart type for a query.

    PHASE 2 ENHANCEMENT: Now supports context-aware weighted scoring.

    Args:
        query_or_keywords: Either a query string or keywords list (for backward compatibility)
        query: Optional query string (if first arg is keywords list)
        threshold: Minimum confidence threshold (default: 0.5)
        parsed_entities: Optional parsed entities (for context analysis)
        use_weighted_scoring: If True, use context-aware weights (default: True)

    Returns:
        Best matching chart type or None if no confident match

    Examples:
        >>> get_best_chart_type("top 5 produtos mais vendidos")
        'bar_horizontal'
        >>> get_best_chart_type("evolução das vendas por mês")
        'line'
        >>> get_best_chart_type(['top', 'produtos'], "top 5 produtos")
        'bar_horizontal'
    """
    # Handle both calling conventions
    if isinstance(query_or_keywords, str):
        # Called with query string directly
        actual_query = query_or_keywords
    else:
        # Called with keywords list first (legacy style from tests)
        actual_query = query if query else ""

    scores = get_chart_type_hints(actual_query, parsed_entities, use_weighted_scoring)

    # Find chart type with highest score
    best_type = max(scores.items(), key=lambda x: x[1])

    if best_type[1] >= threshold:
        logger.debug(
            f"Best chart type: {best_type[0]} (confidence: {best_type[1]:.2f})"
        )
        return best_type[0]

    logger.debug(
        f"No confident chart type match (best: {best_type[0]} at {best_type[1]:.2f})"
    )
    return None


def detect_aggregation_hint(query: str) -> Optional[str]:
    """
    Detect aggregation function hint from query.

    Args:
        query: User query

    Returns:
        Aggregation function name or None

    Examples:
        >>> detect_aggregation_hint("média de vendas")
        'avg'
        >>> detect_aggregation_hint("contar clientes")
        'count'
    """
    normalized = normalize_text(query)

    # Helper to check if keyword matches with word boundaries
    def keyword_matches(kw: str, text: str) -> bool:
        normalized_kw = normalize_text(kw)
        pattern = r"\b" + re.escape(normalized_kw) + r"\b"
        return bool(re.search(pattern, text))

    # Check for explicit aggregation keywords
    for agg_func, keywords in AGGREGATION_KEYWORDS.items():
        for keyword in keywords:
            if keyword_matches(keyword, normalized):
                return agg_func

    # Infer from context
    if any(
        keyword_matches(kw, normalized)
        for kw in ["vendas", "faturamento", "receita", "valor"]
    ):
        return "sum"

    if any(
        keyword_matches(kw, normalized) for kw in ["quantidade", "numero", "quantos"]
    ):
        return "count"

    return None


def detect_sort_order(query: str) -> Optional[str]:
    """
    Detect sort order from query.

    Args:
        query: User query

    Returns:
        'asc', 'desc', or None

    Examples:
        >>> detect_sort_order("top 10 produtos")
        'desc'
        >>> detect_sort_order("menores valores")
        'asc'
    """
    normalized = normalize_text(query)

    # Helper to check if keyword matches with word boundaries
    def keyword_matches(kw: str, text: str) -> bool:
        normalized_kw = normalize_text(kw)
        pattern = r"\b" + re.escape(normalized_kw) + r"\b"
        return bool(re.search(pattern, text))

    # Check for explicit sort keywords
    for sort_order, keywords in SORT_KEYWORDS.items():
        for keyword in keywords:
            if keyword_matches(keyword, normalized):
                return sort_order

    # Default for rankings is descending
    if any(keyword_matches(kw, normalized) for kw in ["top", "ranking", "maiores"]):
        return "desc"

    return None


def detect_grouping(query: str) -> List[str]:
    """
    Detect grouping indicators in query.

    Args:
        query: User query

    Returns:
        List of potential grouping terms

    Examples:
        >>> detect_grouping("vendas por estado e produto")
        ['estado', 'produto']
    """
    normalized = normalize_text(query)
    groupings = []
    seen = set()

    # Pattern: "por X", "agrupado por X", etc.
    # Also handle "por X e Y" or "por X, Y"
    patterns = [
        r"por\s+([^,\s]+(?:\s+e\s+[^,\s]+)*)",  # Captures "por X e Y"
        r"agrupado\s+por\s+([^,\s]+(?:\s+e\s+[^,\s]+)*)",
        r"dividido\s+por\s+([^,\s]+(?:\s+e\s+[^,\s]+)*)",
        r"separado\s+por\s+([^,\s]+(?:\s+e\s+[^,\s]+)*)",
    ]

    for pattern in patterns:
        matches = re.findall(pattern, normalized)
        for match in matches:
            # Split by " e " to get multiple groupings
            parts = re.split(r"\s+e\s+", match)
            for part in parts:
                part = part.strip()
                if part and part not in seen:
                    groupings.append(part)
                    seen.add(part)

    return groupings


def detect_hue_column(query: str) -> Optional[str]:
    """
    Detect column that should be used for color differentiation (hue).

    Args:
        query: User query

    Returns:
        Term suggesting hue column or None

    Examples:
        >>> detect_hue_column("vendas por estado e produto")
        'produto'
    """
    groupings = detect_grouping(query)

    # If multiple groupings, second one is often the hue
    if len(groupings) >= 2:
        return groupings[1]

    return None


def requires_top_n(query: str) -> bool:
    """
    Check if query requires limiting to top N results.

    Args:
        query: User query

    Returns:
        True if top N is required

    Examples:
        >>> requires_top_n("top 5 produtos")
        True
        >>> requires_top_n("todos os produtos")
        False
    """
    normalized = normalize_text(query)

    top_n_indicators = [
        r"\btop\s+\d+",
        r"\d+\s+maiore",
        r"\d+\s+menore",
        r"\d+\s+primeiro",
        r"ranking",
    ]

    return any(re.search(pattern, normalized) for pattern in top_n_indicators)


def get_keyword_summary(
    query: str,
    parsed_entities: Optional[dict] = None,
    use_weighted_scoring: bool = True,
) -> Dict[str, any]:
    """
    Get a comprehensive summary of detected keywords and hints.

    PHASE 2 ENHANCEMENT: Now includes context information.

    Args:
        query: User query
        parsed_entities: Optional parsed entities
        use_weighted_scoring: If True, use context-aware weights

    Returns:
        Dictionary with all detected keywords, hints, and context

    Examples:
        >>> summary = get_keyword_summary("top 5 produtos mais vendidos em 2015")
        >>> summary['chart_type']
        'bar_horizontal'
        >>> summary['sort_order']
        'desc'
        >>> summary['context']['no_temporal_comparison']
        True
    """
    context = (
        extract_query_context(query, parsed_entities) if use_weighted_scoring else None
    )

    return {
        "keywords": detect_keywords(query),
        "chart_type": get_best_chart_type(
            query,
            parsed_entities=parsed_entities,
            use_weighted_scoring=use_weighted_scoring,
        ),
        "chart_type_scores": get_chart_type_hints(
            query, parsed_entities, use_weighted_scoring
        ),
        "aggregation": detect_aggregation_hint(query),
        "sort_order": detect_sort_order(query),
        "groupings": detect_grouping(query),
        "hue_hint": detect_hue_column(query),
        "requires_top_n": requires_top_n(query),
        "context": context,  # PHASE 2: Include context
    }
