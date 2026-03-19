"""
Query parsing utilities for entity extraction.

This module provides functions to parse user queries and extract
relevant entities such as numbers, dates, categories, and column references.
"""

import re
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from src.graphic_classifier.utils.text_cleaner import (
    normalize_text,
    extract_numbers,
    extract_quoted_terms,
)
from src.graphic_classifier.utils.ranking_detector import (
    extract_ranking_info,
    has_ranking_keywords,
)


logger = logging.getLogger(__name__)


# Patterns for entity detection
YEAR_PATTERN = r"\b(?:19|20)\d{2}\b"
MONTH_NAMES_PT = {
    "janeiro": 1,
    "jan": 1,
    "fevereiro": 2,
    "fev": 2,
    "março": 3,
    "mar": 3,
    "abril": 4,
    "abr": 4,
    "maio": 5,
    "mai": 5,
    "junho": 6,
    "jun": 6,
    "julho": 7,
    "jul": 7,
    "agosto": 8,
    "ago": 8,
    "setembro": 9,
    "set": 9,
    "outubro": 10,
    "out": 10,
    "novembro": 11,
    "nov": 11,
    "dezembro": 12,
    "dez": 12,
}

# Operators that indicate ranking or ordering
RANKING_OPERATORS = [
    "top",
    "ranking",
    "maiores",
    "menores",
    "melhores",
    "piores",
    "primeiro",
    "primeiros",
    "ultimo",
    "ultimos",
    "maior",
    "menor",
]

# Operators for comparison
COMPARISON_OPERATORS = ["comparar", "comparação", "versus", "vs", "entre", "diferença"]

# Operators for temporal analysis
TEMPORAL_OPERATORS = [
    "histórico",
    "tendência",
    "evolução",
    "ao longo",
    "desde",
    "timeline",
    "crescimento",
    "variação",
    "mudança",
]

# Aggregation operators
# FASE 1.3: Removido mapeamento incorreto "maior" → "max"
# "maior aumento de vendas" NÃO significa MAX, significa SUM + ORDER DESC
AGGREGATION_OPERATORS = {
    "soma": "sum",
    "somar": "sum",
    "total": "sum",
    "totalizar": "sum",
    "média": "avg",
    "media": "avg",
    "médio": "avg",
    "medio": "avg",
    "contar": "count",
    "quantidade": "count",
    "número": "count",
    "numero": "count",
    "mínimo": "min",
    "minimo": "min",
    # REMOVIDO: "menor": "min"  # Ambíguo - pode significar ordenação
    "máximo": "max",
    "maximo": "max",
    # REMOVIDO: "maior": "max"  # Ambíguo - pode significar ordenação
    "mediana": "median",
}


def parse_query(query: str) -> Dict[str, Any]:
    """
    Parse user query and extract relevant entities.

    Extracts:
    - Numbers (top N, years, values)
    - Dates and temporal references
    - Operators (ranking, comparison, temporal)
    - Aggregation functions
    - Quoted terms (explicit references)
    - Potential column references

    Args:
        query: Natural language query from user

    Returns:
        Dictionary containing extracted entities

    Examples:
        >>> parse_query("top 5 produtos mais vendidos em 2015")
        {
            'top_n': 5,
            'years': [2015],
            'operators': ['top', 'mais'],
            'aggregation': 'sum',
            ...
        }
    """
    result = {
        "original_query": query,
        "normalized_query": normalize_text(query),
        "top_n": None,
        "years": [],
        "months": [],
        "numbers": [],
        "operators": [],
        "aggregation": None,
        "quoted_terms": [],
        "potential_columns": [],
        "metric_hints": [],
        "has_ranking": False,
        "has_comparison": False,
        "has_temporal": False,
    }

    # FASE 3.1: Detectar ranking operations UPSTREAM
    # Usar módulo ranking_detector para detecção preventiva
    ranking_info = extract_ranking_info(query)
    if ranking_info:
        result["top_n"] = ranking_info["top_n"]
        result["ranking_sort_order"] = ranking_info["sort_order"]
        result["ranking_type"] = ranking_info["ranking_type"]
        result["has_ranking"] = True

        logger.info(
            f"[parse_query] Ranking operation detected UPSTREAM: "
            f"top_n={ranking_info['top_n']}, order={ranking_info['sort_order']}, "
            f"type={ranking_info['ranking_type']}"
        )

    # Extract numbers
    numbers = extract_numbers(query)
    result["numbers"] = numbers

    # Detect top N (fallback para backward compatibility se ranking_info não detectou)
    if not result.get("top_n"):
        result["top_n"] = detect_top_n(query, numbers)

    # Extract years
    result["years"] = extract_years(query)

    # Extract months
    result["months"] = extract_months(query)

    # Extract quoted terms
    result["quoted_terms"] = extract_quoted_terms(query)

    # Detect operators
    operators = detect_operators(query)
    result["operators"] = operators

    # Classify operator types (normalize operators lists for comparison)
    normalized_ranking_ops = [normalize_text(op) for op in RANKING_OPERATORS]
    normalized_comparison_ops = [normalize_text(op) for op in COMPARISON_OPERATORS]
    normalized_temporal_ops = [normalize_text(op) for op in TEMPORAL_OPERATORS]

    result["has_ranking"] = any(op in normalized_ranking_ops for op in operators)
    result["has_comparison"] = any(op in normalized_comparison_ops for op in operators)
    result["has_temporal"] = any(op in normalized_temporal_ops for op in operators)

    # Detect aggregation function
    result["aggregation"] = detect_aggregation(query)

    # Extract potential column references (words that might be column names)
    result["potential_columns"] = extract_potential_columns(query)

    # Extract metric hints (explicit metric references)
    result["metric_hints"] = extract_metric_hints(query)

    # CENTRALIZAÇÃO: Filtros são responsabilidade exclusiva do filter_classifier.
    # Nenhum filtro deve ser gerado pelo graphic_classifier.
    result["filters"] = {}

    logger.debug(f"Parsed query: {result}")

    return result


def detect_top_n(query: str, numbers: List[int]) -> Optional[int]:
    """
    Detect "top N" pattern in query.

    Args:
        query: User query
        numbers: List of numbers found in query

    Returns:
        N value if "top N" pattern found, None otherwise

    Examples:
        >>> detect_top_n("top 5 produtos", [5, 2015])
        5
        >>> detect_top_n("10 maiores clientes", [10])
        10
    """
    normalized = normalize_text(query)

    # Pattern: "top N", "primeiros N", "N maiores", etc.
    patterns = [
        r"top\s+(\d+)",
        r"primeiro[s]?\s+(\d+)",
        r"(\d+)\s+maiore[s]?",
        r"(\d+)\s+menore[s]?",
        r"(\d+)\s+melhore[s]?",
        r"(\d+)\s+piore[s]?",
    ]

    for pattern in patterns:
        match = re.search(pattern, normalized)
        if match:
            return int(match.group(1))

    # If we have "top" or similar without explicit number, use first number found
    if re.search(r"\b(top|ranking|maiores|menores)\b", normalized) and numbers:
        return numbers[0]

    return None


def extract_years(query: str) -> List[int]:
    """
    Extract year values from query.

    Args:
        query: User query

    Returns:
        List of years found

    Examples:
        >>> extract_years("vendas em 2023 e 2015")
        [2023, 2015]
    """
    years = re.findall(YEAR_PATTERN, query)
    return [int(year) for year in years]


def extract_months(query: str) -> List[int]:
    """
    Extract month references from query.

    Args:
        query: User query

    Returns:
        List of month numbers (1-12)

    Examples:
        >>> extract_months("vendas em janeiro e fevereiro")
        [1, 2]
    """
    normalized = normalize_text(query)
    months = []

    for month_name, month_num in MONTH_NAMES_PT.items():
        if month_name in normalized:
            months.append(month_num)

    return sorted(list(set(months)))


def detect_operators(query: str) -> List[str]:
    """
    Detect operator keywords in query.

    Args:
        query: User query

    Returns:
        List of detected operators

    Examples:
        >>> detect_operators("comparar vendas entre estados")
        ['comparar', 'entre']
    """
    normalized = normalize_text(query)
    operators = []

    # Combine all operator lists
    all_operators = RANKING_OPERATORS + COMPARISON_OPERATORS + TEMPORAL_OPERATORS

    for operator in all_operators:
        # Normalize operator for matching
        normalized_op = normalize_text(operator)
        # Use word boundary matching for better accuracy
        pattern = r"\b" + re.escape(normalized_op) + r"\b"
        if re.search(pattern, normalized):
            # Add normalized version to match what tests expect
            operators.append(normalized_op)

    return operators


def detect_aggregation(query: str) -> Optional[str]:
    """
    Detect aggregation function from query.

    Args:
        query: User query

    Returns:
        Aggregation function name (sum, avg, count, etc.) or None

    Examples:
        >>> detect_aggregation("média de vendas por estado")
        'avg'
        >>> detect_aggregation("contar número de clientes")
        'count'
        >>> detect_aggregation("número de vendas")
        'sum'  # Because "vendas" is monetary context
    """
    normalized = normalize_text(query)

    # ====================================================================
    # PRIORITY 1: Check for monetary context with "número/quantidade"
    # ====================================================================
    # "número de vendas" should mean SUM (sum of sales values), not COUNT (count rows)
    monetary_keywords = [
        "vendas",
        "venda",
        "faturamento",
        "receita",
        "valor",
        "vendido",
    ]
    has_monetary_context = any(word in normalized for word in monetary_keywords)
    has_quantity_word = any(word in normalized for word in ["numero", "quantidade"])

    if has_monetary_context and has_quantity_word:
        # Verify this is not an explicit COUNT request like "contar número de clientes"
        explicit_count_patterns = [
            "contar",
            "quantos",
            "quantas",
            "numero de clientes",
            "quantidade de clientes",
        ]
        has_explicit_count = any(
            pattern in normalized for pattern in explicit_count_patterns
        )

        if not has_explicit_count:
            return "sum"

    # ====================================================================
    # PRIORITY 2: Check for explicit aggregation keywords
    # ====================================================================
    for keyword, agg_func in AGGREGATION_OPERATORS.items():
        normalized_keyword = normalize_text(keyword)
        # Use word boundary matching
        pattern = r"\b" + re.escape(normalized_keyword) + r"\b"
        if re.search(pattern, normalized):
            # Skip "número" and "quantidade" if already handled above
            if keyword in ["numero", "número", "quantidade"] and has_monetary_context:
                continue
            return agg_func

    # ====================================================================
    # PRIORITY 3: Context-based inference
    # ====================================================================
    inference_words = {
        "count": [
            "distribuicao",
            "quantos",
            "quantas",
        ],  # Pure counting queries
        "sum": [
            "evolucao",
            "crescimento",
            "total",
            "totais",
            "faturamento",
            "venda",
            "vendas",
            "receita",
            "valor",
        ],  # Monetary/quantitative aggregations
    }

    for agg_func, words in inference_words.items():
        for word in words:
            pattern = r"\b" + re.escape(word) + r"\b"
            if re.search(pattern, normalized):
                return agg_func

    return None


def extract_potential_columns(query: str) -> List[str]:
    """
    Extract words that might be column references.

    This identifies nouns and key terms that could map to columns.

    Args:
        query: User query

    Returns:
        List of potential column reference terms

    Examples:
        >>> extract_potential_columns("vendas de produtos por estado")
        ['vendas', 'produtos', 'estado']
    """
    normalized = normalize_text(query)

    # Remove common stopwords and operators
    stopwords = [
        "o",
        "a",
        "os",
        "as",
        "de",
        "da",
        "do",
        "das",
        "dos",
        "em",
        "no",
        "na",
        "nos",
        "nas",
        "por",
        "para",
        "com",
        "e",
        "ou",
        "que",
        "se",
        "me",
        "te",
        "qual",
        "quais",
        "entre",
        "mais",
        "menos",
        "como",
        "sobre",
    ]

    words = normalized.split()
    potential_columns = []

    for word in words:
        # Skip if stopword, operator, or too short
        if word in stopwords or word in RANKING_OPERATORS or len(word) < 3:
            continue

        # Skip pure numbers
        if word.isdigit():
            continue

        potential_columns.append(word)

    return potential_columns


def extract_metric_hints(query: str) -> List[str]:
    """
    Extract explicit metric-related terms from query.

    This function identifies words that strongly suggest a metric is being referenced,
    helping to fill empty metrics fields.

    Args:
        query: User query

    Returns:
        List of terms that likely refer to metrics

    Examples:
        >>> extract_metric_hints("total de vendas por produto")
        ['vendas', 'total']
        >>> extract_metric_hints("quantidade vendida em junho")
        ['quantidade']
    """
    normalized = normalize_text(query)

    # Metric-related keywords
    metric_indicators = [
        "vendas",
        "venda",
        "faturamento",
        "receita",
        "valor",
        "valores",
        "quantidade",
        "qtd",
        "volume",
        "peso",
        "total",
        "totais",
        "lucro",
        "custo",
        "preco",
        "ticket",
        "montante",
        "soma",
    ]

    # Phrases that indicate metrics
    metric_phrases = [
        r"total\s+de\s+(\w+)",  # "total de vendas"
        r"valor\s+de\s+(\w+)",  # "valor de vendas"
        r"quantidade\s+de\s+(\w+)",  # "quantidade de produtos"
        r"volume\s+de\s+(\w+)",  # "volume de vendas"
        r"soma\s+de\s+(\w+)",  # "soma de valores"
    ]

    hints = []

    # Extract from simple keywords
    words = normalized.split()
    for word in words:
        if word in metric_indicators:
            hints.append(word)

    # Extract from phrases
    for pattern in metric_phrases:
        matches = re.findall(pattern, normalized)
        for match in matches:
            if match not in hints:
                hints.append(match)
                # Also add the full phrase context
                if match not in metric_indicators:
                    hints.append(match)

    return hints


def extract_filters(query: str) -> Dict[str, Any]:
    """
    DEPRECATED — Filtros são responsabilidade exclusiva do filter_classifier.

    Esta função é mantida apenas por backward-compatibility (assinatura pública).
    Retorna sempre um dicionário vazio. Toda lógica de detecção de filtros
    (UF, IDs de entidades, datas) é centralizada no agente filter_classifier,
    que usa LLM + alias.yaml + validação semântica.

    Args:
        query: User query (ignorado)

    Returns:
        Dicionário vazio — filtros devem vir exclusivamente de filter_final.
    """
    return {}


def detect_date_range(query: str) -> Optional[Dict[str, Any]]:
    """
    Detect date range in query.

    Args:
        query: User query

    Returns:
        Dictionary with start and end dates if detected

    Examples:
        >>> detect_date_range("vendas entre janeiro e março de 2015")
        {'start_month': 1, 'end_month': 3, 'year': 2015}
    """
    normalized = normalize_text(query)

    # Pattern: "entre X e Y"
    between_pattern = r"entre\s+(\w+)\s+e\s+(\w+)"
    match = re.search(between_pattern, normalized)

    if not match:
        return None

    start_term, end_term = match.groups()

    # Check if they are months
    start_month = MONTH_NAMES_PT.get(start_term)
    end_month = MONTH_NAMES_PT.get(end_term)

    if start_month and end_month:
        result = {"start_month": start_month, "end_month": end_month}

        # Try to find associated year
        years = extract_years(query)
        if years:
            result["year"] = years[0]

        return result

    return None


def extract_comparison_entities(query: str) -> Optional[Dict[str, List[str]]]:
    """
    Extract entities being compared.

    Args:
        query: User query

    Returns:
        Dictionary with entities being compared

    Examples:
        >>> extract_comparison_entities("comparar vendas entre SP e RJ")
        {'entities': ['SP', 'RJ'], 'type': 'state'}
    """
    normalized = normalize_text(query)

    # Pattern: "entre X e Y"
    between_pattern = r"entre\s+([A-Za-z0-9]+)\s+e\s+([A-Za-z0-9]+)"
    match = re.search(
        between_pattern, query
    )  # Use original query for case-sensitive matching

    if match:
        entity1, entity2 = match.groups()

        # Check if they are state codes
        if entity1.upper() in [
            "AC",
            "AL",
            "AP",
            "AM",
            "BA",
            "CE",
            "DF",
            "ES",
            "GO",
            "MA",
            "MT",
            "MS",
            "MG",
            "PA",
            "PB",
            "PR",
            "PE",
            "PI",
            "RJ",
            "RN",
            "RS",
            "RO",
            "RR",
            "SC",
            "SP",
            "SE",
            "TO",
        ]:
            return {"entities": [entity1.upper(), entity2.upper()], "type": "state"}

        return {"entities": [entity1, entity2], "type": "unknown"}

    return None


def is_chart_query(query: str) -> bool:
    """
    Determine if query requires a chart/visualization.

    Args:
        query: User query

    Returns:
        True if query requires visualization, False otherwise

    Examples:
        >>> is_chart_query("top 5 produtos")
        True
        >>> is_chart_query("qual o nome do cliente 12345")
        False
    """
    normalized = normalize_text(query)

    # Queries that typically DON'T need charts
    no_chart_patterns = [
        r"\bqual\s+(o|a)\s+nome\b",
        r"\bqual\s+(o|a)\s+codigo\b",
        r"\bqual\s+(o|a)\s+valor\s+de\b",
        r"\bquando\b",
        r"\bonde\b",
        r"\bquem\s+e\b",
    ]

    for pattern in no_chart_patterns:
        if re.search(pattern, normalized):
            return False

    # Queries that typically DO need charts
    chart_patterns = [
        r"\b(top|ranking|maiores|menores)\b",
        r"\b(comparar|comparacao|versus|vs)\b",
        r"\b(historico|tendencia|evolucao)\b",
        r"\b(distribuicao|proporcao|percentual)\b",
        r"\b(crescimento|variacao|mudanca)\b",
    ]

    for pattern in chart_patterns:
        if re.search(pattern, normalized):
            return True

    # Default to True (assume chart is needed)
    return True
