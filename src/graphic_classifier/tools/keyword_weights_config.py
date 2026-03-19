"""
Keyword Weights Configuration for Context-Aware Scoring.

This module defines dynamic keyword weights based on semantic context.
This is a core component of Phase 2: Context-Aware Keywords.

Each keyword has:
- base_weight: Initial weight (0.0-1.0)
- boost_conditions: Context-based boosts that increase the weight

The total weight for a keyword is calculated as:
    weight = base_weight + sum(boost for each satisfied condition)
    weight = min(weight, 1.0)  # Cap at 1.0

This enables disambiguation of ambiguous queries:
- "top 5 produtos" → bar_horizontal (high weight due to no_temporal_comparison)
- "top 5 produtos com crescimento entre maio e junho" → bar_vertical_composed
  (low weight for bar_horizontal due to temporal context)
"""

from typing import Dict

# ====================
# KEYWORD WEIGHTS CONFIGURATION
# ====================

KEYWORD_WEIGHTS: Dict[str, Dict[str, Dict]] = {
    # --------------------------------------------------
    # BAR_HORIZONTAL: Simple Rankings/Top-N
    # --------------------------------------------------
    "bar_horizontal": {
        "top": {
            "base_weight": 0.5,
            "boost_conditions": {
                "no_temporal_comparison": 0.3,  # SE nao ha comparacao temporal
                "no_comparison_keywords": 0.2,  # SE nao ha "comparar", "versus"
                "single_dimension": 0.1,  # SE apenas 1 dimension
            },
            "description": "Top-N ranking keyword",
        },
        "ranking": {
            "base_weight": 0.4,
            "boost_conditions": {
                "no_temporal_comparison": 0.3,
                "has_top_n": 0.2,  # SE ha "top N" explicito
            },
            "description": "Generic ranking keyword",
        },
        "maiores": {
            "base_weight": 0.3,
            "boost_conditions": {
                "single_dimension": 0.2,
                "no_temporal_comparison": 0.2,
            },
            "description": "Superlative for ranking",
        },
        "menores": {
            "base_weight": 0.3,
            "boost_conditions": {
                "single_dimension": 0.2,
                "no_temporal_comparison": 0.2,
            },
            "description": "Superlative for ranking (ascending)",
        },
        "melhores": {
            "base_weight": 0.3,
            "boost_conditions": {
                "no_temporal_comparison": 0.2,
                "has_top_n": 0.1,
            },
            "description": "Best/top performers",
        },
        "piores": {
            "base_weight": 0.3,
            "boost_conditions": {
                "no_temporal_comparison": 0.2,
                "has_top_n": 0.1,
            },
            "description": "Worst performers",
        },
    },
    # --------------------------------------------------
    # BAR_VERTICAL_COMPOSED: Temporal Comparison
    # --------------------------------------------------
    "bar_vertical_composed": {
        "crescimento": {
            "base_weight": 0.6,
            "boost_conditions": {
                "has_temporal_dimension": 0.3,  # SE tem Mes, Ano, etc
                "multi_value_temporal": 0.2,  # SE 2+ periodos
                "has_comparison_keywords": 0.1,
            },
            "description": "Growth/increase over time",
        },
        "variacao": {
            "base_weight": 0.6,
            "boost_conditions": {
                "has_temporal_dimension": 0.3,
                "multi_value_temporal": 0.2,
            },
            "description": "Variation over time",
        },
        "aumento": {
            "base_weight": 0.5,
            "boost_conditions": {
                "has_temporal_dimension": 0.3,
                "between_periods_pattern": 0.2,  # "entre X e Y"
            },
            "description": "Increase over time",
        },
        "reducao": {
            "base_weight": 0.5,
            "boost_conditions": {
                "has_temporal_dimension": 0.3,
                "between_periods_pattern": 0.2,
            },
            "description": "Decrease over time",
        },
        "mudanca": {
            "base_weight": 0.5,
            "boost_conditions": {
                "has_temporal_dimension": 0.2,
                "multi_value_temporal": 0.2,
            },
            "description": "Change over time",
        },
        "entre": {
            "base_weight": 0.4,
            "boost_conditions": {
                "two_temporal_values": 0.3,  # "entre maio e junho"
                "has_temporal_operator": 0.2,  # + "crescimento", "variacao"
            },
            "description": "Between periods indicator",
        },
        "antes e depois": {
            "base_weight": 0.7,
            "boost_conditions": {
                "has_temporal_dimension": 0.2,
            },
            "description": "Before and after comparison",
        },
        "comparar periodos": {
            "base_weight": 0.8,
            "boost_conditions": {
                "multi_value_temporal": 0.2,
            },
            "description": "Explicit period comparison",
        },
    },
    # --------------------------------------------------
    # BAR_VERTICAL_STACKED: Composition/Nested
    # --------------------------------------------------
    "bar_vertical_stacked": {
        "composicao": {
            "base_weight": 0.7,
            "boost_conditions": {
                "multi_dimension": 0.2,
                "nested_ranking": 0.1,
            },
            "description": "Composition breakdown",
        },
        "dentro": {
            "base_weight": 0.4,
            "boost_conditions": {
                "multi_dimension": 0.3,
                "followed_by_dimension": 0.2,  # "dentro de [dimension]"
            },
            "description": "Within/inside indicator",
        },
        "nos": {
            "base_weight": 0.3,
            "boost_conditions": {
                "nested_ranking_pattern": 0.4,  # "top N X nos top M Y"
                "multi_dimension": 0.2,
            },
            "description": "In/within (nested context)",
        },
        "divisao por": {
            "base_weight": 0.6,
            "boost_conditions": {
                "multi_dimension": 0.3,
            },
            "description": "Division by subcategory",
        },
        "distribuicao dentro de": {
            "base_weight": 0.8,
            "boost_conditions": {
                "multi_dimension": 0.2,
            },
            "description": "Distribution within categories",
        },
        "empilhado": {
            "base_weight": 0.9,
            "boost_conditions": {},
            "description": "Explicit stacked request",
        },
    },
    # --------------------------------------------------
    # BAR_VERTICAL: Direct Comparison
    # --------------------------------------------------
    "bar_vertical": {
        "comparar": {
            "base_weight": 0.6,
            "boost_conditions": {
                "no_temporal_comparison": 0.2,  # SE nao ha temporal, e categorical
                "has_comparison_keywords": 0.1,
            },
            "description": "Generic comparison",
        },
        "versus": {
            "base_weight": 0.7,
            "boost_conditions": {
                "no_temporal_comparison": 0.2,
            },
            "description": "Explicit versus comparison",
        },
        "vs": {
            "base_weight": 0.7,
            "boost_conditions": {
                "no_temporal_comparison": 0.2,
            },
            "description": "Explicit versus comparison (abbreviated)",
        },
        "diferenca": {
            "base_weight": 0.5,
            "boost_conditions": {
                "no_temporal_comparison": 0.2,
                "has_comparison_keywords": 0.1,
            },
            "description": "Difference between values",
        },
        "contrastar": {
            "base_weight": 0.5,
            "boost_conditions": {
                "no_temporal_comparison": 0.2,
            },
            "description": "Contrast between categories",
        },
    },
    # --------------------------------------------------
    # LINE: Temporal Trend (Single Series)
    # --------------------------------------------------
    "line": {
        "historico": {
            "base_weight": 0.7,
            "boost_conditions": {
                "has_temporal_dimension": 0.2,
                "single_dimension": 0.1,  # SE apenas 1 dimension (serie unica)
            },
            "description": "Historical trend",
        },
        "evolucao": {
            "base_weight": 0.7,
            "boost_conditions": {
                "has_temporal_dimension": 0.2,
                "single_dimension": 0.1,
            },
            "description": "Evolution over time",
        },
        "tendencia": {
            "base_weight": 0.7,
            "boost_conditions": {
                "has_temporal_dimension": 0.2,
            },
            "description": "Trend over time",
        },
        "ao longo do tempo": {
            "base_weight": 0.8,
            "boost_conditions": {
                "has_temporal_dimension": 0.2,
            },
            "description": "Over time indicator",
        },
        "timeline": {
            "base_weight": 0.8,
            "boost_conditions": {
                "has_temporal_dimension": 0.2,
            },
            "description": "Timeline visualization",
        },
    },
    # --------------------------------------------------
    # LINE_COMPOSED: Multiple Temporal Series
    # --------------------------------------------------
    "line_composed": {
        "evolucao de": {
            "base_weight": 0.6,
            "boost_conditions": {
                "has_temporal_dimension": 0.2,
                "multi_dimension": 0.2,  # SE multiplas categorias
            },
            "description": "Evolution of multiple categories",
        },
        "comparar evolucao": {
            "base_weight": 0.8,
            "boost_conditions": {
                "has_temporal_dimension": 0.2,
            },
            "description": "Compare evolution of categories",
        },
        "tendencia por": {
            "base_weight": 0.6,
            "boost_conditions": {
                "has_temporal_dimension": 0.2,
                "multi_dimension": 0.2,
            },
            "description": "Trend by category",
        },
        "multiplas linhas": {
            "base_weight": 0.9,
            "boost_conditions": {},
            "description": "Explicit multiple lines request",
        },
    },
    # --------------------------------------------------
    # PIE: Proportional Distribution
    # --------------------------------------------------
    "pie": {
        "percentual": {
            "base_weight": 0.8,
            "boost_conditions": {
                "no_temporal_comparison": 0.1,  # Pie geralmente nao e temporal
            },
            "description": "Percentage distribution",
        },
        "proporcao": {
            "base_weight": 0.8,
            "boost_conditions": {
                "no_temporal_comparison": 0.1,
            },
            "description": "Proportion distribution",
        },
        "participacao": {
            "base_weight": 0.7,
            "boost_conditions": {
                "no_temporal_comparison": 0.1,
            },
            "description": "Participation/share",
        },
        "%": {
            "base_weight": 0.9,
            "boost_conditions": {},
            "description": "Explicit percentage symbol",
        },
        "distribuicao": {
            "base_weight": 0.5,
            "boost_conditions": {
                "no_temporal_comparison": 0.2,
                "single_dimension": 0.1,
            },
            "description": "Distribution (may be pie or histogram)",
        },
        "fatia": {
            "base_weight": 0.7,
            "boost_conditions": {},
            "description": "Slice/share indicator",
        },
    },
    # --------------------------------------------------
    # HISTOGRAM: Value Distribution
    # --------------------------------------------------
    "histogram": {
        "frequencia": {
            "base_weight": 0.8,
            "boost_conditions": {
                "no_temporal_comparison": 0.1,
            },
            "description": "Frequency distribution",
        },
        "faixa": {
            "base_weight": 0.6,
            "boost_conditions": {
                "no_temporal_comparison": 0.2,
            },
            "description": "Range/bin indicator",
        },
        "intervalo": {
            "base_weight": 0.6,
            "boost_conditions": {
                "no_temporal_comparison": 0.2,
            },
            "description": "Interval indicator",
        },
        "distribuicao de": {
            "base_weight": 0.7,
            "boost_conditions": {
                "no_temporal_comparison": 0.2,
            },
            "description": "Distribution of values",
        },
        "histograma": {
            "base_weight": 0.9,
            "boost_conditions": {},
            "description": "Explicit histogram request",
        },
    },
}


# ====================
# HELPER FUNCTIONS
# ====================


def get_keyword_config(chart_type: str, keyword: str) -> dict:
    """
    Get keyword configuration for a specific chart type and keyword.

    Args:
        chart_type: Chart type (e.g., "bar_horizontal")
        keyword: Keyword to lookup

    Returns:
        Keyword configuration dict or None if not found

    Examples:
        >>> config = get_keyword_config("bar_horizontal", "top")
        >>> config["base_weight"]
        0.5
        >>> config["boost_conditions"]["no_temporal_comparison"]
        0.3
    """
    chart_keywords = KEYWORD_WEIGHTS.get(chart_type, {})
    return chart_keywords.get(keyword)


def get_all_keywords_for_chart_type(chart_type: str) -> list:
    """
    Get all configured keywords for a chart type.

    Args:
        chart_type: Chart type

    Returns:
        List of keyword strings

    Examples:
        >>> get_all_keywords_for_chart_type("bar_horizontal")
        ['top', 'ranking', 'maiores', 'menores', 'melhores', 'piores']
    """
    chart_keywords = KEYWORD_WEIGHTS.get(chart_type, {})
    return list(chart_keywords.keys())


def get_boost_conditions_for_keyword(chart_type: str, keyword: str) -> dict:
    """
    Get boost conditions for a specific keyword.

    Args:
        chart_type: Chart type
        keyword: Keyword

    Returns:
        Dictionary of boost conditions

    Examples:
        >>> get_boost_conditions_for_keyword("bar_horizontal", "top")
        {
            "no_temporal_comparison": 0.3,
            "no_comparison_keywords": 0.2,
            "single_dimension": 0.1
        }
    """
    config = get_keyword_config(chart_type, keyword)
    if config:
        return config.get("boost_conditions", {})
    return {}


def calculate_max_possible_weight(chart_type: str, keyword: str) -> float:
    """
    Calculate maximum possible weight for a keyword.

    This is base_weight + sum of all boost conditions.

    Args:
        chart_type: Chart type
        keyword: Keyword

    Returns:
        Maximum possible weight (capped at 1.0)

    Examples:
        >>> calculate_max_possible_weight("bar_horizontal", "top")
        1.0  # 0.5 + 0.3 + 0.2 + 0.1 = 1.1, capped at 1.0
    """
    config = get_keyword_config(chart_type, keyword)
    if not config:
        return 0.0

    base = config.get("base_weight", 0.0)
    boosts = sum(config.get("boost_conditions", {}).values())

    return min(base + boosts, 1.0)


def validate_weights_config() -> tuple[bool, list]:
    """
    Validate the weights configuration for consistency.

    Checks:
    - All base_weights are in range [0.0, 1.0]
    - All boost values are in range [0.0, 1.0]
    - All chart types are valid

    Returns:
        (is_valid, list_of_warnings)

    Examples:
        >>> is_valid, warnings = validate_weights_config()
        >>> is_valid
        True
        >>> len(warnings)
        0
    """
    warnings = []

    for chart_type, keywords in KEYWORD_WEIGHTS.items():
        for keyword, config in keywords.items():
            # Check base_weight
            base = config.get("base_weight", 0.0)
            if not 0.0 <= base <= 1.0:
                warnings.append(
                    f"{chart_type}.{keyword}: base_weight {base} out of range [0.0, 1.0]"
                )

            # Check boost conditions
            boosts = config.get("boost_conditions", {})
            for condition, boost_value in boosts.items():
                if not 0.0 <= boost_value <= 1.0:
                    warnings.append(
                        f"{chart_type}.{keyword}.{condition}: boost {boost_value} out of range [0.0, 1.0]"
                    )

            # Check if max possible weight exceeds reasonable threshold
            max_weight = calculate_max_possible_weight(chart_type, keyword)
            if max_weight > 1.0:
                warnings.append(
                    f"{chart_type}.{keyword}: max possible weight {max_weight} exceeds 1.0 "
                    f"(will be capped)"
                )

    is_valid = len(warnings) == 0
    return is_valid, warnings


# Run validation on import
_is_valid, _warnings = validate_weights_config()
if not _is_valid:
    import logging

    logger = logging.getLogger(__name__)
    logger.warning(f"Keyword weights configuration has {len(_warnings)} warnings:")
    for warning in _warnings:
        logger.warning(f"  - {warning}")
