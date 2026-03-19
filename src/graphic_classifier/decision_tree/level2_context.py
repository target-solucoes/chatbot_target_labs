"""
Level 2: Context Analysis - Context-Based Disambiguation.

This module implements the second level of the decision tree, performing
context-based disambiguation for cases that don't have clear Level 1 patterns
but can still be classified with reasonable confidence (0.75-0.90).

Disambiguation strategies:
1. Ranking Disambiguation (top/ranking keywords + context)
2. Comparison Disambiguation (comparar/versus + dimension type)
3. Multi-Value Dimension Analysis (filter vs dimension intent)

Reference: graph_classifier_diagnosis.md - Section 5.2 (NIVEL 2: CONTEXT ANALYSIS)
"""

import re
import logging
from typing import Optional, Dict, List

from src.graphic_classifier.utils.text_cleaner import normalize_text
from src.graphic_classifier.tools.context_analyzer import (
    detect_filter_vs_dimension_intent,
)


logger = logging.getLogger(__name__)


class Level2Analyzer:
    """
    Level 2 analyzer for context-based disambiguation.

    This class uses semantic context to resolve ambiguities between
    similar chart types, achieving 0.75-0.90 confidence.
    """

    # Confidence threshold for Level 2 bypass
    CONFIDENCE_THRESHOLD = 0.75

    def __init__(self):
        """Initialize Level 2 analyzer with disambiguation configurations."""
        # Use stemmed versions of keywords to match normalized queries
        self.ranking_keywords = [
            "top",
            "maiore",  # stemmed from "maiores"
            "menore",  # stemmed from "menores"
            "ranking",
            "melhore",  # stemmed from "melhores"
            "piore",  # stemmed from "piores"
        ]

        self.comparison_keywords = [
            "comparar",
            "comparacao",
            "versus",
            "vs",
            "entre",
            "diferenca",
            "contrastar",
        ]

        self.temporal_comparison_keywords = [
            "crescimento",
            "variacao",
            "aumento",
            "reducao",
            "mudanca",
            "queda",  # ADDED: negative variation
            "diminuicao",  # ADDED: negative variation
            "declinio",  # ADDED: negative variation
            "subida",  # ADDED: positive variation
        ]

    def analyze(
        self,
        query: str,
        context: Dict,
        keyword_scores: Dict,
        parsed_entities: Optional[Dict] = None,
    ) -> Optional[Dict]:
        """
        Execute Level 2 context analysis on query.

        Args:
            query: User query
            context: Context extracted by context_analyzer
            keyword_scores: Scores from keyword detector
            parsed_entities: Parsed entities from parse_query (optional)

        Returns:
            {
                "chart_type": str,
                "confidence": float,
                "reasoning": str,
                "level": 2
            } if confident disambiguation achieved, None otherwise

        Examples:
            >>> analyzer = Level2Analyzer()
            >>> analyzer.analyze("top 5 produtos", context, scores)
            {
                "chart_type": "bar_horizontal",
                "confidence": 0.90,
                "reasoning": "Ranking without temporal comparison...",
                "level": 2
            }
        """
        normalized = normalize_text(query)

        # [2.1] Ranking Disambiguation
        result = self._disambiguate_ranking(
            query, normalized, context, keyword_scores, parsed_entities
        )
        if result and result["confidence"] >= self.CONFIDENCE_THRESHOLD:
            logger.info(
                f"[Level2] Ranking disambiguated: {result['chart_type']} "
                f"(confidence={result['confidence']:.2f})"
            )
            return result

        # [2.2] Comparison Disambiguation
        result = self._disambiguate_comparison(
            query, normalized, context, keyword_scores, parsed_entities
        )
        if result and result["confidence"] >= self.CONFIDENCE_THRESHOLD:
            logger.info(
                f"[Level2] Comparison disambiguated: {result['chart_type']} "
                f"(confidence={result['confidence']:.2f})"
            )
            return result

        # [2.3] Multi-Value Dimension Analysis
        result = self._analyze_multi_value_dimension(
            query, normalized, context, keyword_scores, parsed_entities
        )
        if result and result["confidence"] >= self.CONFIDENCE_THRESHOLD:
            logger.info(
                f"[Level2] Multi-value dimension analyzed: {result['chart_type']} "
                f"(confidence={result['confidence']:.2f})"
            )
            return result

        # No confident disambiguation achieved
        logger.debug("[Level2] No confident disambiguation achieved")
        return None

    def _disambiguate_ranking(
        self,
        query: str,
        normalized: str,
        context: Dict,
        keyword_scores: Dict,
        parsed_entities: Optional[Dict],
    ) -> Optional[Dict]:
        """
        [2.1] Disambiguate ranking queries.

        Logic:
        - Ranking + temporal comparison → line_composed (0.85) [SLOPE CHART]
        - Ranking + comparison keywords → bar_vertical (0.80)
        - Ranking only → bar_horizontal (0.90)

        Examples:
            - "top 5 produtos com crescimento" → line_composed (temporal variation)
            - "top 5 produtos queda entre maio e junho" → line_composed (slope chart)
            - "top 5 comparando SP e RJ" → bar_vertical
            - "top 5 produtos" → bar_horizontal
        """
        # Check for ranking keywords
        has_ranking = any(kw in normalized for kw in self.ranking_keywords)

        if not has_ranking:
            return None

        # Context check 0: Check for comparison with multiple locations first
        # Pattern: "top 5 produtos comparando SP e RJ"
        multi_location_pattern = r"([A-Z]{2}(?:\s*(?:,|e)\s*[A-Z]{2})+)"
        has_multi_location = bool(re.search(multi_location_pattern, query))

        if has_multi_location and context.get("has_comparison_keywords", False):
            return {
                "chart_type": "bar_vertical",
                "confidence": 0.85,
                "reasoning": (
                    "[Level 2.1] Ranking with multiple location comparison: "
                    "ranking + comparison keywords with multiple geographic values. "
                    "This indicates comparison of rankings across locations, "
                    "requiring bar_vertical for side-by-side comparison."
                ),
                "level": 2,
            }

        # Context check 1: Has temporal comparison?
        # CRITICAL FIX: bar_vertical_composed is DEPRECATED
        # Temporal comparisons (including variation/queda/aumento) MUST use line_composed
        # This creates a slope chart showing the trend line for each entity
        if context.get("has_temporal_comparison", False) or context.get(
            "between_periods_pattern", False
        ):
            return {
                "chart_type": "line_composed",
                "confidence": 0.85,
                "reasoning": (
                    "[Level 2.1] Ranking with temporal comparison context: "
                    "ranking keywords detected with temporal comparison indicators. "
                    "Despite ranking intent, temporal comparison takes priority, "
                    "requiring line_composed for temporal variation visualization (slope chart)."
                ),
                "level": 2,
            }

        # Context check 2: Has comparison keywords?
        # Only check if NOT temporal (temporal was already handled above)
        if context.get("has_comparison_keywords", False) and not context.get(
            "has_temporal_comparison", False
        ):
            # Additional check: verify it's not just filter comparison
            filter_intent = detect_filter_vs_dimension_intent(query, parsed_entities)

            # Check for explicit multi-location pattern in the query
            multi_location_pattern = r"([A-Z]{2}(?:\s*(?:,|e)\s*[A-Z]{2})+)"
            has_multi_location = bool(re.search(multi_location_pattern, query))

            if filter_intent.get("is_dimension", False) or has_multi_location:
                return {
                    "chart_type": "bar_vertical",
                    "confidence": 0.80,
                    "reasoning": (
                        "[Level 2.1] Ranking with comparison keywords: "
                        "ranking + comparison intent with multiple dimension values. "
                        "This indicates side-by-side comparison of categories, "
                        "requiring bar_vertical."
                    ),
                    "level": 2,
                }

        # Context check 3: No temporal/comparison (simple ranking)
        # Only return bar_horizontal if there's truly NO comparison or temporal context
        if not context.get("has_comparison_keywords", False) and not context.get(
            "has_temporal_comparison", False
        ):
            return {
                "chart_type": "bar_horizontal",
                "confidence": 0.90,
                "reasoning": (
                    "[Level 2.1] Simple ranking without temporal or comparison context: "
                    "ranking keywords detected without temporal comparison or "
                    "multi-value comparison. This is a straightforward top-N ranking, "
                    "best visualized with horizontal bars."
                ),
                "level": 2,
            }

        # Ambiguous case - return lower confidence
        return {
            "chart_type": "bar_horizontal",
            "confidence": 0.70,
            "reasoning": (
                "[Level 2.1] Ranking with ambiguous context: "
                "defaulting to bar_horizontal but with lower confidence due to "
                "unclear context signals."
            ),
            "level": 2,
        }

    def _disambiguate_comparison(
        self,
        query: str,
        normalized: str,
        context: Dict,
        keyword_scores: Dict,
        parsed_entities: Optional[Dict],
    ) -> Optional[Dict]:
        """
        [2.2] Disambiguate comparison queries.

        Logic:
        - Comparison + temporal dimension + continuous → line_composed (0.85)
        - Comparison + temporal dimension + discrete → bar_vertical_composed (0.85)
        - Comparison + multiple explicit categories → bar_vertical (0.85)
        - Comparison + single category → bar_vertical (0.80)

        Examples:
            - "comparar vendas ao longo do tempo" → line_composed
            - "comparar vendas entre janeiro e fevereiro" → bar_vertical_composed
            - "comparar SP, RJ e MG" → bar_vertical
        """
        # Check for comparison keywords
        has_comparison = any(kw in normalized for kw in self.comparison_keywords)

        if not has_comparison:
            return None

        # Context check 1: Temporal dimension with continuous trend?
        temporal_trend_keywords = [
            "evolucao",
            "historico",
            "ao longo",
            "tendencia",
        ]
        has_temporal_trend = any(kw in normalized for kw in temporal_trend_keywords)

        # Check if there's explicit "ao longo do tempo" pattern
        has_ao_longo_tempo = bool(re.search(r"ao\s+longo\s+do?\s+tempo", normalized))

        if has_temporal_trend and (
            context.get("has_temporal_dimension", False) or has_ao_longo_tempo
        ):
            return {
                "chart_type": "line_composed",
                "confidence": 0.85,
                "reasoning": (
                    "[Level 2.2] Comparison with continuous temporal trend: "
                    "comparison keywords + temporal trend indicators (evolucao, historico). "
                    "This indicates comparison of temporal series across multiple categories, "
                    "requiring line_composed for multi-series visualization."
                ),
                "level": 2,
            }

        # Context check 2: Temporal dimension with discrete periods?
        # CRITICAL FIX: bar_vertical_composed is DEPRECATED
        # Even discrete temporal periods should use line_composed for slope visualization
        # Check for "entre" pattern specifically for temporal comparison
        has_entre_pattern = "entre" in normalized and context.get(
            "two_temporal_values", False
        )

        if (
            context.get("has_temporal_comparison", False)
            and context.get("two_temporal_values", False)
        ) or has_entre_pattern:
            return {
                "chart_type": "line_composed",
                "confidence": 0.85,
                "reasoning": (
                    "[Level 2.2] Comparison across discrete time periods: "
                    "comparison keywords + discrete temporal periods (e.g., entre maio e junho). "
                    "This requires line_composed for temporal variation visualization (slope chart)."
                ),
                "level": 2,
            }

        # Context check 3: Multiple explicit categories?
        # Pattern: "SP, RJ e MG" or similar
        multiple_category_pattern = r"[A-Z]{2}(?:,\s*[A-Z]{2})+(?:\s+e\s+[A-Z]{2})?"
        has_multiple_categories = bool(re.search(multiple_category_pattern, query))

        if has_multiple_categories:
            categories = re.findall(r"[A-Z]{2}", query)
            return {
                "chart_type": "bar_vertical",
                "confidence": 0.85,
                "reasoning": (
                    f"[Level 2.2] Comparison with multiple explicit categories: "
                    f"comparison keywords + {len(categories)} explicit categories {categories}. "
                    f"This indicates direct comparison between categories, "
                    f"requiring bar_vertical for side-by-side comparison."
                ),
                "level": 2,
            }

        # Context check 4: Generic comparison (default to bar_vertical)
        return {
            "chart_type": "bar_vertical",
            "confidence": 0.75,
            "reasoning": (
                "[Level 2.2] Generic comparison query: "
                "comparison keywords detected without specific temporal or "
                "categorical context. Defaulting to bar_vertical for direct comparison."
            ),
            "level": 2,
        }

    def _analyze_multi_value_dimension(
        self,
        query: str,
        normalized: str,
        context: Dict,
        keyword_scores: Dict,
        parsed_entities: Optional[Dict],
    ) -> Optional[Dict]:
        """
        [2.3] Analyze multi-value dimension intent.

        This resolves the critical ambiguity:
        - "vendas de SP" → SP is FILTER (bar_horizontal)
        - "vendas de SP e RJ" → SP, RJ are DIMENSION values (bar_vertical)

        Logic:
        - 2+ values + comparison intent → dimension (bar_vertical)
        - 2+ temporal values → dimension (bar_vertical_composed)
        - 1 value → filter (bar_horizontal)

        Examples:
            - "vendas em SP e RJ" + comparison → bar_vertical
            - "vendas em SP" → bar_horizontal (com filtro)
            - "vendas em maio e junho" → bar_vertical_composed
        """
        # Check for multi-value temporal pattern directly in query
        # Pattern: "maio e junho", "janeiro, fevereiro e marco"
        month_pattern = r"(janeiro|fevereiro|marco|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)"
        months_in_query = re.findall(month_pattern, normalized, re.IGNORECASE)

        if len(months_in_query) >= 2:
            # CRITICAL FIX: bar_vertical_composed is DEPRECATED
            # Multiple months = temporal comparison -> use line_composed
            return {
                "chart_type": "line_composed",
                "confidence": 0.85,
                "reasoning": (
                    "[Level 2.3] Multi-value temporal dimension detected: "
                    f"{len(months_in_query)} temporal values ({', '.join(months_in_query[:3])}) "
                    f"indicate discrete period comparison. "
                    f"This requires line_composed for temporal variation visualization (slope chart)."
                ),
                "level": 2,
            }

        # Use context_analyzer helper for geographic patterns
        filter_intent = detect_filter_vs_dimension_intent(query, parsed_entities)

        # If dimension intent detected
        if filter_intent.get("is_dimension", False):
            # Check if temporal dimension
            if context.get("multi_value_temporal", False):
                # CRITICAL FIX: bar_vertical_composed is DEPRECATED
                return {
                    "chart_type": "line_composed",
                    "confidence": 0.85,
                    "reasoning": (
                        "[Level 2.3] Multi-value temporal dimension detected: "
                        f"multiple temporal values indicate period comparison. "
                        f"Reason: {filter_intent.get('reason')}. "
                        f"This requires line_composed for temporal variation visualization (slope chart)."
                    ),
                    "level": 2,
                }
            else:
                return {
                    "chart_type": "bar_vertical",
                    "confidence": 0.80,
                    "reasoning": (
                        "[Level 2.3] Multi-value dimension detected: "
                        f"multiple dimension values indicate direct comparison. "
                        f"Reason: {filter_intent.get('reason')}. "
                        f"This requires bar_vertical for side-by-side comparison."
                    ),
                    "level": 2,
                }

        # If filter intent detected
        if filter_intent.get("is_filter", False):
            # Check if there's ranking context
            has_ranking = any(kw in normalized for kw in self.ranking_keywords)

            if has_ranking:
                return {
                    "chart_type": "bar_horizontal",
                    "confidence": 0.85,
                    "reasoning": (
                        "[Level 2.3] Single-value filter with ranking: "
                        f"ranking keywords + single filter value. "
                        f"Reason: {filter_intent.get('reason')}. "
                        f"This indicates simple ranking with geographic/categorical filter."
                    ),
                    "level": 2,
                }

        # No clear intent
        return None
