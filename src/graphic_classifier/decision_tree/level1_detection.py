"""
Level 1: Detection - High-Confidence Pattern Matching.

This module implements the first level of the decision tree, detecting
inequivocal patterns that can be classified with high confidence (0.90-0.95)
without LLM invocation.

Patterns detected:
1. Temporal Comparison Detection (line_composed)
2. Composition Pattern Detection (bar_vertical_stacked)
3. Percentage/Proportion Detection (pie)
4. Simple Temporal Trend (line)

Reference: graph_classifier_diagnosis.md - Section 5.2 (NIVEL 1: DETECTION)
"""

import re
import logging
from typing import Optional, Dict, List

from src.graphic_classifier.utils.text_cleaner import normalize_text
from src.graphic_classifier.utils.ranking_detector import extract_nested_ranking


logger = logging.getLogger(__name__)


class Level1Detector:
    """
    Level 1 detector for high-confidence pattern matching.

    This class implements fast, deterministic pattern detection that
    can bypass LLM calls for clear-cut cases.
    """

    # Confidence threshold for Level 1 bypass
    CONFIDENCE_THRESHOLD = 0.90

    def __init__(self):
        """Initialize Level 1 detector with pattern configurations."""
        self.temporal_comparison_keywords = [
            "crescimento",
            "variacao",
            "aumento",
            "reducao",
            "mudanca",
            "comparacao",
            "comparar",
            "queda",  # ADDED: negative variation (ex: "queda nas vendas")
            "diminuicao",  # ADDED: negative variation
            "declinio",  # ADDED: negative variation
            "subida",  # ADDED: positive variation
        ]

        self.temporal_operators = [
            "entre",
            "de",
            "para",
            "comparar",
        ]

        self.percentage_keywords = [
            "percentual",
            "proporcao",
            "porcentagem",
            "%",
            "participacao",
            "fatia",
            "distribuicao percentual",
        ]

        self.temporal_trend_keywords = [
            "evolucao",
            "historico",
            "tendencia",
            "ao longo",
            "timeline",
            "progressao",
        ]

        # Meses e anos para detecção de períodos temporais
        self.temporal_terms = [
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
            # Abreviações de meses
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
            # Anos (expandível)
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

    def detect(
        self, query: str, context: Dict, parsed_entities: Optional[Dict] = None
    ) -> Optional[Dict]:
        """
        Execute Level 1 detection on query.

        Args:
            query: User query
            context: Context extracted by context_analyzer
            parsed_entities: Parsed entities from parse_query (optional)

        Returns:
            {
                "chart_type": str,
                "confidence": float,
                "reasoning": str,
                "level": 1
            } if high-confidence pattern detected, None otherwise

        Examples:
            >>> detector = Level1Detector()
            >>> detector.detect("crescimento entre maio e junho")
            {
                "chart_type": "line_composed",
                "confidence": 0.85,
                "reasoning": "Temporal comparison pattern detected...",
                "level": 1
            }
        """
        normalized = normalize_text(query)

        # [1.1] Temporal Comparison Detection
        result = self._detect_temporal_comparison(query, normalized, context)
        if result and result["confidence"] >= self.CONFIDENCE_THRESHOLD:
            logger.info(
                f"[Level1] Temporal comparison detected: {result['chart_type']} "
                f"(confidence={result['confidence']:.2f})"
            )
            return result

        # [1.2] Composition Pattern Detection
        result = self._detect_composition_pattern(query, normalized, context)
        if result and result["confidence"] >= self.CONFIDENCE_THRESHOLD:
            logger.info(
                f"[Level1] Composition pattern detected: {result['chart_type']} "
                f"(confidence={result['confidence']:.2f})"
            )
            return result

        # [1.3] Percentage/Proportion Detection
        result = self._detect_percentage_pattern(query, normalized, context)
        if result and result["confidence"] >= self.CONFIDENCE_THRESHOLD:
            logger.info(
                f"[Level1] Percentage pattern detected: {result['chart_type']} "
                f"(confidence={result['confidence']:.2f})"
            )
            return result

        # [1.4] Simple Temporal Trend
        result = self._detect_simple_temporal_trend(query, normalized, context)
        if result and result["confidence"] >= self.CONFIDENCE_THRESHOLD:
            logger.info(
                f"[Level1] Temporal trend detected: {result['chart_type']} "
                f"(confidence={result['confidence']:.2f})"
            )
            return result

        # No high-confidence pattern detected
        logger.debug("[Level1] No high-confidence pattern detected")
        return None

    def _detect_temporal_comparison(
        self, query: str, normalized: str, context: Dict
    ) -> Optional[Dict]:
        """
        [1.1] Detect temporal comparison pattern.

        Pattern: [temporal_operator] + [periodo1] + [periodo2]

        Examples:
            - "crescimento entre maio e junho"
            - "variacao de 2015 para 2016"
            - "aumento entre janeiro e fevereiro"

        Returns line_composed with confidence 0.85
        """
        # Check for temporal comparison keywords
        has_temporal_operator = any(
            kw in normalized for kw in self.temporal_comparison_keywords
        )

        if not has_temporal_operator:
            return None

        # Check for period comparison pattern
        # Pattern 1: "entre [periodo1] e [periodo2]"
        # Suporta "fev 2015" (normalize_text remove "/"), "mes de maio", etc.
        # Captura: palavra + opcionalmente numeros (ano) + opcionalmente "de ano"
        between_pattern = r"entre\s+([\w]+(?:\s+\d{4})?(?:\s+de\s+\d{4})?)\s+e\s+([\w]+(?:\s+\d{4})?(?:\s+de\s+\d{4})?)"
        match = re.search(between_pattern, normalized)

        if match:
            period1, period2 = match.groups()

            # Normalizar períodos para comparação
            # Verificar se QUALQUER PARTE do período contém um termo temporal
            period1_lower = period1.lower()
            period2_lower = period2.lower()

            # Verificar se contém termo temporal
            is_temporal = any(
                term in period1_lower or term in period2_lower
                for term in self.temporal_terms
            )

            if is_temporal:
                return {
                    "chart_type": "line_composed",
                    "confidence": 0.85,
                    "reasoning": (
                        f"[Level 1.1] Temporal comparison pattern detected: "
                        f"temporal operator + 'entre {period1} e {period2}'. "
                        f"This indicates comparison of values across different time periods, "
                        f"mapped to line_composed (semantic type for temporal variation)."
                    ),
                    "level": 1,
                }

        # Pattern 2: "de [periodo1] para [periodo2]"
        # Suporta "de fev 2015 para mar 2015" (normalize_text remove "/")
        # Captura: palavra + opcionalmente numeros (ano) + opcionalmente "de ano"
        from_to_pattern = r"de\s+([\w]+(?:\s+\d{4})?(?:\s+de\s+\d{4})?)\s+(?:para|a)\s+([\w]+(?:\s+\d{4})?(?:\s+de\s+\d{4})?)"
        match = re.search(from_to_pattern, normalized)

        if match:
            period1, period2 = match.groups()

            # Verificar se contém termo temporal
            period1_lower = period1.lower()
            period2_lower = period2.lower()

            is_temporal = any(
                term in period1_lower or term in period2_lower
                for term in self.temporal_terms
            )

            if is_temporal:
                return {
                    "chart_type": "line_composed",
                    "confidence": 0.85,
                    "reasoning": (
                        f"[Level 1.1] Temporal comparison pattern detected: "
                        f"temporal operator + 'de {period1} para {period2}'. "
                        f"This indicates temporal progression comparison, "
                        f"mapped to line_composed (semantic type for temporal variation)."
                    ),
                    "level": 1,
                }

        # Pattern 3: Context-based detection (already extracted by context_analyzer)
        if context.get("between_periods_pattern") and context.get(
            "has_temporal_comparison"
        ):
            return {
                "chart_type": "line_composed",
                "confidence": 0.85,
                "reasoning": (
                    "[Level 1.1] Temporal comparison detected via context analysis: "
                    "temporal comparison keywords + period comparison pattern. "
                    "Mapped to line_composed (semantic type for temporal variation)."
                ),
                "level": 1,
            }

        return None

    def _detect_composition_pattern(
        self, query: str, normalized: str, context: Dict
    ) -> Optional[Dict]:
        """
        [1.2] Detect composition/nested ranking pattern.

        Pattern: "top N [dim1] nos/em [top M] [dim2]"

        Examples:
            - "top 3 produtos nos 5 maiores clientes"
            - "5 melhores vendedores nos 3 maiores estados"

        Returns bar_vertical_stacked with confidence 0.90
        """
        # Use ranking_detector to extract nested pattern
        nested_info = extract_nested_ranking(query)

        if nested_info.get("is_nested", False):
            outer_n = nested_info.get("outer_n")
            inner_n = nested_info.get("inner_n")
            outer_entity = nested_info.get("outer_entity")
            inner_entity = nested_info.get("inner_entity")

            return {
                "chart_type": "bar_vertical_stacked",
                "confidence": 0.90,
                "reasoning": (
                    f"[Level 1.2] Composition/nested ranking pattern detected: "
                    f"'top {inner_n} {inner_entity} nos {outer_n} {outer_entity}'. "
                    f"This indicates hierarchical composition requiring stacked bars "
                    f"to show breakdown of {inner_entity} within each {outer_entity}."
                ),
                "level": 1,
            }

        # Alternative pattern: "composicao de X por Y"
        composition_keywords = ["composicao", "distribuicao dentro"]
        has_composition_keyword = any(kw in normalized for kw in composition_keywords)

        if has_composition_keyword:
            # Pattern: "composicao de X por Y" or "composicao de X"
            composition_pattern = (
                r"composicao\s+de\s+(\w+)(?:\s+(?:por|em|nos)\s+(\w+))?"
            )
            match = re.search(composition_pattern, normalized)

            if match:
                dim1 = match.group(1)
                dim2 = match.group(2) if match.lastindex >= 2 else None
                return {
                    "chart_type": "bar_vertical_stacked",
                    "confidence": 0.90,
                    "reasoning": (
                        f"[Level 1.2] Composition pattern detected: "
                        f"'composicao de {dim1}' {'por ' + dim2 if dim2 else ''}. "
                        f"This requires stacked bars to show composition breakdown."
                    ),
                    "level": 1,
                }

        return None

    def _detect_percentage_pattern(
        self, query: str, normalized: str, context: Dict
    ) -> Optional[Dict]:
        """
        [1.3] Detect percentage/proportion pattern.

        Keywords: percentual, proporcao, %, participacao

        Examples:
            - "participacao de cada regiao nas vendas"
            - "qual a porcentagem de vendas por produto"
            - "distribuicao percentual"

        Returns pie with confidence 0.90
        """
        # Check for percentage keywords
        has_percentage = any(kw in normalized for kw in self.percentage_keywords)

        if has_percentage:
            # Find which keyword matched for better reasoning
            matched_keywords = [
                kw for kw in self.percentage_keywords if kw in normalized
            ]

            return {
                "chart_type": "pie",
                "confidence": 0.90,
                "reasoning": (
                    f"[Level 1.3] Percentage/proportion pattern detected: "
                    f"keywords {matched_keywords}. "
                    f"This indicates proportional distribution visualization, "
                    f"best represented by pie chart."
                ),
                "level": 1,
            }

        return None

    def _detect_simple_temporal_trend(
        self, query: str, normalized: str, context: Dict
    ) -> Optional[Dict]:
        """
        [1.4] Detect simple temporal trend pattern.

        Pattern: [temporal_trend_keyword] + [time_dimension] + NO [category]

        Examples:
            - "evolucao de vendas por mes"
            - "historico de vendas"
            - "tendencia ao longo do tempo"

        Returns line with confidence 0.90

        IMPORTANT: This should NOT match if there are multiple explicit categories
        (that would be line_composed).
        """
        # Check for temporal trend keywords
        has_trend = any(kw in normalized for kw in self.temporal_trend_keywords)

        if not has_trend:
            return None

        # Check for time dimension keywords (more flexible, using stems)
        # Note: normalize_text does stemming, so "mes" -> "me", "anos" -> "ano"
        time_dimensions = [
            "me",
            "ano",
            "data",
            "tempo",
            "periodo",
            "dia",
            "mensal",
            "anual",
        ]
        # Also check for "por [time]" pattern - fixed to handle word boundaries and stems
        time_pattern = (
            r"(?:por|ao longo do?)\s+(me\w*|ano\w*|data|tempo|periodo\w*|dia\w*)"
        )
        has_time_dimension = any(kw in normalized for kw in time_dimensions) or bool(
            re.search(time_pattern, normalized)
        )

        # Check if there are explicit multiple categories
        # Pattern: "SP, RJ e MG" or "cada [categoria]"
        multiple_category_patterns = [
            r"[A-Z]{2},\s*[A-Z]{2}",  # "SP, RJ"
            r"cada\s+\w+",  # "cada produto"
            r"para\s+(?:cada|todos)",  # "para cada"
            r"\w+,\s*\w+\s+e\s+\w+",  # "produto1, produto2 e produto3"
        ]

        has_multiple_categories = any(
            re.search(pattern, query) for pattern in multiple_category_patterns
        )

        if has_time_dimension and not has_multiple_categories:
            matched_keywords = [
                kw for kw in self.temporal_trend_keywords if kw in normalized
            ]

            return {
                "chart_type": "line",
                "confidence": 0.90,
                "reasoning": (
                    f"[Level 1.4] Simple temporal trend detected: "
                    f"keywords {matched_keywords} + time dimension. "
                    f"No multiple explicit categories detected, indicating single series. "
                    f"This requires line chart for continuous trend visualization."
                ),
                "level": 1,
            }

        return None
