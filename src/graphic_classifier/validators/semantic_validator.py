"""
Semantic Validator - Validates semantic anchor against query keywords.

This module implements the SECOND LAYER of the semantic-first architecture.
It validates that the semantic anchor extracted by the LLM is consistent
with explicit keywords in the query.

CRITICAL RULES:
- Validates semantic anchor BEFORE heuristics run
- Checks for consistency between LLM output and query keywords
- Returns validation status (pass/fail) with warnings
- Does NOT modify the anchor (only validates)

References:
- graph_classifier_diagnosis.md (Section on invariants)
- graph_classifier_correction.md (FASE 1 specifications)
"""

import logging
import re
from typing import List, Tuple
from dataclasses import dataclass

from src.graphic_classifier.llm.semantic_anchor import SemanticAnchor

logger = logging.getLogger(__name__)


# ============================================================================
# VALIDATION RESULT
# ============================================================================


@dataclass
class SemanticValidationResult:
    """
    Result of semantic anchor validation.

    Fields:
        is_valid: Whether the anchor passes validation
        warnings: List of validation warnings (if any)
        failed_checks: List of failed validation checks
        passed_checks: List of passed validation checks
    """

    is_valid: bool
    warnings: List[str]
    failed_checks: List[str]
    passed_checks: List[str]


# ============================================================================
# KEYWORD SETS (CANONICAL)
# ============================================================================


class SemanticKeywords:
    """
    Canonical keyword sets for semantic validation.

    These keyword sets are used to validate that the LLM's semantic
    classification is consistent with explicit keywords in the query.
    """

    # Variation keywords (compare_variation)
    VARIATION_KEYWORDS = {
        "queda",
        "aumento",
        "crescimento",
        "reducao",
        "redução",
        "declinio",
        "declínio",
        "variacao",
        "variação",
        "mudanca",
        "mudança",
        "evolucao",
        "evolução",
        "diferenca",
        "diferença",
        "comparacao",
        "comparação",
        "versus",
        "vs",
        "contra",
    }

    # Negative polarity keywords
    NEGATIVE_KEYWORDS = {
        "queda",
        "reducao",
        "redução",
        "declinio",
        "declínio",
        "diminuicao",
        "diminuição",
        "caiu",
        "caíu",
        "menor",
        "pior",
        "piores",
        "bottom",
        "ultimo",
        "últimos",
        "ultimas",
        "últimas",
    }

    # Positive polarity keywords
    POSITIVE_KEYWORDS = {
        "aumento",
        "crescimento",
        "expansao",
        "expansão",
        "cresceu",
        "maior",
        "melhor",
        "melhores",
        "top",
        "primeiro",
        "primeiros",
        "primeira",
        "primeiras",
        "lider",
        "líder",
        "destaque",
    }

    # Temporal comparison keywords
    # NOTE: Excludes common Portuguese prepositions like "de", "em", "para"
    # which cause false positives in non-temporal queries (e.g., "top 10 clientes em valor de vendas")
    TEMPORAL_COMPARISON_KEYWORDS = {
        "entre",
        "ate",
        "até",
        "desde",
        "comparar",
        "variacao",
        "variação",
        "vs",
        "versus",
        "contra",
    }

    # Temporal keywords (general)
    TEMPORAL_KEYWORDS = {
        "historico",
        "histórico",
        "evolucao",
        "evolução",
        "tendencia",
        "tendência",
        "serie",
        "série",
        "temporal",
        "ao longo",
    }

    # Ranking keywords
    RANKING_KEYWORDS = {
        "top",
        "ranking",
        "maior",
        "menor",
        "melhor",
        "pior",
        "primeiro",
        "ultimo",
        "últimos",
        "lider",
        "líder",
        "classificacao",
        "classificação",
    }

    # Distribution keywords
    DISTRIBUTION_KEYWORDS = {
        "participacao",
        "participação",
        "proporcao",
        "proporção",
        "distribuicao",
        "distribuição",
        "percentual",
        "percent",
        "fatia",
        "composicao",
        "composição",
    }


# ============================================================================
# SEMANTIC VALIDATOR
# ============================================================================


class SemanticValidator:
    """
    Validates semantic anchors against query keywords.

    This validator ensures that the semantic anchor extracted by the LLM
    is consistent with explicit keywords found in the user's query.

    Validation Checks:
    1. Variation Check: If query contains variation keywords, semantic_goal should be compare_variation
    2. Polarity Check: If query contains negative keywords, polarity should be negative
    3. Temporal Comparison Check: If query contains temporal comparison keywords, comparison_axis should be temporal
    4. Ranking Check: If query contains ranking keywords, semantic_goal should be ranking

    Usage:
        validator = SemanticValidator()
        result = validator.validate(anchor, query)
        if not result.is_valid:
            print(f"Validation warnings: {result.warnings}")
    """

    def __init__(self):
        """Initialize the validator."""
        logger.info("[SemanticValidator] Initialized")

    def validate(self, anchor: SemanticAnchor, query: str) -> SemanticValidationResult:
        """
        Validate semantic anchor against query keywords.

        Args:
            anchor: Semantic anchor to validate
            query: Original user query

        Returns:
            SemanticValidationResult: Validation result with warnings and failed checks

        Example:
            >>> validator = SemanticValidator()
            >>> anchor = SemanticAnchor(
            ...     semantic_goal="compare_variation",
            ...     comparison_axis="temporal",
            ...     polarity="negative",
            ...     requires_time_series=True,
            ...     entity_scope="vendas",
            ...     confidence=0.95,
            ...     reasoning="Temporal comparison with negative polarity"
            ... )
            >>> result = validator.validate(anchor, "queda nas vendas entre maio e junho")
            >>> assert result.is_valid
        """
        logger.info(f"[SemanticValidator] Validating anchor for query: '{query}'")

        query_lower = query.lower()
        warnings = []
        failed_checks = []
        passed_checks = []

        # ====================================================================
        # CHECK 1: Variation Keywords
        # ====================================================================
        variation_found = self._find_keywords(
            query_lower, SemanticKeywords.VARIATION_KEYWORDS
        )
        if variation_found:
            if anchor.semantic_goal == "compare_variation":
                passed_checks.append(
                    f"Variation keywords detected ({variation_found}) and "
                    f"semantic_goal is 'compare_variation' ✓"
                )
            else:
                failed_checks.append(
                    f"Query contains variation keywords ({variation_found}) but "
                    f"semantic_goal is '{anchor.semantic_goal}' (expected 'compare_variation')"
                )
                warnings.append(
                    f"VARIATION MISMATCH: Keywords {variation_found} suggest compare_variation, "
                    f"but LLM classified as {anchor.semantic_goal}"
                )

        # ====================================================================
        # CHECK 2: Negative Polarity Keywords
        # ====================================================================
        negative_found = self._find_keywords(
            query_lower, SemanticKeywords.NEGATIVE_KEYWORDS
        )
        if negative_found:
            if anchor.polarity == "negative":
                passed_checks.append(
                    f"Negative keywords detected ({negative_found}) and "
                    f"polarity is 'negative' ✓"
                )
            else:
                failed_checks.append(
                    f"Query contains negative keywords ({negative_found}) but "
                    f"polarity is '{anchor.polarity}' (expected 'negative')"
                )
                warnings.append(
                    f"POLARITY MISMATCH: Keywords {negative_found} suggest negative polarity, "
                    f"but LLM classified as {anchor.polarity}"
                )

        # ====================================================================
        # CHECK 3: Positive Polarity Keywords
        # ====================================================================
        positive_found = self._find_keywords(
            query_lower, SemanticKeywords.POSITIVE_KEYWORDS
        )
        if positive_found:
            if anchor.polarity == "positive":
                passed_checks.append(
                    f"Positive keywords detected ({positive_found}) and "
                    f"polarity is 'positive' ✓"
                )
            else:
                # Only warn if there are no negative keywords (which would override)
                if not negative_found:
                    failed_checks.append(
                        f"Query contains positive keywords ({positive_found}) but "
                        f"polarity is '{anchor.polarity}' (expected 'positive')"
                    )
                    warnings.append(
                        f"POLARITY MISMATCH: Keywords {positive_found} suggest positive polarity, "
                        f"but LLM classified as {anchor.polarity}"
                    )

        # ====================================================================
        # CHECK 4: Temporal Comparison Keywords
        # ====================================================================
        temporal_comparison_found = self._find_keywords(
            query_lower, SemanticKeywords.TEMPORAL_COMPARISON_KEYWORDS
        )

        # Also check for temporal patterns like "entre X e Y", "de X para Y"
        temporal_patterns = [
            r"entre\s+\w+\s+e\s+\w+",
            r"de\s+\w+\s+para\s+\w+",
            r"de\s+\w+\s+ate\s+\w+",
            r"de\s+\w+\s+até\s+\w+",
        ]
        temporal_pattern_match = any(
            re.search(pattern, query_lower) for pattern in temporal_patterns
        )

        if temporal_comparison_found or temporal_pattern_match:
            if anchor.comparison_axis == "temporal":
                passed_checks.append(
                    f"Temporal comparison detected and comparison_axis is 'temporal' ✓"
                )
            else:
                failed_checks.append(
                    f"Query contains temporal comparison indicators but "
                    f"comparison_axis is '{anchor.comparison_axis}' (expected 'temporal')"
                )
                warnings.append(
                    f"TEMPORAL AXIS MISMATCH: Query suggests temporal comparison, "
                    f"but LLM classified axis as {anchor.comparison_axis}"
                )

        # ====================================================================
        # CHECK 5: Ranking Keywords
        # ====================================================================
        ranking_found = self._find_keywords(
            query_lower, SemanticKeywords.RANKING_KEYWORDS
        )

        # Also check for "top N" or "N melhores" patterns
        ranking_patterns = [
            r"top\s+\d+",
            r"\d+\s+(maior|menor|melhor|pior)",
            r"(primeiro|ultimo|últimos?)\s+\d+",
        ]
        ranking_pattern_match = any(
            re.search(pattern, query_lower) for pattern in ranking_patterns
        )

        if ranking_found or ranking_pattern_match:
            # Ranking can be semantic_goal or just have positive/negative polarity
            if anchor.semantic_goal == "ranking":
                passed_checks.append(
                    f"Ranking keywords detected and semantic_goal is 'ranking' ✓"
                )
            else:
                # This is not necessarily a failure - some ranking queries might be trends
                # Just log it as a note
                logger.debug(
                    f"[SemanticValidator] Ranking keywords found but goal is {anchor.semantic_goal}"
                )

        # ====================================================================
        # FINAL VALIDATION
        # ====================================================================

        is_valid = len(failed_checks) == 0

        if is_valid:
            logger.info(
                f"[SemanticValidator] Validation PASSED - "
                f"{len(passed_checks)} checks passed, 0 failed"
            )
        else:
            logger.warning(
                f"[SemanticValidator] Validation FAILED - "
                f"{len(failed_checks)} checks failed, {len(passed_checks)} passed"
            )
            for warning in warnings:
                logger.warning(f"[SemanticValidator] {warning}")

        return SemanticValidationResult(
            is_valid=is_valid,
            warnings=warnings,
            failed_checks=failed_checks,
            passed_checks=passed_checks,
        )

    def _find_keywords(self, text: str, keywords: set) -> List[str]:
        """
        Find keywords from a set in the text.

        Args:
            text: Text to search (should be lowercase)
            keywords: Set of keywords to search for

        Returns:
            List of found keywords
        """
        found = []
        for keyword in keywords:
            # Use word boundary to avoid partial matches
            pattern = r"\b" + re.escape(keyword) + r"\b"
            if re.search(pattern, text):
                found.append(keyword)
        return found
