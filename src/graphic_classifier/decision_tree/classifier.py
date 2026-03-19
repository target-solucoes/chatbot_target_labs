"""
Decision Tree Classifier - Main Orchestrator.

This module implements the main DecisionTreeClassifier that orchestrates
the three-level classification strategy:

Level 1 (Detection): Fast pattern matching (0.90-0.95 confidence)
Level 2 (Context Analysis): Context-based disambiguation (0.75-0.90 confidence)
Level 3 (Fallback): LLM-based classification for ambiguous cases

The classifier reduces LLM dependency from 95% to 10-30% of queries,
improving response time and classification accuracy.

Reference: graph_classifier_diagnosis.md - FASE 3
"""

import logging
from typing import Optional, Dict

from src.graphic_classifier.decision_tree.level1_detection import Level1Detector
from src.graphic_classifier.decision_tree.level2_context import Level2Analyzer
from src.graphic_classifier.tools.context_analyzer import extract_query_context


logger = logging.getLogger(__name__)


class DecisionTreeClassifier:
    """
    Three-level decision tree classifier for chart type detection.

    This classifier implements a hierarchical strategy that attempts to
    classify queries deterministically before falling back to LLM:

    1. Level 1: High-confidence pattern detection (bypass LLM if confidence >= 0.90)
    2. Level 2: Context-based disambiguation (bypass LLM if confidence >= 0.75)
    3. Level 3: Return None to trigger LLM fallback

    Expected metrics:
    - 70-90% of queries resolved in Level 1 or 2
    - 10-30% require LLM fallback
    - Overall accuracy >= 95%
    - Average response time: ~0.5-0.8s (vs 2-3s with LLM)

    Examples:
        >>> classifier = DecisionTreeClassifier()
        >>> result = classifier.classify(
        ...     query="top 5 produtos com crescimento entre maio e junho",
        ...     context=context,
        ...     keyword_scores=scores
        ... )
        >>> result
        {
            "chart_type": "bar_vertical_composed",
            "confidence": 0.95,
            "reasoning": "...",
            "level_used": 1
        }
    """

    def __init__(self, level1_threshold: float = 0.90, level2_threshold: float = 0.75):
        """
        Initialize decision tree classifier.

        Args:
            level1_threshold: Confidence threshold for Level 1 bypass (default: 0.90)
            level2_threshold: Confidence threshold for Level 2 bypass (default: 0.75)
        """
        self.level1_detector = Level1Detector()
        self.level2_analyzer = Level2Analyzer()

        self.level1_threshold = level1_threshold
        self.level2_threshold = level2_threshold

        # Metrics tracking
        self.metrics = {
            "level1_hits": 0,
            "level2_hits": 0,
            "llm_fallbacks": 0,
            "total_queries": 0,
        }

        logger.info(
            f"[DecisionTreeClassifier] Initialized with thresholds: "
            f"L1={level1_threshold}, L2={level2_threshold}"
        )

    def classify(
        self,
        query: str,
        context: Optional[Dict] = None,
        keyword_scores: Optional[Dict] = None,
        parsed_entities: Optional[Dict] = None,
    ) -> Dict:
        """
        Classify query using three-level decision tree.

        Args:
            query: User query to classify
            context: Context extracted by context_analyzer (optional, will extract if None)
            keyword_scores: Keyword scores from keyword detector (optional)
            parsed_entities: Parsed entities from parse_query (optional)

        Returns:
            {
                "chart_type": str | None,  # None triggers LLM fallback
                "confidence": float,
                "reasoning": str,
                "level_used": int,  # 1, 2, or 3 (LLM)
                "bypassed_llm": bool  # True if Level 1 or 2 succeeded
            }

        Examples:
            >>> classifier = DecisionTreeClassifier()

            # Level 1 success (high confidence pattern)
            >>> classifier.classify("crescimento entre maio e junho")
            {
                "chart_type": "bar_vertical_composed",
                "confidence": 0.95,
                "level_used": 1,
                "bypassed_llm": True
            }

            # Level 2 success (context-based)
            >>> classifier.classify("top 5 produtos")
            {
                "chart_type": "bar_horizontal",
                "confidence": 0.90,
                "level_used": 2,
                "bypassed_llm": True
            }

            # Level 3 fallback (requires LLM)
            >>> classifier.classify("vendas por produto")
            {
                "chart_type": None,
                "confidence": 0.0,
                "level_used": 3,
                "bypassed_llm": False
            }
        """
        self.metrics["total_queries"] += 1

        # Extract context if not provided
        if context is None:
            context = extract_query_context(query, parsed_entities)
            logger.debug(f"[DecisionTree] Context extracted: {context}")

        # Initialize keyword_scores if not provided
        if keyword_scores is None:
            keyword_scores = {}

        logger.info(f"[DecisionTree] Starting classification for query: '{query}'")

        # ============================================================
        # LEVEL 1: DETECTION (Confidence: 0.90-0.95)
        # ============================================================
        try:
            result = self.level1_detector.detect(query, context, parsed_entities)

            if result and result.get("confidence", 0) >= self.level1_threshold:
                self.metrics["level1_hits"] += 1
                result["bypassed_llm"] = True
                result["level_used"] = 1

                logger.info(
                    f"[DecisionTree] ✓ Level 1 SUCCESS: {result['chart_type']} "
                    f"(confidence={result['confidence']:.2f})"
                )

                self._log_metrics()
                return result

            logger.debug(
                "[DecisionTree] Level 1 did not reach threshold, continuing..."
            )

        except Exception as e:
            logger.error(f"[DecisionTree] Level 1 error: {e}", exc_info=True)

        # ============================================================
        # LEVEL 2: CONTEXT ANALYSIS (Confidence: 0.75-0.90)
        # ============================================================
        try:
            result = self.level2_analyzer.analyze(
                query, context, keyword_scores, parsed_entities
            )

            if result and result.get("confidence", 0) >= self.level2_threshold:
                self.metrics["level2_hits"] += 1
                result["bypassed_llm"] = True
                result["level_used"] = 2

                logger.info(
                    f"[DecisionTree] ✓ Level 2 SUCCESS: {result['chart_type']} "
                    f"(confidence={result['confidence']:.2f})"
                )

                self._log_metrics()
                return result

            logger.debug(
                "[DecisionTree] Level 2 did not reach threshold, continuing..."
            )

        except Exception as e:
            logger.error(f"[DecisionTree] Level 2 error: {e}", exc_info=True)

        # ============================================================
        # LEVEL 3: FALLBACK (Requires LLM)
        # ============================================================
        self.metrics["llm_fallbacks"] += 1

        logger.info(
            "[DecisionTree] → Level 3 FALLBACK: No confident decision, "
            "triggering LLM classification"
        )

        self._log_metrics()

        return {
            "chart_type": None,  # Signal to call LLM
            "confidence": 0.0,
            "reasoning": (
                "[Level 3] No high-confidence pattern or context detected. "
                "Falling back to LLM for complex analysis."
            ),
            "level_used": 3,
            "bypassed_llm": False,
        }

    def get_metrics(self) -> Dict:
        """
        Get current classification metrics.

        Returns:
            {
                "total_queries": int,
                "level1_hits": int,
                "level2_hits": int,
                "llm_fallbacks": int,
                "level1_percentage": float,
                "level2_percentage": float,
                "llm_percentage": float,
                "deterministic_percentage": float  # Level 1 + 2
            }
        """
        total = self.metrics["total_queries"]

        if total == 0:
            return {
                **self.metrics,
                "level1_percentage": 0.0,
                "level2_percentage": 0.0,
                "llm_percentage": 0.0,
                "deterministic_percentage": 0.0,
            }

        return {
            **self.metrics,
            "level1_percentage": (self.metrics["level1_hits"] / total) * 100,
            "level2_percentage": (self.metrics["level2_hits"] / total) * 100,
            "llm_percentage": (self.metrics["llm_fallbacks"] / total) * 100,
            "deterministic_percentage": (
                (self.metrics["level1_hits"] + self.metrics["level2_hits"]) / total
            )
            * 100,
        }

    def reset_metrics(self):
        """Reset metrics counters."""
        self.metrics = {
            "level1_hits": 0,
            "level2_hits": 0,
            "llm_fallbacks": 0,
            "total_queries": 0,
        }
        logger.info("[DecisionTree] Metrics reset")

    def _log_metrics(self):
        """Log current metrics."""
        metrics = self.get_metrics()

        logger.info(
            f"[DecisionTree Metrics] "
            f"Total={metrics['total_queries']}, "
            f"L1={metrics['level1_percentage']:.1f}%, "
            f"L2={metrics['level2_percentage']:.1f}%, "
            f"LLM={metrics['llm_percentage']:.1f}%, "
            f"Deterministic={metrics['deterministic_percentage']:.1f}%"
        )

    def set_thresholds(
        self, level1: Optional[float] = None, level2: Optional[float] = None
    ):
        """
        Update confidence thresholds dynamically.

        This allows tuning based on production metrics.

        Args:
            level1: New Level 1 threshold (0.0-1.0)
            level2: New Level 2 threshold (0.0-1.0)

        Examples:
            >>> classifier.set_thresholds(level1=0.85, level2=0.70)
            # More aggressive bypassing (higher LLM avoidance)

            >>> classifier.set_thresholds(level1=0.95, level2=0.85)
            # More conservative (higher accuracy, more LLM calls)
        """
        if level1 is not None:
            old_l1 = self.level1_threshold
            self.level1_threshold = level1
            logger.info(
                f"[DecisionTree] Level 1 threshold updated: {old_l1} → {level1}"
            )

        if level2 is not None:
            old_l2 = self.level2_threshold
            self.level2_threshold = level2
            logger.info(
                f"[DecisionTree] Level 2 threshold updated: {old_l2} → {level2}"
            )


class DecisionTreeResult:
    """
    Convenience class for decision tree results.

    This provides a more structured interface for handling results.
    """

    def __init__(self, result_dict: Dict):
        self.chart_type = result_dict.get("chart_type")
        self.confidence = result_dict.get("confidence", 0.0)
        self.reasoning = result_dict.get("reasoning", "")
        self.level_used = result_dict.get("level_used", 3)
        self.bypassed_llm = result_dict.get("bypassed_llm", False)

    def should_use_llm(self) -> bool:
        """Check if LLM fallback is needed."""
        return self.chart_type is None

    def is_high_confidence(self) -> bool:
        """Check if result has high confidence (>= 0.85)."""
        return self.confidence >= 0.85

    def is_deterministic(self) -> bool:
        """Check if result was determined by Level 1 or 2."""
        return self.level_used in [1, 2] and self.bypassed_llm

    def __repr__(self):
        return (
            f"DecisionTreeResult("
            f"chart_type={self.chart_type}, "
            f"confidence={self.confidence:.2f}, "
            f"level={self.level_used}, "
            f"bypassed_llm={self.bypassed_llm})"
        )
