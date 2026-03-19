"""
Insight Alignment Corrector - FASE 5: Output Alignment Validation

This module applies automatic corrections to alignment issues detected by
the InsightAlignmentValidator.

Correction strategies:
1. Add missing metrics to detailed_insights with placeholder formulas
2. Ensure key_findings has minimum 3 items
3. Fill executive_summary if missing
4. Normalize numeric formats for consistency

Reference: insight_generator_planning.md - Section 7.4
"""

import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class AlignmentCorrector:
    """
    Applies automatic corrections to alignment issues.

    INVARIANT: Corrections MUST NOT modify the LLM-generated narrative text.
    Only structural fields (detailed_insights, key_findings) can be modified.
    """

    def __init__(self):
        """Initialize corrector with correction statistics."""
        self.corrections_applied = []

    def correct(
        self,
        narrative: str,
        detailed_insights: List[Dict[str, Any]],
        key_findings: List[str],
        executive_summary: Optional[Dict[str, str]],
        validation_result: Dict[str, Any],
        composed_metrics: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Apply automatic corrections to alignment issues.

        Args:
            narrative: The narrative text (READ-ONLY)
            detailed_insights: List of detailed insight dicts (CAN MODIFY)
            key_findings: List of key finding strings (CAN MODIFY)
            executive_summary: Optional executive summary dict (CAN MODIFY)
            validation_result: Result from InsightAlignmentValidator
            composed_metrics: Optional metrics from MetricComposer

        Returns:
            Dict with corrected fields:
                - detailed_insights: List[Dict]
                - key_findings: List[str]
                - executive_summary: Optional[Dict]
                - corrections_applied: List[str] - log of corrections
        """
        logger.info("[AlignmentCorrector] Starting alignment correction")

        self.corrections_applied = []

        # Make copies to avoid modifying originals
        corrected_detailed = list(detailed_insights)
        corrected_findings = list(key_findings)
        corrected_summary = dict(executive_summary) if executive_summary else None

        # Correction 1: Add missing metrics to detailed_insights
        if validation_result.get("missing_in_detailed"):
            corrected_detailed = self._add_missing_metrics(
                narrative,
                corrected_detailed,
                validation_result["missing_in_detailed"],
                composed_metrics,
            )

        # Correction 2: Ensure minimum key_findings
        if len(corrected_findings) < 3:
            corrected_findings = self._ensure_minimum_findings(
                corrected_findings, corrected_detailed, narrative
            )

        # Correction 3: Fill executive_summary if missing
        if not corrected_summary or not corrected_summary.get("title"):
            corrected_summary = self._fill_executive_summary(
                corrected_summary, narrative
            )

        # Correction 4: Normalize detailed_insights format
        corrected_detailed = self._normalize_detailed_format(corrected_detailed)

        logger.info(
            f"[AlignmentCorrector] Correction complete: "
            f"{len(self.corrections_applied)} correction(s) applied"
        )

        return {
            "detailed_insights": corrected_detailed,
            "key_findings": corrected_findings,
            "executive_summary": corrected_summary,
            "corrections_applied": self.corrections_applied,
        }

    def _add_missing_metrics(
        self,
        narrative: str,
        detailed_insights: List[Dict[str, Any]],
        missing_metrics: List[Dict[str, Any]],
        composed_metrics: Optional[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Add placeholder entries for metrics mentioned in narrative but missing in detailed.

        Strategy:
        - Extract context from narrative where metric is mentioned
        - Create placeholder insight with best-effort formula
        - Mark as "auto-generated" in metadata
        """
        for missing in missing_metrics:
            metric_type = missing.get("type", "metric")
            values = missing.get("values", [])
            context = missing.get("context", "")

            # Create placeholder insight
            placeholder = {
                "title": f"{metric_type.title()} (auto-generated)",
                "metric_name": f"{metric_type.title()} (auto-generated)",
                "formula": self._infer_formula(metric_type, values, context),
                "value": values[0] if values else "N/A",
                "interpretation": f"Referência extraída da narrativa: {context[:80]}...",
                "_auto_generated": True,
            }

            detailed_insights.append(placeholder)

            self.corrections_applied.append(
                f"Added placeholder for missing metric: {metric_type}"
            )

            logger.warning(
                f"[AlignmentCorrector] Added placeholder for missing metric: {metric_type}"
            )

        return detailed_insights

    def _infer_formula(self, metric_type: str, values: List[str], context: str) -> str:
        """
        Infer a best-effort formula based on metric type and context.
        """
        if not values:
            return f"{metric_type.title()} = [valor não especificado]"

        # Common formula patterns by metric type
        if metric_type in [
            "concentração",
            "concentracao",
            "share",
            "participação",
            "participacao",
        ]:
            if len(values) >= 2:
                return f"Concentração = {values[0]} / {values[1]} = {values[0]}"
            return f"Concentração = {values[0]}"

        elif metric_type in ["gap"]:
            if len(values) >= 2:
                return f"Gap = {values[0]} - {values[1]} = [calculado]"
            return f"Gap = {values[0]}"

        elif metric_type in ["variação", "variacao", "crescimento", "queda", "delta"]:
            if len(values) >= 2:
                return f"Variação = ({values[0]} - {values[1]}) / {values[1]} = [calculado]"
            return f"Variação = {values[0]}"

        elif metric_type in ["média", "media", "average"]:
            return f"Média = {values[0]}"

        elif metric_type in ["total", "sum"]:
            return f"Total = {values[0]}"

        elif metric_type in ["top"]:
            if len(values) >= 2:
                return f"Top N = {values[0]} / Total {values[1]} = [percentual]"
            return f"Top N = {values[0]}"

        else:
            # Generic formula
            return f"{metric_type.title()} = {' | '.join(values)}"

    def _ensure_minimum_findings(
        self,
        key_findings: List[str],
        detailed_insights: List[Dict[str, Any]],
        narrative: str,
    ) -> List[str]:
        """
        Ensure key_findings has at least 3 items.

        Strategy:
        1. If < 3, extract from detailed_insights interpretations
        2. If still < 3, extract key sentences from narrative
        """
        if len(key_findings) >= 3:
            return key_findings

        # Strategy 1: Extract from detailed_insights
        for insight in detailed_insights:
            if len(key_findings) >= 3:
                break

            interpretation = insight.get("interpretation", "")
            if interpretation and len(interpretation) <= 140:
                if interpretation not in key_findings:
                    key_findings.append(interpretation)
                    self.corrections_applied.append(
                        "Added key_finding from detailed_insight interpretation"
                    )

        # Strategy 2: Extract key sentences from narrative
        if len(key_findings) < 3:
            sentences = [s.strip() for s in narrative.split(".") if s.strip()]

            for sentence in sentences:
                if len(key_findings) >= 3:
                    break

                # Only add sentences with numeric data (more likely to be key findings)
                if any(char.isdigit() for char in sentence) and len(sentence) <= 140:
                    if sentence not in key_findings:
                        key_findings.append(sentence)
                        self.corrections_applied.append(
                            "Added key_finding from narrative sentence"
                        )

        # Strategy 3: Generic fallback if still < 3
        while len(key_findings) < 3:
            key_findings.append("Análise adicional recomendada para insight completo")
            self.corrections_applied.append("Added generic key_finding placeholder")

        logger.info(
            f"[AlignmentCorrector] Ensured minimum key_findings: {len(key_findings)}"
        )

        return key_findings

    def _fill_executive_summary(
        self, executive_summary: Optional[Dict[str, str]], narrative: str
    ) -> Dict[str, str]:
        """
        Fill executive_summary if missing or incomplete.

        Strategy:
        - Title: First sentence from narrative (max 100 chars)
        - Introduction: First paragraph from narrative (max 300 chars)
        """
        if not executive_summary:
            executive_summary = {}

        if not executive_summary.get("title"):
            # Extract title from first sentence
            sentences = narrative.split(".")
            title = sentences[0].strip() if sentences else "Análise de Dados"

            # Truncate to 100 chars
            if len(title) > 100:
                title = title[:97] + "..."

            executive_summary["title"] = title
            self.corrections_applied.append("Generated executive_summary title")

        if not executive_summary.get("introduction"):
            # Extract introduction from first paragraph (or first 300 chars)
            intro = narrative[:300].strip()

            # Try to end at a sentence boundary
            last_period = intro.rfind(".")
            if last_period > 100:  # Only if we have a reasonable sentence
                intro = intro[: last_period + 1]
            elif len(narrative) > 300:
                intro += "..."

            executive_summary["introduction"] = intro
            self.corrections_applied.append("Generated executive_summary introduction")

        return executive_summary

    def _normalize_detailed_format(
        self, detailed_insights: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Normalize detailed_insights format to ensure consistency.

        Ensures all insights have required fields for the unified schema:
        - title
        - formula
        - interpretation

        Keeps auxiliary fields (metric_name, value) when available for backward compatibility.
        """
        normalized = []

        for insight in detailed_insights:
            normalized_insight = {}

            # Canonical unified field
            title = (
                insight.get("title")
                or insight.get("metric_name")
                or insight.get("metric")
                or "Métrica sem nome"
            )
            normalized_insight["title"] = title
            # Keep metric_name for compatibility with validator/older code paths
            normalized_insight["metric_name"] = title

            # Ensure formula exists
            normalized_insight["formula"] = insight.get("formula", "N/A")

            # Preserve value if present (optional in unified schema)
            if "value" in insight:
                normalized_insight["value"] = insight.get("value", "N/A")

            # Ensure interpretation exists
            normalized_insight["interpretation"] = insight.get(
                "interpretation", insight.get("content", "")
            )

            # Preserve metadata if present
            if "_auto_generated" in insight:
                normalized_insight["_auto_generated"] = True

            normalized.append(normalized_insight)

        return normalized


def apply_corrections(
    narrative: str,
    detailed_insights: List[Dict[str, Any]],
    key_findings: List[str],
    executive_summary: Optional[Dict[str, str]],
    validation_result: Dict[str, Any],
    composed_metrics: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Convenience function for applying corrections.

    Args:
        narrative: The narrative text (READ-ONLY)
        detailed_insights: List of detailed insight dicts
        key_findings: List of key finding strings
        executive_summary: Optional executive summary dict
        validation_result: Result from InsightAlignmentValidator
        composed_metrics: Optional metrics from MetricComposer

    Returns:
        Dict with corrected fields and corrections log
    """
    corrector = AlignmentCorrector()
    return corrector.correct(
        narrative,
        detailed_insights,
        key_findings,
        executive_summary,
        validation_result,
        composed_metrics,
    )
