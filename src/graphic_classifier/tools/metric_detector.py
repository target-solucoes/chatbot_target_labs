"""
Metric Detector - Dynamic alias.yaml-based metric detection.

This module detects metrics from user queries by resolving keywords
against the alias.yaml configuration. All metric mappings are derived
dynamically from alias.yaml -- no hardcoded column names.

Original purpose: Resolve ambiguities in keywords like "vendas" that
can mean different metrics depending on context.

Now: All metric names and keyword mappings come from alias.yaml,
making this module dataset-agnostic.
"""

import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class MetricDetectionResult:
    """Resultado da deteccao de metrica com contexto."""

    metric_name: str
    confidence: float
    reasoning: str
    context_keywords: List[str]
    ambiguity_resolved: bool = False


def _load_alias_config() -> Dict[str, Any]:
    """Load alias.yaml configuration for metric resolution."""
    try:
        from src.shared_lib.core.config import load_alias_data

        return load_alias_data()
    except Exception as e:
        logger.error(f"[MetricDetector] Failed to load alias config: {e}")
        return {}


def _build_keyword_to_metric_map(alias_data: Dict[str, Any]) -> Dict[str, str]:
    """
    Build reverse mapping: keyword (lowercase) -> real column name.

    Only includes keywords that map to numeric (metric) columns.
    """
    columns = alias_data.get("columns", {})
    column_types = alias_data.get("column_types", {})
    numeric_cols = set(column_types.get("numeric", []))

    keyword_map = {}
    for real_col, aliases in columns.items():
        if real_col in numeric_cols and isinstance(aliases, list):
            # Also map the column name itself
            keyword_map[real_col.lower()] = real_col
            for alias in aliases:
                keyword_map[alias.lower()] = real_col
    return keyword_map


def _get_numeric_columns(alias_data: Dict[str, Any]) -> List[str]:
    """Get numeric column names from alias.yaml."""
    return alias_data.get("column_types", {}).get("numeric", [])


class MetricDetector:
    """
    Detector contextual de metricas que resolve ambiguidades baseado
    no contexto semantico da query.

    All metric mappings are derived from alias.yaml.
    No hardcoded column names.
    """

    def __init__(self, alias_mapper=None):
        """
        Inicializa o detector com mapeamentos derivados de alias.yaml.

        Args:
            alias_mapper: AliasMapper opcional para resolucao de aliases
        """
        self.alias_mapper = alias_mapper

        # Load config from alias.yaml
        self._alias_data = _load_alias_config()
        self._numeric_columns = _get_numeric_columns(self._alias_data)
        self._keyword_to_metric = _build_keyword_to_metric_map(self._alias_data)

        # Load metric priority from alias.yaml (scalable per-dataset config)
        priority = self._alias_data.get("metric_priority", {})
        self._default_metric = priority.get("default_metric") or (
            self._numeric_columns[0] if self._numeric_columns else None
        )
        self._aggregation_metric = (
            priority.get("aggregation_metric") or self._default_metric
        )

        # Context keywords are generic (not dataset-specific)
        # These indicate the NATURE of the query, not specific columns
        self.quantity_keywords = [
            "quantidade",
            "qtd",
            "volume",
            "unidades",
            "itens",
            "numero de",
            "aumento",
            "crescimento",
            "reducao",
            "queda",
            "variacao",
            "diferenca",
        ]

        self.monetary_keywords = [
            "faturamento",
            "receita",
            "valor",
            "r$",
            "reais",
            "dinheiro",
            "financeiro",
            "lucro",
            "ganho",
            "rendimento",
            "cobranca",
            "custo",
            "gasto",
            "pagamento",
            "tarifa",
            "mensalidade",
            "preco",
        ]

        # Build direct_metric_mapping from alias.yaml (replaces hardcoded dict)
        self.direct_metric_mapping = self._keyword_to_metric

        logger.info(
            f"[MetricDetector] Initialized with {len(self._numeric_columns)} numeric columns, "
            f"{len(self._keyword_to_metric)} keyword mappings from alias.yaml"
        )

    def detect_metric(
        self, query: str, parsed_entities: Optional[Dict[str, Any]] = None
    ) -> MetricDetectionResult:
        """
        Detecta a metrica apropriada baseada na query e contexto.

        Uses alias.yaml keyword mappings for resolution.

        Args:
            query: Query do usuario
            parsed_entities: Entidades parseadas (opcional)

        Returns:
            MetricDetectionResult com metrica detectada e confianca
        """
        query_lower = query.lower()
        parsed_entities = parsed_entities or {}

        # Step 1: Try direct keyword match from alias.yaml
        # This is the primary resolution method
        matched_metrics = {}
        for keyword, metric in self._keyword_to_metric.items():
            if keyword in query_lower:
                if metric not in matched_metrics:
                    matched_metrics[metric] = []
                matched_metrics[metric].append(keyword)

        if len(matched_metrics) == 1:
            # Unambiguous match
            metric_name = list(matched_metrics.keys())[0]
            keywords = matched_metrics[metric_name]
            return MetricDetectionResult(
                metric_name=metric_name,
                confidence=0.90,
                reasoning=f"Direct alias.yaml match: {', '.join(keywords)} -> {metric_name}",
                context_keywords=keywords,
                ambiguity_resolved=False,
            )

        if len(matched_metrics) > 1:
            # Multiple metrics matched - use context to disambiguate
            return self._resolve_ambiguity(
                query_lower, matched_metrics, parsed_entities
            )

        # Step 2: Fallback - no direct match
        return self._fallback_metric_detection(query_lower, parsed_entities)

    def _resolve_ambiguity(
        self,
        query_lower: str,
        matched_metrics: Dict[str, List[str]],
        parsed_entities: Dict[str, Any],
    ) -> MetricDetectionResult:
        """
        Resolve ambiguity when multiple metrics match the query.

        Priority logic (scalable via alias.yaml metric_priority):
        1. If monetary context detected and aggregation_metric is a candidate -> use it
        2. Boost based on context keywords (monetary vs quantity)
        3. If still tied, prefer aggregation_metric over others
        4. Final tiebreaker: highest keyword match count
        """
        scores = {}
        for metric in matched_metrics:
            scores[metric] = len(matched_metrics[metric])

        # Boost based on context keywords
        has_monetary = any(kw in query_lower for kw in self.monetary_keywords)
        has_quantity = any(kw in query_lower for kw in self.quantity_keywords)

        if has_monetary and not has_quantity:
            # Monetary context: prioritize aggregation_metric if it's a candidate
            if self._aggregation_metric and self._aggregation_metric in scores:
                scores[self._aggregation_metric] += 3
                logger.info(
                    f"[MetricDetector] Monetary context: boosting aggregation_metric "
                    f"'{self._aggregation_metric}' (from alias.yaml metric_priority)"
                )
            else:
                # Generic boost for monetary-sounding metrics
                for metric in scores:
                    if any(
                        kw in metric.lower()
                        for kw in ["charge", "total", "valor", "preco", "custo"]
                    ):
                        scores[metric] += 2
        elif has_quantity and not has_monetary:
            # Boost quantity-sounding metrics
            for metric in scores:
                if any(
                    kw in metric.lower()
                    for kw in ["qtd", "quantidade", "count", "tenure"]
                ):
                    scores[metric] += 2
        else:
            # No clear context: slight boost for aggregation_metric (accumulated values
            # are generally more meaningful for SUM/ranking)
            if self._aggregation_metric and self._aggregation_metric in scores:
                scores[self._aggregation_metric] += 1

        best_metric = max(scores, key=scores.get)
        keywords = matched_metrics[best_metric]

        logger.info(
            f"[MetricDetector] Resolved ambiguity: {best_metric} "
            f"(from {len(matched_metrics)} candidates, scores={scores})"
        )

        return MetricDetectionResult(
            metric_name=best_metric,
            confidence=0.85,
            reasoning=(
                f"Ambiguity resolved via context + metric_priority. "
                f"Candidates: {list(matched_metrics.keys())}. "
                f"Selected: {best_metric}"
            ),
            context_keywords=keywords,
            ambiguity_resolved=True,
        )

    def _fallback_metric_detection(
        self,
        query_lower: str,
        parsed_entities: Dict[str, Any],
    ) -> MetricDetectionResult:
        """
        Fallback when no direct keyword match found.

        Uses alias.yaml metric_priority for intelligent defaults:
        - SUM/ranking queries -> aggregation_metric
        - COUNT queries -> default_metric
        - No context -> aggregation_metric (accumulated values are more meaningful)
        """
        aggregation = parsed_entities.get("aggregation")

        # Fallback 1: COUNT aggregation -> use default_metric
        if aggregation == "count" and self._default_metric:
            return MetricDetectionResult(
                metric_name=self._default_metric,
                confidence=0.60,
                reasoning=f"Fallback: COUNT aggregation, using default_metric {self._default_metric}",
                context_keywords=["count"],
                ambiguity_resolved=False,
            )

        # Fallback 2: SUM/AVG or ranking context -> prefer aggregation_metric
        if aggregation in ["sum", "avg"] and self._aggregation_metric:
            logger.info(
                f"[MetricDetector] Fallback: SUM/AVG context -> using aggregation_metric "
                f"{self._aggregation_metric} (from alias.yaml metric_priority)"
            )
            return MetricDetectionResult(
                metric_name=self._aggregation_metric,
                confidence=0.60,
                reasoning=(
                    f"Fallback: {aggregation.upper()} aggregation, "
                    f"using aggregation_metric ({self._aggregation_metric})"
                ),
                context_keywords=[aggregation],
                ambiguity_resolved=False,
            )

        # Fallback 3: No context -> use aggregation_metric (more meaningful for generic queries)
        fallback = self._aggregation_metric or self._default_metric
        if fallback:
            logger.info(
                f"[MetricDetector] Fallback: No context -> defaulting to {fallback}"
            )
            return MetricDetectionResult(
                metric_name=fallback,
                confidence=0.50,
                reasoning=f"Fallback: No clear context, using metric ({fallback})",
                context_keywords=[],
                ambiguity_resolved=False,
            )

        # No numeric columns at all
        return MetricDetectionResult(
            metric_name="",
            confidence=0.0,
            reasoning="No numeric columns found in alias.yaml",
            context_keywords=[],
            ambiguity_resolved=False,
        )

    def get_all_supported_metrics(self) -> List[str]:
        """Retorna lista de todas as metricas suportadas (from alias.yaml)."""
        return list(self._numeric_columns)

    def validate_metric_compatibility(
        self, metric_name: str, aggregation: str
    ) -> Tuple[bool, Optional[str]]:
        """
        Valida se a metrica e compativel com a agregacao.

        Generic validation - not tied to specific column names.
        """
        # All numeric metrics support all standard aggregations
        return True, None


# =============================================================================
# Funcao Helper para Integracao no Workflow
# =============================================================================


def detect_metric_from_query(
    query: str, parsed_entities: Optional[Dict[str, Any]] = None, alias_mapper=None
) -> Dict[str, Any]:
    """
    Funcao helper para detectar metrica a partir da query.

    Args:
        query: Query do usuario
        parsed_entities: Entidades parseadas (opcional)
        alias_mapper: AliasMapper (opcional)

    Returns:
        Dict com metrica detectada e metadados.
    """
    detector = MetricDetector(alias_mapper=alias_mapper)
    detection_result = detector.detect_metric(query, parsed_entities)

    return {
        "metric_name": detection_result.metric_name,
        "confidence": detection_result.confidence,
        "reasoning": detection_result.reasoning,
        "ambiguity_resolved": detection_result.ambiguity_resolved,
        "context_keywords": detection_result.context_keywords,
    }
