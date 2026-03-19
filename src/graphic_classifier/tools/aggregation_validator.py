"""
Aggregation Validator - FASE 1.3 (Etapas 1.3.1 e 1.3.2)

Este modulo implementa validacao semantica de agregacoes,
corrigindo casos absurdos como MAX para vendas totais.

Problema Original:
- Query: "maior aumento de vendas"
- Agregacao atual: max (ABSURDO - maior valor unitario)
- Agregacao esperada: sum (correto - total de vendas)

Referencia: planning_graph_classifier_diagnosis.md - FASE 1, Etapa 1.3
"""

import re
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class AggregationValidationResult:
    """Resultado da validacao de agregacao."""

    aggregation: str
    is_valid: bool
    confidence: float
    reasoning: str
    original_aggregation: Optional[str] = None
    was_corrected: bool = False


class AggregationValidator:
    """
    Validador de agregacoes que corrige agregacoes absurdas
    baseado no contexto semantico da query.

    Esta classe implementa as Etapas 1.3.1 e 1.3.2 do planejamento:
    - Regras contextuais de agregacao
    - Validacao semantica de agregacao
    """

    # Cache for numeric columns loaded from alias.yaml
    _numeric_columns_cache: Optional[List[str]] = None

    def __init__(self):
        """Inicializa o validador com regras contextuais."""

        # =====================================================================
        # Etapa 1.3.1: Regras Contextuais de Agregacao
        # =====================================================================

        # Contextos que indicam SUM (somatório, acumulação)
        self.sum_contexts = [
            "total",
            "soma",
            "somar",
            "acumulado",
            "vendas por",  # "vendas por periodo" = soma
            "faturamento por",
            "quantidade por",
            "volume por",
            "aumento",  # "maior aumento" = soma ordenada DESC
            "crescimento",
            "reducao",
            "queda",
            "variacao",
            "diferenca",
        ]

        # Contextos que indicam AVG (média)
        self.avg_contexts = [
            "media",
            "medio",
            "em media",
            "ticket medio",
            "valor medio",
            "preco medio",
        ]

        # Contextos que indicam COUNT (contagem)
        self.count_contexts = [
            "quantos",
            "quantas",
            "numero de",
            "quantidade de",
            "contar",
            "contagem",
        ]

        # Contextos que indicam MAX (valor máximo UNITÁRIO)
        # IMPORTANTE: MAX só deve ser usado para valor único máximo,
        # NÃO para "maior aumento de vendas" (que é SUM + ORDER DESC)
        self.max_contexts = [
            "maior valor unitario",
            "preco maximo",
            "teto",
            "pico",
            "recorde individual",
        ]

        # Contextos que indicam MIN (valor mínimo unitário)
        self.min_contexts = [
            "menor valor unitario",
            "preco minimo",
            "piso",
            "menor individual",
        ]

        # IMPORTANTE: Keywords AMBÍGUAS que NÃO devem indicar MAX/MIN
        # "maior" em "maior aumento de vendas" significa ordenação, não MAX
        self.ambiguous_keywords = [
            "maior",
            "menor",
            "top",
            "principais",
            "melhores",
            "piores",
        ]

    @classmethod
    def _is_numeric_metric(cls, metric_name: str) -> bool:
        """
        Check if a metric is a numeric column (loaded from alias.yaml).

        Replaces hardcoded checks like `metric_name in ["Qtd_Vendida", ...]`.
        """
        if cls._numeric_columns_cache is None:
            try:
                from src.shared_lib.core.dataset_config import DatasetConfig

                cls._numeric_columns_cache = (
                    DatasetConfig.get_instance().numeric_columns
                )
            except Exception:
                try:
                    from src.shared_lib.core.config import get_metric_columns

                    cls._numeric_columns_cache = get_metric_columns()
                except Exception:
                    cls._numeric_columns_cache = []
        return metric_name in cls._numeric_columns_cache

    def validate_aggregation(
        self,
        query: str,
        metric_name: str,
        proposed_aggregation: Optional[str] = None,
        parsed_entities: Optional[Dict[str, Any]] = None,
    ) -> AggregationValidationResult:
        """
        Valida e corrige agregacao baseada no contexto.

        Esta funcao implementa a Etapa 1.3.2: Validacao semantica de agregacao.

        Args:
            query: Query do usuario
            metric_name: Nome da metrica (Qtd_Vendida, Valor_Vendido, etc.)
            proposed_aggregation: Agregacao proposta (opcional)
            parsed_entities: Entidades parseadas (opcional)

        Returns:
            AggregationValidationResult com agregacao validada/corrigida

        Examples:
            >>> validator = AggregationValidator()
            >>> result = validator.validate_aggregation(
            ...     "maior aumento de vendas",
            ...     "Qtd_Vendida",
            ...     "max"
            ... )
            >>> result.aggregation
            'sum'
            >>> result.was_corrected
            True
        """
        query_lower = query.lower()
        parsed_entities = parsed_entities or {}

        # =====================================================================
        # Regra 1: Detectar contexto da query
        # =====================================================================

        sum_score = 0.0
        avg_score = 0.0
        count_score = 0.0
        max_score = 0.0
        min_score = 0.0

        # Contar keywords de cada contexto
        for keyword in self.sum_contexts:
            if keyword in query_lower:
                sum_score += 1.0

        for keyword in self.avg_contexts:
            if keyword in query_lower:
                avg_score += 1.0

        for keyword in self.count_contexts:
            if keyword in query_lower:
                count_score += 1.0

        for keyword in self.max_contexts:
            if keyword in query_lower:
                max_score += 1.0

        for keyword in self.min_contexts:
            if keyword in query_lower:
                min_score += 1.0

        # =====================================================================
        # Regra 2: Numeric (metric) columns should prefer SUM over MAX/MIN
        # =====================================================================
        # MAX/MIN are inappropriate for cumulative metric columns
        # Metric columns are loaded dynamically from alias.yaml
        # =====================================================================

        is_numeric_metric = self._is_numeric_metric(metric_name)

        if is_numeric_metric and proposed_aggregation in ["max", "min"]:
            logger.warning(
                f"[AggregationValidator] ABSURD aggregation detected: "
                f"{proposed_aggregation} for numeric metric '{metric_name}'"
            )

            # Corrigir para SUM (agregação correta para vendas)
            return AggregationValidationResult(
                aggregation="sum",
                is_valid=True,
                confidence=0.95,
                reasoning=(
                    f"CORRECTED: {proposed_aggregation.upper()} is inappropriate for "
                    f"numeric metric '{metric_name}'. Changed to SUM."
                ),
                original_aggregation=proposed_aggregation,
                was_corrected=True,
            )

        # =====================================================================
        # Regra 3: "maior/menor" + contexto de vendas = SUM + ORDER
        # =====================================================================
        # "maior aumento de vendas" = SUM + ORDER DESC (não MAX)
        # =====================================================================

        has_ambiguous_keyword = any(kw in query_lower for kw in self.ambiguous_keywords)
        has_sales_context = any(
            kw in query_lower
            for kw in ["vendas", "faturamento", "receita", "quantidade"]
        )

        if has_ambiguous_keyword and has_sales_context:
            # "maior vendas" = soma ordenada DESC, não MAX
            logger.info(
                "[AggregationValidator] Ambiguous keyword + sales context detected: "
                "interpreting as SUM with ordering, not MAX"
            )
            sum_score += 2.0  # Forte boost para SUM

        # =====================================================================
        # Regra 4: Determinar agregação correta baseada em scores
        # =====================================================================

        total_score = sum_score + avg_score + count_score + max_score + min_score

        if total_score == 0:
            # Fallback: sem contexto claro
            return self._fallback_aggregation(
                metric_name, proposed_aggregation, parsed_entities
            )

        # Normalizar scores
        sum_confidence = sum_score / total_score
        avg_confidence = avg_score / total_score
        count_confidence = count_score / total_score
        max_confidence = max_score / total_score
        min_confidence = min_score / total_score

        # Selecionar agregação com maior score
        scores = {
            "sum": sum_confidence,
            "avg": avg_confidence,
            "count": count_confidence,
            "max": max_confidence,
            "min": min_confidence,
        }

        best_aggregation = max(scores, key=scores.get)
        best_confidence = scores[best_aggregation]

        # Verificar se houve correção
        was_corrected = (
            proposed_aggregation is not None
            and proposed_aggregation != best_aggregation
        )

        reasoning = (
            f"Context-based aggregation: {best_aggregation.upper()} "
            f"(confidence={best_confidence:.2f}). "
        )

        if was_corrected:
            reasoning += f"Corrected from {proposed_aggregation.upper()}."

        logger.info(
            f"[AggregationValidator] Validated aggregation: {best_aggregation} "
            f"(confidence={best_confidence:.2f}, corrected={was_corrected})"
        )

        return AggregationValidationResult(
            aggregation=best_aggregation,
            is_valid=True,
            confidence=best_confidence,
            reasoning=reasoning,
            original_aggregation=proposed_aggregation if was_corrected else None,
            was_corrected=was_corrected,
        )

    def _fallback_aggregation(
        self,
        metric_name: str,
        proposed_aggregation: Optional[str],
        parsed_entities: Dict[str, Any],
    ) -> AggregationValidationResult:
        """
        Fallback para quando nenhum contexto claro é detectado.

        Args:
            metric_name: Nome da métrica
            proposed_aggregation: Agregação proposta
            parsed_entities: Entidades parseadas

        Returns:
            AggregationValidationResult com agregação de fallback
        """
        # Fallback 1: Usar agregação proposta se for válida
        if proposed_aggregation and proposed_aggregation != "max":
            return AggregationValidationResult(
                aggregation=proposed_aggregation,
                is_valid=True,
                confidence=0.60,
                reasoning=f"Fallback: Using proposed aggregation {proposed_aggregation.upper()}",
                original_aggregation=None,
                was_corrected=False,
            )

        # Fallback 2: Default baseado no tipo de metrica
        if self._is_numeric_metric(metric_name):
            # Numeric metric columns: default to SUM
            return AggregationValidationResult(
                aggregation="sum",
                is_valid=True,
                confidence=0.80,
                reasoning="Fallback: SUM is default for numeric metric columns",
                original_aggregation=proposed_aggregation,
                was_corrected=(proposed_aggregation != "sum"),
            )

        # Fallback 3: Default universal
        return AggregationValidationResult(
            aggregation="sum",
            is_valid=True,
            confidence=0.50,
            reasoning="Fallback: SUM is universal default",
            original_aggregation=proposed_aggregation,
            was_corrected=(proposed_aggregation != "sum"),
        )

    def get_recommended_aggregation(self, metric_name: str, query: str) -> str:
        """
        Retorna agregacao recomendada para metrica e query.

        Args:
            metric_name: Nome da métrica
            query: Query do usuário

        Returns:
            Nome da agregação recomendada
        """
        result = self.validate_aggregation(query, metric_name)
        return result.aggregation


# =============================================================================
# Função Helper para Integração no Workflow
# =============================================================================


def validate_and_correct_aggregation(
    query: str,
    metric_name: str,
    proposed_aggregation: Optional[str] = None,
    parsed_entities: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Funcao helper para validar e corrigir agregacao.

    Esta funcao pode ser integrada no workflow do graphic_classifier.

    Args:
        query: Query do usuario
        metric_name: Nome da metrica
        proposed_aggregation: Agregacao proposta (opcional)
        parsed_entities: Entidades parseadas (opcional)

    Returns:
        Dict com agregacao validada e metadados:
        {
            "aggregation": str,
            "is_valid": bool,
            "confidence": float,
            "was_corrected": bool,
            "reasoning": str
        }

    Examples:
        >>> result = validate_and_correct_aggregation(
        ...     "maior aumento de vendas",
        ...     "Qtd_Vendida",
        ...     "max"
        ... )
        >>> result["aggregation"]
        'sum'
        >>> result["was_corrected"]
        True
    """
    validator = AggregationValidator()
    validation_result = validator.validate_aggregation(
        query=query,
        metric_name=metric_name,
        proposed_aggregation=proposed_aggregation,
        parsed_entities=parsed_entities,
    )

    return {
        "aggregation": validation_result.aggregation,
        "is_valid": validation_result.is_valid,
        "confidence": validation_result.confidence,
        "was_corrected": validation_result.was_corrected,
        "reasoning": validation_result.reasoning,
        "original_aggregation": validation_result.original_aggregation,
    }
