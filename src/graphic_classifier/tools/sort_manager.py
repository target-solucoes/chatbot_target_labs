"""
Sort Manager - FASE 2: Correcao de Ordenacao.

Este modulo implementa o gerenciamento de ordenacao (sorting) com suporte a:
1. Mapeamento de keywords de ordenacao
2. Sort por campos calculados (variation, difference, growth)
3. Validacao sort vs intent

Resolve o problema de ordenacao incorreta identificado no diagnostico:
- "maior aumento" deve ordenar por VARIACAO, nao por valor absoluto
- Validacao garante consistencia entre intent e sort
"""

import re
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from src.graphic_classifier.utils.text_cleaner import normalize_text

logger = logging.getLogger(__name__)


# =============================================================================
# SORT CONFIGURATION
# =============================================================================


@dataclass
class SortConfig:
    """
    Configuracao de ordenacao.

    Attributes:
        by: Campo de ordenacao (value, variation, difference, growth, etc.)
        order: Ordem (asc, desc)
        requires_calculated_field: Se requer campo calculado
        calculated_field_type: Tipo de campo calculado (variation, difference, etc.)
    """

    by: str
    order: str  # "asc" ou "desc"
    requires_calculated_field: bool = False
    calculated_field_type: Optional[str] = None


# =============================================================================
# KEYWORD PATTERNS
# =============================================================================


class SortKeywordDetector:
    """
    Detector de keywords de ordenacao.

    Identifica keywords que indicam ordenacao e infere a direcao (asc/desc)
    e o campo de ordenacao.
    """

    # Keywords que indicam ordenacao DESCENDENTE (maiores para menores)
    DESC_KEYWORDS = [
        "maior",
        "maiores",
        "mais",
        "top",
        "ranking",
        "melhor",
        "melhores",
        "maximo",
        "maximos",
    ]

    # Keywords que indicam ordenacao ASCENDENTE (menores para maiores)
    # REFINAMENTO: Expandido para suportar polaridade negativa completa
    # Nota: Inclui versões normalizadas (stemming) para garantir detecção
    ASC_KEYWORDS = [
        "menor",
        "menores",
        "menos",
        "meno",  # Versão normalizada de "menos"
        "minimo",
        "minimos",
        "pior",
        "piores",
        "bottom",
        "baixo",
        "baixos",
        "inferior",
        "inferiores",
        "reduzido",
        "reduzidos",
        "limitado",
        "limitados",
    ]

    # Keywords que indicam VARIACAO/CRESCIMENTO (requerem campo calculado)
    VARIATION_KEYWORDS = [
        "aumento",
        "crescimento",
        "variacao",
        "mudanca",
        "evolucao",
        "incremento",
        "alta",
        "subida",
    ]

    # Keywords que indicam REDUCAO (variacao negativa)
    REDUCTION_KEYWORDS = [
        "reducao",
        "queda",
        "declinio",
        "diminuicao",
        "decrescimo",
        "baixa",
        "descida",
    ]

    # Keywords que indicam DIFERENCA
    DIFFERENCE_KEYWORDS = [
        "diferenca",
        "delta",
        "gap",
        "distancia",
    ]

    @staticmethod
    def detect_sort_keywords(query: str) -> Dict[str, any]:
        """
        Detecta keywords de ordenacao na query.

        Args:
            query: Query do usuario

        Returns:
            Dicionario com:
            - has_sort: bool
            - order: "asc" ou "desc"
            - requires_calculated_field: bool
            - calculated_field_type: "variation", "difference", etc.
        """
        normalized = normalize_text(query)

        result = {
            "has_sort": False,
            "order": None,
            "requires_calculated_field": False,
            "calculated_field_type": None,
        }

        # Detectar ordenacao descendente
        has_desc = any(kw in normalized for kw in SortKeywordDetector.DESC_KEYWORDS)

        # Detectar ordenacao ascendente
        has_asc = any(kw in normalized for kw in SortKeywordDetector.ASC_KEYWORDS)

        # Detectar variacao/crescimento
        has_variation = any(
            kw in normalized for kw in SortKeywordDetector.VARIATION_KEYWORDS
        )

        # Detectar reducao
        has_reduction = any(
            kw in normalized for kw in SortKeywordDetector.REDUCTION_KEYWORDS
        )

        # Detectar diferenca
        has_difference = any(
            kw in normalized for kw in SortKeywordDetector.DIFFERENCE_KEYWORDS
        )

        # Determinar se tem sort
        if has_desc or has_asc:
            result["has_sort"] = True

            # Determinar ordem (ASC tem prioridade sobre DESC para evitar ambiguidade)
            if has_asc:
                result["order"] = "asc"
                logger.debug(
                    f"[SortKeywordDetector] Detected ASCENDING keywords (negative polarity) in query: '{query}'"
                )
            elif has_desc:
                result["order"] = "desc"

            # Determinar se requer campo calculado
            if has_variation or has_reduction:
                result["requires_calculated_field"] = True
                result["calculated_field_type"] = "variation"
            elif has_difference:
                result["requires_calculated_field"] = True
                result["calculated_field_type"] = "difference"

        return result


# =============================================================================
# SORT FIELD RESOLVER
# =============================================================================


class SortFieldResolver:
    """
    Resolvedor de campos de ordenacao.

    Determina qual campo deve ser usado para ordenacao baseado em:
    - Intent da query
    - Keywords detectadas
    - Estrutura de dimensoes
    """

    @staticmethod
    def resolve(
        query: str,
        intent: str,
        intent_config: Optional[Dict] = None,
        dimensions: Optional[List[str]] = None,
        metrics: Optional[List[str]] = None,
    ) -> SortConfig:
        """
        Resolve o campo de ordenacao.

        Args:
            query: Query do usuario
            intent: Intent detectado
            intent_config: Configuracao do intent
            dimensions: Lista de dimensoes
            metrics: Lista de metricas

        Returns:
            SortConfig com campo e ordem de ordenacao
        """
        # Detectar keywords de sort
        sort_keywords = SortKeywordDetector.detect_sort_keywords(query)

        # Se intent_config tem sort_config, usar como base MAS respeitar keywords ASC
        # REFINAMENTO: Polaridade negativa (menor, pior, etc.) tem prioridade sobre intent_config
        if intent_config and "sort_config" in intent_config:
            base_sort = intent_config["sort_config"]
            if base_sort:
                # Se keywords detectaram ASC (polaridade negativa), usar ASC
                # Caso contrário, usar a ordem definida no intent_config
                order = (
                    sort_keywords.get("order")
                    if sort_keywords.get("has_sort")
                    else base_sort.get("order", "desc")
                )

                return SortConfig(
                    by=base_sort.get("by", "value"),
                    order=order,
                    requires_calculated_field=intent_config.get(
                        "requires_calculated_fields", False
                    ),
                    calculated_field_type=base_sort.get("by")
                    if base_sort.get("by") in ["variation", "difference"]
                    else None,
                )

        # Caso contrario, inferir do intent e keywords
        # FASE 3 - Etapa 3.2: Atualizado para usar novos nomes de intents
        if intent == "temporal_comparison_analysis":
            # Intent unificado que requer variacao (anteriormente: month_to_month_comparison + temporal_variation_analysis)
            # REFINAMENTO: Respeitar polaridade negativa (menor, pior, etc.) para inverter ordem
            order = (
                sort_keywords.get("order") if sort_keywords.get("has_sort") else "desc"
            )
            return SortConfig(
                by="variation",
                order=order,
                requires_calculated_field=True,
                calculated_field_type="variation",
            )
        elif intent == "entity_ranking":
            # Ranking de entidades por valor (anteriormente: product_ranking)
            return SortConfig(
                by="value",
                order=sort_keywords.get("order", "desc"),
                requires_calculated_field=False,
                calculated_field_type=None,
            )
        elif intent == "temporal_trend":
            # Tendencia temporal ordenada por tempo
            return SortConfig(
                by="temporal",
                order="asc",
                requires_calculated_field=False,
                calculated_field_type=None,
            )
        else:
            # Default: ordenar por valor
            return SortConfig(
                by="value",
                order=sort_keywords.get("order", "desc")
                if sort_keywords.get("has_sort")
                else "desc",
                requires_calculated_field=False,
                calculated_field_type=None,
            )


# =============================================================================
# SORT VALIDATOR
# =============================================================================


class SortValidator:
    """
    Validador de consistencia entre sort e intent.

    Verifica se a configuracao de ordenacao e compativel com o intent detectado.
    """

    # Mapeamento de intent -> sort esperado
    # FASE 3 - Etapa 3.2: Atualizado para usar novos nomes de intents
    EXPECTED_SORT_BY_INTENT = {
        "temporal_comparison_analysis": [
            "variation",
            "difference",
            "growth",
        ],  # Unificacao de month_to_month_comparison + temporal_variation_analysis
        "entity_ranking": ["value"],  # Anteriormente: product_ranking
        "temporal_trend": ["temporal"],
        "proportion_analysis": ["value"],
        "categorical_comparison": ["value"],
        "period_distribution": ["temporal", "value"],
        "composition_analysis": ["value"],
    }

    @staticmethod
    def validate(intent: str, sort_config: SortConfig) -> Tuple[bool, List[str]]:
        """
        Valida consistencia entre intent e sort.

        Args:
            intent: Intent detectado
            sort_config: Configuracao de sort

        Returns:
            (is_valid, warnings)
        """
        warnings = []

        expected_sorts = SortValidator.EXPECTED_SORT_BY_INTENT.get(intent)

        if not expected_sorts:
            # Intent nao tem expectativa de sort, aceitar qualquer
            return True, []

        # Verificar se sort.by esta na lista esperada
        if sort_config.by not in expected_sorts:
            warnings.append(
                f"Sort by '{sort_config.by}' may be inconsistent with intent '{intent}'. "
                f"Expected one of: {expected_sorts}"
            )

        # Verificar se requer campo calculado mas nao esta configurado
        # FASE 3 - Etapa 3.2: Atualizado para usar novo nome de intent
        if intent == "temporal_comparison_analysis":
            if not sort_config.requires_calculated_field:
                warnings.append(
                    f"Intent '{intent}' requires calculated field for sorting, "
                    f"but requires_calculated_field=False"
                )

        is_valid = len(warnings) == 0

        if not is_valid:
            logger.warning(
                f"[SortValidator] Validation warnings for intent '{intent}': {warnings}"
            )

        return is_valid, warnings


# =============================================================================
# CALCULATED FIELD GENERATOR
# =============================================================================


class CalculatedFieldGenerator:
    """
    Gerador de configuracao para campos calculados.

    Gera especificacoes para campos calculados como variacao, diferenca, etc.
    """

    @staticmethod
    def generate_variation_spec(
        metric_name: str, dimensions: List[str]
    ) -> Dict[str, any]:
        """
        Gera especificacao para campo de variacao.

        Args:
            metric_name: Nome da metrica
            dimensions: Lista de dimensoes

        Returns:
            Especificacao do campo calculado
        """
        return {
            "type": "variation",
            "metric": metric_name,
            "formula": "current - previous",
            "alias": f"{metric_name}_variation",
            "description": f"Variation of {metric_name} between periods",
            "temporal_dimension": next(
                (
                    dim
                    for dim in dimensions
                    if dim.lower() in ["mes", "month", "ano", "year", "data", "date"]
                ),
                None,
            ),
        }

    @staticmethod
    def generate_difference_spec(
        metric_name: str, base_value: Optional[float] = None
    ) -> Dict[str, any]:
        """
        Gera especificacao para campo de diferenca.

        Args:
            metric_name: Nome da metrica
            base_value: Valor base para comparacao (opcional)

        Returns:
            Especificacao do campo calculado
        """
        return {
            "type": "difference",
            "metric": metric_name,
            "formula": "value - base_value" if base_value else "value_A - value_B",
            "alias": f"{metric_name}_difference",
            "description": f"Difference of {metric_name}",
            "base_value": base_value,
        }

    @staticmethod
    def generate_growth_rate_spec(metric_name: str) -> Dict[str, any]:
        """
        Gera especificacao para taxa de crescimento.

        Args:
            metric_name: Nome da metrica

        Returns:
            Especificacao do campo calculado
        """
        return {
            "type": "growth_rate",
            "metric": metric_name,
            "formula": "((current - previous) / previous) * 100",
            "alias": f"{metric_name}_growth_rate",
            "description": f"Growth rate of {metric_name} (%)",
            "unit": "%",
        }


# =============================================================================
# SORT MANAGER (Main Interface)
# =============================================================================


class SortManager:
    """
    Gerenciador principal de ordenacao.

    Interface unificada para:
    1. Detectar keywords de ordenacao
    2. Resolver campo de ordenacao
    3. Validar consistencia
    4. Gerar campos calculados
    """

    def __init__(self):
        self.keyword_detector = SortKeywordDetector()
        self.field_resolver = SortFieldResolver()
        self.validator = SortValidator()
        self.calculated_field_generator = CalculatedFieldGenerator()

    def process(
        self,
        query: str,
        intent: str,
        intent_config: Optional[Dict] = None,
        dimensions: Optional[List[str]] = None,
        metrics: Optional[List[str]] = None,
        parsed_entities: Optional[
            Dict[str, any]
        ] = None,  # NOVO: para suportar semantic mapping
    ) -> Dict[str, any]:
        """
        Processa ordenacao completa.

        Args:
            query: Query do usuario
            intent: Intent detectado
            intent_config: Configuracao do intent
            dimensions: Lista de dimensoes
            metrics: Lista de metricas
            parsed_entities: Entidades parsed (CRITICAL: contém sort_by de semantic mapping)

        Returns:
            Dicionario com:
            - sort_config: SortConfig
            - is_valid: bool
            - warnings: List[str]
            - calculated_field_spec: Dict (se aplicavel)
        """
        # CRITICAL: Priorizar sort_by e sort_order do semantic mapping
        # Quando compare_variation é detectado, semantic_mapper define sort_by="variation"
        # e sort_order baseado em polarity. Isso tem PRIORIDADE ABSOLUTA.
        if parsed_entities:
            semantic_sort_by = parsed_entities.get("sort_by")
            semantic_sort_order = parsed_entities.get("sort_order")

            if semantic_sort_by:
                logger.info(
                    f"[SortManager] Using sort_by from semantic mapping: {semantic_sort_by} "
                    f"(order={semantic_sort_order})"
                )

                # Criar SortConfig diretamente do semantic mapping
                sort_config_dict = {
                    "by": semantic_sort_by,
                    "order": semantic_sort_order or "asc",
                }

                # Determinar se é campo calculado (variation)
                requires_calculated = semantic_sort_by == "variation"
                calculated_field_type = "variation" if requires_calculated else None

                # Gerar spec de campo calculado se variation
                calculated_field_spec = None
                if requires_calculated and metrics:
                    metric_name = metrics[0] if metrics else "value"
                    calculated_field_spec = (
                        self.calculated_field_generator.generate_variation_spec(
                            metric_name, dimensions or []
                        )
                    )

                return {
                    "sort_config": sort_config_dict,
                    "requires_calculated_field": requires_calculated,
                    "calculated_field_type": calculated_field_type,
                    "calculated_field_spec": calculated_field_spec,
                    "is_valid": True,
                    "warnings": [],
                }

        # FALLBACK: Lógica original para casos sem semantic mapping
        # Resolver campo de ordenacao
        sort_config = self.field_resolver.resolve(
            query, intent, intent_config, dimensions, metrics
        )

        # Validar consistencia
        is_valid, warnings = self.validator.validate(intent, sort_config)

        # Gerar spec de campo calculado se necessario
        calculated_field_spec = None
        if sort_config.requires_calculated_field and metrics:
            metric_name = metrics[0] if metrics else "value"

            if sort_config.calculated_field_type == "variation":
                calculated_field_spec = (
                    self.calculated_field_generator.generate_variation_spec(
                        metric_name, dimensions or []
                    )
                )
            elif sort_config.calculated_field_type == "difference":
                calculated_field_spec = (
                    self.calculated_field_generator.generate_difference_spec(
                        metric_name
                    )
                )
            elif sort_config.calculated_field_type == "growth_rate":
                calculated_field_spec = (
                    self.calculated_field_generator.generate_growth_rate_spec(
                        metric_name
                    )
                )

        result = {
            "sort_config": {"by": sort_config.by, "order": sort_config.order},
            "requires_calculated_field": sort_config.requires_calculated_field,
            "calculated_field_type": sort_config.calculated_field_type,
            "calculated_field_spec": calculated_field_spec,
            "is_valid": is_valid,
            "warnings": warnings,
        }

        logger.debug(
            f"[SortManager] Processed sort for intent '{intent}': "
            f"by={sort_config.by}, order={sort_config.order}, "
            f"requires_calculated={sort_config.requires_calculated_field}"
        )

        return result


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def detect_sort_keywords(query: str) -> Dict[str, any]:
    """Helper function para detectar keywords de ordenacao."""
    return SortKeywordDetector.detect_sort_keywords(query)


def resolve_sort_field(
    query: str,
    intent: str,
    intent_config: Optional[Dict] = None,
    dimensions: Optional[List[str]] = None,
    metrics: Optional[List[str]] = None,
) -> SortConfig:
    """Helper function para resolver campo de ordenacao."""
    return SortFieldResolver.resolve(query, intent, intent_config, dimensions, metrics)


def validate_sort_config(
    intent: str, sort_config: SortConfig
) -> Tuple[bool, List[str]]:
    """Helper function para validar configuracao de sort."""
    return SortValidator.validate(intent, sort_config)
