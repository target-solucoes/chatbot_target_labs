"""
Intent Classifier - FASE 2 + FASE 3: Sistema de Intents Especificos.

Este modulo implementa a taxonomia de intents especificos conforme definido em
planning_graph_classifier_diagnosis.md - FASE 2, Etapa 2.1 e FASE 3, Etapa 3.2.

A taxonomia de intents permite classificar queries com alta precisao,
mapeando cada intent para configuracoes especificas de grafico.

Taxonomia de Intents (FASE 3 - Refatorada):
- temporal_comparison_analysis: Analise de comparacao temporal (unificacao de month_to_month_comparison + temporal_variation_analysis)
- entity_ranking: Ranking de entidades (produtos, clientes, regioes, vendedores)
- period_distribution: Distribuicao por periodo
- categorical_comparison: Comparacao categorica simples
- temporal_trend: Tendencia temporal continua
- proportion_analysis: Analise de proporcao/percentual
- composition_analysis: Analise de composicao (nested ranking)
"""

import re
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from src.graphic_classifier.utils.text_cleaner import normalize_text

logger = logging.getLogger(__name__)


@dataclass
class IntentConfig:
    """
    Configuracao associada a um intent especifico.

    Attributes:
        chart_type: Tipo de grafico recomendado
        requires_temporal_comparison: Se requer comparacao temporal
        requires_calculated_fields: Se requer campos calculados (variacao, etc.)
        dimension_structure: Estrutura de dimensoes (primary, series)
        sort_config: Configuracao de ordenacao
        aggregation_hint: Sugestao de agregacao
    """

    chart_type: str
    requires_temporal_comparison: bool = False
    requires_calculated_fields: bool = False
    dimension_structure: Optional[Dict[str, str]] = None
    sort_config: Optional[Dict[str, str]] = None
    aggregation_hint: Optional[str] = None


# =============================================================================
# TAXONOMIA DE INTENTS
# =============================================================================

INTENT_TAXONOMY = {
    # FASE 3 - Etapa 3.2: Intent unificado para comparacoes temporais
    # (Anteriormente: month_to_month_comparison + temporal_variation_analysis)
    # REFACTORED: Migrated from bar_vertical_composed to line_composed
    "temporal_comparison_analysis": IntentConfig(
        chart_type="line_composed",
        requires_temporal_comparison=True,
        requires_calculated_fields=True,
        dimension_structure={
            "primary": "temporal",  # Mes, Trimestre, etc. (must be first for line charts)
            "series": "entity",  # Produto, Cliente, Regiao, etc.
        },
        sort_config={"by": "variation", "order": "desc"},
        aggregation_hint="sum",
    ),
    # FASE 3 - Etapa 3.2: Intent renomeado de product_ranking para entity_ranking
    # (Agora suporta rankings de clientes, produtos, regioes, vendedores)
    "entity_ranking": IntentConfig(
        chart_type="bar_horizontal",
        requires_temporal_comparison=False,
        requires_calculated_fields=False,
        dimension_structure={"primary": "entity", "series": None},
        sort_config={"by": "value", "order": "desc"},
        aggregation_hint="sum",
    ),
    "period_distribution": IntentConfig(
        chart_type="bar_vertical_stacked",
        requires_temporal_comparison=False,
        requires_calculated_fields=False,
        dimension_structure={"primary": "temporal", "series": "entity"},
        sort_config=None,
        aggregation_hint="sum",
    ),
    "categorical_comparison": IntentConfig(
        chart_type="bar_vertical",
        requires_temporal_comparison=False,
        requires_calculated_fields=False,
        dimension_structure={"primary": "entity", "series": None},
        sort_config=None,
        aggregation_hint="sum",
    ),
    "temporal_trend": IntentConfig(
        chart_type="line_composed",  # FIXED: 'line' is not in schema, use 'line_composed'
        requires_temporal_comparison=False,
        requires_calculated_fields=False,
        dimension_structure={"primary": "temporal", "series": None},
        sort_config={"by": "temporal", "order": "asc"},
        aggregation_hint="sum",
    ),
    "proportion_analysis": IntentConfig(
        chart_type="pie",
        requires_temporal_comparison=False,
        requires_calculated_fields=False,
        dimension_structure={"primary": "entity", "series": None},
        sort_config={"by": "value", "order": "desc"},
        aggregation_hint="sum",
    ),
    "composition_analysis": IntentConfig(
        chart_type="bar_vertical_stacked",
        requires_temporal_comparison=False,
        requires_calculated_fields=False,
        dimension_structure={"primary": "outer_entity", "series": "inner_entity"},
        sort_config=None,
        aggregation_hint="sum",
    ),
}


# =============================================================================
# FASE 3 - ETAPA 3.1: ENTITY KEYWORDS FOR RANKING DIFFERENTIATION
# =============================================================================

ENTITY_KEYWORDS = {
    "customer_ranking": [
        "cliente",
        "clientes",
        "customer",
        "customers",
        "comprador",
        "compradores",
    ],
    "product_ranking": ["produto", "produtos", "product", "products", "item", "itens"],
    "region_ranking": [
        "regiao",
        "regioes",
        "estado",
        "estados",
        "uf",
        "cidade",
        "cidades",
    ],
    "seller_ranking": [
        "vendedor",
        "vendedores",
        "representante",
        "representantes",
        "seller",
        "sellers",
    ],
}


# =============================================================================
# INTENT PATTERNS
# =============================================================================


class IntentPattern:
    """
    Pattern para deteccao de intent especifico.

    Cada pattern define:
    - Keywords obrigatorias
    - Patterns regex
    - Funcao de validacao customizada
    - Score de confianca
    """

    def __init__(
        self,
        intent_name: str,
        keywords: Optional[List[str]] = None,
        regex_patterns: Optional[List[str]] = None,
        custom_validator: Optional[callable] = None,
        confidence_score: float = 0.90,
    ):
        self.intent_name = intent_name
        self.keywords = keywords or []
        self.regex_patterns = regex_patterns or []
        self.custom_validator = custom_validator
        self.confidence_score = confidence_score

    def matches(self, query: str, normalized: str, context: Dict) -> Tuple[bool, float]:
        """
        Verifica se o pattern corresponde a query.

        Args:
            query: Query original
            normalized: Query normalizada
            context: Contexto extraido (de context_analyzer)

        Returns:
            (matches, confidence_score)
        """
        # Verificar keywords
        if self.keywords:
            keyword_matches = sum(1 for kw in self.keywords if kw in normalized)
            if keyword_matches == 0:
                return False, 0.0

        # Verificar regex patterns
        if self.regex_patterns:
            regex_matches = sum(
                1 for pattern in self.regex_patterns if re.search(pattern, normalized)
            )
            if regex_matches == 0:
                return False, 0.0

        # Verificar validador customizado
        if self.custom_validator:
            is_valid = self.custom_validator(query, normalized, context)
            if not is_valid:
                return False, 0.0

        # Se passou em todas as verificacoes, retorna match com confidence
        return True, self.confidence_score


# =============================================================================
# FASE 3 - ETAPA 3.1: ENTITY CLASSIFICATION HELPER
# =============================================================================


def classify_ranking_entity(query: str, normalized: str) -> Optional[str]:
    """
    Classifica o tipo de entidade em uma query de ranking.

    FASE 3 - Etapa 3.1: Diferenciar Rankings por Entidade

    Esta funcao detecta se a query esta pedindo um ranking de:
    - customer_ranking: Ranking de clientes
    - product_ranking: Ranking de produtos
    - region_ranking: Ranking de regioes/estados
    - seller_ranking: Ranking de vendedores

    Args:
        query: Query original
        normalized: Query normalizada

    Returns:
        Tipo de entidade ("customer_ranking", "product_ranking", etc.) ou None
    """
    # Verificar cada tipo de entidade em ordem de prioridade
    for entity_type, keywords in ENTITY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in normalized:
                logger.debug(
                    f"[classify_ranking_entity] Detected entity_type='{entity_type}' "
                    f"from keyword='{keyword}'"
                )
                return entity_type

    # Fallback: se nenhum keyword foi detectado, assumir product_ranking (default)
    logger.debug(
        "[classify_ranking_entity] No specific entity detected, defaulting to 'product_ranking'"
    )
    return "product_ranking"


# =============================================================================
# VALIDADORES CUSTOMIZADOS
# =============================================================================


def validate_temporal_comparison_analysis(
    query: str, normalized: str, context: Dict
) -> bool:
    """
    Valida se a query e uma analise de comparacao temporal.

    FASE 3 - Etapa 3.2: Validador unificado que substitui:
    - validate_month_to_month_comparison
    - validate_temporal_variation_analysis

    Criterios (combinacao dos dois validadores anteriores):
    1. Deve ter dois periodos temporais OU keyword de variacao/crescimento
    2. Deve ter contexto temporal ou comparacao
    3. Deve mencionar uma entidade (produto, cliente, vendedor, regiao)
    4. NAO deve ser query de agregacao total (resposta pontual)
    """
    # CRITERIO 0 (BLOQUEADOR): Detectar queries de agregacao total
    # Exemplos: "Qual foi o total de vendas entre...", "Quanto vendemos de..."
    aggregation_total_keywords = [
        "total",
        "soma",
        "quanto",
        "qual foi",
        "qual o valor",
        "quantia",
    ]
    is_aggregation_total = any(kw in normalized for kw in aggregation_total_keywords)

    # Se e uma query de agregacao total, NAO e comparacao temporal
    # (query espera um numero unico, nao uma serie temporal)
    if is_aggregation_total:
        logger.debug(
            "[validate_temporal_comparison_analysis] Detected aggregation total query "
            "(keywords: total/soma/quanto). This requires a single value, not temporal comparison."
        )
        return False

    # Criterio 1a: Dois periodos temporais
    has_two_periods = context.get("between_periods_pattern") or context.get(
        "two_temporal_values"
    )

    # Criterio 1b: Keyword de variacao/crescimento
    variation_keywords = [
        "aumento",
        "crescimento",
        "variacao",
        "reducao",
        "queda",
        "mudanca",
    ]
    has_variation = any(kw in normalized for kw in variation_keywords)

    # Criterio 2: Contexto temporal ou comparacao
    has_temporal = (
        context.get("has_temporal_comparison")
        or context.get("has_temporal_dimension")
        or context.get("between_periods_pattern")
        or context.get("has_comparison_keywords")
    )

    # Criterio 3: Menciona entidade (produto, vendas, cliente, etc.)
    entity_keywords = [
        "produto",
        "cliente",
        "vendedor",
        "regiao",
        "estado",
        "linha",
        "venda",
        "vendas",
        "receita",
        "faturamento",
    ]
    has_entity = any(kw in normalized for kw in entity_keywords)

    # Query valida se:
    # (Tem dois periodos OU tem keyword de variacao) E tem contexto temporal E menciona entidade
    # E NAO e agregacao total
    return (has_two_periods or has_variation) and has_temporal and has_entity


def validate_entity_ranking(query: str, normalized: str, context: Dict) -> bool:
    """
    Valida se a query e um ranking de entidades.

    FASE 3 - Etapa 3.1: Diferencia entre rankings de diferentes entidades
    (clientes, produtos, regioes, vendedores) usando classify_ranking_entity().

    FASE 3 - Etapa 3.2: Renomeado de validate_product_ranking para
    validate_entity_ranking para refletir o suporte a multiplas entidades.

    Criterios:
    1. Deve ter keyword de ranking (top, maiores, etc.)
    2. NAO deve ter comparacao temporal complexa
    3. Detecta automaticamente o tipo de entidade (customer, product, region, seller)
    """
    ranking_keywords = ["top", "maiore", "menore", "ranking", "melhore", "piore"]
    has_ranking = any(kw in normalized for kw in ranking_keywords)

    # NAO deve ter comparacao temporal (se tiver, e temporal_comparison_analysis)
    has_temporal_comparison = context.get("has_temporal_comparison", False)

    # FASE 3 - Etapa 3.1: Detectar tipo de entidade para melhor logging
    if has_ranking and not has_temporal_comparison:
        entity_type = classify_ranking_entity(query, normalized)
        logger.debug(
            f"[validate_entity_ranking] Ranking query detected with entity_type='{entity_type}'"
        )
        # Armazenar entity_type no context para uso posterior
        context["detected_entity_type"] = entity_type

    return has_ranking and not has_temporal_comparison


def validate_period_distribution(query: str, normalized: str, context: Dict) -> bool:
    """
    Valida se a query e uma distribuicao por periodo.

    Criterios:
    1. Deve mencionar distribuicao ou composicao
    2. Deve ter dimensao temporal
    """
    distribution_keywords = ["distribuicao", "composicao", "quebra", "segmentacao"]
    has_distribution = any(kw in normalized for kw in distribution_keywords)

    has_temporal = context.get("has_temporal_dimension", False)

    return has_distribution and has_temporal


# =============================================================================
# DEFINICAO DE PATTERNS
# =============================================================================

INTENT_PATTERNS = [
    # FASE 3 - Etapa 3.2: Pattern unificado para comparacao temporal
    # (Substitui: month_to_month_comparison + temporal_variation_analysis)
    IntentPattern(
        intent_name="temporal_comparison_analysis",
        keywords=[
            "entre",
            "de",
            "para",
            "aumento",
            "crescimento",
            "variacao",
            "reducao",
            "queda",
        ],
        regex_patterns=[
            # Support optional years in temporal phrases (e.g., 'maio 2016')
            r"entre\s+\w+(?:\s+\d{4})?\s+e\s+\w+(?:\s+\d{4})?",
            r"de\s+\w+(?:\s+\d{4})?\s+para\s+\w+(?:\s+\d{4})?",
            r"\w+\s+vs\s+\w+",
        ],
        custom_validator=validate_temporal_comparison_analysis,
        confidence_score=0.95,
    ),
    # FASE 3 - Etapa 3.2: Pattern renomeado de product_ranking para entity_ranking
    IntentPattern(
        intent_name="entity_ranking",
        keywords=["top", "maiore", "menore", "ranking"],
        custom_validator=validate_entity_ranking,
        confidence_score=0.90,
    ),
    # Pattern 4: period_distribution
    IntentPattern(
        intent_name="period_distribution",
        keywords=["distribuicao", "composicao"],
        custom_validator=validate_period_distribution,
        confidence_score=0.88,
    ),
    # Pattern 5: proportion_analysis
    IntentPattern(
        intent_name="proportion_analysis",
        keywords=["percentual", "proporcao", "participacao", "fatia"],
        confidence_score=0.90,
    ),
    # Pattern 6: temporal_trend
    IntentPattern(
        intent_name="temporal_trend",
        keywords=["evolucao", "historico", "tendencia", "ao longo", "serie temporal"],
        confidence_score=0.88,
    ),
]


# =============================================================================
# INTENT CLASSIFIER
# =============================================================================


class IntentClassifier:
    """
    Classificador de intents especificos (FASE 2).

    Este classificador implementa a taxonomia de intents definida no
    planning_graph_classifier_diagnosis.md e mapeia cada intent para
    configuracoes especificas de grafico.
    """

    def __init__(self):
        """Initialize intent classifier with patterns."""
        self.patterns = INTENT_PATTERNS
        self.taxonomy = INTENT_TAXONOMY

    def classify(
        self, query: str, context: Dict, parsed_entities: Optional[Dict] = None
    ) -> Optional[Dict]:
        """
        Classifica o intent da query.

        Args:
            query: Query original
            context: Contexto extraido (de context_analyzer)
            parsed_entities: Entidades parseadas (opcional)

        Returns:
            {
                "intent": str,
                "confidence": float,
                "config": IntentConfig,
                "reasoning": str
            }
            ou None se nenhum intent foi detectado com confianca suficiente
        """
        normalized = normalize_text(query)

        logger.debug(f"[IntentClassifier] Classifying query: {query}")
        logger.debug(f"[IntentClassifier] Context: {context}")

        # Tentar cada pattern em ordem de prioridade
        for pattern in self.patterns:
            matches, confidence = pattern.matches(query, normalized, context)

            if matches:
                intent_name = pattern.intent_name
                config = self.taxonomy.get(intent_name)

                if not config:
                    logger.warning(
                        f"[IntentClassifier] Intent '{intent_name}' matched but not in taxonomy"
                    )
                    continue

                reasoning = self._generate_reasoning(
                    intent_name, pattern, query, context
                )

                result = {
                    "intent": intent_name,
                    "confidence": confidence,
                    "config": config,
                    "reasoning": reasoning,
                }

                logger.info(
                    f"[IntentClassifier] Intent detected: {intent_name} "
                    f"(confidence={confidence:.2f})"
                )

                return result

        logger.debug("[IntentClassifier] No specific intent detected")
        return None

    def _generate_reasoning(
        self, intent_name: str, pattern: IntentPattern, query: str, context: Dict
    ) -> str:
        """
        Gera reasoning para o intent detectado.

        Args:
            intent_name: Nome do intent
            pattern: Pattern que fez match
            query: Query original
            context: Contexto

        Returns:
            String com reasoning
        """
        reasoning_parts = [f"[Intent Classifier] Detected intent: '{intent_name}'"]

        # Adicionar keywords que fizeram match
        if pattern.keywords:
            normalized = normalize_text(query)
            matched_kws = [kw for kw in pattern.keywords if kw in normalized]
            if matched_kws:
                reasoning_parts.append(f"Keywords matched: {matched_kws}")

        # Adicionar informacao de contexto
        if context.get("has_temporal_comparison"):
            reasoning_parts.append("Temporal comparison detected")

        if context.get("between_periods_pattern"):
            reasoning_parts.append("Between periods pattern detected")

        if context.get("has_ranking"):
            reasoning_parts.append("Ranking pattern detected")

        return ". ".join(reasoning_parts)

    def get_config_for_intent(self, intent_name: str) -> Optional[IntentConfig]:
        """
        Retorna a configuracao para um intent especifico.

        Args:
            intent_name: Nome do intent

        Returns:
            IntentConfig ou None se intent nao existe
        """
        return self.taxonomy.get(intent_name)

    def list_intents(self) -> List[str]:
        """
        Lista todos os intents disponiveis.

        Returns:
            Lista de nomes de intents
        """
        return list(self.taxonomy.keys())


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def classify_intent(
    query: str, context: Dict, parsed_entities: Optional[Dict] = None
) -> Optional[Dict]:
    """
    Helper function para classificar intent.

    Args:
        query: Query original
        context: Contexto extraido
        parsed_entities: Entidades parseadas (opcional)

    Returns:
        Resultado da classificacao ou None
    """
    classifier = IntentClassifier()
    return classifier.classify(query, context, parsed_entities)


def get_intent_config(intent_name: str) -> Optional[IntentConfig]:
    """
    Helper function para obter configuracao de um intent.

    Args:
        intent_name: Nome do intent

    Returns:
        IntentConfig ou None
    """
    return INTENT_TAXONOMY.get(intent_name)
