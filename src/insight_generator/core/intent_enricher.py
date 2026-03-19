"""
Intent Enrichment Module for Insight Generator.

This module enriches the base intent classification with semantic metadata
to improve insight generation relevance and quality.

FASE 1 Implementation - Intent Enrichment
"""

import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import Enum

logger = logging.getLogger(__name__)


class Polarity(Enum):
    """
    Sentiment polarity of the query intent.

    Determines the narrative tone and focus of insights.
    """

    POSITIVE = "positive"  # crescimento, aumento, melhoria, ganho
    NEGATIVE = "negative"  # queda, baixa, perda, redução, diminuição
    NEUTRAL = "neutral"  # histórico, comparação, distribuição


class TemporalFocus(Enum):
    """
    Temporal dimension of the analysis.

    Determines which temporal metrics are most relevant.
    """

    SINGLE_PERIOD = "single_period"  # análise de um único período
    PERIOD_OVER_PERIOD = (
        "period_over_period"  # comparação entre períodos (ex: maio vs junho)
    )
    TIME_SERIES = "time_series"  # série temporal completa (ex: evolução mensal)
    SEASONALITY = "seasonality"  # análise de padrões sazonais


class ComparisonType(Enum):
    """
    Type of comparison being requested.

    Determines comparison metrics and narrative structure.
    """

    NONE = "none"  # sem comparação explícita
    CATEGORY_VS_CATEGORY = (
        "category_vs_category"  # produto vs produto, cliente vs cliente
    )
    PERIOD_VS_PERIOD = "period_vs_period"  # maio vs junho, 2015 vs 2016
    ACTUAL_VS_TARGET = "actual_vs_target"  # real vs meta (futuro)


@dataclass
class EnrichedIntent:
    """
    Intent enriquecido com metadados semânticos.

    Esta estrutura expande o intent classificado pelo graphic_classifier
    com informações contextuais que direcionam a geração de insights.

    Attributes:
        base_intent: Intent original classificado (ranking, variation, trend, etc.)
        polarity: Polaridade semântica da query (positive, negative, neutral)
        temporal_focus: Tipo de análise temporal (single_period, period_over_period, etc.)
        comparison_type: Tipo de comparação solicitada (none, category_vs_category, etc.)
        suggested_metrics: Lista de métricas sugeridas baseadas no contexto
        key_entities: Entidades principais extraídas da query (produtos, clientes, períodos)
        filters_context: Contexto dos filtros aplicados (região, categoria, período)
        narrative_angle: Ângulo narrativo sugerido para os insights
    """

    base_intent: str
    polarity: Polarity
    temporal_focus: TemporalFocus
    comparison_type: ComparisonType
    suggested_metrics: List[str] = field(default_factory=list)
    key_entities: List[str] = field(default_factory=list)
    filters_context: Dict[str, Any] = field(default_factory=dict)
    narrative_angle: str = ""


class IntentEnricher:
    """
    Enriquece intent com análise semântica profunda.

    Esta classe analisa a query original do usuário e metadados do pipeline
    para extrair informações semânticas que não são capturadas pelo
    graphic_classifier básico.

    Methods:
        enrich: Método principal que retorna EnrichedIntent
    """

    # Palavras-chave para detecção de polaridade
    POSITIVE_KEYWORDS = [
        "crescimento",
        "cresceram",
        "crescer",
        "aumento",
        "aumentaram",
        "aumentar",
        "melhoria",
        "melhoraram",
        "melhorar",
        "ganho",
        "ganharam",
        "ganhar",
        "alta",
        "subida",
        "subir",
        "elevação",
        "elevar",
        "expansão",
        "expandir",
        "sucesso",
        "destaque",
        "destaques",
        "melhor",
        "melhores",
        "top",
    ]

    NEGATIVE_KEYWORDS = [
        "queda",
        "quedas",
        "caiu",
        "cair",
        "caíram",
        "baixa",
        "baixas",
        "baixo",
        "redução",
        "reduziu",
        "reduzir",
        "perda",
        "perdas",
        "perdeu",
        "perder",
        "perderam",
        "diminuição",
        "diminuiu",
        "diminuir",
        "declínio",
        "declinar",
        "pior",
        "piores",
        "retração",
        "retrair",
        "problema",
        "problemas",
    ]

    # Palavras-chave para detecção de foco temporal
    PERIOD_COMPARISON_KEYWORDS = [
        "entre",
        "versus",
        "vs",
        "comparado",
        "comparação",
        "de {} para",
        "de {} até",
        "entre {} e {}",
    ]

    TIME_SERIES_KEYWORDS = [
        "evolução",
        "histórico",
        "ao longo",
        "durante",
        "tendência",
        "trajetória",
        "progressão",
        "desenvolvimento",
    ]

    SEASONALITY_KEYWORDS = [
        "sazonal",
        "sazonalidade",
        "ciclo",
        "padrão mensal",
        "padrão anual",
        "recorrente",
    ]

    # Mapeamento de intent → métricas sugeridas
    INTENT_METRICS_MAP = {
        "ranking": ["concentration", "gap", "top_n", "cumulative"],
        "variation": ["delta", "growth_rate", "absolute_change", "period_comparison"],
        "trend": ["slope", "momentum", "volatility", "forecast"],
        "comparison": ["ratio", "index", "benchmark_gap", "relative_position"],
        "distribution": ["mean", "median", "std_dev", "percentiles", "outliers"],
        "temporal": ["trend", "volatility", "seasonality", "inflection_points"],
        "composition": ["share", "pareto", "hhi", "diversity_index"],
    }

    def __init__(self):
        """Inicializa o IntentEnricher."""
        logger.debug("[IntentEnricher] Initialized")

    def enrich(
        self,
        base_intent: str,
        user_query: str,
        chart_spec: Dict[str, Any],
        analytics_result: Dict[str, Any],
    ) -> EnrichedIntent:
        """
        Enriquece o intent com análise semântica.

        Args:
            base_intent: Intent classificado pelo graphic_classifier
            user_query: Query original do usuário
            chart_spec: Especificação do gráfico (inclui filtros, dimensões)
            analytics_result: Resultado da execução analítica

        Returns:
            EnrichedIntent com metadados semânticos
        """
        logger.info(f"[IntentEnricher] Enriching intent: {base_intent}")
        logger.debug(f"[IntentEnricher] User query: {user_query}")

        # Detectar polaridade
        polarity = self._detect_polarity(user_query)
        logger.debug(f"[IntentEnricher] Detected polarity: {polarity.value}")

        # Detectar foco temporal
        temporal_focus = self._detect_temporal_focus(user_query, chart_spec)
        logger.debug(
            f"[IntentEnricher] Detected temporal_focus: {temporal_focus.value}"
        )

        # Detectar tipo de comparação
        comparison_type = self._detect_comparison_type(user_query, chart_spec)
        logger.debug(
            f"[IntentEnricher] Detected comparison_type: {comparison_type.value}"
        )

        # Sugerir métricas baseadas em intent e contexto
        suggested_metrics = self._suggest_metrics(base_intent, polarity, temporal_focus)
        logger.debug(f"[IntentEnricher] Suggested metrics: {suggested_metrics}")

        # Extrair entidades-chave
        key_entities = self._extract_entities(user_query, chart_spec)
        logger.debug(f"[IntentEnricher] Key entities: {key_entities}")

        # Extrair contexto de filtros
        filters_context = self._extract_filters_context(chart_spec)
        logger.debug(f"[IntentEnricher] Filters context: {filters_context}")

        # Definir ângulo narrativo
        narrative_angle = self._define_narrative_angle(
            base_intent, polarity, temporal_focus, comparison_type
        )
        logger.debug(f"[IntentEnricher] Narrative angle: {narrative_angle}")

        enriched = EnrichedIntent(
            base_intent=base_intent,
            polarity=polarity,
            temporal_focus=temporal_focus,
            comparison_type=comparison_type,
            suggested_metrics=suggested_metrics,
            key_entities=key_entities,
            filters_context=filters_context,
            narrative_angle=narrative_angle,
        )

        logger.info("[IntentEnricher] Intent enrichment completed successfully")
        return enriched

    def _detect_polarity(self, user_query: str) -> Polarity:
        """
        Detecta a polaridade semântica da query.

        Args:
            user_query: Query original do usuário

        Returns:
            Polarity enum value
        """
        query_lower = user_query.lower()

        # Verificar palavras-chave negativas
        for keyword in self.NEGATIVE_KEYWORDS:
            if keyword in query_lower:
                logger.debug(f"[IntentEnricher] Negative keyword found: {keyword}")
                return Polarity.NEGATIVE

        # Verificar palavras-chave positivas
        for keyword in self.POSITIVE_KEYWORDS:
            if keyword in query_lower:
                logger.debug(f"[IntentEnricher] Positive keyword found: {keyword}")
                return Polarity.POSITIVE

        # Default: neutral
        return Polarity.NEUTRAL

    def _detect_temporal_focus(
        self, user_query: str, chart_spec: Dict[str, Any]
    ) -> TemporalFocus:
        """
        Detecta o foco temporal da análise.

        Args:
            user_query: Query original do usuário
            chart_spec: Especificação do gráfico

        Returns:
            TemporalFocus enum value
        """
        query_lower = user_query.lower()

        # Verificar comparação entre períodos
        # Padrões: "entre X e Y", "de X para Y", "X vs Y"
        if re.search(r"\bentre\b.*\be\b", query_lower):
            logger.debug(
                "[IntentEnricher] Period comparison pattern found: 'entre X e Y'"
            )
            return TemporalFocus.PERIOD_OVER_PERIOD

        if re.search(r"\bde\b.*\b(para|até|a)\b", query_lower):
            logger.debug(
                "[IntentEnricher] Period comparison pattern found: 'de X para Y'"
            )
            return TemporalFocus.PERIOD_OVER_PERIOD

        if re.search(r"(vs|versus|comparado)", query_lower):
            logger.debug("[IntentEnricher] Comparison pattern found")
            return TemporalFocus.PERIOD_OVER_PERIOD

        # Verificar série temporal
        for keyword in self.TIME_SERIES_KEYWORDS:
            if keyword in query_lower:
                logger.debug(f"[IntentEnricher] Time series keyword found: {keyword}")
                return TemporalFocus.TIME_SERIES

        # Verificar sazonalidade
        for keyword in self.SEASONALITY_KEYWORDS:
            if keyword in query_lower:
                logger.debug(f"[IntentEnricher] Seasonality keyword found: {keyword}")
                return TemporalFocus.SEASONALITY

        # Verificar se há dimensão temporal no chart_spec
        dimensions = chart_spec.get("dimensions", [])
        for dim in dimensions:
            if isinstance(dim, dict):
                column = dim.get("column", "").lower()
                if any(
                    temporal_col in column
                    for temporal_col in ["data", "mes", "ano", "date", "month", "year"]
                ):
                    logger.debug(f"[IntentEnricher] Temporal dimension found: {column}")
                    return TemporalFocus.TIME_SERIES

        # Default: single period
        return TemporalFocus.SINGLE_PERIOD

    def _detect_comparison_type(
        self, user_query: str, chart_spec: Dict[str, Any]
    ) -> ComparisonType:
        """
        Detecta o tipo de comparação solicitada.

        Args:
            user_query: Query original do usuário
            chart_spec: Especificação do gráfico

        Returns:
            ComparisonType enum value
        """
        query_lower = user_query.lower()

        # Verificar comparação de período
        if re.search(r"\bentre\b.*\be\b", query_lower):
            # Checar se é comparação temporal
            if any(
                temporal in query_lower
                for temporal in [
                    "maio",
                    "junho",
                    "janeiro",
                    "fevereiro",
                    "março",
                    "abril",
                    "julho",
                    "agosto",
                    "setembro",
                    "outubro",
                    "novembro",
                    "dezembro",
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
                ]
            ):
                logger.debug("[IntentEnricher] Period vs period comparison detected")
                return ComparisonType.PERIOD_VS_PERIOD

        # Verificar comparação categórica explícita
        if re.search(r"(vs|versus|comparado|comparação)", query_lower):
            logger.debug("[IntentEnricher] Category vs category comparison detected")
            return ComparisonType.CATEGORY_VS_CATEGORY

        # Verificar se chart_type indica comparação
        chart_type = chart_spec.get("chart_type", "")
        if (
            "composed" in chart_type
            or "grouped" in chart_type
            or "stacked" in chart_type
        ):
            logger.debug(
                f"[IntentEnricher] Comparison implied by chart_type: {chart_type}"
            )
            return ComparisonType.CATEGORY_VS_CATEGORY

        # Default: none
        return ComparisonType.NONE

    def _suggest_metrics(
        self, base_intent: str, polarity: Polarity, temporal_focus: TemporalFocus
    ) -> List[str]:
        """
        Sugere métricas relevantes baseadas no contexto.

        Args:
            base_intent: Intent base
            polarity: Polaridade detectada
            temporal_focus: Foco temporal

        Returns:
            Lista de métricas sugeridas
        """
        metrics = []

        # Métricas base por intent
        base_metrics = self.INTENT_METRICS_MAP.get(base_intent, [])
        metrics.extend(base_metrics)

        # Adicionar métricas específicas por polaridade
        if polarity == Polarity.NEGATIVE:
            metrics.extend(["loss_magnitude", "decline_rate", "impact_assessment"])
        elif polarity == Polarity.POSITIVE:
            metrics.extend(["gain_magnitude", "growth_rate", "opportunity_assessment"])

        # Adicionar métricas específicas por foco temporal
        if temporal_focus == TemporalFocus.PERIOD_OVER_PERIOD:
            metrics.extend(["delta", "growth_rate", "absolute_change"])
        elif temporal_focus == TemporalFocus.TIME_SERIES:
            metrics.extend(["trend", "momentum", "volatility"])
        elif temporal_focus == TemporalFocus.SEASONALITY:
            metrics.extend(["seasonality_index", "cyclical_pattern"])

        # Remover duplicatas mantendo ordem
        metrics = list(dict.fromkeys(metrics))

        return metrics

    def _extract_entities(
        self, user_query: str, chart_spec: Dict[str, Any]
    ) -> List[str]:
        """
        Extrai entidades-chave da query e chart_spec.

        Args:
            user_query: Query original do usuário
            chart_spec: Especificação do gráfico

        Returns:
            Lista de entidades principais
        """
        entities = []

        # Extrair entidades de dimensões
        dimensions = chart_spec.get("dimensions", [])
        for dim in dimensions:
            if isinstance(dim, dict):
                alias = dim.get("alias", dim.get("column", ""))
                if alias:
                    entities.append(alias)

        # Extrair entidades de métricas
        metrics = chart_spec.get("metrics", [])
        for metric in metrics:
            if isinstance(metric, dict):
                alias = metric.get("alias", metric.get("column", ""))
                if alias:
                    entities.append(alias)

        # Extrair menções explícitas de meses
        months = [
            "janeiro",
            "fevereiro",
            "março",
            "abril",
            "maio",
            "junho",
            "julho",
            "agosto",
            "setembro",
            "outubro",
            "novembro",
            "dezembro",
        ]
        query_lower = user_query.lower()
        for month in months:
            if month in query_lower:
                entities.append(month.capitalize())

        # Extrair anos mencionados
        years = re.findall(r"\b(20\d{2}|19\d{2})\b", user_query)
        entities.extend(years)

        return entities

    def _extract_filters_context(self, chart_spec: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extrai contexto dos filtros aplicados.

        Args:
            chart_spec: Especificação do gráfico

        Returns:
            Dicionário com contexto de filtros
        """
        filters = chart_spec.get("filters", {})

        context = {
            "has_filters": len(filters) > 0,
            "filter_count": len(filters),
            "filter_types": list(filters.keys()) if filters else [],
            "temporal_filter": any(
                key.lower() in ["ano", "mes", "data", "year", "month", "date"]
                for key in filters.keys()
            )
            if filters
            else False,
            "categorical_filter": any(
                key.lower() not in ["ano", "mes", "data", "year", "month", "date"]
                for key in filters.keys()
            )
            if filters
            else False,
        }

        return context

    def _define_narrative_angle(
        self,
        base_intent: str,
        polarity: Polarity,
        temporal_focus: TemporalFocus,
        comparison_type: ComparisonType,
    ) -> str:
        """
        Define o ângulo narrativo dos insights.

        Args:
            base_intent: Intent base
            polarity: Polaridade
            temporal_focus: Foco temporal
            comparison_type: Tipo de comparação

        Returns:
            String descrevendo o ângulo narrativo
        """
        # Combinar contextos para definir narrativa
        angle_parts = []

        # Componente de intent
        if base_intent == "ranking":
            angle_parts.append("análise de posicionamento e concentração")
        elif base_intent == "variation":
            angle_parts.append("análise de variação e mudança")
        elif base_intent == "trend":
            angle_parts.append("análise de tendência e evolução")
        elif base_intent == "comparison":
            angle_parts.append("análise comparativa")
        elif base_intent == "distribution":
            angle_parts.append("análise de distribuição")

        # Componente de polaridade
        if polarity == Polarity.NEGATIVE:
            angle_parts.append("com foco em quedas e riscos")
        elif polarity == Polarity.POSITIVE:
            angle_parts.append("com foco em crescimento e oportunidades")

        # Componente temporal
        if temporal_focus == TemporalFocus.PERIOD_OVER_PERIOD:
            angle_parts.append("entre períodos específicos")
        elif temporal_focus == TemporalFocus.TIME_SERIES:
            angle_parts.append("ao longo do tempo")
        elif temporal_focus == TemporalFocus.SEASONALITY:
            angle_parts.append("considerando padrões sazonais")

        # Componente de comparação
        if comparison_type == ComparisonType.PERIOD_VS_PERIOD:
            angle_parts.append("comparando desempenho temporal")
        elif comparison_type == ComparisonType.CATEGORY_VS_CATEGORY:
            angle_parts.append("comparando categorias distintas")

        return ", ".join(angle_parts)
