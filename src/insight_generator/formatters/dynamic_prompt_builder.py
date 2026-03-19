"""
DynamicPromptBuilder - Intent-based Dynamic Prompt Construction.

FASE 3 Implementation - Dynamic Prompt Builder

This module constructs LLM prompts dynamically based on enriched intent
and composed metrics, eliminating rigid chart-type-based templates.

Key Principles:
    1. Intent-driven: prompt structure follows user intention, not chart type
    2. Metric-aware: only mentions metrics that were actually computed
    3. Context-rich: includes filters, temporal scope, and polarity
    4. Transparent: enforces formula traceability in LLM output
    5. Modular: combines persona + context + metrics + format rules

Architecture:
    ANALYSIS_PERSONAS: Intent-specific analyst personas
    FORMAT_RULES: Universal output format requirements
    DynamicPromptBuilder: Main orchestrator class
"""

from typing import Dict, Any, List, Optional
import logging

from ..core.intent_enricher import (
    EnrichedIntent,
    Polarity,
    TemporalFocus,
    ComparisonType,
)

logger = logging.getLogger(__name__)


# ============================================================================
# ANALYSIS PERSONAS - Intent-based analyst profiles
# ============================================================================

ANALYSIS_PERSONAS: Dict[str, str] = {
    # -------------------------------------------------------------------------
    # VARIATION ANALYSIS - Positive polarity
    # -------------------------------------------------------------------------
    "variation_positive": """Você é um analista de crescimento especializado em 
identificar oportunidades de expansão. Seu foco é:
- Quantificar ganhos e aceleração com precisão
- Identificar drivers de sucesso e momentum positivo
- Avaliar sustentabilidade do crescimento
- Destacar oportunidades de replicação e escalabilidade
- Contextualizar ganhos no cenário competitivo""",
    # -------------------------------------------------------------------------
    # VARIATION ANALYSIS - Negative polarity
    # -------------------------------------------------------------------------
    "variation_negative": """Você é um analista de riscos especializado em 
impactos de quedas e retrações. Seu foco é:
- Quantificar a magnitude da queda com precisão
- Identificar as categorias mais afetadas e causa raiz
- Avaliar riscos e implicações estratégicas
- Sugerir ações corretivas quando apropriado
- Contextualizar a queda em relação ao mercado""",
    # -------------------------------------------------------------------------
    # RANKING ANALYSIS - Concentration focus
    # -------------------------------------------------------------------------
    "ranking_concentration": """Você é um analista de portfólio especializado em 
concentração e riscos de dependência. Seu foco é:
- Avaliar níveis de concentração (Top N vs total)
- Identificar gaps competitivos críticos
- Analisar riscos de dependência excessiva
- Sugerir estratégias de diversificação
- Avaliar dinâmica competitiva e mudanças de posição""",
    # -------------------------------------------------------------------------
    # TEMPORAL ANALYSIS - Trend focus
    # -------------------------------------------------------------------------
    "temporal_trend": """Você é um analista de tendências especializado em 
padrões temporais e projeções. Seu foco é:
- Identificar direção e força da tendência
- Detectar pontos de inflexão e sazonalidade
- Avaliar consistência e volatilidade da série
- Projetar cenários baseados em padrões observados
- Contextualizar a evolução temporal""",
    # -------------------------------------------------------------------------
    # COMPARISON ANALYSIS - Gap focus
    # -------------------------------------------------------------------------
    "comparison_gap": """Você é um analista competitivo especializado em 
análise comparativa e benchmarking. Seu foco é:
- Quantificar diferenças absolutas e relativas
- Contextualizar gaps em termos estratégicos
- Identificar vantagens e desvantagens competitivas
- Sugerir ações de convergência ou diferenciação
- Avaliar magnitude das diferenças observadas""",
    # -------------------------------------------------------------------------
    # COMPOSITION ANALYSIS - Distribution focus
    # -------------------------------------------------------------------------
    "composition_distribution": """Você é um analista de composição especializado em 
estrutura de portfólio e balanceamento. Seu foco é:
- Avaliar a composição atual e proporções relativas
- Identificar desequilíbrios críticos
- Analisar índices de diversificação (HHI, Gini)
- Sugerir rebalanceamentos estratégicos
- Avaliar riscos de concentração em componentes""",
    # -------------------------------------------------------------------------
    # DISTRIBUTION ANALYSIS - Statistical focus
    # -------------------------------------------------------------------------
    "distribution_statistical": """Você é um analista quantitativo especializado em 
padrões de distribuição e dispersão. Seu foco é:
- Identificar forma da distribuição (normal, assimétrica, bimodal)
- Detectar outliers e valores extremos
- Avaliar medidas de dispersão (amplitude, desvio)
- Contextualizar distribuição observada
- Identificar implicações operacionais do padrão""",
    # -------------------------------------------------------------------------
    # GENERIC FALLBACK - Neutral analysis
    # -------------------------------------------------------------------------
    "generic_analytical": """Você é um analista de dados generalista especializado em 
análise executiva baseada em evidências. Seu foco é:
- Extrair insights estratégicos dos dados fornecidos
- Quantificar relações e padrões identificados
- Contextualizar descobertas no cenário analítico
- Fornecer interpretações objetivas e acionáveis
- Manter transparência total com fórmulas explícitas""",
}


# ============================================================================
# FORMAT RULES - Universal output requirements (JSON structure)
# ============================================================================

FORMAT_RULES = """
═══════════════════════════════════════════════════════════════
📋 REGRAS DE FORMATO (OBRIGATÓRIAS)
═══════════════════════════════════════════════════════════════

1. RESPOSTA EM JSON ESTRUTURADO:
{
  "narrative": "Texto livre de 400-800 caracteres com análise executiva completa...",
  "detailed_insights": [
    {
      "metric_name": "Nome da Métrica",
      "formula": "Fórmula completa = A op B → Resultado",
      "value": "Resultado formatado",
      "interpretation": "Implicação estratégica concisa"
    }
  ],
  "key_findings": ["bullet 1", "bullet 2", "bullet 3"]
}

2. TRANSPARÊNCIA TOTAL:
   - TODA métrica mencionada em "narrative" DEVE aparecer em "detailed_insights"
   - TODA entrada em "detailed_insights" DEVE ter a fórmula explícita
   - Formato de fórmula: "Base = numerador / denominador → resultado"
   
   EXEMPLOS CORRETOS:
   ✓ "Top 3 = R$ 8,66M / Total R$ 12,68M → 68,3%"
   ✓ "Gap = Líder - Segundo = R$ 3,4M - R$ 2,1M = R$ 1,3M (62% maior)"
   ✓ "Variação = (Final - Inicial) / Inicial = (450 - 300) / 300 = +50%"
   
   ANTI-EXEMPLOS (NUNCA FAÇA):
   ✗ "Top 3 representa 68,3%" (sem fórmula)
   ✗ "O líder tem 62% a mais" (sem valores base)

3. NARRATIVA EXPLICATIVA (campo "narrative"):
   - Texto fluido de 400-800 caracteres
   - Conecte conclusões aos dados: "[Conclusão] baseada em [número específico]"
   - Use linguagem executiva, não telegráfica
   - Sem emojis, sem repetições
   - Mencione apenas métricas disponíveis nos dados fornecidos

4. KEY_FINDINGS:
   - Exatamente 3-5 bullets
   - Máximo 140 caracteres cada
   - Acionáveis e com valores concretos
   - Formato: "Ação/Risco/Oportunidade + valor quantificado"

5. ALINHAMENTO OBRIGATÓRIO:
   - Todo valor em "narrative" está em "detailed_insights"
   - Todo valor em "detailed_insights" é mencionado em "narrative"
   - Valores numéricos são consistentes entre seções
"""


# ============================================================================
# DynamicPromptBuilder - Main orchestrator
# ============================================================================


class DynamicPromptBuilder:
    """
    Constrói prompts dinâmicos baseados em intenção e métricas.

    Esta classe elimina a dependência de templates fixos por chart_type,
    criando prompts contextuais que refletem:
    - A intenção real do usuário (intent enriched)
    - As métricas disponíveis (metric composer output)
    - O contexto analítico (filtros, período, polaridade)

    Methods:
        build_prompt: Método principal que retorna o prompt completo
        _select_persona: Seleciona persona baseada em intent + polarity
        _build_context_section: Constrói seção de contexto analítico
        _build_metrics_section: Formata métricas com fórmulas
        _build_task_section: Define tarefa específica para a LLM
    """

    def __init__(self):
        """Inicializa o builder."""
        self.personas = ANALYSIS_PERSONAS
        self.format_rules = FORMAT_RULES
        logger.info(
            "[DynamicPromptBuilder] Initialized with %d personas", len(self.personas)
        )

    def build_prompt(
        self,
        enriched_intent: EnrichedIntent,
        composed_metrics: Dict[str, Any],
        chart_spec: Optional[Dict[str, Any]] = None,
        analytics_metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Constrói prompt dinâmico completo.

        Args:
            enriched_intent: Intent enriquecido (FASE 1 output)
            composed_metrics: Métricas compostas (FASE 2 output)
            chart_spec: Especificação do gráfico (filtros, dimensões, etc.)
            analytics_metadata: Metadados do analytics_executor

        Returns:
            Prompt estruturado completo para a LLM

        Raises:
            ValueError: Se parâmetros obrigatórios estiverem ausentes
        """
        # Validação de entrada
        if not enriched_intent:
            raise ValueError("enriched_intent is required")
        if not composed_metrics:
            raise ValueError("composed_metrics is required")

        logger.info(
            "[DynamicPromptBuilder] Building prompt for intent=%s, polarity=%s",
            enriched_intent.base_intent,
            enriched_intent.polarity.value,
        )

        # 1. Selecionar persona apropriada
        persona = self._select_persona(enriched_intent)

        # 2. Construir seção de contexto
        context_section = self._build_context_section(
            enriched_intent, chart_spec, analytics_metadata
        )

        # 3. Construir seção de métricas
        metrics_section = self._build_metrics_section(composed_metrics)

        # 4. Construir seção de tarefa
        task_section = self._build_task_section(enriched_intent)

        # 5. Montar prompt final
        full_prompt = f"""
{persona}

{context_section}

{metrics_section}

{self.format_rules}

{task_section}
"""

        logger.info(
            "[DynamicPromptBuilder] Built prompt with %d characters", len(full_prompt)
        )

        return full_prompt.strip()

    def _select_persona(self, enriched_intent: EnrichedIntent) -> str:
        """
        Seleciona persona baseada em intent + polarity + temporal_focus.

        Logic:
            1. Check base_intent + polarity combinations first
            2. Fallback to base_intent only
            3. Fallback to generic if no match

        Args:
            enriched_intent: Intent enriquecido

        Returns:
            Persona string apropriada
        """
        base = enriched_intent.base_intent.lower()
        polarity = enriched_intent.polarity

        # Combinações específicas: intent + polarity
        if base == "variation":
            if polarity == Polarity.POSITIVE:
                return self.personas["variation_positive"]
            elif polarity == Polarity.NEGATIVE:
                return self.personas["variation_negative"]
            else:
                # Variation neutral -> use positive as default
                return self.personas["variation_positive"]

        # Ranking -> concentration focus
        if base == "ranking":
            return self.personas["ranking_concentration"]

        # Trend/Temporal -> trend focus
        if base in ["trend", "temporal"]:
            return self.personas["temporal_trend"]

        # Comparison -> gap focus
        if base == "comparison":
            return self.personas["comparison_gap"]

        # Composition -> distribution focus
        if base == "composition":
            return self.personas["composition_distribution"]

        # Distribution -> statistical focus
        if base == "distribution":
            return self.personas["distribution_statistical"]

        # Fallback genérico
        logger.warning(
            "[DynamicPromptBuilder] No specific persona for intent=%s, using generic",
            base,
        )
        return self.personas["generic_analytical"]

    def _build_context_section(
        self,
        enriched_intent: EnrichedIntent,
        chart_spec: Optional[Dict[str, Any]],
        analytics_metadata: Optional[Dict[str, Any]],
    ) -> str:
        """
        Constrói seção de contexto analítico.

        Inclui:
            - Intenção do usuário
            - Filtros aplicados
            - Período temporal
            - Scope da análise

        Args:
            enriched_intent: Intent enriquecido
            chart_spec: Especificação do gráfico
            analytics_metadata: Metadados de execução

        Returns:
            String formatada com contexto
        """
        lines = [
            "═══════════════════════════════════════════════════════════════",
            "🎯 CONTEXTO DA ANÁLISE",
            "═══════════════════════════════════════════════════════════════",
            "",
        ]

        # Intenção e polaridade
        lines.append(f"📌 Intenção: {enriched_intent.base_intent.upper()}")
        lines.append(f"📌 Polaridade: {enriched_intent.polarity.value}")
        lines.append(f"📌 Foco Temporal: {enriched_intent.temporal_focus.value}")
        lines.append(f"📌 Tipo de Comparação: {enriched_intent.comparison_type.value}")
        lines.append("")

        # Filtros aplicados (se disponível)
        if chart_spec and "filters" in chart_spec:
            filters = chart_spec["filters"]
            if filters:
                lines.append("🔍 Filtros Aplicados:")
                for key, value in filters.items():
                    lines.append(f"  - {key}: {value}")
                lines.append("")

        # Período temporal (se disponível)
        if analytics_metadata and "time_range" in analytics_metadata:
            time_range = analytics_metadata["time_range"]
            lines.append(
                f"📅 Período: {time_range.get('start', 'N/A')} a {time_range.get('end', 'N/A')}"
            )
            lines.append("")

        # Scope da análise (se disponível)
        if analytics_metadata:
            total_rows = analytics_metadata.get("total_rows")
            filtered_rows = analytics_metadata.get("filtered_rows")
            if total_rows and filtered_rows:
                filter_ratio = (filtered_rows / total_rows) * 100
                lines.append(
                    f"📊 Scope: {filtered_rows:,} registros de {total_rows:,} ({filter_ratio:.1f}%)"
                )
                lines.append("")

        # Ângulo narrativo sugerido
        if enriched_intent.narrative_angle:
            lines.append(f"💡 Ângulo Narrativo: {enriched_intent.narrative_angle}")
            lines.append("")

        # Métricas sugeridas
        if enriched_intent.suggested_metrics:
            lines.append(
                f"📈 Métricas Prioritárias: {', '.join(enriched_intent.suggested_metrics)}"
            )
            lines.append("")

        return "\n".join(lines)

    def _build_metrics_section(self, composed_metrics: Dict[str, Any]) -> str:
        """
        Constrói seção de métricas com fórmulas explícitas.

        Formata as métricas calculadas pelo MetricComposer de forma legível
        e auditável, com fórmulas explícitas.

        Args:
            composed_metrics: Métricas compostas (FASE 2 output)

        Returns:
            String formatada com métricas e fórmulas
        """
        lines = [
            "═══════════════════════════════════════════════════════════════",
            "📊 MÉTRICAS CALCULADAS",
            "═══════════════════════════════════════════════════════════════",
            "",
            "⚠️  IMPORTANTE: Use APENAS as métricas listadas abaixo.",
            "⚠️  NÃO invente ou infira métricas não presentes.",
            "",
        ]

        # Agrupa métricas por módulo
        modules = {}
        for key, value in composed_metrics.items():
            # Skip campos de contexto
            if key in ["_metadata", "chart_type", "intent"]:
                continue

            # Detecta módulo baseado em prefixo ou key
            module_name = self._detect_module_name(key)
            if module_name not in modules:
                modules[module_name] = []
            modules[module_name].append((key, value))

        # Formata por módulo
        for module_name, metrics in modules.items():
            lines.append(f"### {module_name.upper()}")
            lines.append("")
            for key, value in metrics:
                formatted = self._format_metric_value(key, value)
                lines.append(f"  {formatted}")
            lines.append("")

        return "\n".join(lines)

    def _detect_module_name(self, metric_key: str) -> str:
        """
        Detecta o nome do módulo baseado na chave da métrica.

        Args:
            metric_key: Chave da métrica

        Returns:
            Nome do módulo inferido
        """
        key_lower = metric_key.lower()

        if any(
            prefix in key_lower
            for prefix in ["variation", "variacao", "delta", "growth"]
        ):
            return "Variation"
        elif any(
            prefix in key_lower
            for prefix in ["concentration", "concentracao", "top", "hhi"]
        ):
            return "Concentration"
        elif any(prefix in key_lower for prefix in ["gap", "diff", "diferenca"]):
            return "Gap"
        elif any(
            prefix in key_lower
            for prefix in ["temporal", "trend", "tendencia", "momentum"]
        ):
            return "Temporal"
        elif any(
            prefix in key_lower for prefix in ["distribution", "distribuicao", "std"]
        ):
            return "Distribution"
        elif any(
            prefix in key_lower
            for prefix in ["comparative", "comparativo", "ratio", "index"]
        ):
            return "Comparative"
        else:
            return "General"

    def _format_metric_value(self, key: str, value: Any) -> str:
        """
        Formata um valor de métrica para o prompt.

        Args:
            key: Chave da métrica
            value: Valor da métrica

        Returns:
            String formatada
        """
        # Skip valores não numéricos de contexto
        if isinstance(value, (dict, list)):
            return ""

        # Formata valores numéricos
        if isinstance(value, (int, float)):
            if key.endswith("_pct") or "percentual" in key.lower():
                return f"• {key}: {value:.2f}%"
            elif abs(value) >= 1_000_000:
                return f"• {key}: {value / 1_000_000:.2f}M"
            elif abs(value) >= 1_000:
                return f"• {key}: {value:,.0f}"
            else:
                return f"• {key}: {value:.2f}"
        else:
            # String ou outros tipos
            return f"• {key}: {value}"

    def _build_task_section(self, enriched_intent: EnrichedIntent) -> str:
        """
        Constrói seção de tarefa específica para a LLM.

        Define claramente o que a LLM deve fazer com as informações fornecidas.

        Args:
            enriched_intent: Intent enriquecido

        Returns:
            String com instruções de tarefa
        """
        lines = [
            "═══════════════════════════════════════════════════════════════",
            "📝 TAREFA",
            "═══════════════════════════════════════════════════════════════",
            "",
        ]

        # Tarefa principal baseada em intent
        base = enriched_intent.base_intent.lower()

        if base == "variation":
            if enriched_intent.polarity == Polarity.NEGATIVE:
                lines.append(
                    "Gere insights estratégicos focados em: **impacto e riscos da queda observada**"
                )
            else:
                lines.append(
                    "Gere insights estratégicos focados em: **oportunidades e sustentabilidade do crescimento**"
                )
        elif base == "ranking":
            lines.append(
                "Gere insights estratégicos focados em: **concentração, gaps competitivos e riscos de dependência**"
            )
        elif base in ["trend", "temporal"]:
            lines.append(
                "Gere insights estratégicos focados em: **tendência temporal, momentum e projeções**"
            )
        elif base == "comparison":
            lines.append(
                "Gere insights estratégicos focados em: **diferenças relativas e gaps competitivos**"
            )
        elif base == "composition":
            lines.append(
                "Gere insights estratégicos focados em: **composição atual e oportunidades de rebalanceamento**"
            )
        elif base == "distribution":
            lines.append(
                "Gere insights estratégicos focados em: **padrões de distribuição e outliers**"
            )
        else:
            lines.append("Gere insights estratégicos baseados nas métricas fornecidas")

        lines.append("")

        # Métricas prioritárias
        if enriched_intent.suggested_metrics:
            lines.append(
                f"Priorize métricas de: **{', '.join(enriched_intent.suggested_metrics)}**"
            )
            lines.append("")

        # Ângulo narrativo
        if enriched_intent.narrative_angle:
            lines.append(f"Ângulo narrativo: **{enriched_intent.narrative_angle}**")
            lines.append("")

        lines.append("Retorne APENAS o JSON estruturado conforme especificado acima.")
        lines.append("")

        return "\n".join(lines)


# ============================================================================
# Factory function for backward compatibility
# ============================================================================


def build_dynamic_prompt(
    enriched_intent: EnrichedIntent,
    composed_metrics: Dict[str, Any],
    chart_spec: Optional[Dict[str, Any]] = None,
    analytics_metadata: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Factory function para construir prompt dinâmico.

    Esta função fornece uma interface simples para uso em nodes do LangGraph.

    Args:
        enriched_intent: Intent enriquecido (FASE 1)
        composed_metrics: Métricas compostas (FASE 2)
        chart_spec: Especificação do gráfico (opcional)
        analytics_metadata: Metadados de execução (opcional)

    Returns:
        Prompt completo estruturado
    """
    builder = DynamicPromptBuilder()
    return builder.build_prompt(
        enriched_intent, composed_metrics, chart_spec, analytics_metadata
    )


# ============================================================================
# Exports
# ============================================================================

__all__ = [
    "DynamicPromptBuilder",
    "build_dynamic_prompt",
    "ANALYSIS_PERSONAS",
    "FORMAT_RULES",
]
