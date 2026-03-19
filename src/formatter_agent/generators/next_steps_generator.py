"""
NextStepsGenerator - Generates strategic recommendations via LLM
==================================================================

Responsible for:
- Creating exactly 3 direct and actionable strategic next steps
- Providing context-aware recommendations based on insights
- Fallback templates when LLM fails
"""

import json
import logging
import time
from typing import Dict, Any, List

from langchain_google_genai import ChatGoogleGenerativeAI

from ..models.formatter_schemas import NextStepsOutput
from ..core.settings import get_retry_config

logger = logging.getLogger(__name__)


class NextStepsGenerator:
    """
    Generates strategic next steps via Google Gemini LLM.

    Creates exactly 3 direct and actionable recommendations based on:
    - Analysis insights
    - Query context
    - Chart patterns identified
    """

    # Fallback next steps by chart type
    FALLBACK_NEXT_STEPS = {
        "bar_horizontal": [
            "Investigar causas da concentração nos principais items do ranking e desenvolver estratégias de retenção",
            "Avaliar oportunidades de crescimento nos items de menor performance para reduzir dependência",
            "Estabelecer monitoramento contínuo dos top performers para identificar mudanças de padrão rapidamente",
        ],
        "bar_vertical": [
            "Analisar fatores que diferenciam categorias com melhor performance para replicar práticas de sucesso",
            "Estabelecer benchmarks baseados nas categorias líderes para orientar melhorias nas demais",
            "Investigar tendências temporais de cada categoria para identificar oportunidades de crescimento",
        ],
        "line": [
            "Investigar causas de variações significativas no período para melhor compreensão dos drivers temporais",
            "Desenvolver modelo preditivo baseado na tendência identificada para planejamento estratégico",
            "Comparar com períodos equivalentes anteriores para identificar mudanças estruturais relevantes",
        ],
        "pie": [
            "Avaliar oportunidades nos segmentos menores da distribuição para potencial crescimento inexplorado",
            "Desenvolver estratégias específicas para os principais segmentos identificados na análise",
            "Monitorar evolução da distribuição ao longo do tempo para detectar mudanças de concentração",
        ],
    }

    def __init__(self, llm: ChatGoogleGenerativeAI):
        """
        Initialize generator with LLM instance.

        Args:
            llm: Configured ChatGoogleGenerativeAI instance
        """
        self.llm = llm
        self.retry_config = get_retry_config()
        logger.info("NextStepsGenerator initialized")

    def generate(
        self,
        parsed_inputs: Dict[str, Any],
        synthesized_insights: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Generate exactly 3 strategic next steps.

        Args:
            parsed_inputs: Parsed input data for context
            synthesized_insights: Synthesized insights with narrative and key findings

        Returns:
            Dictionary containing:
            {
                "next_steps": List[str] (exactly 3 steps),
                "_fallback_used": bool
            }
        """
        start_time = time.time()

        logger.info("Generating 3 strategic next steps")

        # Build prompt
        prompt = self._build_prompt(synthesized_insights, parsed_inputs)

        # Try LLM generation with retry
        for attempt in range(1, self.retry_config["max_attempts"] + 1):
            try:
                logger.debug(
                    f"LLM call attempt {attempt}/{self.retry_config['max_attempts']}"
                )

                response = self.llm.invoke(prompt)
                result = self._parse_response(response.content)
                result["_fallback_used"] = False

                # Capture tokens from LLM response
                from src.shared_lib.utils.token_tracker import extract_token_usage

                tokens = extract_token_usage(response, self.llm)
                logger.debug(f"[NextStepsGenerator] Tokens used: {tokens}")
                result["_tokens"] = tokens  # Include tokens for aggregation

                execution_time = time.time() - start_time
                logger.info(
                    f"Next steps generated successfully in {execution_time:.2f}s"
                )
                return result

            except Exception as e:
                logger.warning(
                    f"LLM call attempt {attempt} failed: {e}",
                    exc_info=(attempt == self.retry_config["max_attempts"]),
                )

                if attempt < self.retry_config["max_attempts"]:
                    time.sleep(self.retry_config["delay"])
                else:
                    # All retries exhausted, use fallback
                    logger.error(
                        "All LLM attempts failed for next steps. Using fallback."
                    )
                    result = self._fallback_next_steps(parsed_inputs)
                    execution_time = time.time() - start_time
                    logger.info(
                        f"Fallback next steps generated in {execution_time:.2f}s"
                    )
                    return result

    def _build_prompt(
        self,
        synthesized_insights: Dict[str, Any],
        parsed_inputs: Dict[str, Any],
    ) -> str:
        """
        Build LLM prompt for next steps generation.

        Args:
            synthesized_insights: Synthesized insights
            parsed_inputs: Parsed input data

        Returns:
            Formatted prompt string
        """
        query = parsed_inputs.get("query", "")
        key_findings = synthesized_insights.get("key_findings", [])
        narrative = synthesized_insights.get("narrative", "")

        # Extract available dimensions and metrics for context
        chart_spec = parsed_inputs.get("chart_spec", {})
        dimensions = [d.get("name") for d in chart_spec.get("dimensions", [])]
        metrics = [m.get("name") for m in chart_spec.get("metrics", [])]

        # Format key findings
        findings_text = "\n".join([f"  - {finding}" for finding in key_findings])

        prompt = f"""Voce e um consultor estrategico experiente sugerindo proximos passos baseado em analise de dados.

CONTEXTO DA ANALISE:
- Query original: "{query}"
- Dimensoes disponiveis: {", ".join(dimensions) if dimensions else "N/A"}
- Metricas disponiveis: {", ".join(metrics) if metrics else "N/A"}

PRINCIPAIS DESCOBERTAS:
{findings_text}

SINTESE EXECUTIVA:
{narrative}

TAREFA:
Gere EXATAMENTE 3 proximos passos estrategicos diretos e acionaveis em formato JSON.

FORMATO DE SAIDA (JSON):
{{
  "next_steps": [
    "Primeiro proximo passo direto e estrategico",
    "Segundo proximo passo direto e estrategico",
    "Terceiro proximo passo direto e estrategico"
  ]
}}

DIRETRIZES:

1. OBJETIVIDADE:
   - Cada passo deve ser uma sentenca direta e clara (maximo 200 caracteres)
   - Evite verbos genericos como "analisar", "verificar" - seja especifico
   - Foque em acoes concretas e estrategicas

2. COERENCIA:
   - Proximos passos devem ser coerentes com insights e metricas processados
   - Base recomendacoes em dados especificos mencionados (valores, nomes, categorias)
   - Mantenha conexao clara com as descobertas principais

3. ESTRATEGICO E ACIONAVEL:
   - Cada passo deve ter valor estrategico claro
   - Deve ser algo que o usuario pode executar ou implementar
   - Evite obviedades ou generalidades

4. RESTRICOES:
   - Retorne APENAS JSON valido
   - Exatamente 3 proximos passos
   - Nao use emojis
   - Maximo 200 caracteres por passo

EXEMPLO DE SAIDA:
{{
  "next_steps": [
    "Identificar oportunidades de cross-sell e up-sell com os 3 maiores clientes (23700, 2313, 24362) utilizando produtos de maior margem",
    "Avaliar possiveis efeitos de sazonalidade ou campanhas anteriores que impulsionaram o desempenho desses clientes",
    "Examinar a viabilidade de estrategias de fidelizacao para fortalecer a concentracao saudavel no Top 3"
  ]
}}"""

        return prompt

    def _parse_response(self, content: str) -> Dict[str, Any]:
        """
        Parse and validate LLM JSON response.

        Args:
            content: Raw JSON string from LLM

        Returns:
            Validated dictionary with strategic_actions and suggested_analyses

        Raises:
            ValueError: If parsing or validation fails
        """
        try:
            data = json.loads(content)
            validated = NextStepsOutput(**data)
            return validated.model_dump()
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            raise ValueError(f"Invalid JSON from LLM: {e}")
        except Exception as e:
            logger.error(f"Failed to validate LLM response: {e}")
            raise ValueError(f"Validation error: {e}")

    def _fallback_next_steps(self, parsed_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate fallback next steps using templates when LLM fails.

        Args:
            parsed_inputs: Parsed input data

        Returns:
            Dictionary with fallback next steps
        """
        chart_type = parsed_inputs.get("chart_type", "")

        # Try specific chart type first
        if chart_type in self.FALLBACK_NEXT_STEPS:
            steps = self.FALLBACK_NEXT_STEPS[chart_type]
        else:
            # Try base chart type (e.g., bar_vertical for bar_vertical_composed)
            base_type = (
                chart_type.split("_")[0] + "_" + chart_type.split("_")[1]
                if "_" in chart_type
                else chart_type
            )
            if base_type in self.FALLBACK_NEXT_STEPS:
                steps = self.FALLBACK_NEXT_STEPS[base_type]
            else:
                # Generic fallback
                steps = [
                    "Realizar analise detalhada dos padroes identificados para compreender drivers de performance",
                    "Estabelecer metricas de acompanhamento continuo baseadas nos principais indicadores descobertos",
                    "Desenvolver plano de acao focado nos insights de maior impacto estrategico identificados",
                ]

        return {
            "next_steps": steps,
            "_fallback_used": True,
            "_tokens": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
        }
