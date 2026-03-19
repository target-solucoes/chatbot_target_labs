"""
ExecutiveSummaryGenerator - Generates title and introduction via LLM
======================================================================

Responsible for:
- Generating professional, contextual titles (max 80 chars)
- Creating introductory paragraphs with filter context
- Providing fallback templates when LLM fails
- Logging all generation attempts
"""

import json
import logging
import time
from typing import Dict, Any

from langchain_google_genai import ChatGoogleGenerativeAI

from ..models.formatter_schemas import ExecutiveSummaryOutput
from ..core.settings import get_retry_config

logger = logging.getLogger(__name__)


class ExecutiveSummaryGenerator:
    """
    Generates executive summary (title + introduction) via LLM.

    Uses Google Gemini model to create contextual, professional summaries that:
    - Capture the essence of the analysis
    - Mention applied filters
    - Set appropriate tone for the report
    """

    # Fallback title templates by chart type
    FALLBACK_TITLES = {
        "bar_horizontal": "Ranking de {dimension} por {metric}",
        "bar_vertical": "Comparação de {metric} entre {dimension}",
        "bar_vertical_composed": "Análise Comparativa de Múltiplas {metric}",
        "bar_vertical_stacked": "Composição de {metric} por {dimension}",
        "line": "Evolução de {metric} ao Longo do Tempo",
        "line_composed": "Análise Temporal de Múltiplas {metric}",
        "pie": "Distribuição de {metric} por {dimension}",
        "histogram": "Distribuição de Frequência de {metric}",
    }

    def __init__(self, llm: ChatGoogleGenerativeAI):
        """
        Initialize generator with LLM instance.

        Args:
            llm: Configured ChatGoogleGenerativeAI instance
        """
        self.llm = llm
        self.retry_config = get_retry_config()
        logger.info("ExecutiveSummaryGenerator initialized")

    def generate(
        self,
        parsed_inputs: Dict[str, Any],
        handler_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Generate executive summary via LLM with fallback support.

        Args:
            parsed_inputs: Parsed data from InputParser (query, filters, data, etc.)
            handler_context: Chart-specific context from handler

        Returns:
            Dictionary containing:
            {
                "title": str,
                "introduction": str,
                "subtitle": str (optional),
                "filters_applied_description": str,
                "_fallback_used": bool (if fallback was triggered)
            }
        """
        start_time = time.time()
        chart_type = parsed_inputs.get("chart_type", "")

        logger.info(f"Generating executive summary for chart_type='{chart_type}'")

        # Build prompt
        prompt = self._build_prompt(parsed_inputs, handler_context)

        # Try LLM generation with retry
        for attempt in range(1, self.retry_config["max_attempts"] + 1):
            try:
                logger.debug(
                    f"LLM call attempt {attempt}/{self.retry_config['max_attempts']}"
                )

                response = self.llm.invoke(prompt)
                result = self._parse_response(response.content)

                # Capture tokens from LLM response
                from src.shared_lib.utils.token_tracker import extract_token_usage

                tokens = extract_token_usage(response, self.llm)
                logger.debug(f"[ExecutiveSummary] Tokens used: {tokens}")

                # Add additional context
                result["subtitle"] = parsed_inputs.get("query", "")
                result["filters_applied_description"] = self._get_filter_description(
                    parsed_inputs.get("filters", {})
                )
                result["_fallback_used"] = False
                result["_tokens"] = tokens  # Include tokens for aggregation

                execution_time = time.time() - start_time
                logger.info(
                    f"Executive summary generated successfully in {execution_time:.2f}s"
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
                        "All LLM attempts failed for executive summary. Using fallback."
                    )
                    result = self._fallback_summary(parsed_inputs, handler_context)
                    execution_time = time.time() - start_time
                    logger.info(f"Fallback summary generated in {execution_time:.2f}s")
                    return result

    def _build_prompt(
        self,
        parsed_inputs: Dict[str, Any],
        handler_context: Dict[str, Any],
    ) -> str:
        """
        Build LLM prompt for executive summary generation.

        Args:
            parsed_inputs: Parsed input data
            handler_context: Chart-specific context

        Returns:
            Formatted prompt string
        """
        query = parsed_inputs.get("query", "")
        chart_desc = handler_context.get("chart_type_description", "análise")
        filters = parsed_inputs.get("filters", {})
        data = parsed_inputs.get("data", [])
        data_preview = data[:3] if data else []

        # Extract metric and dimension info
        chart_spec = parsed_inputs.get("chart_spec", {})
        metrics = chart_spec.get("metrics", [{}])
        dimensions = chart_spec.get("dimensions", [{}])

        metric_alias = metrics[0].get("alias", "Métrica") if metrics else "Métrica"
        dimension_alias = (
            dimensions[0].get("alias", "Dimensão") if dimensions else "Dimensão"
        )

        # Format filter description
        filter_desc = self._format_filters(filters)

        # Format data preview
        data_preview_str = self._format_data_preview(data_preview)

        # Calculate total records
        total_records = len(data)

        prompt = f"""Você é um analista de dados experiente gerando um executive summary profissional.

CONTEXTO DA ANÁLISE:
- Query do usuário: "{query}"
- Tipo de análise: {chart_desc}
- Filtros aplicados: {filter_desc}
- Métrica principal: {metric_alias}
- Dimensão analisada: {dimension_alias}
- Total de registros: {total_records}

PREVIEW DOS DADOS:
{data_preview_str}

TAREFA:
Gere um JSON com um título e introdução profissionais para esta análise.

FORMATO DE SAÍDA (JSON):
{{
  "title": "Título profissional e conciso (máximo 80 caracteres) que capture a essência da análise",
  "introduction": "Parágrafo introdutório (2-3 sentenças, 50-300 caracteres) contextualizando a análise e mencionando filtros aplicados quando relevante"
}}

DIRETRIZES:
1. TÍTULO:
   - Máximo 80 caracteres
   - Específico e acionável
   - Capture a essência da análise
   - Tom profissional e objetivo
   - Exemplo: "Análise de Concentração de Faturamento no Estado de São Paulo"

2. INTRODUÇÃO:
   - Entre 50 e 300 caracteres
   - 2-3 sentenças claras e diretas
   - Contextualize a análise
   - Mencione filtros aplicados quando relevante
   - Indique o escopo dos dados analisados
   - Tom profissional mas acessível

3. RESTRIÇÕES:
   - Retorne APENAS JSON válido
   - Não use emojis
   - Não use jargão excessivo
   - Seja objetivo e direto

EXEMPLO DE SAÍDA:
{{
  "title": "Top 5 Clientes por Faturamento em São Paulo - 2015",
  "introduction": "Esta análise apresenta os cinco principais clientes por volume de faturamento no estado de São Paulo durante o ano de 2015. O ranking permite identificar concentrações e oportunidades estratégicas de relacionamento comercial."
}}"""

        return prompt

    def _parse_response(self, content: str) -> Dict[str, Any]:
        """
        Parse and validate LLM JSON response.

        Args:
            content: Raw JSON string from LLM

        Returns:
            Validated dictionary with title and introduction

        Raises:
            ValueError: If parsing or validation fails
        """
        try:
            data = json.loads(content)
            validated = ExecutiveSummaryOutput(**data)
            return validated.model_dump()
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            raise ValueError(f"Invalid JSON from LLM: {e}")
        except Exception as e:
            logger.error(f"Failed to validate LLM response: {e}")
            raise ValueError(f"Validation error: {e}")

    def _fallback_summary(
        self,
        parsed_inputs: Dict[str, Any],
        handler_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Generate fallback summary using templates when LLM fails.

        Args:
            parsed_inputs: Parsed input data
            handler_context: Chart-specific context

        Returns:
            Dictionary with fallback title and introduction
        """
        chart_type = parsed_inputs.get("chart_type", "")
        chart_spec = parsed_inputs.get("chart_spec", {})

        # Extract metric and dimension
        metrics = chart_spec.get("metrics", [{}])
        dimensions = chart_spec.get("dimensions", [{}])

        metric_alias = metrics[0].get("alias", "Métrica") if metrics else "Métrica"
        dimension_alias = (
            dimensions[0].get("alias", "Dimensão") if dimensions else "Dimensão"
        )

        # Generate title from template
        template = self.FALLBACK_TITLES.get(
            chart_type, "Análise de {metric} por {dimension}"
        )
        title = template.format(metric=metric_alias, dimension=dimension_alias)

        # Generate introduction
        filters = parsed_inputs.get("filters", {})
        filter_desc = self._get_filter_description(filters)
        total_records = len(parsed_inputs.get("data", []))

        chart_desc = handler_context.get("chart_type_description", "dados")

        intro = f"Esta análise apresenta {chart_desc} com base nos dados disponíveis. "
        if filters:
            intro += f"{filter_desc}. "
        intro += f"Total de {total_records} registros analisados."

        return {
            "title": title[:80],  # Ensure max length
            "introduction": intro,
            "subtitle": parsed_inputs.get("query", ""),
            "filters_applied_description": filter_desc,
            "_fallback_used": True,
            "_tokens": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
        }

    def _format_filters(self, filters: Dict[str, Any]) -> str:
        """
        Format filters dictionary into readable description.

        Args:
            filters: Dictionary of applied filters

        Returns:
            Formatted string description
        """
        if not filters:
            return "Nenhum filtro aplicado"

        descriptions = []
        for key, value in filters.items():
            if isinstance(value, list):
                descriptions.append(f"{key} em [{', '.join(map(str, value))}]")
            elif isinstance(value, dict):
                # Handle complex filters like date ranges
                if "between" in value:
                    descriptions.append(
                        f"{key} entre {value['between'][0]} e {value['between'][1]}"
                    )
                else:
                    descriptions.append(f"{key}: {value}")
            else:
                descriptions.append(f"{key} = {value}")

        return "; ".join(descriptions)

    def _format_data_preview(self, data: list[Dict[str, Any]]) -> str:
        """
        Format data preview for prompt.

        Args:
            data: List of data dictionaries (max 3)

        Returns:
            Formatted string with data preview
        """
        if not data:
            return "(sem dados disponíveis)"

        lines = []
        for row in data:
            row_str = ", ".join([f"{k}: {v}" for k, v in row.items()])
            lines.append(f"  - {row_str}")

        return "\n".join(lines)

    def _get_filter_description(self, filters: Dict[str, Any]) -> str:
        """
        Get human-readable filter description.

        Args:
            filters: Dictionary of applied filters

        Returns:
            Human-readable description
        """
        if not filters:
            return "Sem filtros aplicados"

        descriptions = []
        for key, value in filters.items():
            if isinstance(value, dict) and "between" in value:
                descriptions.append(
                    f"{key} entre {value['between'][0]} e {value['between'][1]}"
                )
            elif isinstance(value, list):
                descriptions.append(f"{key} em {', '.join(map(str, value))}")
            else:
                descriptions.append(f"{key}: {value}")

        return "Filtros aplicados: " + "; ".join(descriptions)
