"""
Formatter Agent Entry Point
============================

Main entry point for the formatter agent (Phase 4 of the pipeline).

This agent:
1. Consolidates outputs from all previous agents
2. Generates executive summary via LLM
3. Synthesizes insights into cohesive narrative via LLM
4. Creates strategic recommendations via LLM
5. Formats data tables
6. Assembles structured JSON output
"""

import logging
from typing import Dict, Any

from .graph.workflow import create_formatter_workflow, execute_formatter_workflow
from .graph.state import FormatterState

logger = logging.getLogger(__name__)


def run_formatter(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute the formatter agent workflow.

    This is the main entry point for Phase 4 - the formatter agent.
    It orchestrates the complete formatting workflow including:
    - Input validation
    - Handler selection
    - LLM-powered content generation
    - Data formatting
    - Output assembly

    Args:
        state: FormatterState dictionary with inputs from all previous agents.
               Expected keys:
               - query: str
               - chart_type: str
               - filter_final: Dict (from filter_classifier)
               - chart_spec: Dict (from graphic_classifier)
               - analytics_result: Dict (from analytics_executor)
               - plotly_result: Dict (from plotly_generator)
               - insight_result: Dict (from insight_generator)

    Returns:
        Structured JSON output with formatted results:
        {
            "status": "success" | "error",
            "format_version": "1.0.0",
            "timestamp": str,
            "executive_summary": {...},
            "visualization": {...},
            "insights": {...},
            "next_steps": {...},
            "data": {...},
            "metadata": {...}
        }

    Example:
        >>> state = {
        ...     "query": "top 5 clientes por faturamento",
        ...     "chart_type": "bar_horizontal",
        ...     "filter_final": {"UF_Cliente": "SP"},
        ...     "chart_spec": {...},
        ...     "analytics_result": {...},
        ...     "plotly_result": {...},
        ...     "insight_result": {...}
        ... }
        >>> result = run_formatter(state)
        >>> print(result["status"])
        "success"
    """
    logger.info("=== FORMATTER AGENT STARTED ===")
    logger.info(f"Query: '{state.get('query', 'N/A')}'")
    logger.info(f"Chart Type: '{state.get('chart_type', 'N/A')}'")

    try:
        # Create workflow (cached in production for performance)
        workflow = create_formatter_workflow(verbose=False)

        # Execute workflow
        result_state = execute_formatter_workflow(state, workflow=workflow)

        # Extract formatter output
        formatter_output = result_state.get("formatter_output", {})
        final_status = result_state.get("status", "unknown")

        logger.info(f"=== FORMATTER AGENT COMPLETED === Status: {final_status}")

        return formatter_output

    except Exception as e:
        logger.error(f"Formatter agent failed with exception: {e}", exc_info=True)

        # Return error output
        return {
            "status": "error",
            "format_version": "1.0.0",
            "error": {
                "message": f"Formatter agent failed: {str(e)}",
                "recovery": "none",
                "critical": True,
            },
            "executive_summary": {
                "title": "Erro no Processamento",
                "introduction": "Não foi possível completar a formatação dos resultados.",
            },
        }
