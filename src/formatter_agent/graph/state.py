"""
FormatterState - State schema for formatter agent workflow
===========================================================

Defines the TypedDict schema for the formatter agent's LangGraph workflow state.
Includes inputs from all previous agents and internal processing states.
"""

from typing import TypedDict, Dict, Any, Optional


class FormatterState(TypedDict, total=False):
    """
    State schema for the formatter agent workflow.

    This state consolidates inputs from all previous agents in the pipeline
    and tracks the formatter's internal processing steps.

    **PARALLEL EXECUTION:**
    The workflow executes generate_executive_summary and synthesize_insights
    nodes in PARALLEL for performance optimization. Both nodes depend only on
    parsed_inputs and write to independent fields (executive_summary and
    synthesized_insights respectively).

    The generate_next_steps node runs SEQUENTIALLY after both parallel nodes
    complete, as it depends on synthesized_insights.

    Attributes:
        INPUTS (from previous agents):
            query: Original user query string
            chart_type: Chart type identifier (bar_horizontal, line, pie, etc.)
            filter_final: Final filters from filter_classifier
            chart_spec: Complete chart specification from graphic_classifier
            analytics_result: Processed data from analytics_executor
            plotly_result: Generated chart from plotly_generator
            insight_result: Generated insights from insight_generator

        INTERNAL PROCESSING:
            parsed_inputs: Validated and structured inputs
            chart_handler: Name of the handler being used
            executive_summary: LLM-generated title and introduction (PARALLEL)
            synthesized_insights: LLM-generated narrative synthesis (PARALLEL)
            next_steps: LLM-generated strategic recommendations (SEQUENTIAL)
            formatted_data_table: Formatted data table (markdown/HTML)

        OUTPUT:
            formatter_output: Final structured JSON output
            status: Processing status (success/error)
            error: Error message if any
    """

    # ==================== INPUTS FROM PREVIOUS AGENTS ====================

    # Core inputs
    query: str
    chart_type: str

    # From filter_classifier (Phase 0)
    filter_final: Dict[str, Any]

    # From graphic_classifier (Phase 1)
    chart_spec: Dict[str, Any]  # Complete ChartOutput

    # From analytics_executor (Phase 2)
    analytics_result: Dict[str, Any]  # Complete AnalyticsOutput

    # From plotly_generator (Phase 3a)
    plotly_result: Dict[str, Any]

    # From insight_generator (Phase 3b)
    insight_result: Dict[str, Any]

    # Token tracking propagated from pipeline orchestrator
    # agent_name -> {input_tokens, output_tokens, total_tokens}
    agent_tokens: Dict[str, Dict[str, int]]

    # ==================== INTERNAL PROCESSING ====================

    # Parsed and validated inputs
    parsed_inputs: Dict[str, Any]

    # Handler selection
    chart_handler: str

    # LLM-generated outputs
    executive_summary: Dict[str, Any]
    synthesized_insights: Dict[str, Any]
    next_steps: Dict[str, Any]

    # Formatted outputs
    formatted_data_table: str

    # ==================== FINAL OUTPUT ====================

    # Complete structured JSON output
    formatter_output: Dict[str, Any]

    # Status tracking
    status: str
    error: Optional[str]
