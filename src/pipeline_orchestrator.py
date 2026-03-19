"""
Pipeline Integration Utilities - Complete 4-Phase Pipeline

This module provides utilities for seamless integration between:
- Phase 0: filter_classifier (filter management) - with conditional execution
- Phase 1: graphic_classifier (chart classification)
- Phase 2: analytics_executor (data processing)
- Phase 3: insight_generator + plotly_generator (parallel execution)
- Phase 4: formatter_agent (structured output generation)

Features:
- Integrated workflow execution across all phases
- Filter management with conversational context
- Parallel execution of insights and visualization
- LLM-powered formatting and synthesis
- Convenience functions for common use cases
- Pipeline result validation and extraction
- Performance tracking across all phases
- Conditional filter execution (optimized - skips when unnecessary)
"""

import logging
import time
from typing import Dict, Any, Optional, Tuple
from pathlib import Path

from langgraph.graph import StateGraph, END

# Import query analyzer for conditional filter execution
from src.shared_lib.utils.query_analyzer import needs_filter_classification

# Import filter_classifier components
from src.filter_classifier.graph.workflow import create_filter_workflow
from src.filter_classifier.graph.nodes import (
    parse_filter_query,
    load_filter_context,
    validate_detected_values,  # FASE 1 FIX - add validate step
    expand_temporal_periods_node,  # FASE 1 FIX - add expansion node
    validate_filter_columns as validate_filter_columns_node,
    identify_filter_operations,
    apply_filter_operations,
    persist_filters,
    format_filter_output,
)
from src.filter_classifier.graph.edges import (
    should_validate_filters,
    has_validation_errors,
)
from src.filter_classifier.models.filter_state import FilterGraphState

# Import graphic_classifier components
from src.graphic_classifier.graph.nodes import (
    extract_semantic_anchor_node,
    validate_semantic_anchor_node,
    map_semantic_to_chart_node,
    parse_query_node,
    load_dataset_metadata_node,
    detect_keywords_node,
    classify_intent_node,
    map_columns_node,
    generate_output_node,
    execute_analytics_node,
)
from src.graphic_classifier.graph.edges import should_map_columns

# Import plotly_generator components (Phase 4)
from src.plotly_generator.plotly_generator_agent import PlotlyGeneratorAgent

from src.shared_lib.utils.logger import get_logger
from src.shared_lib.utils.performance_monitor import PerformanceMonitor

logger = get_logger(__name__)


class IntegratedPipelineResult:
    """
    Result container for integrated pipeline execution.

    This class provides convenient access to results from both phases
    of the pipeline and includes performance metrics.
    """

    def __init__(self, state: Dict[str, Any], execution_time: float):
        """
        Initialize result container.

        Args:
            state: Final GraphState after pipeline execution
            execution_time: Total execution time in seconds
        """
        self._state = state
        self._execution_time = execution_time

    @property
    def query(self) -> str:
        """Original user query."""
        return self._state.get("query", "")

    @property
    def classifier_output(self) -> Dict[str, Any]:
        """Output from Phase 1 (Classifier)."""
        return self._state.get("output", {})

    @property
    def executor_output(self) -> Optional[Dict[str, Any]]:
        """Output from Phase 2 (Executor). None if executor wasn't run."""
        return self._state.get("executor_output")

    @property
    def plotly_output(self) -> Optional[Dict[str, Any]]:
        """Output from Phase 3 (Plotly Generator). None if generator wasn't run."""
        return self._state.get("plotly_output")

    @property
    def insight_result(self) -> Optional[Dict[str, Any]]:
        """Output from Phase 3 (Insight Generator). None if generator wasn't run."""
        return self._state.get("insight_result")

    @property
    def insights(self) -> list:
        """List of insights generated (empty if not run or failed)."""
        insight_result = self.insight_result
        if insight_result and isinstance(insight_result, dict):
            return insight_result.get("insights", [])
        return []

    @property
    def formatted_insights(self) -> str:
        """Executive markdown-formatted insights (empty string if not available)."""
        insight_result = self.insight_result
        if insight_result and isinstance(insight_result, dict):
            return insight_result.get("formatted_insights", "")
        return ""

    @property
    def final_output(self) -> Optional[Dict[str, Any]]:
        """Merged output from insight_generator and plotly_generator (if merge node was used)."""
        return self._state.get("final_output")

    @property
    def formatter_output(self) -> Optional[Dict[str, Any]]:
        """Output from formatter agent (Phase 4). None if formatter wasn't run."""
        return self._state.get("formatter_output")

    @property
    def chart_type(self) -> Optional[str]:
        """Detected chart type."""
        if self.classifier_output:
            return self.classifier_output.get("chart_type")
        return None

    @property
    def non_graph_output(self) -> Optional[Dict[str, Any]]:
        """Output from non_graph_executor (None if not executed)."""
        return self._state.get("non_graph_output")

    @property
    def is_non_graph_query(self) -> bool:
        """Check if query was handled by non_graph_executor."""
        return self.non_graph_output is not None

    @property
    def query_type(self) -> Optional[str]:
        """
        Type of query executed.

        Returns:
            - For graph queries: chart_type
            - For non-graph queries: query_type from non_graph_output
        """
        if self.is_non_graph_query:
            return self.non_graph_output.get("query_type")
        return self.chart_type

    @property
    def intent(self) -> str:
        """Classified user intent."""
        return self._state.get("intent", "unknown")

    @property
    def confidence(self) -> float:
        """Classification confidence (0.0 to 1.0)."""
        return self._state.get("confidence", 0.0)

    @property
    def data(self) -> list:
        """Processed data from executor (empty list if no execution)."""
        executor_out = self.executor_output
        if executor_out and isinstance(executor_out, dict):
            return executor_out.get("data", [])
        return []

    @property
    def plotly_config(self) -> Dict[str, Any]:
        """Plotly configuration for visualization."""
        # Prefer plotly_generator output if available
        plotly_out = self.plotly_output
        if (
            plotly_out
            and isinstance(plotly_out, dict)
            and plotly_out.get("status") == "success"
        ):
            return plotly_out.get("config", {})
        # Fallback to executor's plotly_config
        executor_out = self.executor_output
        if executor_out and isinstance(executor_out, dict):
            return executor_out.get("plotly_config", {})
        return {}

    @property
    def plotly_figure(self):
        """Plotly Figure object (if plotly_generator was run)."""
        plotly_out = self.plotly_output
        if plotly_out and isinstance(plotly_out, dict):
            return plotly_out.get("figure")
        return None

    @property
    def plotly_html(self) -> Optional[str]:
        """Rendered HTML of chart (if plotly_generator was run)."""
        plotly_out = self.plotly_output
        if plotly_out and isinstance(plotly_out, dict):
            return plotly_out.get("html")
        return None

    @property
    def plotly_file_path(self) -> Optional[str]:
        """File path of saved chart (if plotly_generator was run)."""
        plotly_out = self.plotly_output
        if plotly_out and isinstance(plotly_out, dict):
            return plotly_out.get("file_path")
        return None

    @property
    def execution_time(self) -> float:
        """Total pipeline execution time in seconds."""
        return self._execution_time

    @property
    def engine_used(self) -> Optional[str]:
        """Execution engine used (DuckDB/Pandas). None if no execution."""
        return self._state.get("engine_used")

    @property
    def active_filters(self) -> Dict[str, Any]:
        """Active filters from filter_classifier (empty dict if not used)."""
        return self._state.get("filter_final", {})

    @property
    def filter_operations(self) -> Dict[str, Any]:
        """CRUD operations performed on filters (empty dict if not used)."""
        filter_output = self._state.get("output", {})
        if isinstance(filter_output, dict):
            return {
                "ADICIONAR": filter_output.get("ADICIONAR", {}),
                "ALTERAR": filter_output.get("ALTERAR", {}),
                "REMOVER": filter_output.get("REMOVER", {}),
                "MANTER": filter_output.get("MANTER", {}),
            }
        return {}

    @property
    def status(self) -> str:
        """
        Overall pipeline status.

        Returns:
            'success': Pipeline completed successfully
            'partial_success': Classifier succeeded but executor failed
            'skipped': No chart type detected (executor skipped)
            'error': Pipeline failed
        """
        executor_out = self.executor_output
        if executor_out and isinstance(executor_out, dict):
            exec_status = executor_out.get("status")
            if exec_status == "success":
                return "success"
            elif exec_status == "skipped":
                return "skipped"
            elif exec_status == "error":
                return "partial_success" if self.chart_type else "error"
        elif self.chart_type:
            return "partial_success"  # Classifier succeeded, no executor

        return "error"

    @property
    def errors(self) -> list:
        """List of errors encountered during pipeline execution."""
        errors = self._state.get("errors", [])

        # Add executor errors if present
        executor_out = self.executor_output
        if (
            executor_out
            and isinstance(executor_out, dict)
            and executor_out.get("status") == "error"
        ):
            executor_error = executor_out.get("error", {})
            if isinstance(executor_error, dict):
                error_msg = executor_error.get("message", "Unknown executor error")
                errors.append(f"Executor: {error_msg}")

        return errors

    @property
    def has_data(self) -> bool:
        """Check if pipeline produced data."""
        return len(self.data) > 0

    @property
    def agent_tokens(self) -> Dict[str, Dict[str, int]]:
        """
        Token usage por agente individual.

        Returns:
            Dict com agent_name -> {input_tokens, output_tokens, total_tokens}
        """
        return self._state.get("agent_tokens", {})

    @property
    def total_tokens(self) -> Dict[str, int]:
        """
        Total de tokens agregado de todos os agentes.

        Returns:
            Dict com input_tokens, output_tokens, total_tokens
        """
        agent_tokens_data = self.agent_tokens
        if not agent_tokens_data:
            return {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}

        total = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
        for agent_tokens in agent_tokens_data.values():
            total["input_tokens"] += agent_tokens.get("input_tokens", 0)
            total["output_tokens"] += agent_tokens.get("output_tokens", 0)
            total["total_tokens"] += agent_tokens.get("total_tokens", 0)

        return total

    def get_state(self) -> Dict[str, Any]:
        """Get the complete raw state for advanced use cases."""
        return self._state

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert result to dictionary format.

        Returns:
            Dictionary with all pipeline results including filter, plotly, and insights information
        """
        try:
            return {
                "query": self.query,
                "status": self.status,
                "intent": self.intent,
                "chart_type": self.chart_type,
                "confidence": self.confidence,
                "active_filters": self.active_filters,
                "filter_operations": self.filter_operations,
                "classifier_output": self.classifier_output,
                "executor_output": self.executor_output,
                "plotly_output": self.plotly_output,
                "insight_result": self.insight_result,
                "insights": self.insights,
                "formatted_insights": self.formatted_insights,
                "final_output": self.final_output,
                "formatter_output": self.formatter_output,
                "non_graph_output": self.non_graph_output,
                "is_non_graph_query": self.is_non_graph_query,
                "query_type": self.query_type,
                "data": self.data,
                "plotly_config": self.plotly_config,
                "plotly_file_path": self.plotly_file_path,
                "errors": self.errors,
                "execution_time": self.execution_time,
                "engine_used": self.engine_used,
            }
        except Exception as e:
            logger.error(f"Error in to_dict(): {e}", exc_info=True)
            return {
                "query": self._state.get("query", ""),
                "status": "error",
                "error_message": f"Error converting result to dict: {str(e)}",
                "execution_time": self._execution_time,
            }

    def __repr__(self) -> str:
        """String representation of the result."""
        return (
            f"IntegratedPipelineResult("
            f"status={self.status}, "
            f"chart_type={self.chart_type}, "
            f"rows={len(self.data)}, "
            f"time={self.execution_time:.3f}s"
            f")"
        )


def route_after_classifier(state: FilterGraphState) -> str:
    """
    Roteamento após graph_classifier.

    FASE 4 - CORREÇÃO CRÍTICA: requires_tabular_data NÃO significa "sem gráfico".
    Significa que o gráfico precisa de dados em formato tabular/pivotado.

    Lógica CORRIGIDA:
    - Se chart_type is None → non_graph_executor (query não-gráfica)
    - Senão → execute_analytics (incluindo temporal_comparison_analysis)

    Exemplos:
    - "qual o total de vendas?" → chart_type=None → non_graph
    - "produtos de maio para junho" → chart_type=bar_vertical_composed,
      requires_tabular_data=True → analytics (precisa pivotar dados)

    Args:
        state: Pipeline state após classificação

    Returns:
        "non_graph" ou "analytics"
    """
    chart_type = state.get("output", {}).get("chart_type")
    requires_tabular = state.get("output", {}).get("requires_tabular_data", False)
    intent = state.get("output", {}).get("intent", "")

    # CORREÇÃO: Apenas chart_type=None vai para non_graph
    if chart_type is None:
        logger.info("Routing to non_graph_executor (no chart type detected)")
        return "non_graph"
    else:
        # Qualquer query com chart_type vai para analytics, incluindo as que precisam de dados tabulares
        logger.info(
            f"Routing to analytics_executor (chart_type={chart_type}, "
            f"intent={intent}, requires_tabular={requires_tabular})"
        )
        return "analytics"


def create_full_integrated_workflow(
    include_filter_classifier: bool = True,
    include_executor: bool = True,
    verbose: bool = False,
):
    """
    Create fully integrated workflow: filter_classifier → graphic_classifier → analytics_executor.

    This creates the complete 3-phase pipeline with optional components:
    - Phase 0 (optional): Filter classification and management
    - Phase 1: Chart classification
    - Phase 2 (optional): Analytics execution

    Workflow Structure (all phases enabled):
    ```
    START
      ↓
    [FILTER CLASSIFIER]
      load_filter_context
        ↓
      parse_filter_query
        ↓
      [should_validate_filters?]
        ├─ validate → validate_filter_columns → identify_filter_operations
        └─ skip → identify_filter_operations
          ↓
      apply_filter_operations
        ↓
      persist_filters
        ↓
      format_filter_output
      ↓
    [GRAPHIC CLASSIFIER]
      parse_query
        ↓
      load_dataset_metadata
        ↓
      detect_keywords
        ↓
      classify_intent
        ↓
      [should_map_columns?]
        ├─ map → map_columns → generate_output
        └─ no_chart → generate_output
      ↓
    [ANALYTICS EXECUTOR]
      execute_analytics
        ↓
      END
    ```

    Args:
        include_filter_classifier: Include Phase 0 (filter management)
        include_executor: Include Phase 2 (analytics execution)
        verbose: Enable verbose logging

    Returns:
        Compiled StateGraph ready for execution

    Example:
        >>> # Full pipeline with filters
        >>> workflow = create_full_integrated_workflow()
        >>> state = initialize_full_pipeline_state("Vendas de SP em 2020")
        >>> result = workflow.invoke(state)
        >>> print(result["filter_final"])  # {'UF_Cliente': 'SP', 'Ano': 2020}
        >>> print(result["output"]["chart_type"])  # 'bar_horizontal'
        >>> print(result["executor_output"]["status"])  # 'success'
    """
    logger.info(
        f"Creating full integrated workflow: "
        f"filter_classifier={include_filter_classifier}, "
        f"executor={include_executor}"
    )

    # Create the state graph with FilterGraphState (extends GraphState)
    workflow = StateGraph(FilterGraphState)

    # ========================================================================
    # ADD NODES - Phase 0 (Filter Classifier)
    # ========================================================================

    if include_filter_classifier:
        logger.debug("Adding Phase 0 (filter_classifier) nodes")
        workflow.add_node("load_filter_context", load_filter_context)
        workflow.add_node("parse_filter_query", parse_filter_query)
        workflow.add_node(
            "validate_detected_values", validate_detected_values
        )  # FASE 1 FIX
        workflow.add_node(
            "expand_temporal_periods", expand_temporal_periods_node
        )  # FASE 1 FIX
        workflow.add_node("validate_filter_columns", validate_filter_columns_node)
        workflow.add_node("identify_filter_operations", identify_filter_operations)
        workflow.add_node("apply_filter_operations", apply_filter_operations)
        workflow.add_node("persist_filters", persist_filters)
        workflow.add_node("format_filter_output", format_filter_output)

    # ========================================================================
    # ADD NODES - Phase 1 (Graphic Classifier)
    # ========================================================================

    logger.debug("Adding Phase 1 (graphic_classifier) nodes")
    # FASE 1: Semantic-First Architecture (CRITICAL - MUST execute first)
    workflow.add_node("extract_semantic_anchor", extract_semantic_anchor_node)
    workflow.add_node("validate_semantic_anchor", validate_semantic_anchor_node)
    workflow.add_node("map_semantic_to_chart", map_semantic_to_chart_node)

    # Legacy nodes (subordinate to semantic layer)
    workflow.add_node("parse_query", parse_query_node)
    workflow.add_node("load_dataset_metadata", load_dataset_metadata_node)
    workflow.add_node("detect_keywords", detect_keywords_node)
    workflow.add_node("classify_intent", classify_intent_node)
    workflow.add_node("map_columns", map_columns_node)
    workflow.add_node("generate_output", generate_output_node)

    # ========================================================================
    # ADD NODES - Phase 2 (Analytics Executor)
    # ========================================================================

    if include_executor:
        logger.debug("Adding Phase 2 (analytics_executor) nodes")
        workflow.add_node("execute_analytics", execute_analytics_node)

    # ========================================================================
    # ADD NODES - Phase 2-alt (Non-Graph Executor)
    # ========================================================================

    logger.debug("Adding Phase 2-alt (non_graph_executor) node")
    workflow.add_node("non_graph_executor", non_graph_executor_node)

    # ========================================================================
    # ADD EDGES - Phase 0 (Filter Classifier)
    # ========================================================================

    if include_filter_classifier:
        logger.debug("Adding Phase 0 edges")

        # Set entry point at filter_classifier
        workflow.set_entry_point("load_filter_context")

        # Filter workflow edges (FASE 1 FIX - add validate_detected_values and expand_temporal_periods)
        workflow.add_edge("load_filter_context", "parse_filter_query")
        workflow.add_edge(
            "parse_filter_query", "validate_detected_values"
        )  # FASE 1 FIX
        workflow.add_edge(
            "validate_detected_values", "expand_temporal_periods"
        )  # FASE 1 FIX

        workflow.add_conditional_edges(
            "expand_temporal_periods",  # FASE 1 FIX - changed from parse_filter_query
            should_validate_filters,
            {
                "validate": "validate_filter_columns",
                "skip": "identify_filter_operations",
            },
        )

        workflow.add_conditional_edges(
            "validate_filter_columns",
            has_validation_errors,
            {"error": "format_filter_output", "continue": "identify_filter_operations"},
        )

        workflow.add_edge("identify_filter_operations", "apply_filter_operations")
        workflow.add_edge("apply_filter_operations", "persist_filters")
        workflow.add_edge("persist_filters", "format_filter_output")

        # Connect filter_classifier to graphic_classifier
        workflow.add_edge("format_filter_output", "extract_semantic_anchor")
    else:
        # If no filter_classifier, start at graphic_classifier (semantic layer)
        workflow.set_entry_point("extract_semantic_anchor")

    # ========================================================================
    # ADD EDGES - Phase 1 (Graphic Classifier)
    # ========================================================================

    logger.debug("Adding Phase 1 edges")

    # CRITICAL: Semantic-First Pipeline (MUST execute first)
    workflow.add_edge("extract_semantic_anchor", "validate_semantic_anchor")
    workflow.add_edge("validate_semantic_anchor", "map_semantic_to_chart")
    workflow.add_edge("map_semantic_to_chart", "parse_query")

    # Legacy pipeline (subordinate to semantic layer)
    workflow.add_edge("parse_query", "load_dataset_metadata")
    workflow.add_edge("load_dataset_metadata", "detect_keywords")
    workflow.add_edge("detect_keywords", "classify_intent")

    workflow.add_conditional_edges(
        "classify_intent",
        should_map_columns,
        {"map": "map_columns", "no_chart": "generate_output"},
    )

    workflow.add_edge("map_columns", "generate_output")

    # ========================================================================
    # ADD EDGES - Phase 2 (Analytics Executor) & Phase 2-alt (Non-Graph Executor)
    # ========================================================================

    if include_executor:
        logger.debug("Adding Phase 2 edges with conditional routing")

        # Conditional routing após generate_output
        workflow.add_conditional_edges(
            "generate_output",
            route_after_classifier,
            {"non_graph": "non_graph_executor", "analytics": "execute_analytics"},
        )

        # non_graph_executor vai direto para END (não passa por insights/plotly/formatter)
        workflow.add_edge("non_graph_executor", END)

        # execute_analytics também vai para END
        workflow.add_edge("execute_analytics", END)
    else:
        workflow.add_edge("generate_output", END)

    # ========================================================================
    # COMPILE
    # ========================================================================

    logger.info("Compiling full integrated workflow")

    try:
        compiled_workflow = workflow.compile()
        logger.info(
            f"Full integrated workflow compiled successfully: "
            f"filter={'enabled' if include_filter_classifier else 'disabled'}, "
            f"executor={'enabled' if include_executor else 'disabled'}"
        )
        return compiled_workflow

    except Exception as e:
        logger.error(f"Failed to compile full integrated workflow: {str(e)}")
        raise


def initialize_full_pipeline_state(
    query: str, data_source: Optional[str] = None
) -> FilterGraphState:
    """
    Initialize state for the full integrated pipeline.

    This creates a FilterGraphState with all required fields for the complete
    3-phase pipeline (filter → classifier → executor).

    Args:
        query: User's natural language query
        data_source: Optional path to data source

    Returns:
        FilterGraphState with all required fields initialized

    Example:
        >>> state = initialize_full_pipeline_state("Top 5 clientes de SP")
        >>> workflow = create_full_integrated_workflow()
        >>> result = workflow.invoke(state)
    """
    return {
        # Input
        "query": query,
        # Filter-specific fields (Phase 0)
        "filter_history": [],
        "current_filters": {},
        "filter_operations": {},
        "filter_final": {},
        "detected_filter_columns": [],
        "filter_confidence": 0.0,
        # Parsing results (Phase 1)
        "parsed_entities": {},
        "detected_keywords": [],
        # Classification results (Phase 1)
        "intent": "",
        "chart_type": None,
        "confidence": 0.0,
        # Column mapping (Phase 1)
        "columns_mentioned": [],
        "mapped_columns": {},
        # Dataset validation (Phase 1)
        "data_source": data_source,
        "available_columns": None,
        # Output (Phase 1)
        "output": {},
        # Error handling
        "errors": [],
        # Analytics executor fields (Phase 2)
        "executor_input": None,
        "executor_output": None,
        "execution_time": None,
        "engine_used": None,
        # Phase 3 fields (parallel execution)
        "plotly_output": None,
        "insight_result": None,
        # Token tracking (all agents)
        "agent_tokens": {},
    }


def run_integrated_pipeline(
    query: str,
    include_filter_classifier: Optional[bool] = None,
    include_executor: bool = True,
    include_plotly_generator: bool = False,
    data_path: Optional[str] = None,
    reset_filters: bool = False,
    save_plotly_html: bool = True,
    save_plotly_png: bool = False,
) -> IntegratedPipelineResult:
    """
    Run the complete integrated pipeline with a single function call.

    This is the primary convenience function for executing all phases:
    - Phase 0: Filter classification and management (conditional - auto-detected)
    - Phase 1: Chart classification
    - Phase 2: Analytics execution (optional)
    - Phase 3: Plotly chart generation (optional)

    Args:
        query: Natural language query
        include_filter_classifier: Include Phase 0 (filter management).
            If None (default), automatically detects if filter is needed using heuristics.
            If True, forces filter execution. If False, skips filter execution.
        include_executor: Include Phase 2 (analytics execution)
        include_plotly_generator: Include Phase 3 (plotly chart generation)
        data_path: Optional path to data source (overrides default)
        reset_filters: If True, clears all existing filters before processing
        save_plotly_html: If True, saves generated chart as HTML (requires include_plotly_generator)
        save_plotly_png: If True, saves generated chart as PNG (requires kaleido)

    Returns:
        IntegratedPipelineResult with complete pipeline results

    Example:
        >>> # Full pipeline with auto-detection of filter need
        >>> result = run_integrated_pipeline(
        ...     "Top 5 clientes de SP em 2015",
        ...     include_plotly_generator=True
        ... )
        >>> print(result.active_filters)  # {'UF_Cliente': 'SP', 'Ano': 2015}
        >>> print(result.chart_type)  # 'bar_horizontal'
        >>> print(len(result.data))  # 5
        >>> result.plotly_figure.show()  # Display interactive chart

        >>> # Generic query - filter automatically skipped
        >>> result = run_integrated_pipeline("mostre um gráfico de vendas")
        >>> print(result.active_filters)  # {} (filter was skipped)

        >>> # Update filters conversationally
        >>> result = run_integrated_pipeline("Agora para RJ")
        >>> print(result.active_filters)  # {'UF_Cliente': 'RJ', 'Ano': 2015}

        >>> # Force filter execution
        >>> result = run_integrated_pipeline(
        ...     "Top 5 customers",
        ...     include_filter_classifier=True
        ... )

        >>> # Force filter skip
        >>> result = run_integrated_pipeline(
        ...     "Top 5 customers",
        ...     include_filter_classifier=False
        ... )
    """
    start_time = time.perf_counter()

    # Auto-detect if filter classifier is needed (if not explicitly set)
    # FASE 2, Etapa 2.2: Enhanced logging for routing decisions
    if include_filter_classifier is None:
        from src.shared_lib.utils.query_analyzer import analyze_query

        # Get detailed analysis for logging
        analysis = analyze_query(query)
        include_filter_classifier = analysis.needs_filter

        logger.info(
            f"🔍 Filter classifier auto-detection: "
            f"{'ENABLED' if include_filter_classifier else 'SKIPPED'} "
            f"(heuristic analysis)"
        )
        logger.info(
            f"   Analysis details: confidence={analysis.confidence:.2f}, "
            f"reason='{analysis.reason}'"
        )
        if analysis.detected_entities:
            logger.info(f"   Detected entities: {analysis.detected_entities}")
        if analysis.detected_keywords:
            logger.info(f"   Detected keywords: {analysis.detected_keywords}")
    else:
        logger.info(
            f"Filter classifier: "
            f"{'ENABLED' if include_filter_classifier else 'DISABLED'} "
            f"(explicit)"
        )

    logger.info(
        f"Running integrated pipeline: "
        f"filter_classifier={include_filter_classifier}, "
        f"executor={include_executor}, "
        f"plotly_generator={include_plotly_generator}"
    )
    logger.info(f"Query: {query}")

    try:
        # Clear filters if requested
        if reset_filters and include_filter_classifier:
            from src.filter_classifier.utils.filter_persistence import FilterPersistence

            persistence = FilterPersistence()
            persistence.clear()
            logger.info("Filters reset before processing")

        # Create integrated workflow
        workflow = create_full_integrated_workflow(
            include_filter_classifier=include_filter_classifier,
            include_executor=include_executor,
        )

        # Initialize state
        state = initialize_full_pipeline_state(query, data_source=data_path)

        # Execute workflow
        logger.info("Executing workflow...")
        final_state = workflow.invoke(state)

        # ==================================================
        # PHASE 3 (Optional): PLOTLY GENERATION
        # ==================================================
        if include_plotly_generator and include_executor:
            logger.info("Phase 3: Generating Plotly chart...")

            # Check if executor was successful
            executor_output = final_state.get("executor_output")
            if executor_output and executor_output.get("status") == "success":
                try:
                    # Initialize plotly generator
                    plotly_agent = PlotlyGeneratorAgent(
                        save_html=save_plotly_html, save_png=save_plotly_png
                    )

                    # Generate chart
                    chart_spec = final_state.get("output", {})
                    plotly_result = plotly_agent.generate(chart_spec, executor_output)

                    # Store result in state
                    final_state["plotly_output"] = plotly_result

                    if plotly_result["status"] == "success":
                        logger.info(
                            f"Plotly chart generated successfully: "
                            f"{plotly_result['metadata'].get('render_time', 0):.3f}s"
                        )
                        if plotly_result.get("file_path"):
                            logger.info(f"Chart saved to: {plotly_result['file_path']}")
                    else:
                        logger.warning(
                            f"Plotly generation failed: "
                            f"{plotly_result.get('error', {}).get('message', 'Unknown error')}"
                        )

                except Exception as e:
                    logger.error(f"Plotly generation error: {e}", exc_info=True)
                    final_state["plotly_output"] = {
                        "status": "error",
                        "error": {"type": "PlotlyGeneratorError", "message": str(e)},
                    }
            else:
                logger.warning(
                    "Skipping plotly generation - executor did not succeed "
                    f"(status: {executor_output.get('status') if executor_output else 'None'})"
                )
        elif include_plotly_generator and not include_executor:
            logger.warning(
                "Plotly generator requires executor to be enabled - skipping plotly generation"
            )

        execution_time = time.perf_counter() - start_time

        # Create result object
        result = IntegratedPipelineResult(final_state, execution_time)

        logger.info(
            f"Pipeline completed: status={result.status}, "
            f"chart_type={result.chart_type}, "
            f"filters={len(result.active_filters) if include_filter_classifier else 'N/A'}, "
            f"rows={len(result.data)}, "
            f"time={execution_time:.3f}s"
        )

        return result

    except Exception as e:
        execution_time = time.perf_counter() - start_time
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)

        # Create error result
        error_state = {
            "query": query,
            "filter_final": {},
            "output": {
                "chart_type": None,
                "message": f"Pipeline error: {str(e)}",
                "errors": [str(e)],
            },
            "errors": [str(e)],
            "intent": "error",
            "confidence": 0.0,
            "executor_output": None,
            "engine_used": None,
        }

        return IntegratedPipelineResult(error_state, execution_time)


def run_integrated_pipeline_with_insights(
    query: str,
    include_filter_classifier: Optional[bool] = None,
    include_insights: bool = True,
    data_path: Optional[str] = None,
    reset_filters: bool = False,
) -> IntegratedPipelineResult:
    """
    Run the complete integrated pipeline with insight generation.

    This function executes the full 5-agent pipeline with parallel execution:
    - Phase 0: Filter classification and management (conditional - auto-detected)
    - Phase 1: Chart classification
    - Phase 2: Analytics execution
    - Phase 3 (Parallel):
        - Insight generation (insight_generator)
        - Plotly chart generation (plotly_generator)
    - Phase 4: Merge results

    The insight_generator and plotly_generator run in parallel for better performance.
    Results are available immediately as they complete.

    Args:
        query: Natural language query
        include_filter_classifier: Include Phase 0 (filter management).
            If None (default), automatically detects if filter is needed using heuristics.
            If True, forces filter execution. If False, skips filter execution.
        include_insights: Include Phase 3 (insight generation)
        data_path: Optional path to data source (overrides default)
        reset_filters: If True, clears all existing filters before processing

    Returns:
        IntegratedPipelineResult with complete pipeline results including insights

    Example:
        >>> # Full pipeline with insights and auto-detection
        >>> result = run_integrated_pipeline_with_insights(
        ...     "Top 5 clientes de SP em 2015"
        ... )
        >>> print(result.active_filters)  # {'UF_Cliente': 'SP', 'Ano': 2015}
        >>> print(result.chart_type)  # 'bar_horizontal'
        >>> print(len(result.data))  # 5
        >>> result.plotly_figure.show()  # Display interactive chart
        >>> print(result.insights)  # List of insight strings

        >>> # Generic query - filter automatically skipped
        >>> result = run_integrated_pipeline_with_insights(
        ...     "mostre um gráfico de vendas"
        ... )
        >>> print(result.active_filters)  # {} (filter was skipped)

        >>> # Without insights (just chart)
        >>> result = run_integrated_pipeline_with_insights(
        ...     "Top 5 customers",
        ...     include_insights=False
        ... )
    """
    start_time = time.perf_counter()

    # Auto-detect if filter classifier is needed (if not explicitly set)
    # FASE 2, Etapa 2.2: Enhanced logging for routing decisions
    if include_filter_classifier is None:
        from src.shared_lib.utils.query_analyzer import analyze_query

        # Get detailed analysis for logging
        analysis = analyze_query(query)
        include_filter_classifier = analysis.needs_filter

        logger.info(
            f"🔍 Filter classifier auto-detection: "
            f"{'ENABLED' if include_filter_classifier else 'SKIPPED'} "
            f"(heuristic analysis)"
        )
        logger.info(
            f"   Analysis details: confidence={analysis.confidence:.2f}, "
            f"reason='{analysis.reason}'"
        )
        if analysis.detected_entities:
            logger.info(f"   Detected entities: {analysis.detected_entities}")
        if analysis.detected_keywords:
            logger.info(f"   Detected keywords: {analysis.detected_keywords}")
    else:
        logger.info(
            f"Filter classifier: "
            f"{'ENABLED' if include_filter_classifier else 'DISABLED'} "
            f"(explicit)"
        )

    logger.info(
        f"Running integrated pipeline with insights: "
        f"filter_classifier={include_filter_classifier}, "
        f"insights={include_insights}"
    )
    logger.info(f"Query: {query}")

    try:
        # Clear filters if requested
        if reset_filters and include_filter_classifier:
            from src.filter_classifier.utils.filter_persistence import FilterPersistence

            persistence = FilterPersistence()
            persistence.clear()
            logger.info("Filters reset before processing")

        # Create integrated workflow with insights
        workflow = create_full_integrated_workflow_with_insights(
            include_filter_classifier=include_filter_classifier,
            include_insights=include_insights,
        )

        # Initialize state
        state = initialize_full_pipeline_state(query, data_source=data_path)

        # Execute workflow with performance tracking (includes parallel execution of insight_generator and plotly_generator)
        logger.info("Executing workflow with insights...")
        perf = PerformanceMonitor()

        # Track phases
        filter_start = time.perf_counter()

        # Execute workflow
        final_state = workflow.invoke(state)

        # FASE 2 FIX: Use real execution times from nodes if available, otherwise estimate
        # Check if nodes already calculated their own times (FASE 2 enhancement)
        filter_time_from_node = final_state.get("filter_execution_time", None)
        classifier_time_from_node = final_state.get("classifier_execution_time", None)

        # Use real times if available, otherwise fallback to estimation
        if filter_time_from_node is not None and filter_time_from_node > 0:
            filter_time = filter_time_from_node
            logger.info(
                f"Using real filter_classifier time from node: {filter_time:.4f}s"
            )
        else:
            # Fallback to estimation (old logic)
            analytics_time = final_state.get("execution_time", 0.0)
            plotly_time = final_state.get("plotly_execution_time", 0.0)
            insight_time = final_state.get("insight_execution_time", 0.0)
            total_elapsed = time.perf_counter() - start_time
            pre_analytics_time = (
                total_elapsed - analytics_time - plotly_time - insight_time
            )

            if include_filter_classifier:
                filter_time = pre_analytics_time * 0.35
            else:
                filter_time = 0.0
            logger.debug(f"Using estimated filter_classifier time: {filter_time:.4f}s")

        if classifier_time_from_node is not None and classifier_time_from_node > 0:
            classifier_time = classifier_time_from_node
            logger.info(
                f"Using real graphic_classifier time from node: {classifier_time:.4f}s"
            )
        else:
            # Fallback to estimation (old logic)
            analytics_time = final_state.get("execution_time", 0.0)
            plotly_time = final_state.get("plotly_execution_time", 0.0)
            insight_time = final_state.get("insight_execution_time", 0.0)
            total_elapsed = time.perf_counter() - start_time
            pre_analytics_time = (
                total_elapsed - analytics_time - plotly_time - insight_time
            )

            if include_filter_classifier:
                classifier_time = pre_analytics_time * 0.65
            else:
                classifier_time = pre_analytics_time
            logger.debug(
                f"Using estimated graphic_classifier time: {classifier_time:.4f}s"
            )

        # Store times in state for formatter (preserve real times)
        final_state["filter_execution_time"] = filter_time
        final_state["classifier_execution_time"] = classifier_time

        execution_time = time.perf_counter() - start_time

        # Create result object
        result = IntegratedPipelineResult(final_state, execution_time)

        logger.info(
            f"Pipeline with insights completed: status={result.status}, "
            f"chart_type={result.chart_type}, "
            f"filters={len(result.active_filters) if include_filter_classifier else 'N/A'}, "
            f"rows={len(result.data)}, "
            f"insights={len(result.insights) if include_insights else 'N/A'}, "
            f"time={execution_time:.3f}s"
        )

        return result

    except Exception as e:
        execution_time = time.perf_counter() - start_time
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)

        # Create error result
        error_state = {
            "query": query,
            "filter_final": {},
            "output": {
                "chart_type": None,
                "message": f"Pipeline error: {str(e)}",
                "errors": [str(e)],
            },
            "errors": [str(e)],
            "intent": "error",
            "confidence": 0.0,
            "executor_output": None,
            "engine_used": None,
            "insight_result": None,
            "plotly_output": None,
        }

        return IntegratedPipelineResult(error_state, execution_time)


def classify_query(
    query: str, include_filter_classifier: bool = True
) -> Dict[str, Any]:
    """
    Run only Phase 1 (Classifier) without analytics execution.

    Convenience function for cases where you only need chart classification
    without data processing. Optionally includes filter classification.

    Args:
        query: Natural language query
        include_filter_classifier: Include Phase 0 (filter management)

    Returns:
        Chart specification dictionary (ChartOutput format)

    Example:
        >>> # With filters
        >>> output = classify_query("top 10 products by sales in SP")
        >>> print(output['chart_type'])  # 'bar_horizontal'
        >>> print(output['filters'])  # {'UF_Cliente': 'SP'}
        >>> print(output['top_n'])  # 10

        >>> # Without filters
        >>> output = classify_query("top 10 products", include_filter_classifier=False)
        >>> print(output['chart_type'])  # 'bar_horizontal'
    """
    logger.info(
        f"Running classifier only (Phase 1, with_filters={include_filter_classifier})"
    )

    result = run_integrated_pipeline(
        query,
        include_filter_classifier=include_filter_classifier,
        include_executor=False,
    )
    return result.classifier_output


def execute_analytics_from_spec(
    chart_spec: Dict[str, Any], data_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Execute analytics (Phase 2) from an existing chart specification.

    Useful when you already have a chart specification and want to
    execute the analytics separately.

    Args:
        chart_spec: Chart specification (ChartOutput format)
        data_path: Optional path to data source

    Returns:
        Analytics output dictionary (AnalyticsOutput format)

    Example:
        >>> spec = classify_query("top 5 customers")
        >>> analytics = execute_analytics_from_spec(spec)
        >>> print(analytics['status'])  # 'success'
        >>> print(len(analytics['data']))  # 5
    """
    from src.analytics_executor.agent import AnalyticsExecutorAgent

    logger.info("Executing analytics from existing specification")

    executor = AnalyticsExecutorAgent(data_path=data_path)
    return executor.execute_from_chart_output(chart_spec, data_path=data_path)


def run_batch_pipeline(
    queries: list[str],
    include_filter_classifier: bool = True,
    include_executor: bool = True,
    data_path: Optional[str] = None,
    reset_filters_between_queries: bool = False,
) -> list[IntegratedPipelineResult]:
    """
    Run integrated pipeline for multiple queries in batch.

    Args:
        queries: List of natural language queries
        include_filter_classifier: Include Phase 0 (filter management)
        include_executor: Include Phase 2 (analytics execution)
        data_path: Optional path to data source
        reset_filters_between_queries: If True, clears filters before each query

    Returns:
        List of IntegratedPipelineResult, one per query

    Example:
        >>> # Batch with filter continuity (conversational)
        >>> queries = [
        ...     "top 5 customers in SP",
        ...     "and in 2020",  # Maintains SP filter
        ...     "now for RJ"    # Changes to RJ, keeps 2020
        ... ]
        >>> results = run_batch_pipeline(queries)
        >>> for r in results:
        ...     print(f"{r.query}: {r.active_filters}")

        >>> # Batch with isolated queries
        >>> queries = ["top 5 customers", "sales by month", "distribution by state"]
        >>> results = run_batch_pipeline(
        ...     queries,
        ...     reset_filters_between_queries=True
        ... )
    """
    logger.info(
        f"Running batch pipeline: {len(queries)} queries, "
        f"filters={'enabled' if include_filter_classifier else 'disabled'}"
    )

    results = []
    for i, query in enumerate(queries, 1):
        logger.info(f"Processing query {i}/{len(queries)}")
        result = run_integrated_pipeline(
            query,
            include_filter_classifier=include_filter_classifier,
            include_executor=include_executor,
            data_path=data_path,
            reset_filters=reset_filters_between_queries,
        )
        results.append(result)

    # Log summary
    successful = sum(1 for r in results if r.status == "success")
    avg_time = sum(r.execution_time for r in results) / len(results) if results else 0
    logger.info(
        f"Batch completed: {successful}/{len(queries)} successful, "
        f"avg time: {avg_time:.3f}s"
    )

    return results


def validate_pipeline_result(
    result: IntegratedPipelineResult,
) -> Tuple[bool, list[str]]:
    """
    Validate pipeline result completeness and correctness.

    Args:
        result: IntegratedPipelineResult to validate

    Returns:
        Tuple of (is_valid, error_messages)

    Example:
        >>> result = run_integrated_pipeline("top 5 customers")
        >>> is_valid, errors = validate_pipeline_result(result)
        >>> if not is_valid:
        ...     print("Validation errors:", errors)
    """
    errors = []

    # Check status
    if result.status == "error":
        errors.append("Pipeline execution failed")

    # Check chart type was detected
    if result.chart_type is None and result.status not in ["error", "skipped"]:
        errors.append("No chart type detected")

    # Check confidence
    if result.confidence < 0.3:
        errors.append(f"Low confidence: {result.confidence:.2f}")

    # Check executor output if expected
    if result.executor_output:
        exec_status = result.executor_output.get("status")
        if exec_status == "error":
            errors.append("Analytics execution failed")

        # Check data presence
        if exec_status == "success" and not result.has_data:
            errors.append("No data returned from executor")

    # Check for errors in state
    if result.errors:
        errors.extend([f"State error: {e}" for e in result.errors])

    is_valid = len(errors) == 0
    return is_valid, errors


def create_full_integrated_workflow_with_insights(
    include_filter_classifier: bool = True,
    include_executor: bool = True,
    include_insights: bool = True,
    verbose: bool = False,
):
    """
    Create fully integrated workflow with insight_generator running in parallel (fan-out pattern).

    This creates the complete pipeline with parallel execution:
    - Phase 0 (optional): Filter classification and management
    - Phase 1: Chart classification
    - Phase 2: Analytics execution
    - Phase 3 (parallel): Insight generation + Plotly generation
    - Phase 4: Merge results

    Workflow Structure (all phases enabled):
    ```
    START
      ↓
    [FILTER CLASSIFIER] (optional)
      ↓
    [GRAPHIC CLASSIFIER]
      ↓
    [ANALYTICS EXECUTOR]
      ↓
    ┌─────────────────┴─────────────────┐
    ↓                                   ↓
    insight_generator             plotly_generator
    ↓                                   ↓
    └─────────────────┬─────────────────┘
      ↓
    merge_results
      ↓
    END
    ```

    Args:
        include_filter_classifier: Include Phase 0 (filter management)
        include_executor: Include Phase 2 (analytics execution)
        include_insights: Include Phase 3 (insight generation)
        verbose: Enable verbose logging

    Returns:
        Compiled StateGraph ready for execution

    Example:
        >>> workflow = create_full_integrated_workflow_with_insights()
        >>> state = initialize_full_pipeline_state("Top 5 clientes")
        >>> result = workflow.invoke(state)
        >>> print(result["insight_result"])
        >>> print(result["plotly_output"])
    """
    logger.info(
        f"Creating full integrated workflow with insights: "
        f"filter_classifier={include_filter_classifier}, "
        f"executor={include_executor}, "
        f"insights={include_insights}"
    )

    # Create the state graph with FilterGraphState (extends GraphState)
    workflow = StateGraph(FilterGraphState)

    # ========================================================================
    # ADD NODES - Phase 0 (Filter Classifier)
    # ========================================================================

    if include_filter_classifier:
        logger.debug("Adding Phase 0 (filter_classifier) nodes")
        workflow.add_node("load_filter_context", load_filter_context)
        workflow.add_node("parse_filter_query", parse_filter_query)
        workflow.add_node(
            "validate_detected_values", validate_detected_values
        )  # FASE 1 FIX
        workflow.add_node(
            "expand_temporal_periods", expand_temporal_periods_node
        )  # FASE 1 FIX
        workflow.add_node("validate_filter_columns", validate_filter_columns_node)
        workflow.add_node("identify_filter_operations", identify_filter_operations)
        workflow.add_node("apply_filter_operations", apply_filter_operations)
        workflow.add_node("persist_filters", persist_filters)
        workflow.add_node("format_filter_output", format_filter_output)

    # ========================================================================
    # ADD NODES - Phase 1 (Graphic Classifier)
    # ========================================================================

    logger.debug("Adding Phase 1 (graphic_classifier) nodes")
    # FASE 1: Semantic-First Architecture (CRITICAL - MUST execute first)
    workflow.add_node("extract_semantic_anchor", extract_semantic_anchor_node)
    workflow.add_node("validate_semantic_anchor", validate_semantic_anchor_node)
    workflow.add_node("map_semantic_to_chart", map_semantic_to_chart_node)

    # Legacy nodes (subordinate to semantic layer)
    workflow.add_node("parse_query", parse_query_node)
    workflow.add_node("load_dataset_metadata", load_dataset_metadata_node)
    workflow.add_node("detect_keywords", detect_keywords_node)
    workflow.add_node("classify_intent", classify_intent_node)
    workflow.add_node("map_columns", map_columns_node)
    workflow.add_node("generate_output", generate_output_node)

    # ========================================================================
    # ADD NODES - Phase 2 (Analytics Executor)
    # ========================================================================

    if include_executor:
        logger.debug("Adding Phase 2 (analytics_executor) nodes")
        workflow.add_node("execute_analytics", execute_analytics_node)

    # ========================================================================
    # ADD NODES - Phase 2-alt (Non-Graph Executor)
    # ========================================================================

    logger.debug("Adding Phase 2-alt (non_graph_executor) node")
    workflow.add_node("non_graph_executor", non_graph_executor_node)

    # ========================================================================
    # ADD NODES - Phase 3 (Parallel: Insight Generator + Plotly Generator)
    # ========================================================================

    if include_insights:
        logger.debug(
            "Adding Phase 3 (parallel: insight_generator + plotly_generator) nodes"
        )
        workflow.add_node("insight_generator", insight_generator_node)
        workflow.add_node("plotly_generator", plotly_generator_node)
        # Phase 4: Formatter Agent (replaces merge_results)
        workflow.add_node("formatter", formatter_node)

    # ========================================================================
    # ADD EDGES - Phase 0 (Filter Classifier)
    # ========================================================================

    if include_filter_classifier:
        logger.debug("Adding Phase 0 edges")

        # Set entry point at filter_classifier
        workflow.set_entry_point("load_filter_context")

        # Filter workflow edges (FASE 1 FIX - add validate_detected_values and expand_temporal_periods)
        workflow.add_edge("load_filter_context", "parse_filter_query")
        workflow.add_edge(
            "parse_filter_query", "validate_detected_values"
        )  # FASE 1 FIX
        workflow.add_edge(
            "validate_detected_values", "expand_temporal_periods"
        )  # FASE 1 FIX

        workflow.add_conditional_edges(
            "expand_temporal_periods",  # FASE 1 FIX - changed from parse_filter_query
            should_validate_filters,
            {
                "validate": "validate_filter_columns",
                "skip": "identify_filter_operations",
            },
        )

        workflow.add_conditional_edges(
            "validate_filter_columns",
            has_validation_errors,
            {"error": "format_filter_output", "continue": "identify_filter_operations"},
        )

        workflow.add_edge("identify_filter_operations", "apply_filter_operations")
        workflow.add_edge("apply_filter_operations", "persist_filters")
        workflow.add_edge("persist_filters", "format_filter_output")

        # Connect filter_classifier to graphic_classifier
        workflow.add_edge("format_filter_output", "extract_semantic_anchor")
    else:
        # If no filter_classifier, start at graphic_classifier (semantic layer)
        workflow.set_entry_point("extract_semantic_anchor")

    # ========================================================================
    # ADD EDGES - Phase 1 (Graphic Classifier)
    # ========================================================================

    logger.debug("Adding Phase 1 edges")

    # CRITICAL: Semantic-First Pipeline (MUST execute first)
    workflow.add_edge("extract_semantic_anchor", "validate_semantic_anchor")
    workflow.add_edge("validate_semantic_anchor", "map_semantic_to_chart")
    workflow.add_edge("map_semantic_to_chart", "parse_query")

    # Legacy pipeline (subordinate to semantic layer)
    workflow.add_edge("parse_query", "load_dataset_metadata")
    workflow.add_edge("load_dataset_metadata", "detect_keywords")
    workflow.add_edge("detect_keywords", "classify_intent")

    workflow.add_conditional_edges(
        "classify_intent",
        should_map_columns,
        {"map": "map_columns", "no_chart": "generate_output"},
    )

    workflow.add_edge("map_columns", "generate_output")

    # ========================================================================
    # ADD EDGES - Phase 2 (Analytics Executor) & Phase 2-alt (Non-Graph Executor)
    # ========================================================================

    if include_executor:
        logger.debug("Adding Phase 2 edges with conditional routing")

        # Conditional routing após generate_output
        workflow.add_conditional_edges(
            "generate_output",
            route_after_classifier,
            {"non_graph": "non_graph_executor", "analytics": "execute_analytics"},
        )

        # non_graph_executor vai direto para END (não passa por insights/plotly/formatter)
        workflow.add_edge("non_graph_executor", END)

        # Connect to Phase 3 if insights enabled
        if include_insights:
            # Parallel execution: execute_analytics fans out to both insight_generator and plotly_generator
            # Both converge to formatter (fan-in pattern)
            workflow.add_edge("execute_analytics", "insight_generator")
            workflow.add_edge("execute_analytics", "plotly_generator")
            workflow.add_edge("insight_generator", "formatter")
            workflow.add_edge("plotly_generator", "formatter")
            workflow.add_edge("formatter", END)
        else:
            workflow.add_edge("execute_analytics", END)
    else:
        workflow.add_edge("generate_output", END)

    # ========================================================================
    # COMPILE
    # ========================================================================

    logger.info("Compiling full integrated workflow with insights")

    try:
        compiled_workflow = workflow.compile()
        logger.info(
            f"Full integrated workflow with insights compiled successfully: "
            f"filter={'enabled' if include_filter_classifier else 'disabled'}, "
            f"executor={'enabled' if include_executor else 'disabled'}, "
            f"insights={'enabled' if include_insights else 'disabled'}"
        )
        return compiled_workflow

    except Exception as e:
        logger.error(
            f"Failed to compile full integrated workflow with insights: {str(e)}"
        )
        raise


def non_graph_executor_node(state: FilterGraphState) -> Dict[str, Any]:
    """
    Node para execução de queries não-gráficas (Phase 2-alt).

    Acionado quando:
    - chart_type is None (query não-gráfica)
    - requires_tabular_data is True (solicitação de tabela)

    Fluxo:
    1. Extrai data_path do state
    2. Inicializa NonGraphExecutorAgent
    3. Executa agent.execute(state)
    4. Retorna resultado em non_graph_output

    Args:
        state: Current pipeline state com query, filter_final, data_source

    Returns:
        Dict com:
        - non_graph_output: resultado estruturado
        - execution_time: tempo de execução
    """
    from src.non_graph_executor.agent import NonGraphExecutorAgent
    from src.shared_lib.utils.json_serialization import sanitize_for_json

    logger.info("Executing non_graph_executor node...")
    start_time = time.perf_counter()

    try:
        # Extrair data_path
        data_path = state.get("data_source")

        # Inicializar agente (sem setup_logs para evitar duplicação)
        agent = NonGraphExecutorAgent(data_path=data_path, setup_logs=False)

        # Executar
        result = agent.execute(state)

        # Ensure the output is JSON-serializable (datetime/date/timedelta -> safe primitives)
        non_graph_output = sanitize_for_json(result.get("non_graph_output"))

        execution_time = time.perf_counter() - start_time

        logger.info(
            f"Non-graph executor completed in {execution_time:.3f}s: "
            f"query_type={(non_graph_output or {}).get('query_type')}, "
            f"status={(non_graph_output or {}).get('status')}"
        )

        return {
            "non_graph_output": non_graph_output,
            "execution_time": execution_time,
        }

    except Exception as e:
        execution_time = time.perf_counter() - start_time
        logger.error(f"Error in non_graph_executor node: {e}", exc_info=True)

        return {
            "non_graph_output": {
                "status": "error",
                "query_type": "unknown",
                "error": {"message": str(e), "type": "NonGraphExecutorError"},
                "metadata": {},
                "performance_metrics": {"execution_time": execution_time},
            },
            "execution_time": execution_time,
        }


def insight_generator_node(state: FilterGraphState) -> Dict[str, Any]:
    """
    Node for insight generation (Phase 3 - parallel with plotly_generator).

    Generates strategic insights from analytics results using the InsightGeneratorAgent.

    Args:
        state: Current pipeline state

    Returns:
        Dictionary with only insight_result field (to avoid conflicts with parallel nodes)
    """
    from src.insight_generator.agent import InsightGeneratorAgent
    import time

    logger.info("Executing insight_generator node...")
    start_time = time.perf_counter()

    try:
        # Check if executor was successful
        executor_output = state.get("executor_output")
        if not executor_output or executor_output.get("status") != "success":
            logger.warning("Skipping insight generation - executor did not succeed")
            execution_time = time.perf_counter() - start_time
            return {
                "insight_result": {
                    "status": "skipped",
                    "message": "Executor did not succeed",
                    "insights": [],
                },
                "insight_execution_time": execution_time,
            }

        # Initialize insight generator agent
        agent = InsightGeneratorAgent(setup_logs=False, validate=False)

        # Generate insights
        chart_spec = state.get("output", {})

        # Propagate user_query to insight_generator so it can enrich intent
        # and build contextual prompts (the query lives in pipeline state)
        pipeline_query = state.get("query", "")
        if pipeline_query and "user_query" not in chart_spec:
            chart_spec = {**chart_spec, "user_query": pipeline_query}

        insight_result = agent.generate(chart_spec, executor_output)

        execution_time = time.perf_counter() - start_time
        # Use detailed_insights (new schema) with fallback to insights (legacy)
        insights_count = len(
            insight_result.get("detailed_insights", insight_result.get("insights", []))
        )
        logger.info(
            f"Insight generation completed in {execution_time:.3f}s: "
            f"status={insight_result['status']}, "
            f"insights={insights_count}"
        )

        # CRITICAL: Extract and propagate agent_tokens from insight_result
        result_dict = {
            "insight_result": insight_result,
            "insight_execution_time": execution_time,
        }

        # If insight_result contains _agent_tokens (from workflow), propagate them
        if "_agent_tokens" in insight_result:
            # Merge insight_generator tokens into state's agent_tokens
            if "agent_tokens" not in state:
                state["agent_tokens"] = {}

            insight_tokens = insight_result["_agent_tokens"].get(
                "insight_generator", {}
            )
            if insight_tokens:
                result_dict["agent_tokens"] = {
                    **state.get("agent_tokens", {}),
                    "insight_generator": insight_tokens,
                }
                logger.info(
                    f"[insight_generator_node] Propagating tokens: "
                    f"input={insight_tokens.get('input_tokens', 0)}, "
                    f"output={insight_tokens.get('output_tokens', 0)}, "
                    f"total={insight_tokens.get('total_tokens', 0)}"
                )

        return result_dict

    except Exception as e:
        execution_time = time.perf_counter() - start_time
        logger.error(
            f"Error in insight_generator node after {execution_time:.3f}s: {e}",
            exc_info=True,
        )
        return {
            "insight_result": {
                "status": "error",
                "error": str(e),
                "insights": [],
            },
            "insight_execution_time": execution_time,
        }


def plotly_generator_node(state: FilterGraphState) -> Dict[str, Any]:
    """
    Node for plotly chart generation (Phase 3 - parallel with insight_generator).

    Generates interactive Plotly charts using the PlotlyGeneratorAgent.

    Args:
        state: Current pipeline state

    Returns:
        Dictionary with only plotly_output field (to avoid conflicts with parallel nodes)
    """
    import time

    logger.info("Executing plotly_generator node...")
    start_time = time.perf_counter()

    try:
        # Check if executor was successful
        executor_output = state.get("executor_output")
        if not executor_output or executor_output.get("status") != "success":
            logger.warning("Skipping plotly generation - executor did not succeed")
            execution_time = time.perf_counter() - start_time
            return {
                "plotly_output": {
                    "status": "skipped",
                    "message": "Executor did not succeed",
                },
                "plotly_execution_time": execution_time,
            }

        # Initialize plotly generator agent
        plotly_agent = PlotlyGeneratorAgent(save_html=True, save_png=False)

        # Generate chart
        chart_spec = state.get("output", {})
        plotly_result = plotly_agent.generate(chart_spec, executor_output)

        execution_time = time.perf_counter() - start_time
        logger.info(
            f"Plotly generation completed in {execution_time:.3f}s: status={plotly_result['status']}"
        )

        return {
            "plotly_output": plotly_result,
            "plotly_execution_time": execution_time,
        }

    except Exception as e:
        execution_time = time.perf_counter() - start_time
        logger.error(
            f"Error in plotly_generator node after {execution_time:.3f}s: {e}",
            exc_info=True,
        )
        return {
            "plotly_output": {
                "status": "error",
                "error": {"type": "PlotlyGeneratorError", "message": str(e)},
            },
            "plotly_execution_time": execution_time,
        }


def formatter_node(state: FilterGraphState) -> Dict[str, Any]:
    """
    Node for executing the formatter agent (Phase 4).

    Consolidates outputs from all previous agents and generates structured
    JSON output with:
    - Executive summary via LLM
    - Synthesized insights narrative via LLM
    - Strategic next steps via LLM
    - Formatted data tables
    - Complete metadata
    - Performance metrics from all agents

    This node replaces the old merge_results_node, providing richer
    and more structured output.

    Args:
        state: Current pipeline state with all previous agent outputs

    Returns:
        Dictionary with formatter_output field containing structured JSON
    """
    from src.formatter_agent import run_formatter
    from src.shared_lib.utils.json_serialization import sanitize_for_json
    import time

    logger.info("Executing formatter node (Phase 4)...")
    start_time = time.perf_counter()

    try:
        # Collect execution times from all agents
        # FASE 2 FIX: Detect if agents actually executed by checking state modifications

        # Filter classifier: check if filter_final was populated
        filter_classifier_executed = bool(state.get("filter_final", {}))
        filter_time = state.get("filter_execution_time", 0.0)
        if filter_time == 0.0 and filter_classifier_executed:
            # Agent executed but time not tracked - estimate minimal time
            filter_time = 0.01
            logger.warning(
                "[formatter_node] filter_classifier executed but time=0.0, using minimal estimate"
            )

        # Graphic classifier: check if output was generated
        graphic_output = state.get("output", {})
        graphic_classifier_executed = bool(
            graphic_output.get("chart_type") or graphic_output.get("intent")
        )
        classifier_time = state.get("classifier_execution_time", 0.0)
        if classifier_time == 0.0 and graphic_classifier_executed:
            # Agent executed but time not tracked - estimate minimal time
            classifier_time = 0.01
            logger.warning(
                "[formatter_node] graphic_classifier executed but time=0.0, using minimal estimate"
            )

        performance_metrics = {
            "filter_classifier_execution_time": filter_time,
            "graphic_classifier_execution_time": classifier_time,
            "analytics_executor_execution_time": state.get(
                "execution_time", 0.0
            ),  # from execute_analytics_node
            "plotly_generator_execution_time": state.get("plotly_execution_time", 0.0),
            "insight_generator_execution_time": state.get(
                "insight_execution_time", 0.0
            ),
        }

        # Prepare state for formatter agent
        formatter_input = {
            "query": state.get("query", ""),
            "chart_type": state.get("output", {}).get("chart_type"),
            "filter_final": state.get("filter_final", {}),
            "chart_spec": state.get("output", {}),
            "analytics_result": state.get("executor_output", {}),
            "plotly_result": state.get("plotly_output", {}),
            "insight_result": state.get("insight_result", {}),
            "performance_metrics": performance_metrics,
            # Token tracking (LLM agents only)
            "agent_tokens": state.get("agent_tokens", {}),
        }

        # Execute formatter agent
        formatter_output = run_formatter(formatter_input)

        # Ensure the output is JSON-serializable (datetime/date/timedelta -> safe primitives)
        formatter_output = sanitize_for_json(formatter_output)

        execution_time = time.perf_counter() - start_time

        # Add formatter execution time to the output
        if "performance_metrics" not in formatter_output:
            formatter_output["performance_metrics"] = {}

        formatter_output["performance_metrics"]["formatter_execution_time"] = (
            execution_time
        )

        # Calculate total execution time
        total_time = sum(v or 0.0 for v in performance_metrics.values()) + (
            execution_time or 0.0
        )

        formatter_output["performance_metrics"]["total_execution_time"] = total_time

        # Also add individual agent times
        formatter_output["performance_metrics"].update(performance_metrics)

        # Sync metadata execution section with authoritative performance metrics
        metadata = formatter_output.get("metadata", {})
        if isinstance(metadata, dict):
            execution_section = metadata.get("execution_time", {})
            if not isinstance(execution_section, dict):
                execution_section = {}

            execution_section.update(
                {
                    "filter_classifier": performance_metrics.get(
                        "filter_classifier_execution_time", 0.0
                    ),
                    "graphic_classifier": performance_metrics.get(
                        "graphic_classifier_execution_time", 0.0
                    ),
                    "analytics_executor": performance_metrics.get(
                        "analytics_executor_execution_time", 0.0
                    ),
                    "plotly_generator": performance_metrics.get(
                        "plotly_generator_execution_time", 0.0
                    ),
                    "insight_generator": performance_metrics.get(
                        "insight_generator_execution_time", 0.0
                    ),
                    "formatter": formatter_output["performance_metrics"].get(
                        "formatter_execution_time",
                        execution_section.get("formatter", 0.0),
                    ),
                    "total_execution_time": formatter_output["performance_metrics"].get(
                        "total_execution_time",
                        execution_section.get("total_execution_time", 0.0),
                    ),
                }
            )

            metadata["execution_time"] = execution_section
            metadata["formatter_execution_time"] = formatter_output[
                "performance_metrics"
            ].get(
                "formatter_execution_time",
                metadata.get("formatter_execution_time", 0.0),
            )
            metadata["total_execution_time"] = formatter_output[
                "performance_metrics"
            ].get("total_execution_time", metadata.get("total_execution_time", 0.0))

        logger.info(
            f"Formatter node completed successfully in {execution_time:.3f}s: "
            f"status={formatter_output.get('status')}, "
            f"total_pipeline_time={total_time:.3f}s"
        )

        # Return formatter_output to be merged into state
        return {"formatter_output": formatter_output}

    except Exception as e:
        execution_time = time.perf_counter() - start_time
        logger.error(
            f"Error in formatter node after {execution_time:.3f}s: {e}", exc_info=True
        )

        # Return minimal error output
        return {
            "formatter_output": {
                "status": "error",
                "format_version": "1.0.0",
                "error": {
                    "message": f"Formatter node failed: {str(e)}",
                    "recovery": "partial",
                    "critical": False,
                },
                "executive_summary": {
                    "title": "Erro no Processamento",
                    "introduction": "Não foi possível completar a formatação dos resultados.",
                },
                "performance_metrics": {
                    "formatter_execution_time": execution_time,
                    "error": "Could not collect full performance metrics due to error",
                },
            }
        }


def merge_results_node(state: FilterGraphState) -> Dict[str, Any]:
    """
    DEPRECATED: Node for merging results from insight_generator and plotly_generator.

    This node has been replaced by formatter_node (Phase 4) which provides
    richer, LLM-powered formatting and structured JSON output.

    This function is kept for backward compatibility only and may be removed
    in future versions. Use formatter_node instead.

    Combines results from parallel nodes into a unified final_output.

    Args:
        state: Current pipeline state

    Returns:
        Dictionary with only final_output field

    Note:
        Consider migrating to formatter_node for:
        - Executive summaries via LLM
        - Synthesized insights narratives
        - Strategic next steps recommendations
        - Richer structured output
    """
    from datetime import datetime

    logger.warning(
        "merge_results_node is DEPRECATED. Use formatter_node for richer output."
    )

    logger.info("Executing merge_results node...")

    try:
        # Extract results from parallel nodes
        insight_result = state.get("insight_result", {})
        plotly_result = state.get("plotly_output", {})
        chart_spec = state.get("output", {})

        # Build final output
        final_output = {
            "status": "success",
            "chart": {
                "type": chart_spec.get("chart_type"),
                "plotly_config": plotly_result.get("config", {}),
                "file_path": plotly_result.get("file_path"),
                "html": plotly_result.get("html"),
            },
            "insights": {
                "status": insight_result.get("status"),
                "items": insight_result.get("insights", []),
                "metadata": insight_result.get("metadata", {}),
            },
            "metadata": {
                "chart_type": chart_spec.get("chart_type"),
                "timestamp": datetime.now().isoformat(),
                "query": state.get("query", ""),
            },
        }

        logger.info("Results merged successfully")

        # Return both final_output AND preserve insight_result in state
        return {
            "final_output": final_output,
            # Keep insight_result in state so IntegratedPipelineResult can access it
            # (LangGraph will merge this with existing state)
        }

    except Exception as e:
        logger.error(f"Error in merge_results node: {e}", exc_info=True)
        return {
            "final_output": {
                "status": "error",
                "error": str(e),
            }
        }


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    # Result container
    "IntegratedPipelineResult",
    # Workflow creation
    "create_full_integrated_workflow",
    "create_full_integrated_workflow_with_insights",
    "initialize_full_pipeline_state",
    # Pipeline execution
    "run_integrated_pipeline",
    "run_integrated_pipeline_with_insights",
    "classify_query",
    "execute_analytics_from_spec",
    "run_batch_pipeline",
    # Validation
    "validate_pipeline_result",
    # Nodes (for testing)
    "formatter_node",
    "merge_results_node",  # DEPRECATED
]
