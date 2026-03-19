"""
LangGraph workflow definition and compilation.

This module defines the complete agent workflow by connecting nodes
and edges into a compiled LangGraph that can be executed.
"""

import logging
from typing import Optional

from langgraph.graph import StateGraph, END

from src.graphic_classifier.graph.state import GraphState
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
    initialize_state,
    attempt_fallback_node,
)
from src.graphic_classifier.graph.edges import (
    should_map_columns,
    should_attempt_fallback,
    should_route_after_fallback,
)

logger = logging.getLogger(__name__)


def create_workflow(verbose: bool = False):
    """
    Create and compile the LangGraph workflow for chart classification.

    This function builds the complete agent workflow by:
    1. Defining all nodes (processing steps)
    2. Connecting nodes with edges (sequential and conditional)
    3. Compiling the graph into an executable workflow

    Workflow Structure (FASE 1 - Semantic-First):
    ```
    START
      ↓
    extract_semantic_anchor (LLM extracts pure semantic intent - FIRST LAYER)
      ↓
    validate_semantic_anchor (validate against keywords)
      ↓
    map_semantic_to_chart (deterministic mapping to chart family)
      ↓
    parse_query (extract entities, numbers, dates)
      ↓
    load_dataset_metadata (extract available columns from dataset)
      ↓
    detect_keywords (identify chart indicators)
      ↓
    classify_intent (refine with heuristics - SUBORDINATE to semantic anchor)
      ↓
    [should_map_columns?]
      ├─ chart_type != None → map_columns (resolve column references with validation)
      │                           ↓
      │                      generate_output (filter invalid columns)
      │                           ↓
      │                      [should_attempt_fallback?]  ◄─ FASE 6
      │                           ├─ success → END
      │                           └─ fallback → attempt_fallback
      │                                           ↓
      │                                      [should_route_after_fallback?]
      │                                           ├─ retry → map_columns (with new chart type)
      │                                           ├─ route_to_text → END (with redirect)
      │                                           └─ final → END
      └─ chart_type == None → generate_output (no chart needed)
                                  ↓
                                 END
    ```

    Args:
        verbose: If True, enable verbose logging for debugging

    Returns:
        Compiled StateGraph ready for execution

    Example:
        >>> workflow = create_workflow()
        >>> state = initialize_state("top 5 produtos mais vendidos")
        >>> result = workflow.invoke(state)
        >>> print(result["output"])
    """
    logger.info("Creating LangGraph workflow")

    # Create the state graph
    workflow = StateGraph(GraphState)

    # ========================================================================
    # ADD NODES
    # ========================================================================

    logger.debug("Adding workflow nodes")

    # FASE 1: Semantic-First Architecture (FIRST LAYER)
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

    # FASE 6: Fallback and Routing
    workflow.add_node("attempt_fallback", attempt_fallback_node)

    # ========================================================================
    # ADD EDGES
    # ========================================================================

    logger.debug("Adding workflow edges")

    # CRITICAL: Set entry point to semantic layer (FIRST LAYER)
    workflow.set_entry_point("extract_semantic_anchor")

    # FASE 1: Semantic-First Pipeline (MUST execute first)
    workflow.add_edge("extract_semantic_anchor", "validate_semantic_anchor")
    workflow.add_edge("validate_semantic_anchor", "map_semantic_to_chart")
    workflow.add_edge("map_semantic_to_chart", "parse_query")

    # Legacy pipeline (now subordinate to semantic layer)
    workflow.add_edge("parse_query", "load_dataset_metadata")
    workflow.add_edge("load_dataset_metadata", "detect_keywords")
    workflow.add_edge("detect_keywords", "classify_intent")

    # Conditional edge: route based on chart_type
    workflow.add_conditional_edges(
        "classify_intent",
        should_map_columns,
        {
            "map": "map_columns",  # Chart identified: map columns
            "no_chart": "generate_output",  # No chart: skip to output
        },
    )

    # Edge from map_columns to generate_output
    workflow.add_edge("map_columns", "generate_output")

    # FASE 6: Fallback routing after generate_output
    # Check if fallback is needed (null chart, empty data, etc.)
    workflow.add_conditional_edges(
        "generate_output",
        should_attempt_fallback,
        {
            "success": END,  # Visualization succeeded, finish
            "fallback": "attempt_fallback",  # Need fallback/routing
        },
    )

    # After fallback attempt, decide next action
    workflow.add_conditional_edges(
        "attempt_fallback",
        should_route_after_fallback,
        {
            "retry": "map_columns",  # Retry with degraded chart type
            "route_to_text": END,  # Route to non_graph_executor (handled externally)
            "final": END,  # Proceed to end
        },
    )

    # ========================================================================
    # COMPILE
    # ========================================================================

    logger.info("Compiling workflow")

    try:
        compiled_workflow = workflow.compile()
        logger.info("Workflow compiled successfully")
        return compiled_workflow

    except Exception as e:
        logger.error(f"Failed to compile workflow: {str(e)}")
        raise


def execute_workflow(query: str, workflow=None) -> dict:
    """
    Execute the workflow for a given query.

    This is a convenience function that:
    1. Creates initial state
    2. Executes workflow (creating it if needed)
    3. Returns the final output

    Args:
        query: User's natural language query
        workflow: Pre-compiled workflow (optional, will create if None)

    Returns:
        Dictionary containing the chart specification output

    Example:
        >>> result = execute_workflow("top 10 clientes por receita")
        >>> print(result["chart_type"])
        'bar_horizontal'
    """
    logger.info(f"Executing workflow for query: {query}")

    # Create workflow if not provided
    if workflow is None:
        workflow = create_workflow()

    # Initialize state
    initial_state = initialize_state(query)

    # Execute workflow
    try:
        final_state = workflow.invoke(initial_state)

        # Extract and return output
        output = final_state.get("output", {})

        logger.info(
            f"Workflow execution completed: chart_type={output.get('chart_type')}"
        )

        return output

    except Exception as e:
        logger.error(f"Workflow execution failed: {str(e)}")
        return {
            "chart_type": None,
            "message": f"Workflow execution error: {str(e)}",
            "errors": [str(e)],
            "metrics": [],
        }


def visualize_workflow(output_path: str = "workflow_diagram.png"):
    """
    Generate a visual diagram of the workflow graph.

    This function creates a diagram showing the nodes and edges
    of the workflow, useful for documentation and debugging.

    Args:
        output_path: Path where to save the diagram image

    Note:
        Requires pygraphviz to be installed for diagram generation.
        If not available, will log a warning and skip visualization.
    """
    try:
        workflow = create_workflow()

        # Try to get the mermaid representation
        try:
            mermaid_graph = workflow.get_graph().draw_mermaid()
            logger.info(f"Workflow Mermaid diagram:\n{mermaid_graph}")

            # Save to file
            with open(output_path.replace(".png", ".mmd"), "w") as f:
                f.write(mermaid_graph)

            logger.info(
                f"Workflow diagram saved to {output_path.replace('.png', '.mmd')}"
            )

        except Exception as e:
            logger.warning(f"Could not generate Mermaid diagram: {str(e)}")

    except ImportError:
        logger.warning(
            "Workflow visualization requires pygraphviz or graphviz. "
            "Install with: pip install pygraphviz"
        )
    except Exception as e:
        logger.error(f"Failed to visualize workflow: {str(e)}")


# ============================================================================
# WORKFLOW CONFIGURATION
# ============================================================================


class WorkflowConfig:
    """
    Configuration class for workflow customization.

    This class allows customization of workflow behavior without
    modifying the core workflow definition.
    """

    def __init__(
        self,
        enable_caching: bool = True,
        max_retries: int = 0,
        timeout_seconds: Optional[int] = None,
        fallback_chart_type: Optional[str] = None,
    ):
        """
        Initialize workflow configuration.

        Args:
            enable_caching: Enable result caching for faster repeated queries
            max_retries: Maximum number of retries for failed classifications
            timeout_seconds: Timeout for LLM calls (None = no timeout)
            fallback_chart_type: Default chart type if classification fails
        """
        self.enable_caching = enable_caching
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self.fallback_chart_type = fallback_chart_type


def create_custom_workflow(config: WorkflowConfig):
    """
    Create a workflow with custom configuration.

    This function allows creating workflows with different configurations
    for different use cases (e.g., production vs development).

    Args:
        config: WorkflowConfig instance with custom settings

    Returns:
        Compiled StateGraph with custom configuration applied

    Example:
        >>> config = WorkflowConfig(enable_caching=True, max_retries=1)
        >>> workflow = create_custom_workflow(config)
    """
    logger.info(f"Creating custom workflow with config: {config.__dict__}")

    # For now, return standard workflow
    # Future enhancement: apply config settings
    workflow = create_workflow()

    # Store config for potential use in nodes
    # (would require modifying nodes to accept config)
    workflow._custom_config = config

    return workflow


def create_integrated_workflow(include_executor: bool = True, verbose: bool = False):
    """
    Create integrated workflow including Phase 1 (Classifier) and Phase 2 (Executor).

    This workflow provides complete end-to-end processing:
    - Phase 1: Natural language query → Structured chart specification
    - Phase 2: Chart specification → Executed analytics + Plotly-ready data

    Workflow Structure (with executor):
    ```
    START
      ↓
    parse_query
      ↓
    detect_keywords
      ↓
    classify_intent
      ↓
    [should_map_columns?]
      ├─ chart_type != None → map_columns
      │                           ↓
      │                      generate_output
      │                           ↓
      │                      execute_analytics (Phase 2)
      └─ chart_type == None → generate_output
                                  ↓
                                 END
    ```

    Args:
        include_executor: If True, includes analytics execution node (Phase 2)
        verbose: If True, enable verbose logging for debugging

    Returns:
        Compiled StateGraph ready for execution with full pipeline

    Example:
        >>> # Full integrated pipeline
        >>> workflow = create_integrated_workflow(include_executor=True)
        >>> state = initialize_state("top 5 customers by revenue")
        >>> result = workflow.invoke(state)
        >>> print(result["executor_output"]["status"])  # 'success'
        >>> print(len(result["executor_output"]["data"]))  # 5

        >>> # Classifier only (Phase 1)
        >>> workflow = create_integrated_workflow(include_executor=False)
        >>> result = workflow.invoke(state)
        >>> print(result["output"]["chart_type"])  # 'bar_horizontal'
    """
    logger.info(f"Creating integrated workflow (include_executor={include_executor})")

    # Create the state graph
    workflow = StateGraph(GraphState)

    # ========================================================================
    # ADD NODES - Phase 1 (Classifier)
    # ========================================================================

    logger.debug("Adding Phase 1 nodes")

    # FASE 1: Semantic-First Architecture (FIRST LAYER)
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
    # ADD NODES - Phase 2 (Executor)
    # ========================================================================

    if include_executor:
        logger.debug("Adding Phase 2 nodes")
        workflow.add_node("execute_analytics", execute_analytics_node)

    # ========================================================================
    # ADD EDGES - Phase 1
    # ========================================================================

    logger.debug("Adding Phase 1 edges")

    # CRITICAL: Set entry point to semantic layer (FIRST LAYER)
    workflow.set_entry_point("extract_semantic_anchor")

    # FASE 1: Semantic-First Pipeline (MUST execute first)
    workflow.add_edge("extract_semantic_anchor", "validate_semantic_anchor")
    workflow.add_edge("validate_semantic_anchor", "map_semantic_to_chart")
    workflow.add_edge("map_semantic_to_chart", "parse_query")

    # Legacy pipeline (now subordinate to semantic layer)
    workflow.add_edge("parse_query", "load_dataset_metadata")
    workflow.add_edge("load_dataset_metadata", "detect_keywords")
    workflow.add_edge("detect_keywords", "classify_intent")

    # Conditional edge: route based on chart_type
    workflow.add_conditional_edges(
        "classify_intent",
        should_map_columns,
        {
            "map": "map_columns",  # Chart identified: map columns
            "no_chart": "generate_output",  # No chart: skip to output
        },
    )

    # Edge from map_columns to generate_output
    workflow.add_edge("map_columns", "generate_output")

    # ========================================================================
    # ADD EDGES - Phase 2 Integration
    # ========================================================================

    if include_executor:
        logger.debug("Adding Phase 2 edges")

        # After generate_output, execute analytics
        workflow.add_edge("generate_output", "execute_analytics")

        # Terminal edge from executor
        workflow.add_edge("execute_analytics", END)
    else:
        # Terminal edge from generate_output (Phase 1 only)
        workflow.add_edge("generate_output", END)

    # ========================================================================
    # COMPILE
    # ========================================================================

    logger.info("Compiling integrated workflow")

    try:
        compiled_workflow = workflow.compile()
        logger.info(
            f"Integrated workflow compiled successfully (executor={'enabled' if include_executor else 'disabled'})"
        )
        return compiled_workflow

    except Exception as e:
        logger.error(f"Failed to compile integrated workflow: {str(e)}")
        raise


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "create_workflow",
    "execute_workflow",
    "visualize_workflow",
    "WorkflowConfig",
    "create_custom_workflow",
    "create_integrated_workflow",
]
