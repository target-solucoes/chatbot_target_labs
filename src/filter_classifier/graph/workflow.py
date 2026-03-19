"""
LangGraph workflow definition and compilation for filter_classifier.

This module defines the complete filter classification workflow by connecting
nodes and conditional edges into a compiled LangGraph that can be executed.
"""

import logging
from typing import Optional

from langgraph.graph import StateGraph, END

from src.filter_classifier.graph.state import FilterGraphState
from src.filter_classifier.graph.nodes import (
    parse_filter_query,
    validate_detected_values,
    expand_temporal_periods_node,
    load_filter_context,
    validate_filter_columns,
    identify_filter_operations,
    apply_filter_operations,
    persist_filters,
    format_filter_output
)
from src.filter_classifier.graph.edges import (
    should_validate_filters,
    has_validation_errors
)

logger = logging.getLogger(__name__)


def create_filter_workflow(verbose: bool = False):
    """
    Create and compile the LangGraph workflow for filter classification.

    This function builds the complete filter management workflow by:
    1. Defining all 7 nodes (processing steps)
    2. Connecting nodes with edges (sequential and conditional)
    3. Compiling the graph into an executable workflow

    Workflow Structure:
    ```
    START
      ↓
    load_filter_context (load previous session filters)
      ↓
    parse_filter_query (LLM detects filters and CRUD operations)
      ↓
    validate_detected_values (NEW - validate values exist in dataset)
      ↓
    [should_validate_filters?]
      ├─ validate → validate_filter_columns
      │                ↓
      │             [has_validation_errors?]
      │                ├─ error → format_filter_output (skip operations)
      │                └─ continue → identify_filter_operations
      │                                 ↓
      │                              apply_filter_operations
      │                                 ↓
      │                              persist_filters
      │                                 ↓
      │                              format_filter_output
      └─ skip → identify_filter_operations (no new filters, but may have REMOVE/KEEP)
                   ↓
                apply_filter_operations
                   ↓
                persist_filters
                   ↓
                format_filter_output
      ↓
     END
    ```

    Args:
        verbose: If True, enable verbose logging for debugging

    Returns:
        Compiled StateGraph ready for execution

    Example:
        >>> workflow = create_filter_workflow()
        >>> initial_state = {
        ...     "query": "Mostre dados de SP",
        ...     "filter_history": [],
        ...     "current_filters": {},
        ...     ...
        ... }
        >>> result = workflow.invoke(initial_state)
        >>> print(result["output"]["filter_final"])
    """
    logger.info("[Workflow] Creating filter classification workflow")

    # Create the state graph
    workflow = StateGraph(FilterGraphState)

    # ========================================================================
    # ADD NODES
    # ========================================================================

    logger.debug("[Workflow] Adding workflow nodes")

    workflow.add_node("load_filter_context", load_filter_context)
    workflow.add_node("parse_filter_query", parse_filter_query)
    workflow.add_node("validate_detected_values", validate_detected_values)  # NEW
    workflow.add_node("expand_temporal_periods", expand_temporal_periods_node)  # NEW - FASE 1.1
    workflow.add_node("validate_filter_columns", validate_filter_columns)
    workflow.add_node("identify_filter_operations", identify_filter_operations)
    workflow.add_node("apply_filter_operations", apply_filter_operations)
    workflow.add_node("persist_filters", persist_filters)
    workflow.add_node("format_filter_output", format_filter_output)

    # ========================================================================
    # ADD EDGES
    # ========================================================================

    logger.debug("[Workflow] Adding workflow edges")

    # Set entry point
    workflow.set_entry_point("load_filter_context")

    # Sequential edges (unconditional)
    workflow.add_edge("load_filter_context", "parse_filter_query")

    # NEW: Add value validation step after parsing
    workflow.add_edge("parse_filter_query", "validate_detected_values")

    # NEW - FASE 1.1: Add temporal period expansion after value validation
    workflow.add_edge("validate_detected_values", "expand_temporal_periods")

    # Conditional edge 1: should we validate filters?
    # NOW comes from expand_temporal_periods instead of validate_detected_values
    workflow.add_conditional_edges(
        "expand_temporal_periods",
        should_validate_filters,
        {
            "validate": "validate_filter_columns",  # Filters detected: validate them
            "skip": "identify_filter_operations"    # No filters: skip validation
        }
    )

    # Conditional edge 2: do we have validation errors?
    workflow.add_conditional_edges(
        "validate_filter_columns",
        has_validation_errors,
        {
            "error": "format_filter_output",         # Critical errors: skip to output
            "continue": "identify_filter_operations"  # No critical errors: continue
        }
    )

    # Sequential edges after validation
    workflow.add_edge("identify_filter_operations", "apply_filter_operations")
    workflow.add_edge("apply_filter_operations", "persist_filters")
    workflow.add_edge("persist_filters", "format_filter_output")

    # Terminal edge
    workflow.add_edge("format_filter_output", END)

    # ========================================================================
    # COMPILE
    # ========================================================================

    logger.info("[Workflow] Compiling workflow")

    try:
        compiled_workflow = workflow.compile()
        logger.info("[Workflow] Filter workflow compiled successfully")
        return compiled_workflow

    except Exception as e:
        logger.error(f"[Workflow] Failed to compile workflow: {str(e)}")
        raise


def initialize_filter_state(query: str) -> FilterGraphState:
    """
    Initialize a FilterGraphState with default values.

    This is a helper function to create a properly structured initial state
    for the filter workflow.

    Args:
        query: User's natural language query

    Returns:
        FilterGraphState with all required fields initialized

    Example:
        >>> state = initialize_filter_state("Mostre vendas de SP em 2020")
        >>> workflow = create_filter_workflow()
        >>> result = workflow.invoke(state)
    """
    return {
        # Filter-specific fields
        "query": query,
        "filter_history": [],
        "current_filters": {},
        "filter_operations": {},
        "filter_final": {},
        "detected_filter_columns": [],
        "filter_confidence": 0.0,

        # GraphState base fields (required for compatibility)
        "parsed_entities": {},
        "columns_mentioned": [],
        "errors": [],
        "chart_type": None,
        "intent": "",
        "confidence": 0.0,
        "mapped_columns": {},
        "output": {}
    }


def execute_filter_workflow(query: str, workflow=None) -> dict:
    """
    Execute the filter workflow for a given query.

    This is a convenience function that:
    1. Creates initial state
    2. Executes workflow (creating it if needed)
    3. Returns the final output

    Args:
        query: User's natural language query
        workflow: Pre-compiled workflow (optional, will create if None)

    Returns:
        Dictionary containing the filter specification output with structure:
        {
            "ADICIONAR": {...},
            "ALTERAR": {...},
            "REMOVER": {...},
            "MANTER": {...},
            "filter_final": {...},
            "metadata": {...}
        }

    Example:
        >>> result = execute_filter_workflow("Filtre por SP e ano 2020")
        >>> print(result["filter_final"])
        {'UF_Cliente': 'SP', 'Ano': 2020}
    """
    logger.info(f"[Workflow] Executing filter workflow for query: {query}")

    # Create workflow if not provided
    if workflow is None:
        workflow = create_filter_workflow()

    # Initialize state
    initial_state = initialize_filter_state(query)

    # Execute workflow
    try:
        final_state = workflow.invoke(initial_state)

        # Extract and return output
        output = final_state.get("output", {})

        filter_count = len(output.get("filter_final", {}))
        logger.info(f"[Workflow] Execution completed: {filter_count} final filters")

        return output

    except Exception as e:
        logger.error(f"[Workflow] Workflow execution failed: {str(e)}")
        return {
            "ADICIONAR": {},
            "ALTERAR": {},
            "REMOVER": {},
            "MANTER": {},
            "filter_final": {},
            "metadata": {
                "confidence": 0.0,
                "timestamp": "",
                "columns_detected": [],
                "errors": [f"Workflow execution error: {str(e)}"],
                "status": "error"
            }
        }


def visualize_filter_workflow(output_path: str = "filter_workflow_diagram.mmd"):
    """
    Generate a visual diagram of the filter workflow graph.

    This function creates a Mermaid diagram showing the nodes and edges
    of the workflow, useful for documentation and debugging.

    Args:
        output_path: Path where to save the Mermaid diagram file

    Note:
        Generates a .mmd file that can be rendered with Mermaid.js
        or various Markdown viewers that support Mermaid diagrams.
    """
    try:
        workflow = create_filter_workflow()

        # Get the Mermaid representation
        try:
            mermaid_graph = workflow.get_graph().draw_mermaid()
            logger.info(f"[Workflow] Mermaid diagram:\n{mermaid_graph}")

            # Save to file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(mermaid_graph)

            logger.info(f"[Workflow] Diagram saved to {output_path}")

        except Exception as e:
            logger.warning(f"[Workflow] Could not generate Mermaid diagram: {str(e)}")

    except Exception as e:
        logger.error(f"[Workflow] Failed to visualize workflow: {str(e)}")


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "create_filter_workflow",
    "initialize_filter_state",
    "execute_filter_workflow",
    "visualize_filter_workflow"
]
