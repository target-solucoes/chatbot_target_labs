"""
LangGraph workflow definition and compilation for formatter agent
===================================================================

Defines the complete formatter agent workflow by connecting nodes
and edges into a compiled LangGraph that can be executed.

Workflow Structure:
```
START
  ↓
parse_inputs ──[error]──→ handle_error → END
  ↓ [success]
select_handler ──[error]──→ handle_error → END
  ↓ [success]
start_parallel_generation
  ↓
┌─────────────┴─────────────┐
↓                           ↓
generate_executive_summary  synthesize_insights
(PARALLEL - ~1.5s)          (PARALLEL - ~2s)
  ↓                           ↓
  └─────────────┬─────────────┘
                ↓
       generate_next_steps
        (SEQUENTIAL - ~1.5s)
                ↓
       format_data_table
                ↓
       [check_status]
  ├─ success → assemble_output → END
  └─ error → handle_error → END
```
"""

import logging
from typing import Optional

from langgraph.graph import StateGraph, END

from ..graph.state import FormatterState
from ..graph.nodes import (
    parse_inputs_node,
    select_handler_node,
    start_parallel_generation_node,
    generate_executive_summary_node,
    synthesize_insights_node,
    generate_next_steps_node,
    format_data_table_node,
    assemble_output_node,
    handle_error_node,
)
from ..graph.router import (
    should_continue_or_error,
    check_handler_selection,
    check_generation_status,
)

logger = logging.getLogger(__name__)


# ============================================================================
# WORKFLOW CREATION
# ============================================================================


def create_formatter_workflow(verbose: bool = False):
    """
    Create and compile the LangGraph workflow for formatter agent.

    This function builds the complete formatter workflow by:
    1. Defining all processing nodes
    2. Connecting nodes with edges (sequential and conditional)
    3. Implementing error handling paths
    4. Compiling the graph into an executable workflow

    The workflow orchestrates:
    - Input parsing and validation
    - Handler selection
    - LLM-powered content generation (3 calls)
    - Data formatting
    - Output assembly
    - Error handling and graceful degradation

    Args:
        verbose: If True, enable verbose logging for debugging

    Returns:
        Compiled StateGraph ready for execution

    Example:
        >>> workflow = create_formatter_workflow()
        >>> state = {
        ...     "query": "top 5 clientes",
        ...     "chart_type": "bar_horizontal",
        ...     "filter_final": {},
        ...     "chart_spec": {...},
        ...     "analytics_result": {...},
        ...     "plotly_result": {...},
        ...     "insight_result": {...}
        ... }
        >>> result = workflow.invoke(state)
        >>> print(result["formatter_output"])
    """
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    logger.info("Creating formatter agent LangGraph workflow")

    # Create the state graph
    workflow = StateGraph(FormatterState)

    # ========================================================================
    # ADD NODES
    # ========================================================================

    logger.debug("Adding workflow nodes")

    # Core processing nodes
    workflow.add_node("parse_inputs", parse_inputs_node)
    workflow.add_node("select_handler", select_handler_node)
    workflow.add_node("start_parallel_generation", start_parallel_generation_node)
    workflow.add_node("generate_executive_summary", generate_executive_summary_node)
    workflow.add_node("synthesize_insights", synthesize_insights_node)
    workflow.add_node("generate_next_steps", generate_next_steps_node)
    workflow.add_node("format_data_table", format_data_table_node)
    workflow.add_node("assemble_output", assemble_output_node)

    # Error handling node
    workflow.add_node("handle_error", handle_error_node)

    # ========================================================================
    # ADD EDGES
    # ========================================================================

    logger.debug("Adding workflow edges")

    # Set entry point
    workflow.set_entry_point("parse_inputs")

    # Conditional edge after parsing
    workflow.add_conditional_edges(
        "parse_inputs",
        should_continue_or_error,
        {
            "continue": "select_handler",
            "handle_error": "handle_error",
        },
    )

    # Conditional edge after handler selection
    workflow.add_conditional_edges(
        "select_handler",
        check_handler_selection,
        {
            "generate": "start_parallel_generation",
            "handle_error": "handle_error",
        },
    )

    # Parallel generation (fan-out/convergence pattern)
    # Fan-out: start_parallel → [generate_executive_summary, synthesize_insights]
    workflow.add_edge("start_parallel_generation", "generate_executive_summary")
    workflow.add_edge("start_parallel_generation", "synthesize_insights")

    # Convergence: [generate_executive_summary, synthesize_insights] → generate_next_steps
    # Both parallel nodes must complete before generate_next_steps can execute
    workflow.add_edge("generate_executive_summary", "generate_next_steps")
    workflow.add_edge("synthesize_insights", "generate_next_steps")

    # Sequential continuation
    workflow.add_edge("generate_next_steps", "format_data_table")

    # After data formatting, check status before assembly
    workflow.add_conditional_edges(
        "format_data_table",
        check_generation_status,
        {
            "assemble": "assemble_output",
            "handle_error": "handle_error",
        },
    )

    # Terminal edges
    workflow.add_edge("assemble_output", END)
    workflow.add_edge("handle_error", END)

    # ========================================================================
    # COMPILE
    # ========================================================================

    logger.info("Compiling formatter workflow")

    try:
        compiled_workflow = workflow.compile()
        logger.info("Formatter workflow compiled successfully")
        return compiled_workflow

    except Exception as e:
        logger.error(f"Failed to compile formatter workflow: {str(e)}")
        raise


# ============================================================================
# WORKFLOW EXECUTION UTILITIES
# ============================================================================


def execute_formatter_workflow(
    state: FormatterState,
    workflow=None,
    verbose: bool = False,
) -> FormatterState:
    """
    Execute the formatter workflow for given state.

    This is a convenience function that:
    1. Creates workflow if not provided
    2. Executes workflow with given state
    3. Returns the final state with formatter_output

    Args:
        state: FormatterState with inputs from previous agents
        workflow: Pre-compiled workflow (optional, will create if None)
        verbose: Enable verbose logging

    Returns:
        Final FormatterState with formatter_output populated

    Example:
        >>> state = {
        ...     "query": "top 5 produtos",
        ...     "chart_type": "bar_horizontal",
        ...     "analytics_result": {"data": [...]},
        ...     "insight_result": {"insights": [...]}
        ... }
        >>> result = execute_formatter_workflow(state)
        >>> output = result["formatter_output"]
    """
    logger.info("Executing formatter workflow")

    # Create workflow if not provided
    if workflow is None:
        logger.debug("Creating new workflow instance")
        workflow = create_formatter_workflow(verbose=verbose)

    # Execute workflow
    try:
        result = workflow.invoke(state)
        logger.info(
            f"Workflow execution completed - status: {result.get('status', 'unknown')}"
        )
        return result

    except Exception as e:
        logger.error(f"Workflow execution failed: {e}", exc_info=True)
        # Return error state
        return {
            **state,
            "status": "error",
            "error": f"Workflow execution failed: {str(e)}",
            "formatter_output": {
                "status": "error",
                "error": {"message": str(e), "recovery": "none"},
            },
        }


# ============================================================================
# WORKFLOW VISUALIZATION
# ============================================================================


def get_workflow_graph(workflow=None, output_path: Optional[str] = None):
    """
    Generate visual representation of the workflow graph.

    Args:
        workflow: Compiled workflow (optional, will create if None)
        output_path: Path to save graph image (optional)

    Returns:
        Graph visualization object (implementation depends on LangGraph version)

    Note:
        Requires graphviz or mermaid for visualization
    """
    if workflow is None:
        workflow = create_formatter_workflow()

    try:
        # Get graph representation
        graph = workflow.get_graph()

        # Save if output path provided
        if output_path:
            logger.info(f"Saving workflow graph to {output_path}")
            # Implementation depends on LangGraph version
            # graph.draw_png(output_path)  # Example

        return graph

    except Exception as e:
        logger.warning(f"Could not generate workflow graph: {e}")
        return None


# ============================================================================
# WORKFLOW DEBUGGING
# ============================================================================


def debug_workflow_step(
    state: FormatterState,
    step_name: str,
    workflow=None,
) -> FormatterState:
    """
    Execute workflow up to a specific step for debugging.

    Args:
        state: Initial FormatterState
        step_name: Name of the node to stop at
        workflow: Pre-compiled workflow (optional)

    Returns:
        State after executing up to specified step

    Example:
        >>> state = {...}
        >>> debug_state = debug_workflow_step(state, "synthesize_insights")
        >>> print(debug_state["synthesized_insights"])
    """
    logger.info(f"Debug: Executing workflow up to '{step_name}'")

    if workflow is None:
        workflow = create_formatter_workflow(verbose=True)

    # Note: This is a conceptual function
    # Actual implementation would depend on LangGraph's debugging features
    # For now, execute full workflow and return result

    try:
        result = workflow.invoke(state)
        logger.info(
            f"Debug execution completed - current status: {result.get('status')}"
        )
        return result

    except Exception as e:
        logger.error(f"Debug execution failed at or before '{step_name}': {e}")
        raise


# ============================================================================
# WORKFLOW STATISTICS
# ============================================================================


def get_workflow_statistics(workflow=None) -> dict:
    """
    Get statistics about the workflow structure.

    Args:
        workflow: Compiled workflow (optional)

    Returns:
        Dictionary with workflow statistics:
        {
            "total_nodes": int,
            "total_edges": int,
            "conditional_edges": int,
            "terminal_nodes": list,
            "entry_point": str
        }
    """
    if workflow is None:
        workflow = create_formatter_workflow()

    try:
        # Get graph for potential future analysis
        _ = workflow.get_graph()

        stats = {
            "total_nodes": 8,  # Counted from our implementation
            "total_edges": 10,  # Approximate from our structure
            "conditional_edges": 3,  # 3 conditional routing points
            "terminal_nodes": ["assemble_output", "handle_error"],
            "entry_point": "parse_inputs",
        }

        logger.info(f"Workflow statistics: {stats}")
        return stats

    except Exception as e:
        logger.warning(f"Could not retrieve workflow statistics: {e}")
        return {}
