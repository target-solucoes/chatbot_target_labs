"""
LangGraph Workflow for Plotly Generator

This module defines the complete LangGraph workflow for chart generation.
The workflow orchestrates the flow through validation, adaptation, generation,
and saving nodes.

Workflow Structure:
    START
      ↓
    validate_inputs
      ↓
    [conditional: if valid]
      ↓
    adapt_inputs
      ↓
    generate_plot
      ↓
    save_output
      ↓
    END

Author: Claude Code
Date: 2025-11-12
Version: 1.0
"""

from langgraph.graph import StateGraph, END
from src.plotly_generator.graph.state import PlotlyGeneratorState
from src.plotly_generator.graph.nodes import (
    validate_inputs_node,
    adapt_inputs_node,
    generate_plot_node,
    save_output_node,
)
from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


def create_plotly_generator_workflow() -> StateGraph:
    """
    Create and compile the Plotly Generator LangGraph workflow.

    This workflow orchestrates the complete chart generation process:
    1. Validate inputs (chart_spec + analytics_result)
    2. Adapt inputs to internal format
    3. Generate Plotly chart
    4. Save chart to disk

    Returns:
        Compiled StateGraph ready for execution

    Example Usage:
        >>> workflow = create_plotly_generator_workflow()
        >>> initial_state = {
        ...     "chart_spec": {...},
        ...     "analytics_result": {...},
        ...     "status": "pending",
        ...     "validation_errors": [],
        ...     "save_html": True,
        ...     "save_png": False
        ... }
        >>> final_state = workflow.invoke(initial_state)
        >>> if final_state["status"] == "success":
        ...     print(f"Chart saved to: {final_state['file_path']}")

    Node Flow:
        - validate_inputs: Checks that inputs are valid
        - adapt_inputs: Transforms inputs to internal format
        - generate_plot: Creates Plotly Figure
        - save_output: Saves chart to disk

    Conditional Edges:
        - After validation: Continue only if status="validated"
        - After adaptation: Continue only if status="adapted"
        - After generation: Continue only if status="generated"
    """
    logger.info("Creating Plotly Generator workflow")

    # Create workflow
    workflow = StateGraph(PlotlyGeneratorState)

    # Add nodes
    workflow.add_node("validate_inputs", validate_inputs_node)
    workflow.add_node("adapt_inputs", adapt_inputs_node)
    workflow.add_node("generate_plot", generate_plot_node)
    workflow.add_node("save_output", save_output_node)

    # Set entry point
    workflow.set_entry_point("validate_inputs")

    # Add conditional edge after validation
    workflow.add_conditional_edges(
        "validate_inputs",
        _should_continue_after_validation,
        {"adapt_inputs": "adapt_inputs", END: END},
    )

    # Add conditional edge after adaptation
    workflow.add_conditional_edges(
        "adapt_inputs",
        _should_continue_after_adaptation,
        {"generate_plot": "generate_plot", END: END},
    )

    # Add conditional edge after generation
    workflow.add_conditional_edges(
        "generate_plot",
        _should_continue_after_generation,
        {"save_output": "save_output", END: END},
    )

    # Save output always goes to END
    workflow.add_edge("save_output", END)

    # Compile and return
    compiled = workflow.compile()
    logger.info("Workflow compiled successfully")
    return compiled


def _should_continue_after_validation(state: PlotlyGeneratorState) -> str:
    """
    Decide whether to continue after validation.

    Args:
        state: Current workflow state

    Returns:
        "adapt_inputs" if validation passed, END otherwise
    """
    if state["status"] == "validated":
        logger.debug("Validation passed - continuing to adapt_inputs")
        return "adapt_inputs"
    else:
        logger.warning(
            f"Validation failed - terminating workflow (status={state['status']})"
        )
        return END


def _should_continue_after_adaptation(state: PlotlyGeneratorState) -> str:
    """
    Decide whether to continue after adaptation.

    Args:
        state: Current workflow state

    Returns:
        "generate_plot" if adaptation passed, END otherwise
    """
    if state["status"] == "adapted":
        logger.debug("Adaptation passed - continuing to generate_plot")
        return "generate_plot"
    else:
        logger.warning(
            f"Adaptation failed - terminating workflow (status={state['status']})"
        )
        return END


def _should_continue_after_generation(state: PlotlyGeneratorState) -> str:
    """
    Decide whether to continue after generation.

    Args:
        state: Current workflow state

    Returns:
        "save_output" if generation passed, END otherwise
    """
    if state["status"] == "generated":
        logger.debug("Generation passed - continuing to save_output")
        return "save_output"
    else:
        logger.warning(
            f"Generation failed - terminating workflow (status={state['status']})"
        )
        return END


def get_workflow_structure() -> dict:
    """
    Get the workflow structure as a dictionary.

    Useful for documentation and debugging.

    Returns:
        Dict describing the workflow structure
    """
    return {
        "name": "plotly_generator_workflow",
        "nodes": [
            {
                "name": "validate_inputs",
                "description": "Validate chart_spec and analytics_result inputs",
                "outputs": ["validated", "error"],
            },
            {
                "name": "adapt_inputs",
                "description": "Transform inputs to internal plotting format",
                "outputs": ["adapted", "error"],
            },
            {
                "name": "generate_plot",
                "description": "Generate Plotly Figure using appropriate generator",
                "outputs": ["generated", "error"],
            },
            {
                "name": "save_output",
                "description": "Save chart to disk as HTML/PNG",
                "outputs": ["success"],
            },
        ],
        "edges": [
            {"from": "START", "to": "validate_inputs"},
            {
                "from": "validate_inputs",
                "to": "adapt_inputs",
                "condition": "status==validated",
            },
            {"from": "validate_inputs", "to": "END", "condition": "status==error"},
            {
                "from": "adapt_inputs",
                "to": "generate_plot",
                "condition": "status==adapted",
            },
            {"from": "adapt_inputs", "to": "END", "condition": "status==error"},
            {
                "from": "generate_plot",
                "to": "save_output",
                "condition": "status==generated",
            },
            {"from": "generate_plot", "to": "END", "condition": "status==error"},
            {"from": "save_output", "to": "END"},
        ],
    }


# Export main function
__all__ = ["create_plotly_generator_workflow", "get_workflow_structure"]
