"""
LangGraph Workflow Components for Plotly Generator

This package contains the LangGraph workflow implementation for chart generation.

Components:
- state: State schema definition (PlotlyGeneratorState)
- nodes: Processing nodes (validate, adapt, generate, save)
- workflow: Workflow definition and compilation

Author: Claude Code
Date: 2025-11-12
Version: 1.0
"""

from src.plotly_generator.graph.state import PlotlyGeneratorState
from src.plotly_generator.graph.workflow import (
    create_plotly_generator_workflow,
    get_workflow_structure,
)
from src.plotly_generator.graph.nodes import (
    validate_inputs_node,
    adapt_inputs_node,
    generate_plot_node,
    save_output_node,
)

__all__ = [
    # State
    "PlotlyGeneratorState",
    # Workflow
    "create_plotly_generator_workflow",
    "get_workflow_structure",
    # Nodes
    "validate_inputs_node",
    "adapt_inputs_node",
    "generate_plot_node",
    "save_output_node",
]
