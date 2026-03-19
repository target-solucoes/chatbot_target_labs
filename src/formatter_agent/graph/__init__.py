"""Graph module initialization - LangGraph workflow components"""

from .state import FormatterState
from .nodes import (
    parse_inputs_node,
    select_handler_node,
    generate_executive_summary_node,
    synthesize_insights_node,
    generate_next_steps_node,
    format_data_table_node,
    assemble_output_node,
    handle_error_node,
)
from .router import (
    should_continue_or_error,
    check_handler_selection,
    check_generation_status,
)
from .workflow import (
    create_formatter_workflow,
    execute_formatter_workflow,
    get_workflow_graph,
    debug_workflow_step,
    get_workflow_statistics,
)

__all__ = [
    # State
    "FormatterState",
    # Nodes
    "parse_inputs_node",
    "select_handler_node",
    "generate_executive_summary_node",
    "synthesize_insights_node",
    "generate_next_steps_node",
    "format_data_table_node",
    "assemble_output_node",
    "handle_error_node",
    # Routers
    "should_continue_or_error",
    "check_handler_selection",
    "check_generation_status",
    # Workflow
    "create_formatter_workflow",
    "execute_formatter_workflow",
    "get_workflow_graph",
    "debug_workflow_step",
    "get_workflow_statistics",
]
