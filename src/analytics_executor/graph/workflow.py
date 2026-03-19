"""
LangGraph Workflow for Analytics Executor.

This module implements the complete workflow that orchestrates the
analytics execution pipeline using LangGraph's StateGraph.

The workflow follows this pattern:
    Input → Parse → Route → Tool Handler → Format → Output

Flow Details:
    1. parse_input_node: Loads data and extracts schema
    2. route_by_chart_type: Routes to appropriate tool handler
    3. tool_handle_*: Executes chart-specific logic
    4. format_output_node: Formats final result
"""

from langgraph.graph import StateGraph, END
import logging

from .state import AnalyticsState
from .nodes import parse_input_node, format_output_node
from .router import route_by_chart_type

# Import all tool handlers
from ..tools.bar_horizontal import tool_handle_bar_horizontal
from ..tools.bar_vertical import tool_handle_bar_vertical

# REMOVED: bar_vertical_composed (migrated to line_composed)
from ..tools.bar_vertical_stacked import tool_handle_bar_vertical_stacked
from ..tools.line import tool_handle_line
from ..tools.line_composed import tool_handle_line_composed
from ..tools.pie import tool_handle_pie
from ..tools.histogram import tool_handle_histogram
from ..tools.null_chart import tool_handle_null

logger = logging.getLogger(__name__)


def create_analytics_executor_graph():
    """
    Cria o graph do analytics_executor usando LangGraph.

    Este é o coração da nova arquitetura. O graph define:
    - Nodes: Funções que processam o state
    - Edges: Conexões entre nodes (fixas ou condicionais)
    - Entry point: Onde o fluxo começa
    - End point: Onde o fluxo termina

    Fluxo do Graph:
    ┌─────────────────┐
    │  parse_input    │  ← Entry point
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │  router         │  ← Conditional routing based on chart_type
    └────────┬────────┘
             │
             ├──→ handle_bar_horizontal ──┐
             ├──→ handle_bar_vertical ────┤
             ├──→ handle_bar_vertical_    │

             │    stacked ─────────────────┤
             ├──→ handle_line ────────────┤
             ├──→ handle_line_composed ───┤
             ├──→ handle_pie ─────────────┤
             ├──→ handle_histogram ───────┤
             └──→ handle_null ────────────┤
                                          │
                                          ▼
                                 ┌────────────────┐
                                 │ format_output  │
                                 └────────┬───────┘
                                          │
                                          ▼
                                        [END]

    Returns:
        CompiledGraph: Graph compilado pronto para execução via invoke()

    Example:
        >>> graph = create_analytics_executor_graph()
        >>> initial_state = {"chart_spec": {...}}
        >>> result = graph.invoke(initial_state)
        >>> print(result["final_output"]["status"])
        'success'

    Notes:
        - O graph é stateless - cada invocação é independente
        - Erros são capturados e tratados dentro dos nodes
        - O state flui através de todos os nodes sequencialmente
        - O router é o único ponto de decisão condicional
    """
    logger.info("Creating analytics executor graph")

    # Criar StateGraph com AnalyticsState
    graph = StateGraph(AnalyticsState)

    # ========================================================================
    # NODES - Adicionar todos os nodes ao graph
    # ========================================================================

    logger.debug("Adding nodes to graph")

    # Input parsing node
    graph.add_node("parse_input", parse_input_node)

    # Tool handler nodes (1 por chart type)
    graph.add_node("handle_bar_horizontal", tool_handle_bar_horizontal)
    graph.add_node("handle_bar_vertical", tool_handle_bar_vertical)
    # REMOVED: bar_vertical_composed (migrated to line_composed)
    graph.add_node("handle_bar_vertical_stacked", tool_handle_bar_vertical_stacked)
    graph.add_node("handle_line", tool_handle_line)
    graph.add_node("handle_line_composed", tool_handle_line_composed)
    graph.add_node("handle_pie", tool_handle_pie)
    graph.add_node("handle_histogram", tool_handle_histogram)
    graph.add_node("handle_null", tool_handle_null)

    # Output formatting node
    graph.add_node("format_output", format_output_node)

    logger.debug("All nodes added successfully")

    # ========================================================================
    # EDGES - Definir conexões entre nodes
    # ========================================================================

    logger.debug("Configuring edges")

    # Entry point - o fluxo sempre começa em parse_input
    graph.set_entry_point("parse_input")

    # CONDITIONAL EDGE: Router baseado em chart_type
    # Este é o ÚNICO ponto de decisão no graph
    # A função route_by_chart_type() retorna o nome do próximo node
    graph.add_conditional_edges(
        source="parse_input",
        path=route_by_chart_type,
        path_map={
            # Mapeamento: valor retornado pelo router → nome do node
            "bar_horizontal": "handle_bar_horizontal",
            "bar_vertical": "handle_bar_vertical",
            # REMOVED: bar_vertical_composed (migrated to line_composed)
            "bar_vertical_stacked": "handle_bar_vertical_stacked",
            "line": "handle_line",
            "line_composed": "handle_line_composed",
            "pie": "handle_pie",
            "histogram": "handle_histogram",
            "null": "handle_null",
            "format_output": "format_output",  # Rota direta em caso de erro
        },
    )

    logger.debug("Conditional routing configured")

    # CONVERGENCE: Todos os tool handlers convergem para format_output
    # Isso garante que independente do chart type, sempre formatamos a saída
    tool_nodes = [
        "handle_bar_horizontal",
        "handle_bar_vertical",
        # REMOVED: bar_vertical_composed
        "handle_bar_vertical_stacked",
        "handle_line",
        "handle_line_composed",
        "handle_pie",
        "handle_histogram",
        "handle_null",
    ]

    for node_name in tool_nodes:
        graph.add_edge(node_name, "format_output")

    logger.debug(f"Convergence edges added for {len(tool_nodes)} tool nodes")

    # FINAL EDGE: format_output → END
    graph.add_edge("format_output", END)

    logger.debug("Final edge to END configured")

    # ========================================================================
    # COMPILE - Compilar o graph para execução
    # ========================================================================

    logger.info("Compiling graph")
    compiled_graph = graph.compile()

    logger.info(
        "Analytics executor graph compiled successfully. "
        f"Nodes: {len(tool_nodes) + 2} (1 input + {len(tool_nodes)} tools + 1 output)"
    )

    return compiled_graph


def get_graph_structure() -> dict:
    """
    Retorna estrutura do graph para documentação e debugging.

    Útil para:
    - Documentação automática
    - Debugging de fluxos
    - Visualização de arquitetura
    - Testes de estrutura

    Returns:
        dict: Estrutura do graph com nodes, edges e routing

    Example:
        >>> structure = get_graph_structure()
        >>> print(structure["entry_point"])
        'parse_input'
        >>> print(len(structure["tool_nodes"]))
        9
        >>> print(structure["routing_logic"])
        'route_by_chart_type'
    """
    return {
        "entry_point": "parse_input",
        "exit_point": "END",
        "nodes": {
            "input": "parse_input",
            "output": "format_output",
            "tools": [
                "handle_bar_horizontal",
                "handle_bar_vertical",
                # REMOVED: bar_vertical_composed
                "handle_bar_vertical_stacked",
                "handle_line",
                "handle_line_composed",
                "handle_pie",
                "handle_histogram",
                "handle_null",
            ],
        },
        "routing_logic": "route_by_chart_type",
        "conditional_edges": {
            "source": "parse_input",
            "destinations": {
                "bar_horizontal": "handle_bar_horizontal",
                "bar_vertical": "handle_bar_vertical",
                # REMOVED: bar_vertical_composed
                "bar_vertical_stacked": "handle_bar_vertical_stacked",
                "line": "handle_line",
                "line_composed": "handle_line_composed",
                "pie": "handle_pie",
                "histogram": "handle_histogram",
                "null": "handle_null",
            },
        },
        "convergence": {
            "source": "all tool nodes",
            "destination": "format_output",
        },
        "description": (
            "LangGraph workflow for analytics execution. "
            "Routes chart specifications to appropriate tool handlers "
            "based on chart_type, executes DuckDB queries, and formats output."
        ),
    }


def validate_graph_structure() -> tuple[bool, str]:
    """
    Valida que o graph foi construído corretamente.

    Verifica:
    - Graph compila sem erros
    - Todos os 9 chart types têm handlers
    - Entry e exit points estão definidos
    - Convergência está configurada

    Returns:
        tuple: (is_valid, message)

    Example:
        >>> is_valid, msg = validate_graph_structure()
        >>> assert is_valid, msg
        >>> print(msg)
        'Graph structure is valid'
    """
    try:
        # Tentar compilar o graph
        graph = create_analytics_executor_graph()

        # Verificar que graph foi criado
        if graph is None:
            return False, "Graph compilation returned None"

        # Verificar estrutura
        structure = get_graph_structure()
        expected_tools = 9

        if len(structure["nodes"]["tools"]) != expected_tools:
            return (
                False,
                f"Expected {expected_tools} tool nodes, "
                f"got {len(structure['nodes']['tools'])}",
            )

        return True, "Graph structure is valid"

    except Exception as e:
        return False, f"Graph validation failed: {str(e)}"


# Expor funções principais
__all__ = [
    "create_analytics_executor_graph",
    "get_graph_structure",
    "validate_graph_structure",
]
