import streamlit as st
from typing import Any, List, Dict, Optional

# Extratores de estado
from .utils.state_extractor import (
    extract_filter_data,
    extract_graphic_classifier_data,
    extract_analytics_executor_data,
    extract_non_graph_executor_data,
    extract_insight_generator_data,
    extract_plotly_generator_data,
    extract_formatter_data,
    extract_global_metrics
)

# Extrator de traces (inputs/outputs por agente e tool)
from .utils.trace_extractor import extract_all_traces

# Tabs de renderizacao
from .tabs import (
    render_pipeline_overview,
    render_filter_tab,
    render_classifier_tab,
    render_executor_tab,
    render_non_graph_tab,
    render_insights_tab,
    render_formatter_tab,
    render_performance_tab,
    render_raw_state_tab
)


def _render_io_section(trace: Dict[str, Any]) -> None:
    """
    Render Input/Output expanders for an agent trace.

    Displays 4 expanders:
    - Input (Geral): raw input received by the agent
    - Input (Tools): inputs sent to each tool within the agent
    - Output (Geral): raw output produced by the agent
    - Output (Tools): outputs returned by each tool within the agent
    """
    if not trace:
        return

    st.markdown("---")
    st.markdown("#### Rastreio de Inputs & Outputs")

    # --- Input (Geral) ---
    with st.expander("Input (Geral)", expanded=False):
        agent_input = trace.get("agent_input", {})
        if agent_input and any(v is not None for v in agent_input.values()):
            st.json(agent_input)
        else:
            st.info("Nenhum input registrado para este agente.")

    # --- Input (Tools) ---
    with st.expander("Input (Tools)", expanded=False):
        tools = trace.get("tools", [])
        if not tools:
            st.info("Nenhuma tool registrada para este agente.")
        else:
            for i, tool in enumerate(tools):
                st.markdown(f"**`{tool['name']}`**")
                if tool.get("description"):
                    st.caption(tool["description"])
                tool_input = tool.get("input", {})
                if tool_input:
                    st.json(tool_input)
                else:
                    st.caption("Sem input.")
                if i < len(tools) - 1:
                    st.markdown("---")

    # --- Output (Geral) ---
    with st.expander("Output (Geral)", expanded=False):
        agent_output = trace.get("agent_output", {})
        if agent_output and any(v is not None for v in agent_output.values()):
            st.json(agent_output)
        else:
            st.info("Nenhum output registrado para este agente.")

    # --- Output (Tools) ---
    with st.expander("Output (Tools)", expanded=False):
        tools = trace.get("tools", [])
        if not tools:
            st.info("Nenhuma tool registrada para este agente.")
        else:
            for i, tool in enumerate(tools):
                st.markdown(f"**`{tool['name']}`**")
                if tool.get("description"):
                    st.caption(tool["description"])
                tool_output = tool.get("output", {})
                if tool_output:
                    st.json(tool_output)
                else:
                    st.caption("Sem output.")
                if i < len(tools) - 1:
                    st.markdown("---")


def render_developer_panel(
    result: Any = None,
    state_snapshots: Optional[List[Dict]] = None,
    performance_monitor: Optional[Any] = None,
) -> None:
    """
    Renderiza o painel Developer Mode.

    Args:
        result: Resultado completo da execucao do pipeline.
        state_snapshots: Lista de snapshots intermediarios (opcional).
        performance_monitor: Monitor de performance da sessao (opcional).
    """
    with st.expander("Developer Mode", expanded=False):
        st.markdown("### Debug e Observabilidade")

        # Recupera estado final para renderizar - tenta da lista de snapshots senao cria dict vazio
        final_state = state_snapshots[-1] if state_snapshots and len(state_snapshots) > 0 else {}

        if not final_state:
            st.warning("Nenhum dado capturado no Developer Mode para esta requisicao.")
            return

        non_graph_data = extract_non_graph_executor_data(final_state)
        is_non_graph = non_graph_data.get("is_active", False)

        # Extract all agent traces for I/O display
        traces = extract_all_traces(final_state)

        # Estrutura de tabs -- aba Non-Graph Debug aparece somente quando acionada
        tab_labels = [
            "Pipeline Overview",
            "Filter (FASE 0)",
            "Classifier (FASE 1)",
            "Executor (FASE 2)",
        ]
        if is_non_graph:
            tab_labels.append("Non-Graph Debug")
        tab_labels += [
            "Insights & Plotly (FASE 3)",
            "Formatter (FASE 4)",
            "Performance",
            "Raw State",
        ]

        tabs = st.tabs(tab_labels)

        try:
            idx = 0
            with tabs[idx]:
                render_pipeline_overview(final_state, extract_global_metrics(final_state))
            idx += 1

            with tabs[idx]:
                render_filter_tab(extract_filter_data(final_state))
                _render_io_section(traces.get("filter_classifier"))
            idx += 1

            with tabs[idx]:
                render_classifier_tab(extract_graphic_classifier_data(final_state))
                _render_io_section(traces.get("graphic_classifier"))
            idx += 1

            with tabs[idx]:
                render_executor_tab(extract_analytics_executor_data(final_state), non_graph_data)
                if is_non_graph:
                    _render_io_section(traces.get("non_graph_executor"))
                else:
                    _render_io_section(traces.get("analytics_executor"))
            idx += 1

            if is_non_graph:
                with tabs[idx]:
                    render_non_graph_tab(non_graph_data)
                    _render_io_section(traces.get("non_graph_executor"))
                idx += 1

            with tabs[idx]:
                render_insights_tab(extract_insight_generator_data(final_state), extract_plotly_generator_data(final_state))
                st.markdown("---")
                st.markdown("##### Traces: Insight Generator")
                _render_io_section(traces.get("insight_generator"))
                st.markdown("##### Traces: Plotly Generator")
                _render_io_section(traces.get("plotly_generator"))
            idx += 1

            with tabs[idx]:
                render_formatter_tab(extract_formatter_data(final_state))
                _render_io_section(traces.get("formatter"))
            idx += 1

            with tabs[idx]:
                render_performance_tab(extract_global_metrics(final_state))
            idx += 1

            with tabs[idx]:
                render_raw_state_tab(final_state)

        except Exception as e:
            st.error(f"Erro ao renderizar abas do Developer Mode: {e}")
