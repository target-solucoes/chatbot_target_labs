import streamlit as st
from typing import Dict, Any

def render_pipeline_overview(final_state: Dict[str, Any], global_metrics: Dict[str, Any]):
    st.markdown("### Visão Geral da Execução")
    
    # Phase 0
    st.markdown("#### FASE 0: Filter Classifier")
    if "filter_final" in final_state or "output" in final_state:
        st.success("Executado (filtros aplicados ou modificados)")
        with st.expander("Ver Filtros Ativos"):
            st.json(final_state.get("filter_final", {}))
    else:
        st.info("Ignorado (sem contexto de filtros)")

    # Phase 1
    st.markdown("#### FASE 1: Graphic Classifier")
    chart_type = final_state.get("output", {}).get("chart_type") if isinstance(final_state.get("output"), dict) else None
    confidence = final_state.get("confidence", 0.0)
    intent = final_state.get("intent", "unknown")
    if chart_type:
        st.success(f"Executado (Chart: {chart_type} | Intent: {intent} | Trust: {confidence:.2f})")
    else:
        st.warning(f"Executado (Sem tipo de gráfico | Intent: {intent})")
        
    # Roteamento
    st.markdown("---")
    if chart_type:
        st.info(f"Roteamento: route_after_classifier -> **analytics_executor** (chart_type={chart_type})")
    else:
        st.info("Roteamento: route_after_classifier -> **non_graph_executor** (sem chart_type)")
        
    st.markdown("---")

    # Phase 2
    st.markdown("#### FASE 2: Executor")
    if final_state.get("executor_output"):
        st.success("Analytics Executor executado com sucesso.")
        st.write(f"Linhas resultantes: {final_state.get('executor_output', {}).get('row_count', 0)}")
    elif final_state.get("non_graph_output"):
        st.success("Non-Graph Executor executado com sucesso.")
    else:
        st.error("Nenhum executor foi executado com sucesso ou gerou saída.")
        
    # Phase 3
    st.markdown("#### FASE 3: Insight & Plotly Generator")
    if final_state.get("insight_result"):
        st.success("Insight Generator executado.")
    else:
        st.info("Insight Generator ignorado ou não concluído.")
        
    if final_state.get("plotly_output"):
        st.success("Plotly Generator executado.")
    else:
        st.info("Plotly Generator ignorado ou não concluído.")

    # Phase 4
    st.markdown("#### FASE 4: Formatter Agent")
    if final_state.get("formatter_output"):
        st.success("Formatter consolidou os resultados finais.")
    else:
        st.info("Formatter ignorado.")

    st.markdown("---")
    st.metric("Tempo Total", f"{global_metrics.get('total_time', 0):.3f}s")
