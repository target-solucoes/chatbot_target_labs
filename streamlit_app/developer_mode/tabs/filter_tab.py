import streamlit as st
from typing import Dict, Any

def render_filter_tab(data: Dict[str, Any]):
    st.markdown("### Filter Classifier (Arquitetura Avançada - Otimizada)")
    
    if data.get("skipped"):
        st.warning("A Fase 0 (Filter Classifier) foi pulada pois a query não continha filtros ou necessidade de classificação condicional.")
        return

    st.markdown(f"**Query**: `{data.get('query', '')}`")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Filtros Anteriores:**")
        st.json(data.get("current_filters", {}))
        
    with col2:
        st.markdown("**Filtros Detectados e Consolidados (Ativos):**")
        st.json(data.get("filter_final", {}))
        
    st.markdown("---")
    
    with st.expander("Operações CRUD de Filtros"):
        st.json(data.get("filter_operations", {}))
        
    with st.expander("Histórico de Filtros e Colunas Mencionadas"):
        st.json({
            "detected_filter_columns": data.get("detected_filter_columns"),
            "filter_history": data.get("filter_history")
        })
