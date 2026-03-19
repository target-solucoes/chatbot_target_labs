import streamlit as st
from typing import Dict, Any

def render_classifier_tab(data: Dict[str, Any]):
    st.markdown("### Graphic Classifier (FASE 1)")
    
    col1, col2 = st.columns(2)
    col1.metric("Chart Type", str(data.get('chart_type', 'None')))
    col2.metric("Confiança", f"{data.get('confidence', 0.0):.2f}")
    
    st.markdown("**Intent Classificado:**")
    st.info(str(data.get('intent', 'unknown')))
    
    st.markdown("---")
    
    with st.expander("Âncora Semântica & Validação", expanded=True):
        st.json({
            "semantic_anchor": data.get("semantic_anchor"),
            "semantic_validation": data.get("semantic_validation"),
            "semantic_mapping": data.get("semantic_mapping")
        })
        
    with st.expander("Métricas & Dimensões", expanded=True):
        col_m, col_d = st.columns(2)
        with col_m:
            st.markdown("**Métricas:**")
            st.json(data.get("metrics", []))
        with col_d:
            st.markdown("**Dimensões:**")
            st.json(data.get("dimensions", []))
            
    with st.expander("Output Completo (ChartOutput JSON)"):
        st.json(data.get("output", {}))
        
    with st.expander("Legacy Outputs (Entidades & Keywords)"):
        st.json({
            "parsed_entities": data.get("parsed_entities"),
            "detected_keywords": data.get("detected_keywords")
        })
