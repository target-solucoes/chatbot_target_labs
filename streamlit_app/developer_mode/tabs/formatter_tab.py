import streamlit as st
from typing import Dict, Any

def render_formatter_tab(data: Dict[str, Any]):
    st.markdown("### Formatter Agent (FASE 4)")
    
    if not data or list(data.keys()) == []:
        st.warning("Formatter Output não disponível (A fase pode ter sido pulada para uma query non-graph)")
        return
        
    st.success("JSON Consolidado pelo Agent Formatter.")
    
    with st.expander("Visão Executiva (Executive Summary)", expanded=True):
        st.json(data.get("executive_summary", {}))
        
    with st.expander("Visualização & Insights Sintetizados", expanded=True):
        st.json({
            "visualization": data.get("visualization"),
            "insights": data.get("insights")
        })
        
    with st.expander("Next Steps Sugeridos"):
        st.json(data.get("next_steps", []))
        
    with st.expander("Output JSON Completo"):
        st.json(data)
