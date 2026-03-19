import streamlit as st
from typing import Dict, Any

def render_insights_tab(insight_data: Dict[str, Any], plotly_data: Dict[str, Any]):
    st.markdown("### Insights & Visualização (FASE 3)")
    
    # Plotly Generator Configs
    st.markdown("#### Plotly Generator")
    if plotly_data.get("status"):
        st.info(f"Tipo de Gráfico: **{plotly_data.get('chart_type', 'N/A')}** | Status: **{plotly_data.get('status', 'N/A')}**")
        st.markdown(f"**FilePath**: `{plotly_data.get('file_path', 'N/A')}`")
        with st.expander("Ver Plotly Config (Renderizado)", expanded=False):
            st.json(plotly_data.get("config", {}))
    else:
        st.warning("Nenhum dado do Plotly Generator disponível.")
        
    st.markdown("---")
    
    # Insight Generator Configs
    st.markdown("#### Insight Generator")
    status = insight_data.get("status", "unknown")
    if status == "success":
        st.success("Insights gerados com sucesso")
        
        with st.expander("Insights Formatados (Markdown)", expanded=True):
            st.markdown(insight_data.get("formatted_insights", "N/A"))
            
        with st.expander("Insights Brutos (Lista)"):
            st.json(insight_data.get("insights", []))
            
        with st.expander("Detailed Insights"):
            st.json(insight_data.get("detailed_insights", {}))
            
    else:
        st.warning(f"Insights não foram gerados ou falharam. Status: {status}")
