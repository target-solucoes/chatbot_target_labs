import streamlit as st
from typing import Dict, Any
from ..utils.formatters import format_sql, format_dataframe

def render_executor_tab(data: Dict[str, Any], non_graph_data: Dict[str, Any]):
    st.markdown("### Executor (FASE 2)")
    
    if data.get("sql_query"):
        st.markdown("##### 🔍 Query SQL Gerada (Analytics Executor)")
        st.code(format_sql(data["sql_query"]), language="sql")
        
        st.markdown(f"**Engine**: `{data.get('engine_used', 'N/A')}` | **Linhas**: `{data.get('row_count', 0)}` Status: `{data.get('status', 'unknown')}`")
        
        with st.expander("Visualizar Preview de Dados (Top 100)"):
            if data.get("data_preview"):
                df = format_dataframe(data["data_preview"])
                st.dataframe(df, use_container_width=True)
            else:
                st.info("Nenhum dado gerado.")
                
        with st.expander("Plotly Config Gerada"):
            st.json(data.get("plotly_config", {}))
            
    elif non_graph_data.get("result") or non_graph_data.get("conversational_response"):
        st.markdown("##### 📝 Saída Textual (Non-Graph Executor)")
        st.info(f"**Query Type**: {non_graph_data.get('query_type', 'unknown')}")
        st.markdown(non_graph_data.get("conversational_response", ""))
        
        with st.expander("Ver Output Completo (Non-Graph)"):
            st.json(non_graph_data)
            
        with st.expander("Visualizar Preview de Dados Suporte (Top 100)"):
            if non_graph_data.get("data_preview"):
                df = format_dataframe(non_graph_data["data_preview"])
                st.dataframe(df, use_container_width=True)
            else:
                st.info("Nenhum dado de suporte fornecido.")
                
    else:
        st.warning("Nenhum dado de execução encontrado no estado final.")
