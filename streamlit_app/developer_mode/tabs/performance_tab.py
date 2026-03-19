import streamlit as st
from typing import Dict, Any
from ..utils.formatters import format_tokens
import pandas as pd

def render_performance_tab(global_metrics: Dict[str, Any]):
    st.markdown("### Métricas de Performance e Custos")
    
    total_time = global_metrics.get("total_time", 0.0)
    st.metric("Tempo de Execução End-to-End", f"{total_time:.3f}s")
    
    st.markdown("---")
    st.markdown("#### Consumo de Tokens (LLMs)")
    
    agent_tokens = global_metrics.get("agent_tokens", {})
    if agent_tokens:
        token_data = format_tokens(agent_tokens)
        df_tokens = pd.DataFrame(token_data)
        st.dataframe(df_tokens, use_container_width=True)
    else:
        st.info("Dados de consumo de tokens não disponíveis.")
        
    st.markdown("---")
    st.markdown("#### Erros Registrados")
    errors = global_metrics.get("errors", [])
    if errors:
        for error in errors:
            st.error(error)
    else:
        st.success("Nenhum erro registrado neste pipeline.")
