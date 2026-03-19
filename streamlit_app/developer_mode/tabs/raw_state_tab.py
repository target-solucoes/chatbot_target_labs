import streamlit as st
import json
from typing import Dict, Any
from ..utils.formatters import format_json

def render_raw_state_tab(final_state: Dict[str, Any]):
    st.markdown("### Visualização do Raw State Completo")
    st.info("Este é o dump completo do state do pipeline para diagnóstico avançado.")
    
    try:
        json_str = format_json(final_state)
        st.download_button(
            label="💾 Download Raw State JSON",
            data=json_str,
            file_name="pipeline_raw_state.json",
            mime="application/json"
        )
        st.json(final_state)
    except Exception as e:
        st.error(f"Erro ao exibir estado completo: {str(e)}")
        st.write(final_state)
