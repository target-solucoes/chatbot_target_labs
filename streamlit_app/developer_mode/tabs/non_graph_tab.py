import streamlit as st
from typing import Dict, Any
from ..utils.formatters import format_sql, format_dataframe


def render_non_graph_tab(data: Dict[str, Any]) -> None:
    st.markdown("### Non-Graph Executor (FASE 2)")

    if not data.get("is_active"):
        st.info("Non-Graph Executor nao foi acionado nesta requisicao.")
        return

    # ---------- Status badge ----------
    status = data.get("status", "unknown")
    status_color = {"success": "green", "error": "red", "partial": "orange"}.get(status, "gray")
    st.markdown(f"**Status:** :{status_color}[{status.upper()}]")

    st.divider()

    # ---------- 1. Input ----------
    st.markdown("#### 1. Input Interpretado")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Query:**")
        st.code(data.get("query") or "N/A", language=None)
    with col2:
        filters = data.get("filters_applied") or {}
        st.markdown("**Filtros Aplicados:**")
        if filters:
            st.json(filters)
        else:
            st.info("Nenhum filtro ativo.")

    st.divider()

    # ---------- 2. Classificacao ----------
    st.markdown("#### 2. Classificacao da Query")
    col3, col4, col5 = st.columns(3)
    with col3:
        st.metric("Query Type", data.get("query_type") or "N/A")
    with col4:
        st.metric("Execution Path", data.get("execution_path") or "legacy")
    with col5:
        st.metric("Intent Type", data.get("intent_type") or "N/A")

    # Aggregations & group_by (dynamic path)
    aggregations = data.get("aggregations")
    group_by = data.get("group_by")
    order_by = data.get("order_by")

    if aggregations or group_by or order_by:
        with st.expander("Detalhes do Intent (Dynamic Path)"):
            if aggregations:
                st.markdown("**Agregacoes:**")
                st.json(aggregations)
            if group_by:
                st.markdown(f"**Group By:** `{', '.join(group_by)}`")
            if order_by:
                st.markdown(f"**Order By:** `{order_by.get('column')} {order_by.get('direction', '').upper()}`")

    st.divider()

    # ---------- 3. Query SQL ----------
    sql_query = data.get("sql_query")
    if sql_query:
        st.markdown("#### 3. Query SQL Gerada (DuckDB)")
        st.code(format_sql(sql_query), language="sql")
        st.divider()

    # ---------- 4. Execucao & Output ----------
    section_num = 4 if sql_query else 3
    st.markdown(f"#### {section_num}. Resultado da Execucao")

    col6, col7 = st.columns(2)
    with col6:
        st.metric("Linhas Retornadas", data.get("row_count") or 0)
    with col7:
        st.metric("Engine", data.get("engine") or "DuckDB")

    if data.get("summary"):
        st.markdown("**Resumo:**")
        st.info(data["summary"])

    if data.get("conversational_response"):
        st.markdown("**Resposta Conversacional:**")
        st.markdown(data["conversational_response"])

    with st.expander("Preview de Dados (Top 100)"):
        if data.get("data_preview"):
            df = format_dataframe(data["data_preview"])
            st.dataframe(df, use_container_width=True)
        else:
            st.info("Nenhum dado retornado.")

    st.divider()

    # ---------- 5. Performance ----------
    section_num += 1
    st.markdown(f"#### {section_num}. Metricas de Performance")
    perf = data.get("performance_metrics") or {}
    if perf:
        p_cols = st.columns(len(perf))
        labels = {
            "total_time": "Tempo Total",
            "classification_time": "Classificacao",
            "execution_time": "Execucao SQL",
            "llm_time": "LLM",
        }
        for idx, (key, val) in enumerate(perf.items()):
            label = labels.get(key, key)
            p_cols[idx].metric(label, f"{val:.3f}s")
    else:
        st.info("Metricas de performance nao disponiveis.")

    tokens = data.get("total_tokens") or {}
    if tokens.get("total_tokens"):
        st.markdown(
            f"**Tokens:** Input `{tokens.get('input_tokens', 0)}` | "
            f"Output `{tokens.get('output_tokens', 0)}` | "
            f"Total `{tokens.get('total_tokens', 0)}`"
        )

    # ---------- 6. Erro (se houver) ----------
    error = data.get("error")
    if error:
        st.divider()
        st.markdown("#### Erro Detectado")
        st.error(f"**{error.get('type', 'Error')}:** {error.get('message', '')}")

    st.divider()

    # ---------- 7. JSON Completo ----------
    section_num += 1
    with st.expander(f"JSON Completo do Non-Graph Output"):
        st.json(data.get("raw_output") or {})
