# -*- coding: utf-8 -*-
"""
Display Components for Streamlit Chatbot

Rendering functions for each component of the formatter JSON output.
"""

import streamlit as st
from typing import Dict, List, Any, Optional
import streamlit.components.v1 as components
import re


# ============================================================================
# Helper Functions
# ============================================================================


def _escape_special_chars(text: str) -> str:
    r"""
    Escape special characters that cause rendering issues in Streamlit markdown.

    Handles:
    - $ → \$ (prevents LaTeX math mode interpretation, critical for R$ currency)
    - ` → ' (prevents unintended inline code blocks from product names/data)
    - Stray single * cleaned up (prevents broken italic from unmatched asterisks)

    Note: _ (underscore) is intentionally NOT escaped — it only triggers italic
    in markdown when used as _word_ boundaries, and most occurrences in data
    (e.g., UF_Cliente, Cod_Vendedor) are mid-word where markdown ignores them.

    Args:
        text: Text to escape

    Returns:
        Text with special characters safely escaped for Streamlit rendering
    """
    if not text:
        return text

    # Escape dollar signs that are not already escaped
    # This prevents R$ from being interpreted as LaTeX math delimiters
    text = re.sub(r"(?<!\\)\$", r"\$", text)

    # Replace backticks with single quotes to prevent inline code blocks
    text = text.replace("`", "'")

    # Clean up stray single * that are not part of ** bold pairs
    # A stray * would cause broken italic rendering
    # Strategy: find * not adjacent to another *, replace with empty
    text = re.sub(r"(?<!\*)\*(?!\*)", "", text)

    return text


def _bold_numbers(text: str) -> str:
    """
    Automatically highlights numbers in text by applying bold formatting.

    Detects and bolds:
    - Percentages: 7,38%, 18.64%
    - Monetary values with suffixes: 24.46M, 814.95M, 1.5K, 3.2B
    - Currency values: R$ 1.234,56
    - Multipliers: 12,54x, 3.2x
    - Numbers with thousand separators: 1.234.567 (BR) or 1,234,567 (US)
    - Decimal numbers: 123,45 or 123.45
    - Integer numbers: 12345
    - Negative values: -5.401, -34,1%

    FASE 5C Strategy:
    1. Strip ALL existing ** markers for a clean slate (prevents ****artifacts)
    2. Protect R$ currency prefix with placeholder
    3. Apply bold uniformly to all numeric expressions
    4. Merge R$ prefix into adjacent bold numbers
    5. Clean up artifacts and escape special chars

    Args:
        text: Text to process

    Returns:
        Text with numbers wrapped in ** for markdown bold, and special chars escaped
    """
    if not text:
        return text

    # STEP 0: Remove ALL existing ** markers for a clean slate.
    # The LLM may output **bold** around numbers, entity names, etc.
    # Stripping all ensures no possibility of nested/double bold (****).
    # Entity emphasis (e.g., **Santa Catarina**) is sacrificed for rendering
    # consistency — numbers will be re-bolded uniformly below.
    text = text.replace("**", "")

    # STEP 1: Protect R$ currency prefix with placeholder.
    # This allows us to detect and merge R$ with adjacent numbers in step 4.
    _CURRENCY_PH = "\x00CIFRAO\x00"
    text = text.replace("R$", _CURRENCY_PH)

    # STEP 2: Apply bold to all numeric expressions.
    # Pattern breakdown:
    #   -?              optional negative sign
    #   \d              at least one digit to start
    #   [\d.,]*         more digits, dots (thousands BR), commas (decimal BR)
    #   \d              must end with a digit (prevents trailing . or ,)
    #   [%MKBx]?        optional magnitude/unit suffix
    # OR just a single digit optionally with suffix.
    # Lookbehind: not preceded by word char or * (avoid mid-word/double-bold)
    # Lookahead: not followed by word char or * (avoid mid-word/double-bold)
    _NUM_PATTERN = re.compile(
        r"(?<![\w*])"
        r"-?"
        r"(?:\d[\d.,]*\d|\d)"  # multi-digit with separators, or single digit
        r"[%MKBx]?"
        r"(?![\w*])",
        re.IGNORECASE,
    )
    text = _NUM_PATTERN.sub(r"**\g<0>**", text)

    # STEP 3: Merge R$ placeholder with adjacent bolded number.
    # Transforms: "\x00CIFRAO\x00 **24.463.356**" → "**R$ 24.463.356**"
    # Handles optional whitespace between R$ and the number.
    text = re.sub(
        r"\x00CIFRAO\x00\s*\*\*(-?[\d.,]+[MKBx]?)\*\*",
        r"**R$ \1**",
        text,
        flags=re.IGNORECASE,
    )

    # Restore any remaining R$ placeholders not adjacent to bolded numbers
    # (e.g., "R$" mentioned in isolation without a numeric value)
    text = text.replace(_CURRENCY_PH, "R$")

    # STEP 4: Clean up potential artifacts.
    # Remove empty bold markers that could result from edge cases.
    text = re.sub(r"\*\*\s*\*\*", "", text)

    # STEP 5: Escape special characters for safe Streamlit markdown rendering.
    return _escape_special_chars(text)


# ============================================================================
# Main Rendering Functions
# ============================================================================


def render_executive_summary(summary: Dict) -> None:
    """
    Render executive summary section (title + introduction).

    FASE 5B: Title comes from LLM-generated 'titulo' field (via executive_summary.title).
    Introduction comes from LLM-generated 'contexto' field (via executive_summary.introduction).
    Both are semantically distinct - title is a short descriptor, introduction contextualizes
    the analysis scope and filters without duplicating the main response content.

    Args:
        summary: executive_summary dict from formatter output
    """
    if not summary:
        return

    # Title: clean any residual bold markers before rendering in H3
    title = summary.get("title", "Análise de Dados")
    clean_title = title.replace("**", "")
    st.markdown(f"### {clean_title}")

    # Introduction: contextualizes filters/scope (distinct from main response)
    introduction = summary.get("introduction", "")
    if introduction:
        st.markdown(introduction)


def render_filters_badge(filters: Dict[str, Any]) -> None:
    """
    Render active filters as colored badges

    Args:
        filters: Dictionary of active filters
    """
    if not filters:
        return

    # Create columns for badges
    cols = st.columns(min(len(filters), 4))

    for idx, (key, value) in enumerate(filters.items()):
        col_idx = idx % 4
        with cols[col_idx]:
            # Clean key name (remove underscores, capitalize)
            clean_key = key.replace("_", " ").title()
            st.markdown(
                f'<span style="background-color: #e3f2fd; color: #1976d2; '
                f"padding: 4px 12px; border-radius: 12px; font-size: 0.85em; "
                f'display: inline-block; margin: 2px;">'
                f"{clean_key}: <b>{value}</b></span>",
                unsafe_allow_html=True,
            )


def render_plotly_chart(chart_data: Dict) -> None:
    """
    Render Plotly chart from HTML string.

    Supports dynamic height: if the Plotly figure specifies a height in its
    layout, the iframe will match it. Otherwise falls back to 500px.

    Args:
        chart_data: Chart dict containing 'html' and metadata
    """
    if not chart_data:
        return

    html_content = chart_data.get("html", "")

    if html_content:
        # Try to extract the figure height from the Plotly HTML layout
        # Plotly embeds it as "height": NNN in the JSON config
        chart_height = 500  # default fallback
        height_match = re.search(r'"height"\s*:\s*(\d+)', html_content)
        if height_match:
            chart_height = int(height_match.group(1))

        # Add a small buffer for padding
        components.html(html_content, height=chart_height + 20, scrolling=False)
    else:
        st.warning("Grafico nao disponivel")


def render_insights(
    insights: Dict,
    resposta: str = "",
    dados_destacados: Optional[List[str]] = None,
) -> None:
    """
    Render insights section using FASE 2 native fields.

    FASE 5B Redesign:
    - Uses 'resposta' directly as the main analysis text (Resumo Executivo)
    - Uses 'dados_destacados' as bullet points (Principais Achados)
    - Eliminates duplicate narrative/introduction content
    - Eliminates triple repetition (content=formula=interpretation)
    - Eliminates generic "Destaque N" titles

    Falls back to legacy insights dict if native fields are not available.

    Args:
        insights: insights dict from formatter output (legacy fallback)
        resposta: FASE 2 native response text (primary)
        dados_destacados: FASE 2 native key findings list (primary)
    """
    # Determine content source: prefer FASE 2 native fields
    main_text = resposta or insights.get("narrative", "") if insights else resposta
    highlights = dados_destacados if dados_destacados else []

    # Fallback to legacy detailed_insights if no dados_destacados
    legacy_insights = []
    if not highlights and insights:
        legacy_insights = insights.get("detailed_insights", [])

    if not main_text and not highlights and not legacy_insights:
        return

    st.markdown("---")
    st.markdown("#### 📊 Insights")

    # ========================================================================
    # Section 1: Resumo Executivo (main response text)
    # ========================================================================
    if main_text:
        st.markdown("##### 📌 Resumo Executivo")
        main_text_formatted = _bold_numbers(main_text)
        st.markdown(main_text_formatted)
        st.markdown("")  # spacing

    # ========================================================================
    # Section 2: Principais Achados (key findings as bullet points)
    # ========================================================================
    if highlights:
        st.markdown("##### 🔍 Principais Achados")
        st.markdown("")  # spacing
        for dado in highlights:
            dado_formatted = _bold_numbers(dado)
            st.markdown(f"- {dado_formatted}")
    elif legacy_insights:
        # Legacy fallback: render detailed_insights without triple repetition
        st.markdown("##### 🔍 Principais Achados")
        st.markdown("")  # spacing
        for insight in legacy_insights:
            interpretation = insight.get("interpretation", "")
            if interpretation:
                interpretation_formatted = _bold_numbers(interpretation)
                st.markdown(f"- {interpretation_formatted}")


def render_next_steps(next_steps: Dict) -> None:
    """
    Render next steps section (3 direct strategic recommendations)

    Args:
        next_steps: next_steps dict from formatter output
    """
    if not next_steps:
        return

    st.markdown("---")
    st.markdown("#### 🧭 Próximos Passos")

    # Get next steps items (exactly 3)
    items = next_steps.get("items", [])

    if items:
        for step in items:
            st.markdown(f"- {step}")
    else:
        st.info("Nenhum proximo passo disponivel no momento.")


def render_data_table(data: Dict) -> None:
    """
    Render data table section

    Args:
        data: data dict from formatter output
    """
    if not data:
        return

    st.markdown("---")

    # Summary Table
    summary_table = data.get("summary_table", {})
    if summary_table:
        headers = summary_table.get("headers", [])
        rows = summary_table.get("rows", [])
        total_rows = summary_table.get("total_rows", 0)
        showing_rows = summary_table.get("showing_rows", 0)

        if headers and rows:
            # Create dataframe for display
            import pandas as pd

            df = pd.DataFrame(rows, columns=headers)
            st.dataframe(df, use_container_width=True)


def render_metadata_debug(metadata: Dict) -> None:
    """
    Render metadata and debug information

    Args:
        metadata: metadata dict from formatter output
    """
    # Metadata/debug rendering removed per user request
    return


def render_error(error_info: Any) -> None:
    """
    Render error message

    Args:
        error_info: Error information (string or dict)
    """
    if isinstance(error_info, dict):
        error_msg = error_info.get("message", str(error_info))
    else:
        error_msg = str(error_info)

    st.error(f"Erro ao processar consulta: {error_msg}")


def render_loading_state(message: str = "Processando...") -> None:
    """
    Render loading state with spinner

    Args:
        message: Loading message to display
    """
    with st.spinner(message):
        st.empty()


def render_non_graph_response(non_graph_output: Dict) -> None:
    """
    Render non-graph executor output (summary-only display)

    For non_graph_output, we display the appropriate content:
    - 'conversational_response' for conversational queries
    - 'summary' for data queries (metadata, aggregation, lookup, etc.)
    - 'data' as dataframe for tabular queries

    Args:
        non_graph_output: Complete non_graph JSON output
    """
    if not non_graph_output:
        render_error("Nenhuma resposta disponivel")
        return

    status = non_graph_output.get("status", "unknown")

    if status == "error":
        error = non_graph_output.get("error")
        render_error(error)
        return

    query_type = non_graph_output.get("query_type")

    # For TABULAR queries, render data as dataframe
    if query_type == "tabular":
        data = non_graph_output.get("data")

        if data and isinstance(data, list) and len(data) > 0:
            import pandas as pd

            df = pd.DataFrame(data)

            # Display summary if available
            summary = non_graph_output.get("summary")
            if summary:
                response_formatted = _bold_numbers(summary)
                st.markdown(response_formatted)

            # Display dataframe
            st.dataframe(df, use_container_width=True)
        else:
            st.info("Nenhum dado disponível para exibição tabular.")
        return

    # For OTHER query types, display text response
    # Extract text response - check conversational_response first, then summary
    conversational_response = non_graph_output.get("conversational_response")
    summary = non_graph_output.get("summary")

    # Prioritize conversational_response for conversational queries
    response_text = conversational_response if conversational_response else summary

    if response_text:
        # Apply auto-bold formatting to numbers in response
        response_formatted = _bold_numbers(response_text)
        st.markdown(response_formatted)
    else:
        st.info("Resposta processada com sucesso, mas sem resumo disponível.")


def render_complete_response(formatter_output: Dict) -> None:
    """
    Render complete formatter output in correct order

    This is a convenience function that renders all sections in the proper sequence.

    Args:
        formatter_output: Complete formatter JSON output
    """
    if not formatter_output:
        render_error("Nenhuma resposta disponivel")
        return

    status = formatter_output.get("status", "unknown")

    if status == "error":
        error = formatter_output.get("error")
        render_error(error)
        return

    # Render in correct order
    # 1. Executive Summary (title + introduction)
    executive_summary = formatter_output.get("executive_summary", {})
    render_executive_summary(executive_summary)

    # 2. Visualization (chart)
    st.markdown("")
    visualization = formatter_output.get("visualization", {})
    chart = visualization.get("chart", {})
    render_plotly_chart(chart)

    # 3. Insights (FASE 5B: use native fields directly)
    insights = formatter_output.get("insights", {})
    resposta = formatter_output.get("resposta", "")
    dados_destacados = formatter_output.get("dados_destacados", [])
    render_insights(insights, resposta=resposta, dados_destacados=dados_destacados)

    # 4. Next Steps
    next_steps = formatter_output.get("next_steps", {})
    render_next_steps(next_steps)

    # 5. Data Table
    data = formatter_output.get("data", {})
    render_data_table(data)

    # Metadata/Debug rendering removed per user request


def render_unified_response(output_type: str, output_data: Dict) -> None:
    """
    Unified renderer that handles both non_graph and formatter outputs.

    This is the recommended function to use in app.py for rendering any pipeline output.

    Args:
        output_type: Type of output ("non_graph" or "formatter")
        output_data: The output data dictionary
    """
    if output_type == "non_graph":
        render_non_graph_response(output_data)
    elif output_type == "formatter":
        render_complete_response(output_data)
    else:
        render_error(f"Tipo de output desconhecido: {output_type}")
