"""
State definition for LangGraph analytics executor workflow.

Defines the shared state structure that flows through all nodes
in the analytics execution pipeline.
"""

from typing import TypedDict, Optional, Dict, Any
import pandas as pd


class AnalyticsState(TypedDict, total=False):
    """
    State compartilhado entre todos os nós do LangGraph.

    Este state é passado através de todos os nodes no workflow do
    analytics executor, permitindo que cada node leia e atualize
    informações conforme necessário.

    Fluxo de dados:
    1. parse_input_node: preenche chart_spec, schema, data_source_path
    2. router: lê chart_spec.chart_type para decisão de roteamento
    3. tool_handle_*: preenche result_dataframe, plotly_config, sql_query
    4. format_output_node: lê todos os campos e gera final_output

    Attributes:
        chart_spec: ChartOutput do graphic_classifier contendo todas as
            especificações do gráfico (tipo, dimensions, metrics, etc)
        schema: Dicionário mapeando nomes de colunas para tipos de dados
        data_source_path: Caminho absoluto para o arquivo de dados (Parquet/CSV)

        result_dataframe: DataFrame pandas com o resultado da query SQL
        sql_query: Query SQL completa que foi executada
        execution_success: Flag indicando se a execução foi bem-sucedida
        engine_used: Nome do engine usado para execução (sempre "DuckDB")

        plotly_config: Configuração Plotly completa para renderizar o gráfico

        final_output: Resultado final formatado, pronto para ser retornado

        error_message: Mensagem de erro se execution_success = False
    """

    # ========================================================================
    # INPUT (preenchido por parse_input_node)
    # ========================================================================

    chart_spec: Dict[str, Any]
    """ChartOutput validado do graphic_classifier com especificações completas"""

    schema: Dict[str, str]
    """Schema do dataset: {column_name: data_type}"""

    data_source_path: str
    """Caminho absoluto para o arquivo de dados (Parquet ou CSV)"""

    # ========================================================================
    # EXECUTION (preenchido por tool_handle_*)
    # ========================================================================

    result_dataframe: Optional[pd.DataFrame]
    """Resultado da query SQL como DataFrame pandas"""

    sql_query: Optional[str]
    """Query SQL completa que foi executada"""

    execution_success: bool
    """True se a execução foi bem-sucedida, False em caso de erro"""

    engine_used: str
    """Nome do engine de execução (sempre "DuckDB" na nova arquitetura)"""

    # ========================================================================
    # VISUALIZATION (preenchido por tool_handle_*)
    # ========================================================================

    plotly_config: Optional[Dict[str, Any]]
    """
    Configuração Plotly completa para renderizar o gráfico.
    
    Estrutura típica:
    {
        "data": [...],  # Lista de traces
        "layout": {     # Configuração de layout
            "title": "...",
            "xaxis": {...},
            "yaxis": {...},
            ...
        }
    }
    """

    # ========================================================================
    # OUTPUT (preenchido por format_output_node)
    # ========================================================================

    final_output: Optional[Dict[str, Any]]
    """
    Resultado final formatado, pronto para ser retornado ao cliente.
    
    Estrutura típica:
    {
        "chart_type": "bar_horizontal",
        "data": [...],  # Dados em formato JSON
        "plotly_config": {...},
        "sql_query": "...",
        "engine_used": "DuckDB",
        "metadata": {...}
    }
    """

    # ========================================================================
    # ERROR HANDLING
    # ========================================================================

    error_message: Optional[str]
    """Mensagem de erro detalhada se execution_success = False"""
