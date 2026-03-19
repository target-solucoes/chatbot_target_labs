"""
Tool Handler for Null Charts.

This module implements the specific logic for null charts (queries without visualization),
including:
- Minimal validation requirements
- SQL generation for aggregated data without visualization
- Simple data output without Plotly configuration
"""

from typing import Dict, Any
import pandas as pd
import logging

from .base import BaseToolHandler

logger = logging.getLogger(__name__)


class ToolHandlerNull(BaseToolHandler):
    """
    Tool handler para null charts (sem visualização gráfica).

    Este handler é usado quando:
    - Usuário solicita apenas dados agregados sem gráfico
    - Query é puramente analítica/informacional
    - Resposta é tabular ao invés de visual

    Requirements:
    - 0 ou mais dimensions
    - 0 ou mais metrics
    - Validação mínima (aceita qualquer combinação válida)

    SQL Pattern:
        SELECT dimensions..., AGG(metrics)...
        FROM dataset
        WHERE filters
        GROUP BY dimensions
        ORDER BY sort
        LIMIT top_n

    Output:
        - DataFrame com dados agregados
        - Sem configuração Plotly (ou config minimalista)
    """

    def validate_chart_spec(self, chart_spec: Dict[str, Any]) -> None:
        """
        Valida requirements mínimos para null chart.

        Null charts são muito permissivos:
        - Aceitam qualquer número de dimensions (0+)
        - Aceitam qualquer número de metrics (0+)
        - Apenas validação base de campos obrigatórios

        Args:
            chart_spec: ChartOutput validado do graphic_classifier

        Raises:
            ValueError: Apenas se campos obrigatórios faltarem
        """
        # Validar campos mínimos obrigatórios
        # Nota: dimensions e metrics são opcionais para null charts
        required_fields = ["chart_type", "data_source"]
        for field in required_fields:
            if field not in chart_spec:
                raise ValueError(
                    f"Missing required field in chart_spec: '{field}'. "
                    f"Null chart requires at minimum: {required_fields}"
                )

        # Garantir que dimensions e metrics existem (mesmo que vazios)
        if "dimensions" not in chart_spec:
            chart_spec["dimensions"] = []
        if "metrics" not in chart_spec:
            chart_spec["metrics"] = []

        # Validar que colunas existem no schema (se houver)
        dimensions = chart_spec.get("dimensions", [])
        metrics = chart_spec.get("metrics", [])

        for dim in dimensions:
            col_name = dim.get("name")
            if col_name and col_name not in self.schema:
                raise ValueError(
                    f"Dimension column '{col_name}' not found in schema. "
                    f"Available columns: {list(self.schema.keys())}"
                )

        for metric in metrics:
            col_name = metric.get("name")
            if col_name and col_name not in self.schema:
                raise ValueError(
                    f"Metric column '{col_name}' not found in schema. "
                    f"Available columns: {list(self.schema.keys())}"
                )

        logger.debug(
            f"Null chart validation passed: "
            f"dimensions={len(dimensions)}, "
            f"metrics={len(metrics)}"
        )

        # Aviso se não há metrics (apenas dimensions)
        if len(metrics) == 0 and len(dimensions) > 0:
            logger.info(
                "Null chart with 0 metrics detected. Query will return distinct dimension values."
            )

        # Aviso se não há dimensions (apenas metrics)
        if len(dimensions) == 0 and len(metrics) > 0:
            logger.info(
                "Null chart with 0 dimensions detected. Query will return aggregated metrics across entire dataset."
            )

    def build_sql(self, chart_spec: Dict[str, Any]) -> str:
        """
        Constrói SQL para null chart.

        SQL Pattern (geral):
            SELECT dimensions..., AGG(metrics)...
            FROM dataset
            WHERE filters
            GROUP BY dimensions
            ORDER BY sort
            LIMIT top_n

        Casos especiais:
        - 0 dimensions + metrics: Agregação total (sem GROUP BY)
        - dimensions + 0 metrics: SELECT DISTINCT dimensions
        - 0 dimensions + 0 metrics: SELECT COUNT(*) (contagem total)

        Args:
            chart_spec: ChartOutput validado

        Returns:
            str: Query SQL completa e executável

        Example:
            >>> # Caso 1: Agregação total
            >>> chart_spec = {
            ...     "dimensions": [],
            ...     "metrics": [{"name": "sales", "aggregation": "sum", "alias": "Total"}]
            ... }
            >>> handler.build_sql(chart_spec)
            'SELECT SUM(sales) AS "Total"\\nFROM dataset'

            >>> # Caso 2: Valores distintos
            >>> chart_spec = {
            ...     "dimensions": [{"name": "region", "alias": "Região"}],
            ...     "metrics": []
            ... }
            >>> handler.build_sql(chart_spec)
            'SELECT DISTINCT region AS "Região"\\nFROM dataset\\nORDER BY "Região" ASC'
        """
        dimensions = chart_spec.get("dimensions", [])
        metrics = chart_spec.get("metrics", [])

        # Caso especial: 0 dimensions + 0 metrics → COUNT(*)
        if len(dimensions) == 0 and len(metrics) == 0:
            logger.info(
                "Null chart with 0 dimensions and 0 metrics. Returning total row count."
            )
            return "SELECT COUNT(*) AS total_rows\nFROM dataset"

        # Caso especial: dimensions sem metrics → SELECT DISTINCT
        if len(dimensions) > 0 and len(metrics) == 0:
            logger.info("Null chart with only dimensions. Using SELECT DISTINCT.")
            dim_parts = []
            for dim in dimensions:
                col = self.sql_builder.escape_identifier(dim["name"])
                alias = self.sql_builder.escape_identifier(
                    dim.get("alias", dim["name"])
                )
                dim_parts.append(f"{col} AS {alias}")

            select_clause = "SELECT DISTINCT " + ", ".join(dim_parts)
            from_clause = self.build_from_clause()
            where_clause = self.build_where_clause(chart_spec.get("filters", {}))
            order_by_clause = self.build_order_by_clause(chart_spec)
            limit_clause = self.build_limit_clause(chart_spec)

            sql_parts = [select_clause, from_clause]
            if where_clause:
                sql_parts.append(where_clause)
            if order_by_clause:
                sql_parts.append(order_by_clause)
            if limit_clause:
                sql_parts.append(limit_clause)

            sql = "\n".join(sql_parts)
            logger.debug(f"Generated SQL for null chart (DISTINCT):\n{sql}")
            return sql

        # Caso geral: usar métodos padrão da base
        select_clause = self.build_select_clause(chart_spec)
        from_clause = self.build_from_clause()
        where_clause = self.build_where_clause(chart_spec.get("filters", {}))
        group_by_clause = self.build_group_by_clause(chart_spec)
        order_by_clause = self.build_order_by_clause(chart_spec)
        limit_clause = self.build_limit_clause(chart_spec)

        # Montar SQL final
        sql_parts = [select_clause, from_clause]

        if where_clause:
            sql_parts.append(where_clause)
        if group_by_clause:
            sql_parts.append(group_by_clause)
        if order_by_clause:
            sql_parts.append(order_by_clause)
        if limit_clause:
            sql_parts.append(limit_clause)

        sql = "\n".join(sql_parts)
        logger.debug(f"Generated SQL for null chart:\n{sql}")

        return sql

    def build_plotly_config(
        self, chart_spec: Dict[str, Any], data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Constrói configuração minimalista para null chart.

        Null charts não têm visualização, então retornamos:
        - Estrutura mínima para compatibilidade
        - Dados em formato tabular
        - Tipo "table" para indicar visualização tabular

        Args:
            chart_spec: ChartOutput original
            data: DataFrame com resultado da query

        Returns:
            dict: Configuração minimalista indicando dados tabulares

        Example:
            >>> data = pd.DataFrame({
            ...     "Região": ["Sul", "Norte"],
            ...     "Total": [1000, 800]
            ... })
            >>> config = handler.build_plotly_config(chart_spec, data)
            >>> config["type"]
            'table'
        """
        # Configuração minimalista para null chart
        config = {
            "type": "table",
            "data": data.to_dict(orient="records"),
            "columns": list(data.columns),
            "row_count": len(data),
            "title": chart_spec.get("title", "Query Result"),
            "description": "Tabular data without graphical visualization",
        }

        logger.debug(
            f"Generated minimal config for null chart: "
            f"rows={len(data)}, "
            f"columns={len(data.columns)}"
        )

        return config


# ============================================================================
# LANGGRAPH NODE FUNCTION
# ============================================================================


def tool_handle_null(state: dict) -> dict:
    """
    Nó do LangGraph para processar null charts.

    Este nó é invocado pelo router quando:
    - chart_type == "null"
    - Query não requer visualização gráfica
    - Usuário solicita apenas dados agregados

    Executa o pipeline completo:
    1. Instancia ToolHandlerNull
    2. Executa query (validate → build SQL → execute DuckDB)
    3. Gera config minimalista (tabular)
    4. Atualiza state com resultados

    Args:
        state: AnalyticsState contendo:
            - chart_spec: ChartOutput do graphic_classifier
            - schema: {column_name: data_type}

    Returns:
        dict: State atualizado com:
            - result_dataframe: DataFrame com resultado da query
            - plotly_config: Config minimalista (type="table")
            - execution_success: True
            - sql_query: SQL executado
            - engine_used: "DuckDB"

    Raises:
        ValueError: Se validação do chart_spec falhar
        AnalyticsExecutionError: Se execução da query falhar

    Example:
        >>> state = {
        ...     "chart_spec": {
        ...         "chart_type": "null",
        ...         "dimensions": [{"name": "region"}],
        ...         "metrics": [{"name": "sales", "aggregation": "sum"}],
        ...         "data_source": "data.parquet"
        ...     },
        ...     "schema": {"sales": "DOUBLE", "region": "VARCHAR"}
        ... }
        >>> updated_state = tool_handle_null(state)
        >>> updated_state["execution_success"]
        True
        >>> updated_state["plotly_config"]["type"]
        'table'
    """
    chart_spec = state["chart_spec"]
    schema = state.get("schema", {})
    data_source_path = chart_spec["data_source"]

    logger.info(f"Processing null chart: {chart_spec.get('title', 'untitled')}")

    # Instanciar handler
    handler = ToolHandlerNull(data_source_path=data_source_path, schema=schema)

    # Executar pipeline completo
    result_df = handler.execute(chart_spec)
    plotly_config = handler.build_plotly_config(chart_spec, result_df)

    # Atualizar state
    state["result_dataframe"] = result_df
    state["plotly_config"] = plotly_config
    state["execution_success"] = True
    state["sql_query"] = handler.build_sql(chart_spec)
    state["engine_used"] = "DuckDB"

    logger.info(
        f"Null chart processed successfully: "
        f"rows={len(result_df)}, "
        f"columns={list(result_df.columns)}"
    )

    return state
