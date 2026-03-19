"""
Tool Handler for Bar Horizontal Charts.

This module implements the specific logic for horizontal bar charts, including:
- Validation for bar horizontal requirements
- SQL generation with top_n support
- Plotly configuration for horizontal orientation
"""

from typing import Dict, Any
import pandas as pd
import logging

from .base import BaseToolHandler

logger = logging.getLogger(__name__)


class ToolHandlerBarHorizontal(BaseToolHandler):
    """
    Tool handler para bar horizontal charts.

    Requirements:
    - Exatamente 1 dimension (categorias no eixo Y)
    - Mínimo 1 metric (valores no eixo X)
    - Sort geralmente DESC (maiores valores no topo)
    - top_n comum (exibir top N categorias)

    SQL Pattern:
        SELECT dimension, AGG(metric) as metric_alias
        FROM dataset
        WHERE filters
        GROUP BY dimension
        ORDER BY metric_alias DESC
        LIMIT top_n

    Plotly Config:
        - type: "bar"
        - orientation: "h" (horizontal)
        - x: metric values
        - y: dimension categories
    """

    def validate_chart_spec(self, chart_spec: Dict[str, Any]) -> None:
        """
        Valida requirements específicos de bar horizontal.

        Validações:
        - Exatamente 1 dimension
        - Mínimo 1 metric

        Args:
            chart_spec: ChartOutput validado do graphic_classifier

        Raises:
            ValueError: Se não atende requirements de bar horizontal
        """
        # Validação base (campos obrigatórios e colunas existem)
        super().validate_chart_spec(chart_spec)

        # Validações específicas
        dimensions = chart_spec.get("dimensions", [])
        if len(dimensions) != 1:
            raise ValueError(
                f"Bar horizontal requires EXACTLY 1 dimension, got {len(dimensions)}"
            )

        metrics = chart_spec.get("metrics", [])
        if len(metrics) < 1:
            raise ValueError(
                f"Bar horizontal requires at least 1 metric, got {len(metrics)}"
            )

        logger.debug(
            f"Bar horizontal validation passed: "
            f"dimension={dimensions[0]['name']}, "
            f"metrics={[m['name'] for m in metrics]}"
        )

    def build_sql(self, chart_spec: Dict[str, Any]) -> str:
        """
        Constrói SQL para bar horizontal chart.

        SQL Pattern:
            SELECT dimension, AGG(metric) as metric_alias
            FROM dataset
            WHERE filters
            GROUP BY dimension
            ORDER BY metric_alias DESC
            LIMIT top_n  -- se presente

        Note: Quando top_n está presente, o método execute() da base class
        executará automaticamente uma query separada para calcular totais
        globais do dataset filtrado (sem LIMIT). Isso garante que percentuais
        sejam calculados corretamente no insight_generator.

        Args:
            chart_spec: ChartOutput validado

        Returns:
            str: Query SQL completa e executável

        Example:
            >>> chart_spec = {
            ...     "dimensions": [{"name": "region", "alias": "Região"}],
            ...     "metrics": [{"name": "sales", "aggregation": "sum", "alias": "Vendas"}],
            ...     "top_n": 10,
            ...     "sort": {"by": "Vendas", "order": "desc"}
            ... }
            >>> handler.build_sql(chart_spec)
            'SELECT region AS "Região", SUM(sales) AS "Vendas"\\nFROM dataset\\nGROUP BY region\\nORDER BY "Vendas" DESC\\nLIMIT 10'
        """
        # Construir cláusulas usando métodos da base
        select_clause = self.build_select_clause(chart_spec)
        from_clause = self.build_from_clause()
        where_clause = self.build_where_clause(chart_spec.get("filters", {}))
        group_by_clause = self.build_group_by_clause(chart_spec)
        order_by_clause = self.build_order_by_clause(chart_spec)
        limit_clause = self.build_limit_clause(chart_spec)

        # Construir query padrão (totais globais agora são calculados via query separada)
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
        logger.debug(f"Generated SQL for bar_horizontal:\n{sql}")

        return sql

    def build_plotly_config(
        self, chart_spec: Dict[str, Any], data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Constrói configuração Plotly específica para bar horizontal.

        Config Pattern:
            - type: "bar"
            - orientation: "h"
            - x: metric values (horizontal axis)
            - y: dimension categories (vertical axis)

        Args:
            chart_spec: ChartOutput original
            data: DataFrame com resultado da query

        Returns:
            dict: Configuração Plotly pronta para renderização

        Example:
            >>> data = pd.DataFrame({
            ...     "Região": ["Sul", "Norte", "Centro"],
            ...     "Vendas": [1000, 800, 600]
            ... })
            >>> config = handler.build_plotly_config(chart_spec, data)
            >>> config["data"][0]["type"]
            'bar'
            >>> config["data"][0]["orientation"]
            'h'
        """
        dimension_col = chart_spec["dimensions"][0].get(
            "alias", chart_spec["dimensions"][0]["name"]
        )
        metric_col = chart_spec["metrics"][0].get(
            "alias", chart_spec["metrics"][0]["name"]
        )

        # Configuração visual do spec
        visual = chart_spec.get("visual", {})
        show_values = visual.get("show_values", True)
        color = visual.get("color", "#1f77b4")

        config = {
            "data": [
                {
                    "type": "bar",
                    "orientation": "h",
                    "x": data[metric_col].tolist(),
                    "y": data[dimension_col].tolist(),
                    "marker": {"color": color},
                    "text": data[metric_col].tolist() if show_values else None,
                    "textposition": "outside" if show_values else None,
                }
            ],
            "layout": {
                "title": chart_spec.get("title", ""),
                "xaxis": {"title": metric_col},
                "yaxis": {"title": dimension_col},
                "showlegend": False,
                "height": max(400, len(data) * 30),  # Altura dinâmica
            },
        }

        logger.debug(
            f"Generated Plotly config for bar_horizontal: "
            f"data_points={len(data)}, "
            f"dimension={dimension_col}, "
            f"metric={metric_col}"
        )

        return config


# ============================================================================
# LANGGRAPH NODE FUNCTION
# ============================================================================


def tool_handle_bar_horizontal(state: dict) -> dict:
    """
    Nó do LangGraph para processar bar horizontal charts.

    Este nó é invocado pelo router quando chart_type == "bar_horizontal".
    Executa o pipeline completo:
    1. Instancia ToolHandlerBarHorizontal
    2. Executa query (validate → build SQL → execute DuckDB)
    3. Gera config Plotly
    4. Atualiza state com resultados

    Args:
        state: AnalyticsState contendo:
            - chart_spec: ChartOutput do graphic_classifier
            - schema: {column_name: data_type}

    Returns:
        dict: State atualizado com:
            - result_dataframe: DataFrame com resultado da query
            - plotly_config: Configuração Plotly
            - execution_success: True
            - sql_query: SQL executado
            - engine_used: "DuckDB"

    Raises:
        ValueError: Se validação do chart_spec falhar
        AnalyticsExecutionError: Se execução da query falhar

    Example:
        >>> state = {
        ...     "chart_spec": {...},
        ...     "schema": {"sales": "DOUBLE", "region": "VARCHAR"}
        ... }
        >>> updated_state = tool_handle_bar_horizontal(state)
        >>> updated_state["execution_success"]
        True
    """
    chart_spec = state["chart_spec"]
    schema = state.get("schema", {})
    data_source_path = chart_spec["data_source"]

    logger.info(
        f"Processing bar_horizontal chart: {chart_spec.get('title', 'untitled')}"
    )

    # Instanciar handler
    handler = ToolHandlerBarHorizontal(data_source_path=data_source_path, schema=schema)

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
        f"Bar horizontal processed successfully: "
        f"rows={len(result_df)}, "
        f"columns={list(result_df.columns)}"
    )

    return state
