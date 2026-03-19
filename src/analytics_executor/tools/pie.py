"""
Tool Handler for Pie Charts.

This module implements the specific logic for pie charts, including:
- Validation for pie chart requirements (exactly 1 dimension)
- SQL generation with aggregation
- Plotly configuration for pie visualization with colors and labels
"""

from typing import Dict, Any, List
import pandas as pd
import logging

from .base import BaseToolHandler

logger = logging.getLogger(__name__)


class ToolHandlerPie(BaseToolHandler):
    """
    Tool handler para pie charts.

    Requirements:
    - Exatamente 1 dimension (categorias para fatias)
    - Mínimo 1 metric (valores para percentuais)
    - Sort geralmente DESC (fatias maiores primeiro)
    - Suporte a paletas de cores

    SQL Pattern:
        SELECT dimension, AGG(metric) as metric_alias
        FROM dataset
        WHERE filters
        GROUP BY dimension
        ORDER BY metric_alias DESC
        LIMIT top_n  -- opcional

    Plotly Config:
        - type: "pie"
        - labels: dimension values
        - values: metric values
        - textinfo: "label+percent" ou "percent"
        - colors: paleta configurável
    """

    def validate_chart_spec(self, chart_spec: Dict[str, Any]) -> None:
        """
        Valida requirements específicos de pie chart.

        Validações:
        - Exatamente 1 dimension (fatias do gráfico)
        - Mínimo 1 metric (valores para percentuais)

        Args:
            chart_spec: ChartOutput validado do graphic_classifier

        Raises:
            ValueError: Se não atende requirements de pie chart
        """
        # Validação base (campos obrigatórios e colunas existem)
        super().validate_chart_spec(chart_spec)

        # Validações específicas
        dimensions = chart_spec.get("dimensions", [])
        if len(dimensions) != 1:
            raise ValueError(
                f"Pie chart requires EXACTLY 1 dimension, got {len(dimensions)}. "
                f"Pie charts represent parts of a whole with a single categorical dimension."
            )

        metrics = chart_spec.get("metrics", [])
        if len(metrics) < 1:
            raise ValueError(
                f"Pie chart requires at least 1 metric, got {len(metrics)}. "
                f"The metric represents the value/size of each slice."
            )

        logger.debug(
            f"Pie chart validation passed: "
            f"dimension={dimensions[0]['name']}, "
            f"metric={metrics[0]['name']}"
        )

    def build_sql(self, chart_spec: Dict[str, Any]) -> str:
        """
        Constrói SQL para pie chart.

        IMPORTANTE: Para pie charts, NAO aplicamos LIMIT no SQL porque a limitacao
        de categorias com agregacao em "OUTROS" e feita posteriormente no
        plotly_generator. Isso garante que:
        1. Todos os dados sejam retornados do banco
        2. O plotly_generator possa calcular corretamente o total de "OUTROS"
        3. O grafico reflita Top N + OUTROS com valores precisos

        SQL Pattern:
            SELECT dimension, AGG(metric) as metric_alias
            FROM dataset
            WHERE filters
            GROUP BY dimension
            ORDER BY metric_alias DESC
            -- SEM LIMIT: limitacao feita no plotly_generator com "OUTROS"

        Args:
            chart_spec: ChartOutput validado

        Returns:
            str: Query SQL completa e executável

        Example:
            >>> chart_spec = {
            ...     "dimensions": [{"name": "category", "alias": "Categoria"}],
            ...     "metrics": [{"name": "revenue", "aggregation": "sum", "alias": "Receita"}],
            ...     "top_n": 10,
            ...     "sort": {"by": "Receita", "order": "desc"}
            ... }
            >>> handler.build_sql(chart_spec)
            'SELECT category AS "Categoria", SUM(revenue) AS "Receita"\\nFROM dataset\\nGROUP BY category\\nORDER BY "Receita" DESC'
        """
        # Construir cláusulas usando métodos da base
        select_clause = self.build_select_clause(chart_spec)
        from_clause = self.build_from_clause()
        where_clause = self.build_where_clause(chart_spec.get("filters", {}))
        group_by_clause = self.build_group_by_clause(chart_spec)
        order_by_clause = self.build_order_by_clause(chart_spec)
        # NAO aplicar LIMIT no SQL para pie charts - limitacao sera feita no plotly_generator
        # com agregacao correta em "OUTROS"

        # Montar SQL final
        sql_parts = [select_clause, from_clause]

        if where_clause:
            sql_parts.append(where_clause)
        if group_by_clause:
            sql_parts.append(group_by_clause)
        if order_by_clause:
            sql_parts.append(order_by_clause)
        # Removido: limit_clause - pie charts nao devem ter LIMIT no SQL

        sql = "\n".join(sql_parts)
        logger.debug(f"Generated SQL for pie chart (no LIMIT - handled by plotly_generator):\n{sql}")

        return sql

    def build_plotly_config(
        self, chart_spec: Dict[str, Any], data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Constrói configuração Plotly específica para pie chart.

        Config Pattern:
            - type: "pie"
            - labels: categorias (dimension)
            - values: valores (metric)
            - textinfo: mostrar label + percentual
            - colors: paleta de cores configurável

        Args:
            chart_spec: ChartOutput original
            data: DataFrame com resultado da query

        Returns:
            dict: Configuração Plotly pronta para renderização

        Example:
            >>> data = pd.DataFrame({
            ...     "Categoria": ["A", "B", "C"],
            ...     "Receita": [1000, 500, 300]
            ... })
            >>> config = handler.build_plotly_config(chart_spec, data)
            >>> config["data"][0]["type"]
            'pie'
            >>> len(config["data"][0]["labels"])
            3
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
        palette_name = visual.get("palette", "default")

        # Determinar textinfo
        if show_values:
            textinfo = "label+percent+value"
        else:
            textinfo = "percent"

        # Obter paleta de cores
        colors = self._get_color_palette(palette_name, len(data))

        config = {
            "data": [
                {
                    "type": "pie",
                    "labels": data[dimension_col].tolist(),
                    "values": data[metric_col].tolist(),
                    "textinfo": textinfo,
                    "textposition": "auto",
                    "marker": {"colors": colors},
                    "hovertemplate": "<b>%{label}</b><br>"
                    + f"{metric_col}: %{{value}}<br>"
                    + "Percentual: %{percent}<br>"
                    + "<extra></extra>",
                }
            ],
            "layout": {
                "title": chart_spec.get("title", ""),
                "showlegend": True,
                "legend": {"orientation": "v", "yanchor": "middle", "y": 0.5},
            },
        }

        logger.debug(
            f"Generated Plotly config for pie chart: "
            f"slices={len(data)}, "
            f"dimension={dimension_col}, "
            f"metric={metric_col}, "
            f"palette={palette_name}"
        )

        return config

    def _get_color_palette(self, palette_name: str, num_colors: int) -> List[str]:
        """
        Retorna paleta de cores com número suficiente de cores.

        Paletas disponíveis:
        - default: Cores padrão do Plotly
        - pastel: Cores suaves e pastéis
        - vibrant: Cores vibrantes e saturadas
        - professional: Cores corporativas
        - warm: Tons quentes
        - cool: Tons frios

        Args:
            palette_name: Nome da paleta
            num_colors: Número de cores necessárias

        Returns:
            list: Lista de cores em formato hex
        """
        palettes = {
            "default": [
                "#1f77b4",
                "#ff7f0e",
                "#2ca02c",
                "#d62728",
                "#9467bd",
                "#8c564b",
                "#e377c2",
                "#7f7f7f",
                "#bcbd22",
                "#17becf",
            ],
            "pastel": [
                "#AEC6CF",
                "#FFB347",
                "#B39EB5",
                "#FF6961",
                "#779ECB",
                "#CFCFC4",
                "#FFD1DC",
                "#C1E1C1",
                "#FAE7B5",
                "#FDFD96",
            ],
            "vibrant": [
                "#FF5733",
                "#33FF57",
                "#3357FF",
                "#F333FF",
                "#FF33A1",
                "#FFD700",
                "#00CED1",
                "#FF1493",
                "#00FF00",
                "#FF4500",
            ],
            "professional": [
                "#003f5c",
                "#2f4b7c",
                "#665191",
                "#a05195",
                "#d45087",
                "#f95d6a",
                "#ff7c43",
                "#ffa600",
            ],
            "warm": [
                "#8B0000",
                "#FF4500",
                "#FF8C00",
                "#FFD700",
                "#FFFF00",
                "#ADFF2F",
            ],
            "cool": [
                "#00008B",
                "#0000CD",
                "#0000FF",
                "#1E90FF",
                "#00BFFF",
                "#87CEEB",
            ],
        }

        # Obter paleta (ou default se não existir)
        palette = palettes.get(palette_name, palettes["default"])

        # Se precisar de mais cores, repetir paleta
        if num_colors > len(palette):
            repeats = (num_colors // len(palette)) + 1
            palette = palette * repeats

        return palette[:num_colors]


# ============================================================================
# LANGGRAPH NODE FUNCTION
# ============================================================================


def tool_handle_pie(state: dict) -> dict:
    """
    Nó do LangGraph para processar pie charts.

    Este nó é invocado pelo router quando chart_type == "pie".
    Executa o pipeline completo:
    1. Instancia ToolHandlerPie
    2. Executa query (validate → build SQL → execute DuckDB)
    3. Gera config Plotly com paleta de cores
    4. Atualiza state com resultados

    Args:
        state: AnalyticsState contendo:
            - chart_spec: ChartOutput do graphic_classifier
            - schema: {column_name: data_type}

    Returns:
        dict: State atualizado com:
            - result_dataframe: DataFrame com resultado da query
            - plotly_config: Configuração Plotly com pie chart
            - execution_success: True
            - sql_query: SQL executado
            - engine_used: "DuckDB"

    Raises:
        ValueError: Se validação do chart_spec falhar
        AnalyticsExecutionError: Se execução da query falhar

    Example:
        >>> state = {
        ...     "chart_spec": {
        ...         "chart_type": "pie",
        ...         "dimensions": [{"name": "category"}],
        ...         "metrics": [{"name": "sales", "aggregation": "sum"}],
        ...         "data_source": "data.parquet"
        ...     },
        ...     "schema": {"sales": "DOUBLE", "category": "VARCHAR"}
        ... }
        >>> updated_state = tool_handle_pie(state)
        >>> updated_state["execution_success"]
        True
    """
    chart_spec = state["chart_spec"]
    schema = state.get("schema", {})
    data_source_path = chart_spec["data_source"]

    logger.info(f"Processing pie chart: {chart_spec.get('title', 'untitled')}")

    # Instanciar handler
    handler = ToolHandlerPie(data_source_path=data_source_path, schema=schema)

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
        f"Pie chart processed successfully: "
        f"slices={len(result_df)}, "
        f"total_value={result_df.iloc[:, 1].sum()}"
    )

    return state
