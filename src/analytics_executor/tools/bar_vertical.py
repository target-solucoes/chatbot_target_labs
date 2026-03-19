"""
Tool Handler for Bar Vertical Charts.

This module implements the specific logic for vertical bar charts, including:
- Validation for bar vertical requirements
- SQL generation (similar to bar_horizontal)
- Plotly configuration for vertical orientation
"""

from typing import Dict, Any, List
import pandas as pd
import logging

from .base import BaseToolHandler

logger = logging.getLogger(__name__)


class ToolHandlerBarVertical(BaseToolHandler):
    """
    Tool handler para bar vertical charts.

    Requirements:
    - Exatamente 1 dimension (categorias no eixo X)
    - Mínimo 1 metric (valores no eixo Y)
    - Sort pode ser ASC ou DESC
    - top_n opcional (não obrigatório como em bar_horizontal)

    SQL Pattern:
        SELECT dimension, AGG(metric) as metric_alias
        FROM dataset
        WHERE filters
        GROUP BY dimension
        ORDER BY metric_alias [ASC|DESC]
        LIMIT top_n  -- opcional

    Plotly Config:
        - type: "bar"
        - orientation: "v" (vertical - padrão)
        - x: dimension categories
        - y: metric values
    """

    def validate_chart_spec(self, chart_spec: Dict[str, Any]) -> None:
        """
        Valida requirements específicos de bar vertical.

        Validações:
        - Exatamente 1 dimension
        - Mínimo 1 metric

        Args:
            chart_spec: ChartOutput validado do graphic_classifier

        Raises:
            ValueError: Se não atende requirements de bar vertical
        """
        # Validação base (campos obrigatórios e colunas existem)
        super().validate_chart_spec(chart_spec)

        # Validações específicas
        dimensions = chart_spec.get("dimensions", [])
        if len(dimensions) != 1:
            raise ValueError(
                f"Bar vertical requires EXACTLY 1 dimension, got {len(dimensions)}"
            )

        metrics = chart_spec.get("metrics", [])
        if len(metrics) < 1:
            raise ValueError(
                f"Bar vertical requires at least 1 metric, got {len(metrics)}"
            )

        logger.debug(
            f"Bar vertical validation passed: "
            f"dimension={dimensions[0]['name']}, "
            f"metrics={[m['name'] for m in metrics]}"
        )

    def build_sql(self, chart_spec: Dict[str, Any]) -> str:
        """
        Constrói SQL para bar vertical chart.

        SQL Pattern:
            SELECT dimension, AGG(metric) as metric_alias
            FROM dataset
            WHERE filters
            GROUP BY dimension
            ORDER BY [dimension|metric] [ASC|DESC]
            LIMIT top_n  -- se presente

        Args:
            chart_spec: ChartOutput validado

        Returns:
            str: Query SQL completa e executável

        Example:
            >>> chart_spec = {
            ...     "dimensions": [{"name": "product", "alias": "Produto"}],
            ...     "metrics": [{"name": "quantity", "aggregation": "sum", "alias": "Quantidade"}],
            ...     "sort": {"by": "Produto", "order": "asc"}
            ... }
            >>> handler.build_sql(chart_spec)
            'SELECT product AS "Produto", SUM(quantity) AS "Quantidade"\\nFROM dataset\\nGROUP BY product\\nORDER BY "Produto" ASC'
        """
        # Construir cláusulas usando métodos da base
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
        logger.debug(f"Generated SQL for bar_vertical:\n{sql}")

        return sql

    def build_plotly_config(
        self, chart_spec: Dict[str, Any], data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Constrói configuração Plotly específica para bar vertical.

        Config Pattern:
            - type: "bar"
            - orientation: "v" (vertical - default)
            - x: dimension categories (horizontal axis)
            - y: metric values (vertical axis)

        Args:
            chart_spec: ChartOutput original
            data: DataFrame com resultado da query

        Returns:
            dict: Configuração Plotly pronta para renderização

        Example:
            >>> data = pd.DataFrame({
            ...     "Produto": ["A", "B", "C"],
            ...     "Quantidade": [100, 200, 150]
            ... })
            >>> config = handler.build_plotly_config(chart_spec, data)
            >>> config["data"][0]["type"]
            'bar'
            >>> config["data"][0]["orientation"]
            'v'
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

        # Obter paleta de cores
        colors = self._get_color_palette(palette_name, len(data))

        # Construir trace
        trace = {
            "type": "bar",
            "orientation": "v",
            "x": data[dimension_col].tolist(),
            "y": data[metric_col].tolist(),
            "marker": {"color": colors},
            "text": data[metric_col].tolist() if show_values else None,
            "textposition": "auto" if show_values else None,
            "hovertemplate": f"<b>%{{x}}</b><br>{metric_col}: %{{y}}<extra></extra>",
        }

        config = {
            "data": [trace],
            "layout": {
                "title": chart_spec.get("title", ""),
                "xaxis": {"title": dimension_col},
                "yaxis": {"title": metric_col},
                "showlegend": False,
            },
        }

        logger.debug(f"Built Plotly config for bar_vertical with {len(data)} bars")
        return config

    def _get_color_palette(self, palette_name: str, n_colors: int) -> List[str]:
        """
        Retorna paleta de cores baseada no nome.

        Args:
            palette_name: Nome da paleta ("default", "blues", "greens", etc.)
            n_colors: Número de cores necessárias

        Returns:
            List[str]: Lista de cores em formato hex
        """
        palettes = {
            "default": [
                "#636EFA",
                "#EF553B",
                "#00CC96",
                "#AB63FA",
                "#FFA15A",
                "#19D3F3",
                "#FF6692",
                "#B6E880",
                "#FF97FF",
                "#FECB52",
            ],
            "blues": [
                "#08519c",
                "#3182bd",
                "#6baed6",
                "#9ecae1",
                "#c6dbef",
                "#deebf7",
            ],
            "greens": [
                "#006d2c",
                "#31a354",
                "#74c476",
                "#a1d99b",
                "#c7e9c0",
                "#edf8e9",
            ],
            "reds": [
                "#a50f15",
                "#de2d26",
                "#fb6a4a",
                "#fc9272",
                "#fcbba1",
                "#fee5d9",
            ],
            "purples": [
                "#54278f",
                "#756bb1",
                "#9e9ac8",
                "#bcbddc",
                "#dadaeb",
                "#efedf5",
            ],
        }

        palette = palettes.get(palette_name, palettes["default"])

        # Se precisar mais cores, repetir a paleta
        if n_colors > len(palette):
            palette = palette * ((n_colors // len(palette)) + 1)

        return palette[:n_colors]


# ============================================================================
# LANGGRAPH NODE FUNCTION
# ============================================================================


def tool_handle_bar_vertical(state: dict) -> dict:
    """
    Nó do LangGraph para processar bar vertical charts.

    Args:
        state: AnalyticsState com chart_spec

    Returns:
        dict: State atualizado com result_dataframe e plotly_config
    """
    chart_spec = state["chart_spec"]
    schema = state.get("schema", {})
    data_source_path = chart_spec["data_source"]

    # Instanciar handler
    handler = ToolHandlerBarVertical(data_source_path=data_source_path, schema=schema)

    # Executar pipeline completo
    result_df = handler.execute(chart_spec)
    plotly_config = handler.build_plotly_config(chart_spec, result_df)

    # Atualizar state
    state["result_dataframe"] = result_df
    state["plotly_config"] = plotly_config
    state["execution_success"] = True
    state["sql_query"] = handler.build_sql(chart_spec)
    state["engine_used"] = "DuckDB"

    return state
