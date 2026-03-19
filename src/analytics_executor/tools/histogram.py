"""
Tool Handler for Histogram Charts.

This module implements the specific logic for histogram charts, including:
- Validation for histogram requirements (0 dimensions, raw data)
- SQL generation WITHOUT GROUP BY (returns raw metric values)
- Plotly configuration for histogram with bins
"""

from typing import Dict, Any
import pandas as pd
import logging

from .base import BaseToolHandler

logger = logging.getLogger(__name__)


class ToolHandlerHistogram(BaseToolHandler):
    """
    Tool handler para histogram charts.

    Requirements:
    - ZERO dimensions (histograms mostram distribuição de valores)
    - Exatamente 1 metric (valores para criar bins/distribuição)
    - SQL SEM GROUP BY (retorna valores brutos da métrica)
    - Plotly cria bins automaticamente ou usa nbins especificado

    SQL Pattern (DIFERENTE dos outros - SEM GROUP BY):
        SELECT metric as metric_alias
        FROM dataset
        WHERE filters
        ORDER BY metric [ASC|DESC]  -- opcional
        LIMIT top_n  -- opcional

    Plotly Config:
        - type: "histogram"
        - x: metric values (Plotly cria bins automaticamente)
        - nbinsx: número de bins (opcional)
        - histnorm: normalização ("", "percent", "probability", "density")
    """

    def validate_chart_spec(self, chart_spec: Dict[str, Any]) -> None:
        """
        Valida requirements específicos de histogram.

        Validações:
        - ZERO dimensions (histogram não agrupa por categoria)
        - Exatamente 1 metric

        Args:
            chart_spec: ChartOutput validado do graphic_classifier

        Raises:
            ValueError: Se não atende requirements de histogram
        """
        # Validação base (campos obrigatórios e colunas existem)
        # Note: Modificamos para permitir dimensions vazio
        required_fields = ["chart_type", "metrics", "data_source"]
        for field in required_fields:
            if field not in chart_spec:
                raise ValueError(
                    f"Missing required field in chart_spec: '{field}'. "
                    f"ChartSpec must contain: {required_fields}"
                )

        # Validar dimensions
        dimensions = chart_spec.get("dimensions", [])
        if len(dimensions) != 0:
            raise ValueError(
                f"Histogram requires ZERO dimensions (shows distribution of raw values), "
                f"got {len(dimensions)}. Histograms automatically create bins from metric values."
            )

        # Validar metrics
        metrics = chart_spec.get("metrics", [])
        if len(metrics) != 1:
            raise ValueError(
                f"Histogram requires EXACTLY 1 metric, got {len(metrics)}. "
                f"The metric values are used to create the histogram distribution."
            )

        # Validar que metric existe no schema
        metric = metrics[0]
        col_name = metric.get("name")
        if not col_name:
            raise ValueError(f"Metric missing 'name' field: {metric}")
        if col_name not in self.schema:
            raise ValueError(
                f"Column '{col_name}' not found in schema. "
                f"Available columns: {sorted(self.schema.keys())}"
            )

        logger.debug(
            f"Histogram validation passed: metric={metrics[0]['name']}, dimensions=0"
        )

    def build_sql(self, chart_spec: Dict[str, Any]) -> str:
        """
        Constrói SQL para histogram chart.

        SQL Pattern (SEM GROUP BY - valores brutos):
            SELECT metric as metric_alias
            FROM dataset
            WHERE filters
            ORDER BY metric [ASC|DESC]  -- opcional
            LIMIT top_n  -- opcional

        IMPORTANTE: Sem GROUP BY! Histogram precisa de valores brutos
        para que Plotly possa criar bins.

        Args:
            chart_spec: ChartOutput validado

        Returns:
            str: Query SQL completa e executável

        Example:
            >>> chart_spec = {
            ...     "dimensions": [],
            ...     "metrics": [{"name": "price", "alias": "Preço"}],
            ...     "top_n": 10000
            ... }
            >>> handler.build_sql(chart_spec)
            'SELECT price AS "Preço"\\nFROM dataset\\nLIMIT 10000'
        """
        # SELECT apenas a metric (SEM agregação)
        metric = chart_spec["metrics"][0]
        col = self.sql_builder.escape_identifier(metric["name"])
        alias = self.sql_builder.escape_identifier(metric.get("alias", metric["name"]))

        select_clause = f"SELECT {col} AS {alias}"
        from_clause = self.build_from_clause()

        # WHERE (filtros podem ser aplicados)
        where_clause = self.build_where_clause(chart_spec.get("filters", {}))

        # ORDER BY (opcional - útil para amostragem)
        order_by_clause = ""
        sort = chart_spec.get("sort")
        if sort and sort.get("by"):
            order_by_clause = self.build_order_by_clause(chart_spec)

        # LIMIT (importante para datasets grandes!)
        limit_clause = self.build_limit_clause(chart_spec)
        if not limit_clause:
            # Sugerir um limite padrão se não especificado
            logger.warning(
                "No LIMIT specified for histogram query. "
                "Consider adding top_n to avoid loading too much data."
            )

        # Montar SQL final (SEM GROUP BY)
        sql_parts = [select_clause, from_clause]

        if where_clause:
            sql_parts.append(where_clause)
        if order_by_clause:
            sql_parts.append(order_by_clause)
        if limit_clause:
            sql_parts.append(limit_clause)

        sql = "\n".join(sql_parts)
        logger.debug(f"Generated SQL for histogram (NO GROUP BY):\n{sql}")

        return sql

    def build_plotly_config(
        self, chart_spec: Dict[str, Any], data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Constrói configuração Plotly específica para histogram.

        Config Pattern:
            - type: "histogram"
            - x: metric values (Plotly cria bins automaticamente)
            - nbinsx: número de bins
            - histnorm: tipo de normalização
            - marker: cores

        Args:
            chart_spec: ChartOutput original
            data: DataFrame com valores brutos da metric

        Returns:
            dict: Configuração Plotly pronta para renderização

        Example:
            >>> data = pd.DataFrame({
            ...     "Preço": [10, 15, 12, 18, 14, 11, 20, 25, 22, 19]
            ... })
            >>> config = handler.build_plotly_config(chart_spec, data)
            >>> config["data"][0]["type"]
            'histogram'
        """
        metric_col = chart_spec["metrics"][0].get(
            "alias", chart_spec["metrics"][0]["name"]
        )

        # Configuração visual
        visual = chart_spec.get("visual", {})
        nbins = visual.get("nbins", 30)  # Número de bins (default 30)
        histnorm = visual.get("histnorm", "")  # "", "percent", "probability", "density"
        palette_name = visual.get("palette", "default")

        # Obter cor principal
        colors = self._get_color_palette(palette_name, 1)

        # Construir trace
        trace = {
            "type": "histogram",
            "x": data[metric_col].tolist(),
            "nbinsx": nbins,
            "marker": {"color": colors[0], "line": {"color": "white", "width": 1}},
            "hovertemplate": (
                f"{metric_col}: %{{x}}<br>Frequência: %{{y}}<br><extra></extra>"
            ),
        }

        # Adicionar normalização se especificado
        if histnorm:
            trace["histnorm"] = histnorm

        config = {
            "data": [trace],
            "layout": {
                "title": chart_spec.get("title", ""),
                "xaxis": {"title": metric_col},
                "yaxis": {"title": self._get_yaxis_title(histnorm)},
                "showlegend": False,
                "bargap": 0.05,  # Small gap between bins
            },
        }

        logger.debug(
            f"Built Plotly config for histogram with {nbins} bins, "
            f"{len(data)} data points"
        )
        return config

    def _get_yaxis_title(self, histnorm: str) -> str:
        """
        Retorna título apropriado para eixo Y baseado em normalização.

        Args:
            histnorm: Tipo de normalização ("", "percent", "probability", "density")

        Returns:
            str: Título do eixo Y
        """
        titles = {
            "": "Frequência",
            "percent": "Percentual (%)",
            "probability": "Probabilidade",
            "density": "Densidade",
        }
        return titles.get(histnorm, "Frequência")

    def _get_color_palette(self, palette_name: str, n_colors: int) -> list:
        """
        Retorna paleta de cores baseada no nome.

        Args:
            palette_name: Nome da paleta ("default", "blues", "greens", etc.)
            n_colors: Número de cores necessárias

        Returns:
            list: Lista de cores em formato hex
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


def tool_handle_histogram(state: dict) -> dict:
    """
    Nó do LangGraph para processar histogram charts.

    Args:
        state: AnalyticsState com chart_spec

    Returns:
        dict: State atualizado com result_dataframe e plotly_config
    """
    chart_spec = state["chart_spec"]
    schema = state.get("schema", {})
    data_source_path = chart_spec["data_source"]

    # Instanciar handler
    handler = ToolHandlerHistogram(data_source_path=data_source_path, schema=schema)

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
