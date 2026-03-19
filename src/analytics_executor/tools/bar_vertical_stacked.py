"""
Tool Handler for Bar Vertical Stacked Charts (Stacked Bars).

This module implements the specific logic for bar vertical stacked charts with
stacked bars, including:
- Validation for 2 dimensions (same as composed)
- SQL generation with 2 dimensions GROUP BY (identical to composed)
- Plotly configuration creating stacked bars (barmode='stack')
"""

from typing import Dict, Any, List
import pandas as pd
import logging

from .base import BaseToolHandler

logger = logging.getLogger(__name__)


class ToolHandlerBarVerticalStacked(BaseToolHandler):
    """
    Tool handler para bar vertical stacked charts (barras empilhadas).

    Requirements:
    - Exatamente 2 dimensions (categoria principal + subcategoria)
    - Mínimo 1 metric
    - Sort pode ser ASC ou DESC
    - SQL IDÊNTICO ao bar_vertical_composed (diferença apenas no Plotly)

    SQL Pattern:
        SELECT dimension1, dimension2, AGG(metric) as metric_alias
        FROM dataset
        WHERE filters
        GROUP BY dimension1, dimension2
        ORDER BY dimension1 ASC, dimension2 ASC

    Plotly Config:
        - type: "bar"
        - orientation: "v"
        - barmode: "stack" (barras empilhadas verticalmente)
        - Múltiplas traces: uma por valor da segunda dimension
        - x: primeira dimension
        - y: metric values (empilhados)
    """

    def validate_chart_spec(self, chart_spec: Dict[str, Any]) -> None:
        """
        Valida requirements específicos de bar vertical stacked.

        Validações:
        - Exatamente 2 dimensions
        - Mínimo 1 metric

        Note: Validação idêntica ao bar_vertical_composed.

        Args:
            chart_spec: ChartOutput validado do graphic_classifier

        Raises:
            ValueError: Se não atende requirements de bar vertical stacked
        """
        # Validação base (campos obrigatórios e colunas existem)
        super().validate_chart_spec(chart_spec)

        # Validações específicas
        dimensions = chart_spec.get("dimensions", [])

        # Deve ter exatamente 2 dimensions
        if len(dimensions) != 2:
            raise ValueError(
                f"Bar vertical stacked requires EXACTLY 2 dimensions, got {len(dimensions)}. "
                f"First dimension represents categories on X axis, "
                f"second dimension represents stacked segments within each bar."
            )

        metrics = chart_spec.get("metrics", [])
        if len(metrics) < 1:
            raise ValueError(
                f"Bar vertical stacked requires at least 1 metric, got {len(metrics)}"
            )

        logger.debug(
            f"Bar vertical stacked validation passed: "
            f"dimension1={dimensions[0]['name']}, "
            f"dimension2={dimensions[1]['name']}, "
            f"metrics={[m['name'] for m in metrics]}"
        )

    def build_sql(self, chart_spec: Dict[str, Any]) -> str:
        """
        Constrói SQL para bar vertical stacked chart.

        SQL Pattern (IDÊNTICO ao bar_vertical_composed):
            SELECT dimension1, dimension2, AGG(metric) as metric_alias
            FROM dataset
            WHERE filters
            GROUP BY dimension1, dimension2
            ORDER BY dimension1 ASC, dimension2 ASC

        Note: SQL é idêntico ao composed. A diferença está apenas na
        visualização Plotly (barmode='stack' vs 'group').

        Args:
            chart_spec: ChartOutput validado

        Returns:
            str: Query SQL completa e executável

        Example:
            >>> chart_spec = {
            ...     "dimensions": [
            ...         {"name": "region", "alias": "Região"},
            ...         {"name": "product", "alias": "Produto"}
            ...     ],
            ...     "metrics": [{"name": "sales", "aggregation": "sum", "alias": "Vendas"}]
            ... }
            >>> handler.build_sql(chart_spec)
            'SELECT region AS "Região", product AS "Produto", SUM(sales) AS "Vendas"\\n...'
        """
        # Construir cláusulas básicas usando métodos da base
        select_clause = self.build_select_clause(chart_spec)
        from_clause = self.build_from_clause()
        base_where_clause = self.build_where_clause(chart_spec.get("filters", {}))
        group_by_clause = self.build_group_by_clause(chart_spec)

        # Dimensões originais (nome das colunas no dataset)
        dimension1 = chart_spec["dimensions"][0]["name"]
        dimension2 = chart_spec["dimensions"][1]["name"]

        # Detect optional parameters for nested ranking: group_top_n (M) and top_n (N)
        # Backwards-compatible: if group_top_n/top_m is NOT provided, keep legacy behavior
        top_n = chart_spec.get("top_n")
        group_top_n = chart_spec.get("group_top_n") or chart_spec.get("top_m")

        # Escape identifiers for SQL
        dim1_escaped = self.sql_builder.escape_identifier(dimension1)
        dim2_escaped = self.sql_builder.escape_identifier(dimension2)

        # CRITICAL: For nested ranking queries, we need to determine which dimension
        # represents the MAIN GROUP (for partitioning) vs the SUBGROUP (items within each partition).
        #
        # The classifier may map dimensions in different orders depending on the query structure.
        # For example:
        #   - "top 3 produtos dos 5 maiores estados" → [UF_Cliente, Des_Linha_Produto]
        #   - "top 3 clientes dos 5 maiores estados" → [UF_Cliente, Cod_Cliente]
        #
        # For nested ranking "top N items within top M groups":
        #   - We PARTITION BY the GROUP dimension (to separate into M groups)
        #   - We SELECT top N items within each partition
        #
        # In bar_vertical_stacked for nested ranking:
        #   - The dimension with group_top_n limit is the PARTITION dimension
        #   - The dimension with top_n limit is the ITEM dimension (selected within partition)
        #
        # Strategy: Check which dimension appears to be the "group" based on which one
        # should be limited to group_top_n (M) groups. Since both dimensions will appear
        # in the result, we use dim1 (first dimension) as the partition key when group_top_n
        # is present, because the classifier tends to put the "main group" first when it
        # detects nested ranking patterns.

        if group_top_n and top_n:
            # For nested ranking: dim1 is the MAIN GROUP (partition key)
            # dim2 is the SUBGROUP (items within each partition)
            partition_dim = dim1_escaped
            item_dim = dim2_escaped
            logger.info(
                f"[build_sql] Nested ranking: PARTITION BY {partition_dim} (main group), SELECT top {top_n} of {item_dim} (items) per partition"
            )
        else:
            # For regular stacked bar: use default semantics
            partition_dim = dim2_escaped
            item_dim = dim1_escaped

        # If both group_top_n (M) and top_n (N) are provided we perform the two-step
        # approach: 1) compute TOP M groups by metric total, 2) for those M groups
        # compute TOP N subgroups per group using ROW_NUMBER() OVER (PARTITION BY ...)
        #
        # For bar_vertical_stacked:
        #   - dim1 = X-axis (subgroup, e.g., Cod_Cliente)
        #   - dim2 = Stack/Layer (main group, e.g., UF_Cliente)
        #
        # For nested ranking "top 3 clients from top 5 states":
        #   - group_top_n=5 → Top 5 **dim2** (states) by total metric
        #   - top_n=3 → Top 3 **dim1** (clients) within each dim2 (state)
        #
        # This returns M×N rows (e.g., 5 states × 3 clients = 15 rows)
        if group_top_n and top_n:
            # Metric original column (non-aliased) used in aggregation inside window
            metric = chart_spec["metrics"][0]["name"]
            metric_col = self.sql_builder.escape_identifier(metric)

            # Build subquery that selects top M groups (partition dimension) by total metric
            top_groups_subquery_parts = [
                f"SELECT {partition_dim} AS {partition_dim}, SUM({metric_col}) AS __group_total",
                self.build_from_clause(),
            ]
            if base_where_clause:
                top_groups_subquery_parts.append(base_where_clause)
            top_groups_subquery_parts.append(f"GROUP BY {partition_dim}")
            top_groups_subquery_parts.append("ORDER BY __group_total DESC")
            top_groups_subquery_parts.append(f"LIMIT {int(group_top_n)}")

            top_groups_subquery = "\n".join(top_groups_subquery_parts)

            # Build WHERE that restricts main query to those top M groups (partition dimension)
            top_groups_in_clause = f"{partition_dim} IN (SELECT {partition_dim} FROM ({top_groups_subquery}) __top_groups)"

            # Combine existing where with the IN clause
            if base_where_clause:
                # base_where_clause starts with 'WHERE '
                where_clause = base_where_clause + f" AND {top_groups_in_clause}"
            else:
                where_clause = "WHERE " + top_groups_in_clause

            # Build final SQL using QUALIFY to pick top N items (item_dim) per group (partition_dim)
            qualify_clause = (
                f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {partition_dim} "
                f"ORDER BY SUM({metric_col}) DESC) <= {int(top_n)}"
            )

            # Default order: preserve order by partition_dim (main group) then item_dim (items within group)
            order_by_clause = f"ORDER BY {partition_dim} ASC, {item_dim} ASC"

            sql_parts = [select_clause, from_clause]
            if where_clause:
                sql_parts.append(where_clause)
            if group_by_clause:
                sql_parts.append(group_by_clause)

            # QUALIFY must come after GROUP BY in DuckDB
            sql_parts.append(qualify_clause)
            sql_parts.append(order_by_clause)

            sql = "\n".join(sql_parts)
            logger.debug(
                f"Generated two-step SQL for bar_vertical_stacked (top {group_top_n} groups × top {top_n} subgroups):\n{sql}"
            )
            return sql

        # Fallback: legacy behavior (single aggregated query identical to composed)
        # Permitir override do sort se especificado
        sort = chart_spec.get("sort")
        if sort and sort.get("by"):
            order_by_clause = self.build_order_by_clause(chart_spec)
        else:
            # Default: ordenar por ambas dimensions ASC
            order_by_clause = f"ORDER BY {dim1_escaped} ASC, {dim2_escaped} ASC"

        # Montar SQL final
        sql_parts = [select_clause, from_clause]

        if base_where_clause:
            sql_parts.append(base_where_clause)
        if group_by_clause:
            sql_parts.append(group_by_clause)
        sql_parts.append(order_by_clause)

        # LIMIT pode ser aplicado (menos comum em stacked)
        limit_clause = self.build_limit_clause(chart_spec)
        if limit_clause:
            sql_parts.append(limit_clause)

        sql = "\n".join(sql_parts)
        logger.debug(f"Generated SQL for bar_vertical_stacked (legacy):\n{sql}")

        return sql

    def build_plotly_config(
        self, chart_spec: Dict[str, Any], data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Constrói configuração Plotly para bar vertical stacked (stacked bars).

        Cria uma trace (segmento empilhado) para cada valor único da segunda dimension.

        Config Pattern:
            - type: "bar"
            - orientation: "v"
            - barmode: "stack" (CHAVE: barras empilhadas)
            - Múltiplas traces (uma por subcategoria)
            - x: valores da primeira dimension
            - y: valores da metric

        Args:
            chart_spec: ChartOutput original
            data: DataFrame com resultado da query

        Returns:
            dict: Configuração Plotly pronta para renderização

        Example:
            >>> data = pd.DataFrame({
            ...     "Região": ["Norte", "Sul", "Norte", "Sul"],
            ...     "Produto": ["A", "A", "B", "B"],
            ...     "Vendas": [100, 80, 150, 120]
            ... })
            >>> config = handler.build_plotly_config(chart_spec, data)
            >>> len(config["data"])  # 2 traces (Produto A e B)
            2
            >>> config["layout"]["barmode"]
            'stack'
        """
        dimension1_col = chart_spec["dimensions"][0].get(
            "alias", chart_spec["dimensions"][0]["name"]
        )
        dimension2_col = chart_spec["dimensions"][1].get(
            "alias", chart_spec["dimensions"][1]["name"]
        )
        metric_col = chart_spec["metrics"][0].get(
            "alias", chart_spec["metrics"][0]["name"]
        )

        # Configuração visual
        visual = chart_spec.get("visual", {})
        show_values = visual.get("show_values", False)
        palette_name = visual.get("palette", "default")

        # Obter valores únicos da segunda dimension
        unique_dim2 = data[dimension2_col].unique()
        colors = self._get_color_palette(palette_name, len(unique_dim2))

        # Criar uma trace por valor da segunda dimension
        traces = []
        for idx, dim2_value in enumerate(unique_dim2):
            # Filtrar dados para este valor da segunda dimension
            filtered_data = data[data[dimension2_col] == dim2_value]

            trace = {
                "type": "bar",
                "name": str(dim2_value),
                "x": filtered_data[dimension1_col].tolist(),
                "y": filtered_data[metric_col].tolist(),
                "marker": {"color": colors[idx]},
                "text": filtered_data[metric_col].tolist() if show_values else None,
                "textposition": "inside"
                if show_values
                else None,  # inside para stacked
                "hovertemplate": (
                    f"<b>%{{x}}</b><br>"
                    f"{dimension2_col}: {dim2_value}<br>"
                    f"{metric_col}: %{{y}}<br>"
                    "<extra></extra>"
                ),
            }
            traces.append(trace)

        config = {
            "data": traces,
            "layout": {
                "title": chart_spec.get("title", ""),
                "xaxis": {"title": dimension1_col},
                "yaxis": {"title": metric_col},
                "barmode": "stack",  # CHAVE: barras empilhadas verticalmente
                "showlegend": True,
                "legend": {"orientation": "v", "x": 1.02, "y": 1},
            },
        }

        logger.debug(
            f"Built Plotly config for bar_vertical_stacked with {len(traces)} segments"
        )
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


def tool_handle_bar_vertical_stacked(state: dict) -> dict:
    """
    Nó do LangGraph para processar bar vertical stacked charts.

    Args:
        state: AnalyticsState com chart_spec

    Returns:
        dict: State atualizado com result_dataframe e plotly_config
    """
    chart_spec = state["chart_spec"]
    schema = state.get("schema", {})
    data_source_path = chart_spec["data_source"]

    # Instanciar handler
    handler = ToolHandlerBarVerticalStacked(
        data_source_path=data_source_path, schema=schema
    )

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
