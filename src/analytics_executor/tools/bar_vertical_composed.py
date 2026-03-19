"""
Tool Handler for Bar Vertical Composed Charts (DEPRECATED - FASE 3).

⚠️ WARNING: This module is DEPRECATED and should NOT be used.
⚠️ All temporal comparisons have been migrated to line_composed.
⚠️ This executor is kept only for backward compatibility and will redirect
⚠️ calls to the line_composed executor.

MIGRATION NOTES (FASE 3):
- bar_vertical_composed was removed as a semantic type
- Temporal comparisons now use line_composed (semantic type)
- This executor logs critical warnings and delegates to line_composed

If you see this executor being called, it indicates a REGRESSION in the
classification pipeline that needs to be fixed immediately.
"""

from typing import Dict, Any, List
import pandas as pd
import logging

from .base import BaseToolHandler

logger = logging.getLogger(__name__)


class ToolHandlerBarVerticalComposed(BaseToolHandler):
    """
    DEPRECATED: Tool handler para bar vertical composed charts.

    This class is deprecated as of FASE 3. All calls are redirected to
    the line_composed handler with critical logging.
    """

    def __init__(self):
        """Initialize the deprecated handler with warnings."""
        super().__init__()
        logger.critical(
            "[ToolHandlerBarVerticalComposed] DEPRECATED EXECUTOR INSTANTIATED! "
            "This should NEVER happen in FASE 3. All temporal comparisons "
            "should use line_composed executor."
        )

    def validate_chart_spec(self, chart_spec: Dict[str, Any]) -> None:
        """
        DEPRECATED: Validation for bar vertical composed.

        ⚠️ CRITICAL WARNING: This method should NEVER be called in FASE 3+.
        ⚠️ If you're seeing this log, there's a REGRESSION in the classification pipeline.

        The classification system should auto-correct bar_vertical_composed to
        line_composed at multiple validation layers before reaching this executor.
        """
        logger.critical(
            "[ToolHandlerBarVerticalComposed.validate_chart_spec] "
            "DEPRECATED EXECUTOR METHOD CALLED! This indicates a classification "
            "pipeline regression. Chart spec should use 'line_composed', not "
            "'bar_vertical_composed'."
        )

        # Log the chart spec for debugging
        logger.error(
            f"[ToolHandlerBarVerticalComposed] Received chart_spec with type: "
            f"{chart_spec.get('chart_type')} (expected: should be line_composed by now)"
        )
        """
        Valida requirements específicos de bar vertical composed.

        Validações:
        - Exatamente 2 dimensions
        - Mínimo 1 metric

        Args:
            chart_spec: ChartOutput validado do graphic_classifier

        Raises:
            ValueError: Se não atende requirements de bar vertical composed
        """
        # Validação base (campos obrigatórios e colunas existem)
        super().validate_chart_spec(chart_spec)

        # Validações específicas
        dimensions = chart_spec.get("dimensions", [])

        # Deve ter exatamente 2 dimensions
        if len(dimensions) != 2:
            raise ValueError(
                f"Bar vertical composed requires EXACTLY 2 dimensions, got {len(dimensions)}. "
                f"First dimension represents categories on X axis, "
                f"second dimension represents grouped bars within each category."
            )

        metrics = chart_spec.get("metrics", [])
        if len(metrics) < 1:
            raise ValueError(
                f"Bar vertical composed requires at least 1 metric, got {len(metrics)}"
            )

        logger.debug(
            f"Bar vertical composed validation passed: "
            f"dimension1={dimensions[0]['name']}, "
            f"dimension2={dimensions[1]['name']}, "
            f"metrics={[m['name'] for m in metrics]}"
        )

    def build_sql(self, chart_spec: Dict[str, Any]) -> str:
        """
        Constrói SQL para bar vertical composed chart.

        SQL Pattern:
            SELECT dimension1, dimension2, AGG(metric) as metric_alias
            FROM dataset
            WHERE filters
            GROUP BY dimension1, dimension2
            ORDER BY dimension1 ASC, dimension2 ASC

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
        # Construir cláusulas usando métodos da base
        select_clause = self.build_select_clause(chart_spec)
        from_clause = self.build_from_clause()
        where_clause = self.build_where_clause(chart_spec.get("filters", {}))
        group_by_clause = self.build_group_by_clause(chart_spec)

        # Order by: dimension1 ASC, dimension2 ASC (padrão para barras agrupadas)
        dimension1 = chart_spec["dimensions"][0]["name"]
        dimension2 = chart_spec["dimensions"][1]["name"]

        dim1_escaped = self.sql_builder.escape_identifier(dimension1)
        dim2_escaped = self.sql_builder.escape_identifier(dimension2)

        # Permitir override do sort se especificado
        sort = chart_spec.get("sort")
        if sort and sort.get("by"):
            order_by_clause = self.build_order_by_clause(chart_spec)
        else:
            # Default: ordenar por ambas dimensions ASC
            order_by_clause = f"ORDER BY {dim1_escaped} ASC, {dim2_escaped} ASC"

        # Montar SQL final
        sql_parts = [select_clause, from_clause]

        if where_clause:
            sql_parts.append(where_clause)
        if group_by_clause:
            sql_parts.append(group_by_clause)
        sql_parts.append(order_by_clause)

        # LIMIT pode ser aplicado (menos comum em composed)
        limit_clause = self.build_limit_clause(chart_spec)
        if limit_clause:
            sql_parts.append(limit_clause)

        sql = "\n".join(sql_parts)
        logger.debug(f"Generated SQL for bar_vertical_composed:\n{sql}")

        return sql

    def build_plotly_config(
        self, chart_spec: Dict[str, Any], data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Constrói configuração Plotly para bar vertical composed (grouped bars).

        Cria uma trace (grupo de barras) para cada valor único da segunda dimension.

        FASE 4 - Correção #3: Para temporal_comparison_analysis, calcula variação
        e cria visualização com dados pivotados.

        Config Pattern:
            - type: "bar"
            - orientation: "v"
            - barmode: "group"
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
            'group'
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

        # FASE 4 - Correção #3: Calcular variação para comparações temporais
        intent = chart_spec.get("intent", "")
        if intent == "temporal_comparison_analysis":
            logger.info(
                "[build_plotly_config] Temporal comparison detected, "
                "calculating variation"
            )
            data = self._calculate_variation(
                data, metric_col, dimension1_col, dimension2_col, chart_spec
            )

        # Configuração visual
        visual = chart_spec.get("visual", {})
        show_values = visual.get("show_values", False)
        palette_name = visual.get("palette", "default")

        # FASE 4 - Correção #3: Detectar se dados foram pivotados
        # Após pivotação, dimension2_col não existe mais - períodos viram colunas
        is_pivoted = (
            intent == "temporal_comparison_analysis"
            and dimension2_col not in data.columns
        )

        traces = []

        if is_pivoted:
            # Dados pivotados: cada período é uma coluna separada
            # Colunas: [entity_dim, May, June, variation, variation_pct]
            period_columns = [
                col
                for col in data.columns
                if col not in [dimension1_col, "variation", "variation_pct"]
            ]

            colors = self._get_color_palette(palette_name, len(period_columns))

            logger.info(
                f"[build_plotly_config] Creating {len(period_columns)} traces for "
                f"pivoted temporal data: {period_columns}"
            )

            # Criar uma trace por período
            for idx, period in enumerate(period_columns):
                trace = {
                    "type": "bar",
                    "name": str(period),
                    "x": data[dimension1_col].tolist(),
                    "y": data[period].tolist(),
                    "marker": {"color": colors[idx]},
                    "text": data[period].tolist() if show_values else None,
                    "textposition": "auto" if show_values else None,
                    "hovertemplate": (
                        f"<b>%{{x}}</b><br>"
                        f"Período: {period}<br>"
                        f"{metric_col}: %{{y}}<br>"
                        "<extra></extra>"
                    ),
                }
                traces.append(trace)
        else:
            # Dados não-pivotados: estrutura normal com dimension2_col
            unique_dim2 = data[dimension2_col].unique()
            colors = self._get_color_palette(palette_name, len(unique_dim2))

            # Criar uma trace por valor da segunda dimension
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
                    "textposition": "auto" if show_values else None,
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
                "barmode": "group",  # CHAVE: barras lado a lado
                "showlegend": True,
                "legend": {"orientation": "v", "x": 1.02, "y": 1},
            },
        }

        logger.debug(
            f"Built Plotly config for bar_vertical_composed with {len(traces)} groups"
        )
        return config

    def _calculate_variation(
        self,
        data: pd.DataFrame,
        metric_col: str,
        dimension1_col: str,
        dimension2_col: str,
        chart_spec: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Calcula variação entre períodos temporais para análises de comparação.

        FASE 4 - CORREÇÃO CRÍTICA ESTRUTURAL:
        - Identificação explícita e confiável de dimensões
        - Pivotagem declarada e auditável
        - Ordenação temporal cronológica (não lexicográfica)
        - Tratamento explícito de divisão por zero
        - Aplicação de sort/top_n em etapa separada

        Esta função resolve os seguintes problemas:
        1. Detecção frágil de dimensão temporal
        2. Pivotagem implícita não declarada
        3. Ordenação lexicográfica incorreta
        4. Fallback silencioso para divisão por zero
        5. Mistura de transformação com apresentação

        Args:
            data: DataFrame original com dados agregados
            metric_col: Nome da coluna de métrica
            dimension1_col: Nome da primeira dimensão
            dimension2_col: Nome da segunda dimensão
            chart_spec: Especificação do gráfico (para verificar intent)

        Returns:
            DataFrame pivotado com colunas de variação

        Example:
            Input:
            | Produto | Month | Vendas |
            |---------|-------|--------|
            | A       | May   | 100    |
            | A       | June  | 150    |

            Output:
            | Produto | May | June | variation | variation_pct |
            |---------|-----|------|-----------|---------------|
            | A       | 100 | 150  | 50        | 50.0          |
        """
        # Verificar se é um intent de comparação temporal
        intent = chart_spec.get("intent", "")
        if intent != "temporal_comparison_analysis":
            logger.debug(
                f"[_calculate_variation] Skipping variation calculation for intent: {intent}"
            )
            return data

        # FASE 4 - CORREÇÃO #1: Identificação EXPLÍCITA de dimensões
        dimensions = chart_spec.get("dimensions", [])

        temporal_dim_info = None
        entity_dim_info = None

        # Identificar baseado em temporal_granularity E fallback para nome de coluna
        for dim in dimensions:
            if dim.get("temporal_granularity"):
                temporal_dim_info = dim
            elif dim.get("name") in [
                "Mes",
                "Month",
                "Ano",
                "Year",
                "Trimestre",
                "Quarter",
            ]:
                # Fallback: nomes conhecidos de colunas temporais
                temporal_dim_info = dim
            else:
                entity_dim_info = dim

        # Validação EXPLÍCITA: falhar se não conseguir identificar
        if not temporal_dim_info or not entity_dim_info:
            logger.error(
                "[_calculate_variation] FAILED to identify temporal and entity dimensions. "
                f"Dimensions: {dimensions}"
            )
            logger.error(f"  temporal_dim_info: {temporal_dim_info}")
            logger.error(f"  entity_dim_info: {entity_dim_info}")
            raise ValueError(
                "Cannot calculate temporal variation: unable to identify temporal and entity dimensions"
            )

        temporal_dim = temporal_dim_info.get("alias", temporal_dim_info["name"])
        entity_dim = entity_dim_info.get("alias", entity_dim_info["name"])

        logger.info(
            f"[_calculate_variation] Identified dimensions: "
            f"entity='{entity_dim}', temporal='{temporal_dim}', metric='{metric_col}'"
        )

        try:
            # FASE 4 - CORREÇÃO #2: Pivotagem EXPLÍCITA e DECLARADA
            logger.info("[_calculate_variation] Pivoting data for temporal comparison")

            pivot_df = data.pivot_table(
                index=entity_dim,
                columns=temporal_dim,
                values=metric_col,
                aggfunc="sum",
                fill_value=0,  # Preencher valores ausentes com 0
            ).reset_index()

            # Obter lista de períodos (colunas do pivot)
            period_columns = [col for col in pivot_df.columns if col != entity_dim]

            if len(period_columns) < 2:
                logger.warning(
                    f"[_calculate_variation] Need at least 2 periods for comparison, "
                    f"got {len(period_columns)}: {period_columns}"
                )
                return data

            # FASE 4 - CORREÇÃO #3: Ordenação TEMPORAL CORRETA (não lexicográfica)
            temporal_granularity = temporal_dim_info.get(
                "temporal_granularity", ""
            ).lower()

            if temporal_granularity == "month":
                # Ordenar meses cronologicamente
                month_order = {
                    "January": 1,
                    "Jan": 1,
                    "Janeiro": 1,
                    "February": 2,
                    "Feb": 2,
                    "Fevereiro": 2,
                    "March": 3,
                    "Mar": 3,
                    "Março": 3,
                    "April": 4,
                    "Apr": 4,
                    "Abril": 4,
                    "May": 5,
                    "Mai": 5,
                    "Maio": 5,
                    "June": 6,
                    "Jun": 6,
                    "Junho": 6,
                    "July": 7,
                    "Jul": 7,
                    "Julho": 7,
                    "August": 8,
                    "Aug": 8,
                    "Agosto": 8,
                    "September": 9,
                    "Sep": 9,
                    "Setembro": 9,
                    "October": 10,
                    "Oct": 10,
                    "Outubro": 10,
                    "November": 11,
                    "Nov": 11,
                    "Novembro": 11,
                    "December": 12,
                    "Dec": 12,
                    "Dezembro": 12,
                }
                period_columns = sorted(
                    period_columns,
                    key=lambda x: month_order.get(x, 99),  # Unknown months go to end
                )
                logger.info(
                    f"[_calculate_variation] Sorted months chronologically: {period_columns}"
                )
            else:
                # Para outros casos, assumir ordenação alfabética é aceitável
                period_columns = sorted(period_columns)
                logger.info(f"[_calculate_variation] Sorted periods: {period_columns}")

            # Calcular variação entre primeiro e último período
            first_period = period_columns[0]
            last_period = period_columns[-1]

            logger.info(
                f"[_calculate_variation] Calculating variation: "
                f"{last_period} - {first_period}"
            )

            # FASE 4 - CORREÇÃO #4: Tratamento EXPLÍCITO de divisão por zero
            def safe_variation_pct(row):
                """Calcula variação percentual com tratamento de casos especiais."""
                first_val = row[first_period]
                last_val = row[last_period]

                if first_val == 0:
                    if last_val == 0:
                        return 0.0  # Sem mudança (ambos zero)
                    else:
                        return None  # Crescimento infinito (não representável como %)

                return ((last_val - first_val) / first_val) * 100

            # Variação absoluta
            pivot_df["variation"] = pivot_df[last_period] - pivot_df[first_period]

            # Variação percentual (com tratamento)
            pivot_df["variation_pct"] = pivot_df.apply(safe_variation_pct, axis=1)

            # Log de valores nulos gerados
            null_count = pivot_df["variation_pct"].isna().sum()
            if null_count > 0:
                logger.warning(
                    f"[_calculate_variation] {null_count} entities have undefined "
                    f"variation_pct (division by zero - base period = 0)"
                )

            # FASE 4 - CORREÇÃO #5: Aplicar sort e top_n APÓS cálculo (etapa separada)
            sort_config = chart_spec.get("sort", {})
            if sort_config.get("by") == "variation":
                sort_order = sort_config.get("order", "desc")
                ascending = sort_order == "asc"

                # Ordenar, colocando NaN ao final
                pivot_df = pivot_df.sort_values(
                    by="variation",
                    ascending=ascending,
                    na_position="last",  # NaN vai para o final
                ).reset_index(drop=True)

                logger.info(
                    f"[_calculate_variation] Sorted by variation ({sort_order})"
                )

            # Aplicar top_n se especificado
            top_n = chart_spec.get("top_n")
            if top_n and top_n > 0:
                pivot_df = pivot_df.head(top_n)
                logger.info(f"[_calculate_variation] Limited to top {top_n} entities")

            logger.info(
                f"[_calculate_variation] ✅ Successfully calculated variation for "
                f"{len(pivot_df)} entities across {len(period_columns)} periods"
            )

            return pivot_df

        except Exception as e:
            logger.error(
                f"[_calculate_variation] Error calculating variation: {str(e)}",
                exc_info=True,
            )
            # Em caso de erro, retornar dados originais (fallback seguro)
            return data

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


def tool_handle_bar_vertical_composed(state: dict) -> dict:
    """
    Nó do LangGraph para processar bar vertical composed charts.

    FASE 4 - Correção #3: Suporta cálculo automático de variação para
    análises de temporal_comparison_analysis.

    Args:
        state: AnalyticsState com chart_spec

    Returns:
        dict: State atualizado com result_dataframe e plotly_config
    """
    chart_spec = state["chart_spec"]
    schema = state.get("schema", {})
    data_source_path = chart_spec["data_source"]

    # Log informações de debug para temporal comparison
    intent = chart_spec.get("intent", "")
    if intent == "temporal_comparison_analysis":
        logger.info(
            "[tool_handle_bar_vertical_composed] Temporal comparison analysis detected:\n"
            f"  - Intent: {intent}\n"
            f"  - Dimensions: {[d.get('alias', d['name']) for d in chart_spec.get('dimensions', [])]}\n"
            f"  - Metrics: {[m.get('alias', m['name']) for m in chart_spec.get('metrics', [])]}\n"
            f"  - Sort: {chart_spec.get('sort', {})}\n"
            f"  - Filters: {chart_spec.get('filters', {})}"
        )

    # Instanciar handler
    handler = ToolHandlerBarVerticalComposed(
        data_source_path=data_source_path, schema=schema
    )

    # Executar pipeline completo
    result_df = handler.execute(chart_spec)

    # Log resultado após execução SQL
    logger.info(
        f"[tool_handle_bar_vertical_composed] SQL execution complete: "
        f"{len(result_df)} rows, columns={list(result_df.columns)}"
    )

    # Build plotly config (aqui que a variação é calculada se necessário)
    plotly_config = handler.build_plotly_config(chart_spec, result_df)

    # Log após plotly config (dados podem ter sido pivotados)
    logger.info(
        f"[tool_handle_bar_vertical_composed] Plotly config built: "
        f"{len(plotly_config.get('data', []))} traces created"
    )

    # Atualizar state
    state["result_dataframe"] = result_df
    state["plotly_config"] = plotly_config
    state["execution_success"] = True
    state["sql_query"] = handler.build_sql(chart_spec)
    state["engine_used"] = "DuckDB"

    return state
