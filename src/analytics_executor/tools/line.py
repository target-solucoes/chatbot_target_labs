"""
Tool Handler for Line Charts.

⚠️ DEPRECATION WARNING ⚠️
This module is DEPRECATED as of FASE 2 (Line Family Redefinition).
Use ToolHandlerLineComposed instead for all temporal series visualization.

MIGRATION GUIDE:
- Old: chart_type="line" with 1 dimension
- New: chart_type="line_composed" with render_variant="single_line"

The semantic type is now always "line_composed".
Visual variant (single vs multi) is decided by RenderSelector.

This module implements the specific logic for line charts, including:
- Validation for temporal dimension requirements
- SQL generation with chronological ordering
- Plotly configuration for line visualization with temporal data
"""

import warnings
from typing import Dict, Any, Set
import pandas as pd
import logging

from .base import BaseToolHandler

logger = logging.getLogger(__name__)


class ToolHandlerLine(BaseToolHandler):
    """
    Tool handler para line charts (single series).

    ⚠️ DEPRECATED: Use ToolHandlerLineComposed instead.

    Requirements:
    - Exatamente 1 dimension (geralmente temporal para eixo X)
    - Mínimo 1 metric (valores para eixo Y)
    - Sort ASC obrigatório (ordem cronológica)
    - Primeira dimension preferencialmente temporal

    SQL Pattern:
        SELECT temporal_dim, AGG(metric) as metric_alias
        FROM dataset
        WHERE filters
        GROUP BY temporal_dim
        ORDER BY temporal_dim ASC

    Plotly Config:
        - type: "scatter"
        - mode: "lines" ou "lines+markers"
        - x: temporal dimension
        - y: metric values
    """

    def __init__(self):
        """Initialize with deprecation warning."""
        super().__init__()
        warnings.warn(
            "ToolHandlerLine is deprecated. Use ToolHandlerLineComposed with "
            "render_variant='single_line' instead. This handler will be removed "
            "in a future version.",
            DeprecationWarning,
            stacklevel=2,
        )
        logger.warning(
            "[ToolHandlerLine] DEPRECATED: This handler is deprecated. "
            "Use ToolHandlerLineComposed instead."
        )

    # Keywords que indicam dimensões temporais
    TEMPORAL_KEYWORDS: Set[str] = {
        "data",
        "date",
        "datetime",
        "mes",
        "month",
        "ano",
        "year",
        "trimestre",
        "quarter",
        "semestre",
        "dia",
        "day",
        "semana",
        "week",
        "hora",
        "hour",
        "tempo",
        "time",
        "periodo",
        "period",
    }

    def validate_chart_spec(self, chart_spec: Dict[str, Any]) -> None:
        """
        Valida requirements específicos de line chart.

        Validações:
        - Exatamente 1 dimension
        - Mínimo 1 metric
        - Dimension preferencialmente temporal (warning se não for)
        - Sort não pode ser DESC (ordem cronológica)

        Args:
            chart_spec: ChartOutput validado do graphic_classifier

        Raises:
            ValueError: Se não atende requirements de line chart
        """
        # Validação base (campos obrigatórios e colunas existem)
        super().validate_chart_spec(chart_spec)

        # Validações específicas
        dimensions = chart_spec.get("dimensions", [])
        if len(dimensions) != 1:
            raise ValueError(
                f"Line chart requires EXACTLY 1 dimension, got {len(dimensions)}. "
                f"Use line_composed for multiple series."
            )

        metrics = chart_spec.get("metrics", [])
        if len(metrics) < 1:
            raise ValueError(
                f"Line chart requires at least 1 metric, got {len(metrics)}"
            )

        # Verificar se dimension é temporal (warning apenas, não é obrigatório)
        dimension = dimensions[0]
        if not self._is_temporal_dimension(dimension):
            logger.warning(
                f"Line chart dimension '{dimension['name']}' does not appear to be temporal. "
                f"Line charts typically use temporal dimensions for X-axis. "
                f"Consider using bar chart if this is categorical data."
            )

        # Verificar sort não é DESC (ordem cronológica obrigatória)
        sort = chart_spec.get("sort", {})
        if sort.get("order", "").lower() == "desc":
            logger.warning(
                "Line chart with DESC sort detected. Changing to ASC for chronological order. "
                "Line charts should display data in chronological sequence."
            )
            # Não levantamos exceção, apenas corrigimos silenciosamente

        logger.debug(
            f"Line chart validation passed: "
            f"dimension={dimension['name']}, "
            f"is_temporal={self._is_temporal_dimension(dimension)}, "
            f"metrics={[m['name'] for m in metrics]}"
        )

    def _is_temporal_dimension(self, dimension: Dict[str, Any]) -> bool:
        """
        Verifica se dimension é temporal.

        Detecta através de:
        1. Keyword matching no nome da coluna
        2. Campo temporal_granularity presente
        3. Campo data_type indicando temporal (future enhancement)

        Args:
            dimension: Dict com informações da dimension

        Returns:
            bool: True se dimension parece ser temporal
        """
        name_lower = dimension["name"].lower()

        # Check 1: Keyword matching
        if any(keyword in name_lower for keyword in self.TEMPORAL_KEYWORDS):
            logger.debug(
                f"Dimension '{dimension['name']}' detected as temporal (keyword match)"
            )
            return True

        # Check 2: temporal_granularity field presente
        if dimension.get("temporal_granularity"):
            logger.debug(
                f"Dimension '{dimension['name']}' detected as temporal "
                f"(granularity: {dimension['temporal_granularity']})"
            )
            return True

        # Check 3: Alias também pode conter keyword temporal
        alias_lower = dimension.get("alias", "").lower()
        if any(keyword in alias_lower for keyword in self.TEMPORAL_KEYWORDS):
            logger.debug(
                f"Dimension alias '{dimension.get('alias')}' detected as temporal"
            )
            return True

        return False

    def _convert_temporal_values(
        self, x_values: list, dimension_spec: Dict[str, Any], filters: Dict[str, Any]
    ) -> tuple:
        """
        Converte valores temporais numéricos em strings de data quando possível.

        Para valores como mês (1-12), trimestre (1-4), etc., tenta extrair
        o ano dos filtros e converter para formato de data adequado.

        Args:
            x_values: Lista de valores do eixo X
            dimension_spec: Especificação da dimensão temporal
            filters: Filtros aplicados (pode conter informação de ano)

        Returns:
            tuple: (valores_convertidos, tipo_eixo)
                - valores_convertidos: Lista com valores (strings de data ou originais)
                - tipo_eixo: "date", "category" ou "linear"
        """
        if not x_values:
            return x_values, "linear"

        # Se valores já são strings de data (contém "-" ou "/"), manter como date
        if isinstance(x_values[0], str) and any(
            char in str(x_values[0]) for char in ["-", "/"]
        ):
            logger.debug(
                f"X values are already date strings, using type='date': {x_values[0]}"
            )
            return x_values, "date"

        # Se valores não são numéricos, usar como categoria
        if not all(isinstance(v, (int, float)) for v in x_values):
            logger.debug(f"X values are not numeric, using type='category'")
            return x_values, "category"

        # Valores numéricos: tentar converter baseado em temporal_granularity
        granularity = dimension_spec.get("temporal_granularity", "").lower()

        # Extrair ano dos filtros
        year = self._extract_year_from_filters(filters)

        if not year:
            # Sem contexto de ano, usar valores numéricos como linear ou category
            logger.debug(
                f"No year context found in filters, using numeric values as type='linear'"
            )
            return x_values, "linear"

        # Converter baseado na granularidade
        converted_values = []

        if granularity == "month":
            # Converter mês (1-12) para "YYYY-MM-01" (primeiro dia do mês)
            for month in x_values:
                try:
                    month_int = int(month)
                    if 1 <= month_int <= 12:
                        # Adicionar dia 01 para formato de data completo
                        converted_values.append(f"{year}-{month_int:02d}-01")
                    else:
                        converted_values.append(str(month))
                except (ValueError, TypeError):
                    converted_values.append(str(month))

            logger.info(
                f"Converted {len(x_values)} month values to date format (year={year}): "
                f"{x_values[0]} -> {converted_values[0]}"
            )
            return converted_values, "date"

        elif granularity == "quarter":
            # Converter trimestre (1-4) para primeiro dia do primeiro mês do trimestre
            for quarter in x_values:
                try:
                    quarter_int = int(quarter)
                    if 1 <= quarter_int <= 4:
                        # Usar primeiro mês do trimestre com dia 01
                        month = (quarter_int - 1) * 3 + 1
                        converted_values.append(f"{year}-{month:02d}-01")
                    else:
                        converted_values.append(str(quarter))
                except (ValueError, TypeError):
                    converted_values.append(str(quarter))

            logger.info(
                f"Converted {len(x_values)} quarter values to date format (year={year})"
            )
            return converted_values, "date"

        elif granularity in ["day", "date"]:
            # Se granularidade é dia mas valores são numéricos, usar linear
            logger.debug(f"Day granularity with numeric values, using type='linear'")
            return x_values, "linear"

        # Outras granularidades: usar valores originais como linear
        logger.debug(
            f"Granularity '{granularity}' not supported for conversion, using type='linear'"
        )
        return x_values, "linear"

    def _extract_year_from_filters(self, filters: Dict[str, Any]) -> int:
        """
        Extrai o ano dos filtros aplicados.

        Procura por filtros de data e extrai o ano.
        Suporta formatos:
        - {"Data": ["2015-01-01", "2015-12-31"]}
        - {"Data": {"between": ["2015-01-01", "2015-12-31"]}}
        - {"Ano": 2015}

        Args:
            filters: Dicionário de filtros

        Returns:
            int: Ano extraído ou None se não encontrado
        """
        if not filters:
            return None

        # Procurar por filtro de Ano direto
        if "Ano" in filters:
            try:
                return int(filters["Ano"])
            except (ValueError, TypeError):
                pass

        # Procurar por filtro de Data (dynamic temporal column from DatasetConfig)
        temporal_col_name = None
        try:
            from src.shared_lib.core.dataset_config import DatasetConfig

            temporal_col_name = DatasetConfig.get_instance().temporal_column_name
        except Exception:
            try:
                from src.shared_lib.core.config import get_temporal_columns

                _tc = get_temporal_columns()
                temporal_col_name = _tc[0] if _tc else None
            except Exception:
                pass

        date_filter = None
        if temporal_col_name:
            date_filter = filters.get(temporal_col_name)

        if not date_filter:
            return None

        # Extrair data do filtro
        date_value = None

        if isinstance(date_filter, dict):
            # Formato: {"between": ["2015-01-01", "2015-12-31"]}
            if "between" in date_filter and date_filter["between"]:
                date_value = date_filter["between"][0]

        elif isinstance(date_filter, list) and date_filter:
            # Formato: ["2015-01-01", "2015-12-31"]
            date_value = date_filter[0]

        elif isinstance(date_filter, str):
            # Formato: "2015-01-01"
            date_value = date_filter

        # Extrair ano da string de data
        if date_value:
            try:
                # Converter para string se for timestamp
                date_str = str(date_value)

                # Extrair ano (primeiros 4 dígitos após possível timestamp prefix)
                import re

                year_match = re.search(r"(\d{4})", date_str)
                if year_match:
                    year = int(year_match.group(1))
                    logger.debug(f"Extracted year {year} from filter: {date_value}")
                    return year

            except (ValueError, TypeError, AttributeError):
                pass

        logger.debug("Could not extract year from filters")
        return None

    def build_sql(self, chart_spec: Dict[str, Any]) -> str:
        """
        Constrói SQL para line chart.

        SQL Pattern:
            SELECT DATE_TRUNC('month', Data) AS temporal_alias, AGG(metric) as metric_alias
            FROM dataset
            WHERE filters
            GROUP BY DATE_TRUNC('month', Data)
            ORDER BY DATE_TRUNC('month', Data) ASC  -- SEMPRE ASC para ordem cronológica

        REFACTORED (Fase 2): Usa DATE_TRUNC na coluna Data para manter tipo datetime,
        em vez de usar colunas auxiliares inteiras (Mes, Ano).

        Args:
            chart_spec: ChartOutput validado

        Returns:
            str: Query SQL completa e executável

        Example:
            >>> chart_spec = {
            ...     "dimensions": [{"name": "Mes", "alias": "Mes", "temporal_granularity": "month"}],
            ...     "metrics": [{"name": "revenue", "aggregation": "sum", "alias": "Receita"}],
            ...     "sort": {"by": "Mes", "order": "asc"}
            ... }
            >>> handler.build_sql(chart_spec)
            'SELECT DATE_TRUNC(\\'month\\', Data) AS \"Mes\", SUM(revenue) AS \"Receita\"...'
        """
        # Obter especificação da dimensão temporal
        temporal_dim_spec = chart_spec["dimensions"][0]
        temporal_dim = temporal_dim_spec["name"]
        temporal_alias = temporal_dim_spec.get("alias", temporal_dim)
        granularity = temporal_dim_spec.get("temporal_granularity", "").lower()

        # Resolve base temporal column from DatasetConfig
        try:
            from src.shared_lib.core.dataset_config import DatasetConfig

            _tc = DatasetConfig.get_instance().temporal_column_name
            base_temporal_col = _tc if _tc else None
        except Exception:
            try:
                from src.shared_lib.core.config import get_temporal_columns

                _temporal_cols = get_temporal_columns()
                base_temporal_col = _temporal_cols[0] if _temporal_cols else None
            except Exception:
                base_temporal_col = None

        if base_temporal_col is None:
            # No temporal column: use the dimension name directly (non-temporal dataset)
            base_temporal_col = temporal_dim

        # Construir SELECT clause customizada para usar DATE_TRUNC
        select_parts = []

        # Dimension temporal: usar DATE_TRUNC se for Mes, Ano, Trimestre, etc.
        if self._should_use_date_trunc(temporal_dim, granularity):
            trunc_granularity = self._map_granularity_to_date_trunc(
                temporal_dim, granularity
            )
            temporal_escaped_alias = self.sql_builder.escape_identifier(temporal_alias)
            select_parts.append(
                f"DATE_TRUNC('{trunc_granularity}', \"{base_temporal_col}\") AS {temporal_escaped_alias}"
            )
            logger.info(
                f"[build_sql] Using DATE_TRUNC('{trunc_granularity}', \"{base_temporal_col}\") for temporal dimension"
            )
        else:
            temporal_escaped = self.sql_builder.escape_identifier(temporal_dim)
            temporal_escaped_alias = self.sql_builder.escape_identifier(temporal_alias)
            select_parts.append(f"{temporal_escaped} AS {temporal_escaped_alias}")

        # Metrics (com agregação)
        for metric in chart_spec.get("metrics", []):
            col = self.sql_builder.escape_identifier(metric["name"])
            column_name = metric["name"]

            user_specified_agg = metric.get("aggregation", "sum")
            selected_agg = self.aggregation_selector.select_aggregation(
                column_name=column_name,
                schema=self.schema,
                user_specified=user_specified_agg,
            )

            column_type = self.aggregation_selector.get_column_type(
                column_name, self.schema
            )
            validated_agg, was_corrected = (
                self.aggregation_validator.validate_and_correct(
                    column_name=column_name,
                    aggregation=selected_agg,
                    column_type=column_type,
                )
            )

            agg = validated_agg.upper()
            alias = self.sql_builder.escape_identifier(
                metric.get("alias", metric["name"])
            )
            select_parts.append(f"{agg}({col}) AS {alias}")

        select_clause = "SELECT " + ", ".join(select_parts)

        # Construir demais cláusulas
        from_clause = self.build_from_clause()
        where_clause = self.build_where_clause(chart_spec.get("filters", {}))

        # GROUP BY: usar mesma expressão DATE_TRUNC se aplicável
        if self._should_use_date_trunc(temporal_dim, granularity):
            trunc_granularity = self._map_granularity_to_date_trunc(
                temporal_dim, granularity
            )
            group_by_clause = (
                f"GROUP BY DATE_TRUNC('{trunc_granularity}', \"{base_temporal_col}\")"
            )
        else:
            temporal_escaped = self.sql_builder.escape_identifier(temporal_dim)
            group_by_clause = f"GROUP BY {temporal_escaped}"

        # ORDER BY forçado para ASC (ordem cronológica)
        if self._should_use_date_trunc(temporal_dim, granularity):
            trunc_granularity = self._map_granularity_to_date_trunc(
                temporal_dim, granularity
            )
            order_by_clause = f"ORDER BY DATE_TRUNC('{trunc_granularity}', \"{base_temporal_col}\") ASC"
        else:
            dimension_col = temporal_dim_spec.get("alias", temporal_dim)
            order_by_clause = (
                f"ORDER BY {self.sql_builder.escape_identifier(dimension_col)} ASC"
            )

        # LIMIT geralmente não usado em line charts (queremos ver toda a série)
        limit_clause = self.build_limit_clause(chart_spec)

        # Montar SQL final
        sql_parts = [select_clause, from_clause]

        if where_clause:
            sql_parts.append(where_clause)
        sql_parts.append(group_by_clause)
        sql_parts.append(order_by_clause)  # SEMPRE incluir ORDER BY
        if limit_clause:
            sql_parts.append(limit_clause)

        sql = "\n".join(sql_parts)
        logger.debug(f"Generated SQL for line chart:\n{sql}")

        return sql

    def _should_use_date_trunc(self, dim_name: str, granularity: str) -> bool:
        """
        Determina se deve usar DATE_TRUNC baseado no nome da dimensão e granularidade.

        Args:
            dim_name: Nome da dimensão
            granularity: Granularidade temporal

        Returns:
            bool: True se deve usar DATE_TRUNC
        """
        auxiliary_cols = {
            "mes",
            "ano",
            "trimestre",
            "dia",
            "month",
            "year",
            "quarter",
            "day",
        }
        if dim_name.lower() in auxiliary_cols:
            return True
        if granularity in {"month", "year", "quarter", "day"}:
            return True
        return False

    def _map_granularity_to_date_trunc(self, dim_name: str, granularity: str) -> str:
        """
        Mapeia nome de dimensão ou granularidade para argumento DATE_TRUNC.

        Args:
            dim_name: Nome da dimensão
            granularity: Granularidade

        Returns:
            str: Argumento para DATE_TRUNC
        """
        if granularity:
            return granularity.lower()

        mapping = {
            "mes": "month",
            "month": "month",
            "ano": "year",
            "year": "year",
            "trimestre": "quarter",
            "quarter": "quarter",
            "dia": "day",
            "day": "day",
        }

        return mapping.get(dim_name.lower(), "month")

    def build_plotly_config(
        self, chart_spec: Dict[str, Any], data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Constrói configuração Plotly específica para line chart.

        Config Pattern:
            - type: "scatter"
            - mode: "lines" ou "lines+markers"
            - x: temporal dimension
            - y: metric values
            - line: configurações de estilo

        Args:
            chart_spec: ChartOutput original
            data: DataFrame com resultado da query

        Returns:
            dict: Configuração Plotly pronta para renderização

        Example:
            >>> data = pd.DataFrame({
            ...     "Data": ["2023-01", "2023-02", "2023-03"],
            ...     "Receita": [1000, 1200, 1100]
            ... })
            >>> config = handler.build_plotly_config(chart_spec, data)
            >>> config["data"][0]["type"]
            'scatter'
            >>> config["data"][0]["mode"]
            'lines+markers'
        """
        dimension_col = chart_spec["dimensions"][0].get(
            "alias", chart_spec["dimensions"][0]["name"]
        )
        metric_col = chart_spec["metrics"][0].get(
            "alias", chart_spec["metrics"][0]["name"]
        )

        # Configuração visual do spec
        visual = chart_spec.get("visual", {})
        show_markers = visual.get("show_markers", True)
        color = visual.get("color", "#1f77b4")
        line_width = visual.get("line_width", 2)

        # Determinar mode
        mode = "lines+markers" if show_markers else "lines"

        # Processar valores do eixo X para temporal
        x_values = data[dimension_col].tolist()
        x_axis_type = "linear"

        dimension_spec = chart_spec["dimensions"][0]
        is_temporal = self._is_temporal_dimension(dimension_spec)

        # REFACTORED (Fase 2): Converter timestamps para ISO strings
        if is_temporal:
            # Verificar se valores são timestamps/datetime
            if x_values and hasattr(x_values[0], "strftime"):
                # Converter pandas.Timestamp ou datetime para ISO string
                x_values = [val.strftime("%Y-%m-%d") for val in x_values]
                x_axis_type = "date"
                logger.info(
                    f"[build_plotly_config] Converted {len(x_values)} timestamps to ISO strings"
                )
            else:
                # Fallback: tentar conversão antiga para valores numéricos
                x_values, x_axis_type = self._convert_temporal_values(
                    x_values, dimension_spec, chart_spec.get("filters", {})
                )

        config = {
            "data": [
                {
                    "type": "scatter",
                    "mode": mode,
                    "x": x_values,
                    "y": data[metric_col].tolist(),
                    "line": {
                        "color": color,
                        "width": line_width,
                    },
                    "marker": {
                        "size": 6,
                        "color": color,
                    }
                    if show_markers
                    else {},
                    "hovertemplate": f"<b>{dimension_col}: %{{x}}</b><br>"
                    + f"{metric_col}: %{{y}}<br>"
                    + "<extra></extra>",
                }
            ],
            "layout": {
                "title": chart_spec.get("title", ""),
                "xaxis": {
                    "title": dimension_col,
                    "type": x_axis_type,
                },
                "yaxis": {"title": metric_col},
                "showlegend": False,
                "hovermode": "x unified",
            },
        }

        logger.debug(
            f"Generated Plotly config for line chart: "
            f"data_points={len(data)}, "
            f"dimension={dimension_col}, "
            f"metric={metric_col}, "
            f"mode={mode}, "
            f"x_axis_type={x_axis_type}"
        )

        return config


# ============================================================================
# LANGGRAPH NODE FUNCTION
# ============================================================================


def tool_handle_line(state: dict) -> dict:
    """
    Nó do LangGraph para processar line charts.

    Este nó é invocado pelo router quando chart_type == "line".
    Executa o pipeline completo:
    1. Instancia ToolHandlerLine
    2. Executa query (validate → build SQL → execute DuckDB)
    3. Gera config Plotly com linha temporal
    4. Atualiza state com resultados

    Args:
        state: AnalyticsState contendo:
            - chart_spec: ChartOutput do graphic_classifier
            - schema: {column_name: data_type}

    Returns:
        dict: State atualizado com:
            - result_dataframe: DataFrame com resultado da query
            - plotly_config: Configuração Plotly com line chart
            - execution_success: True
            - sql_query: SQL executado
            - engine_used: "DuckDB"

    Raises:
        ValueError: Se validação do chart_spec falhar
        AnalyticsExecutionError: Se execução da query falhar

    Example:
        >>> state = {
        ...     "chart_spec": {
        ...         "chart_type": "line",
        ...         "dimensions": [{"name": "date"}],
        ...         "metrics": [{"name": "sales", "aggregation": "sum"}],
        ...         "data_source": "data.parquet"
        ...     },
        ...     "schema": {"sales": "DOUBLE", "date": "DATE"}
        ... }
        >>> updated_state = tool_handle_line(state)
        >>> updated_state["execution_success"]
        True
    """
    chart_spec = state["chart_spec"]
    schema = state.get("schema", {})
    data_source_path = chart_spec["data_source"]

    logger.info(f"Processing line chart: {chart_spec.get('title', 'untitled')}")

    # Instanciar handler
    handler = ToolHandlerLine(data_source_path=data_source_path, schema=schema)

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
        f"Line chart processed successfully: "
        f"data_points={len(result_df)}, "
        f"dimension={chart_spec['dimensions'][0]['name']}"
    )

    return state
