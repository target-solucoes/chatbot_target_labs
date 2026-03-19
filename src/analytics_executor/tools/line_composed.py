"""
Tool Handler for Line Composed Charts (Temporal Series).

FASE 2: This handler now supports BOTH single and multiple series.
It is the SEMANTIC TYPE for all temporal variation visualization.

Visual variant (single_line vs multi_line) is determined by RenderSelector
based on the number of categories, not by the handler itself.

This module implements the specific logic for line composed charts with temporal
series, including:
- Validation for temporal dimensions (first dimension must be temporal)
- SQL generation with 1+ dimensions GROUP BY
- Plotly configuration creating single or multiple traces
"""

from typing import Dict, Any, List, Set
import pandas as pd
import logging

from .base import BaseToolHandler

logger = logging.getLogger(__name__)


class ToolHandlerLineComposed(BaseToolHandler):
    """
    Tool handler para line composed charts (séries temporais - single ou múltiplas).

    FASE 2 UPDATE: Now accepts 1+ dimensions instead of requiring exactly 2.

    Requirements:
    - 1+ dimensions (primeira DEVE ser temporal)
    - Sort ASC na dimension temporal (ordem cronológica)
    - Mínimo 1 metric

    SQL Pattern (1 dimension):
        SELECT temporal_dim, AGG(metric) as metric_alias
        FROM dataset
        WHERE filters
        GROUP BY temporal_dim
        ORDER BY temporal_dim ASC

    SQL Pattern (2+ dimensions):
        SELECT temporal_dim, category_dim, AGG(metric) as metric_alias
        FROM dataset
        WHERE filters
        GROUP BY temporal_dim, category_dim
        ORDER BY temporal_dim ASC, category_dim ASC

    Plotly Config:
        - type: "scatter"
        - mode: "lines+markers"
        - Single trace (1 dimension) OR Multiple traces (2+ dimensions)
        - x: temporal dimension
        - y: metric values
        - showlegend: True if multiple traces
    """

    # Keywords para detecção de dimensões temporais
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
        "hora",
        "hour",
        "minuto",
        "minute",
        "segundo",
        "second",
        "tempo",
        "time",
        "periodo",
        "period",
    }

    def validate_chart_spec(self, chart_spec: Dict[str, Any]) -> None:
        """
        Valida requirements específicos de line composed.

        Validações:
        - Exatamente 2 dimensions (temporal + categoria)
        - Primeira dimension deve ser temporal
        - Mínimo 1 metric
        - Sort deve ser ASC (ordem cronológica) se especificado

        Args:
            chart_spec: ChartOutput validado do graphic_classifier

        Raises:
            ValueError: Se não atende requirements de line composed
        """
        # Validação base (campos obrigatórios e colunas existem)
        super().validate_chart_spec(chart_spec)

        # Validações específicas
        dimensions = chart_spec.get("dimensions", [])

        # FASE 2: Deve ter 1+ dimensions (no mínimo temporal)
        if len(dimensions) < 1:
            raise ValueError(
                f"Line composed requires at least 1 dimension (temporal), "
                f"got {len(dimensions)}. First dimension should be temporal (date, month, etc.)."
            )

        # Primeira dimension deve ser temporal (UNLESS dataset has no temporal columns)
        first_dim = dimensions[0]
        if not self._is_temporal_dimension(first_dim):
            # Check if dataset has ANY temporal columns configured
            try:
                from src.shared_lib.core.config import get_temporal_columns

                has_temporal_dataset = bool(get_temporal_columns())
            except Exception:
                has_temporal_dataset = False

            if has_temporal_dataset:
                raise ValueError(
                    f"First dimension must be temporal for line composed charts. "
                    f"Got: '{first_dim['name']}'. Temporal dimensions should contain keywords "
                    f"like 'date', 'month', 'year', etc., or have temporal_granularity field."
                )
            else:
                logger.info(
                    f"[ToolHandlerLineComposed] Dataset has no temporal columns. "
                    f"Allowing non-temporal first dimension: '{first_dim['name']}' "
                    f"as ordered X-axis."
                )

        # Log render variant information
        num_dims = len(dimensions)
        if num_dims == 1:
            logger.info(
                f"[ToolHandlerLineComposed] Single dimension detected. "
                f"This will render as single_line variant."
            )
        else:
            logger.info(
                f"[ToolHandlerLineComposed] {num_dims} dimensions detected. "
                f"This will render as multi_line variant with {num_dims - 1} series."
            )

        # Sort deve ser ASC (ordem cronológica) - warning se DESC
        sort = chart_spec.get("sort", {})
        if sort.get("order", "").lower() == "desc":
            logger.warning(
                "Line composed chart with DESC sort may not show chronological order. "
                "Consider using ASC sort for temporal dimension."
            )

        logger.debug(
            f"Line composed validation passed: "
            f"temporal_dim={first_dim['name']}, "
            f"category_dim={dimensions[1]['name'] if len(dimensions) > 1 else 'None (single_line)'}, "
            f"metrics={[m['name'] for m in chart_spec.get('metrics', [])]}"
        )

    def execute(self, chart_spec: Dict[str, Any]) -> pd.DataFrame:
        """
        Execute pipeline with intelligent handling for single_line vs multi_line variants.

        LAYER 6 COMPLIANCE:
        - 1 dimension (temporal only) → single_line: Simple temporal aggregation
        - 2+ dimensions → multi_line: Top N filtering with variation calculation

        Args:
            chart_spec: ChartOutput specification

        Returns:
            pd.DataFrame: Data for visualization
        """
        dimensions = chart_spec.get("dimensions", [])

        # SINGLE_LINE VARIANT: Only 1 dimension (temporal)
        if len(dimensions) == 1:
            logger.info(
                f"[execute] LAYER 6: Single dimension detected → single_line variant. "
                f"Simple temporal aggregation (no Top N filtering needed)."
            )
            # Use parent execute for simple temporal aggregation
            return super().execute(chart_spec)

        # MULTI_LINE VARIANT: 2+ dimensions (temporal + categorical series)
        # Call parent execute to get raw SQL results
        result_df = super().execute(chart_spec)

        # Extract column names
        temporal_col = chart_spec["dimensions"][0].get(
            "alias", chart_spec["dimensions"][0]["name"]
        )
        category_col = chart_spec["dimensions"][1].get(
            "alias", chart_spec["dimensions"][1]["name"]
        )
        metric_col = chart_spec["metrics"][0].get(
            "alias", chart_spec["metrics"][0]["name"]
        )

        # Apply Top N filtering (this also adds variation column)
        filtered_df = self._calculate_variation_and_filter_top_n(
            result_df, chart_spec, temporal_col, category_col, metric_col
        )

        # Log the filtering result
        original_rows = len(result_df)
        filtered_rows = len(filtered_df)
        original_categories = result_df[category_col].nunique()
        filtered_categories = filtered_df[category_col].nunique()

        logger.info(
            f"[execute] PHASE 4 FIX: Filtered data before returning to formatter:\n"
            f"  Rows: {original_rows} → {filtered_rows}\n"
            f"  Categories: {original_categories} → {filtered_categories}\n"
            f"  Columns: {list(filtered_df.columns)}"
        )

        # CRITICAL: Return filtered data (not original)
        return filtered_df

    def _is_temporal_dimension(self, dimension: Dict[str, Any]) -> bool:
        """
        Verifica se uma dimension é temporal.

        Detecta através de:
        1. Keywords no nome da coluna (date, month, year, etc.)
        2. Campo temporal_granularity presente

        Args:
            dimension: Dicionário com info da dimension

        Returns:
            bool: True se dimension é temporal
        """
        name_lower = dimension.get("name", "").lower()

        # Check 1: Keyword matching
        if any(keyword in name_lower for keyword in self.TEMPORAL_KEYWORDS):
            logger.debug(
                f"Dimension '{dimension['name']}' identified as temporal by keyword matching"
            )
            return True

        # Check 2: temporal_granularity field
        if dimension.get("temporal_granularity"):
            logger.debug(
                f"Dimension '{dimension['name']}' identified as temporal by "
                f"temporal_granularity field: {dimension['temporal_granularity']}"
            )
            return True

        logger.debug(f"Dimension '{dimension['name']}' is NOT temporal")
        return False

    def _get_base_temporal_column(self) -> str:
        """Resolve base temporal column name from alias.yaml configuration."""
        try:
            from src.shared_lib.core.config import get_temporal_columns

            temporal_cols = get_temporal_columns()
            return temporal_cols[0] if temporal_cols else "Data"
        except Exception:
            return "Data"

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
            # Sem contexto de ano, usar valores numéricos como linear
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

        # Procurar por filtro de Data
        date_filter = filters.get("Data")

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

                # Extrair ano (primeiros 4 dígitos)
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
        Constrói SQL para line composed chart.

        LAYER 6 COMPLIANCE:
        - 1 dimension (single_line): Simple temporal aggregation
        - 2+ dimensions (multi_line): Temporal + category with optional Top N

        SQL Pattern (single_line):
            SELECT DATE_TRUNC('month', Data) AS temporal_alias, AGG(metric) as metric_alias
            FROM dataset
            WHERE filters
            GROUP BY DATE_TRUNC('month', Data)
            ORDER BY DATE_TRUNC('month', Data) ASC

        SQL Pattern (multi_line):
            SELECT DATE_TRUNC('month', Data) AS temporal_alias, category_dim, AGG(metric) as metric_alias
            FROM dataset
            WHERE filters
            GROUP BY DATE_TRUNC('month', Data), category_dim
            ORDER BY DATE_TRUNC('month', Data) ASC, category_dim ASC

        Args:
            chart_spec: ChartOutput validado

        Returns:
            str: Query SQL completa e executável
        """
        dimensions = chart_spec.get("dimensions", [])

        # SINGLE_LINE VARIANT: Only temporal dimension
        if len(dimensions) == 1:
            return self._build_sql_single_line(chart_spec)

        # MULTI_LINE VARIANT: Temporal + categorical
        return self._build_sql_multi_line(chart_spec)

    def _build_sql_single_line(self, chart_spec: Dict[str, Any]) -> str:
        """
        Build SQL for single_line variant (1 dimension - temporal only).

        Generates simple temporal aggregation without category breakdown.

        Args:
            chart_spec: ChartOutput validado

        Returns:
            str: Query SQL for single_line
        """
        # Obter especificação da dimensão temporal
        temporal_dim_spec = chart_spec["dimensions"][0]
        temporal_dim = temporal_dim_spec["name"]
        temporal_alias = temporal_dim_spec.get("alias", temporal_dim)
        granularity = temporal_dim_spec.get("temporal_granularity", "").lower()
        base_col = self._get_base_temporal_column()

        # Construir SELECT clause
        select_parts = []

        # Dimension temporal: usar DATE_TRUNC se for Mes, Ano, Trimestre, etc.
        if self._should_use_date_trunc(temporal_dim, granularity):
            trunc_granularity = self._map_granularity_to_date_trunc(
                temporal_dim, granularity
            )
            temporal_escaped_alias = self.sql_builder.escape_identifier(temporal_alias)
            select_parts.append(
                f"DATE_TRUNC('{trunc_granularity}', {base_col}) AS {temporal_escaped_alias}"
            )
            logger.info(
                f"[build_sql] SINGLE_LINE: Using DATE_TRUNC('{trunc_granularity}', Data) "
                f"for temporal dimension"
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
            group_by_clause = f"GROUP BY DATE_TRUNC('{trunc_granularity}', {base_col})"
            order_by_clause = (
                f"ORDER BY DATE_TRUNC('{trunc_granularity}', {base_col}) ASC"
            )
        else:
            temporal_escaped = self.sql_builder.escape_identifier(temporal_dim)
            group_by_clause = f"GROUP BY {temporal_escaped}"
            order_by_clause = f"ORDER BY {temporal_escaped} ASC"

        # Construir query final
        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        sql_parts.append(group_by_clause)
        sql_parts.append(order_by_clause)

        sql = "\n".join(sql_parts)
        logger.info(
            f"[build_sql] SINGLE_LINE: Generated simple temporal aggregation SQL"
        )
        logger.debug(f"Generated SQL for single_line:\n{sql}")

        return sql

    def _build_sql_multi_line(self, chart_spec: Dict[str, Any]) -> str:
        """
        Build SQL for multi_line variant (2+ dimensions - temporal + categorical).

        Generates temporal aggregation with category breakdown and optional Top N.

        Args:
            chart_spec: ChartOutput validado

        Returns:
            str: Query SQL for multi_line
        """
        # Obter especificações das dimensões
        temporal_dim_spec = chart_spec["dimensions"][0]
        category_dim_spec = chart_spec["dimensions"][1]

        temporal_dim = temporal_dim_spec["name"]
        category_dim = category_dim_spec["name"]
        temporal_alias = temporal_dim_spec.get("alias", temporal_dim)
        category_alias = category_dim_spec.get("alias", category_dim)

        # Detectar granularidade temporal
        granularity = temporal_dim_spec.get("temporal_granularity", "").lower()
        base_col = self._get_base_temporal_column()

        # Construir SELECT clause customizada para usar DATE_TRUNC
        select_parts = []

        # Dimension temporal: usar DATE_TRUNC se for Mes, Ano, Trimestre, etc.
        if self._should_use_date_trunc(temporal_dim, granularity):
            # Mapear granularidade para DATE_TRUNC
            trunc_granularity = self._map_granularity_to_date_trunc(
                temporal_dim, granularity
            )
            temporal_escaped_alias = self.sql_builder.escape_identifier(temporal_alias)
            select_parts.append(
                f"DATE_TRUNC('{trunc_granularity}', {base_col}) AS {temporal_escaped_alias}"
            )
            logger.info(
                f"[build_sql] Using DATE_TRUNC('{trunc_granularity}', {base_col}) for temporal dimension"
            )
        else:
            # Usar coluna original (ex: Data já é datetime)
            temporal_escaped = self.sql_builder.escape_identifier(temporal_dim)
            temporal_escaped_alias = self.sql_builder.escape_identifier(temporal_alias)
            select_parts.append(f"{temporal_escaped} AS {temporal_escaped_alias}")

        # Dimension categórica
        category_escaped = self.sql_builder.escape_identifier(category_dim)
        category_escaped_alias = self.sql_builder.escape_identifier(category_alias)
        select_parts.append(f"{category_escaped} AS {category_escaped_alias}")

        # Metrics (com agregação)
        for metric in chart_spec.get("metrics", []):
            col = self.sql_builder.escape_identifier(metric["name"])
            column_name = metric["name"]

            # Obtém agregação especificada ou usa seleção inteligente
            user_specified_agg = metric.get("aggregation", "sum")

            # Seleciona agregação apropriada baseada no tipo da coluna
            selected_agg = self.aggregation_selector.select_aggregation(
                column_name=column_name,
                schema=self.schema,
                user_specified=user_specified_agg,
            )

            # Valida e corrige se necessário
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

            # Monta cláusula SQL
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
        group_by_parts = []
        if self._should_use_date_trunc(temporal_dim, granularity):
            trunc_granularity = self._map_granularity_to_date_trunc(
                temporal_dim, granularity
            )
            group_by_parts.append(f"DATE_TRUNC('{trunc_granularity}', {base_col})")
        else:
            temporal_escaped = self.sql_builder.escape_identifier(temporal_dim)
            group_by_parts.append(temporal_escaped)

        category_escaped = self.sql_builder.escape_identifier(category_dim)
        group_by_parts.append(category_escaped)
        group_by_clause = "GROUP BY " + ", ".join(group_by_parts)

        # ORDER BY: usar mesma expressão DATE_TRUNC se aplicável
        order_by_parts = []
        if self._should_use_date_trunc(temporal_dim, granularity):
            trunc_granularity = self._map_granularity_to_date_trunc(
                temporal_dim, granularity
            )
            order_by_parts.append(f"DATE_TRUNC('{trunc_granularity}', {base_col}) ASC")
        else:
            temporal_escaped = self.sql_builder.escape_identifier(temporal_dim)
            order_by_parts.append(f"{temporal_escaped} ASC")

        category_escaped = self.sql_builder.escape_identifier(category_dim)
        order_by_parts.append(f"{category_escaped} ASC")
        order_by_clause = "ORDER BY " + ", ".join(order_by_parts)

        # CRITICAL FIX: For temporal charts with top_n, use CTE to select top N entities
        # then fetch ALL time periods for those entities (prevents data truncation)
        top_n = chart_spec.get("top_n")

        if top_n:
            # Build CTE to select top N entities based on total metric value OR variation
            metric = chart_spec.get("metrics", [])[0]  # Primary metric
            metric_col = self.sql_builder.escape_identifier(metric["name"])
            metric_agg = metric.get("aggregation", "sum").upper()

            # Check if we should sort by variation (delta between periods)
            sort_config = chart_spec.get("sort", {})
            sort_by = sort_config.get("by", "value")
            sort_order = sort_config.get("order", "desc").upper()

            # CRITICAL: For variation sorting, calculate delta in CTE
            # This ensures Top N selection is based on variation, not total volume
            if sort_by == "variation":
                # Extract temporal filter range to identify first and last periods
                # We'll use aggregation with CASE statements to calculate delta
                logger.info(
                    f"[build_sql] Using VARIATION-based Top N selection "
                    f"(sort_by={sort_by}, order={sort_order})"
                )

                # Build CTE with variation calculation
                # Variation = SUM(last_period) - SUM(first_period)
                # We use MIN/MAX of temporal dimension to identify periods
                temporal_escaped = self.sql_builder.escape_identifier(temporal_dim)

                if self._should_use_date_trunc(temporal_dim, granularity):
                    trunc_granularity = self._map_granularity_to_date_trunc(
                        temporal_dim, granularity
                    )
                    temporal_expr = f"DATE_TRUNC('{trunc_granularity}', {base_col})"
                else:
                    temporal_expr = temporal_escaped

                cte_where = where_clause if where_clause else ""
                cte_sql = f"""WITH TopEntities AS (
    SELECT {category_escaped}
    FROM dataset
    {cte_where}
    GROUP BY {category_escaped}
    ORDER BY (
        SUM(CASE WHEN {temporal_expr} = (SELECT MAX({temporal_expr}) FROM dataset {cte_where}) 
            THEN {metric_col} ELSE 0 END) -
        SUM(CASE WHEN {temporal_expr} = (SELECT MIN({temporal_expr}) FROM dataset {cte_where}) 
            THEN {metric_col} ELSE 0 END)
    ) {sort_order}
    LIMIT {top_n}
)"""
                logger.info(
                    f"[build_sql] Generated CTE with DELTA calculation "
                    f"(last_period - first_period) ORDER BY {sort_order}"
                )
            else:
                # Standard CTE: sort by total aggregated value
                cte_where = where_clause if where_clause else ""
                cte_sql = f"""WITH TopEntities AS (
    SELECT {category_escaped}
    FROM dataset
    {cte_where}
    GROUP BY {category_escaped}
    ORDER BY {metric_agg}({metric_col}) DESC
    LIMIT {top_n}
)"""
                logger.info(
                    f"[build_sql] Generated CTE with VALUE-based Top N selection "
                    f"(ORDER BY {metric_agg}({metric_col}) DESC)"
                )

            # CRITICAL FIX: Create CTE with all unique temporal periods
            # This ensures that EVERY category in TopEntities has data for ALL periods,
            # even if some periods have zero sales (no records in original table)
            if self._should_use_date_trunc(temporal_dim, granularity):
                trunc_granularity = self._map_granularity_to_date_trunc(
                    temporal_dim, granularity
                )
                temporal_expr = f"DATE_TRUNC('{trunc_granularity}', {base_col})"
            else:
                temporal_expr = self.sql_builder.escape_identifier(temporal_dim)

            # CTE para todas as datas unicas no periodo filtrado
            temporal_cte = f"""AllPeriods AS (
    SELECT DISTINCT {temporal_expr} AS period
    FROM dataset
    {where_clause if where_clause else ""}
)"""

            # Combinar CTEs
            combined_cte = f"""{cte_sql},
{temporal_cte}"""

            # CRITICAL: Main query uses CROSS JOIN to ensure all (category, period) combinations exist
            # Then LEFT JOIN with actual data, using COALESCE to treat NULLs as 0
            main_from = f"""FROM AllPeriods ap
CROSS JOIN TopEntities te
LEFT JOIN dataset t
    ON t.{category_escaped} = te.{category_escaped}
    AND {temporal_expr if not self._should_use_date_trunc(temporal_dim, granularity) else f"DATE_TRUNC('{trunc_granularity}', t.{base_col})"} = ap.period"""

            # Add WHERE clause to LEFT JOIN if exists
            if where_clause:
                main_from += f"\n    AND {where_clause.replace('WHERE ', '')}"

            # Update SELECT to use CROSS JOIN structure
            select_parts_aliased = []
            temporal_escaped_alias = self.sql_builder.escape_identifier(temporal_alias)
            select_parts_aliased.append(f"ap.period AS {temporal_escaped_alias}")
            select_parts_aliased.append(
                f"te.{category_escaped} AS {category_escaped_alias}"
            )

            # CRITICAL: Use COALESCE to treat NULL aggregations as 0
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
                validated_agg, _ = self.aggregation_validator.validate_and_correct(
                    column_name=column_name,
                    aggregation=selected_agg,
                    column_type=column_type,
                )
                agg = validated_agg.upper()
                alias = self.sql_builder.escape_identifier(
                    metric.get("alias", metric["name"])
                )
                # CRITICAL: COALESCE ensures NULL becomes 0 when no data exists for that period
                select_parts_aliased.append(f"COALESCE({agg}(t.{col}), 0) AS {alias}")

            select_clause_cte = "SELECT " + ", ".join(select_parts_aliased)

            # GROUP BY uses CROSS JOIN columns
            group_by_clause_cte = "GROUP BY ap.period, te.{0}".format(category_escaped)

            # Update ORDER BY to use numeric positions (cleaner for CTE)
            order_by_clause_cte = "ORDER BY 1 ASC, 2 ASC"

            # Assemble final CTE query (NO LIMIT - we want all time periods)
            # CRITICAL: Use combined_cte (with AllPeriods) instead of just cte_sql
            sql_parts_cte = [combined_cte, select_clause_cte, main_from]
            # No WHERE clause needed in main query - filters already applied in CTEs
            sql_parts_cte.append(group_by_clause_cte)
            sql_parts_cte.append(order_by_clause_cte)

            sql = "\n".join(sql_parts_cte)
            logger.info(
                f"[build_sql] Generated CTE-based SQL for line_composed with top_n={top_n} "
                f"(CROSS JOIN ensures all category×period combinations, COALESCE treats missing data as 0)"
            )
            logger.debug(f"Generated CTE SQL for line_composed:\n{sql}")

            return sql

        # Original path (no top_n): simple query without CTE
        sql_parts = [select_clause, from_clause]

        if where_clause:
            sql_parts.append(where_clause)
        sql_parts.append(group_by_clause)
        sql_parts.append(order_by_clause)

        sql = "\n".join(sql_parts)
        logger.debug(f"Generated SQL for line_composed:\n{sql}")

        return sql

    def _should_use_date_trunc(self, dim_name: str, granularity: str) -> bool:
        """
        Determina se deve usar DATE_TRUNC baseado no nome da dimensão e granularidade.

        Args:
            dim_name: Nome da dimensão (ex: "Mes", "Ano", "Trimestre")
            granularity: Granularidade temporal (ex: "month", "year", "quarter")

        Returns:
            bool: True se deve usar DATE_TRUNC
        """
        # Se for coluna auxiliar (Mes, Ano, Trimestre, Dia), usar DATE_TRUNC
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

        # Se granularidade explícita, usar DATE_TRUNC
        if granularity in {"month", "year", "quarter", "day"}:
            return True

        return False

    def _map_granularity_to_date_trunc(self, dim_name: str, granularity: str) -> str:
        """
        Mapeia nome de dimensão ou granularidade para argumento DATE_TRUNC.

        Args:
            dim_name: Nome da dimensão (ex: "Mes", "Ano")
            granularity: Granularidade (ex: "month", "year")

        Returns:
            str: Argumento para DATE_TRUNC ('month', 'year', 'quarter', 'day')
        """
        # Priorizar granularidade explícita
        if granularity:
            return granularity.lower()

        # Mapear baseado no nome da coluna
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

        return mapping.get(dim_name.lower(), "month")  # Default: month

    def _calculate_variation_and_filter_top_n(
        self,
        data: pd.DataFrame,
        chart_spec: Dict[str, Any],
        temporal_col: str,
        category_col: str,
        metric_col: str,
    ) -> pd.DataFrame:
        """
        Calcula variação temporal e filtra Top N categorias quando aplicável.

        REFACTORED FROM bar_vertical_composed:
        Esta lógica foi herdada do gráfico bar_vertical_composed removido.
        Permite ordenar categorias por crescimento/variação e aplicar Top N
        para evitar "gráficos espaguete" com muitas linhas.

        Args:
            data: DataFrame original com dados agregados
            chart_spec: Especificação do gráfico
            temporal_col: Nome da coluna temporal (alias)
            category_col: Nome da coluna de categoria (alias)
            metric_col: Nome da coluna de métrica (alias)

        Returns:
            DataFrame filtrado (somente Top N categorias se aplicável)

        Example:
            Input (5 produtos, 2 meses):
            | Mês  | Produto | Vendas |
            |------|---------|--------|
            | Maio | A       | 100    |
            | Jun  | A       | 200    |  <- Variação +100
            | Maio | B       | 50     |
            | Jun  | B       | 60     |   <- Variação +10

            Output (top_n=1):
            | Mês  | Produto | Vendas |
            |------|---------|--------|
            | Maio | A       | 100    |
            | Jun  | A       | 200    |
        """
        intent = chart_spec.get("intent", "")
        sort_config = chart_spec.get("sort", {})
        top_n = chart_spec.get("top_n")

        # Verificar se é um intent de comparação temporal com variação
        requires_variation = (
            intent == "temporal_comparison_analysis"
            and sort_config.get("by") == "variation"
        )

        # FASE 3: Sempre calcular variação se requisitado, independente de Top N
        if not requires_variation and not top_n:
            # Sem filtro necessário e sem cálculo de variação
            return data

        # Aplicar Top N padrão para evitar gráfico espaguete
        if not top_n and requires_variation:
            top_n = 5
            logger.info(
                "[_calculate_variation_and_filter_top_n] Applying default top_n=5 "
                "to prevent spaghetti chart in temporal comparison"
            )

        # Pivotar dados para calcular variação
        logger.info(
            f"[_calculate_variation_and_filter_top_n] Calculating variation for "
            f"intent={intent}, sort={sort_config}"
        )

        try:
            pivot_df = data.pivot_table(
                index=category_col,
                columns=temporal_col,
                values=metric_col,
                aggfunc="sum",
                fill_value=0,
            ).reset_index()

            # Obter lista de períodos (colunas do pivot)
            period_columns = [col for col in pivot_df.columns if col != category_col]

            # FASE 3: Guard Clause - Verificar número mínimo de períodos
            if len(period_columns) < 2:
                logger.warning(
                    f"[_calculate_variation_and_filter_top_n] Need at least 2 periods "
                    f"for variation calculation, got {len(period_columns)}: {period_columns}. "
                    f"Returning original data without variation column."
                )
                return data

            # FASE 3: Ordenar períodos cronologicamente
            # Suporta datetime (Fase 2), strings de meses, e outros tipos
            temporal_dim_spec = chart_spec["dimensions"][0]
            temporal_granularity = temporal_dim_spec.get(
                "temporal_granularity", ""
            ).lower()

            # Verificar tipo de dados das colunas de período
            sample_period = period_columns[0] if period_columns else None

            if sample_period is not None and isinstance(
                sample_period, (pd.Timestamp, pd.DatetimeIndex)
            ):
                # FASE 2: Se for datetime/timestamp, ordenar diretamente
                period_columns = sorted(period_columns)
                logger.info(
                    f"[_calculate_variation_and_filter_top_n] Sorted datetime periods: "
                    f"{period_columns[0]} to {period_columns[-1]}"
                )
            elif temporal_granularity == "month":
                # Ordenar meses cronologicamente (compatibilidade retroativa)
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
                    key=lambda x: month_order.get(x, 99),
                )
                logger.info(
                    f"[_calculate_variation_and_filter_top_n] Sorted months: {period_columns}"
                )
            else:
                # Ordenação genérica (números, strings, etc)
                period_columns = sorted(period_columns)

            # Calcular variação entre primeiro e último período
            first_period = period_columns[0]
            last_period = period_columns[-1]

            logger.info(
                f"[_calculate_variation_and_filter_top_n] Calculating Delta: "
                f"{last_period} - {first_period}"
            )

            # Variação absoluta (Delta)
            pivot_df["variation"] = pivot_df[last_period] - pivot_df[first_period]

            # CRITICAL FIX: Aplicar filtro de polaridade ANTES da ordenação
            # Quando polarity == "negative" (queda, redução), filtrar apenas variation < 0
            polarity = chart_spec.get("polarity")
            if polarity == "negative":
                original_count = len(pivot_df)
                pivot_df = pivot_df[pivot_df["variation"] < 0].copy()
                filtered_count = len(pivot_df)
                logger.info(
                    f"[_calculate_variation_and_filter_top_n] POLARITY FILTER APPLIED: "
                    f"polarity='negative' → filtered to variation < 0 "
                    f"({original_count} → {filtered_count} categories)"
                )

                # Se não houver dados negativos, retornar DataFrame vazio
                # (isso é semanticamente correto - não há quedas!)
                if filtered_count == 0:
                    logger.warning(
                        f"[_calculate_variation_and_filter_top_n] No categories with variation < 0 found. "
                        f"Query asked for 'queda' but dataset has no negative variations. "
                        f"Returning empty DataFrame (semantically correct)."
                    )
                    return pd.DataFrame(columns=data.columns)

            # Ordenar por variação
            sort_order = sort_config.get("order", "desc")
            ascending = sort_order == "asc"

            pivot_df = pivot_df.sort_values(
                by="variation",
                ascending=ascending,
                na_position="last",
            ).reset_index(drop=True)

            logger.info(
                f"[_calculate_variation_and_filter_top_n] Sorted by variation ({sort_order})"
            )

            # Aplicar top_n
            if top_n and top_n > 0:
                pivot_df = pivot_df.head(top_n)
                logger.info(
                    f"[_calculate_variation_and_filter_top_n] Limited to top {top_n} categories"
                )

            # Filtrar dados originais para manter apenas as categorias selecionadas
            # (todas as categorias se não houver Top N, ou apenas Top N se houver)
            selected_categories = pivot_df[category_col].tolist()
            filtered_data = data[data[category_col].isin(selected_categories)].copy()

            # FASE 3: Adicionar coluna 'variation' ao DataFrame de saída
            # Criar mapeamento categoria -> variação
            variation_map = pivot_df.set_index(category_col)["variation"].to_dict()

            # Adicionar coluna 'variation' ao DataFrame filtrado
            filtered_data["variation"] = filtered_data[category_col].map(variation_map)

            if top_n:
                logger.info(
                    f"[_calculate_variation_and_filter_top_n] ✅ Filtered from "
                    f"{data[category_col].nunique()} to {len(selected_categories)} categories"
                )
            logger.info(
                f"[_calculate_variation_and_filter_top_n] ✅ Added 'variation' column to output DataFrame"
            )

            return filtered_data

        except Exception as e:
            logger.error(
                f"[_calculate_variation_and_filter_top_n] Error: {str(e)}",
                exc_info=True,
            )
            # Fallback: retornar dados originais
            return data

    def build_plotly_config(
        self, chart_spec: Dict[str, Any], data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Constrói configuração Plotly para line composed.

        LAYER 6 COMPLIANCE:
        - 1 dimension (single_line): Single trace representing the metric over time
        - 2+ dimensions (multi_line): Multiple traces (one per category)

        Args:
            chart_spec: ChartOutput original
            data: DataFrame com resultado da query

        Returns:
            dict: Configuração Plotly pronta para renderização
        """
        dimensions = chart_spec.get("dimensions", [])

        # SINGLE_LINE VARIANT: Only temporal dimension - single trace
        if len(dimensions) == 1:
            return self._build_plotly_config_single_line(chart_spec, data)

        # MULTI_LINE VARIANT: Temporal + categorical - multiple traces
        return self._build_plotly_config_multi_line(chart_spec, data)

    def _build_plotly_config_single_line(
        self, chart_spec: Dict[str, Any], data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Build Plotly config for single_line variant (1 dimension - temporal only).

        Generates a single continuous line representing the metric over time.

        Args:
            chart_spec: ChartOutput original
            data: DataFrame com resultado da query

        Returns:
            dict: Configuração Plotly com uma única trace
        """
        temporal_col = chart_spec["dimensions"][0].get(
            "alias", chart_spec["dimensions"][0]["name"]
        )
        metric_col = chart_spec["metrics"][0].get(
            "alias", chart_spec["metrics"][0]["name"]
        )
        metric_name = chart_spec["metrics"][0].get(
            "alias", chart_spec["metrics"][0]["name"]
        )

        logger.info(
            f"[build_plotly_config] SINGLE_LINE: Building single trace for "
            f"{metric_name} over {temporal_col} ({len(data)} data points)"
        )

        # Processar valores do eixo X para temporal
        x_values = data[temporal_col].tolist()
        dimension_spec = chart_spec["dimensions"][0]
        is_temporal = self._is_temporal_dimension(dimension_spec)

        x_axis_type = "linear"

        # Converter timestamps para ISO strings se necessário
        if is_temporal:
            if x_values and hasattr(x_values[0], "strftime"):
                x_values = [val.strftime("%Y-%m-%d") for val in x_values]
                x_axis_type = "date"
                logger.info(
                    f"[build_plotly_config] SINGLE_LINE: Converted {len(x_values)} timestamps to ISO strings"
                )
            else:
                _, x_axis_type = self._convert_temporal_values(
                    x_values, dimension_spec, chart_spec.get("filters", {})
                )

        # Obter valores Y (métrica)
        y_values = data[metric_col].tolist()

        # Criar única trace
        trace = {
            "type": "scatter",
            "mode": "lines+markers",
            "name": metric_name,
            "x": x_values,
            "y": y_values,
            "line": {"color": "#636EFA", "width": 2},
            "marker": {"size": 6},
            "showlegend": False,  # No legend needed for single series
        }

        config = {
            "data": [trace],
            "layout": {
                "xaxis": {"type": x_axis_type, "title": temporal_col},
                "yaxis": {"title": metric_name},
                "showlegend": False,
            },
        }

        logger.info(
            f"[build_plotly_config] SINGLE_LINE: Built Plotly config with 1 trace, "
            f"{len(x_values)} data points"
        )

        return config

    def _build_plotly_config_multi_line(
        self, chart_spec: Dict[str, Any], data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Build Plotly config for multi_line variant (2+ dimensions - temporal + categorical).

        Generates multiple traces (one per category).

        Args:
            chart_spec: ChartOutput original
            data: DataFrame com resultado da query

        Returns:
            dict: Configuração Plotly com múltiplas traces
        """
        temporal_col = chart_spec["dimensions"][0].get(
            "alias", chart_spec["dimensions"][0]["name"]
        )
        category_col = chart_spec["dimensions"][1].get(
            "alias", chart_spec["dimensions"][1]["name"]
        )
        metric_col = chart_spec["metrics"][0].get(
            "alias", chart_spec["metrics"][0]["name"]
        )

        # FASE 4 FIX: Data is already filtered by execute(), no need to filter again
        # Just use the data as-is (it already has variation column and Top N applied)
        logger.info(
            f"[build_plotly_config] Using pre-filtered data: "
            f"{len(data)} rows, {data[category_col].nunique()} categories"
        )

        # Get unique categories from pre-filtered data
        unique_categories = sorted(data[category_col].unique())

        # Define colors palette (Plotly default colors)
        DEFAULT_COLORS = [
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
        ]
        colors = [
            DEFAULT_COLORS[i % len(DEFAULT_COLORS)]
            for i in range(len(unique_categories))
        ]
        show_markers = True  # Always show markers for better visibility

        # Processar valores do eixo X para temporal
        temporal_values_all = data[temporal_col].unique().tolist()
        dimension_spec = chart_spec["dimensions"][0]
        is_temporal = self._is_temporal_dimension(dimension_spec)

        x_axis_type = "linear"

        # REFACTORED (Fase 2): Converter timestamps para ISO strings
        if is_temporal:
            # Verificar se valores são timestamps/datetime
            if temporal_values_all and hasattr(temporal_values_all[0], "strftime"):
                # Converter pandas.Timestamp ou datetime para ISO string
                temporal_values_all = [
                    val.strftime("%Y-%m-%d") for val in temporal_values_all
                ]
                x_axis_type = "date"
                logger.info(
                    f"[build_plotly_config] Converted {len(temporal_values_all)} timestamps to ISO strings"
                )
            else:
                # Fallback: tentar conversão antiga para valores numéricos
                _, x_axis_type = self._convert_temporal_values(
                    temporal_values_all, dimension_spec, chart_spec.get("filters", {})
                )

        # Criar uma trace por categoria
        traces = []
        for idx, category in enumerate(unique_categories):
            # Filtrar dados desta categoria
            category_data = data[data[category_col] == category]

            # Obter valores X da categoria
            x_values = category_data[temporal_col].tolist()

            # Converter valores temporais se necessário
            if is_temporal:
                # Verificar se valores são timestamps/datetime
                if x_values and hasattr(x_values[0], "strftime"):
                    # Converter pandas.Timestamp ou datetime para ISO string
                    x_values = [val.strftime("%Y-%m-%d") for val in x_values]
                else:
                    # Fallback: conversão antiga
                    x_values, _ = self._convert_temporal_values(
                        x_values, dimension_spec, chart_spec.get("filters", {})
                    )

            # Determinar mode
            mode = "lines+markers" if show_markers else "lines"

            trace = {
                "type": "scatter",
                "mode": mode,
                "name": str(category),
                "x": x_values,
                "y": category_data[metric_col].tolist(),
                "line": {"color": colors[idx], "width": 2},
                "marker": {"size": 6} if show_markers else {},
                "hovertemplate": (
                    f"<b>{category}</b><br>"
                    f"{temporal_col}: %{{x}}<br>"
                    f"{metric_col}: %{{y}}<br>"
                    "<extra></extra>"
                ),
            }
            traces.append(trace)

        config = {
            "data": traces,
            "layout": {
                "title": chart_spec.get("title", ""),
                "xaxis": {
                    "title": temporal_col,
                    "type": x_axis_type,
                },
                "yaxis": {"title": metric_col},
                "showlegend": True,
                "legend": {"orientation": "v", "x": 1.02, "y": 1},
                "hovermode": "x unified",
            },
        }

        logger.debug(
            f"Built Plotly config for line_composed with {len(traces)} series, "
            f"x_axis_type={x_axis_type}"
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


def tool_handle_line_composed(state: dict) -> dict:
    """
    Nó do LangGraph para processar line composed charts.

    REFACTORED: Agora suporta temporal_comparison_analysis com cálculo
    automático de variação e filtro Top N (herdado de bar_vertical_composed).

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
            "[tool_handle_line_composed] Temporal comparison analysis detected:\n"
            f"  - Intent: {intent}\n"
            f"  - Dimensions: {[d.get('alias', d['name']) for d in chart_spec.get('dimensions', [])]}\n"
            f"  - Metrics: {[m.get('alias', m['name']) for m in chart_spec.get('metrics', [])]}\n"
            f"  - Sort: {chart_spec.get('sort', {})}\n"
            f"  - Filters: {chart_spec.get('filters', {})}\n"
            f"  - Top N: {chart_spec.get('top_n', 'default=5')}"
        )

    # Instanciar handler
    handler = ToolHandlerLineComposed(data_source_path=data_source_path, schema=schema)

    # Executar pipeline completo (PHASE 4 FIX: now returns filtered data with Top N)
    result_df = handler.execute(chart_spec)

    # Log resultado após execução SQL e filtro Top N
    logger.info(
        f"[tool_handle_line_composed] Execution complete (with Top N filter): "
        f"{len(result_df)} rows, columns={list(result_df.columns)}"
    )

    # Build plotly config (data is already filtered, no need to filter again)
    # PHASE 4 FIX: Pass the already-filtered data
    plotly_config = handler.build_plotly_config(chart_spec, result_df)

    # Log após plotly config
    logger.info(
        f"[tool_handle_line_composed] Plotly config built: "
        f"{len(plotly_config.get('data', []))} traces created"
    )

    # Atualizar state (PHASE 4 FIX: result_dataframe now contains filtered data with variation column)
    state["result_dataframe"] = result_df
    state["plotly_config"] = plotly_config
    state["execution_success"] = True
    state["sql_query"] = handler.build_sql(chart_spec)
    state["engine_used"] = "DuckDB"

    return state
