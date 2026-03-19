"""
Base Tool Handler for chart-specific SQL generation and execution.

This module provides the abstract base class that all chart type handlers
must extend. It includes:
- SQL building utilities
- DuckDB execution pipeline
- Pre and post-execution validation
- Plotly config building
"""

from abc import ABC, abstractmethod
from typing import Dict, Any
import pandas as pd
import duckdb
import logging

from ..utils.sql_builder import SQLBuilder
from ..utils.aggregation_selector import AggregationSelector
from ..utils.aggregation_validator import AggregationValidator

logger = logging.getLogger(__name__)


def _get_virtual_metrics() -> set:
    """Load virtual metric names from alias.yaml metrics section.

    Virtual metrics (e.g. 'Numero de Clientes') are defined in the
    alias.yaml 'metrics' section and represent computed aggregations
    (COUNT(*)) rather than actual dataset columns. They must be
    exempted from column-existence validation.

    Returns:
        Set of virtual metric names.
    """
    try:
        from src.shared_lib.core.config import load_alias_data

        alias_data = load_alias_data()
        return set(alias_data.get("metrics", {}).keys())
    except Exception:
        return set()


class AnalyticsExecutionError(Exception):
    """
    Exception raised when analytics query execution fails.

    This exception is raised when:
    - SQL query fails to execute in DuckDB
    - Data source cannot be loaded
    - Query returns invalid results
    """

    pass


class BaseToolHandler(ABC):
    """
    Classe base abstrata para todos os tool handlers de chart types.

    Cada chart type deve implementar sua própria subclasse que:
    1. Implementa build_sql() - construção de query específica
    2. Opcionalmente sobrescreve validate_chart_spec() - validações específicas
    3. Opcionalmente sobrescreve build_plotly_config() - config Plotly específica

    O pipeline de execução (método execute()) orquestra:
    validate → build SQL → execute DuckDB → validate result

    Princípios de Design:
    - Cada handler é autocontido e isolado
    - Sem fallbacks - erros são expostos explicitamente
    - Apenas DuckDB - performance otimizada
    - Reutilização via herança de métodos utilitários

    Attributes:
        data_source: Caminho para o arquivo de dados (Parquet/CSV)
        schema: Schema do dataset {column_name: data_type}
        sql_builder: Instância de SQLBuilder para construção segura de SQL

    Example:
        ```python
        class ToolHandlerPie(BaseToolHandler):
            def build_sql(self, chart_spec):
                # Implementation specific to pie charts
                select = self.build_select_clause(chart_spec)
                from_clause = self.build_from_clause()
                group_by = self.build_group_by_clause(chart_spec)
                return f"{select}\\n{from_clause}\\n{group_by}"

        handler = ToolHandlerPie(data_source="data.parquet", schema={...})
        result_df = handler.execute(chart_spec)
        ```
    """

    def __init__(self, data_source_path: str, schema: Dict[str, str]):
        """
        Inicializa o handler.

        Args:
            data_source_path: Caminho absoluto para o dataset (Parquet/CSV)
            schema: Schema do dataset mapeando column_name → data_type
                   Exemplo: {"sales": "DOUBLE", "region": "VARCHAR"}
        """
        self.data_source = data_source_path
        self.schema = schema
        self.sql_builder = SQLBuilder()

        # Inicializa seletores de agregacao inteligentes
        self.aggregation_selector = AggregationSelector()
        self.aggregation_validator = AggregationValidator(strict_mode=False)

        logger.debug(
            f"Initialized {self.__class__.__name__} with "
            f"data_source={data_source_path}, "
            f"schema_columns={len(schema)}"
        )

    # ========================================================================
    # ABSTRACT METHODS (MUST be implemented by subclasses)
    # ========================================================================

    @abstractmethod
    def build_sql(self, chart_spec: Dict[str, Any]) -> str:
        """
        Constrói SQL a partir do ChartSpec.

        DEVE ser implementado por todas as subclasses.

        Cada chart type tem requirements específicos de SQL:
        - bar_horizontal: SELECT dimensions, AGG(metrics) GROUP BY ORDER BY LIMIT
        - pie: SELECT 1 dimension, AGG(metrics) GROUP BY
        - line: SELECT temporal_dim, metrics ORDER BY temporal ASC
        - histogram: SELECT metrics (SEM GROUP BY)

        Args:
            chart_spec: ChartOutput validado do graphic_classifier contendo:
                - chart_type: Tipo do gráfico
                - dimensions: Lista de dimensions
                - metrics: Lista de metrics com aggregation
                - filters: Dicionário de filtros
                - sort: Especificação de ordenação
                - top_n: Limite de resultados (opcional)

        Returns:
            str: Query SQL completa e executável no DuckDB

        Raises:
            ValueError: Se chart_spec não atende requirements do chart type

        Example:
            ```python
            def build_sql(self, chart_spec):
                select = self.build_select_clause(chart_spec)
                from_clause = "FROM dataset"
                where = self.build_where_clause(chart_spec.get("filters", {}))
                group_by = self.build_group_by_clause(chart_spec)
                order_by = self.build_order_by_clause(chart_spec)
                limit = self.build_limit_clause(chart_spec)

                sql_parts = [select, from_clause]
                if where:
                    sql_parts.append(where)
                if group_by:
                    sql_parts.append(group_by)
                if order_by:
                    sql_parts.append(order_by)
                if limit:
                    sql_parts.append(limit)

                return "\\n".join(sql_parts)
            ```
        """
        pass

    # ========================================================================
    # EXECUTION PIPELINE (DO NOT override unless absolutely necessary)
    # ========================================================================

    def execute(self, chart_spec: Dict[str, Any]) -> pd.DataFrame:
        """
        Pipeline completo: validate → build SQL → execute DuckDB → validate result.

        Este é o método principal que orquestra toda a execução.
        Subclasses NÃO devem sobrescrever este método, apenas build_sql().

        Pipeline:
        1. Validação pré-execução (chart_spec e colunas)
        2. Construção SQL (delegado para subclass.build_sql())
        3. Execução no DuckDB (sem fallback)
        4. Se top_n presente: executa query de totais e anexa ao resultado
        5. Validação pós-execução (resultado tem colunas esperadas)

        Args:
            chart_spec: ChartOutput validado do graphic_classifier

        Returns:
            pd.DataFrame: Resultado da query com colunas nomeadas corretamente.
            Se top_n presente, inclui colunas __full_total_{metric} e __full_count
            com totais globais do dataset filtrado (antes do LIMIT).

        Raises:
            ValueError: Se validação pré ou pós-execução falhar
            AnalyticsExecutionError: Se execução SQL falhar

        Example:
            ```python
            handler = ToolHandlerBarHorizontal(data_source="data.parquet", schema={...})
            result_df = handler.execute(chart_spec)
            # result_df contém os dados agregados prontos para visualização
            # Se top_n=5, result_df tem 5 rows + colunas __full_total_* com totais globais
            ```
        """
        logger.info(f"Starting execution pipeline for {self.__class__.__name__}")

        # 1. Validação pré-execução
        self.validate_chart_spec(chart_spec)
        logger.debug("Pre-execution validation passed")

        # 2. Construção SQL (delegado para subclass)
        sql = self.build_sql(chart_spec)
        logger.debug(f"Generated SQL:\n{sql}")

        # 3. Execução DuckDB (main query)
        result_df = self.execute_duckdb(sql)
        logger.info(f"Query executed successfully, result shape: {result_df.shape}")

        # 4. Se top_n presente, executar query de totais globais
        if chart_spec.get("top_n"):
            logger.info(f"top_n={chart_spec['top_n']} detected, executing totals query")
            totals_sql = self.build_totals_sql(chart_spec)
            logger.debug(f"Generated totals SQL:\n{totals_sql}")

            totals_df = self.execute_duckdb(totals_sql)
            logger.info(f"Totals query executed, shape: {totals_df.shape}")

            # Anexar totais ao resultado principal
            result_df = self._attach_totals_to_result(result_df, totals_df, chart_spec)
            logger.info(f"Totals attached to result as __full_total_* columns")

        # 5. Validação pós-execução
        self.validate_result(result_df, chart_spec)
        logger.debug("Post-execution validation passed")

        return result_df

    # ========================================================================
    # VALIDATION METHODS (Can be overridden for chart-specific validation)
    # ========================================================================

    def validate_chart_spec(self, chart_spec: Dict[str, Any]) -> None:
        """
        Valida que ChartSpec contém campos obrigatórios e colunas existem.

        Validação padrão:
        - Campos obrigatórios: chart_type, metrics, dimensions, data_source
        - Todas as colunas (dimensions + metrics) existem no schema

        Subclasses podem sobrescrever para adicionar validações específicas.
        Exemplo: pie chart valida exatamente 1 dimension.

        Args:
            chart_spec: ChartOutput validado

        Raises:
            ValueError: Se campos obrigatórios faltando ou colunas não existem

        Example:
            ```python
            def validate_chart_spec(self, chart_spec):
                # Chama validação base
                super().validate_chart_spec(chart_spec)

                # Validação específica para pie
                if len(chart_spec.get("dimensions", [])) != 1:
                    raise ValueError("Pie chart requires exactly 1 dimension")
            ```
        """
        # Validar campos obrigatórios
        required_fields = ["chart_type", "metrics", "dimensions", "data_source"]
        for field in required_fields:
            if field not in chart_spec:
                raise ValueError(
                    f"Missing required field in chart_spec: '{field}'. "
                    f"ChartSpec must contain: {required_fields}"
                )

        # Validar que há pelo menos 1 metric
        metrics = chart_spec.get("metrics", [])
        if not metrics:
            raise ValueError("ChartSpec must have at least 1 metric")

        # Validar que colunas existem no schema
        missing_columns = []

        # Validar dimensions
        for dim in chart_spec.get("dimensions", []):
            col_name = dim.get("name")
            if not col_name:
                raise ValueError(f"Dimension missing 'name' field: {dim}")
            if col_name not in self.schema:
                missing_columns.append(col_name)

        # Validar metrics
        virtual_metrics = _get_virtual_metrics()
        for metric in metrics:
            col_name = metric.get("name")
            if not col_name:
                raise ValueError(f"Metric missing 'name' field: {metric}")
            # Virtual metrics (e.g. 'Numero de Clientes') are COUNT(*)
            # aggregations defined in alias.yaml, not actual columns
            if col_name in virtual_metrics:
                logger.debug(
                    f"Metric '{col_name}' is a virtual metric (COUNT(*)), "
                    "skipping column existence check"
                )
                continue
            if col_name not in self.schema:
                missing_columns.append(col_name)

        if missing_columns:
            raise ValueError(
                f"Columns not found in schema: {missing_columns}. "
                f"Available columns: {sorted(self.schema.keys())}"
            )

        logger.debug("ChartSpec validation passed")

    def validate_result(
        self, result_df: pd.DataFrame, chart_spec: Dict[str, Any]
    ) -> None:
        """
        Valida que resultado tem estrutura esperada.

        Validação padrão:
        - DataFrame não está vazio
        - Colunas esperadas existem no resultado

        Args:
            result_df: DataFrame resultado da query
            chart_spec: ChartOutput original

        Raises:
            ValueError: Se resultado inválido
        """
        if result_df.empty:
            logger.warning("Query returned empty result (no rows)")
            # Não lançar erro - resultado vazio pode ser válido

        # Verificar colunas esperadas existem
        expected_cols = []

        for dim in chart_spec.get("dimensions", []):
            expected_cols.append(dim.get("alias", dim["name"]))

        for metric in chart_spec.get("metrics", []):
            expected_cols.append(metric.get("alias", metric["name"]))

        missing_cols = set(expected_cols) - set(result_df.columns)
        if missing_cols:
            raise ValueError(
                f"Result missing expected columns: {missing_cols}. "
                f"Result columns: {list(result_df.columns)}"
            )

        logger.debug(
            f"Result validation passed: {len(result_df)} rows, {len(result_df.columns)} columns"
        )

    # ========================================================================
    # DUCKDB EXECUTION (DO NOT override)
    # ========================================================================

    def execute_duckdb(self, sql: str) -> pd.DataFrame:
        """
        Executa SQL via DuckDB (sem fallback).

        Carrega o dataset no DuckDB como tabela 'dataset' e executa a query.
        Qualquer erro é propagado como AnalyticsExecutionError.

        Args:
            sql: Query SQL completa

        Returns:
            pd.DataFrame: Resultado da query

        Raises:
            AnalyticsExecutionError: Se execução falhar (com SQL e erro original)
        """
        conn = None
        try:
            # Criar conexão DuckDB
            conn = duckdb.connect()

            # Carregar DataFrame usando DataLoader (com extração temporal automática)
            from src.analytics_executor.data.data_loader import DataLoader

            loader = DataLoader(cache_size=5)
            df = loader.load(self.data_source)

            # Registrar dataset como tabela usando o DataFrame processado
            # IMPORTANTE: Usa DataFrame com colunas temporais extraídas
            conn.register("dataset", df)

            logger.debug(
                f"Dataset registered with {len(df)} rows, {len(df.columns)} columns"
            )

            # Executar query
            result = conn.execute(sql).df()

            return result

        except Exception as e:
            # Erro detalhado com SQL e mensagem original
            error_msg = (
                f"DuckDB execution failed\n"
                f"SQL: {sql}\n"
                f"Data source: {self.data_source}\n"
                f"Error: {str(e)}"
            )
            logger.error(error_msg)
            raise AnalyticsExecutionError(error_msg) from e

        finally:
            # Garantir que conexão é fechada
            if conn:
                conn.close()

    # ========================================================================
    # SQL BUILDING UTILITIES (Reusable by subclasses)
    # ========================================================================

    def build_select_clause(self, chart_spec: Dict[str, Any]) -> str:
        """
        Constrói SELECT com dimensions + metrics agregados.

        Padrão:
        - Dimensions: SELECT dimension_name AS dimension_alias
        - Metrics: SELECT AGG(metric_name) AS metric_alias

        A agregação é selecionada inteligentemente baseada em:
        1. Tipo da coluna (numeric → SUM, categorical → COUNT)
        2. Configuração do alias.yaml
        3. Schema SQL (fallback)
        4. Validação automática de combinações inválidas

        Args:
            chart_spec: ChartOutput

        Returns:
            str: Cláusula SELECT completa

        Example:
            ```
            SELECT
                "region" AS "region",
                SUM("sales") AS "total_sales",
                AVG("price") AS "avg_price"
            ```
        """
        parts = []

        # Dimensions (sem agregação)
        for dim in chart_spec.get("dimensions", []):
            col = self.sql_builder.escape_identifier(dim["name"])
            alias = self.sql_builder.escape_identifier(dim.get("alias", dim["name"]))
            parts.append(f"{col} AS {alias}")

        # Metrics (com agregação inteligente)
        virtual_metrics = _get_virtual_metrics()
        for metric in chart_spec.get("metrics", []):
            column_name = metric["name"]
            alias = self.sql_builder.escape_identifier(
                metric.get("alias", metric["name"])
            )

            # Virtual metrics (e.g. 'Numero de Clientes') -> COUNT(*)
            if column_name in virtual_metrics:
                parts.append(f"COUNT(*) AS {alias}")
                metric["aggregation"] = "count"
                logger.debug(f"Virtual metric '{column_name}' resolved to COUNT(*)")
                continue

            col = self.sql_builder.escape_identifier(column_name)

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

            # Atualiza métrica com agregação validada (para logging/debug)
            metric["aggregation"] = validated_agg
            if was_corrected:
                metric["_auto_corrected"] = True
                metric["_original_aggregation"] = selected_agg

            # Monta cláusula SQL
            agg = validated_agg.upper()
            alias = self.sql_builder.escape_identifier(
                metric.get("alias", metric["name"])
            )
            parts.append(f"{agg}({col}) AS {alias}")

        return "SELECT " + ", ".join(parts)

    def build_from_clause(self) -> str:
        """
        Constrói FROM clause.

        Sempre retorna "FROM dataset" pois o dataset é registrado
        com este nome no DuckDB.

        Returns:
            str: Cláusula FROM
        """
        return "FROM dataset"

    def build_where_clause(self, filters: Dict[str, Any]) -> str:
        """
        Constrói WHERE a partir de filters.

        Suporta:
        - Equality: {"column": value}
        - IN clause: {"column": [value1, value2, value3, ...]}
        - Temporal Range: {"column": [start_date, end_date]} -> BETWEEN
        - Operator: {"column": {"operator": ">=", "value": 10}}
        - Explicit Between: {"column": {"between": [start, end]}}

        Args:
            filters: Dict com filtros do ChartSpec

        Returns:
            str: Cláusula WHERE (ou string vazia se sem filtros)

        Example:
            ```python
            filters = {
                "region": ["North", "South"],
                "year": {"operator": ">=", "value": 2020},
                "Data": ["2015-02-01", "2015-02-28"]  # Temporal range
            }
            # Returns: WHERE "region" IN ('North', 'South') AND "year" >= 2020
            #          AND "Data" BETWEEN '2015-02-01' AND '2015-02-28'
            ```
        """
        if not filters:
            return ""

        conditions = []

        for col, value in filters.items():
            # FASE 1 - Etapa 1.2: Validar Detecao de Range Temporal
            # Fallback para strings com separador (formato incorreto de filtro temporal)
            if isinstance(value, str) and ", " in value:
                # Tentar converter string concatenada para array temporal
                parts = [v.strip() for v in value.split(", ")]
                if len(parts) == 2 and self._is_temporal_range(parts):
                    logger.warning(
                        f"[build_where_clause] FASE 1.2: Detected malformed temporal filter "
                        f"(string instead of array): {col}='{value}'. Converting to BETWEEN clause."
                    )
                    conditions.append(
                        self.sql_builder.build_between_clause(col, parts[0], parts[1])
                    )
                    continue
                else:
                    # Not a temporal range, treat as regular string
                    logger.debug(
                        f"[build_where_clause] String filter with comma but not temporal: {col}='{value}'"
                    )
                    conditions.append(
                        self.sql_builder.build_comparison(col, "=", value)
                    )
                    continue

            if isinstance(value, list):
                # FASE 4 - CORREÇÃO CRÍTICA: Verificar tipo da coluna no schema
                # ANTES de decidir entre BETWEEN e IN

                # Critério 1: Lista com exatamente 2 valores?
                is_two_element_list = len(value) == 2

                # Critério 2: Valores parecem datas?
                values_look_like_dates = self._is_temporal_range(value)

                # Critério 3: Coluna é temporal no schema?
                column_is_temporal = self._is_column_temporal(col)

                # APENAS aplicar BETWEEN se TODOS os critérios forem True
                if (
                    is_two_element_list
                    and values_look_like_dates
                    and column_is_temporal
                ):
                    # Temporal BETWEEN (caso válido)
                    logger.info(
                        f"[build_where_clause] Applying BETWEEN on temporal column '{col}': "
                        f"{value[0]} to {value[1]}"
                    )
                    conditions.append(
                        self.sql_builder.build_between_clause(col, value[0], value[1])
                    )
                else:
                    # Lista categórica → IN
                    if (
                        is_two_element_list
                        and values_look_like_dates
                        and not column_is_temporal
                    ):
                        logger.warning(
                            f"[build_where_clause] Column '{col}' looks like temporal range "
                            f"but schema type is not temporal. Using IN clause instead of BETWEEN. "
                            f"Values: {value}"
                        )

                    logger.info(
                        f"[build_where_clause] Applying IN clause on column '{col}': "
                        f"{len(value)} values"
                    )
                    conditions.append(self.sql_builder.build_in_clause(col, value))

            elif isinstance(value, dict):
                # Check for explicit between syntax first
                if "between" in value:
                    # Explicit between syntax: {"between": [start, end]}
                    between_values = value["between"]
                    if isinstance(between_values, list) and len(between_values) == 2:
                        conditions.append(
                            self.sql_builder.build_between_clause(
                                col, between_values[0], between_values[1]
                            )
                        )
                else:
                    # Standard operator filter
                    op = value.get("operator", "=")
                    val = value.get("value")
                    if val is not None:
                        conditions.append(
                            self.sql_builder.build_comparison(col, op, val)
                        )

            else:
                # Simple equality
                conditions.append(self.sql_builder.build_comparison(col, "=", value))

        if conditions:
            return "WHERE " + " AND ".join(conditions)
        return ""

    @staticmethod
    def _is_temporal_range(value: Any) -> bool:
        """
        Detect if a list value represents a temporal range.

        ⚠️ DEPRECATED: Este método não deve mais ser usado sozinho.
        Use _is_column_temporal() em conjunto para validação completa.

        Temporal ranges have:
        - Exactly 2 elements
        - Both elements are date-like (datetime, date, or ISO string)

        This method uses heuristics to identify date strings without
        hardcoding specific column names, making it scalable.

        Args:
            value: Value to check

        Returns:
            True if value represents a temporal range

        Examples:
            >>> BaseToolHandler._is_temporal_range(["2015-02-01", "2015-02-28"])
            True

            >>> BaseToolHandler._is_temporal_range(["SP", "RJ"])
            False

            >>> BaseToolHandler._is_temporal_range([2020, 2021])
            True  # Year range

            >>> BaseToolHandler._is_temporal_range(["SP", "RJ", "MG"])
            False  # More than 2 elements
        """
        if not isinstance(value, list) or len(value) != 2:
            return False

        from datetime import datetime, date

        date_count = 0

        for v in value:
            # Check if datetime/date object
            if isinstance(v, (datetime, date)):
                date_count += 1
                continue

            # Check if integer year (1900-2100)
            if isinstance(v, int) and 1900 <= v <= 2100:
                date_count += 1
                continue

            # Check if string that looks like a date
            if isinstance(v, str):
                # Try common date formats
                date_patterns = [
                    "%Y-%m-%d",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y/%m/%d",
                    "%d-%m-%Y",
                    "%d/%m/%Y",
                ]
                is_date = False
                for pattern in date_patterns:
                    try:
                        # Strip microseconds if present
                        clean_str = v.split(".")[0]
                        datetime.strptime(clean_str, pattern)
                        is_date = True
                        break
                    except ValueError:
                        continue

                if is_date:
                    date_count += 1
                    continue

        # Both values must be date-like
        return date_count == 2

    def _is_column_temporal(self, column_name: str) -> bool:
        """
        Verifica se uma coluna é de tipo temporal baseado no schema.

        FASE 4 - CORREÇÃO CRÍTICA: Previne aplicação incorreta de BETWEEN
        em colunas não-temporais.

        Esta função resolve o bug de "zero registros" verificando o tipo
        real da coluna antes de aplicar operadores temporais.

        Args:
            column_name: Nome da coluna a verificar

        Returns:
            True se a coluna for temporal (DATE, TIMESTAMP, DATETIME)

        Examples:
            >>> handler._is_column_temporal("Data")
            True  # coluna DATE

            >>> handler._is_column_temporal("Mes")
            False  # coluna VARCHAR

            >>> handler._is_column_temporal("Des_Linha_Produto")
            False  # coluna VARCHAR
        """
        if not self.schema:
            # Sem schema, não podemos garantir - fallback seguro é False
            logger.warning(
                f"[_is_column_temporal] No schema available for column '{column_name}', "
                f"defaulting to non-temporal (IN clause will be used)"
            )
            return False

        column_type = self.schema.get(column_name, "").upper()

        # Tipos temporais reconhecidos
        temporal_types = [
            "DATE",
            "TIMESTAMP",
            "DATETIME",
            "TIMESTAMP WITH TIME ZONE",
            "TIMESTAMP WITHOUT TIME ZONE",
        ]

        is_temporal = any(t in column_type for t in temporal_types)

        logger.debug(
            f"[_is_column_temporal] Column '{column_name}': "
            f"type='{column_type}', is_temporal={is_temporal}"
        )

        return is_temporal

    def build_group_by_clause(self, chart_spec: Dict[str, Any]) -> str:
        """
        Constrói GROUP BY a partir de dimensions.

        Args:
            chart_spec: ChartOutput

        Returns:
            str: Cláusula GROUP BY (ou string vazia se sem dimensions)

        Example:
            ```
            GROUP BY "region", "product_category"
            ```
        """
        dimensions = chart_spec.get("dimensions", [])
        if not dimensions:
            return ""

        cols = [self.sql_builder.escape_identifier(dim["name"]) for dim in dimensions]
        return "GROUP BY " + ", ".join(cols)

    def build_order_by_clause(self, chart_spec: Dict[str, Any]) -> str:
        """
        Constrói ORDER BY a partir de sort.

        IMPORTANT: When sorting by a metric (aggregated column), we must use
        the alias, not the original column name, because the original column
        is wrapped in an aggregation function (SUM, AVG, etc.) and cannot be
        referenced directly in ORDER BY.

        FASE 4 - Correcao ORDER BY: Traduz nomes genericos (value, variation, temporal)
        para aliases reais das colunas.

        Args:
            chart_spec: ChartOutput

        Returns:
            str: Cláusula ORDER BY (ou string vazia se sem sort)

        Example:
            ```
            ORDER BY "total_sales" DESC
            ```
        """
        sort = chart_spec.get("sort")
        if not sort or not sort.get("by"):
            return ""

        sort_by = sort["by"]
        # Handle None explicitly: get() returns None if key exists with None value
        order = sort.get("order") or "asc"
        order = order.upper()

        metrics = chart_spec.get("metrics", [])
        dimensions = chart_spec.get("dimensions", [])

        # FASE 4 - CORRECAO CRITICA: Traduzir nomes genericos do intent_config
        # para aliases reais das colunas no SELECT

        # 1. "value" -> Primeira metrica (caso comum em rankings)
        if sort_by == "value":
            if metrics:
                primary_metric = metrics[0]
                sort_col = self.sql_builder.escape_identifier(
                    primary_metric.get("alias", primary_metric["name"])
                )
                logger.debug(
                    f"[build_order_by_clause] Translated generic 'value' to metric alias: {sort_col}"
                )
                return f"ORDER BY {sort_col} {order}"
            else:
                logger.warning(
                    "[build_order_by_clause] sort.by='value' but no metrics found. "
                    "Cannot build ORDER BY clause."
                )
                return ""

        # 2. "variation" -> Campo calculado ou primeira metrica
        # (Para queries de comparacao temporal que requerem variacao)
        if sort_by == "variation":
            # Check if there's a calculated field spec with variation
            calculated_field = chart_spec.get("calculated_field")
            if calculated_field and calculated_field.get("type") == "variation":
                # Use the calculated field alias
                variation_alias = calculated_field.get("alias", "variation")
                sort_col = self.sql_builder.escape_identifier(variation_alias)
                logger.debug(
                    f"[build_order_by_clause] Translated 'variation' to calculated field: {sort_col}"
                )
                return f"ORDER BY {sort_col} {order}"

            # Fallback: use first metric (for backward compatibility)
            if metrics:
                primary_metric = metrics[0]
                sort_col = self.sql_builder.escape_identifier(
                    primary_metric.get("alias", primary_metric["name"])
                )
                logger.warning(
                    f"[build_order_by_clause] sort.by='variation' but no calculated field found. "
                    f"Falling back to first metric: {sort_col}"
                )
                return f"ORDER BY {sort_col} {order}"
            else:
                logger.error(
                    "[build_order_by_clause] sort.by='variation' but no metrics or calculated fields. "
                    "Cannot build ORDER BY clause."
                )
                return ""

        # 3. "temporal" -> Primeira dimension temporal
        if sort_by == "temporal":
            # Find first temporal dimension (common names: Data, Mes, Ano, etc.)
            temporal_keywords = [
                "data",
                "mes",
                "ano",
                "month",
                "year",
                "date",
                "trimestre",
                "quarter",
            ]
            temporal_dim = None

            for dim in dimensions:
                dim_name_lower = dim["name"].lower()
                if any(kw in dim_name_lower for kw in temporal_keywords):
                    temporal_dim = dim
                    break

            if not temporal_dim and dimensions:
                # Fallback: use first dimension
                temporal_dim = dimensions[0]
                logger.warning(
                    f"[build_order_by_clause] sort.by='temporal' but no temporal dimension detected. "
                    f"Using first dimension: {temporal_dim['name']}"
                )

            if temporal_dim:
                sort_col = self.sql_builder.escape_identifier(
                    temporal_dim.get("alias", temporal_dim["name"])
                )
                logger.debug(
                    f"[build_order_by_clause] Translated 'temporal' to dimension: {sort_col}"
                )
                return f"ORDER BY {sort_col} {order}"
            else:
                logger.error(
                    "[build_order_by_clause] sort.by='temporal' but no dimensions found. "
                    "Cannot build ORDER BY clause."
                )
                return ""

        # 4. Check if sorting by a metric name (exact match)
        for metric in metrics:
            if metric["name"] == sort_by:
                # Use metric alias instead of original column name
                sort_col = self.sql_builder.escape_identifier(
                    metric.get("alias", metric["name"])
                )
                logger.debug(
                    f"[build_order_by_clause] Matched sort.by to metric name: {sort_by} -> {sort_col}"
                )
                return f"ORDER BY {sort_col} {order}"

        # 5. Check if sorting by a dimension name (exact match)
        for dim in dimensions:
            if dim["name"] == sort_by:
                sort_col = self.sql_builder.escape_identifier(
                    dim.get("alias", dim["name"])
                )
                logger.debug(
                    f"[build_order_by_clause] Matched sort.by to dimension name: {sort_by} -> {sort_col}"
                )
                return f"ORDER BY {sort_col} {order}"

        # 6. Fallback: use the sort_by as-is (for backward compatibility with aliases)
        sort_col = self.sql_builder.escape_identifier(sort_by)
        logger.warning(
            f"[build_order_by_clause] No exact match for sort.by='{sort_by}'. "
            f"Using as-is (may fail if alias doesn't exist): {sort_col}"
        )
        return f"ORDER BY {sort_col} {order}"

    def build_limit_clause(self, chart_spec: Dict[str, Any]) -> str:
        """
        Constrói LIMIT a partir de top_n.

        Args:
            chart_spec: ChartOutput

        Returns:
            str: Cláusula LIMIT (ou string vazia se sem top_n)

        Example:
            ```
            LIMIT 10
            ```
        """
        top_n = chart_spec.get("top_n")
        if top_n:
            return f"LIMIT {top_n}"
        return ""

    # ========================================================================
    # PLOTLY CONFIG (Can be overridden for chart-specific config)
    # ========================================================================

    def build_plotly_config(
        self, chart_spec: Dict[str, Any], data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Constrói configuração Plotly para o chart type.

        Método padrão genérico. Subclasses DEVEM sobrescrever para
        criar configurações específicas do chart type.

        Args:
            chart_spec: ChartOutput
            data: DataFrame com resultado da query

        Returns:
            dict: Configuração Plotly com data e layout

        Example:
            ```python
            def build_plotly_config(self, chart_spec, data):
                return {
                    "data": [{
                        "type": "pie",
                        "labels": data["category"].tolist(),
                        "values": data["value"].tolist()
                    }],
                    "layout": {
                        "title": chart_spec.get("title", "Pie Chart")
                    }
                }
            ```
        """
        dimensions = chart_spec.get("dimensions", [])
        metrics = chart_spec.get("metrics", [])

        # Config genérico básico
        return {
            "data": data.to_dict(orient="records"),
            "layout": {
                "title": chart_spec.get("title", ""),
                "xaxis": {"title": dimensions[0].get("alias", dimensions[0]["name"])}
                if dimensions
                else {},
                "yaxis": {"title": metrics[0].get("alias", metrics[0]["name"])}
                if metrics
                else {},
            },
        }

    # ========================================================================
    # TOTALS QUERY (For top_n scenarios - Two-Query Approach)
    # ========================================================================

    def build_totals_sql(self, chart_spec: Dict[str, Any]) -> str:
        """
        Constrói query de totais globais para cenários com top_n.

        Esta query calcula agregações sobre TODO o dataset filtrado,
        SEM aplicar GROUP BY (para dimensions), ORDER BY ou LIMIT.
        Retorna uma única linha com totais globais.

        A query de totais usa os MESMOS FILTROS da query principal,
        garantindo que os totais reflitam exatamente o universo filtrado.

        Args:
            chart_spec: ChartOutput com filters e metrics

        Returns:
            str: Query SQL que retorna totais globais

        Example Output SQL:
            ```sql
            SELECT
                SUM("Valor_Vendido") AS __total_Valor_Vendido,
                COUNT(*) AS __total_count
            FROM dataset
            WHERE "Regiao" = 'Sul'
            -- NO GROUP BY, NO ORDER BY, NO LIMIT
            ```

        Example Result:
            | __total_Valor_Vendido | __total_count |
            |----------------------|---------------|
            | 1500000.00           | 150           |
        """
        # Reutilizar lógica de filtros (mesmos filtros da query principal)
        where_clause = self.build_where_clause(chart_spec.get("filters", {}))

        # Construir SELECT com agregações para cada métrica
        totals_select_parts = []
        virtual_metrics = _get_virtual_metrics()

        for metric in chart_spec.get("metrics", []):
            column_name = metric["name"]

            # Virtual metrics are already COUNT(*), skip numeric check
            if column_name in virtual_metrics:
                safe_metric_name = column_name.replace(" ", "_")
                alias = f"__total_{safe_metric_name}"
                totals_select_parts.append(
                    f"COUNT(*) AS {self.sql_builder.escape_identifier(alias)}"
                )
                continue

            col = self.sql_builder.escape_identifier(column_name)

            # Verificar se a métrica é numérica
            column_type = self.aggregation_selector.get_column_type(
                column_name, self.schema
            )

            if column_type in ["numeric", "integer", "float", "double"]:
                # Usar mesmo agregador da query principal
                agg = metric.get("aggregation", "sum").upper()

                # Criar alias para total: __total_{nome_metrica}
                safe_metric_name = column_name.replace(" ", "_")
                alias = f"__total_{safe_metric_name}"

                totals_select_parts.append(
                    f"{agg}({col}) AS {self.sql_builder.escape_identifier(alias)}"
                )

        # Adicionar COUNT(*) para contagem total de registros
        totals_select_parts.append("COUNT(*) AS __total_count")

        # Montar SQL final (SEM GROUP BY, SEM ORDER BY, SEM LIMIT)
        sql_parts = [
            "SELECT " + ", ".join(totals_select_parts),
            self.build_from_clause(),
        ]

        if where_clause:
            sql_parts.append(where_clause)

        sql = "\n".join(sql_parts)

        logger.debug(
            f"Generated totals query with {len(chart_spec.get('metrics', []))} metrics"
        )

        return sql

    def _attach_totals_to_result(
        self,
        result_df: pd.DataFrame,
        totals_df: pd.DataFrame,
        chart_spec: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Anexa totais globais ao DataFrame de resultados como colunas de metadata.

        Adiciona colunas __full_total_{metric} e __full_count ao result_df.
        Todas as linhas recebem o mesmo valor (total global), permitindo
        que o result_formatter extraia esses valores posteriormente.

        Args:
            result_df: DataFrame da query principal (Top N rows)
            totals_df: DataFrame da query de totais (1 row com agregados)
            chart_spec: ChartOutput

        Returns:
            pd.DataFrame: result_df com colunas de totais adicionadas

        Example:
            Input result_df (3 rows):
            | cliente | vendas |
            |---------|--------|
            | A       | 50000  |
            | B       | 40000  |
            | C       | 30000  |

            Input totals_df (1 row):
            | __total_vendas | __total_count |
            |----------------|---------------|
            | 250000         | 50            |

            Output result_df (3 rows):
            | cliente | vendas | __full_total_vendas | __full_count |
            |---------|--------|---------------------|--------------|
            | A       | 50000  | 250000              | 50           |
            | B       | 40000  | 250000              | 50           |
            | C       | 30000  | 250000              | 50           |
        """
        if totals_df.empty:
            logger.warning("Totals query returned empty result")
            return result_df

        # Extrair totais da única linha retornada
        totals_row = totals_df.iloc[0]

        # Adicionar cada total como coluna constante (todas linhas = mesmo valor)
        for metric in chart_spec.get("metrics", []):
            metric_name = metric["name"].replace(" ", "_")
            total_col_name = f"__total_{metric_name}"

            if total_col_name in totals_row:
                total_value = totals_row[total_col_name]
                # Nome compatível com result_formatter: __full_total_{metric}
                result_df[f"__full_total_{metric_name}"] = total_value

        # Adicionar contagem total
        if "__total_count" in totals_row:
            result_df["__full_count"] = totals_row["__total_count"]

        logger.info(
            f"Attached {len(chart_spec.get('metrics', []))} global totals "
            f"to result DataFrame (total_count={totals_row.get('__total_count', 'N/A')})"
        )

        return result_df
