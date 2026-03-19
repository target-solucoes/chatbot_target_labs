"""
Auxiliary nodes for LangGraph analytics executor workflow.

This module implements the input and output nodes that wrap the
tool handlers in the analytics execution pipeline.
"""

import logging
from typing import Dict, Any
from pathlib import Path
import pandas as pd

from ..parsers.json_parser import JSONParser, JSONParsingError
from ..data.data_loader import DataLoader
from ..data.column_validator import ColumnValidator
from ..execution.filter_normalizer import FilterNormalizer
from ..utils.aggregation_selector import AggregationSelector
from ..utils.context_detector import ContextDetector

logger = logging.getLogger(__name__)


def parse_input_node(state: dict) -> dict:
    """
    Parse input and prepare state for tool execution.

    This node is the entry point of the workflow. It:
    1. Validates the chart_spec is present
    2. Extracts data_source_path from chart_spec
    3. Loads the dataset using DataLoader
    4. Extracts schema from loaded DataFrame
    5. Updates state with necessary information

    Args:
        state: AnalyticsState with chart_spec already populated

    Returns:
        Updated state with:
        - schema: Dict mapping column names to data types
        - data_source_path: Absolute path to data file
        - execution_success: False by default (updated by tool handlers)

    Raises:
        ValueError: If chart_spec is missing or invalid
        FileNotFoundError: If data source file doesn't exist

    Example:
        >>> state = {"chart_spec": {"chart_type": "pie", "data_source": "data/sales.parquet"}}
        >>> state = parse_input_node(state)
        >>> print(state["data_source_path"])
        'data/sales.parquet'
        >>> print(list(state["schema"].keys())[:3])
        ['sales', 'region', 'date']
    """
    logger.info("parse_input_node: Starting input parsing")

    try:
        # Validate chart_spec exists
        chart_spec = state.get("chart_spec")
        if not chart_spec:
            raise ValueError(
                "chart_spec not found in state. "
                "State must contain a valid chart_spec before parse_input_node."
            )

        # Extract data_source
        data_source = chart_spec.get("data_source")
        if not data_source:
            raise ValueError(
                "data_source not found in chart_spec. "
                "Chart specification must include a data_source field."
            )

        # Resolve data source path
        data_source_path = Path(data_source)

        # Handle relative paths - assume relative to project root
        if not data_source_path.is_absolute():
            # Try common locations
            possible_paths = [
                data_source_path,  # As-is
                Path("data/datasets") / data_source_path.name,  # datasets dir
                Path("data") / data_source_path.name,  # data dir
            ]

            # Add .parquet extension if no extension
            if not data_source_path.suffix:
                possible_paths.extend(
                    [Path(str(p) + ".parquet") for p in possible_paths]
                )

            # Find first existing path
            resolved_path = None
            for path in possible_paths:
                if path.exists():
                    resolved_path = path
                    break

            if resolved_path is None:
                raise FileNotFoundError(
                    f"Data source not found: {data_source}. "
                    f"Tried paths: {[str(p) for p in possible_paths]}"
                )

            data_source_path = resolved_path.resolve()

        # Validate file exists
        if not data_source_path.exists():
            raise FileNotFoundError(
                f"Data source file does not exist: {data_source_path}"
            )

        logger.info(f"parse_input_node: Data source resolved to {data_source_path}")

        # Load dataset to extract schema
        logger.info("parse_input_node: Loading dataset for schema extraction")
        data_loader = DataLoader(cache_size=5)
        df = data_loader.load(data_source_path)

        logger.info(
            f"parse_input_node: Dataset loaded - "
            f"{len(df)} rows, {len(df.columns)} columns"
        )

        # Extract schema from DataFrame
        schema = _extract_schema_from_dataframe(df)

        logger.info(f"parse_input_node: Schema extracted - {len(schema)} columns")

        # Normalize filters if present in chart_spec
        filters = chart_spec.get("filters", {})
        if filters:
            logger.info(
                f"parse_input_node: Normalizing {len(filters)} filter(s) "
                f"against dataset values"
            )
            try:
                normalizer = FilterNormalizer(df, case_sensitive=False)
                normalized_filters = normalizer.normalize_filters(filters)

                # Update chart_spec with normalized filters
                chart_spec["filters"] = normalized_filters
                state["chart_spec"] = chart_spec

                logger.info(
                    f"parse_input_node: Filters normalized successfully. "
                    f"See logs for details of any transformations."
                )
            except Exception as e:
                logger.warning(
                    f"parse_input_node: Filter normalization failed: {e}. "
                    f"Proceeding with original filters."
                )
                # Continue with original filters - don't fail the entire pipeline

        # Update state
        state["data_source_path"] = str(data_source_path)
        state["schema"] = schema
        state["execution_success"] = False  # Will be set to True by tool handler
        state["error_message"] = None

        # Enriquecimento inteligente de agregacoes nas metricas
        logger.info("parse_input_node: Iniciando enriquecimento de agregacoes")
        _enrich_metrics_with_intelligent_aggregations(
            chart_spec=chart_spec,
            schema=schema,
            original_query=state.get("original_query", ""),
        )

        logger.info("parse_input_node: Input parsing completed successfully")

        return state

    except Exception as e:
        error_msg = f"parse_input_node failed: {str(e)}"
        logger.error(error_msg, exc_info=True)

        # Update state with error
        state["execution_success"] = False
        state["error_message"] = error_msg
        state["schema"] = {}
        state["data_source_path"] = ""

        return state


def format_output_node(state: dict) -> dict:
    """
    Format final output from execution results.

    This node is the exit point of the workflow. It:
    1. Collects all execution results from state
    2. Formats data for JSON serialization
    3. Builds final output structure
    4. Handles error cases gracefully

    Args:
        state: AnalyticsState with execution results from tool handler

    Returns:
        Updated state with final_output containing:
        - status: "success" or "error"
        - chart_type: Type of chart
        - data: Processed data as list of dicts
        - plotly_config: Plotly configuration (if successful)
        - sql_query: SQL query executed (if successful)
        - engine_used: Execution engine (always "DuckDB")
        - metadata: Additional metadata
        - error: Error details (if status is "error")

    Example:
        >>> state = {
        ...     "chart_spec": {"chart_type": "pie"},
        ...     "execution_success": True,
        ...     "result_dataframe": df,
        ...     "plotly_config": {...},
        ...     "sql_query": "SELECT ...",
        ...     "engine_used": "DuckDB"
        ... }
        >>> state = format_output_node(state)
        >>> print(state["final_output"]["status"])
        'success'
    """
    logger.info("format_output_node: Starting output formatting")

    try:
        chart_spec = state.get("chart_spec", {})
        execution_success = state.get("execution_success", False)

        if execution_success:
            # Success case - format complete output using ResultFormatter
            result_df = state.get("result_dataframe")

            # Use ResultFormatter to properly extract metadata including full_dataset_totals
            from src.analytics_executor.formatters.result_formatter import (
                ResultFormatter,
            )
            from src.shared_lib.models.schema import AnalyticsInputSpec

            # Convert chart_spec to AnalyticsInputSpec
            try:
                spec = AnalyticsInputSpec(**chart_spec)
            except Exception as e:
                logger.warning(
                    f"Could not convert chart_spec to AnalyticsInputSpec: {e}, using dict"
                )
                spec = chart_spec

            # Format result using ResultFormatter
            formatter = ResultFormatter(validate_schema=False)

            if result_df is not None and isinstance(result_df, pd.DataFrame):
                # Use ResultFormatter to format with proper metadata extraction
                formatted_result = formatter.format(
                    spec=spec,
                    result_df=result_df,
                    engine=state.get("engine_used", "DuckDB"),
                    execution_time=0.0,  # Not tracked here
                )

                # Extract formatted data and metadata
                data = formatted_result.get("data", [])
                metadata = formatted_result.get("metadata", {})
                row_count = len(data)

                logger.info(
                    f"format_output_node: Formatted {row_count} rows with ResultFormatter"
                )

                # Check if full_dataset_totals was extracted
                if "full_dataset_totals" in metadata:
                    logger.info(
                        f"format_output_node: full_dataset_totals extracted: "
                        f"{list(metadata['full_dataset_totals'].keys())}"
                    )

            else:
                data = []
                row_count = 0
                metadata = {
                    "chart_title": chart_spec.get("title"),
                    "chart_description": chart_spec.get("description"),
                    "row_count": row_count,
                    "dimensions": chart_spec.get("dimensions", []),
                    "metrics": chart_spec.get("metrics", []),
                    "filters": chart_spec.get("filters", {}),
                    "sort": chart_spec.get("sort"),
                    "top_n": chart_spec.get("top_n"),
                }

            final_output = {
                "status": "success",
                "chart_type": chart_spec.get("chart_type"),
                "data": data,
                "plotly_config": state.get("plotly_config"),
                "sql_query": state.get("sql_query"),
                "engine_used": state.get("engine_used", "DuckDB"),
                "metadata": metadata,
            }

            # ================================================================
            # FASE 5: ENRICH LIMIT METADATA WITH ACTUAL COUNTS
            # ================================================================
            # If the chart_spec contains limit_metadata from CategoryLimiter,
            # update it with actual execution results
            limit_metadata = chart_spec.get("limit_metadata")
            if limit_metadata:
                # Update with actual counts from execution
                actual_count = len(data)

                # If limit was applied, the actual_count is the display_count
                # We need to get the original_count from metadata if available
                if limit_metadata.get("limit_applied"):
                    # Check if we have full_dataset_totals (for top_n queries)
                    full_totals = metadata.get("full_dataset_totals")
                    if full_totals:
                        # Extract the total count from full_dataset_totals
                        # This represents the count before limiting
                        original_count = full_totals.get(
                            "__full_total_count", actual_count
                        )
                    else:
                        # Fallback: use actual count (may be inaccurate if limit was applied)
                        original_count = actual_count

                    # Enrich limit_metadata with actual execution results
                    enriched_limit_metadata = {
                        **limit_metadata,
                        "original_count": original_count,
                        "display_count": actual_count,
                    }

                    # Add to final output metadata
                    final_output["limit_metadata"] = enriched_limit_metadata

                    logger.info(
                        f"format_output_node: FASE 5 Limit metadata enriched - "
                        f"original={original_count}, display={actual_count}, "
                        f"source={limit_metadata.get('limit_source')}"
                    )
                else:
                    # No limit applied - just pass through
                    final_output["limit_metadata"] = limit_metadata

            logger.info("format_output_node: Output formatted successfully")

        else:
            # Error case - format error output
            error_message = state.get("error_message", "Unknown error")

            logger.warning(
                f"format_output_node: Formatting error output - {error_message}"
            )

            final_output = {
                "status": "error",
                "chart_type": chart_spec.get("chart_type"),
                "data": [],
                "plotly_config": None,
                "sql_query": state.get("sql_query"),
                "engine_used": None,
                "metadata": {
                    "chart_title": chart_spec.get("title"),
                    "error_type": "ExecutionError",
                },
                "error": {"message": error_message, "type": "ExecutionError"},
            }

        # Update state with final output
        state["final_output"] = final_output

        logger.info(
            f"format_output_node: Completed with status={final_output['status']}"
        )

        return state

    except Exception as e:
        error_msg = f"format_output_node failed: {str(e)}"
        logger.error(error_msg, exc_info=True)

        # Fallback error output
        state["final_output"] = {
            "status": "error",
            "chart_type": None,
            "data": [],
            "plotly_config": None,
            "sql_query": None,
            "engine_used": None,
            "metadata": {},
            "error": {"message": error_msg, "type": "FormattingError"},
        }

        return state


def _enrich_metrics_with_intelligent_aggregations(
    chart_spec: Dict[str, Any], schema: Dict[str, str], original_query: str = ""
) -> None:
    """
    Enriquece as metricas do chart_spec com agregacoes inteligentes.

    Esta funcao analisa cada metrica e:
    1. Identifica o tipo da coluna (numeric, categorical, temporal)
    2. Seleciona a agregacao apropriada baseada no tipo
    3. Considera o contexto da query original (se disponivel)
    4. Atualiza in-place o chart_spec com as agregacoes otimizadas

    A funcao preserva agregacoes explicitas do usuario e apenas enriquece
    metricas que estao usando a agregacao padrao (sum).

    Args:
        chart_spec: Especificacao do grafico (modificado in-place)
        schema: Schema SQL do dataset
        original_query: Query original do usuario para contexto (opcional)
    """
    metrics = chart_spec.get("metrics", [])

    if not metrics:
        logger.debug("_enrich_metrics: Nenhuma metrica para enriquecer")
        return

    # Inicializa seletores
    aggregation_selector = AggregationSelector()
    context_detector = ContextDetector()

    enriched_count = 0

    for metric in metrics:
        column_name = metric.get("name")
        if not column_name:
            logger.warning("_enrich_metrics: Metrica sem nome, pulando")
            continue

        current_agg = metric.get("aggregation", "sum")

        # Apenas enriquece se estiver usando agregacao padrao
        # Preserva agregacoes explicitas do usuario
        if current_agg != "sum":
            logger.debug(
                f"_enrich_metrics: Metrica '{column_name}' ja possui agregacao "
                f"explicita '{current_agg}', preservando"
            )
            continue

        # Identifica tipo da coluna
        column_type = aggregation_selector.get_column_type(column_name, schema)

        # Seleciona agregacao baseada no tipo
        suggested_agg = aggregation_selector.select_aggregation(
            column_name=column_name,
            schema=schema,
            user_specified=None,  # Force intelligent selection
        )

        # Refina baseado no contexto da query (se disponivel)
        if original_query:
            refined_agg = context_detector.refine_aggregation(
                base_aggregation=suggested_agg,
                query=original_query,
                column_type=column_type,
            )
        else:
            refined_agg = suggested_agg

        # Atualiza metrica se a agregacao mudou
        if refined_agg != current_agg:
            metric["aggregation"] = refined_agg
            metric["_enriched"] = True
            metric["_original_aggregation"] = current_agg
            enriched_count += 1

            logger.info(
                f"_enrich_metrics: Metrica '{column_name}' enriquecida - "
                f"{current_agg.upper()} -> {refined_agg.upper()} "
                f"(tipo: {column_type})"
            )

    logger.info(
        f"_enrich_metrics: Enriquecimento completo - "
        f"{enriched_count}/{len(metrics)} metricas atualizadas"
    )


def _extract_schema_from_dataframe(df: pd.DataFrame) -> Dict[str, str]:
    """
    Extract schema from pandas DataFrame.

    Maps pandas dtypes to SQL-like type names for validation.

    Args:
        df: Pandas DataFrame

    Returns:
        Dict mapping column names to type strings

    Example:
        >>> df = pd.DataFrame({"sales": [1.0, 2.0], "region": ["A", "B"]})
        >>> schema = _extract_schema_from_dataframe(df)
        >>> schema["sales"]
        'DOUBLE'
        >>> schema["region"]
        'VARCHAR'
    """
    type_mapping = {
        "int64": "BIGINT",
        "int32": "INTEGER",
        "int16": "SMALLINT",
        "int8": "TINYINT",
        "float64": "DOUBLE",
        "float32": "FLOAT",
        "object": "VARCHAR",
        "string": "VARCHAR",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
        "timedelta64[ns]": "INTERVAL",
    }

    schema = {}
    for col in df.columns:
        dtype_str = str(df[col].dtype)
        # Get mapped type or default to VARCHAR
        schema[col] = type_mapping.get(dtype_str, "VARCHAR")

    return schema
