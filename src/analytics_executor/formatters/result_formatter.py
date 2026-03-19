"""
Result Formatter for Analytics Executor Agent.

This module formats execution results into structured JSON output ready for
Plotly visualization. It combines data, metadata, execution details, and
Plotly configuration into a complete output package.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd
import numpy as np

from src.shared_lib.models.schema import (
    AnalyticsInputSpec,
    AnalyticsOutput,
    ExecutionMetadata,
    PlotlyConfig,
    ExecutionResult,
)

# Legacy import removed in Fase 5 - PlotlyConfigBuilder logic moved to tool handlers
# from src.analytics_executor.formatters.plotly_config_builder import PlotlyConfigBuilder, PlotlyConfigBuilderError
from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class ResultFormatterError(Exception):
    """Exception raised when result formatting fails."""

    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        original_error: Optional[Exception] = None,
    ):
        """
        Initialize result formatter error.

        Args:
            message: Error message
            operation: Operation that failed
            original_error: Original exception
        """
        self.message = message
        self.operation = operation
        self.original_error = original_error
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format error message with context."""
        msg = f"Result formatting failed: {self.message}"
        if self.operation:
            msg = f"[{self.operation}] {msg}"
        if self.original_error:
            msg += f"\nOriginal error: {str(self.original_error)}"
        return msg


class ResultFormatter:
    """
    Formatter for analytics execution results.

    This class transforms execution results into structured JSON output compatible
    with Plotly visualization requirements. It:
    - Converts DataFrames to list of dictionaries
    - Builds comprehensive metadata
    - Adds execution performance details
    - Generates Plotly configuration
    - Validates output against schema
    - Handles data type serialization

    The output follows the AnalyticsOutput schema and is ready for direct use
    by Plotly or other visualization libraries.

    Example:
        >>> formatter = ResultFormatter()
        >>> spec = AnalyticsInputSpec(...)
        >>> result_df = pd.DataFrame({'region': ['A', 'B'], 'sales': [100, 200]})
        >>> output = formatter.format(
        ...     spec=spec,
        ...     result_df=result_df,
        ...     engine="DuckDB",
        ...     execution_time=1.23
        ... )
        >>> print(output['status'])
        'success'
    """

    def __init__(self, validate_schema: bool = True):
        """
        Initialize the result formatter.

        Args:
            validate_schema: Whether to validate output against AnalyticsOutput schema (default: True)
        """
        self.validate_schema = validate_schema
        # Legacy - PlotlyConfigBuilder removed in Fase 5
        # self.config_builder = PlotlyConfigBuilder()

        logger.info(f"ResultFormatter initialized: validate_schema={validate_schema}")

    def format(
        self,
        spec: AnalyticsInputSpec,
        result_df: pd.DataFrame,
        engine: str,
        execution_time: float,
    ) -> Dict[str, Any]:
        """
        Format execution result into structured JSON output.

        This is the main method that orchestrates the formatting process:
        1. Convert DataFrame to list of dictionaries
        2. Build metadata object
        3. Create execution details
        4. Generate Plotly configuration
        5. Validate against schema
        6. Return complete output dictionary

        Args:
            spec: Analytics specification from input
            result_df: Processed DataFrame from execution
            engine: Engine used for execution ('DuckDB' or 'Pandas')
            execution_time: Execution time in seconds

        Returns:
            Dictionary conforming to AnalyticsOutput schema

        Raises:
            ResultFormatterError: If formatting fails
            ValueError: If inputs are invalid

        Example:
            >>> formatter = ResultFormatter()
            >>> df = pd.DataFrame({'col1': [1, 2], 'col2': ['A', 'B']})
            >>> output = formatter.format(
            ...     spec=spec,
            ...     result_df=df,
            ...     engine="DuckDB",
            ...     execution_time=1.5
            ... )
        """
        # Validate inputs
        if spec is None:
            raise ValueError("Specification cannot be None")

        if result_df is None:
            raise ValueError("Result DataFrame cannot be None")

        if not isinstance(result_df, pd.DataFrame):
            raise ValueError(
                f"result_df must be pandas DataFrame, got {type(result_df)}"
            )

        if engine not in ["DuckDB", "Pandas"]:
            raise ValueError(f"Engine must be 'DuckDB' or 'Pandas', got '{engine}'")

        if execution_time < 0:
            raise ValueError(
                f"Execution time must be non-negative, got {execution_time}"
            )

        logger.debug(
            f"Formatting result: {len(result_df)} rows, "
            f"{len(result_df.columns)} columns, engine={engine}"
        )

        try:
            # Step 0: Extract full dataset totals from window function columns (if present)
            full_dataset_totals, full_dataset_count, clean_df = (
                self._extract_full_dataset_totals(result_df)
            )
            if full_dataset_totals:
                logger.debug(f"Extracted full dataset totals: {full_dataset_totals}")

            # Step 1: Convert DataFrame to list of dictionaries (usando DataFrame limpo)
            data = self._convert_dataframe_to_dict(clean_df)
            logger.debug(f"DataFrame converted to {len(data)} records")

            # Step 2: Build metadata (passa totais extraídos)
            metadata = self._build_metadata(
                spec, clean_df, full_dataset_totals, full_dataset_count
            )
            logger.debug("Metadata built")

            # Step 3: Build execution details
            execution = self._build_execution_metadata(
                engine=engine,
                execution_time=execution_time,
                row_count=len(result_df),
                filters=spec.filters,
            )
            logger.debug("Execution metadata built")

            # Step 4: Generate Plotly configuration
            plotly_config = self._build_plotly_config(spec, data)
            logger.debug("Plotly config generated")

            # Step 5: Assemble complete output
            output = {
                "status": "success",
                "data": data,
                "metadata": metadata,
                "execution": execution.model_dump(),
                "plotly_config": plotly_config.model_dump(exclude_none=True),
                "error": None,
            }

            # Step 6: Validate against schema (if enabled)
            if self.validate_schema:
                validated_output = self._validate_output(output)
                logger.debug("Output validated against schema")
                return validated_output
            else:
                logger.debug("Schema validation skipped")
                return output

        except ResultFormatterError:
            # Re-raise our custom errors
            raise

        except Exception as e:
            error_msg = f"Unexpected error during result formatting: {str(e)}"
            logger.error(error_msg)
            raise ResultFormatterError(error_msg, operation="format", original_error=e)

    def format_from_execution_result(
        self, spec: AnalyticsInputSpec, execution_result: ExecutionResult
    ) -> Dict[str, Any]:
        """
        Format result from ExecutionResult dataclass.

        This is a convenience method that extracts fields from ExecutionResult
        and calls the main format() method.

        Args:
            spec: Analytics specification
            execution_result: ExecutionResult from query executor

        Returns:
            Formatted output dictionary

        Example:
            >>> formatter = ResultFormatter()
            >>> exec_result = ExecutionResult(
            ...     data=df,
            ...     engine="DuckDB",
            ...     execution_time=1.5,
            ...     error=None
            ... )
            >>> output = formatter.format_from_execution_result(spec, exec_result)
        """
        return self.format(
            spec=spec,
            result_df=execution_result.data,
            engine=execution_result.engine,
            execution_time=execution_result.execution_time,
        )

    def format_error(
        self,
        error_message: str,
        error_type: str = "ExecutionError",
        execution_time: float = 0.0,
    ) -> Dict[str, Any]:
        """
        Format error response.

        Creates a standardized error output conforming to AnalyticsOutput schema.

        Args:
            error_message: Error message to include
            error_type: Type of error (default: 'ExecutionError')
            execution_time: Time spent before error occurred

        Returns:
            Error output dictionary

        Example:
            >>> formatter = ResultFormatter()
            >>> error_output = formatter.format_error(
            ...     error_message="Column not found",
            ...     error_type="ValidationError"
            ... )
            >>> print(error_output['status'])
            'error'
        """
        logger.debug(f"Formatting error response: {error_type} - {error_message}")

        return {
            "status": "error",
            "data": [],
            "metadata": {},
            "execution": {
                "engine": "None",
                "execution_time": round(execution_time, 3),
                "timestamp": datetime.now().isoformat(),
                "row_count": 0,
                "filters_applied": {},
            },
            "plotly_config": {
                "x": None,
                "y": None,
                "color": None,
                "orientation": None,
                "title": None,
                "markers": None,
            },
            "error": {"type": error_type, "message": error_message},
        }

    def _convert_dataframe_to_dict(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Convert DataFrame to list of dictionaries with type serialization.

        This method handles conversion of pandas/numpy types to JSON-serializable types:
        - numpy int/float -> Python int/float
        - NaN -> None
        - Timestamps -> ISO strings

        Args:
            df: DataFrame to convert

        Returns:
            List of dictionaries with serializable types

        Raises:
            ResultFormatterError: If conversion fails
        """
        try:
            # Convert to records (list of dicts)
            records = df.to_dict(orient="records")

            # Serialize numpy/pandas types
            serialized_records = []
            for record in records:
                serialized_record = {}
                for key, value in record.items():
                    serialized_record[key] = self._serialize_value(value)
                serialized_records.append(serialized_record)

            logger.debug(
                f"Converted DataFrame to {len(serialized_records)} serialized records"
            )

            return serialized_records

        except Exception as e:
            error_msg = f"Error converting DataFrame to dict: {str(e)}"
            logger.error(error_msg)
            raise ResultFormatterError(
                error_msg, operation="convert_dataframe", original_error=e
            )

    def _extract_full_dataset_totals(
        self, result_df: pd.DataFrame
    ) -> tuple[Dict[str, float], Optional[int], pd.DataFrame]:
        """
        Extrai totais globais das colunas de window function e remove essas colunas.

        As window functions adicionadas em base.py criam colunas como:
        - __full_total_{metric_name}: Total global da métrica
        - __full_count: Contagem total de registros

        Este método:
        1. Detecta essas colunas
        2. Extrai os valores (todos iguais por linha devido ao OVER())
        3. Remove as colunas do DataFrame
        4. Retorna os totais e o DataFrame limpo

        Args:
            result_df: DataFrame com possíveis colunas de window function

        Returns:
            Tuple de (full_dataset_totals, full_dataset_count, clean_df):
            - full_dataset_totals: Dict com {metric_name: total_value}
            - full_dataset_count: Total de registros (ou None se não disponível)
            - clean_df: DataFrame sem as colunas auxiliares
        """
        full_dataset_totals = {}
        full_dataset_count = None
        columns_to_drop = []

        # Detectar colunas de window function
        for col in result_df.columns:
            if col.startswith("__full_total_"):
                # Extrair nome da métrica original
                metric_name = col.replace("__full_total_", "")

                # Pegar o valor (todos os valores são iguais devido ao OVER())
                # Usar .iloc[0] para pegar o primeiro valor
                if not result_df.empty:
                    total_value = result_df[col].iloc[0]

                    # Converter para float se necessário
                    if pd.notna(total_value):
                        full_dataset_totals[metric_name] = float(total_value)

                # Marcar coluna para remoção
                columns_to_drop.append(col)

            elif col == "__full_count":
                # Extrair contagem total
                if not result_df.empty:
                    count_value = result_df[col].iloc[0]
                    if pd.notna(count_value):
                        full_dataset_count = int(count_value)

                # Marcar coluna para remoção
                columns_to_drop.append(col)

        # Remover colunas auxiliares
        if columns_to_drop:
            clean_df = result_df.drop(columns=columns_to_drop)
            logger.debug(
                f"Removed {len(columns_to_drop)} window function columns: {columns_to_drop}"
            )
        else:
            clean_df = result_df

        return full_dataset_totals, full_dataset_count, clean_df

    def _serialize_value(self, value: Any) -> Any:
        """
        Serialize a single value to JSON-compatible type.

        Args:
            value: Value to serialize

        Returns:
            JSON-serializable value
        """
        # Handle None
        if value is None:
            return None

        # Handle NaN
        if isinstance(value, (float, np.floating)) and np.isnan(value):
            return None

        # Handle numpy integers
        if isinstance(value, (np.integer, np.int64, np.int32, np.int16, np.int8)):
            return int(value)

        # Handle numpy floats
        if isinstance(value, (np.floating, np.float64, np.float32, np.float16)):
            return float(value)

        # Handle numpy booleans
        if isinstance(value, (np.bool_, bool)):
            return bool(value)

        # Handle timestamps
        if isinstance(value, (pd.Timestamp, np.datetime64)):
            return pd.Timestamp(value).isoformat()

        # Handle numpy strings
        if isinstance(value, np.str_):
            return str(value)

        # Return as-is for standard Python types
        return value

    def _build_metadata(
        self,
        spec: AnalyticsInputSpec,
        result_df: pd.DataFrame,
        full_dataset_totals: Optional[Dict[str, float]] = None,
        full_dataset_count: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Build metadata dictionary with chart and data information.

        Args:
            spec: Analytics specification
            result_df: Result DataFrame (clean, sem colunas de window function)
            full_dataset_totals: Totais globais por métrica (extraídos de window functions)
            full_dataset_count: Contagem total de registros (antes de LIMIT)

        Returns:
            Metadata dictionary
        """
        try:
            metadata = {
                "chart_type": spec.chart_type,
                "dimensions": [dim.name for dim in spec.dimensions],
                # Manter informações completas das métricas (name, alias, aggregation)
                # para permitir mapeamento correto no insight_generator
                "metrics": [
                    {
                        "name": metric.name,
                        "alias": metric.alias if metric.alias else metric.name,
                        "aggregation": metric.aggregation,
                    }
                    for metric in spec.metrics
                ],
                "row_count": len(result_df),
                "filters_applied": spec.filters if spec.filters else {},
            }

            # Add visual config if present
            if spec.visual_config:
                metadata.update(spec.visual_config)

            # Add top_n if specified
            if spec.top_n:
                metadata["top_n"] = spec.top_n

            # Add sort info if specified
            if spec.sort:
                metadata["sort"] = {"by": spec.sort.by, "order": spec.sort.order}

            # Add full dataset totals (para cálculos corretos no insight_generator)
            if full_dataset_totals:
                metadata["full_dataset_totals"] = full_dataset_totals
                logger.debug(
                    f"Added full_dataset_totals to metadata: {list(full_dataset_totals.keys())}"
                )

            if full_dataset_count is not None:
                metadata["full_dataset_count"] = full_dataset_count
                metadata["filtered_dataset_row_count"] = (
                    full_dataset_count  # Alias for consistency
                )
                logger.debug(
                    f"Added full_dataset_count to metadata: {full_dataset_count}"
                )

            return metadata

        except Exception as e:
            error_msg = f"Error building metadata: {str(e)}"
            logger.error(error_msg)
            raise ResultFormatterError(
                error_msg, operation="build_metadata", original_error=e
            )

    def _build_execution_metadata(
        self,
        engine: str,
        execution_time: float,
        row_count: int,
        filters: Dict[str, Any],
    ) -> ExecutionMetadata:
        """
        Build execution metadata object.

        Args:
            engine: Engine used
            execution_time: Execution time in seconds
            row_count: Number of rows returned
            filters: Filters applied

        Returns:
            ExecutionMetadata instance
        """
        try:
            return ExecutionMetadata(
                engine=engine,
                execution_time=round(execution_time, 3),  # Round to 3 decimal places
                timestamp=datetime.now().isoformat(),
                row_count=row_count,
                filters_applied=filters if filters else {},
            )

        except Exception as e:
            error_msg = f"Error building execution metadata: {str(e)}"
            logger.error(error_msg)
            raise ResultFormatterError(
                error_msg, operation="build_execution_metadata", original_error=e
            )

    def _build_plotly_config(
        self, spec: AnalyticsInputSpec, data: List[Dict[str, Any]]
    ) -> PlotlyConfig:
        """
        Build Plotly configuration - DEPRECATED in Fase 5.

        In the new LangGraph architecture, Plotly configs are built
        directly by the tool handlers, not by ResultFormatter.

        This method is kept for backward compatibility but will return
        a simple fallback config.

        Args:
            spec: Analytics specification
            data: Processed data

        Returns:
            PlotlyConfig: Basic Plotly configuration
        """
        logger.warning(
            "ResultFormatter._build_plotly_config is deprecated. "
            "Use tool handlers instead."
        )

        # Simple fallback config - return PlotlyConfig instance
        return PlotlyConfig(
            x=None,
            y=None,
            color=None,
            orientation=None,
            title=spec.get("title", "Chart") if hasattr(spec, "get") else "Chart",
            markers=None,
        )

        # Legacy code (PlotlyConfigBuilder removed in Fase 5)
        # try:
        #     config = self.config_builder.build_config(spec, data)
        #     return config
        # except PlotlyConfigBuilderError as e:
        #     error_msg = f"Error building Plotly config: {str(e)}"
        #     logger.error(error_msg)
        #     raise ResultFormatterError(
        #         error_msg,
        #         operation="build_plotly_config",
        #         original_error=e
        #     )

    def _validate_output(self, output: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate output against AnalyticsOutput schema.

        Args:
            output: Output dictionary to validate

        Returns:
            Validated output dictionary

        Raises:
            ResultFormatterError: If validation fails
        """
        try:
            # Validate using Pydantic model
            validated = AnalyticsOutput(**output)

            # Return as dictionary (excluding None values for cleaner output)
            return validated.model_dump(exclude_none=True)

        except Exception as e:
            error_msg = f"Output validation failed: {str(e)}"
            logger.error(error_msg)
            raise ResultFormatterError(
                error_msg, operation="validate_output", original_error=e
            )

    def get_output_summary(self, output: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get summary of formatted output.

        Useful for logging and debugging.

        Args:
            output: Formatted output dictionary

        Returns:
            Summary dictionary

        Example:
            >>> formatter = ResultFormatter()
            >>> output = formatter.format(...)
            >>> summary = formatter.get_output_summary(output)
            >>> print(summary)
        """
        try:
            return {
                "status": output.get("status"),
                "row_count": len(output.get("data", [])),
                "chart_type": output.get("metadata", {}).get("chart_type"),
                "engine": output.get("execution", {}).get("engine"),
                "execution_time": output.get("execution", {}).get("execution_time"),
                "has_error": output.get("error") is not None,
            }
        except Exception as e:
            logger.warning(f"Error getting output summary: {str(e)}")
            return {"error": str(e)}


def format_analytics_result(
    spec: AnalyticsInputSpec,
    result_df: pd.DataFrame,
    engine: str,
    execution_time: float,
) -> Dict[str, Any]:
    """
    Convenience function to format analytics results.

    This function creates a ResultFormatter instance and formats the result,
    providing a simple one-line interface.

    Args:
        spec: Analytics specification
        result_df: Result DataFrame
        engine: Engine used
        execution_time: Execution time

    Returns:
        Formatted output dictionary

    Raises:
        ResultFormatterError: If formatting fails

    Example:
        >>> from src.analytics_executor.formatters.result_formatter import format_analytics_result
        >>> output = format_analytics_result(
        ...     spec=spec,
        ...     result_df=df,
        ...     engine="DuckDB",
        ...     execution_time=1.5
        ... )
    """
    formatter = ResultFormatter()
    return formatter.format(spec, result_df, engine, execution_time)


def format_error_result(
    error_message: str, error_type: str = "ExecutionError"
) -> Dict[str, Any]:
    """
    Convenience function to format error results.

    Args:
        error_message: Error message
        error_type: Error type

    Returns:
        Error output dictionary

    Example:
        >>> from src.analytics_executor.formatters.result_formatter import format_error_result
        >>> error_output = format_error_result("Something went wrong")
    """
    formatter = ResultFormatter()
    return formatter.format_error(error_message, error_type)
