"""
Analytics Executor Agent - Phase 8 Main Orchestrator.

This module implements the Analytics Executor Agent, which integrates all modules.
The agent:
- Receives JSON specifications from graphic_classifier_agent (Phase 1)
- Parses and validates input (Phase 3)
- Loads and validates data (Phase 4)
- Builds and executes queries with fallback (Phases 5-6)
- Formats results for Plotly visualization (Phase 7)

The agent provides centralized error handling, logging, and performance tracking.
"""

import logging
import time
from typing import Dict, Any, Union, Optional
from pathlib import Path
from datetime import datetime

from src.shared_lib.models.schema import (
    AnalyticsInputSpec,
    AnalyticsOutput,
    ChartOutput,
    ExecutionResult,
)
from src.analytics_executor.parsers.json_parser import JSONParser, JSONParsingError
from src.analytics_executor.data.data_loader import DataLoader
from src.analytics_executor.data.column_validator import (
    ColumnValidator,
    ColumnValidationError,
)
from src.analytics_executor.execution.query_executor import (
    QueryExecutor,
    QueryExecutorError,
)
from src.analytics_executor.execution.post_processor import PostProcessor
from src.analytics_executor.formatters.result_formatter import (
    ResultFormatter,
    ResultFormatterError,
)
from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class AnalyticsExecutorAgent:
    """
    Analytics Executor Agent - Main Orchestrator for Phase 8.

    This agent orchestrates the complete analytics execution pipeline,
    integrating all components from Phases 3-7:

    Pipeline:
    1. Parse JSON specification (Phase 3)
    2. Load data source (Phase 4)
    3. Validate columns exist (Phase 4)
    4. Build and execute query with fallback (Phases 5-6)
    5. Post-process results (Phase 6)
    6. Format output for Plotly (Phase 7)

    Features:
    - Seamless integration with Phase 1 (graphic_classifier_agent)
    - Automatic engine fallback (DuckDB → Pandas)
    - Comprehensive error handling and logging
    - Performance tracking
    - Data validation at each step

    Example:
        >>> # Standalone usage
        >>> agent = AnalyticsExecutorAgent()
        >>> json_spec = {
        ...     "chart_type": "bar_horizontal",
        ...     "metrics": [{"name": "sales", "aggregation": "sum"}],
        ...     "dimensions": [{"name": "region"}],
        ...     "data_source": "data/sales.parquet"
        ... }
        >>> result = agent.execute(json_spec)
        >>> print(result['status'])
        'success'

        >>> # Integration with Phase 1
        >>> from src.graphic_classifier.agent import GraphicClassifierAgent
        >>> classifier = GraphicClassifierAgent()
        >>> chart_output = classifier.classify("top 5 customers by sales")
        >>> executor = AnalyticsExecutorAgent()
        >>> result = executor.execute_from_chart_output(chart_output)
    """

    def __init__(
        self,
        data_path: Optional[Union[str, Path]] = None,
        cache_size: int = 5,
        validate_schema: bool = True,
    ):
        """
        Initialize the Analytics Executor Agent.

        Args:
            data_path: Default path to data source. If None, must be provided in execute()
            cache_size: Number of datasets to cache in memory (default: 5)
            validate_schema: Whether to validate output against schema (default: True)
        """
        self.default_data_path = Path(data_path) if data_path else None

        # Initialize components
        self.json_parser = JSONParser()
        self.data_loader = DataLoader(cache_size=cache_size)
        self.column_validator = ColumnValidator()
        self.query_executor = QueryExecutor()
        self.post_processor = PostProcessor()
        self.result_formatter = ResultFormatter(validate_schema=validate_schema)

        # Performance tracking
        self._total_executions = 0
        self._successful_executions = 0
        self._failed_executions = 0
        self._total_execution_time = 0.0

        logger.info(
            f"AnalyticsExecutorAgent initialized: "
            f"default_data_path={self.default_data_path}, "
            f"cache_size={cache_size}, "
            f"validate_schema={validate_schema}"
        )

    def execute(
        self, json_spec: Dict[str, Any], data_path: Optional[Union[str, Path]] = None
    ) -> Dict[str, Any]:
        """
        Execute analytics based on JSON specification.

        This is the main entry point for the agent. It orchestrates the complete
        execution pipeline from parsing to formatting.

        Args:
            json_spec: JSON specification (from graphic_classifier_agent or direct)
            data_path: Path to data source (overrides default_data_path if provided)

        Returns:
            Dictionary conforming to AnalyticsOutput schema with:
            - status: "success" or "error"
            - data: Processed data ready for Plotly
            - metadata: Chart and execution metadata
            - execution: Performance metrics
            - plotly_config: Plotly configuration
            - error: Error details (if status is "error")

        Example:
            >>> agent = AnalyticsExecutorAgent()
            >>> spec = {
            ...     "chart_type": "bar_horizontal",
            ...     "metrics": [{"name": "Valor_Vendido", "aggregation": "sum"}],
            ...     "dimensions": [{"name": "Estado"}],
            ...     "filters": {"Ano": 2015},
            ...     "top_n": 5,
            ...     "data_source": "data/sales.parquet"
            ... }
            >>> result = agent.execute(spec)
            >>> print(f"Status: {result['status']}")
            >>> print(f"Engine: {result['execution']['engine']}")
            >>> print(f"Rows: {len(result['data'])}")
        """
        start_time = time.perf_counter()
        self._total_executions += 1

        try:
            logger.info(f"Starting analytics execution #{self._total_executions}")

            # Step 1: Parse and validate JSON specification
            logger.info("Step 1/6: Parsing JSON specification")
            spec = self._parse_specification(json_spec)
            logger.info(
                f"Specification parsed: chart_type={spec.chart_type}, "
                f"metrics={len(spec.metrics)}, dimensions={len(spec.dimensions)}, "
                f"filters={len(spec.filters)}"
            )

            # Step 2: Resolve data path
            resolved_data_path = self._resolve_data_path(spec, data_path)
            logger.info(f"Step 2/6: Data path resolved: {resolved_data_path}")

            # Step 3: Load data
            logger.info("Step 3/6: Loading data")
            df = self.data_loader.load(resolved_data_path)
            logger.info(f"Data loaded: {len(df)} rows, {len(df.columns)} columns")

            # Step 4: Validate columns
            logger.info("Step 4/6: Validating columns")
            self.column_validator.validate_columns_exist(df, spec)
            logger.info("Column validation passed")

            # Step 5: Execute query
            logger.info("Step 5/6: Executing query")
            execution_result = self.query_executor.execute(spec, df)
            logger.info(
                f"Query executed: engine={execution_result.engine}, "
                f"time={execution_result.execution_time:.3f}s, "
                f"rows={len(execution_result.data)}"
            )

            # Step 5.5: Post-process results
            logger.info("Step 5.5/6: Post-processing results")
            processed_df = self.post_processor.process(execution_result.data, spec)
            logger.info(f"Post-processing complete: {len(processed_df)} rows")

            # Update execution result with processed data
            execution_result.data = processed_df

            # Step 6: Format output
            logger.info("Step 6/6: Formatting output")
            output = self.result_formatter.format_from_execution_result(
                spec, execution_result
            )
            logger.info("Output formatted successfully")

            # Track success
            self._successful_executions += 1
            total_time = time.perf_counter() - start_time
            self._total_execution_time += total_time

            logger.info(
                f"Analytics execution completed successfully in {total_time:.3f}s. "
                f"Engine: {execution_result.engine}, Rows: {len(processed_df)}"
            )

            return output

        except JSONParsingError as e:
            # JSON parsing error
            error_msg = f"JSON parsing failed: {str(e)}"
            logger.error(error_msg)
            return self._format_error_response(
                error_msg, "JSONParsingError", time.perf_counter() - start_time
            )

        except FileNotFoundError as e:
            # Data file not found
            error_msg = f"Data file not found: {str(e)}"
            logger.error(error_msg)
            return self._format_error_response(
                error_msg, "FileNotFoundError", time.perf_counter() - start_time
            )

        except ColumnValidationError as e:
            # Column validation error
            error_msg = f"Column validation failed: {str(e)}"
            logger.error(error_msg)
            return self._format_error_response(
                error_msg, "ColumnValidationError", time.perf_counter() - start_time
            )

        except QueryExecutorError as e:
            # Query execution error (both engines failed)
            error_msg = f"Query execution failed: {str(e)}"
            logger.error(error_msg)
            return self._format_error_response(
                error_msg, "QueryExecutorError", time.perf_counter() - start_time
            )

        except ResultFormatterError as e:
            # Result formatting error
            error_msg = f"Result formatting failed: {str(e)}"
            logger.error(error_msg)
            return self._format_error_response(
                error_msg, "ResultFormatterError", time.perf_counter() - start_time
            )

        except Exception as e:
            # Unexpected error
            error_msg = f"Unexpected error during execution: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return self._format_error_response(
                error_msg, "UnexpectedError", time.perf_counter() - start_time
            )

        finally:
            # Always increment failed counter if not successful
            if self._total_executions != self._successful_executions:
                self._failed_executions += 1

    def execute_from_chart_output(
        self,
        chart_output: Union[ChartOutput, Dict[str, Any]],
        data_path: Optional[Union[str, Path]] = None,
    ) -> Dict[str, Any]:
        """
        Execute analytics from ChartOutput (Phase 1 output).

        This method provides seamless integration with the graphic_classifier_agent,
        automatically converting ChartOutput to AnalyticsInputSpec.

        Args:
            chart_output: ChartOutput from Phase 1 or dict representation
            data_path: Optional path to data source (overrides data_source in chart_output)

        Returns:
            Dictionary conforming to AnalyticsOutput schema

        Example:
            >>> from src.graphic_classifier.agent import GraphicClassifierAgent
            >>> classifier = GraphicClassifierAgent()
            >>> chart_out = classifier.classify("top 5 customers by revenue")
            >>>
            >>> executor = AnalyticsExecutorAgent()
            >>> result = executor.execute_from_chart_output(chart_out)
            >>> print(result['status'])
        """
        logger.info("Executing from ChartOutput (Phase 1 integration)")

        try:
            # Convert to ChartOutput if dict
            if isinstance(chart_output, dict):
                chart_output = ChartOutput(**chart_output)

            # Convert ChartOutput to AnalyticsInputSpec
            spec = AnalyticsInputSpec.from_chart_output(chart_output)

            # Convert to dict for execute()
            spec_dict = spec.model_dump()

            # Execute with converted spec
            return self.execute(spec_dict, data_path=data_path)

        except Exception as e:
            error_msg = f"Failed to execute from ChartOutput: {str(e)}"
            logger.error(error_msg)
            return self._format_error_response(error_msg, "ConversionError", 0.0)

    def _parse_specification(self, json_spec: Dict[str, Any]) -> AnalyticsInputSpec:
        """
        Parse and validate JSON specification.

        Args:
            json_spec: Raw JSON dictionary

        Returns:
            Validated AnalyticsInputSpec

        Raises:
            JSONParsingError: If parsing fails
        """
        return self.json_parser.parse(json_spec)

    def _resolve_data_path(
        self, spec: AnalyticsInputSpec, data_path: Optional[Union[str, Path]]
    ) -> Path:
        """
        Resolve data path from specification or parameters.

        Priority:
        1. Explicitly provided data_path parameter
        2. data_source in specification (as absolute/relative path)
        3. default_data_path set in __init__

        Args:
            spec: Parsed specification
            data_path: Explicitly provided data path

        Returns:
            Resolved Path to data source

        Raises:
            ValueError: If data path cannot be resolved
        """
        # Priority 1: Explicit parameter
        if data_path:
            return Path(data_path)

        # Priority 2: Specification data_source
        if spec.data_source:
            # Check if it's a file path (has extension)
            if "." in spec.data_source:
                return Path(spec.data_source)

            # Otherwise, try to construct path with default location
            if self.default_data_path:
                # If default_data_path is a directory, append data_source as filename
                if self.default_data_path.is_dir():
                    return self.default_data_path / f"{spec.data_source}.parquet"
                # If it's a file, use it as-is
                return self.default_data_path

            # Try default dataset location
            default_dataset_path = Path("data/datasets") / f"{spec.data_source}.parquet"
            if default_dataset_path.exists():
                return default_dataset_path

        # Priority 3: Default data path
        if self.default_data_path:
            return self.default_data_path

        # No path could be resolved
        raise ValueError(
            f"Could not resolve data path. "
            f"Provide data_path parameter, set data_source in specification, "
            f"or initialize agent with default_data_path. "
            f"Spec data_source: {spec.data_source}"
        )

    def _format_error_response(
        self, error_message: str, error_type: str, execution_time: float
    ) -> Dict[str, Any]:
        """
        Format error response.

        Args:
            error_message: Error message
            error_type: Type of error
            execution_time: Time spent before error

        Returns:
            Error output dictionary
        """
        self._failed_executions += 1
        return self.result_formatter.format_error(
            error_message=error_message,
            error_type=error_type,
            execution_time=execution_time,
        )

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get execution statistics for this agent instance.

        Returns:
            Dictionary with statistics:
            - total_executions: Total number of execution attempts
            - successful_executions: Number of successful executions
            - failed_executions: Number of failed executions
            - success_rate: Success rate as percentage
            - avg_execution_time: Average execution time in seconds
            - total_execution_time: Total time spent executing

        Example:
            >>> agent = AnalyticsExecutorAgent()
            >>> # ... execute some queries ...
            >>> stats = agent.get_statistics()
            >>> print(f"Success rate: {stats['success_rate']:.1f}%")
            >>> print(f"Avg time: {stats['avg_execution_time']:.3f}s")
        """
        success_rate = (
            (self._successful_executions / self._total_executions * 100)
            if self._total_executions > 0
            else 0.0
        )

        avg_time = (
            (self._total_execution_time / self._successful_executions)
            if self._successful_executions > 0
            else 0.0
        )

        return {
            "total_executions": self._total_executions,
            "successful_executions": self._successful_executions,
            "failed_executions": self._failed_executions,
            "success_rate": round(success_rate, 2),
            "avg_execution_time": round(avg_time, 3),
            "total_execution_time": round(self._total_execution_time, 3),
        }

    def clear_cache(self) -> None:
        """
        Clear the data loader cache.

        This can be useful to free memory or force reload of updated files.

        Example:
            >>> agent = AnalyticsExecutorAgent()
            >>> agent.execute(spec1)  # Loads and caches data
            >>> agent.execute(spec1)  # Uses cached data
            >>> agent.clear_cache()
            >>> agent.execute(spec1)  # Reloads data
        """
        self.data_loader.clear_cache()
        logger.info("Data loader cache cleared")

    def validate_specification(
        self, json_spec: Dict[str, Any]
    ) -> tuple[bool, Optional[str]]:
        """
        Validate specification without executing.

        This method can be used to check if a specification is valid before
        attempting execution.

        Args:
            json_spec: JSON specification to validate

        Returns:
            Tuple of (is_valid, error_message)
            - is_valid: True if specification is valid
            - error_message: Error message if invalid, None if valid

        Example:
            >>> agent = AnalyticsExecutorAgent()
            >>> spec = {"chart_type": "bar", "metrics": [...], ...}
            >>> is_valid, error = agent.validate_specification(spec)
            >>> if not is_valid:
            ...     print(f"Invalid specification: {error}")
        """
        try:
            self.json_parser.parse(json_spec)
            return True, None
        except JSONParsingError as e:
            return False, str(e)
        except Exception as e:
            return False, f"Unexpected validation error: {str(e)}"

    def __repr__(self) -> str:
        """String representation of the agent."""
        return (
            f"AnalyticsExecutorAgent("
            f"default_data_path={self.default_data_path}, "
            f"executions={self._total_executions}, "
            f"success_rate={self.get_statistics()['success_rate']:.1f}%"
            f")"
        )


def execute_analytics(
    json_spec: Dict[str, Any], data_path: Optional[Union[str, Path]] = None
) -> Dict[str, Any]:
    """
    Convenience function to execute analytics with default agent.

    This function creates an AnalyticsExecutorAgent instance and executes
    the query, providing a simple one-line interface.

    Args:
        json_spec: JSON specification
        data_path: Optional path to data source

    Returns:
        Dictionary conforming to AnalyticsOutput schema

    Example:
        >>> from src.analytics_executor.agent import execute_analytics
        >>> spec = {
        ...     "chart_type": "bar_horizontal",
        ...     "metrics": [{"name": "sales", "aggregation": "sum"}],
        ...     "dimensions": [{"name": "region"}],
        ...     "data_source": "data/sales.parquet"
        ... }
        >>> result = execute_analytics(spec)
        >>> print(result['status'])
    """
    agent = AnalyticsExecutorAgent(data_path=data_path)
    return agent.execute(json_spec)
