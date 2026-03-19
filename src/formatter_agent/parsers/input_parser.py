"""
InputParser - Validates and extracts structured data from pipeline state
=========================================================================

Responsible for:
- Validating required inputs from previous agents
- Extracting and structuring data for formatter processing
- Providing graceful fallbacks for missing data
- Reporting validation errors
"""

import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class InputParser:
    """
    Parse and validate inputs from all agents in the pipeline.

    This class consolidates data from:
    - filter_classifier: filters
    - graphic_classifier: chart specifications
    - analytics_executor: processed data and metadata
    - plotly_generator: chart HTML and configuration
    - insight_generator: generated insights
    """

    def parse(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract and validate all inputs necessary for formatter processing.

        Args:
            state: FormatterState dictionary with inputs from previous agents

        Returns:
            Dictionary with parsed and structured inputs:
            {
                "query": str,
                "chart_type": str,
                "filters": Dict,
                "chart_spec": Dict,
                "data": List[Dict],
                "data_metadata": Dict,
                "insights": List[Dict],
                "plotly_html": str,
                "plotly_file_path": str,
                "validation_errors": List[str]
            }
        """
        errors = []

        # Validate required fields
        if not state.get("query"):
            errors.append("Missing required field: 'query'")
            logger.warning("Missing 'query' in state")

        if not state.get("chart_type"):
            errors.append("Missing required field: 'chart_type'")
            logger.warning("Missing 'chart_type' in state")

        # Extract all components with fallbacks
        parsed = {
            "query": state.get("query", ""),
            "chart_type": state.get("chart_type", ""),
            "filters": self._extract_filters(state),
            "chart_spec": self._extract_chart_spec(state),
            "data": self._extract_data(state),
            "data_metadata": self._extract_metadata(state),
            "insights": self._extract_insights(state),
            "plotly_html": self._extract_plotly_html(state),
            "plotly_file_path": self._extract_plotly_file(state),
            "validation_errors": errors,
            "performance_metrics": state.get("performance_metrics", {}),
            # FASE 4: Pass complete insight_result for unified schema extraction
            "insight_result": state.get("insight_result", {}),
        }

        # Log parsing summary
        logger.info(
            f"InputParser: Parsed state for chart_type='{parsed['chart_type']}', "
            f"data_rows={len(parsed['data'])}, insights={len(parsed['insights'])}, "
            f"errors={len(errors)}"
        )

        return parsed

    def _extract_filters(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract filters from filter_classifier output.

        Args:
            state: Complete state dictionary

        Returns:
            Dictionary of applied filters or empty dict
        """
        filters = state.get("filter_final", {})

        if not filters:
            logger.debug("No filters found in state (filter_final is empty)")
        else:
            logger.debug(f"Extracted filters: {list(filters.keys())}")

        return filters

    def _extract_chart_spec(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract chart specification from graphic_classifier.

        Args:
            state: Complete state dictionary

        Returns:
            Chart specification dict with intent, metrics, dimensions, etc.
        """
        chart_spec = state.get("chart_spec", {})

        if not chart_spec:
            logger.warning("No chart_spec found in state")
        else:
            logger.debug(
                f"Extracted chart_spec: intent='{chart_spec.get('intent', 'N/A')}', "
                f"metrics={len(chart_spec.get('metrics', []))}, "
                f"dimensions={len(chart_spec.get('dimensions', []))}"
            )

        return chart_spec

    def _extract_data(self, state: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract processed data - prioritizes limited data from plotly_result.

        A limitação de categorias ocorre no plotly_generator, e os dados limitados
        são retornados no campo 'limited_data' do plotly_result. Esses são os dados
        que foram REALMENTE plotados no gráfico.

        Prioridade:
        1. plotly_result['limited_data'] - dados após limitação de categorias (CORRETO)
        2. analytics_result['data'] - dados completos originais (FALLBACK)

        Args:
            state: Complete state dictionary

        Returns:
            List of data rows (each row is a dict) - preferencialmente os dados limitados
        """
        # Tentar obter dados limitados do plotly_result (prioridade)
        plotly_result = state.get("plotly_result", {})
        limited_data = plotly_result.get("limited_data")

        if limited_data is not None:
            logger.info(
                f"Using limited_data from plotly_result ({len(limited_data)} rows) - "
                f"these are the actual rows plotted in the chart"
            )
            return limited_data

        # Fallback: usar dados originais do analytics_result
        analytics_result = state.get("analytics_result", {})
        data = analytics_result.get("data", [])

        if not data:
            logger.warning("No data found in analytics_result or plotly_result")
        else:
            logger.debug(
                f"Using original data from analytics_result ({len(data)} rows) - "
                f"no limiting was applied"
            )

        return data

    def _extract_metadata(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract consolidated metadata from analytics_executor.

        Includes:
        - row_count: Number of rows in result
        - full_dataset_totals: Totals from full dataset (before filters)
        - filters_applied: Filters used in query
        - engine_used: Database engine used (DuckDB, etc.)
        - execution_time: Query execution time

        Args:
            state: Complete state dictionary

        Returns:
            Metadata dictionary
        """
        analytics_result = state.get("analytics_result", {})
        analytics_metadata = analytics_result.get("metadata", {})
        analytics_execution = analytics_result.get("execution", {})

        metadata = {
            "row_count": analytics_metadata.get("row_count", 0),
            "full_dataset_totals": analytics_metadata.get("full_dataset_totals", {}),
            "filters_applied": analytics_metadata.get("filters_applied", {}),
            "engine_used": analytics_execution.get("engine", "unknown"),
            "execution_time": analytics_execution.get("execution_time", 0.0),
        }

        logger.debug(
            f"Extracted metadata: row_count={metadata['row_count']}, "
            f"engine={metadata['engine_used']}"
        )

        return metadata

    def _extract_insights(self, state: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract insights from insight_generator.

        Args:
            state: Complete state dictionary

        Returns:
            List of insight dictionaries
        """
        insight_result = state.get("insight_result", {})

        # Use detailed_insights (new unified schema) with fallback to insights (legacy)
        insights = insight_result.get(
            "detailed_insights", insight_result.get("insights", [])
        )

        if not insights:
            logger.warning(
                "No insights found in insight_result (checked detailed_insights and insights)"
            )
        else:
            logger.debug(f"Extracted {len(insights)} insights from insight_result")

        return insights

    def _extract_plotly_html(self, state: Dict[str, Any]) -> str:
        """
        Extract Plotly chart HTML from plotly_generator.

        Args:
            state: Complete state dictionary

        Returns:
            HTML string of the chart or empty string
        """
        plotly_result = state.get("plotly_result", {})
        html = plotly_result.get("html", "")

        if not html:
            logger.warning("No Plotly HTML found in plotly_result")
        else:
            logger.debug(f"Extracted Plotly HTML ({len(html)} chars)")

        return html

    def _extract_plotly_file(self, state: Dict[str, Any]) -> str:
        """
        Extract file path of saved Plotly chart.

        Args:
            state: Complete state dictionary

        Returns:
            File path string or empty string
        """
        plotly_result = state.get("plotly_result", {})
        file_path = plotly_result.get("file_path", "")

        # Convert to string if Path object
        if file_path:
            file_path = str(file_path)
            logger.debug(f"Extracted Plotly file path: {file_path}")
        else:
            logger.debug("No Plotly file path found")

        return file_path
