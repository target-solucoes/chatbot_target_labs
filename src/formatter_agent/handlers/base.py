"""
BaseChartHandler - Abstract base class for chart-specific processing
======================================================================

Defines the interface for chart-type-specific handlers that provide:
- Context extraction for LLM prompts
- Chart-specific descriptions and metadata
- Data preview formatting
- Filter description formatting
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class BaseChartHandler(ABC):
    """
    Abstract base class for chart-type-specific handlers.

    Each chart type (bar_horizontal, line, pie, etc.) should have a concrete
    handler that inherits from this class and implements its abstract methods.

    The handler is responsible for extracting chart-specific context that will
    be used to generate tailored LLM prompts for executive summaries, insights,
    and next steps recommendations.

    Attributes:
        chart_type: Identifier for the chart type (e.g., "bar_horizontal")
    """

    def __init__(self, chart_type: str):
        """
        Initialize handler with chart type.

        Args:
            chart_type: String identifier for the chart type
        """
        self.chart_type = chart_type
        logger.debug(
            f"Initialized {self.__class__.__name__} for chart_type='{chart_type}'"
        )

    @abstractmethod
    def get_context_for_llm(self, parsed_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract chart-specific context for LLM prompt generation.

        This method provides the LLM with contextual information about:
        - What this chart type represents
        - What analysis focus is appropriate
        - What types of insights are expected
        - Any chart-specific parameters (top_n, temporal_granularity, etc.)

        Args:
            parsed_inputs: Parsed and validated inputs from InputParser

        Returns:
            Dictionary with chart-specific context:
            {
                "chart_type_description": str,  # Human-readable description
                "analysis_focus": str,           # What to focus analysis on
                "expected_insights": List[str],  # Types of insights to look for
                "dimension_type": str,           # "categorical", "temporal", etc.
                "metric_aggregation": str,       # Aggregation method used
                ... additional chart-specific fields
            }
        """
        pass

    @abstractmethod
    def get_chart_description(self) -> str:
        """
        Get human-readable description of this chart type.

        Used in executive summaries and context descriptions.

        Returns:
            Clear, concise description of what this chart represents

        Example:
            "gráfico de barras horizontais apresentando ranking de categorias por métrica"
        """
        pass

    @abstractmethod
    def format_data_preview(self, data: List[Dict[str, Any]], top_n: int = 3) -> str:
        """
        Format a preview of the data for inclusion in LLM prompts.

        This should provide a concise, readable representation of the data
        that helps the LLM understand the structure and values.

        Args:
            data: List of data rows (each row is a dict)
            top_n: Number of rows to include in preview (default: 3)

        Returns:
            Formatted string representation of data preview

        Example:
            "- Cliente A: 4,500,000.00\\n- Cliente B: 2,800,000.00\\n- Cliente C: 1,360,000.00"
        """
        pass

    def get_filter_description(self, filters: Dict[str, Any]) -> str:
        """
        Convert technical filters to human-readable descriptions.

        This is a concrete method (not abstract) that provides a default
        implementation for filter description. Subclasses can override if needed.

        Args:
            filters: Dictionary of filter key-value pairs

        Returns:
            Human-readable description of filters

        Examples:
            Input:  {"UF_Cliente": "SP", "Ano": 2015}
            Output: "UF_Cliente = SP; Ano = 2015"

            Input:  {"Data": {"between": ["2015-01-01", "2015-12-31"]}}
            Output: "Data entre 2015-01-01 e 2015-12-31"
        """
        if not filters:
            return "Nenhum filtro aplicado"

        descriptions = []

        for key, value in filters.items():
            if isinstance(value, dict):
                # Handle complex filter structures
                if "between" in value:
                    range_vals = value["between"]
                    descriptions.append(
                        f"{key} entre {range_vals[0]} e {range_vals[1]}"
                    )
                elif "operator" in value and "value" in value:
                    operator = value["operator"]
                    val = value["value"]
                    descriptions.append(f"{key} {operator} {val}")
                else:
                    # Fallback for unknown dict structures
                    descriptions.append(f"{key} = {value}")

            elif isinstance(value, list):
                # Handle list of values (IN clause)
                value_str = ", ".join(map(str, value))
                descriptions.append(f"{key} em [{value_str}]")

            else:
                # Simple key-value filter
                descriptions.append(f"{key} = {value}")

        result = "; ".join(descriptions)
        logger.debug(f"Filter description: {result}")
        return result

    def extract_metric_info(self, parsed_inputs: Dict[str, Any]) -> Dict[str, str]:
        """
        Extract metric information from chart specification.

        Utility method to get metric name, alias, and aggregation method.

        Args:
            parsed_inputs: Parsed inputs containing chart_spec

        Returns:
            Dictionary with metric information:
            {
                "name": str,        # Original metric name
                "alias": str,       # Display alias
                "aggregation": str  # Aggregation method (sum, avg, count, etc.)
            }
        """
        chart_spec = parsed_inputs.get("chart_spec", {})
        metrics = chart_spec.get("metrics", [])

        if not metrics:
            logger.warning("No metrics found in chart_spec")
            return {"name": "Unknown", "alias": "Métrica", "aggregation": "unknown"}

        # Get first metric (most handlers use single metric)
        metric = metrics[0]
        return {
            "name": metric.get("name", "Unknown"),
            "alias": metric.get("alias", "Métrica"),
            "aggregation": metric.get("aggregation", "sum"),
        }

    def extract_dimension_info(self, parsed_inputs: Dict[str, Any]) -> Dict[str, str]:
        """
        Extract dimension information from chart specification.

        Utility method to get dimension name and alias.

        Args:
            parsed_inputs: Parsed inputs containing chart_spec

        Returns:
            Dictionary with dimension information:
            {
                "name": str,   # Original dimension name
                "alias": str   # Display alias
            }
        """
        chart_spec = parsed_inputs.get("chart_spec", {})
        dimensions = chart_spec.get("dimensions", [])

        if not dimensions:
            logger.warning("No dimensions found in chart_spec")
            return {"name": "Unknown", "alias": "Dimensão"}

        # Get first dimension (most handlers use single dimension)
        dimension = dimensions[0]
        return {
            "name": dimension.get("name", "Unknown"),
            "alias": dimension.get("alias", "Dimensão"),
        }
