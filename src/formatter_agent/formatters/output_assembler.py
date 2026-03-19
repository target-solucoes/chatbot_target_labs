"""
OutputAssembler - Assembles final structured JSON output
==========================================================

Responsible for:
- Assembling all formatter components into final JSON structure
- Adding metadata and execution tracking
- Calculating statistics and quality metrics
- Formatting filter descriptions
- Generating chart captions and context
- Ensuring output schema compliance
"""

import logging
import statistics as stats_module
from datetime import datetime
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class OutputAssembler:
    """
    Assembles the final structured JSON output for the formatter agent.

    This class is responsible for combining all processed components
    (executive summary, insights, next steps, data tables) into a
    comprehensive, well-structured JSON output that follows the
    API-first design pattern.

    The output is designed to be consumed by frontend applications
    (Streamlit, web dashboards, etc.) without additional processing.
    """

    def assemble(
        self,
        parsed_inputs: Dict[str, Any],
        executive_summary: Dict[str, Any],
        synthesized_insights: Dict[str, Any],
        next_steps: Dict[str, Any],
        formatted_table: Dict[str, Any],
        execution_times: Dict[str, float],
        agent_tokens: Optional[Dict[str, Any]] = None,
        total_tokens: Optional[Dict[str, int]] = None,
    ) -> Dict[str, Any]:
        """
        Assemble complete output JSON structure.

        Args:
            parsed_inputs: Parsed and validated inputs from InputParser
            executive_summary: Generated title and introduction
            synthesized_insights: LLM-synthesized narrative and findings
            next_steps: Strategic actions and suggested analyses
            formatted_table: Formatted data table (markdown/HTML)
            execution_times: Dictionary of execution times for each component
            agent_tokens: Token usage per agent (for observability)
            total_tokens: Aggregated token usage (for observability)

        Returns:
            Complete structured JSON output following the specification
        """
        logger.info("Assembling final formatter output")

        output = {
            "status": "success",
            "format_version": "1.0.0",
            "timestamp": datetime.now().isoformat(),
            # Section 1: Executive Summary
            "executive_summary": self._assemble_executive_summary(
                executive_summary, parsed_inputs
            ),
            # Section 2: Visualization
            "visualization": self._assemble_visualization(parsed_inputs),
            # Section 3: Insights
            "insights": self._assemble_insights(synthesized_insights),
            # Section 4: Next Steps
            "next_steps": self._assemble_next_steps(next_steps),
            # Section 5: Data
            "data": self._assemble_data(parsed_inputs, formatted_table),
            # Section 6: Metadata
            "metadata": self._assemble_metadata(parsed_inputs, execution_times),
        }

        # Add token tracking information if available
        if agent_tokens is not None:
            output["agent_tokens"] = agent_tokens
        if total_tokens is not None:
            output["total_tokens"] = total_tokens

        logger.info(
            f"Successfully assembled output with {len(output['insights']['key_findings'])} key findings, "
            f"{len(output['next_steps']['items'])} next steps"
        )

        return output

    def _assemble_executive_summary(
        self, executive_summary: Dict[str, Any], parsed_inputs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Assemble executive summary section.

        Args:
            executive_summary: Generated executive summary components
            parsed_inputs: Parsed inputs for context

        Returns:
            Structured executive summary section
        """
        return {
            "title": executive_summary.get("title", ""),
            "subtitle": executive_summary.get(
                "subtitle", parsed_inputs.get("query", "")
            ),
            "introduction": executive_summary.get("introduction", ""),
            "query_original": parsed_inputs.get("query", ""),
            "chart_type": parsed_inputs.get("chart_type", ""),
            "filters_applied": self._format_filters_description(
                parsed_inputs.get("filters", {})
            ),
        }

    def _assemble_visualization(self, parsed_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Assemble visualization section.

        Args:
            parsed_inputs: Parsed inputs containing chart data

        Returns:
            Structured visualization section
        """
        return {
            "chart": {
                "type": parsed_inputs.get("chart_type", ""),
                "html": parsed_inputs.get("plotly_html", ""),
                "file_path": parsed_inputs.get("plotly_file_path", ""),
                "config": parsed_inputs.get("chart_spec", {}),
                "caption": self._generate_chart_caption(parsed_inputs),
            },
            "data_context": {
                "total_records": parsed_inputs.get("data_metadata", {}).get(
                    "row_count", 0
                ),
                "records_displayed": len(parsed_inputs.get("data", [])),
                "aggregation": self._get_aggregation_method(parsed_inputs),
                "date_range": self._extract_date_range(parsed_inputs),
            },
        }

    def _assemble_insights(
        self, synthesized_insights: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Assemble insights section.

        Args:
            synthesized_insights: LLM-synthesized insights

        Returns:
            Structured insights section
        """
        detailed_insights = synthesized_insights.get("detailed_insights", [])

        return {
            "narrative": synthesized_insights.get("narrative", ""),
            "key_findings": synthesized_insights.get("key_findings", []),
            "detailed_insights": detailed_insights,
            "transparency": {
                "formulas_validated": synthesized_insights.get(
                    "transparency_validated", False
                ),
                "transparency_score": self._calculate_transparency_score(
                    detailed_insights
                ),
            },
        }

    def _assemble_next_steps(self, next_steps: Dict[str, Any]) -> Dict[str, Any]:
        """
        Assemble next steps section.

        Args:
            next_steps: Generated next steps (3 direct strategic recommendations)

        Returns:
            Structured next steps section with items list
        """
        return {
            "items": next_steps.get("next_steps", []),
        }

    def _assemble_data(
        self, parsed_inputs: Dict[str, Any], formatted_table: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Assemble data section.

        Args:
            parsed_inputs: Parsed inputs containing raw data
            formatted_table: Formatted table representations

        Returns:
            Structured data section
        """
        raw_data = parsed_inputs.get("data", [])

        return {
            "summary_table": formatted_table,
            "raw_data": raw_data,
            "statistics": self._calculate_statistics(raw_data),
        }

    def _assemble_metadata(
        self, parsed_inputs: Dict[str, Any], execution_times: Dict[str, float]
    ) -> Dict[str, Any]:
        """
        Assemble metadata section including detailed performance metrics.

        Args:
            parsed_inputs: Parsed inputs for context
            execution_times: Execution times for all components

        Returns:
            Structured metadata section with individual agent execution times
        """
        # Import config to get actual model name
        from src.shared_lib.core.config import get_formatter_config

        formatter_config = get_formatter_config()
        actual_model = formatter_config.model
        # Get individual agent execution times from performance_metrics
        performance_metrics = parsed_inputs.get("performance_metrics", {})

        # Calculate total execution time
        filter_time = performance_metrics.get("filter_classifier_execution_time", 0.0)
        classifier_time = performance_metrics.get(
            "graphic_classifier_execution_time", 0.0
        )
        analytics_time = performance_metrics.get(
            "analytics_executor_execution_time", 0.0
        )
        plotly_time = performance_metrics.get("plotly_generator_execution_time", 0.0)
        insight_time = performance_metrics.get("insight_generator_execution_time", 0.0)
        formatter_time = execution_times.get("formatter", 0.0)

        # Total time from all agents
        total_time = (
            filter_time
            + classifier_time
            + analytics_time
            + plotly_time
            + insight_time
            + formatter_time
        )

        return {
            "pipeline_version": "v07_optimization",
            "agents_executed": [
                "filter_classifier",
                "graphic_classifier",
                "analytics_executor",
                "plotly_generator",
                "insight_generator",
                "formatter",
            ],
            # Individual agent execution times
            "execution_time": {
                "filter_classifier": filter_time,
                "graphic_classifier": classifier_time,
                "analytics_executor": analytics_time,
                "plotly_generator": plotly_time,
                "insight_generator": insight_time,
                "formatter": formatter_time,
                "total_execution_time": total_time,
            },
            "formatter_execution_time": formatter_time,
            # FASE 5D: Removed phantom llm_calls_execution_time and llm_calls
            # The formatter no longer makes LLM calls since FASE 4.
            # Previous fields (llm_calls, llm_calls_execution_time) showed
            # misleading data with model names and 0.0s times.
            "data_quality": {
                "completeness": self._calculate_completeness(
                    parsed_inputs.get("data", [])
                ),
                "filters_count": len(parsed_inputs.get("filters", {})),
                "engine_used": parsed_inputs.get("data_metadata", {}).get(
                    "engine_used", "unknown"
                ),
            },
            "total_execution_time": total_time,
        }

    def _format_filters_description(self, filters: Dict) -> Dict[str, str]:
        """
        Convert technical filters into human-readable descriptions.

        Args:
            filters: Dictionary of applied filters

        Returns:
            Dictionary of human-readable filter descriptions
        """
        descriptions = {}

        for key, value in filters.items():
            if isinstance(value, dict):
                # Handle complex filter operators
                if "between" in value:
                    descriptions[key] = f"{value['between'][0]} a {value['between'][1]}"
                elif "operator" in value:
                    descriptions[key] = f"{value['operator']} {value['value']}"
                else:
                    descriptions[key] = str(value)
            elif isinstance(value, list):
                # Handle list values
                descriptions[key] = ", ".join(map(str, value))
            else:
                # Handle simple values
                descriptions[key] = str(value)

        return descriptions

    def _generate_chart_caption(self, parsed_inputs: Dict) -> str:
        """
        Generate descriptive caption for the chart.

        Args:
            parsed_inputs: Parsed inputs containing chart specification

        Returns:
            Human-readable chart caption
        """
        chart_spec = parsed_inputs.get("chart_spec", {})
        metrics = chart_spec.get("metrics", [])
        dimensions = chart_spec.get("dimensions", [])

        # Extract names with fallbacks
        metric_name = metrics[0].get("alias", "Métrica") if metrics else "Métrica"
        dimension_name = (
            dimensions[0].get("alias", "Dimensão") if dimensions else "Dimensão"
        )

        return f"{metric_name} por {dimension_name}"

    def _get_aggregation_method(self, parsed_inputs: Dict) -> str:
        """
        Extract aggregation method from chart specification.

        Args:
            parsed_inputs: Parsed inputs containing chart specification

        Returns:
            Aggregation method (sum, avg, count, etc.)
        """
        chart_spec = parsed_inputs.get("chart_spec", {})
        metrics = chart_spec.get("metrics", [])

        if metrics:
            return metrics[0].get("aggregation", "unknown")

        return "unknown"

    def _extract_date_range(self, parsed_inputs: Dict) -> Optional[str]:
        """
        Extract date range from filters if present.

        Args:
            parsed_inputs: Parsed inputs containing filters

        Returns:
            Date range string or None if not found
        """
        filters = parsed_inputs.get("filters", {})

        for key, value in filters.items():
            if isinstance(value, dict) and "between" in value:
                # Check if this looks like a date filter
                if any(
                    date_keyword in key.lower()
                    for date_keyword in ["data", "date", "dt"]
                ):
                    return f"{value['between'][0]} a {value['between'][1]}"

        return None

    def _calculate_transparency_score(self, insights: List[Dict]) -> float:
        """
        Calculate transparency score based on insights with formulas.

        Args:
            insights: List of detailed insights

        Returns:
            Transparency score (0.0 to 1.0)
        """
        if not insights:
            return 0.0

        with_formulas = sum(1 for insight in insights if self._has_formula(insight))

        return with_formulas / len(insights)

    def _has_formula(self, insight: Dict) -> bool:
        """
        Check if an insight contains a formula.

        Args:
            insight: Insight dictionary

        Returns:
            True if formula is present, False otherwise
        """
        # Check multiple possible locations for formula indicators
        content = insight.get("content", "")
        formula = insight.get("formula", "")

        formula_indicators = ["/", "→", "=", "%", "÷", "×"]

        combined_text = content + " " + formula

        return any(indicator in combined_text for indicator in formula_indicators)

    def _calculate_statistics(self, data: List[Dict]) -> Dict[str, float]:
        """
        Calculate basic statistics from data.

        Args:
            data: List of data records

        Returns:
            Dictionary with statistics (total, mean, median, std)
        """
        if not data:
            return {"total": 0.0, "mean": 0.0, "median": 0.0, "std": 0.0}

        if len(data[0]) < 1:
            return {"total": 0.0, "mean": 0.0, "median": 0.0, "std": 0.0}

        # FASE 4 FIX: Find the numeric metric column
        # Strategy: Iterate through all keys and find the LAST one with numeric values
        # This handles both simple charts (2 cols) and composed charts (3 cols)
        keys = list(data[0].keys())
        metric_key = None

        # Search from the end (metrics are usually last)
        for key in reversed(keys):
            # Check if this column contains numeric values
            sample_values = [
                row.get(key)
                for row in data[: min(5, len(data))]  # Check first 5 rows
                if isinstance(row.get(key), (int, float))
            ]

            if sample_values:
                metric_key = key
                break

        # Fallback: if no numeric column found, try second column (old behavior)
        if metric_key is None:
            if len(keys) > 1:
                metric_key = keys[1]
            else:
                metric_key = keys[0]

        # Extract numeric values from all rows
        values = [
            row[metric_key]
            for row in data
            if isinstance(row.get(metric_key), (int, float))
        ]

        if not values:
            return {"total": 0.0, "mean": 0.0, "median": 0.0, "std": 0.0}

        return {
            "total": sum(values),
            "mean": stats_module.mean(values),
            "median": stats_module.median(values),
            "std": stats_module.stdev(values) if len(values) > 1 else 0.0,
        }

    def _calculate_completeness(self, data: List[Dict]) -> float:
        """
        Calculate data completeness score.

        Args:
            data: List of data records

        Returns:
            Completeness score (0.0 to 1.0)
        """
        if not data:
            return 0.0

        total_cells = len(data) * len(data[0]) if data else 0
        if total_cells == 0:
            return 0.0

        filled_cells = sum(
            1
            for row in data
            for value in row.values()
            if value is not None and value != ""
        )

        return filled_cells / total_cells
