"""
Insight Generator Agent - Main Orchestrator.

This module will contain the InsightGeneratorAgent class that provides
a high-level interface for generating insights.

FASE 1: Placeholder implementation.
FASE 4: Full agent implementation with statistics tracking.
"""

import logging
from typing import Dict, Any, Optional

from .graph.workflow import create_insight_generator_workflow, execute_workflow
from .graph.nodes import initialize_state
from .core.settings import validate_settings

logger = logging.getLogger(__name__)


class InsightGeneratorAgent:
    """
    Main agent for generating strategic insights from analytics results.

    This agent orchestrates the entire insight generation workflow:
    1. Parse input from upstream agents
    2. Calculate chart-type-specific metrics
    3. Build LLM prompts
    4. Generate insights using GPT-5-nano
    5. Validate transparency
    6. Format final output

    FASE 1: Placeholder implementation with basic structure.
    FASE 4: Full implementation with statistics, caching, and error handling.

    Example:
        >>> agent = InsightGeneratorAgent()
        >>> result = agent.generate(chart_spec, analytics_result)
        >>> print(result["insights"])
    """

    def __init__(self, setup_logs: bool = True, validate: bool = True):
        """
        Initialize the Insight Generator Agent.

        Args:
            setup_logs: Whether to configure logging
            validate: Whether to validate settings on initialization

        Raises:
            ValueError: If settings validation fails
            FileNotFoundError: If required files not found

        FASE 1: Basic initialization.
        FASE 4: Add component initialization and workflow caching.
        """
        if setup_logs:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            )

        logger.info("Initializing InsightGeneratorAgent")

        # Validate settings
        if validate:
            try:
                validate_settings()
                logger.info("Settings validation passed")
            except Exception as e:
                logger.error(f"Settings validation failed: {e}")
                raise

        # FASE 4: Initialize components
        # - Create workflow instance
        # - Initialize metric cache
        # - Initialize calculators registry
        # - Load LLM instance

        # Statistics tracking (FASE 4)
        self._query_count = 0
        self._error_count = 0
        self._cache_hits = 0

        logger.info("InsightGeneratorAgent initialization complete")

    def generate(
        self,
        chart_spec: Dict[str, Any],
        analytics_result: Dict[str, Any],
        verbose: bool = False,
    ) -> Dict[str, Any]:
        """
        Generate insights from chart specification and analytics results.

        Args:
            chart_spec: Chart specification from graphic_classifier
            analytics_result: Analytics output from analytics_executor
            verbose: Enable verbose logging

        Returns:
            Dictionary with insights and metadata:
                {
                    "status": "success" | "error",
                    "chart_type": str,
                    "insights": List[Dict],
                    "metadata": Dict,
                    "error": Optional[str]
                }

        FASE 1: Placeholder that returns minimal output.
        FASE 4: Full implementation using workflow execution.

        Example:
            >>> result = agent.generate(chart_spec, analytics_result)
            >>> if result["status"] == "success":
            ...     for insight in result["insights"]:
            ...         print(insight["content"])
        """
        self._query_count += 1
        logger.info(f"Processing insight generation #{self._query_count}")

        try:
            # FASE 4: Use actual workflow execution
            result = execute_workflow(chart_spec, analytics_result, verbose)

            # Use detailed_insights (new schema) with fallback to insights (legacy)
            insights_count = len(
                result.get("detailed_insights", result.get("insights", []))
            )
            logger.info(f"Insight generation complete: {insights_count} insights")

            return result

        except Exception as e:
            self._error_count += 1
            logger.error(f"Error generating insights: {e}")
            return self._format_error_response(str(e))

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get agent usage statistics.

        Returns:
            Dictionary with statistics:
                - total_queries: Total number of queries processed
                - error_count: Number of errors encountered
                - success_rate: Percentage of successful queries
                - cache_hits: Number of cache hits (FASE 4)

        FASE 1: Basic statistics.
        FASE 4: Add cache statistics and performance metrics.
        """
        success_count = self._query_count - self._error_count
        success_rate = (
            (success_count / self._query_count * 100) if self._query_count > 0 else 0.0
        )

        return {
            "total_queries": self._query_count,
            "error_count": self._error_count,
            "success_count": success_count,
            "success_rate": success_rate,
            "cache_hits": self._cache_hits,  # FASE 4
        }

    def _format_error_response(self, error_message: str) -> Dict[str, Any]:
        """
        Format error response.

        Args:
            error_message: Error message string

        Returns:
            Standardized error response dictionary
        """
        return {
            "status": "error",
            "chart_type": "unknown",
            "insights": [],
            "metadata": {
                "calculation_time": 0.0,
                "metrics_count": 0,
                "llm_model": "gpt-5-nano-2025-08-07",
                "timestamp": "",
                "transparency_validated": False,
            },
            "error": error_message,
        }


def create_agent(**kwargs) -> InsightGeneratorAgent:
    """
    Convenience function to create an InsightGeneratorAgent.

    Args:
        **kwargs: Arguments to pass to InsightGeneratorAgent constructor

    Returns:
        Initialized InsightGeneratorAgent instance

    Example:
        >>> agent = create_agent(setup_logs=True)
    """
    return InsightGeneratorAgent(**kwargs)
