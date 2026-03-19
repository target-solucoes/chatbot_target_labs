"""
Plotly Generator Agent - Main Orchestrator

This agent is the fourth component in the multiagent pipeline, responsible for
transforming structured outputs from graphical_classifier and analytics_executor
into interactive Plotly visualizations.

Pipeline Position:
    User Query
        ↓
    [Agent 1: filter_classifier]
        ↓
    [Agent 2: graphical_classifier]
        ↓
    [Agent 3: analytics_executor]
        ↓
    [Agent 4: plotly_generator] ← THIS AGENT
        ↓
    Plotly HTML/PNG Output

Features:
- 8 specialized generators (one per chart type)
- Zero hardcoding - fully dynamic based on specs
- LangGraph workflow orchestration
- Comprehensive error handling
- Performance tracking and statistics
- File saving (HTML/PNG)

Author: Claude Code
Date: 2025-11-12
Version: 1.0
"""

import logging
import time
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
import plotly.graph_objects as go

from src.plotly_generator.generators.router import GeneratorRouter
from src.plotly_generator.utils.plot_styler import PlotStyler
from src.plotly_generator.utils.file_saver import FileSaver
from src.plotly_generator.adapters.input_adapter import InputAdapter
from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class PlotlyGeneratorAgent:
    """
    Main agent for generating interactive Plotly visualizations.

    This agent receives outputs from graphical_classifier and analytics_executor
    and generates interactive charts ready for consumption by web interfaces or reports.

    Supported Chart Types:
        - bar_horizontal
        - bar_vertical
        - bar_vertical_composed
        - bar_vertical_stacked
        - line
        - line_composed
        - pie
        - histogram

    Example Usage:
        >>> agent = PlotlyGeneratorAgent()
        >>> result = agent.generate(chart_spec, analytics_result)
        >>> if result['status'] == 'success':
        >>>     result['figure'].show()
        >>>     print(f"Saved to: {result['file_path']}")
    """

    def __init__(
        self,
        output_dir: Optional[Path] = None,
        save_html: bool = True,
        save_png: bool = False,
    ):
        """
        Initialize the Plotly Generator Agent.

        Args:
            output_dir: Directory to save generated charts (default: generated_plots/)
            save_html: If True, saves charts as interactive HTML
            save_png: If True, saves charts as static PNG (requires kaleido)
        """
        self.output_dir = output_dir or Path("src/plotly_generator/generated_plots")
        self.save_html = save_html
        self.save_png = save_png

        # Initialize components
        self.styler = PlotStyler()
        self.router = GeneratorRouter(self.styler)
        self.adapter = InputAdapter()
        self.file_saver = FileSaver(self.output_dir)

        # Statistics tracking
        self._stats = {
            "total_generations": 0,
            "successful_generations": 0,
            "failed_generations": 0,
            "total_render_time": 0.0,
            "average_render_time": 0.0,
            "charts_by_type": {},
        }

        logger.info(
            f"PlotlyGeneratorAgent initialized (output_dir={self.output_dir}, "
            f"save_html={save_html}, save_png={save_png})"
        )

    def generate(
        self, chart_spec: Dict[str, Any], analytics_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate a Plotly chart from classifier and executor outputs.

        This is the main public interface of the agent. It orchestrates the complete
        workflow: validation → adaptation → generation → saving.

        Args:
            chart_spec: Output from graphical_classifier (ChartOutput schema)
                Required fields:
                - chart_type: str (one of the 8 supported types)
                - metrics: List[MetricSpec]
                - dimensions: List[DimensionSpec]
                Optional fields:
                - title: str
                - visual: VisualConfig (palette, show_values, etc.)

            analytics_result: Output from analytics_executor (AnalyticsOutput schema)
                Required fields:
                - status: str (must be "success")
                - data: List[Dict[str, Any]] (processed data ready for plotting)

        Returns:
            Dict containing:
            - status: "success" | "error"
            - chart_type: str
            - figure: plotly.graph_objects.Figure (if success)
            - html: str (if success and save_html=True)
            - file_path: Path (if success and files were saved)
            - config: Dict (final config used)
            - metadata: Dict (rows_plotted, render_time, generator_used)
            - error: Dict (if status="error")

        Raises:
            ValueError: If inputs are invalid (caught and returned in error dict)
        """
        start_time = time.perf_counter()
        self._stats["total_generations"] += 1

        logger.info("=" * 80)
        logger.info("PLOTLY GENERATOR AGENT - Starting Chart Generation")
        logger.info("=" * 80)
        logger.info(f"Chart Type: {chart_spec.get('chart_type')}")
        logger.info(f"Title: {chart_spec.get('title', 'N/A')}")
        logger.info(f"Metrics: {len(chart_spec.get('metrics', []))} metric(s)")
        logger.info(f"Dimensions: {len(chart_spec.get('dimensions', []))} dimension(s)")
        logger.info(f"Data Rows: {len(analytics_result.get('data', []))}")
        logger.info(f"Analytics Status: {analytics_result.get('status')}")

        try:
            # Step 1: Validate inputs
            logger.info("\n[STEP 1/6] Validating inputs...")
            is_valid, error_message = self.validate_inputs(chart_spec, analytics_result)
            if not is_valid:
                logger.error(f"❌ Input validation failed: {error_message}")
                return self._create_error_response(
                    chart_spec.get("chart_type", "unknown"),
                    "ValidationError",
                    error_message,
                    start_time,
                )
            logger.info("✓ Input validation passed")

            # Step 2: Adapt inputs
            logger.info("\n[STEP 2/6] Adapting inputs to plot parameters...")
            try:
                plot_params = self.adapter.adapt(chart_spec, analytics_result)
                logger.info("✓ Input adaptation successful")
                logger.info(f"  - Chart Type: {plot_params.chart_type}")
                logger.info(f"  - Title: {plot_params.title}")
                logger.info(f"  - Data Shape: {plot_params.data.shape}")
                logger.info(f"  - Color Palette: {plot_params.palette}")
                logger.info(f"  - Show Values: {plot_params.show_values}")
                logger.info("")
            except Exception as e:
                logger.error(f"❌ Input adaptation failed: {e}", exc_info=True)
                return self._create_error_response(
                    chart_spec.get("chart_type", "unknown"),
                    "AdaptationError",
                    f"Failed to adapt inputs: {str(e)}",
                    start_time,
                )

            # Step 3: Select and execute generator
            chart_type = chart_spec["chart_type"]
            logger.info(f"\n[STEP 3/6] Selecting generator for '{chart_type}'...")
            try:
                generator = self.router.get_generator(chart_type)
                generator_name = generator.__class__.__name__
                logger.info(f"✓ Generator selected: {generator_name}")
            except ValueError as e:
                logger.error(f"❌ Generator selection failed: {e}")
                return self._create_error_response(
                    chart_type, "UnsupportedChartTypeError", str(e), start_time
                )

            # Step 4: Validate data for specific generator
            logger.info(f"\n[STEP 4/6] Validating data for {chart_type}...")
            # Convert plot_params.data DataFrame to list of dicts for validation
            data_for_generator = plot_params.data.to_dict("records")
            try:
                generator.validate(chart_spec, data_for_generator)
                logger.info("✓ Generator-specific validation passed")
            except ValueError as e:
                logger.error(f"❌ Generator validation failed: {e}")
                return self._create_error_response(
                    chart_type,
                    "DataValidationError",
                    f"Data validation failed for {chart_type}: {str(e)}",
                    start_time,
                )

            # Step 5: Generate figure
            logger.info(f"\n[STEP 5/6] Generating Plotly figure...")
            generation_start = time.perf_counter()
            try:
                # Use processed data from plot_params (includes converted temporal values)
                figure = generator.generate(chart_spec, data_for_generator)
                generation_time = time.perf_counter() - generation_start
                logger.info(f"✓ Figure generation successful ({generation_time:.3f}s)")
                logger.info(f"  - Traces: {len(figure.data)}")
                logger.info(
                    f"  - Layout Title: {figure.layout.title.text if figure.layout.title else 'N/A'}"
                )
            except Exception as e:
                logger.error(f"❌ Figure generation failed: {e}", exc_info=True)
                return self._create_error_response(
                    chart_type,
                    "GenerationError",
                    f"Failed to generate figure: {str(e)}",
                    start_time,
                )

            # Step 6: Save files (if configured)
            logger.info(f"\n[STEP 6/6] Saving output files...")
            file_path = None
            html_content = None

            if self.save_html:
                try:
                    file_path = self.file_saver.save_html(figure)
                    html_content = figure.to_html(include_plotlyjs="cdn")
                    logger.info(f"✓ Chart saved as HTML: {file_path}")
                    logger.info(f"  - File size: {len(html_content)} bytes")
                except Exception as e:
                    logger.warning(f"⚠ Failed to save HTML: {e}")
                    # Non-fatal error - continue

            if self.save_png:
                try:
                    png_path = self.file_saver.save_png(figure)
                    logger.info(f"✓ Chart saved as PNG: {png_path}")
                except Exception as e:
                    logger.warning(f"⚠ Failed to save PNG: {e}")
                    # Non-fatal error - continue

            # Calculate metrics
            render_time = time.perf_counter() - start_time
            rows_plotted = len(analytics_result["data"])

            # Update statistics
            self._update_stats(chart_type, render_time, success=True)

            logger.info("\n" + "=" * 80)
            logger.info("PLOTLY GENERATOR AGENT - Chart Generation Successful")
            logger.info("=" * 80)
            logger.info(f"Chart Type: {chart_type}")
            logger.info(f"Generator Used: {generator_name}")
            logger.info(f"Rows Plotted: {rows_plotted}")
            logger.info(f"Total Render Time: {render_time:.3f}s")
            logger.info(f"Output File: {file_path or 'Not saved'}")
            logger.info("=" * 80)

            logger.info(
                f"Chart generation successful: {chart_type} "
                f"({rows_plotted} rows, {render_time:.3f}s)"
            )

            # Step 7: Collect limited data if category limiting was applied
            limited_data = generator.get_last_limited_data()
            limit_metadata = generator.get_last_limit_metadata()

            # Step 8: Create success response
            metadata = {
                "rows_plotted": rows_plotted,
                "render_time": render_time,
                "generator_used": generator_name,
                "output_dir": str(self.output_dir),
            }

            # Adicionar informações de limitação se disponíveis
            if limit_metadata:
                metadata["category_limiting"] = limit_metadata
                logger.info(
                    f"Category limiting applied: {limit_metadata['original_count']} → "
                    f"{limit_metadata['limited_count']} categories"
                )

            response = {
                "status": "success",
                "chart_type": chart_type,
                "figure": figure,
                "html": html_content,
                "file_path": str(file_path) if file_path else None,
                "config": self._extract_config(chart_spec),
                "metadata": metadata,
            }

            # Adicionar dados limitados ao response se disponíveis
            if limited_data is not None:
                response["limited_data"] = limited_data
                logger.info(
                    f"Included limited_data in response ({len(limited_data)} rows)"
                )

            return response

        except Exception as e:
            # Catch-all for unexpected errors
            logger.error(f"Unexpected error in generate(): {e}", exc_info=True)
            return self._create_error_response(
                chart_spec.get("chart_type", "unknown"),
                "UnexpectedError",
                f"Unexpected error: {str(e)}",
                start_time,
            )

    def validate_inputs(
        self, chart_spec: Dict[str, Any], analytics_result: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate inputs before generating chart.

        Checks performed:
        - chart_spec contains valid chart_type
        - analytics_result.status == "success"
        - analytics_result.data is not empty
        - Required columns exist in data

        Args:
            chart_spec: ChartOutput from graphical_classifier
            analytics_result: AnalyticsOutput from analytics_executor

        Returns:
            Tuple of (is_valid, error_message)
            - (True, None) if valid
            - (False, error_message) if invalid
        """
        # Validate chart_spec structure
        if not chart_spec:
            return False, "chart_spec is empty or None"

        if "chart_type" not in chart_spec:
            return False, "chart_spec missing required field 'chart_type'"

        chart_type = chart_spec["chart_type"]
        supported_types = [
            "bar_horizontal",
            "bar_vertical",
            "bar_vertical_composed",
            "bar_vertical_stacked",
            "line",
            "line_composed",
            "pie",
            "histogram",
        ]

        if chart_type not in supported_types:
            return False, (
                f"Unsupported chart_type '{chart_type}'. "
                f"Supported types: {', '.join(supported_types)}"
            )

        # Validate analytics_result structure
        if not analytics_result:
            return False, "analytics_result is empty or None"

        status = analytics_result.get("status")
        if status != "success":
            return False, (
                f"analytics_result.status is '{status}' (expected 'success'). "
                "Cannot generate chart from failed analytics execution."
            )

        data = analytics_result.get("data")
        if not data:
            return False, "analytics_result.data is empty - no data to plot"

        if not isinstance(data, list):
            return (
                False,
                f"analytics_result.data must be a list, got {type(data).__name__}",
            )

        # Validate data consistency
        try:
            self.adapter.validate_data_consistency(chart_spec, data)
        except ValueError as e:
            return False, f"Data consistency validation failed: {str(e)}"

        logger.debug("Input validation passed")
        return True, None

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get usage statistics for the agent.

        Returns statistics about chart generation performance and usage patterns.
        Useful for monitoring and optimization.

        Returns:
            Dict containing:
            - total_generations: Total number of charts attempted
            - successful_generations: Number of successful generations
            - failed_generations: Number of failed generations
            - success_rate: Percentage of successful generations
            - total_render_time: Total time spent rendering (seconds)
            - average_render_time: Average time per chart (seconds)
            - charts_by_type: Counter dict of charts by type
        """
        stats = self._stats.copy()

        # Calculate derived metrics
        if stats["total_generations"] > 0:
            stats["success_rate"] = (
                stats["successful_generations"] / stats["total_generations"] * 100
            )
        else:
            stats["success_rate"] = 0.0

        return stats

    def _update_stats(self, chart_type: str, render_time: float, success: bool):
        """Update internal statistics."""
        if success:
            self._stats["successful_generations"] += 1
        else:
            self._stats["failed_generations"] += 1

        self._stats["total_render_time"] += render_time
        self._stats["average_render_time"] = (
            self._stats["total_render_time"] / self._stats["total_generations"]
        )

        # Update counter by type
        if chart_type not in self._stats["charts_by_type"]:
            self._stats["charts_by_type"][chart_type] = 0
        self._stats["charts_by_type"][chart_type] += 1

    def _create_error_response(
        self, chart_type: str, error_type: str, error_message: str, start_time: float
    ) -> Dict[str, Any]:
        """Create standardized error response."""
        render_time = time.perf_counter() - start_time
        self._update_stats(chart_type, render_time, success=False)

        return {
            "status": "error",
            "chart_type": chart_type,
            "figure": None,
            "html": None,
            "file_path": None,
            "config": {},
            "metadata": {
                "rows_plotted": 0,
                "render_time": render_time,
                "generator_used": None,
            },
            "error": {
                "type": error_type,
                "message": error_message,
                "suggestion": self._get_error_suggestion(error_type),
            },
        }

    def _get_error_suggestion(self, error_type: str) -> str:
        """Get helpful suggestion based on error type."""
        suggestions = {
            "ValidationError": "Check that chart_spec and analytics_result follow the correct schemas.",
            "AdaptationError": "Verify that column aliases in chart_spec match data columns.",
            "UnsupportedChartTypeError": "Use one of the 8 supported chart types.",
            "DataValidationError": "Ensure data has correct structure for the selected chart type.",
            "GenerationError": "Check Plotly configuration and data types.",
            "UnexpectedError": "Check logs for stack trace and contact support if issue persists.",
        }
        return suggestions.get(error_type, "Check inputs and try again.")

    def _extract_config(self, chart_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Extract configuration used for generation."""
        return {
            "chart_type": chart_spec.get("chart_type"),
            "title": chart_spec.get("title"),
            "palette": chart_spec.get("visual", {}).get("palette", "Blues"),
            "show_values": chart_spec.get("visual", {}).get("show_values", False),
            "orientation": chart_spec.get("visual", {}).get("orientation"),
            "stacked": chart_spec.get("visual", {}).get("stacked", False),
        }
