"""
LangGraph Nodes for Plotly Generator Workflow

This module defines the processing nodes that compose the chart generation workflow.
Each node performs a specific transformation on the state.

Workflow:
    START → validate_inputs → adapt_inputs → generate_plot → save_output → END

Author: Claude Code
Date: 2025-11-12
Version: 1.0
"""

import time
from pathlib import Path
from typing import Dict, Any

from src.plotly_generator.graph.state import PlotlyGeneratorState
from src.plotly_generator.adapters.input_adapter import InputAdapter
from src.plotly_generator.generators.router import GeneratorRouter
from src.plotly_generator.utils.plot_styler import PlotStyler
from src.plotly_generator.utils.file_saver import FileSaver
from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


def validate_inputs_node(state: PlotlyGeneratorState) -> PlotlyGeneratorState:
    """
    Node 1: Validate inputs before processing.

    Checks performed:
    - chart_spec contains chart_type
    - chart_type is supported
    - analytics_result.status == "success"
    - analytics_result.data is not empty
    - Required fields are present

    Args:
        state: Current workflow state

    Returns:
        Updated state with:
        - validation_errors: List of error messages (empty if valid)
        - status: "validated" if valid, "error" if invalid

    Side Effects:
        Logs validation results
    """
    logger.info("Node: validate_inputs - Starting input validation")
    errors = []

    # Validate chart_spec
    chart_spec = state.get("chart_spec")
    if not chart_spec:
        errors.append("chart_spec is missing or empty")
    elif not isinstance(chart_spec, dict):
        errors.append(f"chart_spec must be a dict, got {type(chart_spec).__name__}")
    else:
        # Validate chart_type
        chart_type = chart_spec.get("chart_type")
        if not chart_type:
            errors.append("chart_spec missing required field 'chart_type'")
        else:
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
                errors.append(
                    f"Unsupported chart_type '{chart_type}'. "
                    f"Supported: {', '.join(supported_types)}"
                )
            else:
                state["chart_type"] = chart_type

    # Validate analytics_result
    analytics_result = state.get("analytics_result")
    if not analytics_result:
        errors.append("analytics_result is missing or empty")
    elif not isinstance(analytics_result, dict):
        errors.append(
            f"analytics_result must be a dict, got {type(analytics_result).__name__}"
        )
    else:
        # Check status
        status = analytics_result.get("status")
        if status != "success":
            errors.append(
                f"analytics_result.status is '{status}' (expected 'success'). "
                "Cannot generate chart from failed analytics execution."
            )

        # Check data
        data = analytics_result.get("data")
        if not data:
            errors.append("analytics_result.data is empty - no data to plot")
        elif not isinstance(data, list):
            errors.append(
                f"analytics_result.data must be a list, got {type(data).__name__}"
            )
        else:
            state["rows_plotted"] = len(data)

    # Update state
    state["validation_errors"] = errors

    if errors:
        state["status"] = "error"
        state["error"] = {
            "type": "ValidationError",
            "message": "; ".join(errors),
            "suggestion": "Check that inputs follow ChartOutput and AnalyticsOutput schemas",
        }
        logger.error(f"Validation failed with {len(errors)} errors: {errors}")
    else:
        state["status"] = "validated"
        logger.info(f"Validation passed for chart_type: {state.get('chart_type')}")

    return state


def adapt_inputs_node(state: PlotlyGeneratorState) -> PlotlyGeneratorState:
    """
    Node 2: Adapt inputs to internal format.

    Uses InputAdapter to:
    - Extract plotting parameters
    - Map column names to aliases
    - Validate data consistency

    Args:
        state: Current workflow state (must be status="validated")

    Returns:
        Updated state with:
        - plot_params: Extracted parameters
        - column_mappings: Column name → alias mapping
        - status: "adapted" if successful, "error" if failed

    Side Effects:
        Logs adaptation results
    """
    logger.info("Node: adapt_inputs - Adapting inputs to internal format")

    adapter = InputAdapter()

    try:
        # Validate data consistency
        adapter.validate_data_consistency(
            state["chart_spec"], state["analytics_result"]["data"]
        )

        # Extract column mappings
        column_mappings = adapter.extract_column_mappings(state["chart_spec"])
        state["column_mappings"] = column_mappings

        # Adapt inputs (this creates PlotParams object)
        plot_params = adapter.adapt(state["chart_spec"], state["analytics_result"])
        state["plot_params"] = {
            "chart_type": plot_params.chart_type,
            "title": plot_params.title,
            "description": plot_params.description,
            "visual_config": plot_params.visual_config,
        }

        state["status"] = "adapted"
        logger.info(f"Adaptation successful for {plot_params.chart_type}")

    except Exception as e:
        state["status"] = "error"
        state["error"] = {
            "type": "AdaptationError",
            "message": f"Failed to adapt inputs: {str(e)}",
            "suggestion": "Verify that column aliases in chart_spec match data columns",
        }
        state["validation_errors"].append(str(e))
        logger.error(f"Adaptation failed: {e}", exc_info=True)

    return state


def generate_plot_node(state: PlotlyGeneratorState) -> PlotlyGeneratorState:
    """
    Node 3: Generate Plotly chart.

    Uses GeneratorRouter to:
    - Select appropriate generator based on chart_type
    - Validate data for specific chart type
    - Generate Plotly Figure

    Args:
        state: Current workflow state (must be status="adapted")

    Returns:
        Updated state with:
        - figure: Generated Plotly Figure
        - html: Rendered HTML string
        - generator_used: Name of generator class
        - render_time: Time taken to render
        - status: "generated" if successful, "error" if failed

    Side Effects:
        Logs generation progress and timing
    """
    logger.info("Node: generate_plot - Starting chart generation")
    start_time = time.perf_counter()

    # Initialize components
    styler = PlotStyler()
    router = GeneratorRouter(styler)

    chart_type = state["chart_spec"]["chart_type"]

    try:
        # Select generator
        generator = router.get_generator(chart_type)
        generator_name = generator.__class__.__name__
        state["generator_used"] = generator_name
        logger.debug(f"Selected generator: {generator_name}")

        # Validate data for this specific generator
        generator.validate(state["chart_spec"], state["analytics_result"]["data"])
        logger.debug("Generator-specific validation passed")

        # Generate figure
        figure = generator.generate(
            state["chart_spec"], state["analytics_result"]["data"]
        )
        logger.debug("Figure generation completed")

        # Render HTML
        html = figure.to_html(include_plotlyjs="cdn")

        # Calculate metrics
        render_time = time.perf_counter() - start_time
        rows_plotted = len(state.get("analytics_result", {}).get("data", []))

        # Update state
        state["figure"] = figure
        state["html"] = html
        state["render_time"] = render_time
        state["rows_plotted"] = rows_plotted
        state["status"] = "generated"

        logger.info(
            f"Chart generated successfully: {chart_type} "
            f"({rows_plotted} rows, {render_time:.3f}s)"
        )

    except ValueError as e:
        # Generator selection or validation error
        state["status"] = "error"
        state["error"] = {
            "type": "GeneratorError",
            "message": str(e),
            "suggestion": "Check that data structure matches chart type requirements",
        }
        state["validation_errors"].append(str(e))
        state["render_time"] = time.perf_counter() - start_time
        logger.error(f"Generator error: {e}")

    except Exception as e:
        # Unexpected generation error
        state["status"] = "error"
        state["error"] = {
            "type": "GenerationError",
            "message": f"Failed to generate chart: {str(e)}",
            "suggestion": "Check Plotly configuration and data types",
        }
        state["validation_errors"].append(str(e))
        state["render_time"] = time.perf_counter() - start_time
        logger.error(f"Generation failed: {e}", exc_info=True)

    return state


def save_output_node(state: PlotlyGeneratorState) -> PlotlyGeneratorState:
    """
    Node 4: Save chart to disk.

    Saves the generated chart based on configuration:
    - HTML: If save_html=True
    - PNG: If save_png=True (requires kaleido)

    Args:
        state: Current workflow state (must be status="generated")

    Returns:
        Updated state with:
        - file_path: Primary file path (HTML if available)
        - file_paths: Dict of all saved files
        - status: "success" if successful, stays "generated" if save disabled

    Side Effects:
        Writes files to disk
        Logs save results

    Note:
        File save errors are non-fatal - chart is still considered successfully
        generated even if saving fails.
    """
    logger.info("Node: save_output - Saving chart files")

    # Skip if generation failed
    if state["status"] != "generated":
        logger.warning("Skipping save_output - chart was not generated")
        return state

    # Initialize file saver
    output_dir = state.get("output_dir", Path("src/plotly_generator/generated_plots"))
    saver = FileSaver(output_dir)

    file_paths = {}

    # Save HTML
    if state.get("save_html", True):
        try:
            html_path = saver.save_html(state["figure"])
            file_paths["html"] = html_path
            state["file_path"] = html_path  # Primary file path
            logger.info(f"Saved HTML: {html_path}")
        except Exception as e:
            logger.warning(f"Failed to save HTML: {e}")
            # Non-fatal - continue

    # Save PNG
    if state.get("save_png", False):
        try:
            png_path = saver.save_png(state["figure"])
            file_paths["png"] = png_path
            logger.info(f"Saved PNG: {png_path}")
        except Exception as e:
            logger.warning(f"Failed to save PNG: {e}")
            # Non-fatal - continue

    # Update state
    if file_paths:
        state["file_paths"] = file_paths
        state["status"] = "success"
        logger.info(f"Chart saved successfully: {len(file_paths)} file(s)")
    else:
        # No files saved (might be intentional if both save flags are False)
        state["status"] = "success"
        logger.info(
            "Chart generated but no files saved (save_html=False, save_png=False)"
        )

    return state


# Export all nodes
__all__ = [
    "validate_inputs_node",
    "adapt_inputs_node",
    "generate_plot_node",
    "save_output_node",
]
