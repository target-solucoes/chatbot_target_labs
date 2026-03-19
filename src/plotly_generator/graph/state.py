"""
LangGraph State Definition for Plotly Generator

This module defines the state schema that flows through the LangGraph workflow
for chart generation. The state contains all inputs, intermediate results, and
final outputs of the generation process.

Author: Claude Code
Date: 2025-11-12
Version: 1.0
"""

from typing import TypedDict, Dict, Any, Optional, List
from pathlib import Path
import plotly.graph_objects as go


class PlotlyGeneratorState(TypedDict, total=False):
    """
    State schema for the Plotly Generator LangGraph workflow.

    This state object flows through all nodes in the graph, accumulating
    data and results at each step.

    Flow:
        validate_inputs → adapt_inputs → generate_plot → save_output

    Attributes:
        # INPUT (provided at workflow start)
        chart_spec: Output from graphical_classifier (ChartOutput schema)
        analytics_result: Output from analytics_executor (AnalyticsOutput schema)

        # ADAPTED DATA (populated by adapt_inputs node)
        plot_params: Extracted and normalized plotting parameters
        column_mappings: Mapping of column names to aliases

        # GENERATION RESULTS (populated by generate_plot node)
        figure: Generated Plotly Figure object
        html: Rendered HTML string
        generator_used: Name of the generator class used

        # VALIDATION & ERRORS
        validation_errors: List of validation error messages
        error: Detailed error information (if any)

        # METADATA
        render_time: Time taken to render the chart (seconds)
        rows_plotted: Number of data rows plotted

        # OUTPUT (populated by save_output node)
        file_path: Path to saved HTML/PNG file
        file_paths: Dict of all saved files {format: path}

        # STATUS
        status: Current workflow status
                - "pending": Initial state
                - "validated": Inputs validated successfully
                - "adapted": Data adapted successfully
                - "generated": Figure generated successfully
                - "saved": Files saved successfully
                - "success": Workflow completed successfully
                - "error": Error occurred

    Example State Progression:

        Initial:
        {
            "chart_spec": {...},
            "analytics_result": {...},
            "status": "pending"
        }

        After validation:
        {
            ...,
            "validation_errors": [],
            "status": "validated"
        }

        After generation:
        {
            ...,
            "figure": <Figure>,
            "html": "<div>...</div>",
            "generator_used": "BarHorizontalGenerator",
            "render_time": 0.234,
            "status": "generated"
        }

        Final:
        {
            ...,
            "file_path": Path("generated_plots/chart_20251112.html"),
            "status": "success"
        }
    """

    # ========== INPUTS ==========
    chart_spec: Dict[str, Any]
    """Output from graphical_classifier containing chart specifications."""

    analytics_result: Dict[str, Any]
    """Output from analytics_executor containing processed data."""

    # ========== CONFIGURATION ==========
    save_html: bool
    """Whether to save chart as HTML (default: True)."""

    save_png: bool
    """Whether to save chart as PNG (default: False)."""

    output_dir: Path
    """Directory for saving chart files."""

    # ========== ADAPTED DATA ==========
    plot_params: Optional[Dict[str, Any]]
    """Extracted plotting parameters from inputs."""

    column_mappings: Optional[Dict[str, str]]
    """Mapping of column names to aliases (name -> alias)."""

    # ========== GENERATION RESULTS ==========
    figure: Optional[go.Figure]
    """Generated Plotly Figure object."""

    html: Optional[str]
    """Rendered HTML string of the chart."""

    generator_used: Optional[str]
    """Name of the generator class that created the chart."""

    # ========== VALIDATION & ERRORS ==========
    validation_errors: List[str]
    """List of validation error messages (empty if valid)."""

    error: Optional[Dict[str, Any]]
    """
    Detailed error information if an error occurred.
    Structure:
    {
        "type": str,        # Error type (e.g., "ValidationError")
        "message": str,     # Human-readable error message
        "suggestion": str   # Suggested fix
    }
    """

    # ========== METADATA ==========
    render_time: float
    """Time taken to render the chart in seconds."""

    rows_plotted: int
    """Number of data rows included in the chart."""

    chart_type: Optional[str]
    """Type of chart being generated (extracted from chart_spec)."""

    # ========== OUTPUT ==========
    file_path: Optional[Path]
    """Primary file path (HTML if save_html=True, else None)."""

    file_paths: Optional[Dict[str, Path]]
    """
    Dictionary of all saved file paths.
    Example: {"html": Path(...), "png": Path(...)}
    """

    # ========== STATUS ==========
    status: str
    """
    Current workflow status. Possible values:
    - "pending": Initial state
    - "validated": Inputs validated
    - "adapted": Data adapted
    - "generated": Chart generated
    - "saved": Files saved
    - "success": Complete success
    - "error": Error occurred
    """
