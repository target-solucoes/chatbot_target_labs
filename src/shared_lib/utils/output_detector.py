"""
Output Detector Utility

Detects which output type is present in pipeline results and provides
unified interface for handling both non_graph_output and formatter_output.
"""

from typing import Dict, Any, Tuple, Optional, Literal
from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)

OutputType = Literal["non_graph", "formatter"]


def detect_output_type(
    pipeline_state: Dict[str, Any],
) -> Tuple[OutputType, Dict[str, Any]]:
    """
    Detect which output is present in the pipeline result.

    The pipeline uses conditional routing to ensure only one output exists:
    - non_graph_output: for metadata, aggregation, lookup, statistical queries
    - formatter_output: for graphical visualization queries

    Args:
        pipeline_state: Pipeline execution result containing state

    Returns:
        Tuple of (output_type, json_data) where:
        - output_type: "non_graph" or "formatter"
        - json_data: The complete JSON output from the agent

    Raises:
        ValueError: If neither output is found (should not happen in normal flow)
    """
    non_graph_output = pipeline_state.get("non_graph_output")
    formatter_output = pipeline_state.get("formatter_output")

    if non_graph_output:
        logger.info("Detected non_graph_output from pipeline")
        return ("non_graph", non_graph_output)
    elif formatter_output:
        logger.info("Detected formatter_output from pipeline")
        return ("formatter", formatter_output)
    else:
        error_msg = (
            "No output found in pipeline result (neither non_graph nor formatter)"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)


def get_output_summary(output_type: OutputType, output_data: Dict[str, Any]) -> str:
    """
    Extract summary text from output based on type.

    For non_graph_output: Returns 'conversational_response' (if present) or 'summary'
    For formatter_output: Returns the executive_summary.introduction field

    Args:
        output_type: Type of output ("non_graph" or "formatter")
        output_data: The output data dictionary

    Returns:
        Summary text string
    """
    if output_type == "non_graph":
        # Non-graph output can have either conversational_response or summary
        # Prioritize conversational_response for conversational queries
        conversational_response = output_data.get("conversational_response")
        if conversational_response:
            return conversational_response

        summary = output_data.get("summary")
        if summary:
            return summary

        return "Sem resumo disponível"
    else:
        # Formatter output has nested structure
        exec_summary = output_data.get("executive_summary", {})
        return exec_summary.get("introduction", "Sem resumo disponível")


def get_execution_time(output_data: Dict[str, Any]) -> float:
    """
    Extract execution time from output data.

    Works for both non_graph_output and formatter_output.

    Args:
        output_data: The output data dictionary

    Returns:
        Execution time in seconds
    """
    metadata = output_data.get("metadata", {})

    # Try different locations where execution time might be
    if "total_execution_time" in metadata:
        return metadata["total_execution_time"]

    if "execution_time" in metadata:
        exec_time_data = metadata["execution_time"]
        if isinstance(exec_time_data, dict):
            return exec_time_data.get("total_execution_time", 0.0)
        return exec_time_data

    # Fallback
    return 0.0


def get_status(output_data: Dict[str, Any]) -> str:
    """
    Extract status from output data.

    Args:
        output_data: The output data dictionary

    Returns:
        Status string ("success", "error", etc.)
    """
    return output_data.get("status", "unknown")


def is_success(output_data: Dict[str, Any]) -> bool:
    """
    Check if output indicates successful execution.

    Args:
        output_data: The output data dictionary

    Returns:
        True if status is "success"
    """
    return get_status(output_data) == "success"


def get_error_message(output_data: Dict[str, Any]) -> Optional[str]:
    """
    Extract error message if present.

    Args:
        output_data: The output data dictionary

    Returns:
        Error message string or None
    """
    return output_data.get("error")
