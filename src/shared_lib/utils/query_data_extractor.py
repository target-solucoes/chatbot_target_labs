"""
Query Data Extractor

Extracts summarized metrics from output data (formatter_output or non_graph_output)
for session logging.
"""

from datetime import datetime
from typing import Dict, Any, Optional, Literal


def _coalesce_time(*candidates: Optional[float]) -> float:
    """
    Return the first numeric candidate (including 0.0). Defaults to 0.0.
    """
    for candidate in candidates:
        if isinstance(candidate, (int, float)):
            return float(candidate)
    return 0.0


def extract_query_data(
    query: str,
    output_data: Dict[str, Any],
    query_id: int,
    output_type: Literal["formatter", "non_graph"] = "formatter",
) -> Dict[str, Any]:
    """
    Extract summarized query data from pipeline output.

    This creates a compact structure for session logging, excluding the
    full output (which is saved separately).

    Supports both formatter_output and non_graph_output.

    Args:
        query: User query string
        output_data: Complete output dictionary (formatter or non_graph)
        query_id: Sequential query ID number
        output_type: Type of output ("formatter" or "non_graph")

    Returns:
        Dictionary with summarized query data
    """
    if output_type == "non_graph":
        return _extract_non_graph_data(query, output_data, query_id)
    else:
        return _extract_formatter_data(query, output_data, query_id)


def _extract_formatter_data(
    query: str, formatter_output: Dict[str, Any], query_id: int
) -> Dict[str, Any]:
    """
    Extract summarized query data from formatter_output.

    Args:
        query: User query string
        formatter_output: Complete formatter output dictionary
        query_id: Sequential query ID number

    Returns:
        Dictionary with summarized query data
    """
    # Extract status
    status = formatter_output.get("status", "unknown")

    performance_metrics = formatter_output.get("performance_metrics", {})

    # Extract execution time
    metadata = formatter_output.get("metadata", {})
    execution_time_data = metadata.get("execution_time", {})
    if not isinstance(execution_time_data, dict):
        execution_time_data = {}

    total_execution_time = _coalesce_time(
        performance_metrics.get("total_execution_time"),
        execution_time_data.get("total_execution_time"),
        metadata.get("total_execution_time"),
    )

    # Extract chart type
    executive_summary = formatter_output.get("executive_summary", {})
    chart_type = executive_summary.get("chart_type", "unknown")

    # Extract filters applied
    filters_applied = executive_summary.get("filters_applied", {})

    # NOVO: Extrair tokens do formatter_output
    token_usage = formatter_output.get("total_tokens", {
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0
    })

    agent_tokens = formatter_output.get("agent_tokens", {})

    filter_time = _coalesce_time(
        performance_metrics.get("filter_classifier_execution_time"),
        execution_time_data.get("filter_classifier"),
    )
    classifier_time = _coalesce_time(
        performance_metrics.get("graphic_classifier_execution_time"),
        execution_time_data.get("graphic_classifier"),
    )
    analytics_time = _coalesce_time(
        performance_metrics.get("analytics_executor_execution_time"),
        execution_time_data.get("analytics_executor"),
    )
    plotly_time = _coalesce_time(
        performance_metrics.get("plotly_generator_execution_time"),
        execution_time_data.get("plotly_generator"),
    )
    insight_time = _coalesce_time(
        performance_metrics.get("insight_generator_execution_time"),
        execution_time_data.get("insight_generator"),
    )
    formatter_time = _coalesce_time(
        performance_metrics.get("formatter_execution_time"),
        execution_time_data.get("formatter"),
    )

    metrics = {
        "filter_classifier_time": filter_time,
        "graphic_classifier_time": classifier_time,
        "analytics_executor_time": analytics_time,
        "plotly_generator_time": plotly_time,
        "insight_generator_time": insight_time,
        "formatter_time": formatter_time,
        "tokens_used": token_usage,
        "agent_tokens": agent_tokens,
    }

    # Extract data quality metrics
    data_quality_full = metadata.get("data_quality", {})
    data_quality = {
        "completeness": data_quality_full.get("completeness", 1.0),
        "filters_count": data_quality_full.get("filters_count", 0),
        "total_records": _extract_total_records(formatter_output),
    }

    # Build query data
    query_data = {
        "query_id": query_id,
        "timestamp": datetime.now().isoformat(),
        "user_query": query,
        "output_type": "formatter",
        "status": status,
        "execution_time": round(total_execution_time, 2),
        "chart_type": chart_type,
        "filters_applied": filters_applied,
        "metrics": metrics,
        "data_quality": data_quality,
        "error": formatter_output.get("error", None),

        # NOVO: Adicionar colunas diretas para Supabase
        "total_input_tokens": token_usage.get("input_tokens", 0),
        "total_output_tokens": token_usage.get("output_tokens", 0),
        "total_tokens": token_usage.get("total_tokens", 0),
        "token_usage_by_agent": agent_tokens
    }

    return query_data


def _extract_non_graph_data(
    query: str, non_graph_output: Dict[str, Any], query_id: int
) -> Dict[str, Any]:
    """
    Extract summarized query data from non_graph_output.

    Args:
        query: User query string
        non_graph_output: Complete non_graph output dictionary
        query_id: Sequential query ID number

    Returns:
        Dictionary with summarized query data
    """
    # Extract status
    status = non_graph_output.get("status", "unknown")

    # Extract execution time
    metadata = non_graph_output.get("metadata", {})
    performance_metrics = non_graph_output.get("performance_metrics", {})
    metadata_exec_time = metadata.get("execution_time")
    total_execution_time = _coalesce_time(
        performance_metrics.get("total_time"),
        performance_metrics.get("total_execution_time"),
        metadata.get("total_execution_time"),
        metadata_exec_time,
    )

    # Extract query type (metadata, aggregation, lookup, etc.)
    query_type = non_graph_output.get("query_type", "unknown")

    # Extract filters applied (if available)
    filters_applied = metadata.get("filters_applied", {})

    # Extract agent execution times from performance_metrics
    agent_times = performance_metrics.get("agent_execution_times", {})
    classification_time = _coalesce_time(
        agent_times.get("graphic_classifier"),
        performance_metrics.get("classification_time"),
    )
    executor_time = _coalesce_time(
        agent_times.get("non_graph_executor"),
        performance_metrics.get("execution_time"),
        metadata_exec_time,
    )

    # NOVO: Extrair tokens do non_graph_output
    token_usage = non_graph_output.get("total_tokens", {
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0
    })

    agent_tokens = non_graph_output.get("agent_tokens", {})

    metrics = {
        "filter_classifier_time": agent_times.get("filter_classifier", 0.0),
        "graphic_classifier_time": classification_time,
        "non_graph_executor_time": executor_time,
        "tokens_used": token_usage,
        "agent_tokens": agent_tokens,
    }

    # Extract data quality metrics
    data_quality = {
        "completeness": 1.0,
        "filters_count": len(filters_applied) if filters_applied else 0,
        "total_records": _extract_non_graph_total_records(non_graph_output),
    }

    # Build query data
    query_data = {
        "query_id": query_id,
        "timestamp": datetime.now().isoformat(),
        "user_query": query,
        "output_type": "non_graph",
        "status": status,
        "execution_time": round(total_execution_time, 2),
        "query_type": query_type,  # Instead of chart_type
        "filters_applied": filters_applied,
        "metrics": metrics,
        "data_quality": data_quality,
        "error": non_graph_output.get("error", None),

        # NOVO: Colunas diretas para Supabase
        "total_input_tokens": token_usage.get("input_tokens", 0),
        "total_output_tokens": token_usage.get("output_tokens", 0),
        "total_tokens": token_usage.get("total_tokens", 0),
        "token_usage_by_agent": agent_tokens
    }

    return query_data


def _extract_non_graph_total_records(non_graph_output: Dict[str, Any]) -> int:
    """
    Extract total records from non_graph output.

    Args:
        non_graph_output: Complete non_graph output

    Returns:
        Total number of records, or 0 if not found
    """
    # Try data array length
    data = non_graph_output.get("data", [])
    if data and isinstance(data, list):
        return len(data)

    # Try metadata
    metadata = non_graph_output.get("metadata", {})
    total_records = metadata.get("total_records")
    if total_records is not None:
        return total_records

    # Default to 0
    return 0


def _extract_total_records(formatter_output: Dict[str, Any]) -> int:
    """
    Extract total records from formatter output.

    Tries multiple locations where this might be stored.

    Args:
        formatter_output: Complete formatter output

    Returns:
        Total number of records, or 0 if not found
    """
    # Try visualization.data_context.total_records
    visualization = formatter_output.get("visualization", {})
    data_context = visualization.get("data_context", {})
    total_records = data_context.get("total_records")

    if total_records is not None:
        return total_records

    # Try data.raw_data length
    data = formatter_output.get("data", {})
    raw_data = data.get("raw_data", [])
    if raw_data:
        return len(raw_data)

    # Try data.summary_table.data length
    summary_table = data.get("summary_table", {})
    table_data = summary_table.get("data", [])
    if table_data:
        return len(table_data)

    # Default to 0
    return 0


__all__ = ["extract_query_data"]
