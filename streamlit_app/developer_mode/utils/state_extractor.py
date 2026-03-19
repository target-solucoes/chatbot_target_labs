from typing import Dict, Any

def extract_filter_data(state: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "query": state.get("query"),
        "current_filters": state.get("current_filters"),
        "filter_history": state.get("filter_history"),
        "detected_filter_columns": state.get("detected_filter_columns"),
        "filter_operations": state.get("output", {}),
        "filter_final": state.get("filter_final"),
        "skipped": "filter_final" not in state and "output" not in state
    }

def extract_graphic_classifier_data(state: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "semantic_anchor": state.get("semantic_anchor"),
        "semantic_validation": state.get("semantic_validation"),
        "semantic_mapping": state.get("semantic_mapping"),
        "parsed_entities": state.get("parsed_entities"),
        "detected_keywords": state.get("detected_keywords"),
        "intent": state.get("intent"),
        "chart_type": state.get("output", {}).get("chart_type") if isinstance(state.get("output"), dict) else None,
        "metrics": state.get("output", {}).get("metrics") if isinstance(state.get("output"), dict) else None,
        "dimensions": state.get("output", {}).get("dimensions") if isinstance(state.get("output"), dict) else None,
        "confidence": state.get("confidence"),
        "output": state.get("output")
    }

def extract_analytics_executor_data(state: Dict[str, Any]) -> Dict[str, Any]:
    executor_output = state.get("executor_output", {}) or {}
    return {
        "sql_query": executor_output.get("sql_query"),
        "data_preview": executor_output.get("data", [])[:100] if executor_output.get("data") else [],
        "plotly_config": executor_output.get("plotly_config"),
        "engine_used": executor_output.get("engine_used"),
        "row_count": executor_output.get("row_count"),
        "status": executor_output.get("status"),
        "metadata": executor_output.get("metadata")
    }

def extract_non_graph_executor_data(state: Dict[str, Any]) -> Dict[str, Any]:
    non_graph_output = state.get("non_graph_output", {}) or {}
    metadata = non_graph_output.get("metadata", {}) or {}
    return {
        "is_active": bool(non_graph_output),
        "status": non_graph_output.get("status"),
        "query_type": non_graph_output.get("query_type"),
        "summary": non_graph_output.get("summary"),
        "result": non_graph_output.get("result"),
        "data_preview": non_graph_output.get("data", [])[:100] if non_graph_output.get("data") else [],
        "conversational_response": non_graph_output.get("conversational_response"),
        "metadata": metadata,
        "sql_query": metadata.get("sql_query"),
        "execution_path": metadata.get("execution_path"),
        "intent_type": metadata.get("intent_type"),
        "aggregations": metadata.get("aggregations"),
        "group_by": metadata.get("group_by"),
        "order_by": metadata.get("order_by"),
        "filters_applied": metadata.get("filters_applied", {}),
        "row_count": metadata.get("row_count"),
        "engine": metadata.get("engine"),
        "performance_metrics": non_graph_output.get("performance_metrics", {}),
        "total_tokens": non_graph_output.get("total_tokens", {}),
        "error": non_graph_output.get("error"),
        "query": state.get("query"),
        "raw_output": non_graph_output,
    }

def extract_insight_generator_data(state: Dict[str, Any]) -> Dict[str, Any]:
    insight_result = state.get("insight_result", {}) or {}
    return {
        "insights": insight_result.get("insights"),
        "detailed_insights": insight_result.get("detailed_insights"),
        "formatted_insights": insight_result.get("formatted_insights"),
        "status": insight_result.get("status")
    }

def extract_plotly_generator_data(state: Dict[str, Any]) -> Dict[str, Any]:
    plotly_output = state.get("plotly_output", {}) or {}
    return {
        "chart_type": plotly_output.get("chart_type"),
        "config": plotly_output.get("config"),
        "file_path": plotly_output.get("file_path"),
        "status": plotly_output.get("status"),
    }

def extract_formatter_data(state: Dict[str, Any]) -> Dict[str, Any]:
    formatter_output = state.get("formatter_output", {}) or {}
    return {
        "executive_summary": formatter_output.get("executive_summary"),
        "visualization": formatter_output.get("visualization"),
        "insights": formatter_output.get("insights"),
        "next_steps": formatter_output.get("next_steps"),
        "metadata": formatter_output.get("metadata"),
        "data_summary": formatter_output.get("data")
    }

def extract_global_metrics(state: Dict[str, Any]) -> Dict[str, Any]:
    from src.shared_lib.utils.output_detector import detect_output_type
    try:
        output_type, _ = detect_output_type(state)
    except:
        output_type = "unknown"
        
    return {
        "total_time": state.get("execution_time", 0.0),
        "agent_tokens": state.get("agent_tokens", {}),
        "output_type": output_type,
        "errors": state.get("errors", [])
    }
