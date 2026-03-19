# -*- coding: utf-8 -*-
"""
Pipeline Runner for Streaming Execution

Handles execution of the multi-agent pipeline with streaming support
for progressive display updates.
"""

import logging
import time
from typing import Dict, Any, Optional, Iterator, List
from dataclasses import dataclass

from src.pipeline_orchestrator import (
    create_full_integrated_workflow_with_insights,
    initialize_full_pipeline_state,
)
from src.graphic_classifier.core.settings import DATASET_PATH


logger = logging.getLogger(__name__)


@dataclass
class PipelineExecutionResult:
    """Result of pipeline execution"""

    success: bool
    output_type: Optional[str] = None  # "non_graph" or "formatter"
    output_data: Optional[Dict] = None  # Unified output field
    formatter_output: Optional[Dict] = (
        None  # Deprecated, kept for backward compatibility
    )
    non_graph_output: Optional[Dict] = None  # New field for non-graph outputs
    execution_time: float = 0.0
    error: Optional[str] = None
    final_state: Optional[Dict] = None


class StreamingPipelineRunner:
    """
    Executes the integrated pipeline with streaming support

    Provides methods to run the pipeline and stream intermediate states
    for progressive display.
    """

    def __init__(self, data_path: Optional[str] = None):
        """
        Initialize pipeline runner

        Args:
            data_path: Optional path to data file (defaults to DATASET_PATH from settings)
        """
        self.data_path = data_path or DATASET_PATH
        self.workflow = None
        self._initialize_workflow()

        try:
            import streamlit as st

            session_id = st.session_state.get("session_id", "unknown")
            logger.info(f"[Session {session_id}] StreamingPipelineRunner initialized")
        except Exception:
            logger.info("StreamingPipelineRunner initialized (non-Streamlit context)")

    def _initialize_workflow(self) -> None:
        """Initialize the LangGraph workflow"""
        try:
            self.workflow = create_full_integrated_workflow_with_insights(
                include_filter_classifier=True,
                include_executor=True,
                include_insights=True,
            )
        except Exception as e:
            raise RuntimeError(f"Failed to initialize workflow: {e}")

    def run_with_streaming(
        self,
        query: str,
        reset_filters: bool = False,
        current_filters: Optional[Dict[str, Any]] = None,
        filter_history: Optional[List[Dict[str, Any]]] = None,
    ) -> Iterator[Dict[str, Any]]:
        """
        Execute pipeline with streaming of intermediate states

        This is a generator that yields ACCUMULATED pipeline states as they become available.
        Each yielded state contains all data from previous steps plus the latest update.

        The pipeline can return EITHER:
        - non_graph_output: for metadata, aggregation, lookup queries
        - formatter_output: for graphical visualization queries

        Args:
            query: User query to process
            reset_filters: Whether to reset filters before execution

        Yields:
            Accumulated pipeline state dicts at various stages of execution
        """
        if not self.workflow:
            raise RuntimeError("Workflow not initialized")

        # Initialize state
        initial_state = initialize_full_pipeline_state(
            query=query, data_source=self.data_path
        )

        if reset_filters:
            initial_state["current_filters"] = {}
            initial_state["filter_history"] = []
        else:
            if current_filters is not None:
                initial_state["current_filters"] = dict(current_filters)
            if filter_history is not None:
                initial_state["filter_history"] = list(filter_history)

        # Handle filter reset
        if reset_filters:
            initial_state["reset_filters"] = True

        # Stream workflow execution
        try:
            from src.shared_lib.core.config import DEVELOPER_MODE
            import streamlit as st
            
            if DEVELOPER_MODE and hasattr(st, "session_state"):
                st.session_state["dev_snapshots"] = []

            # Use stream_mode="values" to get the full state at each step
            # instead of just node names
            for state_snapshot in self.workflow.stream(
                initial_state,
                stream_mode="values",  # This returns the full state, not just updates
            ):
                if DEVELOPER_MODE and hasattr(st, "session_state"):
                    try:
                        # Make a shallow copy of the state snapshot to store its current state
                        st.session_state["dev_snapshots"].append(dict(state_snapshot))
                    except Exception as dev_err:
                        logger.warning(f"Failed to capture snapshot for Developer Mode: {dev_err}")
                        
                # Yield the complete state snapshot
                yield state_snapshot

        except Exception as e:
            # Yield error state
            yield {
                "error": str(e),
                "formatter_output": {"status": "error", "error": str(e)},
            }

    def run_complete(
        self,
        query: str,
        reset_filters: bool = False,
        current_filters: Optional[Dict[str, Any]] = None,
        filter_history: Optional[List[Dict[str, Any]]] = None,
    ) -> PipelineExecutionResult:
        """
        Execute pipeline and return complete result

        This method runs the full pipeline and returns only the final result.
        Handles both non_graph_output and formatter_output.

        Args:
            query: User query to process
            reset_filters: Whether to reset filters before execution

        Returns:
            PipelineExecutionResult with final output
        """
        start_time = time.time()

        try:
            # Initialize state
            state = initialize_full_pipeline_state(
                query=query, data_source=self.data_path
            )

            if reset_filters:
                state["current_filters"] = {}
                state["filter_history"] = []
            else:
                if current_filters is not None:
                    state["current_filters"] = dict(current_filters)
                if filter_history is not None:
                    state["filter_history"] = list(filter_history)

            if reset_filters:
                state["reset_filters"] = True

            # Execute workflow
            final_state = self.workflow.invoke(state)
            execution_time = time.time() - start_time

            # Detect which output is present (non_graph or formatter)
            from src.shared_lib.utils.output_detector import detect_output_type

            try:
                output_type, output_data = detect_output_type(final_state)

                # Check for errors
                if output_data.get("status") == "error":
                    return PipelineExecutionResult(
                        success=False,
                        output_type=output_type,
                        output_data=output_data,
                        error=str(output_data.get("error", "Unknown error")),
                        execution_time=execution_time,
                        final_state=final_state,
                    )

                # NOVO: Adicionar tokens ao output_data
                agent_tokens = final_state.get("agent_tokens", {})
                total_tokens = self._calculate_total_tokens(agent_tokens)

                output_data["agent_tokens"] = agent_tokens
                output_data["total_tokens"] = total_tokens

                logger.info(
                    f"[PipelineRunner] Pipeline complete. Total tokens: {total_tokens.get('total_tokens', 0)}"
                )

                # Success result with unified interface
                result = PipelineExecutionResult(
                    success=True,
                    output_type=output_type,
                    output_data=output_data,
                    execution_time=execution_time,
                    final_state=final_state,
                )

                # Maintain backward compatibility
                if output_type == "formatter":
                    result.formatter_output = output_data
                else:
                    result.non_graph_output = output_data

                return result

            except ValueError as e:
                # No output found
                return PipelineExecutionResult(
                    success=False,
                    error=str(e),
                    execution_time=execution_time,
                    final_state=final_state,
                )

        except Exception as e:
            execution_time = time.time() - start_time
            return PipelineExecutionResult(
                success=False, error=str(e), execution_time=execution_time
            )

    def _calculate_total_tokens(self, agent_tokens: Dict[str, Dict[str, int]]) -> Dict[str, int]:
        """
        Calculate total tokens from all agents

        Args:
            agent_tokens: Dictionary of tokens by agent

        Returns:
            Dict with aggregated input_tokens, output_tokens, total_tokens
        """
        if not agent_tokens:
            return {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}

        total = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
        for tokens in agent_tokens.values():
            total["input_tokens"] += tokens.get("input_tokens", 0)
            total["output_tokens"] += tokens.get("output_tokens", 0)
            total["total_tokens"] += tokens.get("total_tokens", 0)

        return total

    def get_workflow_info(self) -> Dict[str, Any]:
        """
        Get information about the workflow

        Returns:
            Dict with workflow metadata
        """
        if not self.workflow:
            return {"initialized": False}

        return {
            "initialized": True,
            "data_path": self.data_path,
            "agents": [
                "filter_classifier",
                "graphic_classifier",
                "analytics_executor",
                "plotly_generator",
                "insight_generator",
                "formatter",
            ],
        }


class SimplePipelineRunner:
    """
    Simplified pipeline runner without streaming

    Use this for simpler use cases where progressive display is not needed.
    """

    def __init__(self, data_path: Optional[str] = None):
        """
        Initialize simple pipeline runner

        Args:
            data_path: Optional path to data file
        """
        self.data_path = data_path or DATASET_PATH
        self.workflow = create_full_integrated_workflow_with_insights(
            include_filter_classifier=True, include_executor=True, include_insights=True
        )

    def run(self, query: str, reset_filters: bool = False) -> Dict[str, Any]:
        """
        Execute pipeline and return final formatter output

        Args:
            query: User query to process
            reset_filters: Whether to reset filters before execution

        Returns:
            Formatter output dict
        """
        state = initialize_full_pipeline_state(query=query, data_source=self.data_path)

        if reset_filters:
            state["reset_filters"] = True

        final_state = self.workflow.invoke(state)
        return final_state.get("formatter_output", {})


def create_pipeline_runner(
    data_path: Optional[str] = None, streaming: bool = True
) -> Any:
    """
    Factory function to create appropriate pipeline runner

    Args:
        data_path: Optional path to data file
        streaming: Whether to create streaming runner (default) or simple runner

    Returns:
        StreamingPipelineRunner or SimplePipelineRunner instance
    """
    if streaming:
        return StreamingPipelineRunner(data_path=data_path)
    else:
        return SimplePipelineRunner(data_path=data_path)
