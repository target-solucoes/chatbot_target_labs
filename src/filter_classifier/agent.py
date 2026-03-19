"""
Main agent orchestrator for filter classification and management.

This module implements the FilterClassifierAgent class, which provides
a high-level interface for processing natural language queries to detect,
classify, and manage filters with CRUD operations.
"""

import logging
from typing import Dict, Any, Optional
from pathlib import Path

from src.filter_classifier.graph.workflow import (
    create_filter_workflow,
    initialize_filter_state
)
from src.filter_classifier.tools.filter_parser import FilterParser
from src.filter_classifier.utils.filter_persistence import FilterPersistence
from src.filter_classifier.utils.filter_formatter import FilterFormatter
from src.filter_classifier.core.settings import (
    ALIAS_PATH,
    DATASET_PATH,
    SESSION_TIMEOUT_MINUTES,
    validate_settings
)
from src.graphic_classifier.tools.alias_mapper import AliasMapper
from src.shared_lib.utils.logger import setup_logging

logger = logging.getLogger(__name__)


class FilterClassifierAgent:
    """
    Main agent for extracting and managing filters from natural language queries.

    This agent orchestrates the entire filter management workflow:
    1. Accepts natural language queries
    2. Detects filters using LLM-based parsing
    3. Identifies CRUD operations (ADICIONAR, ALTERAR, REMOVER, MANTER)
    4. Applies operations to manage filter state
    5. Persists filters between sessions
    6. Returns structured JSON with filter specifications

    The agent maintains filter context across queries using session persistence,
    enabling conversational filter management.

    Example:
        >>> agent = FilterClassifierAgent()
        >>> result = agent.classify_filters("Mostre vendas de SP")
        >>> print(result['filter_final'])
        {'UF_Cliente': 'SP'}
        >>> result = agent.classify_filters("E do ano 2020")
        >>> print(result['filter_final'])
        {'UF_Cliente': 'SP', 'Ano': 2020}
    """

    def __init__(
        self,
        alias_path: Optional[str] = None,
        dataset_path: Optional[str] = None,
        session_timeout_minutes: Optional[int] = None,
        setup_logs: bool = True
    ):
        """
        Initialize the FilterClassifierAgent.

        Args:
            alias_path: Path to alias.yaml file (uses default if None)
            dataset_path: Path to dataset for column validation (uses default if None)
            session_timeout_minutes: Session timeout in minutes (uses default if None)
            setup_logs: Whether to configure logging system

        Raises:
            ValueError: If settings validation fails
            FileNotFoundError: If required files not found
        """
        # Setup logging
        if setup_logs:
            setup_logging()

        logger.info("[Agent] Initializing FilterClassifierAgent")

        # Validate environment settings
        try:
            validate_settings()
            logger.info("[Agent] Settings validation successful")
        except Exception as e:
            logger.error(f"[Agent] Settings validation failed: {str(e)}")
            raise

        # Store configuration
        self.alias_path = alias_path or ALIAS_PATH
        self.dataset_path = dataset_path or DATASET_PATH
        self.session_timeout = session_timeout_minutes or SESSION_TIMEOUT_MINUTES

        # Initialize alias mapper
        try:
            self.alias_mapper = AliasMapper(alias_path=self.alias_path)
            logger.info(
                f"[Agent] AliasMapper initialized with "
                f"{len(self.alias_mapper.aliases)} columns"
            )
        except Exception as e:
            logger.error(f"[Agent] Failed to initialize AliasMapper: {str(e)}")
            raise

        # Initialize persistence
        try:
            self.persistence = FilterPersistence(
                session_timeout_minutes=self.session_timeout
            )
            logger.info(f"[Agent] FilterPersistence initialized (timeout: {self.session_timeout}m)")
        except Exception as e:
            logger.error(f"[Agent] Failed to initialize FilterPersistence: {str(e)}")
            raise

        # Create workflow
        try:
            self.workflow = create_filter_workflow()
            logger.info("[Agent] Filter workflow created")
        except Exception as e:
            logger.error(f"[Agent] Failed to create workflow: {str(e)}")
            raise

        # Track statistics
        self._query_count = 0
        self._error_count = 0
        self._filter_operations_count = {
            "ADICIONAR": 0,
            "ALTERAR": 0,
            "REMOVER": 0,
            "MANTER": 0
        }

        logger.info("[Agent] FilterClassifierAgent initialization complete")

    def classify_filters(
        self,
        query: str,
        reset_filters: bool = False
    ) -> Dict[str, Any]:
        """
        Classify and manage filters based on user query.

        This is the main entry point for the agent. It processes the query
        through the entire filter workflow and returns structured filter output.

        Args:
            query: Natural language query (e.g., "Mostre vendas de SP em 2020")
            reset_filters: If True, clears all existing filters before processing

        Returns:
            Dictionary with filter specification:
            {
                "ADICIONAR": {col: value, ...},
                "ALTERAR": {col: {"from": old, "to": new}, ...},
                "REMOVER": {col: value, ...},
                "MANTER": {col: value, ...},
                "filter_final": {col: value, ...},
                "metadata": {
                    "confidence": 0.95,
                    "timestamp": "2025-01-05T10:30:00",
                    "columns_detected": ["UF_Cliente", "Ano"],
                    "errors": [],
                    "status": "success"
                }
            }

        Example:
            >>> agent = FilterClassifierAgent()
            >>> result = agent.classify_filters("Filtre por SP")
            >>> print(result['filter_final'])
            {'UF_Cliente': 'SP'}
            >>> result = agent.classify_filters("Agora para RJ")
            >>> print(result['ALTERAR'])
            {'UF_Cliente': {'from': 'SP', 'to': 'RJ'}}
        """
        try:
            self._query_count += 1
            logger.info(f"[Agent] Processing query #{self._query_count}: {query}")

            # Reset filters if requested
            if reset_filters:
                self.clear_filters()
                logger.info("[Agent] Filters reset before processing")

            # Initialize state
            initial_state = initialize_filter_state(query)

            # Execute workflow
            final_state = self.workflow.invoke(initial_state)

            # Extract output
            output = final_state.get("output", {})

            # Update statistics
            self._update_metrics(output)

            # Log result
            filter_count = len(output.get("filter_final", {}))
            confidence = output.get("metadata", {}).get("confidence", 0.0)
            logger.info(
                f"[Agent] Query processed successfully: "
                f"{filter_count} final filters (confidence: {confidence:.2f})"
            )

            return output

        except Exception as e:
            self._error_count += 1
            logger.error(f"[Agent] Error processing query: {str(e)}")
            return self._format_error_response(str(e))

    def get_active_filters(self) -> Dict[str, Any]:
        """
        Get currently active filters from the session.

        Returns:
            Dictionary with active filters: {column: value, ...}

        Example:
            >>> agent = FilterClassifierAgent()
            >>> agent.classify_filters("Filtre por SP em 2020")
            >>> active = agent.get_active_filters()
            >>> print(active)
            {'UF_Cliente': 'SP', 'Ano': 2020}
        """
        try:
            state = self.persistence.load()
            filters = state.get("filter_final", {})
            logger.debug(f"[Agent] Retrieved {len(filters)} active filters")
            return filters
        except Exception as e:
            logger.error(f"[Agent] Error loading active filters: {str(e)}")
            return {}

    def clear_filters(self) -> None:
        """
        Clear all filters from the current session.

        This removes the persisted filter state, allowing a fresh start.

        Example:
            >>> agent = FilterClassifierAgent()
            >>> agent.classify_filters("Filtre por SP")
            >>> agent.clear_filters()
            >>> print(agent.get_active_filters())
            {}
        """
        try:
            self.persistence.clear()
            logger.info("[Agent] Filters cleared successfully")
        except Exception as e:
            logger.error(f"[Agent] Error clearing filters: {str(e)}")

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get agent usage statistics.

        Returns:
            Dictionary with statistics:
            {
                "query_count": 10,
                "error_count": 1,
                "filter_operations": {
                    "ADICIONAR": 5,
                    "ALTERAR": 2,
                    "REMOVER": 1,
                    "MANTER": 15
                },
                "session_info": {
                    "active_filters": 2,
                    "has_active_session": True,
                    "session_expired": False
                }
            }

        Example:
            >>> agent = FilterClassifierAgent()
            >>> agent.classify_filters("Filtre por SP")
            >>> agent.classify_filters("E ano 2020")
            >>> stats = agent.get_statistics()
            >>> print(stats['query_count'])
            2
        """
        try:
            session_info = self.persistence.get_session_info()
            active_filters = self.get_active_filters()

            return {
                "query_count": self._query_count,
                "error_count": self._error_count,
                "filter_operations": self._filter_operations_count.copy(),
                "session_info": {
                    **session_info,
                    "active_filters": len(active_filters)
                }
            }
        except Exception as e:
            logger.error(f"[Agent] Error getting statistics: {str(e)}")
            return {
                "query_count": self._query_count,
                "error_count": self._error_count,
                "filter_operations": self._filter_operations_count.copy(),
                "session_info": {}
            }

    def _update_metrics(self, output: Dict[str, Any]) -> None:
        """Update internal metrics based on output."""
        try:
            # Count CRUD operations
            for operation in ["ADICIONAR", "ALTERAR", "REMOVER", "MANTER"]:
                operation_data = output.get(operation, {})
                if operation_data:
                    self._filter_operations_count[operation] += len(operation_data)

        except Exception as e:
            logger.warning(f"[Agent] Error updating metrics: {str(e)}")

    def _format_error_response(self, error_message: str) -> Dict[str, Any]:
        """
        Format an error response.

        Args:
            error_message: Error description

        Returns:
            Error response dictionary
        """
        formatter = FilterFormatter()
        return formatter.format_error_response(error_message)


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def create_filter_agent(
    alias_path: Optional[str] = None,
    dataset_path: Optional[str] = None,
    session_timeout: Optional[int] = None,
    setup_logs: bool = True
) -> FilterClassifierAgent:
    """
    Convenience function to create a FilterClassifierAgent.

    Args:
        alias_path: Path to alias.yaml
        dataset_path: Path to dataset
        session_timeout: Session timeout in minutes
        setup_logs: Configure logging

    Returns:
        Initialized FilterClassifierAgent

    Example:
        >>> agent = create_filter_agent()
        >>> result = agent.classify_filters("Mostre dados de SP")
    """
    return FilterClassifierAgent(
        alias_path=alias_path,
        dataset_path=dataset_path,
        session_timeout_minutes=session_timeout,
        setup_logs=setup_logs
    )


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "FilterClassifierAgent",
    "create_filter_agent"
]
