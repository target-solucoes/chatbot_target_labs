"""
Main agent orchestrator for chart classification.

This module implements the GraphicClassifierAgent class, which provides
a high-level interface for processing natural language queries and
returning structured chart specifications.
"""

import logging
from typing import Dict, Any, Optional
from pathlib import Path

from src.graphic_classifier.graph.workflow import create_workflow, WorkflowConfig
from src.graphic_classifier.graph.nodes import initialize_state
from src.shared_lib.models.schema import ChartOutput, QueryInput
from src.graphic_classifier.tools.alias_mapper import AliasMapper
from src.graphic_classifier.core.settings import ALIAS_PATH, validate_settings
from src.shared_lib.utils.logger import setup_logging
from src.graphic_classifier.utils.json_formatter import create_error_output, format_output

logger = logging.getLogger(__name__)


class GraphicClassifierAgent:
    """
    Main agent for classifying natural language queries into chart specifications.
    
    This agent orchestrates the entire workflow:
    1. Accepts natural language queries
    2. Processes them through the LangGraph workflow
    3. Returns validated JSON chart specifications
    
    The agent is designed to be stateless and thread-safe, with all
    configuration loaded at initialization time.
    
    Example:
        >>> agent = GraphicClassifierAgent()
        >>> result = agent.classify("top 5 produtos mais vendidos")
        >>> print(result['chart_type'])
        'bar_horizontal'
    """
    
    def __init__(
        self,
        alias_path: Optional[str] = None,
        config: Optional[WorkflowConfig] = None,
        setup_logs: bool = True
    ):
        """
        Initialize the GraphicClassifierAgent.
        
        Args:
            alias_path: Path to alias.yaml file (uses default if None)
            config: Custom workflow configuration (uses default if None)
            setup_logs: Whether to configure logging system
        
        Raises:
            ValueError: If settings validation fails
            FileNotFoundError: If alias file not found
        """
        # Setup logging
        if setup_logs:
            setup_logging()
        
        logger.info("Initializing GraphicClassifierAgent")
        
        # Validate environment settings
        try:
            validate_settings()
            logger.info("Settings validation successful")
        except Exception as e:
            logger.error(f"Settings validation failed: {str(e)}")
            raise
        
        # Store configuration
        self.alias_path = alias_path or ALIAS_PATH
        self.config = config
        
        # Initialize alias mapper (validates alias file exists)
        try:
            self.alias_mapper = AliasMapper(alias_path=self.alias_path)
            logger.info(f"AliasMapper initialized with {len(self.alias_mapper.aliases)} columns")
        except Exception as e:
            logger.error(f"Failed to initialize AliasMapper: {str(e)}")
            raise
        
        # Create workflow
        try:
            if config:
                from src.graphic_classifier.graph.workflow import create_custom_workflow
                self.workflow = create_custom_workflow(config)
                logger.info("Custom workflow created")
            else:
                self.workflow = create_workflow()
                logger.info("Standard workflow created")
        except Exception as e:
            logger.error(f"Failed to create workflow: {str(e)}")
            raise
        
        # Track statistics
        self._query_count = 0
        self._error_count = 0
        
        logger.info("GraphicClassifierAgent initialization complete")
    
    def classify(self, query: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Classify a natural language query and return chart specification.
        
        This is the main entry point for the agent. It processes the query
        through the entire workflow and returns a validated JSON output.
        
        Args:
            query: Natural language query (e.g., "top 5 produtos mais vendidos")
            context: Optional context information (not currently used)
        
        Returns:
            Dictionary with chart specification conforming to ChartOutput schema.

            On success the payload includes:
                - intent, chart_type, title and description
                - metrics: list of metric specifications (name, aggregation, alias, unit)
                - dimensions: list of dimension specifications (name, alias)
                - filters, top_n, sort and visual configuration
                - data_source and output summary template

            On error or no-chart scenarios the payload includes:
                - chart_type: None
                - message: str (explanation)
                - errors: list (if processing errors occurred)
        
        Example:
            >>> agent = GraphicClassifierAgent()
            >>> result = agent.classify("top 10 clientes por faturamento")
            >>> print(result)
            {
                "intent": "ranking",
                "chart_type": "bar_horizontal",
                "title": "Top 10 clientes por faturamento",
                "metrics": [
                    {"name": "Valor_Venda", "aggregation": "sum", "alias": "Valor Venda", "unit": "R$"}
                ],
                "dimensions": [
                    {"name": "Cod_Cliente", "alias": "Cod Cliente"}
                ],
                "filters": {},
                "top_n": 10,
                "sort": {"by": "Valor Venda", "order": "desc"},
                "visual": {"palette": "Blues", "show_values": True, "orientation": "horizontal", "stacked": False, "secondary_chart_type": None, "bins": None},
                "output": {"type": "chart_and_summary", "summary_template": "Os {top_n} principais cod cliente são apresentados ordenados por Valor Venda."}
            }
        """
        self._query_count += 1
        
        logger.info(f"[Query #{self._query_count}] Processing: {query}")
        
        # Validate input
        try:
            query_input = QueryInput(query=query, context=context)
            validated_query = query_input.query
        except Exception as e:
            logger.error(f"[Query #{self._query_count}] Invalid input: {str(e)}")
            self._error_count += 1
            return self._create_error_response(f"Invalid query: {str(e)}")
        
        # Execute workflow
        try:
            # Initialize state with context (includes filter_final from filter_classifier)
            initial_state = initialize_state(validated_query, context=context)

            # Run workflow
            logger.debug(f"[Query #{self._query_count}] Invoking workflow")
            final_state = self.workflow.invoke(initial_state)
            
            # Extract output
            output = final_state.get("output", {})
            
            # Validate output against schema
            validated_output = self.validate_output(
                output,
                query=validated_query,
                state=final_state
            )
            
            # Log successful classification
            chart_type = validated_output.get('chart_type')
            confidence = final_state.get('confidence', 0.0)
            
            logger.info(
                f"[Query #{self._query_count}] Classification successful: "
                f"chart_type={chart_type}, confidence={confidence:.2f}"
            )
            
            return validated_output
            
        except Exception as e:
            logger.error(
                f"[Query #{self._query_count}] Workflow execution failed: {str(e)}",
                exc_info=True
            )
            self._error_count += 1
            return self._create_error_response(
                f"Failed to classify query: {str(e)}"
            )
    
    def validate_output(
        self,
        output: Dict[str, Any],
        *,
        query: Optional[str] = None,
        state: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Validate output dictionary against ChartOutput schema.
        
        This method ensures that the output conforms to the expected
        schema before returning to the caller. It applies the same
        enrichment used by the json formatter so that even partial
        payloads (e.g., from mocked workflows) are normalized.
        
        Args:
            output: Raw output dictionary from workflow
            query: Optional original query string for title generation
            state: Optional workflow state for additional context
        
        Returns:
            Validated output dictionary (chart_type always included, even if None)
        
        Raises:
            ValueError: If output fails validation
        """
        state = state or {}

        payload: Dict[str, Any] = {
            "query": query or state.get("query"),
            "intent": output.get("intent") or state.get("intent"),
            "chart_type": output.get("chart_type", state.get("chart_type")),
            "title": output.get("title"),
            "description": output.get("description"),
            "metrics": output.get("metrics"),
            "dimensions": output.get("dimensions"),
            "filters": output.get("filters"),
            "top_n": output.get("top_n"),
            "sort": output.get("sort"),
            "visual": output.get("visual"),
            "data_source": output.get("data_source") or state.get("data_source"),
            "output": output.get("output"),
            "message": output.get("message"),
            "parsed_entities": state.get("parsed_entities"),
            "mapped_columns": state.get("mapped_columns"),
        }

        try:
            formatted = format_output(payload)

            workflow_errors = state.get("errors") or []
            output_errors = output.get("errors") or []
            combined_errors = [*workflow_errors, *output_errors]

            if combined_errors:
                formatted["errors"] = combined_errors
                formatted["processing_errors"] = combined_errors
                if not formatted.get("message"):
                    formatted["message"] = (
                        "Query processed with warnings. "
                        "See processing_errors for details."
                    )

            return formatted

        except Exception as e:
            logger.error(f"Output validation failed: {str(e)}")
            raise ValueError(f"Invalid output format: {str(e)}")
    
    def classify_batch(
        self,
        queries: list[str],
        context: Optional[Dict[str, Any]] = None
    ) -> list[Dict[str, Any]]:
        """
        Classify multiple queries in batch.
        
        This method processes multiple queries sequentially. For large
        batches, consider implementing parallel processing.
        
        Args:
            queries: List of natural language queries
            context: Optional context applied to all queries
        
        Returns:
            List of chart specifications, one per query
        
        Example:
            >>> agent = GraphicClassifierAgent()
            >>> queries = [
            ...     "top 5 produtos",
            ...     "vendas por mês",
            ...     "distribuição de clientes"
            ... ]
            >>> results = agent.classify_batch(queries)
            >>> len(results)
            3
        """
        logger.info(f"Processing batch of {len(queries)} queries")
        
        results = []
        for i, query in enumerate(queries, 1):
            logger.debug(f"Batch processing: {i}/{len(queries)}")
            result = self.classify(query, context=context)
            results.append(result)
        
        logger.info(f"Batch processing complete: {len(results)} results")
        return results
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get agent usage statistics.
        
        Returns:
            Dictionary with statistics:
                - total_queries: Total queries processed
                - error_count: Number of errors encountered
                - error_rate: Percentage of errors
                - success_rate: Percentage of successful classifications
        """
        total = self._query_count
        errors = self._error_count
        
        return {
            "total_queries": total,
            "successful_queries": total - errors,
            "error_count": errors,
            "error_rate": (errors / total * 100) if total > 0 else 0.0,
            "success_rate": ((total - errors) / total * 100) if total > 0 else 0.0
        }
    
    def reset_statistics(self) -> None:
        """Reset usage statistics counters."""
        self._query_count = 0
        self._error_count = 0
        logger.info("Statistics reset")
    
    def _create_error_response(self, error_message: str) -> Dict[str, Any]:
        """
        Create a standardized error response.
        
        Args:
            error_message: Error description
        
        Returns:
            Error response dictionary with chart_type=None
        """
        error_output = create_error_output(error_message)
        response = error_output.model_dump(exclude_none=False)
        response["errors"] = [error_message]
        return response
    
    def __repr__(self) -> str:
        """String representation of the agent."""
        return (
            f"GraphicClassifierAgent("
            f"queries={self._query_count}, "
            f"errors={self._error_count}, "
            f"alias_path='{Path(self.alias_path).name}'"
            f")"
        )


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

# Global agent instance (lazy-loaded)
_global_agent: Optional[GraphicClassifierAgent] = None


def get_agent() -> GraphicClassifierAgent:
    """
    Get or create the global agent instance.
    
    This function provides a singleton pattern for cases where
    you want to reuse the same agent across multiple calls.
    
    Returns:
        Global GraphicClassifierAgent instance
    
    Example:
        >>> from src.graphic_classifier.agent import get_agent
        >>> agent = get_agent()
        >>> result = agent.classify("top 5 produtos")
    """
    global _global_agent
    
    if _global_agent is None:
        logger.info("Creating global agent instance")
        _global_agent = GraphicClassifierAgent()
    
    return _global_agent


def classify_query(query: str) -> Dict[str, Any]:
    """
    Classify a query using the global agent instance.
    
    This is a convenience function for simple use cases.
    
    Args:
        query: Natural language query
    
    Returns:
        Chart specification dictionary
    
    Example:
        >>> from src.graphic_classifier.agent import classify_query
        >>> result = classify_query("evolução de vendas por mês")
        >>> print(result['chart_type'])
        'line'
    """
    agent = get_agent()
    return agent.classify(query)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "GraphicClassifierAgent",
    "get_agent",
    "classify_query"
]

