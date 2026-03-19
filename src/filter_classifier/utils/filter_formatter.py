"""
Filter Formatter for output formatting.

This module provides the FilterFormatter class responsible for formatting
the final output JSON with CRUD operations and metadata.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime

from src.filter_classifier.models.filter_state import (
    FilterGraphState,
    FilterOutput,
    FilterOutputMetadata
)

logger = logging.getLogger(__name__)


class FilterFormatter:
    """
    Formats filter output into standardized JSON structure.

    This class converts FilterGraphState into a structured FilterOutput
    with CRUD operations, final filters, and metadata.
    """

    def __init__(self):
        """Initialize the FilterFormatter."""
        logger.debug("[FilterFormatter] Initialized")

    def format_output(self, state: FilterGraphState) -> Dict[str, Any]:
        """
        Format filter state into final output structure.

        Args:
            state: Current FilterGraphState

        Returns:
            Formatted output dictionary with CRUD operations and metadata

        Examples:
            >>> formatter = FilterFormatter()
            >>> state = {...}
            >>> output = formatter.format_output(state)
            >>> print(output.keys())
            dict_keys(['ADICIONAR', 'MANTER', 'ALTERAR', 'REMOVER', 'filter_final', 'metadata'])
        """
        logger.info("[FilterFormatter] Formatting filter output")

        # Extract operations from state
        operations = state.get("filter_operations", {})

        # Create output structure
        output_dict = {
            "ADICIONAR": operations.get("ADICIONAR", {}),
            "MANTER": operations.get("MANTER", {}),
            "ALTERAR": operations.get("ALTERAR", {}),
            "REMOVER": operations.get("REMOVER", {}),
            "filter_final": state.get("filter_final", {}),
            "metadata": self._create_metadata(state)
        }

        # Validate with Pydantic if possible
        try:
            validated_output = FilterOutput(**output_dict)
            output_dict = validated_output.model_dump()
            logger.debug("[FilterFormatter] Output validated with Pydantic")
        except Exception as e:
            logger.warning(f"[FilterFormatter] Pydantic validation failed: {str(e)}")
            # Return unvalidated output
            pass

        logger.info(
            f"[FilterFormatter] Output formatted: "
            f"{len(output_dict['filter_final'])} final filters, "
            f"status: {output_dict['metadata'].get('status', 'unknown')}"
        )

        return output_dict

    def _create_metadata(self, state: FilterGraphState) -> Dict[str, Any]:
        """
        Create metadata dictionary from state.

        Args:
            state: Current FilterGraphState

        Returns:
            Metadata dictionary
        """
        errors = state.get("errors", [])

        # Determine status
        if errors:
            status = "error" if any("error" in err.lower() for err in errors) else "partial"
        else:
            status = "success"

        metadata = {
            "confidence": state.get("filter_confidence", 0.0),
            "timestamp": datetime.now().isoformat(),
            "columns_detected": state.get("detected_filter_columns", []),
            "errors": errors,
            "status": status
        }

        # Validate with Pydantic
        try:
            validated_metadata = FilterOutputMetadata(**metadata)
            metadata = validated_metadata.model_dump()
        except Exception as e:
            logger.warning(f"[FilterFormatter] Metadata validation failed: {str(e)}")

        return metadata

    def format_error_response(self, error_message: str, state: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Format an error response.

        Args:
            error_message: Error message to include
            state: Optional partial state

        Returns:
            Error response dictionary

        Examples:
            >>> formatter = FilterFormatter()
            >>> error_output = formatter.format_error_response("Column not found")
            >>> print(error_output['metadata']['status'])
            'error'
        """
        logger.info(f"[FilterFormatter] Formatting error response: {error_message}")

        output_dict = {
            "ADICIONAR": {},
            "MANTER": {},
            "ALTERAR": {},
            "REMOVER": {},
            "filter_final": state.get("filter_final", {}) if state else {},
            "metadata": {
                "confidence": 0.0,
                "timestamp": datetime.now().isoformat(),
                "columns_detected": state.get("detected_filter_columns", []) if state else [],
                "errors": [error_message],
                "status": "error"
            }
        }

        return output_dict

    def format_success_response(
        self,
        operations: Dict[str, Any],
        filter_final: Dict[str, Any],
        confidence: float = 1.0,
        detected_columns: Optional[list] = None
    ) -> Dict[str, Any]:
        """
        Format a success response with explicit parameters.

        Args:
            operations: CRUD operations dict
            filter_final: Final consolidated filters
            confidence: Confidence score (0.0 to 1.0)
            detected_columns: List of detected column names

        Returns:
            Success response dictionary

        Examples:
            >>> formatter = FilterFormatter()
            >>> output = formatter.format_success_response(
            ...     operations={"ADICIONAR": {"UF": "SP"}},
            ...     filter_final={"UF": "SP"},
            ...     confidence=0.95
            ... )
        """
        logger.info("[FilterFormatter] Formatting success response")

        output_dict = {
            "ADICIONAR": operations.get("ADICIONAR", {}),
            "MANTER": operations.get("MANTER", {}),
            "ALTERAR": operations.get("ALTERAR", {}),
            "REMOVER": operations.get("REMOVER", {}),
            "filter_final": filter_final,
            "metadata": {
                "confidence": confidence,
                "timestamp": datetime.now().isoformat(),
                "columns_detected": detected_columns or [],
                "errors": [],
                "status": "success"
            }
        }

        return output_dict

    def add_metadata_field(self, output: Dict[str, Any], key: str, value: Any) -> Dict[str, Any]:
        """
        Add a custom field to metadata.

        Args:
            output: Output dictionary
            key: Metadata key
            value: Metadata value

        Returns:
            Updated output dictionary
        """
        if "metadata" not in output:
            output["metadata"] = {}

        output["metadata"][key] = value
        logger.debug(f"[FilterFormatter] Added metadata field: {key} = {value}")

        return output

    def validate_output_structure(self, output: Dict[str, Any]) -> bool:
        """
        Validate that output has the correct structure.

        Args:
            output: Output dictionary to validate

        Returns:
            True if valid, False otherwise
        """
        required_keys = ["ADICIONAR", "MANTER", "ALTERAR", "REMOVER", "filter_final", "metadata"]

        for key in required_keys:
            if key not in output:
                logger.error(f"[FilterFormatter] Missing required key: {key}")
                return False

        required_metadata_keys = ["confidence", "timestamp", "columns_detected", "errors", "status"]
        metadata = output.get("metadata", {})

        for key in required_metadata_keys:
            if key not in metadata:
                logger.warning(f"[FilterFormatter] Missing metadata key: {key}")

        return True

    def get_operation_summary(self, output: Dict[str, Any]) -> str:
        """
        Get a human-readable summary of operations.

        Args:
            output: Formatted output dictionary

        Returns:
            Summary string

        Examples:
            >>> formatter = FilterFormatter()
            >>> summary = formatter.get_operation_summary(output)
            >>> print(summary)
            'Added 1 filter(s), altered 0 filter(s), removed 0 filter(s)'
        """
        added = len(output.get("ADICIONAR", {}))
        altered = len(output.get("ALTERAR", {}))
        removed = len(output.get("REMOVER", {}))
        maintained = len(output.get("MANTER", {}))

        summary = (
            f"Added {added} filter(s), "
            f"altered {altered} filter(s), "
            f"removed {removed} filter(s), "
            f"maintained {maintained} filter(s)"
        )

        return summary

    def __repr__(self) -> str:
        """String representation of FilterFormatter."""
        return "FilterFormatter()"
