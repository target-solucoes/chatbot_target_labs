"""
Filter Manager for CRUD operations on filters.

This module provides the FilterManager class responsible for applying
CRUD operations (ADICIONAR, ALTERAR, REMOVER, MANTER) to filter sets.
"""

import logging
from typing import Dict, Any, Callable
from copy import deepcopy

from src.filter_classifier.core.settings import OPERATION_PRECEDENCE

logger = logging.getLogger(__name__)


class FilterManager:
    """
    Manages CRUD operations on filter sets.

    This class applies filter operations in the correct precedence order
    and generates the final consolidated filter set.
    """

    def __init__(self):
        """Initialize the FilterManager with operation handlers."""
        self.operations_map: Dict[str, Callable] = {
            "ADICIONAR": self._add_filters,
            "ALTERAR": self._update_filters,
            "REMOVER": self._remove_filters,
            "MANTER": self._keep_filters
        }
        logger.debug("[FilterManager] Initialized with operation handlers")

    def apply_operations(
        self,
        current_filters: Dict[str, Any],
        operations: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Apply CRUD operations to current filters and return final filter set.

        Operations are applied in precedence order: REMOVER → ALTERAR → ADICIONAR → MANTER

        Args:
            current_filters: Currently active filters
            operations: CRUD operations to apply

        Returns:
            Final consolidated filter set after all operations

        Examples:
            >>> manager = FilterManager()
            >>> current = {"UF_Cliente": "SP", "Ano": 2015}
            >>> ops = {"ALTERAR": {"UF_Cliente": {"from": "SP", "to": "SC"}}}
            >>> manager.apply_operations(current, ops)
            {'UF_Cliente': 'SC', 'Ano': 2015}
        """
        logger.info("[FilterManager] Applying filter operations")
        logger.debug(f"[FilterManager] Current filters: {current_filters}")
        logger.debug(f"[FilterManager] Operations: {operations}")

        result = deepcopy(current_filters)

        # Apply operations in precedence order
        for operation_type in OPERATION_PRECEDENCE:
            if operation_type in operations and operations[operation_type]:
                handler = self.operations_map.get(operation_type)
                if handler:
                    logger.debug(f"[FilterManager] Applying {operation_type}: {operations[operation_type]}")
                    result = handler(result, operations[operation_type])
                else:
                    logger.warning(f"[FilterManager] No handler for operation: {operation_type}")

        logger.info(f"[FilterManager] Final filters: {result}")
        return result

    def _add_filters(self, current: Dict[str, Any], new_filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add new filters to current filter set.

        Args:
            current: Current filters
            new_filters: Filters to add

        Returns:
            Updated filter set with new filters added
        """
        result = deepcopy(current)

        for column, value in new_filters.items():
            if column in result:
                logger.warning(f"[FilterManager] Column '{column}' already exists, overwriting")
            result[column] = value
            logger.debug(f"[FilterManager] Added filter: {column} = {value}")

        return result

    def _update_filters(self, current: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update existing filters with new values.

        Args:
            current: Current filters
            updates: Updates in format {column: {"from": old_value, "to": new_value}}
                    or {column: new_value} for simple updates

        Returns:
            Updated filter set with modified values
        """
        result = deepcopy(current)

        for column, update_spec in updates.items():
            if isinstance(update_spec, dict) and "to" in update_spec:
                # Structured update with from/to
                new_value = update_spec["to"]
                old_value = update_spec.get("from")

                if column in result:
                    if old_value is None or result[column] == old_value:
                        result[column] = new_value
                        logger.debug(f"[FilterManager] Updated filter: {column} from {old_value} to {new_value}")
                    else:
                        logger.warning(
                            f"[FilterManager] Cannot update {column}: "
                            f"current value {result[column]} != expected {old_value}"
                        )
                else:
                    # Column doesn't exist, treat as add
                    result[column] = new_value
                    logger.debug(f"[FilterManager] Added new filter during update: {column} = {new_value}")
            else:
                # Simple update (direct value)
                result[column] = update_spec
                logger.debug(f"[FilterManager] Updated filter: {column} = {update_spec}")

        return result

    def _remove_filters(self, current: Dict[str, Any], to_remove: Dict[str, Any]) -> Dict[str, Any]:
        """
        Remove specified filters from current filter set.

        Args:
            current: Current filters
            to_remove: Filters to remove (can be dict with values or just column names)

        Returns:
            Updated filter set with specified filters removed
        """
        result = deepcopy(current)

        # Support both dict format and list format for removal
        if isinstance(to_remove, dict):
            columns_to_remove = list(to_remove.keys())
        elif isinstance(to_remove, list):
            columns_to_remove = to_remove
        else:
            logger.error(f"[FilterManager] Invalid remove format: {type(to_remove)}")
            return result

        for column in columns_to_remove:
            if column in result:
                removed_value = result.pop(column)
                logger.debug(f"[FilterManager] Removed filter: {column} (was {removed_value})")
            else:
                logger.warning(f"[FilterManager] Cannot remove {column}: not in current filters")

        return result

    def _keep_filters(self, current: Dict[str, Any], to_keep: Dict[str, Any]) -> Dict[str, Any]:
        """
        Keep filters unchanged (no-op operation for documentation).

        Args:
            current: Current filters
            to_keep: Filters to keep (for documentation only)

        Returns:
            Unchanged filter set
        """
        logger.debug(f"[FilterManager] Keeping filters: {list(to_keep.keys())}")
        return current

    def validate_operations(self, operations: Dict[str, Any]) -> bool:
        """
        Validate that operation structure is correct.

        Args:
            operations: CRUD operations to validate

        Returns:
            True if valid, False otherwise
        """
        valid_ops = set(self.operations_map.keys())
        provided_ops = set(operations.keys())

        invalid_ops = provided_ops - valid_ops
        if invalid_ops:
            logger.error(f"[FilterManager] Invalid operations: {invalid_ops}")
            return False

        # Validate structure of each operation
        for op_type, op_data in operations.items():
            if not isinstance(op_data, dict):
                logger.error(f"[FilterManager] Operation {op_type} must be a dict, got {type(op_data)}")
                return False

        return True

    def get_operation_summary(self, operations: Dict[str, Any]) -> Dict[str, int]:
        """
        Get summary statistics for operations.

        Args:
            operations: CRUD operations

        Returns:
            Dictionary with count of each operation type
        """
        summary = {}
        for op_type in self.operations_map.keys():
            count = len(operations.get(op_type, {}))
            summary[op_type] = count

        return summary
