"""
Filter Persistence for saving and loading filter state.

This module provides the FilterPersistence class responsible for managing
filter state persistence between sessions.
"""

import json
import logging
import warnings
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime, timedelta
import uuid

from src.filter_classifier.core.settings import STORAGE_PATH, SESSION_TIMEOUT_MINUTES

logger = logging.getLogger(__name__)

warnings.warn(
    "FilterPersistence está DEPRECATED e será removido em uma versão futura. "
    "Utilize st.session_state para isolamento de filtros.",
    DeprecationWarning,
    stacklevel=2,
)


class FilterPersistence:
    """
    Manages persistence of filter state between sessions.

    This class handles saving and loading filter state to/from JSON files,
    with support for session expiration and cleanup.
    """

    def __init__(
        self,
        storage_path: str = STORAGE_PATH,
        session_timeout_minutes: int = SESSION_TIMEOUT_MINUTES,
    ):
        """
        Initialize the FilterPersistence.

        Args:
            storage_path: Path to the filter state storage file
            session_timeout_minutes: Session timeout in minutes
        """
        self.storage_path = Path(storage_path)
        self.session_timeout = timedelta(minutes=session_timeout_minutes)
        self._session_id: Optional[str] = None

        logger.info(
            f"[FilterPersistence] Initialized with storage: {self.storage_path}, "
            f"timeout: {session_timeout_minutes}m"
        )

    def load(self) -> Dict[str, Any]:
        """
        Load filter state from storage.

        Returns:
            Dictionary containing:
                - filter_final: Final consolidated filters
                - filter_history: Historical filter records
                - session_id: Session identifier
                - timestamp: Last update timestamp

            Returns empty state if file doesn't exist or session expired.

        Examples:
            >>> persistence = FilterPersistence()
            >>> state = persistence.load()
            >>> print(state.get("filter_final", {}))
            {'UF_Cliente': 'SP', 'Ano': 2015}
        """
        logger.info("[FilterPersistence] Loading filter state")

        # Check if storage file exists
        if not self.storage_path.exists():
            logger.info(
                "[FilterPersistence] No existing state file, returning empty state"
            )
            return self._create_empty_state()

        try:
            # Read state from file
            with open(self.storage_path, "r", encoding="utf-8") as f:
                state = json.load(f)

            # Check session expiration
            if self.is_session_expired(state):
                logger.info(
                    "[FilterPersistence] Session expired, returning empty state"
                )
                self.clear()
                return self._create_empty_state()

            # Update session ID
            self._session_id = state.get("session_id")

            # Restore types from metadata
            filter_final = state.get("filter_final", {})
            filter_final_restored = self._restore_types(filter_final)
            state["filter_final"] = filter_final_restored

            logger.info(
                f"[FilterPersistence] Loaded state with {len(filter_final_restored)} filters, "
                f"session: {self._session_id}"
            )

            return state

        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"[FilterPersistence] Error loading state: {str(e)}")
            return self._create_empty_state()

    def save(
        self, filter_final: Dict[str, Any], filter_history: List[Dict[str, Any]]
    ) -> None:
        """
        Save filter state to storage.

        Args:
            filter_final: Final consolidated filters
            filter_history: Historical filter records

        Examples:
            >>> persistence = FilterPersistence()
            >>> persistence.save(
            ...     filter_final={"UF_Cliente": "SP"},
            ...     filter_history=[{"timestamp": "...", "filters": {...}}]
            ... )
        """
        logger.info("[FilterPersistence] Saving filter state")

        # Generate session ID if not exists
        if not self._session_id:
            self._session_id = self._generate_session_id()

        # Add type metadata for proper deserialization
        filter_final_with_types = self._add_type_metadata(filter_final)

        # Create state object
        state = {
            "filter_final": filter_final_with_types,
            "filter_history": filter_history,
            "timestamp": datetime.now().isoformat(),
            "session_id": self._session_id,
        }

        try:
            # Ensure parent directory exists
            self.storage_path.parent.mkdir(parents=True, exist_ok=True)

            # Write state to file
            with open(self.storage_path, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, ensure_ascii=False)

            logger.info(
                f"[FilterPersistence] Saved state with {len(filter_final)} filters "
                f"to {self.storage_path}"
            )

        except (IOError, TypeError) as e:
            logger.error(f"[FilterPersistence] Error saving state: {str(e)}")

    def clear(self) -> None:
        """
        Clear filter state (reset).

        Removes the storage file and resets session ID.

        Examples:
            >>> persistence = FilterPersistence()
            >>> persistence.clear()
        """
        logger.info("[FilterPersistence] Clearing filter state")

        if self.storage_path.exists():
            try:
                self.storage_path.unlink()
                logger.info(
                    f"[FilterPersistence] Removed storage file: {self.storage_path}"
                )
            except OSError as e:
                logger.error(
                    f"[FilterPersistence] Error removing storage file: {str(e)}"
                )

        self._session_id = None

    def is_session_expired(self, state: Optional[Dict[str, Any]] = None) -> bool:
        """
        Check if the session has expired.

        Args:
            state: Optional state dict (if not provided, loads from file)

        Returns:
            True if session has expired, False otherwise

        Examples:
            >>> persistence = FilterPersistence(session_timeout_minutes=30)
            >>> persistence.is_session_expired()
            False
        """
        if state is None:
            if not self.storage_path.exists():
                return True

            try:
                with open(self.storage_path, "r", encoding="utf-8") as f:
                    state = json.load(f)
            except (json.JSONDecodeError, IOError):
                return True

        # Get timestamp
        timestamp_str = state.get("timestamp")
        if not timestamp_str:
            logger.warning(
                "[FilterPersistence] No timestamp in state, considering expired"
            )
            return True

        try:
            timestamp = datetime.fromisoformat(timestamp_str)
            age = datetime.now() - timestamp

            is_expired = age > self.session_timeout
            if is_expired:
                logger.info(f"[FilterPersistence] Session expired (age: {age})")

            return is_expired

        except (ValueError, TypeError) as e:
            logger.error(f"[FilterPersistence] Error parsing timestamp: {str(e)}")
            return True

    def get_session_info(self) -> Dict[str, Any]:
        """
        Get information about the current session.

        Returns:
            Dictionary with session metadata

        Examples:
            >>> persistence = FilterPersistence()
            >>> info = persistence.get_session_info()
            >>> print(info["session_id"])
            'abc-123-def-456'
        """
        if not self.storage_path.exists():
            return {
                "exists": False,
                "session_id": None,
                "timestamp": None,
                "expired": True,
            }

        try:
            with open(self.storage_path, "r", encoding="utf-8") as f:
                state = json.load(f)

            return {
                "exists": True,
                "session_id": state.get("session_id"),
                "timestamp": state.get("timestamp"),
                "expired": self.is_session_expired(state),
                "filter_count": len(state.get("filter_final", {})),
                "history_count": len(state.get("filter_history", [])),
            }

        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"[FilterPersistence] Error reading session info: {str(e)}")
            return {
                "exists": True,
                "session_id": None,
                "timestamp": None,
                "expired": True,
                "error": str(e),
            }

    def _create_empty_state(self) -> Dict[str, Any]:
        """
        Create an empty filter state.

        Returns:
            Empty state dictionary
        """
        return {
            "filter_final": {},
            "filter_history": [],
            "timestamp": datetime.now().isoformat(),
            "session_id": self._generate_session_id(),
        }

    def _generate_session_id(self) -> str:
        """
        Generate a unique session identifier.

        Returns:
            UUID-based session ID
        """
        session_id = str(uuid.uuid4())
        logger.debug(f"[FilterPersistence] Generated session ID: {session_id}")
        return session_id

    def _add_type_metadata(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add type metadata to filters for proper serialization/deserialization.

        Converts datetime objects to strings but preserves type information
        so they can be restored correctly on load.

        Args:
            filters: Dictionary of filters

        Returns:
            Dictionary with type metadata added
        """
        import pandas as pd

        result = {}
        for key, value in filters.items():
            # Handle dict with operators (e.g., {"between": [start, end]})
            if isinstance(value, dict):
                if "between" in value:
                    start, end = value["between"]
                    # Convert timestamps to strings
                    if isinstance(start, pd.Timestamp):
                        start = start.isoformat()
                    if isinstance(end, pd.Timestamp):
                        end = end.isoformat()
                    result[key] = {
                        "between": [start, end],
                        "__type__": "datetime_range",
                    }
                else:
                    # Keep other operators as-is
                    result[key] = value
            # Handle list values
            elif isinstance(value, list):
                # Check if list contains timestamps
                converted_list = []
                has_timestamp = False
                for item in value:
                    if isinstance(item, pd.Timestamp):
                        converted_list.append(item.isoformat())
                        has_timestamp = True
                    else:
                        converted_list.append(item)

                if has_timestamp:
                    result[key] = {
                        "values": converted_list,
                        "__type__": "datetime_list",
                    }
                else:
                    result[key] = value
            # Handle single timestamp values
            elif isinstance(value, pd.Timestamp):
                result[key] = {"value": value.isoformat(), "__type__": "datetime"}
            else:
                # Keep other types as-is (strings, numbers, etc.)
                result[key] = value

        return result

    def _restore_types(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Restore original types from metadata after deserialization.

        Converts string representations back to their original types
        (e.g., datetime strings back to pd.Timestamp).

        Args:
            filters: Dictionary with type metadata

        Returns:
            Dictionary with original types restored
        """
        import pandas as pd

        result = {}
        for key, value in filters.items():
            # Check if value has type metadata
            if isinstance(value, dict) and "__type__" in value:
                type_hint = value["__type__"]

                # Restore datetime range
                if type_hint == "datetime_range":
                    start_str, end_str = value["between"]
                    result[key] = {
                        "between": [pd.Timestamp(start_str), pd.Timestamp(end_str)]
                    }
                    logger.debug(
                        f"[FilterPersistence] Restored datetime range for '{key}': "
                        f"[{start_str}, {end_str}]"
                    )

                # Restore datetime list
                elif type_hint == "datetime_list":
                    result[key] = [pd.Timestamp(item) for item in value["values"]]
                    logger.debug(
                        f"[FilterPersistence] Restored datetime list for '{key}' "
                        f"({len(value['values'])} items)"
                    )

                # Restore single datetime
                elif type_hint == "datetime":
                    result[key] = pd.Timestamp(value["value"])
                    logger.debug(
                        f"[FilterPersistence] Restored datetime for '{key}': {value['value']}"
                    )

                else:
                    # Unknown type, keep as-is
                    result[key] = value
            else:
                # No type metadata, keep as-is
                result[key] = value

        return result

    def __repr__(self) -> str:
        """String representation of FilterPersistence."""
        return (
            f"FilterPersistence(storage_path={self.storage_path}, "
            f"timeout={self.session_timeout.total_seconds() / 60:.0f}m)"
        )
