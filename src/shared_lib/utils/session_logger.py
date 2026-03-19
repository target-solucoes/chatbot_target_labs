"""
Session Logger for Structured JSON Logging

Manages per-session logging in structured JSON format with atomic writes.
Includes automatic synchronization to Supabase (if configured).
"""

import json
import os
import tempfile
import shutil
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class SessionLogger:
    """
    Manages logging of user sessions in structured JSON format.

    Each session creates a single JSON file with summarized metrics,
    while full formatter outputs are saved in separate files for reference.
    """

    def __init__(
        self, session_id: str, user_email: str, logs_base_dir: str = "logs/sessions"
    ):
        """
        Initialize SessionLogger.

        Args:
            session_id: Unique session identifier (UUID + timestamp)
            user_email: Authenticated user email
            logs_base_dir: Base directory for session logs
        """
        self.session_id = session_id
        self.user_email = user_email
        self.logs_base_dir = Path(logs_base_dir)

        # Organize logs by date
        today = datetime.now().strftime("%Y-%m-%d")
        self.session_dir = self.logs_base_dir / today
        self.outputs_dir = self.session_dir / "outputs"

        # Ensure directories exist
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.outputs_dir.mkdir(parents=True, exist_ok=True)

        # Session file path
        self.session_file = self.session_dir / f"session_{self.session_id}.json"

    def create_session_file(self) -> None:
        """
        Create initial session file with metadata.

        This should be called once when the session is initialized.
        """
        initial_data = {
            "session_metadata": {
                "session_id": self.session_id,
                "user_email": self.user_email,
                "session_start": datetime.now().isoformat(),
                "session_last_update": datetime.now().isoformat(),
                "total_queries": 0,
                "session_status": "active",
            },
            "queries": [],
            "session_summary": {
                "total_execution_time": 0.0,
                "average_query_time": 0.0,
                "total_successful_queries": 0,
                "total_failed_queries": 0,
                "chart_types_used": [],
                "unique_filters_used": [],
            },
        }

        try:
            self._write_session_data(initial_data)
            logger.info(f"Session file created: {self.session_file}")
        except Exception as e:
            logger.error(f"Failed to create session file: {e}")
            raise

    def log_query(
        self,
        query_data: Dict[str, Any],
        output_data: Optional[Dict[str, Any]] = None,
        output_type: str = "formatter",
    ) -> None:
        """
        Log a query to the session file.

        Supports both formatter_output and non_graph_output.

        Args:
            query_data: Summarized query data (from QueryDataExtractor)
            output_data: Full output data (formatter or non_graph)
            output_type: Type of output ("formatter" or "non_graph")

        Raises:
            RuntimeError: If session is closed and cannot accept new queries
        """
        try:
            # VALIDATE SESSION IS ACTIVE
            if not self.is_session_active():
                raise RuntimeError(
                    f"Cannot log query to closed session {self.session_id}. "
                    f"Session status: {self.get_session_status()}"
                )

            # Read current session data
            session_data = self._read_session_data()

            # Save full output separately (if provided)
            if output_data:
                query_id = query_data.get("query_id", len(session_data["queries"]) + 1)
                output_reference = self._save_output(query_id, output_data, output_type)

                # Use appropriate reference field name
                if output_type == "non_graph":
                    query_data["non_graph_output_reference"] = output_reference
                    query_data["formatter_output_reference"] = None
                else:
                    query_data["formatter_output_reference"] = output_reference
                    query_data["non_graph_output_reference"] = None
            else:
                query_data["formatter_output_reference"] = None
                query_data["non_graph_output_reference"] = None

            # Add query to session
            session_data["queries"].append(query_data)

            # Update metadata
            session_data["session_metadata"]["session_last_update"] = (
                datetime.now().isoformat()
            )
            session_data["session_metadata"]["total_queries"] = len(
                session_data["queries"]
            )

            # Recalculate session summary
            session_data["session_summary"] = self._calculate_session_summary(
                session_data["queries"]
            )

            # Write updated data
            self._write_session_data(session_data)

            logger.debug(
                f"Query {query_data.get('query_id')} ({output_type}) logged to session {self.session_id}"
            )

        except RuntimeError:
            # Re-raise RuntimeError (e.g., session closed) - this should break the flow
            raise
        except Exception as e:
            logger.error(f"Failed to log query: {e}")
            # Don't raise - logging should never break the application

    def close_session(self) -> None:
        """
        Mark session as closed.

        This should be called when the user logs out.
        """
        try:
            session_data = self._read_session_data()
            session_data["session_metadata"]["session_status"] = "closed"
            session_data["session_metadata"]["session_end"] = datetime.now().isoformat()
            session_data["session_metadata"]["session_last_update"] = (
                datetime.now().isoformat()
            )

            self._write_session_data(session_data)
            logger.info(f"Session {self.session_id} closed")

        except Exception as e:
            logger.error(f"Failed to close session: {e}")

    def is_session_active(self) -> bool:
        """
        Check if session is still active (not closed).

        Returns:
            True if session status is "active", False otherwise
        """
        try:
            session_data = self._read_session_data()
            status = session_data.get("session_metadata", {}).get("session_status")
            return status == "active"
        except Exception:
            return False

    def get_session_status(self) -> str:
        """
        Get current session status.

        Returns:
            Session status string ("active", "closed", "unknown", or "error")
        """
        try:
            session_data = self._read_session_data()
            return session_data.get("session_metadata", {}).get(
                "session_status", "unknown"
            )
        except Exception:
            return "error"

    def _save_output(
        self, query_id: int, output_data: Dict, output_type: str = "formatter"
    ) -> str:
        """
        Save full output to a separate file.

        Supports both formatter_output and non_graph_output.

        Args:
            query_id: Query ID number
            output_data: Complete output dictionary
            output_type: Type of output ("formatter" or "non_graph")

        Returns:
            Relative path to the saved output file
        """
        # Filename: {session_id}_query_{query_id}_{output_type}_output.json
        output_filename = (
            f"{self.session_id}_query_{query_id}_{output_type}_output.json"
        )
        output_path = self.outputs_dir / output_filename

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)

            # Return path as string (relative from logs/sessions/)
            return str(output_path)

        except Exception as e:
            logger.error(f"Failed to save {output_type} output: {e}")
            return None

    def _save_formatter_output(self, query_id: int, formatter_output: Dict) -> str:
        """
        DEPRECATED: Use _save_output instead.

        Save full formatter output to a separate file.
        Maintained for backward compatibility.

        Args:
            query_id: Query ID number
            formatter_output: Complete formatter output dictionary

        Returns:
            Relative path to the saved output file
        """
        return self._save_output(query_id, formatter_output, "formatter")

    def _read_session_data(self) -> Dict:
        """
        Read session data from file.

        Returns:
            Session data dictionary

        Raises:
            FileNotFoundError: If session file doesn't exist
            json.JSONDecodeError: If file is corrupted
        """
        if not self.session_file.exists():
            raise FileNotFoundError(f"Session file not found: {self.session_file}")

        with open(self.session_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def _write_session_data(self, data: Dict) -> None:
        """
        Write session data using atomic write operation.

        Uses temporary file + atomic rename to prevent corruption.
        After successful write, syncs to Supabase (if configured).

        Args:
            data: Session data dictionary

        Raises:
            RuntimeError: If write fails
        """
        # Create temporary file in same directory as target
        temp_fd, temp_path = tempfile.mkstemp(
            suffix=".json.tmp", dir=self.session_file.parent, text=True
        )

        try:
            # Write JSON to temporary file
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            # Validate JSON by reading it back
            with open(temp_path, "r", encoding="utf-8") as f:
                json.load(f)

            # Atomic rename (OS-level atomic operation)
            shutil.move(temp_path, self.session_file)

            # Sync to Supabase after successful local save
            self._sync_to_supabase(data)

        except Exception as e:
            # Clean up temporary file if it exists
            if os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except Exception:
                    pass
            raise RuntimeError(f"Failed to write session data: {e}")

    def _sync_to_supabase(self, data: Dict) -> None:
        """
        Sync session data to Supabase (if configured).

        Runs asynchronously to not block the main application.
        Errors are logged but do not raise exceptions.

        Args:
            data: Session data dictionary
        """
        try:
            from .logger_supabase import sync_log_to_supabase

            # Non-blocking sync - errors won't crash the app
            sync_log_to_supabase(data)

        except ImportError:
            # Supabase package not installed - skip silently
            pass
        except Exception as e:
            # Log error but don't propagate - sync is not critical
            logger.debug(f"Supabase sync skipped: {e}")

    def _calculate_session_summary(self, queries: List[Dict]) -> Dict:
        """
        Calculate session summary statistics.

        Supports both formatter and non_graph queries.

        Args:
            queries: List of query data dictionaries

        Returns:
            Session summary dictionary
        """
        if not queries:
            return {
                "total_execution_time": 0.0,
                "average_query_time": 0.0,
                "total_successful_queries": 0,
                "total_failed_queries": 0,
                "chart_types_used": [],
                "query_types_used": [],
                "unique_filters_used": [],

                # NOVO: Tokens zerados
                "total_input_tokens": 0,
                "total_output_tokens": 0,
                "total_tokens": 0,
                "tokens_by_agent": {}
            }

        # Calculate metrics
        successful_queries = [q for q in queries if q.get("status") == "success"]
        failed_queries = [q for q in queries if q.get("status") == "error"]

        total_execution_time = sum(q.get("execution_time", 0.0) for q in queries)
        average_query_time = total_execution_time / len(queries) if queries else 0.0

        # Extract chart types (for formatter queries)
        chart_types = []
        for q in successful_queries:
            if q.get("output_type") == "formatter":
                chart_type = q.get("chart_type")
                if chart_type and chart_type not in chart_types:
                    chart_types.append(chart_type)

        # Extract query types (for non_graph queries)
        query_types = []
        for q in successful_queries:
            if q.get("output_type") == "non_graph":
                query_type = q.get("query_type")
                if query_type and query_type not in query_types:
                    query_types.append(query_type)

        # Extract unique filters
        unique_filters = []
        for q in successful_queries:
            filters_applied = q.get("filters_applied", {})
            for filter_key in filters_applied.keys():
                if filter_key not in unique_filters:
                    unique_filters.append(filter_key)

        # NOVO: Agregar tokens de todas as queries
        total_input_tokens = 0
        total_output_tokens = 0
        total_tokens = 0
        tokens_by_agent = {}

        for query in queries:
            # Agregar totais
            total_input_tokens += query.get("total_input_tokens", 0)
            total_output_tokens += query.get("total_output_tokens", 0)
            total_tokens += query.get("total_tokens", 0)

            # Agregar por agente
            agent_tokens = query.get("token_usage_by_agent", {})
            for agent_name, tokens in agent_tokens.items():
                if agent_name not in tokens_by_agent:
                    tokens_by_agent[agent_name] = {
                        "input_tokens": 0,
                        "output_tokens": 0,
                        "total_tokens": 0
                    }

                tokens_by_agent[agent_name]["input_tokens"] += tokens.get("input_tokens", 0)
                tokens_by_agent[agent_name]["output_tokens"] += tokens.get("output_tokens", 0)
                tokens_by_agent[agent_name]["total_tokens"] += tokens.get("total_tokens", 0)

        return {
            "total_execution_time": round(total_execution_time, 2),
            "average_query_time": round(average_query_time, 2),
            "total_successful_queries": len(successful_queries),
            "total_failed_queries": len(failed_queries),
            "chart_types_used": chart_types,
            "query_types_used": query_types,
            "unique_filters_used": unique_filters,

            # NOVO: Tokens agregados
            "total_input_tokens": total_input_tokens,
            "total_output_tokens": total_output_tokens,
            "total_tokens": total_tokens,
            "tokens_by_agent": tokens_by_agent
        }

    def get_session_file_path(self) -> Path:
        """Get the path to the session file."""
        return self.session_file


__all__ = ["SessionLogger"]
