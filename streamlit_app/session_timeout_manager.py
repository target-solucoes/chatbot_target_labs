"""Session timeout management utilities for the Streamlit application."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import streamlit as st

from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class SessionTimeoutManager:
    """Server-side guard that tracks inactivity and enforces timeouts."""

    DEFAULT_TIMEOUT_MINUTES = 30
    WARNING_BEFORE_TIMEOUT_MINUTES = 5

    def __init__(self, timeout_minutes: Optional[int] = None) -> None:
        """Initialize timeout configuration and baseline state."""
        self.timeout_minutes = timeout_minutes or self.DEFAULT_TIMEOUT_MINUTES
        self.timeout_delta = timedelta(minutes=self.timeout_minutes)

        warning_lead = min(
            self.WARNING_BEFORE_TIMEOUT_MINUTES,
            max(self.timeout_minutes * 0.5, 0.5),
        )
        self.warning_start_minutes = max(self.timeout_minutes - warning_lead, 0.5)
        self.warning_delta = timedelta(minutes=self.warning_start_minutes)

        if "last_activity" not in st.session_state:
            st.session_state["last_activity"] = datetime.now(timezone.utc)

        if "timeout_warning_shown" not in st.session_state:
            st.session_state["timeout_warning_shown"] = False

    def update_activity(self, activity_ts: Optional[str] = None) -> None:
        """Record the most recent interaction timestamp."""
        new_activity = datetime.now(timezone.utc)

        if activity_ts:
            parsed = _parse_timestamp(activity_ts)
            if parsed:
                new_activity = parsed

        previous: Optional[datetime] = st.session_state.get("last_activity")
        st.session_state["last_activity"] = new_activity

        if previous is None or new_activity > previous:
            st.session_state["timeout_warning_shown"] = False

    def get_time_since_last_activity(self) -> timedelta:
        """Return how long the session has been idle."""
        last_activity: Optional[datetime] = st.session_state.get("last_activity")
        if last_activity is None:
            return timedelta(0)
        return datetime.now(timezone.utc) - last_activity

    def is_session_active(self) -> bool:
        """Determine if the session is still within the allowed idle window."""
        return self.get_time_since_last_activity() < self.timeout_delta

    def get_remaining_time(self) -> timedelta:
        """Return remaining time before automatic logout."""
        return self.timeout_delta - self.get_time_since_last_activity()

    def should_show_warning(self) -> bool:
        """Check whether the warning banner should be displayed."""
        time_since_activity = self.get_time_since_last_activity()
        warning_shown = st.session_state.get("timeout_warning_shown", False)
        return (
            time_since_activity >= self.warning_delta
            and time_since_activity < self.timeout_delta
            and not warning_shown
        )

    def render_warning(self, minutes_left: Optional[float] = None) -> None:
        """Render a Streamlit warning alert with remaining minutes."""
        if minutes_left is not None:
            remaining_minutes = max(int(minutes_left), 0)
        else:
            remaining_minutes = max(
                int(self.get_remaining_time().total_seconds() // 60),
                0,
            )
        st.warning(
            "**Inatividade detectada**\n\n"
            f"Sua sessão será encerrada automaticamente em **{remaining_minutes} minutos**.\n\n"
            "Envie uma nova pergunta para mantê-la ativa."
        )
        st.session_state["timeout_warning_shown"] = True

    def handle_timeout(self) -> None:
        """Close the Streamlit session and clear authentication state."""
        session_id = st.session_state.get("session_id")
        try:
            logger.info("Session timeout triggered for session %s", session_id)

            session_logger = st.session_state.get("session_logger")
            if session_logger:
                session_logger.close_session()
                logger.info("Session logger closed during timeout")

            st.session_state["logout_reason"] = "timeout"
            st.session_state["logout_timestamp"] = datetime.now(timezone.utc).isoformat()

            from streamlit_app.email_auth import EmailAuthComponent

            EmailAuthComponent.logout()
            logger.info("User logged out due to timeout")
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("Error handling session timeout: %s", exc)

    def check_and_handle_timeout(self) -> bool:
        """Check inactivity budget and enforce logout when exceeded."""
        if not self.is_session_active():
            self.handle_timeout()
            return True

        if self.should_show_warning():
            self.render_warning()

        return False


def _parse_timestamp(timestamp: Optional[str]) -> Optional[datetime]:
    if not timestamp:
        return None

    sanitized = timestamp.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(sanitized)
    except ValueError:
        logger.debug("Invalid client timestamp received: %s", timestamp)
        return None
