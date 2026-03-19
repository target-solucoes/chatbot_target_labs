"""Client-side session monitoring helpers for Streamlit."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from string import Template
from typing import Any, Dict, Optional

import streamlit as st
import streamlit.components.v1 as components

from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


_SESSION_MONITOR_TEMPLATE = Template(
    """
<script>
(function() {
    const SESSION_ID = "${session_id}";
    const INACTIVITY_TIMEOUT = ${timeout_minutes};
    const HEARTBEAT_INTERVAL = ${heartbeat_ms};
    const WARNING_MARGIN_MINUTES = ${warning_margin};
    const WARNING_TRIGGER_MINUTES = ${warning_trigger_minutes};

    if (!SESSION_ID || SESSION_ID === "unknown") {
        console.warn("[SessionMonitor] Missing session id, skipping monitor init");
        return;
    }

    function createStreamlitBridge() {
        let lastFrameHeight = null;
        let readySent = false;

        function dispatch(type, detail) {
            const event = new CustomEvent(type, { detail: detail || {} });
            window.parent.document.dispatchEvent(event);
        }

        return {
            setComponentReady() {
                if (readySent) {
                    return;
                }
                readySent = true;
                dispatch("streamlit:setComponentReady", {});
            },
            setFrameHeight(height) {
                const targetHeight = typeof height === "number" ? height : document.body.scrollHeight;
                if (targetHeight === lastFrameHeight) {
                    return;
                }
                lastFrameHeight = targetHeight;
                dispatch("streamlit:setFrameHeight", { height: targetHeight });
            },
            setComponentValue(value) {
                dispatch("streamlit:setComponentValue", { value });
            }
        };
    }

    const Streamlit = createStreamlitBridge();

    function initializeMonitor() {
        const STATE = {
            lastActivity: Date.now(),
            warningSent: false,
            hasNotifiedClose: false
        };

        Streamlit.setComponentReady();
        Streamlit.setFrameHeight(0);

        function serializeActivityTs() {
            return new Date(STATE.lastActivity).toISOString();
        }

        function dispatchToStreamlit(payload) {
            const enriched = Object.assign({}, payload, {
                session_id: SESSION_ID,
                timestamp: new Date().toISOString(),
                last_activity_ts: serializeActivityTs()
            });
            Streamlit.setComponentValue(enriched);
        }

        function markActivity() {
            STATE.lastActivity = Date.now();
            STATE.warningSent = false;
            if (STATE.hasNotifiedClose) {
                STATE.hasNotifiedClose = false;
            }
        }

        function sendHeartbeat(reason) {
            dispatchToStreamlit({
                event: "heartbeat",
                reason: reason || "interval"
            });
        }

        function notifyTimeout(inactiveMinutes) {
            dispatchToStreamlit({
                event: "timeout",
                inactive_minutes: inactiveMinutes,
                minutes_until_timeout: 0
            });
        }

        function notifyClose(reason) {
            if (STATE.hasNotifiedClose) {
                return;
            }
            STATE.hasNotifiedClose = true;
            dispatchToStreamlit({
                event: "session_close",
                reason: reason || "page_close"
            });
        }

        const activityEvents = [
            "mousedown",
            "mousemove",
            "keypress",
            "scroll",
            "touchstart",
            "click"
        ];

        activityEvents.forEach(evt => {
            document.addEventListener(evt, markActivity, true);
        });

        // Event: pagehide - Mais confiavel que beforeunload
        // Dispara quando pagina e removida do bfcache
        window.addEventListener("pagehide", (event) => {
            console.log("[SessionMonitor] pagehide detectado, notificando fechamento");
            notifyClose("pagehide");
        }, { capture: true });

        // Event: beforeunload - Classico, menos confiavel
        window.addEventListener("beforeunload", (event) => {
            console.log("[SessionMonitor] beforeunload detectado, notificando fechamento");
            notifyClose("beforeunload");
            // NAO mostrar dialogo de confirmacao
        });

        // Event: unload - Ultima tentativa
        window.addEventListener("unload", () => {
            console.log("[SessionMonitor] unload detectado, notificando fechamento");
            notifyClose("unload");
        });

        document.addEventListener("visibilitychange", () => {
            if (document.hidden) {
                dispatchToStreamlit({
                    event: "page_hidden"
                });
            } else {
                dispatchToStreamlit({
                    event: "page_visible"
                });
            }
        });

        function maybeSendWarning(inactiveMinutes) {
            if (
                STATE.warningSent ||
                inactiveMinutes < WARNING_TRIGGER_MINUTES ||
                inactiveMinutes >= INACTIVITY_TIMEOUT
            ) {
                return;
            }

            STATE.warningSent = true;
            const minutesLeft = Math.max(
                Math.ceil(INACTIVITY_TIMEOUT - inactiveMinutes),
                0
            );

            dispatchToStreamlit({
                event: "timeout_warning",
                inactive_minutes: Math.floor(inactiveMinutes),
                minutes_until_timeout: minutesLeft,
                warning_margin_minutes: WARNING_MARGIN_MINUTES
            });
        }

        function evaluateInactivity() {
            const inactiveMinutes = (Date.now() - STATE.lastActivity) / 60000;
            maybeSendWarning(inactiveMinutes);
            if (inactiveMinutes >= INACTIVITY_TIMEOUT) {
                notifyTimeout(Math.floor(inactiveMinutes));
            }
        }

        setInterval(() => {
            sendHeartbeat("interval");
            evaluateInactivity();
        }, HEARTBEAT_INTERVAL);

        sendHeartbeat("startup");
    }

    initializeMonitor();
})();
</script>
"""
)


@dataclass
class SessionMonitorConfig:
    inactivity_timeout_minutes: int = 30
    heartbeat_interval_seconds: int = 30
    warning_lead_minutes: float = 5.0
    component_key: str = "session-monitor-component"

    @property
    def heartbeat_ms(self) -> int:
        return max(int(self.heartbeat_interval_seconds * 1000), 1000)

    @property
    def warning_trigger_minutes(self) -> float:
        max_lead = min(
            self.warning_lead_minutes,
            max(self.inactivity_timeout_minutes - 0.1, 0.1),
        )
        if max_lead <= 0:
            max_lead = max(self.inactivity_timeout_minutes * 0.5, 0.1)

        trigger = max(self.inactivity_timeout_minutes - max_lead, 0.1)
        return trigger


class SessionMonitorComponent:
    def __init__(self, config: Optional[SessionMonitorConfig] = None) -> None:
        self.config = config or SessionMonitorConfig()
        self._last_heartbeat: Optional[datetime] = None

    def render(self) -> Optional[Dict[str, Any]]:
        session_id = st.session_state.get("session_id", "unknown")
        html_payload = _SESSION_MONITOR_TEMPLATE.safe_substitute(
            session_id=session_id,
            timeout_minutes=self.config.inactivity_timeout_minutes,
            heartbeat_ms=self.config.heartbeat_ms,
            warning_margin=self.config.warning_lead_minutes,
            warning_trigger_minutes=self.config.warning_trigger_minutes,
        )

        event_data = components.html(
            html_payload,
            height=0,
            width=0,
            scrolling=False,
        )

        if not event_data:
            return None

        if not isinstance(event_data, dict):
            logger.debug("Session monitor returned non-dict payload: %s", event_data)
            return None

        return self._process_event(event_data)

    def _process_event(self, event_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        event_type = event_data.get("event")
        if not event_type:
            return None

        if event_type == "heartbeat":
            timestamp = event_data.get("timestamp")
            parsed = _parse_timestamp(timestamp)
            if parsed:
                self._last_heartbeat = parsed
            if timestamp:
                st.session_state["last_client_heartbeat"] = timestamp
            return {
                "action": "update_heartbeat",
                "timestamp": timestamp,
                "last_activity_ts": event_data.get("last_activity_ts"),
            }

        if event_type == "session_close":
            reason = event_data.get("reason", "page_close")
            return {"action": "close_session", "reason": reason}

        if event_type == "timeout":
            return {
                "action": "timeout_session",
                "inactive_minutes": event_data.get("inactive_minutes", 0),
                "last_activity_ts": event_data.get("last_activity_ts"),
            }

        if event_type == "timeout_warning":
            return {
                "action": "timeout_warning",
                "minutes_until_timeout": event_data.get("minutes_until_timeout"),
                "last_activity_ts": event_data.get("last_activity_ts"),
            }

        if event_type == "page_hidden":
            st.session_state["page_hidden"] = True
            return {"action": "page_hidden"}

        if event_type == "page_visible":
            st.session_state["page_hidden"] = False
            return {"action": "page_visible"}

        logger.debug("Session monitor emitted unhandled event: %s", event_type)
        return None


def _parse_timestamp(timestamp: Optional[str]) -> Optional[datetime]:
    if not timestamp:
        return None

    sanitized = timestamp.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(sanitized)
    except ValueError:
        logger.debug("Invalid heartbeat timestamp received: %s", timestamp)
        return None
