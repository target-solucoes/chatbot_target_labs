"""Background tracker that marks Supabase sessions as closed when heartbeats stop."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Event, Lock, Thread
from typing import Dict, Optional

from src.shared_lib.utils.logger import get_logger
from src.shared_lib.utils.session_logger import SessionLogger

logger = get_logger(__name__)


@dataclass
class _SessionActivity:
    session_id: str
    session_logger: SessionLogger
    last_heartbeat: datetime
    closed: bool = False
    disconnect_logged: bool = False  # Flag para evitar log duplicado de desconexao


class SessionActivityTracker:
    """Tracks heartbeats and closes sessions when connection is lost.

    HYBRID TIMEOUT ARCHITECTURE:
    - JavaScript pagehide/unload events (immediate, best effort)
    - Heartbeat timeout fallback (15 min, guaranteed)
    - 24h inactivity timeout (SessionTimeoutManager, independent)

    Escalated Detection:
    - 2 min without heartbeat → WARNING (possible network issue)
    - 10 min without heartbeat → WARNING (suspected tab closure)
    - 15 min without heartbeat → CLOSE SESSION (confirmed)
    """

    CHECK_INTERVAL_SECONDS = 30

    # ESCALATED DETECTION TIMEOUTS
    EARLY_DETECTION_SECONDS = 120   # 2 min = primeira suspeita (rede)
    MODERATE_DETECTION_SECONDS = 600  # 10 min = suspeita forte (aba fechada)
    HEARTBEAT_TIMEOUT_SECONDS = 900   # 15 min = confirmado, fecha sessao

    # NOTA: Timeout de 15 min balanceia entre:
    # - Problemas temporários de rede (5-10 min) → sessão continua ativa
    # - Aba realmente fechada → detectada em tempo razoável (15 min)
    # - JavaScript falha → heartbeat timeout funciona como fallback garantido

    def __init__(self) -> None:
        self._sessions: Dict[str, _SessionActivity] = {}
        self._lock = Lock()
        self._stop_event = Event()
        self._thread: Optional[Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = Thread(
            target=self._monitor_loop, name="session-activity-tracker", daemon=True
        )
        self._thread.start()
        logger.info("Session activity tracker thread started")

    def register_session(self, session_id: str, session_logger: SessionLogger) -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            entry = self._sessions.get(session_id)
            if entry:
                entry.session_logger = session_logger
                entry.last_heartbeat = now
                entry.closed = False
                logger.debug("Session %s re-registered with tracker", session_id)
            else:
                self._sessions[session_id] = _SessionActivity(
                    session_id=session_id,
                    session_logger=session_logger,
                    last_heartbeat=now,
                )
                logger.debug("Session %s registered with tracker", session_id)

    def record_heartbeat(self, session_id: str) -> None:
        with self._lock:
            entry = self._sessions.get(session_id)
            if entry:
                entry.last_heartbeat = datetime.now(timezone.utc)
                # Reset flag de desconexao quando heartbeat volta
                if entry.disconnect_logged:
                    logger.info(
                        "Sessao %s: Heartbeat restaurado apos desconexao.",
                        session_id
                    )
                    entry.disconnect_logged = False

    def mark_session_closed(self, session_id: str) -> None:
        with self._lock:
            if session_id in self._sessions:
                self._sessions.pop(session_id, None)
                logger.debug("Session %s removed from tracker", session_id)

    def _monitor_loop(self) -> None:
        while not self._stop_event.wait(self.CHECK_INTERVAL_SECONDS):
            self._flush_stale_sessions()

    def _flush_stale_sessions(self) -> None:
        """
        Monitora heartbeats com deteccao escalonada.

        HYBRID TIMEOUT ARCHITECTURE:
        - JavaScript pagehide/unload (imediato, best effort)
        - Heartbeat timeout fallback (15 min, garantido)

        Logica escalonada:
        - 2 min sem heartbeat → WARNING log (possivel problema de rede)
        - 10 min sem heartbeat → WARNING log (suspeita forte de aba fechada)
        - 15 min sem heartbeat → FECHA SESSAO (confirmado: aba fechada ou conexao perdida)
        """
        now_utc = datetime.now(timezone.utc)
        early_detection_cutoff = now_utc - timedelta(seconds=self.EARLY_DETECTION_SECONDS)
        moderate_detection_cutoff = now_utc - timedelta(seconds=self.MODERATE_DETECTION_SECONDS)
        heartbeat_timeout_cutoff = now_utc - timedelta(seconds=self.HEARTBEAT_TIMEOUT_SECONDS)
        stale_session_ids = []

        with self._lock:
            for session_id, entry in self._sessions.items():
                # Remover sessoes que foram fechadas externamente (logout manual, pagehide, timeout)
                if entry.closed:
                    stale_session_ids.append(session_id)
                    continue

                time_since_heartbeat = (now_utc - entry.last_heartbeat).total_seconds()

                # LEVEL 3: HEARTBEAT TIMEOUT (15 min) - FECHA SESSAO
                if entry.last_heartbeat < heartbeat_timeout_cutoff:
                    try:
                        entry.session_logger.close_session()
                        logger.info(
                            "Sessao %s encerrada automaticamente: sem heartbeat ha %.0f segundos (%.1f minutos). "
                            "Aba/janela provavelmente foi fechada.",
                            session_id,
                            time_since_heartbeat,
                            time_since_heartbeat / 60
                        )
                    except Exception as exc:
                        logger.warning(
                            "Falha ao encerrar sessao %s durante monitoramento: %s",
                            session_id,
                            exc,
                        )
                    entry.closed = True
                    stale_session_ids.append(session_id)
                    continue

                # LEVEL 2: MODERATE DETECTION (10 min) - WARNING FORTE
                if entry.last_heartbeat < moderate_detection_cutoff and not entry.disconnect_logged:
                    logger.warning(
                        "Sessao %s: Nenhum heartbeat ha %.0f segundos (%.1f minutos). "
                        "Suspeita forte de fechamento de aba/janela.",
                        session_id,
                        time_since_heartbeat,
                        time_since_heartbeat / 60
                    )
                    entry.disconnect_logged = True
                    continue

                # LEVEL 1: EARLY DETECTION (2 min) - WARNING LEVE
                if entry.last_heartbeat < early_detection_cutoff and not entry.disconnect_logged:
                    logger.warning(
                        "Sessao %s: Nenhum heartbeat ha %.0f segundos. "
                        "Possivel problema de rede ou aba em background.",
                        session_id,
                        time_since_heartbeat
                    )
                    entry.disconnect_logged = True

            # Remover sessoes fechadas do tracker
            for session_id in stale_session_ids:
                self._sessions.pop(session_id, None)
                logger.debug("Sessao %s removida do tracker", session_id)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)


_tracker = SessionActivityTracker()
_tracker.start()


def register_session_activity(session_id: str, session_logger: SessionLogger) -> None:
    """Register or refresh a session in the activity tracker."""
    _tracker.register_session(session_id, session_logger)


def record_client_heartbeat(session_id: str) -> None:
    """Record a heartbeat emitted by the client monitor."""
    _tracker.record_heartbeat(session_id)


def mark_session_closed(session_id: str) -> None:
    """Remove a session from the tracker once it was closed explicitly."""
    _tracker.mark_session_closed(session_id)


__all__ = [
    "register_session_activity",
    "record_client_heartbeat",
    "mark_session_closed",
]
