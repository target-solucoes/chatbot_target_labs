import time
import logging
import warnings
from contextlib import contextmanager
from typing import Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class PerformanceMonitor:
    """Monitor de performance para rastrear tempo de execucao de operacoes."""

    def __init__(self):
        self.timings: Dict[str, float] = {}
        self.start_times: Dict[str, float] = {}
        self.session_start = time.perf_counter()

    @contextmanager
    def measure(self, operation: str):
        """
        Context manager para medir tempo de operacoes.

        Args:
            operation: Nome da operacao sendo medida

        Usage:
            with perf.measure("filter_classifier"):
                result = filter_classifier.invoke(...)
        """
        start = time.perf_counter()
        self.start_times[operation] = start

        try:
            logger.info(f"[PERF] Iniciando: {operation}")
            yield
        finally:
            elapsed = time.perf_counter() - start
            self.timings[operation] = elapsed
            logger.info(f"[PERF] {operation}: {elapsed:.3f}s")

    def get_report(self) -> str:
        """
        Gera relatorio de performance com todas as operacoes medidas.

        Returns:
            String formatada com relatorio de performance
        """
        total = sum(self.timings.values())
        session_elapsed = time.perf_counter() - self.session_start

        report = [f"\n{'=' * 60}"]
        report.append("PERFORMANCE REPORT")
        report.append(f"{'=' * 60}")
        report.append(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Session Duration: {session_elapsed:.3f}s")
        report.append(f"{'=' * 60}")

        if not self.timings:
            report.append("No operations measured")
            report.append(f"{'=' * 60}")
            return "\n".join(report)

        # Ordenar por tempo (mais lento primeiro)
        for op, time_taken in sorted(
            self.timings.items(), key=lambda x: x[1], reverse=True
        ):
            percentage = (time_taken / total * 100) if total > 0 else 0
            report.append(f"{op:40s} {time_taken:6.3f}s ({percentage:5.1f}%)")

        report.append(f"{'=' * 60}")
        report.append(f"{'TOTAL (measured operations)':40s} {total:6.3f}s")
        report.append(f"{'=' * 60}")

        return "\n".join(report)

    def get_summary_dict(self) -> Dict[str, float]:
        """
        Retorna um dicionario com todas as metricas de performance.

        Returns:
            Dict com timings de todas as operacoes
        """
        return {
            "timings": self.timings.copy(),
            "total_measured": sum(self.timings.values()),
            "session_duration": time.perf_counter() - self.session_start,
        }

    def reset(self):
        """Limpa todos os timings e reinicia o monitor."""
        self.timings.clear()
        self.start_times.clear()
        self.session_start = time.perf_counter()
        logger.info("[PERF] Performance monitor reset")


_session_monitors: Dict[str, PerformanceMonitor] = {}


def _resolve_session_id(session_id: Optional[str]) -> str:
    if session_id:
        return session_id

    try:
        import streamlit as st

        return st.session_state.get("session_id", "default")
    except Exception:
        return "default"


def get_performance_monitor(session_id: Optional[str] = None) -> PerformanceMonitor:
    """Return a session-scoped PerformanceMonitor instance."""

    resolved_session = _resolve_session_id(session_id)
    cache_key = f"performance_monitor_{resolved_session}"

    try:
        import streamlit as st

        if cache_key not in st.session_state:
            st.session_state[cache_key] = PerformanceMonitor()
            logger.info(f"[Session {resolved_session}] PerformanceMonitor initialized")
        return st.session_state[cache_key]
    except Exception:
        monitor = _session_monitors.get(cache_key)
        if monitor is None:
            monitor = PerformanceMonitor()
            _session_monitors[cache_key] = monitor
            logger.debug(
                f"Creating temporary PerformanceMonitor for session {resolved_session}"
            )
        return monitor


# Deprecated helpers (kept for backward compatibility)
def get_global_monitor() -> PerformanceMonitor:
    warnings.warn(
        "get_global_monitor() is deprecated. Use get_performance_monitor() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return get_performance_monitor()


def reset_global_monitor(session_id: Optional[str] = None):
    warnings.warn(
        "reset_global_monitor() is deprecated. Use get_performance_monitor(...).reset().",
        DeprecationWarning,
        stacklevel=2,
    )
    monitor = get_performance_monitor(session_id)
    monitor.reset()
