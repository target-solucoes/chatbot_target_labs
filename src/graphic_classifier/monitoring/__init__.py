"""
FASE 4: Monitoring Package - Invariant Health Metrics

Este pacote contém ferramentas de monitoramento de invariantes semânticas
em tempo de execução.

Módulos:
- invariant_monitor: Monitor de runtime para detecção de violações
"""

from src.graphic_classifier.monitoring.invariant_monitor import (
    InvariantMonitor,
    InvariantViolation,
    InvariantType,
    SeverityLevel,
    get_invariant_monitor,
    validate_invariants,
)

__all__ = [
    "InvariantMonitor",
    "InvariantViolation",
    "InvariantType",
    "SeverityLevel",
    "get_invariant_monitor",
    "validate_invariants",
]
