"""
Core module - Configuracoes e settings do Plotly Generator.
"""

from src.plotly_generator.core.settings import (
    OUTPUT_DIR,
    DEFAULT_PALETTE,
    SUPPORTED_CHART_TYPES,
    validate_settings
)

__all__ = [
    "OUTPUT_DIR",
    "DEFAULT_PALETTE",
    "SUPPORTED_CHART_TYPES",
    "validate_settings"
]
