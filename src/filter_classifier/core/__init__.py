"""Core configuration and settings for filter_classifier."""

from src.filter_classifier.core.settings import (
    STORAGE_PATH,
    SESSION_TIMEOUT_MINUTES,
    FUZZY_THRESHOLD,
    MIN_CONFIDENCE_THRESHOLD,
    DATASET_SAMPLE_SIZE,
    validate_settings
)

__all__ = [
    "STORAGE_PATH",
    "SESSION_TIMEOUT_MINUTES",
    "FUZZY_THRESHOLD",
    "MIN_CONFIDENCE_THRESHOLD",
    "DATASET_SAMPLE_SIZE",
    "validate_settings",
]
