"""
Environment settings and configuration for filter_classifier.

This module loads environment variables and defines filter-specific constants.
"""

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

# Filter Persistence Configuration
STORAGE_PATH: str = os.getenv(
    "FILTER_STORAGE_PATH",
    ".filter_state.json"
)

SESSION_TIMEOUT_MINUTES: int = int(os.getenv("FILTER_SESSION_TIMEOUT", "30"))

# Validation Configuration
FUZZY_THRESHOLD: float = float(os.getenv("FILTER_FUZZY_THRESHOLD", "0.75"))
MIN_CONFIDENCE_THRESHOLD: float = float(os.getenv("FILTER_MIN_CONFIDENCE", "0.60"))

# Note: Value validation is always active via ValueCatalog (Phase 3).
# GENERIC_TERMS_BLACKLIST and DATASET_SAMPLE_SIZE were removed — replaced by
# positive logic in PreMatchEngine (stopwords) and ValueCatalog (full catalog).

# LLM Configuration (inherited from main settings, but can be overridden)
# Gemini Migration: Try GEMINI_API_KEY first, fall back to OPENAI_API_KEY
# NOTE: Uses gemini-2.5-flash-lite (centralized config), not gemini-2.5-flash
OPENAI_API_KEY: Optional[str] = os.getenv("GEMINI_API_KEY") or os.getenv("OPENAI_API_KEY")
OPENAI_MODEL: str = os.getenv("GEMINI_MODEL") or os.getenv("OPENAI_MODEL", "gemini-2.5-flash-lite")
TEMPERATURE: float = float(os.getenv("TEMPERATURE", "0.7"))
MAX_TOKENS: int = int(os.getenv("MAX_TOKENS", "2000"))

# Data Paths (reuse from shared config)
ALIAS_PATH: str = os.getenv(
    "ALIAS_PATH",
    str(PROJECT_ROOT / "data" / "mappings" / "alias.yaml")
)
DATASET_PATH: str = os.getenv("DATASET_PATH", "")

# Valid Filter Operators
VALID_OPERATORS = ["=", ">", "<", ">=", "<=", "between", "in", "not_in"]

# CRUD Operation Types
CRUD_OPERATIONS = ["ADICIONAR", "ALTERAR", "REMOVER", "MANTER"]

# Operation Precedence (for conflict resolution)
OPERATION_PRECEDENCE = ["REMOVER", "ALTERAR", "ADICIONAR", "MANTER"]


def validate_settings() -> bool:
    """
    Validate that all critical settings are properly configured.

    Returns:
        bool: True if all settings are valid

    Raises:
        ValueError: If settings are invalid
        FileNotFoundError: If required files are missing
    """
    if SESSION_TIMEOUT_MINUTES < 1:
        raise ValueError(f"SESSION_TIMEOUT_MINUTES must be positive, got: {SESSION_TIMEOUT_MINUTES}")

    if FUZZY_THRESHOLD < 0 or FUZZY_THRESHOLD > 1:
        raise ValueError(f"FUZZY_THRESHOLD must be between 0 and 1, got: {FUZZY_THRESHOLD}")

    if MIN_CONFIDENCE_THRESHOLD < 0 or MIN_CONFIDENCE_THRESHOLD > 1:
        raise ValueError(f"MIN_CONFIDENCE_THRESHOLD must be between 0 and 1, got: {MIN_CONFIDENCE_THRESHOLD}")

    if not Path(ALIAS_PATH).exists():
        raise FileNotFoundError(f"Alias file not found at: {ALIAS_PATH}")

    if TEMPERATURE < 0 or TEMPERATURE > 2:
        raise ValueError(f"TEMPERATURE must be between 0 and 2, got: {TEMPERATURE}")

    return True
