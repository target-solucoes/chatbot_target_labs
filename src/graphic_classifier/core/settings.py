"""
Environment settings and configuration variables.

This module loads environment variables and defines project-wide constants.

GEMINI MIGRATION:
- Primary API key: GEMINI_API_KEY (preferred)
- Legacy support: OPENAI_API_KEY (for backward compatibility)
- Default model: gemini-2.5-flash (Google Gemini)
"""

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project root directory
# settings.py is in src/graphic_classifier/core/settings.py
# so we need to go up 4 levels to reach project root
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

# Gemini Configuration (with backward compatibility)
# Try GEMINI_API_KEY first, fall back to OPENAI_API_KEY for legacy support
# NOTE: Most components use gemini-2.5-flash-lite (centralized in shared_lib/core/config.py)
# Only semantic_anchor uses gemini-2.5-flash directly
OPENAI_API_KEY: Optional[str] = os.getenv("GEMINI_API_KEY") or os.getenv("OPENAI_API_KEY")
OPENAI_MODEL: str = os.getenv("GEMINI_MODEL") or os.getenv("OPENAI_MODEL", "gemini-2.5-flash-lite")

# Data Paths
ALIAS_PATH: str = os.getenv(
    "ALIAS_PATH",
    str(PROJECT_ROOT / "data" / "mappings" / "alias.yaml")
)
DATASET_PATH: str = os.getenv("DATASET_PATH", "")

# Agent Configuration
TEMPERATURE: float = float(os.getenv("TEMPERATURE", "0.1"))
MAX_TOKENS: int = int(os.getenv("MAX_TOKENS", "2000"))

# Logging Configuration
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE: str = os.getenv("LOG_FILE", str(PROJECT_ROOT / "logs" / "agent.log"))

# Alias Mapper Configuration
FUZZY_MATCH_THRESHOLD: float = float(os.getenv("FUZZY_MATCH_THRESHOLD", "0.85"))
SEMANTIC_MATCH_THRESHOLD: float = float(os.getenv("SEMANTIC_MATCH_THRESHOLD", "0.80"))

# Valid Chart Types
VALID_CHART_TYPES = [
    "bar_horizontal",
    "bar_vertical",
    "bar_vertical_composed",
    "line",
    "line_composed",
    "pie",
    "bar_vertical_stacked",
    "histogram"
]

# Valid Aggregation Functions
VALID_AGGREGATIONS = ["sum", "avg", "count", "min", "max", "median"]

# Valid Label Formats
VALID_LABEL_FORMATS = ["currency", "percent", "integer", "float"]

# Valid Sort Orders
VALID_SORT_ORDERS = ["asc", "desc"]


def validate_settings() -> bool:
    """
    Validate that all critical settings are properly configured.

    Returns:
        bool: True if all settings are valid, False otherwise
    """
    if not OPENAI_API_KEY:
        raise ValueError(
            "API Key not found. Please set GEMINI_API_KEY (preferred) or "
            "OPENAI_API_KEY (legacy) in environment variables"
        )

    if not Path(ALIAS_PATH).exists():
        raise FileNotFoundError(f"Alias file not found at: {ALIAS_PATH}")

    if TEMPERATURE < 0 or TEMPERATURE > 2:
        raise ValueError(f"TEMPERATURE must be between 0 and 2, got: {TEMPERATURE}")

    if MAX_TOKENS < 1:
        raise ValueError(f"MAX_TOKENS must be positive, got: {MAX_TOKENS}")

    return True

