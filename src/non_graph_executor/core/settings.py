"""
Environment settings and configuration for non_graph_executor.

This module loads environment variables and provides validation
for non-graph executor configuration.

GEMINI MIGRATION:
- Primary API key: GEMINI_API_KEY (preferred)
- Legacy support: OPENAI_API_KEY (for backward compatibility)
"""

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project root directory
# settings.py is in src/non_graph_executor/core/settings.py
# so we need to go up 4 levels to reach project root
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

# Data Paths
DATA_PATH: str = os.getenv("DATASET_PATH") or os.getenv("DATA_PATH", "")

ALIAS_PATH: str = os.getenv(
    "ALIAS_PATH", str(PROJECT_ROOT / "data" / "mappings" / "alias.yaml")
)

# Gemini Configuration (with backward compatibility)
# Try GEMINI_API_KEY first, fall back to OPENAI_API_KEY for legacy support
OPENAI_API_KEY: Optional[str] = os.getenv("GEMINI_API_KEY") or os.getenv("OPENAI_API_KEY")

# Query Processing Configuration
DEFAULT_TABULAR_LIMIT: int = int(os.getenv("DEFAULT_TABULAR_LIMIT", "100"))
MAX_TABULAR_LIMIT: int = int(os.getenv("MAX_TABULAR_LIMIT", "1000"))
QUERY_TIMEOUT: int = int(os.getenv("QUERY_TIMEOUT", "30"))

# Cache Configuration
ENABLE_METADATA_CACHE: bool = (
    os.getenv("ENABLE_METADATA_CACHE", "true").lower() == "true"
)


def validate_settings() -> bool:
    """
    Validate that all critical settings are properly configured.

    Verifica:
    - OpenAI API key está configurada
    - Dataset existe no caminho especificado
    - Arquivo alias.yaml existe
    - Permissões de leitura nos arquivos
    - Configurações numéricas estão em ranges válidos

    Returns:
        bool: True if all settings are valid

    Raises:
        ValueError: If settings are invalid
        FileNotFoundError: If required files are not found
        PermissionError: If files don't have read permissions
    """
    # Validate API Key (Gemini or OpenAI for backward compatibility)
    if not OPENAI_API_KEY:
        raise ValueError(
            "API Key not found. Please set GEMINI_API_KEY (preferred) or "
            "OPENAI_API_KEY (legacy) in environment variables"
        )

    # Validate data file exists
    data_path = Path(DATA_PATH)
    if not data_path.exists():
        raise FileNotFoundError(
            f"Dataset file not found at: {DATA_PATH}\n"
            f"Please ensure DATA_PATH environment variable points to a valid parquet file."
        )

    # Validate read permissions on data file
    if not os.access(data_path, os.R_OK):
        raise PermissionError(f"No read permission for dataset file: {DATA_PATH}")

    # Validate alias file exists
    alias_path = Path(ALIAS_PATH)
    if not alias_path.exists():
        raise FileNotFoundError(
            f"Alias file not found at: {ALIAS_PATH}\n"
            f"Please ensure ALIAS_PATH environment variable points to a valid yaml file."
        )

    # Validate read permissions on alias file
    if not os.access(alias_path, os.R_OK):
        raise PermissionError(f"No read permission for alias file: {ALIAS_PATH}")

    # Validate numeric configurations
    if DEFAULT_TABULAR_LIMIT < 1:
        raise ValueError(
            f"DEFAULT_TABULAR_LIMIT must be positive, got: {DEFAULT_TABULAR_LIMIT}"
        )

    if MAX_TABULAR_LIMIT < DEFAULT_TABULAR_LIMIT:
        raise ValueError(
            f"MAX_TABULAR_LIMIT ({MAX_TABULAR_LIMIT}) must be >= "
            f"DEFAULT_TABULAR_LIMIT ({DEFAULT_TABULAR_LIMIT})"
        )

    if QUERY_TIMEOUT < 1:
        raise ValueError(f"QUERY_TIMEOUT must be positive, got: {QUERY_TIMEOUT}")

    return True
