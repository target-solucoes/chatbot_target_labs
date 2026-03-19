"""
Core Settings for Formatter Agent
===================================

Configuration for LLM models, timeouts, and global parameters.

GEMINI MIGRATION:
- Uses Google Gemini (gemini-2.5-flash-lite) as per project standard
- Configured via centralized LLMConfig in src.shared_lib.core.config
- All LLM instances use get_shared_llm() Singleton pattern
"""

import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


# LLM Configuration (Legacy - kept for backward compatibility)
# NOTE: These are now managed by centralized config in src.shared_lib.core.config
DEFAULT_LLM_MODEL = "gemini-2.5-flash-lite"  # Using Google Gemini as per project standard
DEFAULT_MAX_COMPLETION_TOKENS = (
    2500  # Increased from 1500 to allow more verbose narratives
)
DEFAULT_TEMPERATURE = 0.3  # Balanced: creative but consistent
DEFAULT_LLM_TIMEOUT = 30  # seconds

# Retry Configuration
DEFAULT_RETRY_ATTEMPTS = 2
DEFAULT_RETRY_DELAY = 1.0  # seconds

# Data Limits
MAX_DATA_PREVIEW_ROWS = 3  # For LLM prompts
MAX_INSIGHTS_FOR_SYNTHESIS = 10  # Maximum insights to process
MAX_KEY_FINDINGS = 5  # Maximum key findings to generate

# Output Configuration
OUTPUT_FORMAT_VERSION = "1.0.0"


def get_llm_config(
    model: str = DEFAULT_LLM_MODEL,
    temperature: float = DEFAULT_TEMPERATURE,
    max_tokens: int = DEFAULT_MAX_COMPLETION_TOKENS,
) -> Dict[str, Any]:
    """
    Get LLM configuration dictionary (Legacy function - deprecated).

    DEPRECATED: This function is kept for backward compatibility only.
    New code should use centralized config from src.shared_lib.core.config.get_formatter_config()

    Args:
        model: LLM model name
        temperature: Sampling temperature (0.0 - 2.0)
        max_tokens: Maximum completion tokens

    Returns:
        Configuration dictionary for ChatGoogleGenerativeAI initialization
    """
    config = {
        "model": model,
        "temperature": temperature,
        "max_output_tokens": max_tokens,  # Gemini uses max_output_tokens
        "timeout": DEFAULT_LLM_TIMEOUT,
        "response_mime_type": "application/json",  # Gemini JSON mode
    }

    logger.debug(f"LLM config created: model={model}, temp={temperature}")
    return config


def get_retry_config() -> Dict[str, Any]:
    """
    Get retry configuration for LLM calls.

    Returns:
        Dictionary with retry settings
    """
    return {
        "max_attempts": DEFAULT_RETRY_ATTEMPTS,
        "delay": DEFAULT_RETRY_DELAY,
    }


# Logging configuration
def configure_logging(level: str = "INFO"):
    """
    Configure logging for formatter agent.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info(f"Formatter agent logging configured: level={level}")
