"""
LLM loader with Singleton pattern for formatter agent.

This module implements a Singleton pattern to ensure that only one LLM instance
is created and reused across all formatter agent nodes, reducing initialization
overhead and improving performance.

GEMINI MIGRATION:
- Uses ChatGoogleGenerativeAI (Google Gemini)
- Model: gemini-2.5-flash-lite
- Maintains Singleton pattern for performance (150-600ms saved)

Performance Impact:
- Eliminates 150-600ms of overhead from multiple LLM instantiations
- Reduces memory footprint by reusing single instance

References:
- Authentication: https://colab.research.google.com/github/google-gemini/cookbook/blob/main/quickstarts/Authentication.ipynb
"""

import logging
from typing import Optional
from langchain_google_genai import ChatGoogleGenerativeAI

from src.shared_lib.core.config import get_formatter_config

logger = logging.getLogger(__name__)

# Global singleton instance
_llm_instance: Optional[ChatGoogleGenerativeAI] = None


def get_shared_llm() -> ChatGoogleGenerativeAI:
    """
    Get or create the shared Gemini LLM instance (Singleton pattern).

    This function ensures that only one ChatGoogleGenerativeAI instance is created
    for the entire formatter agent, and all nodes reuse this same instance.
    This significantly reduces initialization overhead (150-600ms saved).

    Returns:
        ChatGoogleGenerativeAI: The shared Gemini LLM instance configured for formatter agent.

    Example:
        >>> # First call - creates new instance
        >>> llm = get_shared_llm()
        >>> logger.info(f"LLM instance ID: {id(llm)}")

        >>> # Second call - returns same instance
        >>> llm2 = get_shared_llm()
        >>> logger.info(f"LLM instance ID: {id(llm2)}")
        >>> assert id(llm) == id(llm2)  # Same instance!
    """
    global _llm_instance

    if _llm_instance is None:
        logger.info("Initializing shared Gemini LLM instance (Singleton) for formatter_agent with centralized config")
        config = get_formatter_config()
        _llm_instance = ChatGoogleGenerativeAI(**config.to_gemini_kwargs())
        logger.info(
            f"Shared Gemini LLM instance created successfully - "
            f"ID: {id(_llm_instance)}, "
            f"Model: {config.model}, "
            f"Timeout: {config.timeout}s, "
            f"Max Retries: {config.max_retries}, "
            f"Max Output Tokens: {config.max_output_tokens}, "
            f"Temperature: {config.temperature}"
        )
    else:
        logger.debug(f"Reusing existing Gemini LLM instance - ID: {id(_llm_instance)}")

    return _llm_instance


def reset_llm_instance():
    """
    Reset the singleton LLM instance.

    This function is useful for testing purposes or when you need to
    force recreation of the LLM instance with different configurations.

    Warning:
        This should only be used in testing scenarios. In production,
        the singleton instance should persist throughout the application lifecycle.
    """
    global _llm_instance
    if _llm_instance is not None:
        logger.info(f"Resetting LLM singleton instance - ID: {id(_llm_instance)}")
        _llm_instance = None
    else:
        logger.debug("LLM instance already None, nothing to reset")
