"""
LLM loader and configuration for graphic_classifier.

This module provides functions to initialize and configure the LLM
for use in the LangGraph workflow.

GEMINI MIGRATION:
- Uses ChatGoogleGenerativeAI (Google Gemini)
- Model: gemini-2.5-flash-lite
- Optimizations: timeout=30s, max_retries=2
- temperature=0.5 for balanced classification

References:
- Authentication: https://colab.research.google.com/github/google-gemini/cookbook/blob/main/quickstarts/Authentication.ipynb
"""

import logging
from typing import Optional
from langchain_google_genai import ChatGoogleGenerativeAI

from src.shared_lib.core.config import get_graphic_config

logger = logging.getLogger(__name__)


def load_llm(
    model: Optional[str] = None,
    temperature: Optional[float] = None,
    max_output_tokens: Optional[int] = None,
) -> ChatGoogleGenerativeAI:
    """
    Initialize and return a configured ChatGoogleGenerativeAI instance.

    Uses centralized Gemini configuration with optimizations:
    - timeout=30s (50% reduction from default 60s)
    - max_retries=2 (fail fast instead of many retries)
    - max_output_tokens=1500 (optimized default)
    - temperature=0.5 (balanced for consistent classification)

    Args:
        model: Gemini model name. Defaults to gemini-2.5-flash-preview-09-2025.
        temperature: Temperature for generation (0.0-2.0). Defaults to 0.5.
        max_output_tokens: Maximum tokens in response. Defaults to 1500.

    Returns:
        ChatGoogleGenerativeAI: Configured LLM instance with Gemini optimizations.

    Example:
        >>> llm = load_llm()
        >>> # LLM now has timeout=30s, max_retries=2, temperature=0.5
        >>> response = llm.invoke("Classify this query: top 5 products")
    """
    logger.info("Initializing Gemini LLM with centralized optimized config")

    # Build overrides
    overrides = {}
    if model:
        overrides["model"] = model
    if temperature is not None:
        overrides["temperature"] = temperature
    if max_output_tokens:
        overrides["max_output_tokens"] = max_output_tokens

    # Get centralized config with optimizations
    config = get_graphic_config(**overrides)
    llm = ChatGoogleGenerativeAI(**config.to_gemini_kwargs())

    logger.info(
        f"Gemini LLM initialized - "
        f"Model: {config.model}, "
        f"Timeout: {config.timeout}s, "
        f"Max Retries: {config.max_retries}, "
        f"Max Output Tokens: {config.max_output_tokens}, "
        f"Temperature: {config.temperature}"
    )

    return llm


def create_structured_llm(
    model: Optional[str] = None,
    temperature: Optional[float] = None,
    max_output_tokens: Optional[int] = None,
) -> ChatGoogleGenerativeAI:
    """
    Create a Gemini LLM instance configured for structured output.

    This variant is optimized for generating JSON-formatted responses
    and can be used with LangChain's structured output tools.

    Includes Gemini optimizations:
    - timeout=30s
    - max_retries=2
    - temperature=0.3 for structured output

    Args:
        model: Gemini model name. Defaults to gemini-2.5-flash-preview-09-2025.
        temperature: Temperature for generation (0.0-2.0). Defaults to 0.3.
        max_output_tokens: Maximum tokens in response. Defaults to 1500.

    Returns:
        ChatGoogleGenerativeAI: Configured LLM instance with Gemini optimizations.

    Example:
        >>> from langchain.output_parsers import PydanticOutputParser
        >>> llm = create_structured_llm()
        >>> parser = PydanticOutputParser(pydantic_object=ChartOutput)
        >>> chain = llm | parser
    """
    llm = load_llm(
        model=model,
        temperature=temperature if temperature is not None else 0.3,
        max_output_tokens=max_output_tokens
    )

    logger.info("Structured Gemini LLM initialized with centralized config")
    return llm
