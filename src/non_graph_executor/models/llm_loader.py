"""
LLM loader and configuration for non_graph_executor.

This module provides functions to initialize and configure the LLM
for use in non-graph query processing with optimized settings for
fast response times.

GEMINI MIGRATION:
- Uses ChatGoogleGenerativeAI (Google Gemini)
- Model: gemini-2.5-flash-lite
- Performance optimizations: timeout=20s (faster than default 30s)
- temperature=0.3 for consistent responses

References:
- Authentication: https://colab.research.google.com/github/google-gemini/cookbook/blob/main/quickstarts/Authentication.ipynb
"""

import logging
from typing import Optional
from dataclasses import dataclass
from langchain_google_genai import ChatGoogleGenerativeAI

from src.shared_lib.core.config import LLMConfig

logger = logging.getLogger(__name__)


@dataclass
class NonGraphLLMConfig(LLMConfig):
    """
    Configuração Gemini LLM otimizada para respostas rápidas.

    Otimizações específicas para non-graph queries:
    - timeout=20s (mais agressivo que padrão de 30s)
    - max_retries=2 (fail fast)
    - max_output_tokens=1500 (suficiente para respostas concisas)
    - temperature=0.3 (baixa para respostas consistentes)
    """

    model: str = "gemini-2.5-flash-lite"
    temperature: float = 0.3
    max_output_tokens: int = 1500
    timeout: int = 20  # Mais rápido que padrão (30s)
    max_retries: int = 2


def load_llm(
    temperature: Optional[float] = None,
    max_output_tokens: Optional[int] = None,
) -> ChatGoogleGenerativeAI:
    """
    Initialize and return a configured ChatGoogleGenerativeAI instance for non-graph queries.

    Usa configuração Gemini otimizada para respostas rápidas:
    - timeout=20s (otimizado para queries rápidas)
    - max_retries=2 (fail fast)
    - max_output_tokens=1500 (padrão otimizado)
    - temperature=0.3 (respostas consistentes)

    Args:
        temperature: Temperature for generation (0.0-2.0).
            Defaults to 0.3 for consistent responses.
            - 0.0-0.3: More deterministic, good for classification
            - 0.5-0.7: Balanced
            - 0.8-1.0: More creative (use sparingly for non-graph)
        max_output_tokens: Maximum tokens in response. Defaults to 1500.
            Can be reduced further for simple queries (e.g., 500 for classification)

    Returns:
        ChatGoogleGenerativeAI: Configured Gemini LLM instance optimized for fast responses.

    Example:
        >>> # Default configuration (temperature=0.3, 1500 tokens)
        >>> llm = load_llm()

        >>> # Override for specific use case
        >>> llm = load_llm(temperature=0.5, max_output_tokens=500)
    """
    # Create configuration
    config = NonGraphLLMConfig()

    # Apply overrides if provided
    if temperature is not None:
        config.temperature = temperature
    if max_output_tokens is not None:
        config.max_output_tokens = max_output_tokens

    logger.info(
        f"Initializing Gemini LLM for non_graph_executor: "
        f"model={config.model}, "
        f"temperature={config.temperature}, "
        f"max_output_tokens={config.max_output_tokens}, "
        f"timeout={config.timeout}s"
    )

    # Convert to ChatGoogleGenerativeAI kwargs and initialize
    llm_kwargs = config.to_gemini_kwargs()

    return ChatGoogleGenerativeAI(**llm_kwargs)
