"""
Token Tracking Utilities

Utilitarios para captura resiliente de tokens de respostas LLM.
"""

import logging
from typing import Dict, Any, Optional, Mapping

logger = logging.getLogger(__name__)


def _as_int(value: Any) -> int:
    try:
        if value is None:
            return 0
        return int(value)
    except Exception:
        return 0


def _zero_tokens() -> Dict[str, int]:
    """Retorna dicionario de tokens zerados (safe fallback)"""
    return {
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
    }


def _normalize_model_name(model_name: str) -> str:
    """
    Normaliza o nome do modelo removendo prefixos específicos de providers.

    Transformações:
    - "models/gemini-2.5-flash-lite" → "gemini-2.5-flash-lite"
    - "models/gemini-2.5-flash" → "gemini-2.5-flash"
    - "gemini-2.5-flash-lite" → "gemini-2.5-flash-lite" (unchanged)
    - "gpt-4o" → "gpt-4o" (unchanged)

    Args:
        model_name: Nome do modelo possivelmente com prefixo

    Returns:
        Nome normalizado do modelo sem prefixos de provider
    """
    if not model_name or model_name == "unknown":
        return model_name

    # Remove prefixo "models/" do Gemini
    if model_name.startswith("models/"):
        normalized = model_name.replace("models/", "", 1)
        logger.debug(
            f"[TokenTracker] Normalized model name: '{model_name}' → '{normalized}'"
        )
        return normalized

    # Já está normalizado
    return model_name


def _normalize_usage_dict(usage: Mapping[str, Any]) -> Dict[str, int]:
    """Normaliza diferentes formatos de contagem de tokens para o schema interno.

    Suporta:
    - LangChain padronizado: input_tokens/output_tokens/total_tokens
    - OpenAI legacy: prompt_tokens/completion_tokens/total_tokens
    - Gemini (langchain-google-genai): prompt_token_count/candidates_token_count/total_token_count
    - Outras variações comuns (fallback): prompt/output/total
    """
    if not usage:
        return _zero_tokens()

    # 1) LangChain padronizado
    if any(k in usage for k in ("input_tokens", "output_tokens", "total_tokens")):
        tokens = {
            "input_tokens": _as_int(usage.get("input_tokens")),
            "output_tokens": _as_int(usage.get("output_tokens")),
            "total_tokens": _as_int(usage.get("total_tokens")),
        }
        if tokens["total_tokens"] == 0:
            tokens["total_tokens"] = tokens["input_tokens"] + tokens["output_tokens"]
        return tokens

    # 2) OpenAI legacy
    if any(k in usage for k in ("prompt_tokens", "completion_tokens", "total_tokens")):
        tokens = {
            "input_tokens": _as_int(usage.get("prompt_tokens")),
            "output_tokens": _as_int(usage.get("completion_tokens")),
            "total_tokens": _as_int(usage.get("total_tokens")),
        }
        if tokens["total_tokens"] == 0:
            tokens["total_tokens"] = tokens["input_tokens"] + tokens["output_tokens"]
        return tokens

    # 3) Gemini (langchain-google-genai)
    if any(
        k in usage
        for k in (
            "prompt_token_count",
            "candidates_token_count",
            "total_token_count",
        )
    ):
        tokens = {
            "input_tokens": _as_int(usage.get("prompt_token_count")),
            "output_tokens": _as_int(usage.get("candidates_token_count")),
            "total_tokens": _as_int(usage.get("total_token_count")),
        }
        if tokens["total_tokens"] == 0:
            tokens["total_tokens"] = tokens["input_tokens"] + tokens["output_tokens"]
        return tokens

    # 4) Fallback genérico
    tokens = {
        "input_tokens": _as_int(usage.get("prompt") or usage.get("input")),
        "output_tokens": _as_int(usage.get("completion") or usage.get("output")),
        "total_tokens": _as_int(usage.get("total")),
    }
    if tokens["total_tokens"] == 0:
        tokens["total_tokens"] = tokens["input_tokens"] + tokens["output_tokens"]
    return tokens


def extract_token_usage(response: Any, llm_instance: Any = None) -> Dict[str, Any]:
    """
    Extrai uso de tokens e nome do modelo de uma resposta LLM (LangChain, OpenAI, Gemini).

    Tenta multiplos metodos de extracao com fallbacks graceful.
    NUNCA levanta exceptions - retorna zeros se falhar.

    Extração de model_name:
    - Se llm_instance fornecido: Usa llm_instance.model
    - De response_metadata: Busca 'model_name' ou 'model'
    - Fallback: "unknown" se modelo não determinado

    Args:
        response: Objeto de resposta do LLM (AIMessage, ChatCompletion, etc.)
        llm_instance: Instância opcional do LLM (e.g., ChatGoogleGenerativeAI) para extrair nome do modelo

    Returns:
        Dict com input_tokens, output_tokens, total_tokens, model_name
        Retorna zeros e "unknown" se extracao falhar
    """
    model_name = "unknown"

    try:
        # Extract model name first (multiple strategies)
        # Strategy 1: From llm_instance.model (most reliable for Gemini)
        if llm_instance and hasattr(llm_instance, "model"):
            model_name = _normalize_model_name(str(llm_instance.model))
            logger.debug(
                f"[TokenTracker] Model name from llm_instance.model: {model_name}"
            )

        # Strategy 2: From response_metadata (fallback)
        elif hasattr(response, "response_metadata") and isinstance(
            response.response_metadata, dict
        ):
            rm = response.response_metadata
            if "model_name" in rm:
                model_name = _normalize_model_name(rm["model_name"])
                logger.debug(
                    f"[TokenTracker] Model name from response_metadata.model_name: {model_name}"
                )
            elif "model" in rm:
                model_name = _normalize_model_name(rm["model"])
                logger.debug(
                    f"[TokenTracker] Model name from response_metadata.model: {model_name}"
                )

        # Strategy 3: From dict response (for custom responses)
        elif isinstance(response, dict) and "model" in response:
            model_name = _normalize_model_name(response["model"])
            logger.debug(f"[TokenTracker] Model name from dict.model: {model_name}")

        # Extract token counts (existing logic)
        # Metodo 1: LangChain usage_metadata (padrao mais recente)
        if hasattr(response, "usage_metadata") and response.usage_metadata:
            tokens = _normalize_usage_dict(response.usage_metadata)
            if tokens["total_tokens"] > 0:
                tokens["model_name"] = model_name
                logger.debug(
                    f"[TokenTracker] Extracted tokens via response.usage_metadata: {tokens}"
                )
                return tokens

        # Metodo 2: LangChain response_metadata
        # - OpenAI: response_metadata['token_usage']
        # - Gemini (langchain-google-genai): response_metadata['usage_metadata']
        if hasattr(response, "response_metadata") and isinstance(
            response.response_metadata, dict
        ):
            rm = response.response_metadata
            if isinstance(rm.get("token_usage"), dict):
                tokens = _normalize_usage_dict(rm["token_usage"])
                if tokens["total_tokens"] > 0:
                    tokens["model_name"] = model_name
                    logger.debug(
                        f"[TokenTracker] Extracted tokens via response_metadata.token_usage: {tokens}"
                    )
                    return tokens
            if isinstance(rm.get("usage_metadata"), dict):
                tokens = _normalize_usage_dict(rm["usage_metadata"])
                if tokens["total_tokens"] > 0:
                    tokens["model_name"] = model_name
                    logger.debug(
                        f"[TokenTracker] Extracted tokens via response_metadata.usage_metadata: {tokens}"
                    )
                    return tokens

        # Metodo 3: Acesso direto a dict (para respostas customizadas)
        if isinstance(response, dict):
            if isinstance(response.get("usage_metadata"), dict):
                tokens = _normalize_usage_dict(response["usage_metadata"])
                if tokens["total_tokens"] > 0:
                    tokens["model_name"] = model_name
                    logger.debug(
                        f"[TokenTracker] Extracted tokens from dict.usage_metadata: {tokens}"
                    )
                    return tokens
            if isinstance(response.get("token_usage"), dict):
                tokens = _normalize_usage_dict(response["token_usage"])
                if tokens["total_tokens"] > 0:
                    tokens["model_name"] = model_name
                    logger.debug(
                        f"[TokenTracker] Extracted tokens from dict.token_usage: {tokens}"
                    )
                    return tokens

        # Nenhum metodo funcionou - retornar zeros (safe fallback)
        logger.debug(
            f"[TokenTracker] No token metadata found in response (type={type(response).__name__})"
        )
        result = _zero_tokens()
        result["model_name"] = model_name
        return result

    except Exception as e:
        # IMPORTANTE: Nunca propagar exceptions de token tracking
        logger.warning(f"[TokenTracker] Failed to extract tokens: {e}", exc_info=False)
        result = _zero_tokens()
        result["model_name"] = model_name
        return result


class TokenTracker:
    """
    Context manager para rastreamento de tokens em blocos de codigo.

    Uso:
        with TokenTracker("agent_name", llm_instance) as tracker:
            response = llm.invoke(messages)
            tracker.capture(response)

        tokens = tracker.get_tokens()  # Includes model_name
    """

    def __init__(self, agent_name: str, llm_instance: Any = None):
        self.agent_name = agent_name
        self.llm_instance = llm_instance
        self.tokens = _zero_tokens()
        self.tokens["model_name"] = "unknown"

    def __enter__(self):
        logger.debug(f"[TokenTracker] Starting tracking for {self.agent_name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None and self.tokens["total_tokens"] > 0:
            logger.info(
                f"[TokenTracker] {self.agent_name}: "
                f"input={self.tokens['input_tokens']}, "
                f"output={self.tokens['output_tokens']}, "
                f"total={self.tokens['total_tokens']}, "
                f"model={self.tokens.get('model_name', 'unknown')}"
            )
        return False  # Nao suprimir exceptions

    def capture(self, response: Any, llm_instance: Any = None) -> None:
        """
        Captura tokens e nome do modelo de uma resposta LLM.

        Args:
            response: Resposta do LLM
            llm_instance: Instância do LLM (opcional, usa self.llm_instance se não fornecido)
        """
        instance = llm_instance or self.llm_instance
        self.tokens = extract_token_usage(response, instance)

    def get_tokens(self) -> Dict[str, Any]:
        """Retorna tokens capturados incluindo model_name"""
        return self.tokens.copy()


__all__ = ["extract_token_usage", "TokenTracker"]
