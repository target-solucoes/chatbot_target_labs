"""
Token Accumulator

Acumula tokens de multiplas chamadas LLM em um unico agente.
Util para agentes como formatter_agent e non_graph_executor.
"""

import logging
from typing import Dict, Set, Any

logger = logging.getLogger(__name__)


class TokenAccumulator:
    """
    Acumula tokens de multiplas chamadas LLM.

    Rastreia também quais modelos foram utilizados.

    Uso:
        accumulator = TokenAccumulator()

        # Primeira chamada LLM
        response1 = llm.invoke(...)
        tokens1 = extract_token_usage(response1, llm1)
        accumulator.add(tokens1)

        # Segunda chamada LLM
        response2 = llm.invoke(...)
        tokens2 = extract_token_usage(response2, llm2)
        accumulator.add(tokens2)

        # Obter totais
        totals = accumulator.get_totals()
        # {"input_tokens": 300, "output_tokens": 150, "total_tokens": 450,
        #  "llm_calls": 2, "models_used": ["gemini-2.5-flash-lite"]}
    """

    def __init__(self):
        self.input_tokens = 0
        self.output_tokens = 0
        self.total_tokens = 0
        self.llm_calls = 0
        self.models_used: Set[str] = set()

    def add(self, tokens: Dict[str, Any]) -> None:
        """
        Adiciona tokens de uma chamada LLM.

        Args:
            tokens: Dict com input_tokens, output_tokens, total_tokens, model_name
        """
        self.input_tokens += tokens.get("input_tokens", 0)
        self.output_tokens += tokens.get("output_tokens", 0)
        self.total_tokens += tokens.get("total_tokens", 0)
        self.llm_calls += 1

        # Track which models were used
        if "model_name" in tokens and tokens["model_name"] != "unknown":
            self.models_used.add(tokens["model_name"])

        logger.debug(
            f"[TokenAccumulator] Added tokens: "
            f"input={tokens.get('input_tokens', 0)}, "
            f"output={tokens.get('output_tokens', 0)}, "
            f"total={tokens.get('total_tokens', 0)}, "
            f"model={tokens.get('model_name', 'unknown')} "
            f"(cumulative: {self.total_tokens}, models: {self.models_used})"
        )

    def get_totals(self) -> Dict[str, Any]:
        """
        Retorna tokens acumulados e modelos utilizados.

        Returns:
            Dict com input_tokens, output_tokens, total_tokens, llm_calls, model_name
            - model_name: Nome do modelo se apenas 1 foi usado, ou "multiple" se múltiplos
        """
        result = {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
        }

        # Sempre expor llm_calls quando houver chamadas LLM registradas
        if self.llm_calls > 0:
            result["llm_calls"] = self.llm_calls

        # Model name: single model or "multiple"
        if len(self.models_used) == 0:
            result["model_name"] = "unknown"
        elif len(self.models_used) == 1:
            result["model_name"] = list(self.models_used)[0]
        else:
            # Multiple models used by this agent
            result["model_name"] = "multiple"
            result["models_used"] = sorted(list(self.models_used))

        return result

    def reset(self) -> None:
        """Reseta contadores (util para reuso)"""
        self.input_tokens = 0
        self.output_tokens = 0
        self.total_tokens = 0
        self.llm_calls = 0
        self.models_used.clear()


__all__ = ["TokenAccumulator"]
