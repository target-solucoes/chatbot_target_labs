"""
Token Tracking Data Models

Schemas para rastreamento estruturado de tokens LLM.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class TokenUsage:
    """Uso de tokens de uma chamada LLM individual"""
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0

    def to_dict(self) -> Dict[str, int]:
        """Converte para dicionario"""
        return {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens
        }

    @classmethod
    def from_dict(cls, data: Dict[str, int]) -> "TokenUsage":
        """Cria a partir de dicionario"""
        return cls(
            input_tokens=data.get("input_tokens", 0),
            output_tokens=data.get("output_tokens", 0),
            total_tokens=data.get("total_tokens", 0)
        )


@dataclass
class AgentTokenUsage:
    """Uso de tokens de um agente (pode ter multiplas chamadas LLM)"""
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    llm_calls: int = 0  # Numero de chamadas LLM (para agentes com multiplas chamadas)

    def add(self, tokens: TokenUsage) -> None:
        """Adiciona tokens de uma chamada LLM"""
        self.input_tokens += tokens.input_tokens
        self.output_tokens += tokens.output_tokens
        self.total_tokens += tokens.total_tokens
        self.llm_calls += 1

    def to_dict(self) -> Dict[str, int]:
        """Converte para dicionario"""
        result = {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens
        }
        if self.llm_calls > 1:
            result["llm_calls"] = self.llm_calls
        return result


__all__ = ["TokenUsage", "AgentTokenUsage"]
