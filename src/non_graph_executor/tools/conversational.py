"""
Conversational handler for non_graph_executor.

This module detects and responds to conversational queries (greetings and
generic messages) with professional responses using Google Gemini LLM
that guide users toward business-related data queries.

Responsibilities:
- Detect conversational queries vs business queries
- Generate professional responses for greetings
- Use Google Gemini LLM for generic conversational responses
- Maintain professional tone and direct users to productive use
"""

import logging
from typing import List
from langchain_google_genai import ChatGoogleGenerativeAI

logger = logging.getLogger(__name__)


class ConversationalHandler:
    """
    Handler para queries conversacionais.

    Detecta saudações e mensagens genéricas, respondendo de forma
    profissional via Google Gemini e direcionando usuário para queries de negócio.

    Attributes:
        llm: ChatGoogleGenerativeAI instance configured with temperature=0.3
        GREETINGS: List of known greeting patterns
        BUSINESS_KEYWORDS: Keywords that indicate business queries
    """

    # Constantes de saudações conhecidas (case-insensitive)
    GREETINGS: List[str] = [
        "oi",
        "olá",
        "ola",
        "hello",
        "hi",
        "hey",
        "bom dia",
        "boa tarde",
        "boa noite",
        "bom-dia",
        "boa-tarde",
        "boa-noite",
        "opa",
    ]

    # Keywords que indicam queries de negócio (case-insensitive)
    BUSINESS_KEYWORDS: List[str] = [
        "vendas",
        "clientes",
        "cliente",
        "produtos",
        "produto",
        "valor",
        "valores",
        "quantidade",
        "quantidades",
        "tabela",
        "dados",
        "registros",
        "pedidos",
        "pedido",
        "total",
        "média",
        "media",
        "soma",
        "quantos",
        "quantas",
        "qual",
        "quais",
        "mostre",
        "mostrar",
        "listar",
        "buscar",
        "encontrar",
    ]

    def __init__(self, llm: ChatGoogleGenerativeAI):
        """
        Initialize conversational handler.

        Args:
            llm: ChatGoogleGenerativeAI instance for generating responses
                (should be configured with temperature=0.3 for fast, consistent responses)
        """
        self.llm = llm
        logger.info("ConversationalHandler initialized successfully")

    def is_conversational(self, query: str) -> bool:
        """
        Detecta se query é conversacional (não-produtiva).

        Critérios para classificação como conversacional:
        1. Query é uma saudação simples (está em GREETINGS)
        2. Query tem <= 3 palavras E não contém keywords de negócio

        Args:
            query: Query do usuário

        Returns:
            True se query é conversacional, False se é query de negócio

        Examples:
            >>> handler.is_conversational("oi")
            True
            >>> handler.is_conversational("bom dia")
            True
            >>> handler.is_conversational("como funciona")
            True
            >>> handler.is_conversational("quantas vendas")
            False
            >>> handler.is_conversational("mostre os clientes")
            False
        """
        query_lower = query.lower().strip()

        # Critério 1: Saudação simples (match exato ou muito similar)
        if query_lower in self.GREETINGS:
            logger.debug(f"Query '{query}' detected as simple greeting")
            return True

        # Critério 2: Query curta (<=3 palavras) sem keywords de negócio
        words = query_lower.split()
        if len(words) <= 3:
            has_business_keyword = any(
                kw in query_lower for kw in self.BUSINESS_KEYWORDS
            )
            if not has_business_keyword:
                logger.debug(
                    f"Query '{query}' detected as conversational "
                    f"(short and no business keywords)"
                )
                return True

        logger.debug(f"Query '{query}' is NOT conversational (business query)")
        return False

    def generate_response(self, query: str, token_accumulator=None) -> str:
        """
        Gera resposta conversacional apropriada.

        Estratégia:
        - Se saudação simples: usa template pré-definido (sem LLM, mais rápido)
        - Se conversacional mas não saudação: usa LLM para resposta contextual

        Args:
            query: Query conversacional do usuário
            token_accumulator: TokenAccumulator para rastrear tokens (opcional)

        Returns:
            Resposta profissional direcionando para uso produtivo

        Example:
            >>> handler.generate_response("oi")
            "Olá! Sou o assistente de análise de dados..."
        """
        query_lower = query.lower().strip()

        # Estratégia 1: Saudação simples → template rápido (sem LLM)
        if query_lower in self.GREETINGS:
            response = self._get_greeting_template()
            logger.info(f"Generated template greeting response for: '{query}'")
            return response

        # Estratégia 2: Conversacional genérico → LLM
        response = self._generate_llm_response(query, token_accumulator)
        logger.info(f"Generated LLM response for conversational query: '{query}'")
        return response

    def _get_greeting_template(self) -> str:
        """
        Retorna template de resposta para saudações simples.

        Template otimizado para ser profissional e direcionar usuário
        para uso produtivo do sistema.

        Returns:
            String com resposta de saudação pré-definida
        """
        return (
            "Olá! Sou o assistente de análise de dados. "
            "Posso ajudá-lo com informações sobre vendas, clientes, produtos e muito mais. "
            "Tente perguntas como 'quantos clientes temos?' ou 'mostre as vendas por estado'."
        )

    def _generate_llm_response(self, query: str, token_accumulator=None) -> str:
        """
        Gera resposta conversacional usando LLM.

        Usado para queries conversacionais que não são saudações simples.
        Mantém tom profissional e direciona usuário para queries de negócio.

        Args:
            query: Query conversacional do usuário
            token_accumulator: TokenAccumulator para rastrear tokens (opcional)

        Returns:
            Resposta gerada pelo LLM

        Raises:
            Exception: Se LLM falhar, retorna fallback response
        """
        prompt = f"""Você é um assistente profissional de análise de dados comerciais.

O usuário disse: "{query}"

Responda de forma profissional e educada, e gentilmente direcione o usuário
para fazer perguntas sobre dados comerciais (vendas, clientes, produtos, etc).

Mantenha a resposta curta (2-3 frases). Seja direto e útil.

Resposta:"""

        try:
            response = self.llm.invoke(prompt)
            response_text = response.content.strip()

            # Capture and accumulate tokens
            from src.shared_lib.utils.token_tracker import extract_token_usage

            tokens = extract_token_usage(response, self.llm)
            if token_accumulator is not None:
                token_accumulator.add(tokens)
                logger.debug(f"[ConversationalHandler] Tokens accumulated: {tokens}")

            logger.debug(f"LLM response generated: {response_text[:100]}...")
            return response_text

        except Exception as e:
            logger.error(
                f"Error generating LLM response for conversational query: {e}",
                exc_info=True,
            )

            # Fallback response em caso de erro
            return (
                "Entendo. Como assistente de análise de dados, estou aqui para "
                "ajudá-lo com informações sobre vendas, clientes, produtos e outras "
                "métricas de negócio. Como posso auxiliá-lo com seus dados?"
            )
