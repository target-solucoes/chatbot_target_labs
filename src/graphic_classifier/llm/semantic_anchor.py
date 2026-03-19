"""
Semantic Anchor Extractor - LLM-based semantic analysis.

This module implements the FIRST LAYER of the semantic-first architecture.
It extracts pure semantic intent from queries using an ultra-light LLM
BEFORE any heuristic or regex processing occurs.

CRITICAL RULES:
- This MUST be executed FIRST in the workflow
- Output is PURELY semantic (NO chart_type, NO dimensions, NO filters)
- Uses Google Gemini 2.5 Flash for reliable JSON output
- Target latency: < 500ms
- Target cost: < $0.0002 per query

LLM Model:
- Model: gemini-2.5-flash (Google Gemini)
- JSON Mode: Enabled for reliable structured output
- Temperature: 0.1 (low for consistency)

References:
- graph_classifier_diagnosis.md (Architectural problems)
- graph_classifier_correction.md (FASE 1 specifications)
"""

import logging
import json
import os
from dataclasses import dataclass, asdict
from typing import Literal, Optional
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ============================================================================
# PYDANTIC MODEL FOR STRUCTURED OUTPUT
# ============================================================================


class SemanticAnchorPydantic(BaseModel):
    """
    Pydantic model for structured LLM output.

    This ensures the LLM returns valid, well-formed JSON.
    """

    semantic_goal: Literal[
        "compare_variation",
        "ranking",
        "trend",
        "distribution",
        "composition",
        "factual",
    ]
    comparison_axis: Literal["temporal", "categorical", "none"]
    polarity: Literal["positive", "negative", "neutral"]
    requires_time_series: bool
    entity_scope: str = Field(..., min_length=1)
    confidence: float = Field(..., ge=0.0, le=1.0)
    reasoning: str = Field(..., min_length=1)


# ============================================================================
# SEMANTIC ANCHOR DATACLASS (STRICT CONTRACT)
# ============================================================================


@dataclass
class SemanticAnchor:
    """
    Semantic anchor extracted from a natural language query.

    This dataclass represents the PURE SEMANTIC INTENT of a query,
    independent of any visualization or structural decisions.

    IMPORTANT CONSTRAINTS:
    - Contains ONLY semantic fields (no chart_type, no dimensions, no filters)
    - All fields are ENUMs or primitives (no free-form strings)
    - This is the ANCHOR for all downstream decisions
    - Heuristics CANNOT contradict this anchor

    Fields:
        semantic_goal: The primary semantic objective of the query
        comparison_axis: The axis along which comparison happens (if any)
        polarity: The direction/polarity of the intent (positive/negative/neutral)
        requires_time_series: Whether the query requires time-based data
        entity_scope: The primary entity being analyzed (e.g., "vendas", "produtos")
        confidence: LLM confidence in this classification (0.0-1.0)
        reasoning: Brief explanation of why this classification was chosen

    Examples:
        Query: "queda nas vendas entre maio e junho"
        -> SemanticAnchor(
            semantic_goal="compare_variation",
            comparison_axis="temporal",
            polarity="negative",
            requires_time_series=True,
            entity_scope="vendas",
            confidence=0.95,
            reasoning="Query explicitly compares temporal periods with negative polarity"
        )

        Query: "top 5 produtos mais vendidos"
        -> SemanticAnchor(
            semantic_goal="ranking",
            comparison_axis="categorical",
            polarity="positive",
            requires_time_series=False,
            entity_scope="produtos",
            confidence=0.98,
            reasoning="Query requests top N ranking with positive polarity"
        )
    """

    semantic_goal: Literal[
        "compare_variation",  # Comparing values between temporal periods
        "ranking",  # Top N / Bottom N of entities
        "trend",  # Temporal evolution/tendency
        "distribution",  # Proportional distribution / percentage
        "composition",  # Hierarchical composition
        "factual",  # Textual answer (no chart)
    ]

    comparison_axis: Literal[
        "temporal",  # Comparison between time periods
        "categorical",  # Comparison between categories
        "none",  # No comparison axis
    ]

    polarity: Literal[
        "positive",  # maior, melhor, top, crescimento, aumento
        "negative",  # menor, pior, bottom, queda, reducao, declinio
        "neutral",  # no clear polarity
    ]

    requires_time_series: bool
    entity_scope: str
    confidence: float
    reasoning: str

    def __post_init__(self):
        """Validate field values after initialization."""
        # Validate confidence range
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(
                f"confidence must be between 0.0 and 1.0, got {self.confidence}"
            )

        # Validate entity_scope is not empty
        if not self.entity_scope or not self.entity_scope.strip():
            raise ValueError("entity_scope cannot be empty")

        # Log anchor creation
        logger.debug(
            f"[SemanticAnchor] Created: {self.semantic_goal} | {self.comparison_axis} | {self.polarity}"
        )


# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================


class SemanticAnalysisError(Exception):
    """Raised when semantic analysis fails."""

    pass


class InvalidSemanticOutputError(SemanticAnalysisError):
    """Raised when LLM output violates the semantic anchor contract."""

    pass


# ============================================================================
# SEMANTIC ANCHOR EXTRACTOR
# ============================================================================


class SemanticAnchorExtractor:
    """
    Extracts semantic anchors from natural language queries using Google Gemini 2.5 Flash.

    This is the FIRST LAYER in the semantic-first architecture and MUST be
    executed before any heuristic or regex processing.

    Design Principles:
    - Ultra-light LLM (Google Gemini 2.5 Flash) for speed and cost
    - Structured JSON output (enforced via response_mime_type)
    - NO chart_type inference (purely semantic)
    - Target latency: < 500ms
    - Target cost: < $0.0002 per query

    Usage:
        extractor = SemanticAnchorExtractor()
        anchor = extractor.extract("queda nas vendas entre maio e junho")
        print(anchor.semantic_goal)  # "compare_variation"
        print(anchor.polarity)  # "negative"
    """

    # System prompt (optimized for brevity - Gemini 2.5 Flash)
    SYSTEM_PROMPT = """Classifique a intenção semântica da query em JSON.

FORMATO (JSON obrigatório):
{
  "semantic_goal": "compare_variation|ranking|trend|distribution|composition|factual",
  "comparison_axis": "temporal|categorical|none",
  "polarity": "positive|negative|neutral",
  "requires_time_series": bool,
  "entity_scope": "string",
  "confidence": 0.0-1.0,
  "reasoning": "1 frase"
}

GOALS:
- compare_variation: Comparar periodos (ex: "variacao maio-junho")
- ranking: Top/Bottom N de UMA dimensao (ex: "top 5 produtos")
- trend: Evolucao temporal (ex: "historico 2015")
- distribution: % ou proporcao
- composition: Hierarquia OU nested ranking (ex: "top 3 X dos 5 maiores Y")
- factual: Resposta textual

POLARITY:
- positive: maior, top, crescimento, aumento, subida
- negative: menor, queda, redução, diminuição, declínio
- neutral: sem polaridade

EXEMPLOS:
"queda vendas maio-junho" → {"semantic_goal":"compare_variation","comparison_axis":"temporal","polarity":"negative","requires_time_series":true,"entity_scope":"vendas","confidence":0.95,"reasoning":"Comparação temporal negativa"}

"quais produtos tiveram queda entre maio e junho" → {"semantic_goal":"compare_variation","comparison_axis":"temporal","polarity":"negative","requires_time_series":true,"entity_scope":"produtos","confidence":0.95,"reasoning":"Variação temporal negativa com ranking implícito"}

"produtos com maior aumento" → {"semantic_goal":"compare_variation","comparison_axis":"temporal","polarity":"positive","requires_time_series":true,"entity_scope":"produtos","confidence":0.95,"reasoning":"Variação temporal positiva"}

"top 5 produtos" -> {"semantic_goal":"ranking","comparison_axis":"categorical","polarity":"positive","requires_time_series":false,"entity_scope":"produtos","confidence":0.98,"reasoning":"Ranking Top N simples"}

"3 maiores clientes dos 5 maiores estados" -> {"semantic_goal":"composition","comparison_axis":"categorical","polarity":"positive","requires_time_series":false,"entity_scope":"clientes","confidence":0.95,"reasoning":"Nested ranking - composicao hierarquica"}

"top 3 produtos para os 10 maiores clientes" -> {"semantic_goal":"composition","comparison_axis":"categorical","polarity":"positive","requires_time_series":false,"entity_scope":"produtos","confidence":0.95,"reasoning":"Nested ranking - top N dentro de top M"}

Retorne APENAS JSON."""

    def __init__(self):
        """Initialize the extractor with Google Gemini 2.5 Flash."""
        logger.info(
            "[SemanticAnchorExtractor] Initializing with Google Gemini 2.5 Flash"
        )

        # Get Gemini API key from environment
        gemini_api_key = os.getenv("GEMINI_API_KEY")
        if not gemini_api_key:
            raise ValueError(
                "GEMINI_API_KEY not found in environment. "
                "Please ensure it's configured in your .env file."
            )

        # Use Google Gemini 2.5 Flash for reliable, fast semantic extraction
        # Gemini 2.5 Flash provides excellent JSON output reliability with native JSON mode
        # This is optimal for structured semantic classification tasks
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash",
            temperature=0.1,  # Very low for consistency
            max_output_tokens=600,  # Increased to 600 to prevent truncation (avg response ~250-400 tokens)
            timeout=15.0,  # Timeout in seconds
            max_retries=2,  # Fast fail for errors
            google_api_key=gemini_api_key,  # Gemini-specific parameter name
            response_mime_type="application/json",  # Native JSON mode for Gemini
        )

        logger.info(
            "[SemanticAnchorExtractor] Initialized - "
            "Model: gemini-2.5-flash, "
            "Timeout: 15s, "
            "JSON Mode: Enabled (response_mime_type), "
            "Temperature: 0.1"
        )

        # Observability: last call metadata
        self.last_llm_response = None
        self.last_token_usage = None

    def extract(self, query: str) -> SemanticAnchor:
        """
        Extract semantic anchor from a natural language query.

        This method:
        1. Sends query to Google Gemini 2.5 Flash with semantic extraction prompt
        2. Parses JSON response into SemanticAnchor dataclass
        3. Validates output against contract
        4. Returns validated semantic anchor

        Args:
            query: Natural language query from user

        Returns:
            SemanticAnchor: Validated semantic anchor

        Raises:
            SemanticAnalysisError: If LLM call fails
            InvalidSemanticOutputError: If LLM output violates contract

        Example:
            >>> extractor = SemanticAnchorExtractor()
            >>> anchor = extractor.extract("queda nas vendas entre maio e junho")
            >>> assert anchor.semantic_goal == "compare_variation"
            >>> assert anchor.polarity == "negative"
        """
        logger.info(
            f"[SemanticAnchorExtractor] Extracting semantic anchor for: '{query}'"
        )

        try:
            # Build messages
            messages = [HumanMessage(content=f"{self.SYSTEM_PROMPT}\n\nQuery: {query}")]

            # Call LLM
            response = self.llm.invoke(messages)

            # Capture token usage (best-effort)
            try:
                from src.shared_lib.utils.token_tracker import extract_token_usage

                self.last_llm_response = response
                self.last_token_usage = extract_token_usage(response, self.llm)
            except Exception:
                # Token tracking must never break semantic extraction
                self.last_llm_response = response
                self.last_token_usage = None

            # Handle empty or None content
            if not response.content:
                logger.error(f"[SemanticAnchorExtractor] LLM returned empty content")
                raise InvalidSemanticOutputError(
                    "LLM returned empty content. This may be due to safety filters or API issues."
                )

            # Handle different content types (string, list, or dict for various LLM response formats)
            if isinstance(response.content, list):
                # Some LLM models return list of dicts with 'type' and 'text' keys
                # Extract the actual JSON from the text content
                raw_output = None
                for part in response.content:
                    if isinstance(part, dict) and part.get("type") == "text":
                        raw_output = part.get("text", "").strip()
                        break

                # If no text part found, join all parts as fallback
                if not raw_output:
                    raw_output = " ".join(
                        str(part) for part in response.content
                    ).strip()
            else:
                raw_output = str(response.content).strip()

            logger.debug(
                f"[SemanticAnchorExtractor] Raw output length: {len(raw_output)} chars"
            )
            logger.debug(
                f"[SemanticAnchorExtractor] Raw output (first 500 chars):\n{raw_output[:500]}"
            )

            # Extract JSON from markdown code blocks if present
            if "```json" in raw_output:
                json_start = raw_output.find("```json") + 7
                json_end = raw_output.rfind("```")
                if json_end > json_start:
                    raw_output = raw_output[json_start:json_end].strip()
            elif "```" in raw_output:
                json_start = raw_output.find("```") + 3
                json_end = raw_output.rfind("```")
                if json_end > json_start:
                    raw_output = raw_output[json_start:json_end].strip()

            # Try to parse JSON with robust repair
            try:
                parsed = json.loads(raw_output)
            except json.JSONDecodeError as e:
                logger.warning(
                    f"[SemanticAnchorExtractor] JSON parse error: {e}, attempting repair"
                )

                # REPAIR LAYER 1: Normalize single quotes to double quotes
                # This handles Gemini returning JSON with single quotes
                import re

                repaired = raw_output

                # Fix keys: 'key' → "key"
                repaired = re.sub(r"'(\w+)'(?=\s*:)", r'"\1"', repaired)

                # Fix string values: : 'value' → : "value"
                # More careful regex to avoid breaking apostrophes inside strings
                repaired = re.sub(r":\s*'([^']*)'", r': "\1"', repaired)

                logger.debug(
                    f"[SemanticAnchorExtractor] Repaired JSON (quote normalization)"
                )

                try:
                    parsed = json.loads(repaired)
                    logger.info(
                        "[SemanticAnchorExtractor] JSON repair successful (quote normalization)"
                    )
                except json.JSONDecodeError as e2:
                    # REPAIR LAYER 2: Use Pydantic model to parse (more forgiving)
                    try:
                        pydantic_model = SemanticAnchorPydantic.model_validate_json(
                            repaired
                        )
                        parsed = pydantic_model.model_dump()
                        logger.info(
                            "[SemanticAnchorExtractor] JSON repair successful (Pydantic)"
                        )
                    except Exception as e3:
                        logger.error(
                            f"[SemanticAnchorExtractor] All JSON repair attempts failed"
                        )
                        logger.error(f"[SemanticAnchorExtractor] Original error: {e}")
                        logger.error(
                            f"[SemanticAnchorExtractor] After normalization: {e2}"
                        )
                        logger.error(f"[SemanticAnchorExtractor] Pydantic error: {e3}")
                        logger.error(
                            f"[SemanticAnchorExtractor] Raw output:\n{raw_output}"
                        )
                        logger.error(
                            f"[SemanticAnchorExtractor] Repaired output:\n{repaired}"
                        )
                        raise InvalidSemanticOutputError(
                            f"LLM returned invalid JSON that couldn't be repaired: {str(e)}"
                        ) from e

            # Validate Pydantic schema
            try:
                pydantic_model = SemanticAnchorPydantic(**parsed)
            except Exception as e:
                logger.error(
                    f"[SemanticAnchorExtractor] Pydantic validation failed: {e}"
                )
                raise InvalidSemanticOutputError(
                    f"LLM output failed schema validation: {str(e)}"
                ) from e

            # Convert to dataclass
            anchor = SemanticAnchor(
                semantic_goal=pydantic_model.semantic_goal,
                comparison_axis=pydantic_model.comparison_axis,
                polarity=pydantic_model.polarity,
                requires_time_series=pydantic_model.requires_time_series,
                entity_scope=pydantic_model.entity_scope,
                confidence=pydantic_model.confidence,
                reasoning=pydantic_model.reasoning,
            )

            logger.info(
                f"[SemanticAnchor] Extracted: goal={anchor.semantic_goal}, "
                f"axis={anchor.comparison_axis}, polarity={anchor.polarity}, "
                f"confidence={anchor.confidence:.2f}"
            )

            return anchor

        except InvalidSemanticOutputError:
            # Re-raise validation errors
            raise

        except Exception as e:
            logger.error(f"[SemanticAnchorExtractor] Extraction failed: {e}")
            raise SemanticAnalysisError(
                f"Failed to extract semantic anchor: {str(e)}"
            ) from e
