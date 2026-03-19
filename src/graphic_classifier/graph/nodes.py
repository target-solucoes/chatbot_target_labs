"""
LangGraph workflow nodes.

This module implements all the node functions that process the GraphState
as it flows through the workflow.
"""

import logging
import re
import time
from pathlib import Path
from typing import Dict, Any, Optional

from langchain_core.messages import SystemMessage, HumanMessage

from src.graphic_classifier.graph.state import GraphState
from src.graphic_classifier.models.llm_loader import load_llm
from src.graphic_classifier.tools.query_parser import parse_query
from src.graphic_classifier.tools.keyword_detector import (
    detect_keywords,
    get_chart_type_hints,
    get_best_chart_type,
)
from src.graphic_classifier.tools.alias_mapper import AliasMapper
from src.shared_lib.data.dataset_column_extractor import DatasetColumnExtractor
from src.graphic_classifier.utils.json_formatter import (
    format_output,
    validate_chart_output,
)
from src.graphic_classifier.core.settings import ALIAS_PATH, PROJECT_ROOT
from src.shared_lib.parsers.chart_spec_transformer import (
    ChartSpecTransformer,
    validate_spec,
)
from src.graphic_classifier.decision_tree.classifier import DecisionTreeClassifier
from src.graphic_classifier.tools.context_analyzer import extract_query_context

logger = logging.getLogger(__name__)

# Initialize global instances (loaded once)
_alias_mapper: AliasMapper = None
_llm = None
_column_extractor: DatasetColumnExtractor = None
_decision_tree_classifier: DecisionTreeClassifier = None


def _get_alias_mapper() -> AliasMapper:
    """Get or initialize the global AliasMapper instance."""
    global _alias_mapper
    if _alias_mapper is None:
        logger.info("Initializing AliasMapper")
        _alias_mapper = AliasMapper(alias_path=ALIAS_PATH)
    return _alias_mapper


def _get_llm():
    """Get or initialize the global LLM instance."""
    global _llm
    if _llm is None:
        logger.info("Initializing LLM")
        _llm = load_llm()
    return _llm


def _get_column_extractor() -> DatasetColumnExtractor:
    """Get or initialize the global DatasetColumnExtractor instance."""
    global _column_extractor
    if _column_extractor is None:
        logger.info("Initializing DatasetColumnExtractor")
        _column_extractor = DatasetColumnExtractor()
    return _column_extractor


def _get_decision_tree_classifier() -> DecisionTreeClassifier:
    """Get or initialize the global DecisionTreeClassifier instance."""
    global _decision_tree_classifier
    if _decision_tree_classifier is None:
        logger.info("Initializing DecisionTreeClassifier (FASE 3)")
        _decision_tree_classifier = DecisionTreeClassifier(
            level1_threshold=0.90, level2_threshold=0.75
        )
    return _decision_tree_classifier


def _load_system_prompt() -> str:
    """Load the system prompt from the markdown file."""
    prompt_path = (
        Path(PROJECT_ROOT)
        / "src"
        / "graphic_classifier"
        / "prompts"
        / "graphic_classifier_prompt.md"
    )
    try:
        with open(prompt_path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        logger.warning(f"System prompt not found at {prompt_path}, using fallback")
        return """You are a data visualization expert. Classify the user's query and determine the best chart type.
Respond in this format:
INTENT: <intent description>
CHART_TYPE: <chart type or null>
CONFIDENCE: <0.0 to 1.0>
REASONING: <explanation>"""


def _validate_pre_format(data: dict) -> list:
    """
    Valida dados antes de formatar output (Fase 1.2 - Validacao Estrita).

    Esta funcao implementa validacao pre-transform para detectar problemas
    antes que eles causem falhas de validacao Pydantic.

    Args:
        data: Dados a serem validados

    Returns:
        Lista de erros encontrados (vazia se valido)
    """
    from src.graphic_classifier.utils.chart_type_sanitizer import (
        validate_chart_type_format,
    )

    errors = []

    # Validar chart_type
    chart_type = data.get("chart_type")
    if chart_type and not validate_chart_type_format(chart_type):
        errors.append(
            f"Invalid chart_type format: '{chart_type}' (contains descriptive text)"
        )

    # Validar estrutura de filters
    filters = data.get("filters", {})
    if filters is not None and not isinstance(filters, dict):
        errors.append(f"Filters must be dict, got {type(filters).__name__}")

    # Validar estrutura de parsed_entities
    parsed_entities = data.get("parsed_entities")
    if parsed_entities is not None and not isinstance(parsed_entities, dict):
        errors.append(
            f"parsed_entities must be dict, got {type(parsed_entities).__name__}"
        )

    # Validar mapped_columns
    mapped_columns = data.get("mapped_columns")
    if mapped_columns is not None and not isinstance(mapped_columns, dict):
        errors.append(
            f"mapped_columns must be dict, got {type(mapped_columns).__name__}"
        )

    return errors


# ============================================================================
# NODE FUNCTIONS
# ============================================================================


def parse_query_node(state: GraphState) -> GraphState:
    """
    Parse the user query to extract entities and metadata.

    This node uses the query_parser tool to extract:
    - Numbers (top N, years, values)
    - Temporal references (months, years)
    - Operators (comparisons, rankings)
    - Categories and potential column references

    FASE 2 Enhancement: Starts timing for graphic_classifier.

    Args:
        state: Current graph state

    Returns:
        Updated graph state with parsed_entities populated
    """
    # FASE 2: Start timing for graphic_classifier
    graphic_classifier_start_time = time.perf_counter()

    query = state.get("query", "")
    logger.info(f"[parse_query_node] Processing query: {query}")

    try:
        # Parse the query using query_parser tool
        parsed_result = parse_query(query)

        # CRITICAL FIX: Merge with existing parsed_entities instead of overwriting
        # This preserves sort_by and sort_order set by semantic mapping
        if "parsed_entities" not in state:
            state["parsed_entities"] = {}

        # Merge parsed_result into existing parsed_entities
        # Only update keys that are not already set (semantic mapping has priority)
        for key, value in parsed_result.items():
            if key not in state["parsed_entities"]:
                state["parsed_entities"][key] = value

        logger.info(
            f"[parse_query_node] Extracted entities: "
            f"top_n={parsed_result.get('top_n')}, "
            f"years={parsed_result.get('years')}, "
            f"aggregation={parsed_result.get('aggregation')}"
        )

        # CENTRALIZAÇÃO: filter_final é populado EXCLUSIVAMENTE pelo filter_classifier.
        # Se filter_final está vazio, significa que não há filtros na query.
        # O graphic_classifier NÃO deve gerar filtros próprios.

    except Exception as e:
        logger.error(f"[parse_query_node] Error parsing query: {str(e)}")
        state["errors"].append(f"Query parsing error: {str(e)}")
        state["parsed_entities"] = {}

    # FASE 2: Store start time in state
    state["_graphic_classifier_start_time"] = graphic_classifier_start_time

    return state


def detect_keywords_node(state: GraphState) -> GraphState:
    """
    Detect keywords and chart-type indicators in the query.

    PHASE 2 ENHANCEMENT: Now includes context-aware scoring.

    This node uses the keyword_detector tool to identify:
    - Chart type indicators (ranking, trend, comparison, etc.)
    - Aggregation hints (sum, average, count)
    - Sorting requirements (top, bottom, ascending, descending)
    - Filtering keywords
    - PHASE 2: Semantic context for weighted keyword scoring

    Args:
        state: Current graph state

    Returns:
        Updated graph state with detected_keywords and query_context populated
    """
    query = state.get("query", "")
    parsed_entities = state.get("parsed_entities", {})

    logger.info(f"[detect_keywords_node] Detecting keywords in query")

    try:
        # Detect keywords using keyword_detector tool
        keywords = detect_keywords(query)

        # Update state with detected keywords
        state["detected_keywords"] = keywords

        # PHASE 2: Get chart type hints with context-aware scoring
        # Pass parsed_entities for dimension analysis in context
        chart_hints = get_chart_type_hints(
            query, parsed_entities=parsed_entities, use_weighted_scoring=True
        )

        # PHASE 2: Extract and store context for debugging/logging
        from src.graphic_classifier.tools.context_analyzer import extract_query_context

        context = extract_query_context(query, parsed_entities)
        state["query_context"] = context

        logger.info(
            f"[detect_keywords_node] Detected {len(keywords)} keywords, "
            f"chart hints (weighted): {list(chart_hints.keys())[:3]}"
        )
        logger.debug(f"[detect_keywords_node] Context: {context}")

    except Exception as e:
        logger.error(f"[detect_keywords_node] Error detecting keywords: {str(e)}")
        state["errors"].append(f"Keyword detection error: {str(e)}")
        state["detected_keywords"] = []
        state["query_context"] = {}

    return state


def classify_intent_node(state: GraphState) -> GraphState:
    """
    Classify user intent and determine the appropriate chart type.

    FASE 2 & 3 ENHANCEMENT: Now uses Intent Classifier + three-level Decision Tree before LLM.

    This node implements a hierarchical classification strategy:
    0. Level 0 (Intent Classifier - FASE 2): Specific intent patterns with configuration mapping
    1. Level 1 (Detection): High-confidence pattern matching (0.90-0.95)
    2. Level 2 (Context Analysis): Context-based disambiguation (0.75-0.90)
    3. Level 3 (Fallback): LLM-based classification for ambiguous cases

    Expected metrics:
    - 70-90% of queries resolved in Level 0, 1 or 2 (no LLM)
    - 10-30% require LLM fallback
    - Overall accuracy >= 95%

    Args:
        state: Current graph state

    Returns:
        Updated graph state with intent, chart_type, and confidence populated
    """
    query = state.get("query", "")
    keywords = state.get("detected_keywords", [])
    parsed_entities = state.get("parsed_entities", {})

    logger.info(f"[classify_intent_node] Classifying intent for query")

    try:
        # Extract query context (needed for all levels)
        context = extract_query_context(query, parsed_entities)
        logger.debug(f"[classify_intent_node] Context extracted: {context}")

        # ============================================================
        # FASE 1: Check Semantic Anchor FIRST (HIGHEST PRIORITY)
        # ============================================================
        # The semantic layer (extract_semantic_anchor_node) MUST be respected.
        # If it determined the query is factual (no visualization), bypass all heuristics.
        semantic_anchor = state.get("semantic_anchor")
        if semantic_anchor:
            goal = semantic_anchor.get("semantic_goal")
            axis = semantic_anchor.get("comparison_axis")

            # Factual query: user wants a single aggregated value, not a chart
            # Examples: "Qual foi o total de vendas?", "Quanto vendemos em 2015?"
            if goal == "factual" and axis == "none":
                logger.info(
                    "[classify_intent_node] Semantic anchor indicates factual query "
                    "(goal=factual, axis=none). Query requires aggregated response, not visualization. "
                    "Bypassing IntentClassifier and routing to non_graph_executor."
                )
                state["intent"] = "factual"
                state["chart_type"] = None
                state["confidence"] = semantic_anchor.get("confidence", 0.95)
                state["level_used"] = -1  # -1 = semantic layer (highest priority)
                return state

        # ============================================================
        # FASE 7: Nested Ranking Detection (BEFORE Intent Classifier)
        # ============================================================
        # Nested ranking patterns like "top N X dos M maiores Y" require
        # bar_vertical_stacked, not bar_horizontal. Detect EARLY to override
        # semantic mapping if necessary.
        from src.graphic_classifier.utils.ranking_detector import extract_nested_ranking

        nested_ranking = extract_nested_ranking(query)

        if nested_ranking.get("is_nested"):
            logger.info(
                f"[classify_intent_node] NESTED RANKING detected: "
                f"top {nested_ranking['top_n']} {nested_ranking.get('subgroup_entity', 'items')} "
                f"within {nested_ranking['group_top_n']} {nested_ranking.get('group_entity', 'groups')}"
            )

            # FASE 7.2: Mapear entidades para colunas usando AliasMapper (via alias.yaml)
            from src.graphic_classifier.utils.ranking_detector import map_nested_ranking_to_columns
            nested_ranking_mapped = map_nested_ranking_to_columns(nested_ranking)

            # FORCE chart_type to bar_vertical_stacked for nested ranking
            # This MUST override any previous classification (even semantic mapping)
            # because Intent Classifier would incorrectly map to bar_horizontal
            current_chart_type = state.get("chart_type")
            state["chart_type"] = "bar_vertical_stacked"
            state["intent"] = "composition_analysis"
            state["confidence"] = 0.95
            state["level_used"] = -2  # -2 = nested ranking override (special layer)

            # Store nested ranking info with column mapping for downstream use
            if "parsed_entities" not in state:
                state["parsed_entities"] = {}
            state["parsed_entities"]["nested_ranking"] = nested_ranking_mapped
            state["parsed_entities"]["group_top_n"] = nested_ranking["group_top_n"]
            state["parsed_entities"]["top_n"] = nested_ranking["top_n"]

            logger.info(
                f"[classify_intent_node] NESTED RANKING OVERRIDE: {current_chart_type or 'None'} -> "
                f"bar_vertical_stacked (nested ranking requires composition, skipping Intent Classifier)"
            )

            # CRITICO: Definir ordem explicita das dimensoes
            # Para "top N X dos M Y": [Y (grupo), X (subgrupo)]
            # Isto garante que o grupo principal seja X-axis e subgrupo seja hue/stack
            ordered_dimensions = []
            if nested_ranking_mapped.get("group_column"):
                ordered_dimensions.append(nested_ranking_mapped["group_column"])
            if nested_ranking_mapped.get("subgroup_column"):
                ordered_dimensions.append(nested_ranking_mapped["subgroup_column"])

            # Set intent_config for downstream processing
            state["intent_config"] = {
                "requires_temporal_comparison": False,
                "requires_calculated_fields": False,
                "dimension_structure": {
                    "primary": "outer_entity",
                    "series": "inner_entity",
                    "ordered_dimensions": ordered_dimensions,  # Ex: [UF_Cliente, Cod_Cliente]
                },
                "sort_config": {"by": "value", "order": nested_ranking.get("sort_order", "desc")},
                "aggregation_hint": "sum",
                "nested_ranking_config": nested_ranking_mapped,
            }

            logger.info(
                f"[classify_intent_node] NESTED RANKING ordered_dimensions: {ordered_dimensions}"
            )

            return state  # Early return - MUST skip Intent Classifier to prevent override

        # ============================================================
        # FASE 2: Level 0 - Intent Classifier (Specific Patterns)
        # ============================================================
        from src.graphic_classifier.tools.intent_classifier import classify_intent

        intent_result = classify_intent(query, context, parsed_entities)

        if intent_result:
            # Success! Use intent classifier result
            intent_name = intent_result.get("intent")
            confidence = intent_result.get("confidence", 0.0)
            intent_config = intent_result.get("config")
            reasoning = intent_result.get("reasoning", "")

            chart_type = intent_config.chart_type

            state["intent"] = intent_name
            state["chart_type"] = chart_type
            state["confidence"] = confidence
            state["level_used"] = 0
            state["intent_config"] = {
                "requires_temporal_comparison": intent_config.requires_temporal_comparison,
                "requires_calculated_fields": intent_config.requires_calculated_fields,
                "dimension_structure": intent_config.dimension_structure,
                "sort_config": intent_config.sort_config,
                "aggregation_hint": intent_config.aggregation_hint,
            }

            # FASE 4 - CORRECAO CRITICA #2: Forcar requires_tabular_data=true para temporal_comparison_analysis
            if intent_name == "temporal_comparison_analysis":
                state["requires_tabular_data"] = True
                logger.info(
                    "[classify_intent_node] FASE 4: Forced requires_tabular_data=True "
                    "for temporal_comparison_analysis intent"
                )

            logger.info(
                f"[classify_intent_node] ✓ Intent Classifier (Level 0) SUCCESS: "
                f"intent={intent_name}, chart_type={chart_type}, "
                f"confidence={confidence:.2f}, bypassed_llm=True, tokens=0"
            )
            logger.debug(f"[classify_intent_node] Reasoning: {reasoning}")
            logger.debug(f"[classify_intent_node] Config: {state['intent_config']}")

            return state

        logger.debug(
            "[classify_intent_node] Intent Classifier returned no match, trying Decision Tree"
        )

        # ============================================================
        # FASE 3: Decision Tree Classification (Levels 1 & 2)
        # ============================================================

        # Get keyword scores for Level 2 analysis
        chart_hints = get_chart_type_hints(
            query, parsed_entities=parsed_entities, use_weighted_scoring=True
        )

        # Get decision tree classifier
        decision_tree = _get_decision_tree_classifier()

        # Attempt classification via decision tree
        dt_result = decision_tree.classify(
            query=query,
            context=context,
            keyword_scores=chart_hints,
            parsed_entities=parsed_entities,
        )

        # Check if decision tree provided confident result (Level 1 or 2)
        if dt_result.get("bypassed_llm", False):
            # Success! Use decision tree result
            chart_type = dt_result.get("chart_type")
            confidence = dt_result.get("confidence", 0.0)
            reasoning = dt_result.get("reasoning", "")
            level = dt_result.get("level_used", 0)

            # Infer intent from chart type
            intent_map = {
                "bar_horizontal": "ranking",
                "bar_vertical": "comparison",
                "line": "temporal_trend",
                "line_composed": "multi_temporal_trend",
                "pie": "distribution",
                "bar_vertical_stacked": "composition",
                "histogram": "value_distribution",
            }
            intent = intent_map.get(chart_type, "unknown")

            state["intent"] = intent
            state["chart_type"] = chart_type
            state["confidence"] = confidence
            state["level_used"] = level

            logger.info(
                f"[classify_intent_node] ✓ Decision Tree Level {level} SUCCESS: "
                f"chart_type={chart_type}, confidence={confidence:.2f}, bypassed_llm=True, tokens=0"
            )
            logger.debug(f"[classify_intent_node] Reasoning: {reasoning}")

            return state

        # ============================================================
        # FASE 3: Level 3 Fallback - LLM Classification
        # ============================================================
        logger.info(
            "[classify_intent_node] Decision Tree returned no confident result, "
            "falling back to LLM (Level 3)"
        )

        # Import token tracker
        from src.shared_lib.utils.token_tracker import extract_token_usage

        llm = _get_llm()
        system_prompt = _load_system_prompt()

        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"Query: {query}"),
        ]

        response = llm.invoke(messages)
        response_text = response.content

        # Capture tokens from LLM response (accumulate within agent)
        tokens = extract_token_usage(response, llm)
        if tokens.get("total_tokens", 0) > 0:
            if "agent_tokens" not in state or not isinstance(
                state.get("agent_tokens"), dict
            ):
                state["agent_tokens"] = {}
            current = state["agent_tokens"].get(
                "graphic_classifier",
                {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "total_tokens": 0,
                    "llm_calls": 0,
                    "model_name": "unknown",
                },
            )
            if not isinstance(current, dict):
                current = {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "total_tokens": 0,
                    "llm_calls": 0,
                    "model_name": "unknown",
                }
            current["input_tokens"] = int(current.get("input_tokens", 0) or 0) + int(
                tokens.get("input_tokens", 0) or 0
            )
            current["output_tokens"] = int(current.get("output_tokens", 0) or 0) + int(
                tokens.get("output_tokens", 0) or 0
            )
            current["total_tokens"] = int(current.get("total_tokens", 0) or 0) + int(
                tokens.get("total_tokens", 0) or 0
            )
            current["llm_calls"] = int(current.get("llm_calls", 0) or 0) + 1

            # Track models used (similar to TokenAccumulator)
            new_model = tokens.get("model_name", "unknown")
            if new_model != "unknown":
                if current.get("model_name") == "unknown":
                    current["model_name"] = new_model
                elif current.get("model_name") != new_model:
                    # Multiple models - track in list
                    if "models_used" not in current:
                        # First time we see a different model
                        current["models_used"] = [current["model_name"], new_model]
                        current["model_name"] = "multiple"
                    elif new_model not in current["models_used"]:
                        current["models_used"].append(new_model)

            state["agent_tokens"]["graphic_classifier"] = current
        logger.info(
            f"[classify_intent_node] LLM tokens captured: "
            f"input={tokens['input_tokens']}, "
            f"output={tokens['output_tokens']}, "
            f"total={tokens['total_tokens']}, "
            f"model={tokens.get('model_name', 'unknown')}"
        )

        # Parse LLM response
        intent_match = re.search(
            r"INTENT:\s*(.+?)(?:\n|$)", response_text, re.IGNORECASE
        )
        chart_type_match = re.search(
            r"CHART_TYPE:\s*(.+?)(?:\n|$)", response_text, re.IGNORECASE
        )
        confidence_match = re.search(
            r"CONFIDENCE:\s*([\d.]+)", response_text, re.IGNORECASE
        )

        if intent_match and chart_type_match and confidence_match:
            from src.graphic_classifier.utils.chart_type_sanitizer import (
                sanitize_chart_type,
                validate_chart_type_format,
            )

            intent = intent_match.group(1).strip()
            chart_type_raw = chart_type_match.group(1).strip()
            confidence = float(confidence_match.group(1))

            # Sanitize chart_type (Issue #1 fix)
            chart_type = sanitize_chart_type(chart_type_raw)

            # Log if sanitization was applied
            if chart_type != chart_type_raw.strip().lower():
                logger.info(
                    f"[classify_intent_node] Sanitized chart_type: "
                    f"'{chart_type_raw}' -> '{chart_type}'"
                )

            # Detect if LLM is not following prompt instructions
            if chart_type and not validate_chart_type_format(chart_type_raw):
                logger.warning(
                    f"[classify_intent_node] LLM returned chart_type with descriptive text: "
                    f"'{chart_type_raw}'. This indicates LLM is not following prompt instructions. "
                    f"Sanitized to: '{chart_type}'"
                )

            state["intent"] = intent
            state["chart_type"] = chart_type
            state["confidence"] = confidence

            logger.info(
                f"[classify_intent_node] LLM classification (Level 3): "
                f"intent='{intent}', chart_type={chart_type}, confidence={confidence}"
            )
        else:
            # Fallback: use keyword-based classification if available
            best_chart_type = get_best_chart_type(
                query,
                parsed_entities=parsed_entities,
                use_weighted_scoring=True,
                threshold=0.5,
            )

            if best_chart_type:
                # Use the chart type with weighted scoring
                confidence = chart_hints.get(best_chart_type, 0.5)
                intent_map = {
                    "bar_horizontal": "ranking",
                    "bar_vertical": "comparison",
                    "line": "temporal_trend",
                    "line_composed": "multi_temporal_trend",
                    "pie": "distribution",
                    "bar_vertical_stacked": "composition",
                    "histogram": "value_distribution",
                }
                intent = intent_map.get(best_chart_type, "unknown")
                state["intent"] = intent
                state["chart_type"] = best_chart_type
                state["confidence"] = confidence
                logger.warning(
                    "[classify_intent_node] Could not parse LLM response, using weighted keyword fallback"
                )
            else:
                # Last resort: mark as unclassified
                state["intent"] = "unclassified"
                state["chart_type"] = None
                state["confidence"] = 0.0
                state["errors"].append(
                    "Could not classify intent from LLM response or keywords"
                )
                logger.error("[classify_intent_node] Classification failed")

    except Exception as e:
        logger.error(f"[classify_intent_node] Error classifying intent: {str(e)}")
        state["errors"].append(f"Intent classification error: {str(e)}")

        # Try keyword fallback with weighted scoring
        try:
            best_chart_type = get_best_chart_type(
                query,
                parsed_entities=parsed_entities,
                use_weighted_scoring=True,
                threshold=0.75,
            )
            if best_chart_type:
                chart_hints = get_chart_type_hints(
                    query, parsed_entities=parsed_entities, use_weighted_scoring=True
                )
                confidence = chart_hints.get(best_chart_type, 0.3)
                # Use fallback with reasonable confidence
                if confidence >= 0.70:
                    intent_map = {
                        "bar_horizontal": "ranking",
                        "bar_vertical": "comparison",
                        "line": "temporal_trend",
                        "line_composed": "multi_temporal_trend",
                        "pie": "distribution",
                        "bar_vertical_stacked": "composition",
                        "histogram": "value_distribution",
                    }
                    intent = intent_map.get(best_chart_type, "unknown")
                    state["intent"] = intent
                    state["chart_type"] = best_chart_type
                    state["confidence"] = confidence
                    logger.info(
                        f"[classify_intent_node] Exception fallback: using weighted keyword "
                        f"classification with confidence={confidence:.2f}"
                    )
                else:
                    # Confidence too low, return error state
                    state["intent"] = "error"
                    state["chart_type"] = None
                    state["confidence"] = 0.0
            else:
                state["intent"] = "error"
                state["chart_type"] = None
                state["confidence"] = 0.0
        except:
            state["intent"] = "error"
            state["chart_type"] = None
            state["confidence"] = 0.0

    # ========================================================================
    # FASE 3 - DEPRECATED TYPE DETECTION: Auto-correct to prevent regressions
    # ========================================================================
    # bar_vertical_composed is deprecated and should NEVER be returned
    # If detected, auto-correct to line_composed and log critical warning
    chart_type = state.get("chart_type")
    if chart_type == "bar_vertical_composed":
        logger.critical(
            "[classify_intent_node] DEPRECATED TYPE DETECTED: bar_vertical_composed. "
            "This should NEVER happen! Auto-correcting to line_composed."
        )

        # Auto-correct to line_composed
        state["chart_type"] = "line_composed"
        state["intent"] = "multi_temporal_trend"

        # Update reasoning to indicate auto-correction
        reasoning = state.get("reasoning", "")
        state["reasoning"] = (
            f"{reasoning} [CRITICAL AUTO-CORRECTION: Deprecated type 'bar_vertical_composed' "
            f"detected and replaced with 'line_composed']"
        )

        logger.warning(
            f"[classify_intent_node] Auto-corrected deprecated type to line_composed"
        )

    # FASE 5: Detecção de dados tabulares
    query_lower = query.lower()
    TABULAR_KEYWORDS = [
        "tabela",
        "dados brutos",
        "registros",
        "mostre os dados",
        "ver dados",
        "dataset",
        "mostrar tabela",
        "exibir tabela",
        "dados em tabela",
    ]

    requires_tabular_data = any(kw in query_lower for kw in TABULAR_KEYWORDS)
    state["requires_tabular_data"] = requires_tabular_data

    if requires_tabular_data:
        logger.info(
            f"[classify_intent_node] Tabular data request detected in query: '{query}'"
        )

    return state


def load_dataset_metadata_node(state: GraphState) -> GraphState:
    """
    Load dataset metadata (column names) early for validation.

    This node extracts the available columns from the data source without
    loading the full dataset into memory. This allows Phase 1 to validate
    that detected columns actually exist before passing to Phase 2.

    Args:
        state: Current graph state

    Returns:
        Updated graph state with data_source and available_columns populated
    """
    logger.info(f"[load_dataset_metadata_node] Loading dataset metadata")

    try:
        from pathlib import Path

        # Determine data source path
        # Priority: 1) state data_source, 2) default path
        data_source = state.get("data_source")

        if not data_source:
            # Use centralized dataset path
            from src.shared_lib.core.config import get_dataset_path
            try:
                data_source = get_dataset_path()
                logger.info(
                    f"[load_dataset_metadata_node] Using configured data source: {data_source}"
                )
            except ValueError:
                logger.warning(
                    f"[load_dataset_metadata_node] No data source specified and DATASET_PATH not set"
                )
                # Don't fail here - let Phase 2 handle it
                state["available_columns"] = []
                return state

        # Store data source in state
        state["data_source"] = data_source

        # Extract available columns
        column_extractor = _get_column_extractor()
        available_columns = column_extractor.get_columns(data_source)

        state["available_columns"] = available_columns

        logger.info(
            f"[load_dataset_metadata_node] Loaded {len(available_columns)} columns from {Path(data_source).name}"
        )
        logger.debug(
            f"[load_dataset_metadata_node] Available columns: {available_columns}"
        )

    except Exception as e:
        logger.error(
            f"[load_dataset_metadata_node] Error loading dataset metadata: {str(e)}"
        )
        state["errors"].append(f"Dataset metadata loading error: {str(e)}")
        state["available_columns"] = []

    return state


def map_columns_node(state: GraphState) -> GraphState:
    """
    Map column references from the query to actual column names.

    This node uses the AliasMapper to resolve:
    - Column references mentioned in the query
    - Metric names
    - Grouping columns
    - Filter columns

    All mappings are done dynamically via alias.yaml with no hardcoding.
    Validates that mapped columns actually exist in the dataset.

    Args:
        state: Current graph state

    Returns:
        Updated graph state with mapped_columns and columns_mentioned populated
    """
    query = state.get("query", "")
    parsed_entities = state.get("parsed_entities", {})

    logger.info(f"[map_columns_node] Mapping columns for query")

    try:
        alias_mapper = _get_alias_mapper()

        # Get available columns from dataset
        available_columns = state.get("available_columns", [])
        available_columns_set = set(available_columns) if available_columns else set()

        # Extract potential column references from parsed entities
        # IMPORTANT: Do NOT extract from filters - filter_classifier is the single source of truth
        # Only use potential_columns from query parsing (dimensions/metrics)
        potential_columns = parsed_entities.get("potential_columns", [])

        # Store raw column mentions
        state["columns_mentioned"] = potential_columns

        # Map each potential column reference and validate against dataset
        mapped_columns = {}
        for col_ref in potential_columns:
            resolved = alias_mapper.resolve(col_ref)
            if resolved:
                # Validate that the resolved column exists in the dataset
                if available_columns_set and resolved not in available_columns_set:
                    logger.warning(
                        f"[map_columns_node] Column '{resolved}' (from '{col_ref}') "
                        f"not found in dataset. Skipping."
                    )
                    continue

                mapped_columns[col_ref] = resolved
                logger.debug(f"[map_columns_node] Mapped '{col_ref}' -> '{resolved}'")
            else:
                logger.warning(
                    f"[map_columns_node] Could not resolve column reference: '{col_ref}'"
                )

        state["mapped_columns"] = mapped_columns

        logger.info(
            f"[map_columns_node] Mapped {len(mapped_columns)}/{len(potential_columns)} column references "
            f"(validated against {len(available_columns_set)} dataset columns)"
        )

    except Exception as e:
        logger.error(f"[map_columns_node] Error mapping columns: {str(e)}")
        state["errors"].append(f"Column mapping error: {str(e)}")
        state["columns_mentioned"] = []
        state["mapped_columns"] = {}

    return state


def generate_output_node(state: GraphState) -> GraphState:
    """
    Generate the final JSON output based on all collected information.

    This node:
    1. Combines all information from previous nodes
    2. Applies chart-specific logic
    3. Validates against the ChartOutput schema
    4. Formats for downstream consumption

    FASE 2 Enhancement: Calculates and stores graphic_classifier execution time.

    Args:
        state: Current graph state

    Returns:
        Updated graph state with output populated
    """
    logger.info(f"[generate_output_node] Generating final output")

    # FASE 2: Calculate graphic_classifier execution time
    start_time = state.get("_graphic_classifier_start_time")
    if start_time:
        classifier_execution_time = time.perf_counter() - start_time
        logger.info(
            f"[generate_output_node] graphic_classifier execution time: {classifier_execution_time:.4f}s"
        )
    else:
        classifier_execution_time = 0.0
        logger.warning("[generate_output_node] No start time found, execution time=0.0")

    try:
        query = state.get("query", "")
        chart_type = state.get("chart_type")
        parsed_entities = state.get("parsed_entities", {})
        mapped_columns = state.get("mapped_columns", {})
        available_columns = state.get("available_columns", [])
        errors = state.get("errors", [])

        # Handle invalid parsed_entities (None indicates error state)
        if parsed_entities is None:
            state["output"] = {
                "chart_type": None,
                "message": "Invalid query state: parsed_entities is None",
                "errors": errors + ["Invalid parsed_entities state"],
                "metrics": [],
            }
            return state

        # PHASE 5+ INTEGRATION: Use filters exclusively from filter_classifier
        # The filter_classifier is the single source of truth for all filters
        # graphic_classifier no longer performs redundant filter parsing
        filter_final = state.get("filter_final", {})

        logger.info(
            f"[generate_output_node] Using filters from filter_classifier: {len(filter_final)} filters"
        )
        logger.debug(f"[generate_output_node] filter_final: {filter_final}")

        # Use filter_final directly as combined_filters
        combined_filters = filter_final

        # ====================================================================
        # FASE 2: DIMENSION MANAGEMENT & SORT RESOLUTION
        # ====================================================================
        intent = state.get("intent", "")
        intent_config = state.get("intent_config")

        # Obter dimensoes do parsed_entities (serao processadas pelo formatter)
        dimensions_raw = parsed_entities.get("dimensions", [])
        metrics_raw = parsed_entities.get("metrics", [])

        # Se temos intent_config, processar dimensoes e sort
        dimension_analysis = None
        sort_analysis = None

        if chart_type and dimensions_raw:
            # Analisar dimensoes e atribuir roles (primary/series)
            from src.graphic_classifier.tools.dimension_manager import (
                analyze_dimensions,
                validate_dimension_structure,
            )

            try:
                dimension_analysis = analyze_dimensions(
                    dimensions_raw, chart_type, intent_config
                )

                # Validar estrutura de dimensoes
                is_valid_dims, dim_errors = validate_dimension_structure(
                    dimensions_raw, chart_type, intent_config
                )

                if not is_valid_dims:
                    logger.warning(
                        f"[generate_output_node] Dimension structure validation failed: {dim_errors}"
                    )
                    errors.extend(dim_errors)

                logger.info(
                    f"[generate_output_node] FASE 2: Dimension analysis complete - "
                    f"{len(dimension_analysis)} dimensions analyzed"
                )

                # Adicionar dimension_analysis ao state para uso downstream
                state["dimension_analysis"] = {
                    name: {
                        "is_temporal": info.is_temporal,
                        "granularity": info.granularity,
                        "role": info.role,
                    }
                    for name, info in dimension_analysis.items()
                }

            except Exception as e:
                logger.error(
                    f"[generate_output_node] Error in dimension analysis: {str(e)}"
                )
                errors.append(f"Dimension analysis error: {str(e)}")

        # Processar sort usando sort_manager
        if intent:
            from src.graphic_classifier.tools.sort_manager import SortManager

            try:
                sort_manager = SortManager()

                # CRITICAL: Pass parsed_entities to allow SortManager to use sort_by from semantic mapping
                sort_analysis = sort_manager.process(
                    query=query,
                    intent=intent,
                    intent_config=intent_config,
                    dimensions=dimensions_raw,
                    metrics=metrics_raw,
                    parsed_entities=parsed_entities,  # NOVO: passar para priorizar semantic mapping
                )

                # Validar e adicionar warnings se houver
                if not sort_analysis["is_valid"]:
                    logger.warning(
                        f"[generate_output_node] Sort validation warnings: {sort_analysis['warnings']}"
                    )
                    # Nao adicionar a errors (sao warnings, nao errors criticos)

                logger.info(
                    f"[generate_output_node] FASE 2: Sort analysis complete - "
                    f"by={sort_analysis['sort_config']['by']}, "
                    f"order={sort_analysis['sort_config']['order']}, "
                    f"requires_calculated={sort_analysis['requires_calculated_field']}"
                )

                # Adicionar sort_analysis ao state para uso downstream
                state["sort_analysis"] = sort_analysis

            except Exception as e:
                logger.error(f"[generate_output_node] Error in sort analysis: {str(e)}")
                errors.append(f"Sort analysis error: {str(e)}")

        # ====================================================================
        # FASE 3.1: VALIDAÇÃO DE RANKING OPERATIONS (Safety Check)
        # ====================================================================
        # Com a detecção upstream em query_parser, este bloco agora é apenas
        # uma validação final (safety net) ao invés de cleanup reativo.
        # Se ranking operations chegarem aqui, algo falhou upstream.

        from src.graphic_classifier.utils.ranking_detector import (
            validate_no_ranking_in_filters,
        )

        is_valid, invalid_filters = validate_no_ranking_in_filters(combined_filters)

        if not is_valid:
            # Remover filtros inválidos (safety net)
            for invalid in invalid_filters:
                key = invalid["key"]
                if key in combined_filters:
                    del combined_filters[key]
                    logger.error(
                        f"[generate_output_node] SAFETY NET: Removed ranking filter that escaped upstream detection: "
                        f"{key}={invalid['value']}. This should have been caught in query_parser!"
                    )

            errors.append(
                f"SAFETY NET: Removed {len(invalid_filters)} ranking filters that escaped upstream detection"
            )

            # Atualizar state com filtros limpos
            state["filter_final"] = combined_filters

        # Prepare data for formatting
        # IMPORTANT: Pass combined_filters to include both parsed and filter_classifier filters
        data = {
            "query": query,
            "intent": state.get("intent"),
            "chart_type": chart_type,
            "parsed_entities": parsed_entities,
            "mapped_columns": mapped_columns,
            "columns_detected": state.get("columns_mentioned"),
            "data_source": state.get("data_source"),
            "filters": combined_filters,  # Use combined filters from both sources
            # FASE 2: Adicionar dimension_analysis, sort_analysis e intent_config
            "dimension_analysis": state.get("dimension_analysis"),
            "sort_analysis": state.get("sort_analysis"),
            "intent_config": state.get("intent_config"),
            # CRITICAL: Add polarity from semantic_anchor for variation filtering
            "polarity": state.get("semantic_anchor", {}).get("polarity")
            if state.get("semantic_anchor")
            else None,
        }

        # FASE 1.2: Validacao pre-format
        pre_errors = _validate_pre_format(data)
        if pre_errors:
            logger.error(
                f"[generate_output_node] Pre-format validation failed: {pre_errors}"
            )
            errors.extend(pre_errors)
            # Adicionar ao state para tracking
            state["errors"] = errors

        # Use json_formatter to create the output
        output = format_output(data)

        # Validate the output
        validated_output = validate_chart_output(output)

        # CHART-TYPE-SPECIFIC TRANSFORMATION
        # Apply intelligent transformation to ensure specs are complete and correct
        chart_type = validated_output.get("chart_type")
        if chart_type and chart_type != "null":
            try:
                transformer = ChartSpecTransformer(available_columns=available_columns)
                transformed_output = transformer.transform(validated_output)

                # Validate transformed output
                is_valid, validation_errors = validate_spec(transformed_output)

                if not is_valid:
                    logger.warning(
                        f"Transformed output validation failed for {chart_type}: {validation_errors}"
                    )
                    errors.extend(validation_errors)
                else:
                    logger.info(
                        f"Chart spec transformation successful for {chart_type}"
                    )

                validated_output = transformed_output

            except Exception as e:
                logger.error(
                    f"Chart spec transformation failed: {str(e)}", exc_info=True
                )
                errors.append(f"Transformation error: {str(e)}")

        # ====================================================================
        # FASE 5: AUTOMATIC CATEGORY LIMITATION (Data Slicing)
        # ====================================================================
        # Apply intelligent category limits for readability WITHOUT changing chart family
        # CRITICAL: This is COSMETIC (how much to show), not SEMANTIC (what chart type)
        from src.graphic_classifier.rendering.category_limiter import CategoryLimiter

        limiter = CategoryLimiter()

        if chart_type and chart_type != "null":
            try:
                # Apply automatic limiting if appropriate
                validated_output = limiter.apply_limit_to_spec(validated_output)

                # Log if limit was applied
                limit_metadata = validated_output.get("limit_metadata")
                if limit_metadata and limit_metadata.get("limit_applied"):
                    logger.info(
                        f"[generate_output_node] FASE 5 Category Limiter: "
                        f"{limit_metadata.get('limit_source')} limit applied - "
                        f"display_count={limit_metadata.get('display_count')}, "
                        f"reason='{limit_metadata.get('limit_reason')}'"
                    )
                else:
                    logger.debug(
                        f"[generate_output_node] FASE 5 Category Limiter: No limit needed for {chart_type}"
                    )

            except Exception as e:
                logger.error(
                    f"[generate_output_node] FASE 5 Category Limiter error: {str(e)}"
                )
                errors.append(f"Category limiter error: {str(e)}")

        # ====================================================================
        # FASE 5: CHART TYPE CROSS-FIELD VALIDATION
        # ====================================================================
        # Validate consistency between chart_type and data structure
        # (dimensions, filters, temporal_granularity)
        from src.graphic_classifier.validators.chart_validator import (
            ChartTypeValidator,
        )

        validator = ChartTypeValidator()

        # Prepare validation input
        validation_input = {
            "chart_type": validated_output.get("chart_type"),
            "dimensions": validated_output.get("dimensions", []),
            "filters": validated_output.get("filters", {}),
            "confidence": state.get("confidence", 0.0),
            "intent": state.get("intent"),
            "group_top_n": validated_output.get("group_top_n"),
            # LAYER 6: Include intent_config for single_line variant detection
            "_intent_config": validated_output.get("_intent_config"),
            "intent_config": state.get("intent_config"),
        }

        # Run validation
        is_valid, validation_warnings = validator.validate(validation_input)

        if not is_valid:
            logger.warning(
                f"[generate_output_node] FASE 5 validation detected {len(validation_warnings)} issue(s): "
                f"{validation_warnings}"
            )

            # Check if we should attempt automatic correction
            confidence = validation_input["confidence"]
            should_correct = confidence < ChartTypeValidator.LOW_CONFIDENCE_THRESHOLD

            if should_correct:
                # Attempt to suggest correction
                suggestion = validator.suggest_correction(
                    validation_input, validation_warnings
                )

                if suggestion:
                    suggested_type = suggestion["suggested_chart_type"]
                    suggested_confidence = suggestion["confidence"]
                    reason = suggestion["reason"]

                    logger.info(
                        f"[generate_output_node] FASE 5 auto-correction: "
                        f"{validation_input['chart_type']} -> {suggested_type} "
                        f"(confidence: {confidence:.2f} -> {suggested_confidence:.2f}). "
                        f"Reason: {reason}"
                    )

                    # Apply correction
                    validated_output["chart_type"] = suggested_type
                    validated_output["confidence"] = suggested_confidence
                    state["chart_type"] = suggested_type
                    state["confidence"] = suggested_confidence

                    # Add correction info to output
                    if "processing_notes" not in validated_output:
                        validated_output["processing_notes"] = []
                    validated_output["processing_notes"].append(
                        {
                            "type": "FASE_5_AUTO_CORRECTION",
                            "original_chart_type": validation_input["chart_type"],
                            "corrected_chart_type": suggested_type,
                            "reason": reason,
                            "warnings": validation_warnings,
                        }
                    )
                else:
                    # No correction suggestion available, log warnings only
                    logger.warning(
                        f"[generate_output_node] FASE 5: No correction suggestion available. "
                        f"Keeping chart_type={validation_input['chart_type']}"
                    )
                    # Add warnings to output for transparency
                    if "validation_warnings" not in validated_output:
                        validated_output["validation_warnings"] = []
                    validated_output["validation_warnings"].extend(validation_warnings)
            else:
                # High confidence but with warnings - log for monitoring
                logger.info(
                    f"[generate_output_node] FASE 5: Validation warnings detected but confidence "
                    f"({confidence:.2f}) >= threshold ({ChartTypeValidator.LOW_CONFIDENCE_THRESHOLD}). "
                    f"Keeping chart_type={validation_input['chart_type']}"
                )
                # Add warnings to output for transparency
                if "validation_warnings" not in validated_output:
                    validated_output["validation_warnings"] = []
                validated_output["validation_warnings"].extend(validation_warnings)
        else:
            logger.debug(
                f"[generate_output_node] FASE 5 validation passed: {validation_input['chart_type']} is consistent"
            )

        # ====================================================================
        # PHASE 5+ FILTER INTEGRITY VALIDATION
        # ====================================================================
        # Ensure that filters in the output exactly match filter_final
        # This prevents any intermediate component from modifying filters
        original_filter_final = state.get("filter_final", {})
        output_filters = validated_output.get("filters", {})

        if original_filter_final != output_filters:
            logger.warning(
                f"[generate_output_node] Filter mismatch detected! "
                f"filter_final={original_filter_final}, output_filters={output_filters}"
            )
            # Force output filters to match filter_final (single source of truth)
            validated_output["filters"] = original_filter_final
            logger.info(
                f"[generate_output_node] Filters corrected to match filter_final: {original_filter_final}"
            )

        # FASE 5: Adicionar flag de dados tabulares ao output
        requires_tabular_data = state.get("requires_tabular_data", False)
        validated_output["requires_tabular_data"] = requires_tabular_data

        # If there were errors during processing, add them to the output
        if errors:
            validated_output["processing_errors"] = errors
            validated_output["errors"] = (
                errors  # Also add to 'errors' for compatibility
            )
            if not validated_output.get("message"):
                validated_output["message"] = (
                    "Query processed with warnings. See processing_errors for details."
                )

        state["output"] = validated_output

        logger.info(
            f"[generate_output_node] Output generated: "
            f"chart_type={validated_output.get('chart_type')}, "
            f"metrics={validated_output.get('metrics')}"
        )

    except Exception as e:
        logger.error(f"[generate_output_node] Error generating output: {str(e)}")

        # Create error output
        state["output"] = {
            "chart_type": None,
            "message": f"Failed to generate output: {str(e)}",
            "errors": state.get("errors", []) + [str(e)],
            "metrics": [],
        }

    # FASE 2: Store classifier execution time
    state["classifier_execution_time"] = classifier_execution_time

    return state


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def execute_analytics_node(state: GraphState) -> GraphState:
    """
    Execute analytics using the analytics_executor_agent (Phase 2 integration).

    This node integrates the Phase 2 analytics executor into the LangGraph workflow,
    allowing seamless execution from classifier to data processing.

    The node:
    1. Takes the output from generate_output_node (Phase 1)
    2. Passes it to the analytics_executor_agent
    3. Executes the query with DuckDB/Pandas fallback
    4. Returns processed data ready for Plotly

    Args:
        state: Current graph state (must contain 'output' from Phase 1)

    Returns:
        Updated graph state with executor_output, execution_time, and engine_used
    """
    logger.info("[execute_analytics_node] Starting analytics execution")

    try:
        # Import here to avoid circular dependency
        from src.analytics_executor.agent import AnalyticsExecutorAgent
        from pathlib import Path
        import time

        # Get Phase 1 output
        phase1_output = state.get("output", {})

        if not phase1_output:
            error_msg = "No output from Phase 1 (classifier)"
            logger.error(f"[execute_analytics_node] {error_msg}")
            state["errors"].append(error_msg)
            state["executor_output"] = {
                "status": "error",
                "error": {"type": "MissingInput", "message": error_msg},
            }
            return state

        # Check if chart type is None (no chart needed)
        if phase1_output.get("chart_type") is None:
            logger.info(
                "[execute_analytics_node] No chart requested, skipping execution"
            )
            state["executor_output"] = {
                "status": "skipped",
                "message": "No chart type specified - analytics execution not needed",
                "data": [],
                "metadata": {},
                "execution": {},
                "plotly_config": {},
            }
            return state

        # Store Phase 1 output as executor input
        state["executor_input"] = phase1_output

        # Determine data path (try to get from settings or use default)
        data_path = phase1_output.get("data_source")
        if not data_path or not Path(data_path).exists():
            # Use centralized dataset path
            from src.shared_lib.core.config import get_dataset_path
            try:
                data_path = get_dataset_path()
                logger.info(
                    f"[execute_analytics_node] Using configured data path: {data_path}"
                )
            except ValueError:
                error_msg = f"Data source not found and DATASET_PATH not set: {data_path}"
                logger.error(f"[execute_analytics_node] {error_msg}")
                state["errors"].append(error_msg)
                state["executor_output"] = {
                    "status": "error",
                    "error": {"type": "DataSourceNotFound", "message": error_msg},
                }
                return state

        # Initialize executor agent
        logger.info(
            f"[execute_analytics_node] Initializing executor with data_path: {data_path}"
        )
        executor = AnalyticsExecutorAgent(default_data_path=data_path)

        # Execute analytics from Phase 1 output
        start_time = time.perf_counter()
        executor_output = executor.execute(phase1_output, data_path=data_path)
        execution_time = time.perf_counter() - start_time

        # Update state with results
        state["executor_output"] = executor_output
        state["execution_time"] = execution_time
        state["engine_used"] = executor_output.get("execution", {}).get(
            "engine", "Unknown"
        )

        logger.info(
            f"[execute_analytics_node] Analytics execution completed: "
            f"status={executor_output.get('status')}, "
            f"engine={state['engine_used']}, "
            f"time={execution_time:.3f}s"
        )

        # If execution failed, add error to state
        if executor_output.get("status") == "error":
            error_info = executor_output.get("error", {})
            error_msg = f"Analytics execution error: {error_info.get('message', 'Unknown error')}"
            state["errors"].append(error_msg)
            logger.error(f"[execute_analytics_node] {error_msg}")

    except Exception as e:
        error_msg = f"Analytics execution failed: {str(e)}"
        logger.error(f"[execute_analytics_node] {error_msg}", exc_info=True)
        state["errors"].append(error_msg)

        state["executor_output"] = {
            "status": "error",
            "error": {"type": "ExecutionException", "message": str(e)},
            "data": [],
            "metadata": {},
            "execution": {},
            "plotly_config": {},
        }

    return state


# ============================================================================
# FASE 1: SEMANTIC-FIRST ARCHITECTURE NODES
# ============================================================================


def extract_semantic_anchor_node(state: GraphState) -> GraphState:
    """
    FASE 1 - NODE 1: Extract semantic anchor from query using LLM.

    This is the FIRST LAYER of the semantic-first architecture.
    It MUST execute BEFORE any heuristic or regex processing.

    Args:
        state: Current graph state

    Returns:
        Updated state with semantic_anchor field populated

    Workflow Position:
        ENTRY_POINT -> extract_semantic_anchor -> validate_semantic -> map_semantic -> ...
    """
    from src.graphic_classifier.llm.semantic_anchor import (
        SemanticAnchorExtractor,
        SemanticAnalysisError,
    )
    from dataclasses import asdict

    logger.info(
        "[extract_semantic_anchor_node] FASE 1.1 - Extracting semantic anchor (FIRST LAYER)"
    )

    query = state.get("query", "")

    # Initialize extractor
    extractor = SemanticAnchorExtractor()

    try:
        # Extract semantic anchor (LLM call)
        anchor = extractor.extract(query)

        # Convert to dict for state storage
        anchor_dict = asdict(anchor)

        # Store in state
        state["semantic_anchor"] = anchor_dict

        logger.info(
            f"[extract_semantic_anchor_node] Semantic anchor extracted: "
            f"goal={anchor.semantic_goal}, axis={anchor.comparison_axis}, "
            f"polarity={anchor.polarity}, confidence={anchor.confidence:.2f}"
        )

    except SemanticAnalysisError as e:
        logger.error(f"[extract_semantic_anchor_node] Semantic extraction failed: {e}")
        state["semantic_anchor"] = None
        state["errors"].append(f"Semantic analysis failed: {str(e)}")

    except Exception as e:
        logger.error(f"[extract_semantic_anchor_node] Unexpected error: {e}")
        state["semantic_anchor"] = None
        state["errors"].append(f"Unexpected error in semantic extraction: {str(e)}")

    finally:
        # CRITICAL: Capture tokens AFTER try-except to ensure persistence even on error
        # Token tracking (LLM call inside graphic_classifier)
        tokens = getattr(extractor, "last_token_usage", None)
        if isinstance(tokens, dict) and int(tokens.get("total_tokens", 0) or 0) > 0:
            if "agent_tokens" not in state or not isinstance(
                state.get("agent_tokens"), dict
            ):
                state["agent_tokens"] = {}
            current = state["agent_tokens"].get(
                "graphic_classifier",
                {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "total_tokens": 0,
                    "llm_calls": 0,
                    "model_name": "unknown",
                },
            )
            if not isinstance(current, dict):
                current = {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "total_tokens": 0,
                    "llm_calls": 0,
                    "model_name": "unknown",
                }
            current["input_tokens"] = int(current.get("input_tokens", 0) or 0) + int(
                tokens.get("input_tokens", 0) or 0
            )
            current["output_tokens"] = int(current.get("output_tokens", 0) or 0) + int(
                tokens.get("output_tokens", 0) or 0
            )
            current["total_tokens"] = int(current.get("total_tokens", 0) or 0) + int(
                tokens.get("total_tokens", 0) or 0
            )
            current["llm_calls"] = int(current.get("llm_calls", 0) or 0) + 1

            # Track models used (similar to TokenAccumulator logic)
            new_model = tokens.get("model_name", "unknown")
            if new_model != "unknown":
                if current.get("model_name") == "unknown":
                    current["model_name"] = new_model
                elif current.get("model_name") != new_model:
                    # Multiple models - track in list
                    if "models_used" not in current:
                        # First time we see a different model
                        current["models_used"] = [current["model_name"], new_model]
                        current["model_name"] = "multiple"
                    elif new_model not in current["models_used"]:
                        current["models_used"].append(new_model)

            state["agent_tokens"]["graphic_classifier"] = current
            logger.debug(
                f"[extract_semantic_anchor_node] Tokens captured: "
                f"input={tokens['input_tokens']}, "
                f"output={tokens['output_tokens']}, "
                f"total={tokens['total_tokens']}, "
                f"model={tokens.get('model_name', 'unknown')}"
            )

    return state


def validate_semantic_anchor_node(state: GraphState) -> GraphState:
    """
    FASE 1 - NODE 2: Validate semantic anchor against query keywords.

    This is the SECOND LAYER of the semantic-first architecture.
    It validates that the LLM's semantic classification is consistent
    with explicit keywords in the query.

    Args:
        state: Current graph state (must have semantic_anchor)

    Returns:
        Updated state with semantic_validation field populated

    Workflow Position:
        extract_semantic_anchor -> validate_semantic -> map_semantic -> ...
    """
    from src.graphic_classifier.validators.semantic_validator import SemanticValidator
    from src.graphic_classifier.llm.semantic_anchor import SemanticAnchor
    from dataclasses import asdict

    logger.info("[validate_semantic_anchor_node] FASE 1.2 - Validating semantic anchor")

    query = state.get("query", "")
    anchor_dict = state.get("semantic_anchor")

    # Skip if no anchor was extracted
    if anchor_dict is None:
        logger.warning(
            "[validate_semantic_anchor_node] No semantic anchor to validate, skipping"
        )
        state["semantic_validation"] = {
            "is_valid": False,
            "warnings": ["No semantic anchor extracted"],
            "failed_checks": [],
            "passed_checks": [],
        }
        return state

    try:
        # Reconstruct SemanticAnchor from dict
        anchor = SemanticAnchor(**anchor_dict)

        # Initialize validator
        validator = SemanticValidator()

        # Validate
        result = validator.validate(anchor, query)

        # Store validation result
        state["semantic_validation"] = asdict(result)

        if result.is_valid:
            logger.info("[validate_semantic_anchor_node] Validation PASSED")
        else:
            logger.warning(
                f"[validate_semantic_anchor_node] Validation FAILED with "
                f"{len(result.failed_checks)} failed checks"
            )
            for warning in result.warnings:
                logger.warning(f"[validate_semantic_anchor_node] {warning}")

    except Exception as e:
        logger.error(f"[validate_semantic_anchor_node] Validation error: {e}")
        state["semantic_validation"] = {
            "is_valid": False,
            "warnings": [f"Validation error: {str(e)}"],
            "failed_checks": [],
            "passed_checks": [],
        }
        state["errors"].append(f"Semantic validation error: {str(e)}")

    return state


def map_semantic_to_chart_node(state: GraphState) -> GraphState:
    """
    FASE 1 - NODE 3: Map semantic anchor to chart family using deterministic rules.

    This is the THIRD LAYER of the semantic-first architecture.
    It maps semantic goals to chart families using HARD INVARIANTS.

    Args:
        state: Current graph state (must have semantic_anchor)

    Returns:
        Updated state with semantic_mapping field and preliminary chart_type

    Workflow Position:
        validate_semantic -> map_semantic -> [legacy heuristics] -> ...
    """
    from src.graphic_classifier.mappers.semantic_mapper import (
        SemanticMapper,
        SemanticMappingError,
    )
    from src.graphic_classifier.llm.semantic_anchor import SemanticAnchor
    from dataclasses import asdict

    logger.info(
        "[map_semantic_to_chart_node] FASE 1.3 - Mapping semantic anchor to chart family"
    )

    anchor_dict = state.get("semantic_anchor")

    # Skip if no anchor was extracted
    if anchor_dict is None:
        logger.warning(
            "[map_semantic_to_chart_node] No semantic anchor to map, skipping"
        )
        state["semantic_mapping"] = None
        return state

    try:
        # Reconstruct SemanticAnchor from dict
        anchor = SemanticAnchor(**anchor_dict)

        # Initialize mapper
        mapper = SemanticMapper()

        # Map to chart family
        mapping = mapper.map(anchor)

        # Store mapping result
        state["semantic_mapping"] = asdict(mapping)

        # CRITICAL: Set preliminary chart_type from semantic mapping
        # This becomes the ANCHOR that heuristics CANNOT contradict
        if mapping.chart_family:
            state["chart_type"] = mapping.chart_family
            logger.info(
                f"[map_semantic_to_chart_node] Semantic chart_type set: "
                f"{mapping.chart_family} (ANCHOR - cannot be contradicted)"
            )

        # Also set sort order from polarity (INVARIANT)
        if mapping.sort_order:
            # Store in parsed_entities for downstream use
            if "parsed_entities" not in state:
                state["parsed_entities"] = {}
            state["parsed_entities"]["sort_order"] = mapping.sort_order
            logger.info(
                f"[map_semantic_to_chart_node] Sort order set from polarity: "
                f"{mapping.sort_order}"
            )

        # Also set sort_by from semantic goal (CRITICAL for variation)
        if mapping.sort_by:
            if "parsed_entities" not in state:
                state["parsed_entities"] = {}
            state["parsed_entities"]["sort_by"] = mapping.sort_by
            logger.info(
                f"[map_semantic_to_chart_node] Sort by set from semantic goal: "
                f"{mapping.sort_by}"
            )

        logger.info(f"[map_semantic_to_chart_node] Mapping: {mapping.reasoning}")

    except SemanticMappingError as e:
        logger.error(f"[map_semantic_to_chart_node] Mapping failed: {e}")
        state["semantic_mapping"] = None
        state["errors"].append(f"Semantic mapping failed: {str(e)}")

    except Exception as e:
        logger.error(f"[map_semantic_to_chart_node] Unexpected error: {e}")
        state["semantic_mapping"] = None
        state["errors"].append(f"Unexpected error in semantic mapping: {str(e)}")

    return state


def initialize_state(
    query: str,
    data_source: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> GraphState:
    """
    Create an initial GraphState for a new query.

    Args:
        query: User's natural language query
        data_source: Optional path to data source
        context: Optional context from upstream agents (e.g., filter_classifier)

    Returns:
        Initialized GraphState with default values
    """
    # Extract filter_final from context if provided
    filter_final = {}
    if context and isinstance(context, dict):
        filter_final = context.get("filter_final", {})

    return {
        "query": query,
        "parsed_entities": {},
        "detected_keywords": [],
        "intent": "",
        "chart_type": None,
        "confidence": 0.0,
        "columns_mentioned": [],
        "mapped_columns": {},
        "data_source": data_source,
        "available_columns": None,
        "output": {},
        "errors": [],
        # Phase 5+: filter_classifier is the single source of truth for filters
        "filter_final": filter_final,
        # Phase 2 fields
        "executor_input": None,
        "executor_output": None,
        "execution_time": None,
        "engine_used": None,
    }


def attempt_fallback_node(state: GraphState) -> GraphState:
    """
    FASE 6: Fallback Node - Intelligent Degradation and Routing

    This node executes when visualization generation fails or produces null.
    It attempts to salvage the response through:
    1. Chart type degradation (e.g., line -> bar)
    2. Explanatory message generation
    3. Routing to non_graph_executor if no visualization possible

    Critical Invariante I4 Enforcement:
    - Every null chart MUST have an explanation
    - No blank screens for users

    Args:
        state: Current graph state

    Returns:
        Updated state with:
        - fallback_chart_type (if degradation succeeded)
        - redirect_to (if routing to text agent)
        - message (explanation for user)
    """
    logger.info("[attempt_fallback_node] Initiating fallback logic")

    from src.graphic_classifier.fallback.fallback_manager import FallbackManager
    from src.graphic_classifier.fallback.message_generator import NullMessageGenerator

    chart_type = state.get("chart_type")
    output = state.get("output", {})
    executor_output = state.get("executor_output", {})

    # Determine failure reason
    failure_reason = "generic_error"
    technical_detail = ""

    # Check if output indicates null chart
    if chart_type is None or chart_type == "null":
        failure_reason = "no_chart_type"
        technical_detail = "Chart type classification returned null"

    # Check if executor returned empty dataset
    elif executor_output:
        summary = executor_output.get("summary_table", {})
        total_rows = summary.get("total_rows", 0)

        if total_rows == 0:
            failure_reason = "no_data_returned"
            technical_detail = "Executor returned empty dataset"

        # Check for insufficient periods (temporal charts)
        if chart_type == "line_composed":
            # Would need to check actual data structure
            # For now, use a heuristic based on row count
            if total_rows < 2:
                failure_reason = "insufficient_periods"
                technical_detail = f"Only {total_rows} period(s) available, need 2+ for temporal evolution"

    # Build chart_spec for fallback manager
    chart_spec = {
        "chart_family": chart_type,
        "intent": state.get("intent", ""),
        "query": state.get("query", ""),
        "dimensions": output.get("dimensions", []),
        "metrics": output.get("metrics", []),
        "filters": state.get("filter_final", {}),
    }

    # Dataset info for message generation
    dataset_info = {}
    if executor_output:
        summary = executor_output.get("summary_table", {})
        dataset_info = {
            "total_rows": summary.get("total_rows", 0),
            "unique_periods": summary.get("unique_values", {}).get("Mes", 0)
            if summary.get("unique_values")
            else 0,
        }

    # Attempt fallback using FallbackManager
    fallback_manager = FallbackManager()
    fallback_result = fallback_manager.attempt_fallback(
        current_chart_type=chart_type or "null",
        failure_reason=failure_reason,
        chart_spec=chart_spec,
        dataset=dataset_info if dataset_info else None,
    )

    logger.info(f"[attempt_fallback_node] Fallback result: {fallback_result}")

    # Store fallback information in state
    state["fallback_result"] = fallback_result

    # Check if we should retry with degraded chart type
    if fallback_result.get("should_retry") and fallback_result.get(
        "fallback_chart_type"
    ):
        logger.info(
            f"[attempt_fallback_node] Degrading from {chart_type} to {fallback_result['fallback_chart_type']}"
        )

        # Update chart type for retry
        state["chart_type"] = fallback_result["fallback_chart_type"]
        state["fallback_triggered"] = True
        state["fallback_message"] = fallback_result.get("user_message", "")

        # Re-generate output with new chart type
        # This will be picked up by the conditional edge

    # Check if we should route to text agent
    elif fallback_result.get("should_route_to_text"):
        logger.warning(
            f"[attempt_fallback_node] No viable visualization - routing to non_graph_executor"
        )

        # Generate explanatory message
        message_generator = NullMessageGenerator()
        redirect_payload = message_generator.generate_redirect_payload(
            failure_category=failure_reason,
            chart_spec=chart_spec,
            technical_detail=technical_detail,
            dataset_info=dataset_info if dataset_info else None,
        )

        # Update state to trigger routing
        state["chart_type"] = None
        state["redirect_to"] = "non_graph_executor"
        state["redirect_payload"] = redirect_payload
        state["non_graph_output"] = redirect_payload  # For compatibility

        # Update output with message
        state["output"] = {
            "chart_type": None,
            "chart_family": "null",
            "message": redirect_payload["message"],
            "redirect_to": "non_graph_executor",
            "technical_detail": redirect_payload["message"]["technical_detail"],
        }

        logger.info(
            f"[attempt_fallback_node] Redirect message: {redirect_payload['message']['title']}"
        )

    return state
