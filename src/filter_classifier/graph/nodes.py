"""
LangGraph workflow nodes for filter_classifier.

This module implements all node functions that process the FilterGraphState
as it flows through the filter classification workflow.
"""

import logging
import time
from typing import Dict, Any, List

from src.filter_classifier.graph.state import FilterGraphState
from src.filter_classifier.tools.filter_parser import FilterParser
from src.filter_classifier.tools.filter_manager import FilterManager
from src.filter_classifier.tools.filter_validator import FilterValidator
from src.filter_classifier.utils.filter_formatter import FilterFormatter
from src.filter_classifier.core.settings import (
    ALIAS_PATH,
    DATASET_PATH,
)
from src.graphic_classifier.tools.alias_mapper import AliasMapper

logger = logging.getLogger(__name__)

# =============================================================================
# GLOBAL SINGLETONS (Thread-Safe, Stateless)
# =============================================================================
# Esses singletons são utilizados apenas para caches de leitura única ou
# comportamento determinístico sem estado mutável compartilhado.
# - _filter_parser: reusa AliasMapper e apenas interpreta texto
# - _filter_validator: leitura do dataset e validações puramente funcionais
# - _filter_formatter: monta o JSON final sem manter estado
# - _alias_mapper: carrega aliases em memória, leitura read-only
# - _temporal_resolver: resolve referências temporais relativas (último mês, etc.)
# =============================================================================
_filter_parser: FilterParser = None
_filter_validator: FilterValidator = None
_filter_formatter: FilterFormatter = None
_alias_mapper: AliasMapper = None
_temporal_resolver: "RelativeTemporalResolver" = None


def _get_filter_parser() -> FilterParser:
    """Get or initialize the global FilterParser instance."""
    global _filter_parser, _alias_mapper
    if _filter_parser is None:
        logger.info("[FilterNodes] Initializing FilterParser")
        if _alias_mapper is None:
            _alias_mapper = AliasMapper(alias_path=ALIAS_PATH)
        _filter_parser = FilterParser(
            alias_mapper=_alias_mapper, dataset_path=DATASET_PATH
        )
    return _filter_parser


def _get_filter_validator() -> FilterValidator:
    """Get or initialize the global FilterValidator instance."""
    global _filter_validator, _alias_mapper
    if _filter_validator is None:
        logger.info("[FilterNodes] Initializing FilterValidator")
        if _alias_mapper is None:
            _alias_mapper = AliasMapper(alias_path=ALIAS_PATH)
        _filter_validator = FilterValidator(
            alias_mapper=_alias_mapper, dataset_path=DATASET_PATH
        )
    return _filter_validator


def _get_filter_formatter() -> FilterFormatter:
    """Get or initialize the global FilterFormatter instance."""
    global _filter_formatter
    if _filter_formatter is None:
        logger.info("[FilterNodes] Initializing FilterFormatter")
        _filter_formatter = FilterFormatter()
    return _filter_formatter


def _get_temporal_resolver() -> "RelativeTemporalResolver":
    """Get or initialize the global RelativeTemporalResolver instance."""
    global _temporal_resolver, _alias_mapper
    if _temporal_resolver is None:
        logger.info("[FilterNodes] Initializing RelativeTemporalResolver")
        from src.filter_classifier.utils.relative_temporal_resolver import (
            RelativeTemporalResolver,
        )

        if _alias_mapper is None:
            _alias_mapper = AliasMapper(alias_path=ALIAS_PATH)
        _temporal_resolver = RelativeTemporalResolver(
            dataset_path=DATASET_PATH, alias_mapper=_alias_mapper
        )
    return _temporal_resolver


# ============================================================================
# NODE FUNCTIONS
# ============================================================================


def parse_filter_query(state: FilterGraphState) -> FilterGraphState:
    """
    Node 1: Parse user query to extract filters and identify CRUD operations.

    Uses FilterParser (LLM-based) to:
    - Detect filters mentioned in the query
    - Extract column names, values, and operators
    - Identify CRUD operations (ADICIONAR, ALTERAR, REMOVER, MANTER)

    NOVO (Fase Temporal): Primeiro resolve referências temporais relativas
    antes do parsing LLM.

    Args:
        state: Current filter graph state

    Returns:
        Updated state with detected_filter_columns, filter_operations,
        filter_confidence, and parsed_entities populated
    """
    query = state.get("query", "")
    current_filters = state.get("current_filters", {})

    logger.info(f"[parse_filter_query] Processing query: {query}")

    try:
        # [NOVO] Etapa 0: Resolver referências temporais relativas ANTES do LLM
        resolver = _get_temporal_resolver()
        resolution_result = resolver.resolve_query(query)

        if resolution_result.has_relative_references:
            # Substituir query por versão resolvida
            resolved_query = resolution_result.resolved_query
            logger.info(
                f"[parse_filter_query] Resolved temporal references: "
                f"'{query}' -> '{resolved_query}'"
            )
            logger.info(
                f"[parse_filter_query] Resolved filters (Date range): {resolution_result.resolved_filters}"
            )

            # Adicionar todos os filtros temporais resolvidos ao current_filters.
            # O RelativeTemporalResolver agora usa _get_discrete_filters() que
            # retorna colunas reais do dataset (ex: ano, mes, ano_mes) com base
            # no temporal_mapping do alias.yaml.
            for col, val in resolution_result.resolved_filters.items():
                current_filters[col] = val
                logger.info(
                    f"[parse_filter_query] Added resolved temporal filter: {col}={val}"
                )

            # Usar query resolvida para parsing LLM
            query = resolved_query

            # Armazenar resolução no estado para referência posterior
            state["temporal_resolution"] = {
                "detected_references": resolution_result.detected_references,
                "resolved_filters": resolution_result.resolved_filters,
                "metadata": resolution_result.metadata,
            }

        # [NOVO] Etapa 0.5: Pre-match values against ValueCatalog (before LLM)
        from src.filter_classifier.tools.pre_match_engine import PreMatchEngine

        pre_match_engine = PreMatchEngine()
        pre_match_candidates = pre_match_engine.find_candidates(query)
        candidates_prompt = pre_match_engine.format_candidates_for_prompt(
            pre_match_candidates
        )

        if pre_match_candidates:
            logger.info(
                f"[parse_filter_query] PreMatchEngine found {len(pre_match_candidates)} "
                f"candidates: {[(c.column, c.value, c.score) for c in pre_match_candidates]}"
            )
        else:
            logger.info("[parse_filter_query] PreMatchEngine found no candidates")

        # Import token tracker
        from src.shared_lib.utils.token_tracker import extract_token_usage

        # Get FilterParser instance
        parser = _get_filter_parser()

        # [EXISTENTE] Etapa 1: Parse query usando LLM (now with pre-match candidates)
        parse_result = parser.parse_query(
            query, current_filters, pre_match_candidates=candidates_prompt
        )

        # Capture tokens if LLM response is available
        if "_llm_response" in parse_result:
            tokens = extract_token_usage(parse_result["_llm_response"], parser.llm)
            if "agent_tokens" not in state:
                state["agent_tokens"] = {}
            state["agent_tokens"]["filter_classifier"] = tokens
            logger.info(
                f"[parse_filter_query] Tokens captured: "
                f"input={tokens['input_tokens']}, "
                f"output={tokens['output_tokens']}, "
                f"total={tokens['total_tokens']}, "
                f"model={tokens.get('model_name', 'unknown')}"
            )
        # If no _llm_response is present, do not register tokens for this agent
        # (keeps logs clean: only agents that called an LLM should appear)

        # Extract detected filters
        detected_filters = parse_result.get("detected_filters", {})

        # [FALLBACK] Inject high-confidence PreMatch candidates that LLM missed.
        # Per filter_enhanced_plan.md: PreMatchEngine is deterministic and reliable;
        # the LLM is confirmatory. If PreMatch found something strong, trust it.
        PREMATCH_FALLBACK_SCORE = 110
        if pre_match_candidates:
            for candidate in pre_match_candidates:
                col = candidate.column
                if col not in detected_filters and candidate.score >= PREMATCH_FALLBACK_SCORE:
                    detected_filters[col] = {
                        "column": col,
                        "value": candidate.value,
                        "operator": "=",
                        "confidence": min(candidate.score / 135.0, 1.0),
                        "source": "pre_match_fallback",
                    }
                    logger.info(
                        f"[parse_filter_query] Fallback: injected {col}={candidate.value} "
                        f"(score={candidate.score}, LLM missed)"
                    )

        # Filter out invalid filters (None values or empty)
        valid_filters = {}
        invalid_filters = []
        for col, filter_spec in detected_filters.items():
            value = filter_spec.get("value")
            # Skip filters with None, empty string, or empty list values
            if value is None or value == "" or value == []:
                invalid_filters.append(col)
                logger.warning(
                    f"[parse_filter_query] Skipping invalid filter: {col}={value}"
                )
            else:
                valid_filters[col] = filter_spec

        detected_columns = list(valid_filters.keys())

        # Extract CRUD operations
        crud_operations = parse_result.get(
            "crud_operations",
            {"ADICIONAR": [], "ALTERAR": [], "REMOVER": [], "MANTER": []},
        )

        # Extract confidence
        confidence = parse_result.get("confidence", 0.0)

        logger.info(
            f"[parse_filter_query] Detected {len(detected_columns)} valid filters "
            f"(skipped {len(invalid_filters)} invalid) (confidence: {confidence:.2f})"
        )
        logger.debug(f"[parse_filter_query] CRUD operations: {crud_operations}")

        # Update state
        return {
            **state,
            "detected_filter_columns": detected_columns,
            "filter_confidence": confidence,
            "parsed_entities": valid_filters,  # Store only valid filter specs
            "pre_match_candidates": [c.to_dict() for c in pre_match_candidates] if pre_match_candidates else [],
            "filter_operations": {
                "ADICIONAR": detected_columns if "ADICIONAR" in crud_operations else [],
                "ALTERAR": crud_operations.get("ALTERAR", []),
                "REMOVER": crud_operations.get("REMOVER", []),
                "MANTER": crud_operations.get("MANTER", []),
            },
            "agent_tokens": state.get(
                "agent_tokens", {}
            ),  # CRITICAL: Preserve token tracking
        }

    except Exception as e:
        logger.error(f"[parse_filter_query] Error: {str(e)}")
        return {
            **state,
            "detected_filter_columns": [],
            "filter_confidence": 0.0,
            "errors": state.get("errors", []) + [f"Parse error: {str(e)}"],
        }


def validate_detected_values(state: FilterGraphState) -> FilterGraphState:
    """
    Node: Validate detected filter values against ValueCatalog.

    Uses positive logic: only accepts values that exist in the pre-computed
    catalog. This replaces the old GENERIC_TERMS_BLACKLIST approach.

    Args:
        state: Current filter graph state

    Returns:
        Updated state with validated parsed_entities
    """
    parsed_entities = state.get("parsed_entities", {})

    if not parsed_entities:
        logger.debug("[validate_detected_values] No filters to validate")
        return state

    logger.info(
        f"[validate_detected_values] Validating {len(parsed_entities)} detected filters"
    )

    try:
        validator = _get_filter_validator()
        validated_entities = {}
        warnings = []

        for column, filter_spec in parsed_entities.items():
            value = filter_spec.get("value")

            # Skip validation for complex operators or None values
            if value is None or isinstance(value, dict):
                validated_entities[column] = filter_spec
                continue

            # Validate single value or list of values
            values_to_check = value if isinstance(value, list) else [value]
            valid_values = []
            has_any_valid = False

            for val in values_to_check:
                is_valid = validator.validate_value_exists(column, val)

                if is_valid:
                    valid_values.append(val)
                    has_any_valid = True
                else:
                    # Try fuzzy suggestions (typo tolerance)
                    suggestions = validator.suggest_valid_values(
                        column, str(val), max_suggestions=1, score_cutoff=70.0
                    )
                    if suggestions:
                        # Auto-correct to best suggestion
                        corrected = suggestions[0]
                        valid_values.append(corrected)
                        has_any_valid = True
                        warnings.append(
                            f"Valor '{val}' corrigido para '{corrected}' em '{column}'"
                        )
                        logger.info(
                            f"[validate_detected_values] Auto-corrected '{val}' -> "
                            f"'{corrected}' in column '{column}'"
                        )
                    else:
                        warnings.append(
                            f"Valor '{val}' nao encontrado em '{column}'"
                        )
                        logger.warning(
                            f"[validate_detected_values] Rejected '{val}' "
                            f"in column '{column}' (not in ValueCatalog)"
                        )

            if has_any_valid:
                # Update filter with validated/corrected values
                if isinstance(value, list):
                    filter_spec["value"] = valid_values
                elif valid_values:
                    filter_spec["value"] = valid_values[0]
                validated_entities[column] = filter_spec
                logger.debug(
                    f"[validate_detected_values] ✓ {column} validated"
                )
            else:
                logger.warning(
                    f"[validate_detected_values] ✗ Removing {column} "
                    f"(no valid values found)"
                )

        logger.info(
            f"[validate_detected_values] Validation complete: "
            f"{len(validated_entities)}/{len(parsed_entities)} filters valid, "
            f"{len(warnings)} warnings"
        )

        return {
            **state,
            "parsed_entities": validated_entities,
            "detected_filter_columns": list(validated_entities.keys()),
            "validation_warnings": warnings,
        }

    except Exception as e:
        logger.error(f"[validate_detected_values] Error during validation: {str(e)}")
        return {
            **state,
            "validation_warnings": [f"Validation error: {str(e)}"],
        }


def expand_temporal_periods_node(state: FilterGraphState) -> FilterGraphState:
    """
    Node (NEW - FASE 1.1): Expand temporal filters to cover all mentioned periods.

    This node implements FASE 1 - Etapas 1.1.1 e 1.1.2 from the planning:
    - Parse multiple temporal periods from query
    - Expand date filters to cover full range

    Problem addressed:
    - Query: "maio de 2016 para junho de 2016"
    - Current filter: ["2016-06-01", "2016-06-30"]  # Only June
    - Expected filter: ["2016-05-01", "2016-06-30"]  # May AND June

    This node runs AFTER parse_filter_query and validate_detected_values
    to ensure date filters cover all temporal periods mentioned in comparisons.

    Reference: planning_graph_classifier_diagnosis.md - FASE 1, Etapas 1.1.1-1.1.3

    Args:
        state: Current filter graph state

    Returns:
        Updated state with expanded temporal filters in parsed_entities
    """
    from src.filter_classifier.utils.temporal_period_expander import (
        TemporalPeriodExpander,
        expand_temporal_filters,
    )

    logger.critical(
        f"\n{'=' * 80}\n🔍 [expand_temporal_periods_node] NODE EXECUTING\n{'=' * 80}"
    )

    parsed_entities = state.get("parsed_entities", {})
    query = state.get("query", "")

    # FASE 1 - CORREÇÃO CRÍTICA: Não fazer early return se parsed_entities vazio
    # O expander regex é um fallback essencial quando o LLM falha na detecção
    if not query:
        logger.debug(
            "[expand_temporal_periods_node] No query provided, skipping expansion"
        )
        return state

    logger.info("[expand_temporal_periods_node] Checking for temporal period expansion")

    try:
        expander = TemporalPeriodExpander()

        # Check if there's a Data filter
        if "Data" not in parsed_entities:
            # BUGFIX: LLM may not detect filters, but we can still create them
            # if we detect temporal comparison patterns in the query
            logger.info(
                "[expand_temporal_periods_node] No Data filter from LLM, checking query patterns"
            )

            # Try to create filter from query patterns
            expanded_value = expander.expand_date_filter(None, query)

            if expanded_value:
                logger.info(
                    f"[expand_temporal_periods_node] Created Data filter from query: {expanded_value}"
                )
                # Create new Data filter
                parsed_entities["Data"] = {
                    "column": "Data",
                    "operator": "between",
                    "value": expanded_value,
                    "confidence": 0.95,
                    "source": "temporal_period_expander",
                }

                # CRITICAL: Update detected_filter_columns so validation knows about new filter
                detected_filter_columns = state.get("detected_filter_columns", [])
                if "Data" not in detected_filter_columns:
                    detected_filter_columns.append("Data")

                # FASE 1 - CORREÇÃO: Atualizar filter_operations para propagar o novo filtro
                filter_operations = state.get(
                    "filter_operations",
                    {"ADICIONAR": [], "ALTERAR": [], "REMOVER": [], "MANTER": []},
                )

                # Adicionar "Data" à operação ADICIONAR se não estiver lá
                if "Data" not in filter_operations.get("ADICIONAR", []):
                    if isinstance(filter_operations["ADICIONAR"], list):
                        filter_operations["ADICIONAR"].append("Data")
                    else:
                        # Se já é dict, adicionar o valor diretamente
                        filter_operations["ADICIONAR"]["Data"] = expanded_value

                logger.info(
                    f"[expand_temporal_periods_node] Updated filter_operations: ADICIONAR={filter_operations['ADICIONAR']}"
                )

                state["parsed_entities"] = parsed_entities
                state["detected_filter_columns"] = detected_filter_columns
                state["filter_operations"] = filter_operations
                logger.info(
                    f"[expand_temporal_periods_node] Updated detected_filter_columns: {detected_filter_columns}"
                )
                return state
            else:
                logger.debug(
                    "[expand_temporal_periods_node] No temporal comparison detected in query"
                )
                return state

        # Get current Data filter value
        data_filter_spec = parsed_entities["Data"]
        current_value = data_filter_spec.get("value")

        # FASE 4 - CORREÇÃO: Log de diagnóstico
        logger.info(
            f"[expand_temporal_periods_node] Current Data filter value from LLM: {current_value}"
        )

        # Attempt expansion (mesmo que LLM tenha retornado apenas 1 período)
        expanded_value = expander.expand_date_filter(current_value, query)

        # FASE 4 - CORREÇÃO: Log resultado da expansão
        logger.info(
            f"[expand_temporal_periods_node] Expansion result: {expanded_value} "
            f"(original: {current_value})"
        )

        if expanded_value and expanded_value != current_value:
            # Expansion was applied
            logger.info(
                f"[expand_temporal_periods_node] ✅ Expanded Data filter: "
                f"{current_value} -> {expanded_value}"
            )

            # Update parsed_entities with expanded filter
            parsed_entities["Data"]["value"] = expanded_value

            # Validate coverage
            validation_result = expander.validate_period_coverage(expanded_value, query)

            if not validation_result["is_valid"]:
                logger.warning(
                    f"[expand_temporal_periods_node] Filter expansion incomplete: "
                    f"covered={validation_result['covered_periods']}, "
                    f"missing={validation_result['missing_periods']}, "
                    f"confidence={validation_result['confidence']:.2f}"
                )
                # Store validation info for debugging
                state["temporal_expansion_validation"] = validation_result
            else:
                logger.info(
                    f"[expand_temporal_periods_node] Filter expansion successful: "
                    f"all {len(validation_result['covered_periods'])} periods covered"
                )
                state["temporal_expansion_validation"] = validation_result

            # Update state
            return {
                **state,
                "parsed_entities": parsed_entities,
                "temporal_filter_expanded": True,
            }
        else:
            logger.debug(
                "[expand_temporal_periods_node] No expansion needed or possible"
            )
            return {
                **state,
                "temporal_filter_expanded": False,
            }

    except Exception as e:
        import traceback

        logger.critical(
            f"[expand_temporal_periods_node] EXCEPTION CAUGHT: {str(e)}\n{traceback.format_exc()}"
        )
        # On error, pass through without expansion
        return {
            **state,
            "temporal_filter_expanded": False,
            "errors": state.get("errors", []) + [f"Temporal expansion error: {str(e)}"],
        }


def load_filter_context(state: FilterGraphState) -> FilterGraphState:
    """
    Node 2: Load filter context from previous session.

    Loads:
    - filter_history: Historical record of filters
    - current_filters: Currently active filters from previous session

    FASE 2 Enhancement: Also performs auto-detection of filter need and starts timing.

    Args:
        state: Current filter graph state

    Returns:
        Updated state with filter_history and current_filters populated
    """
    # FASE 2: Start timing for filter_classifier
    filter_classifier_start_time = time.perf_counter()

    logger.info("[load_filter_context] Loading filter context")

    # FASE 2: Auto-detect if filter_classifier should really execute
    query = state.get("query", "")
    from src.shared_lib.utils.query_analyzer import analyze_query

    analysis = analyze_query(query)
    logger.info(
        f"[load_filter_context] Auto-detection: needs_filter={analysis.needs_filter}, "
        f"confidence={analysis.confidence:.2f}"
    )

    if not analysis.needs_filter:
        logger.info(
            "[load_filter_context] Query does not need filters (heuristic), "
            "filter_classifier will skip processing"
        )

    filter_history = state.get("filter_history") or []
    current_filters = state.get("current_filters") or {}

    # Fallback para filtros já consolidados no estado (session-only)
    if not current_filters:
        current_filters = state.get("filter_final", {}) or {}

    logger.info(
        f"[load_filter_context] Session-scoped context: "
        f"{len(filter_history)} history entries, {len(current_filters)} active filters"
    )

    return {
        **state,
        "filter_history": filter_history,
        "_filter_classifier_start_time": filter_classifier_start_time,  # FASE 2: Track start time
        "_filter_needs_detection": analysis.needs_filter,  # FASE 2: Store detection result
        "current_filters": current_filters,
        "agent_tokens": state.get(
            "agent_tokens", {}
        ),  # CRITICAL: Preserve token tracking
    }


def validate_filter_columns(state: FilterGraphState) -> FilterGraphState:
    """
    Node 3: Validate detected filter columns and values.

    Validates:
    - Column names exist in dataset (with alias resolution)
    - Column values are valid (for categorical columns)
    - Data types are appropriate

    Args:
        state: Current filter graph state

    Returns:
        Updated state with validated columns and potential errors
    """
    detected_columns = state.get("detected_filter_columns", [])
    parsed_entities = state.get("parsed_entities", {})

    logger.info(f"[validate_filter_columns] Validating {len(detected_columns)} columns")

    if not detected_columns:
        logger.debug("[validate_filter_columns] No columns to validate, skipping")
        return state

    try:
        # Get validator instance
        validator = _get_filter_validator()

        # Validate column existence (returns tuple: valid_columns, invalid_columns)
        valid_columns, invalid_columns = validator.validate_columns_exist(
            detected_columns
        )

        if invalid_columns:
            error_msg = f"Invalid columns: {', '.join(invalid_columns)}"
            logger.warning(f"[validate_filter_columns] {error_msg}")

            return {**state, "errors": state.get("errors", []) + [error_msg]}

        # Validate column values for categorical columns
        errors = []
        for col, filter_spec in parsed_entities.items():
            value = filter_spec.get("value")
            operator = filter_spec.get("operator", "=")

            # Only validate for equality operators on categorical columns
            if operator in ["=", "in"]:
                values_to_check = [value] if not isinstance(value, list) else value
                is_valid = validator.validate_column_values(col, values_to_check)

                if not is_valid:
                    errors.append(
                        f"Invalid values for {col}: {', '.join(map(str, values_to_check))}"
                    )

        if errors:
            logger.warning(f"[validate_filter_columns] Validation errors: {errors}")
            return {**state, "errors": state.get("errors", []) + errors}

        logger.info(
            "[validate_filter_columns] All columns and values validated successfully"
        )
        return state

    except Exception as e:
        logger.error(f"[validate_filter_columns] Error: {str(e)}")
        return {
            **state,
            "errors": state.get("errors", []) + [f"Validation error: {str(e)}"],
        }


def identify_filter_operations(state: FilterGraphState) -> FilterGraphState:
    """
    Node 4: Identify and refine CRUD operations.

    This node refines the CRUD operations based on validation results
    and current context. The initial CRUD classification comes from
    parse_filter_query, but this node can adjust based on errors or context.

    Args:
        state: Current filter graph state

    Returns:
        Updated state with refined filter_operations
    """
    logger.info("[identify_filter_operations] Refining CRUD operations")

    # Get current operations from state
    filter_operations = state.get(
        "filter_operations",
        {"ADICIONAR": [], "ALTERAR": [], "REMOVER": [], "MANTER": []},
    )

    errors = state.get("errors", [])
    current_filters = state.get("current_filters", {})
    detected_columns = state.get("detected_filter_columns", [])

    # If there are validation errors, move invalid operations to errors
    if errors:
        logger.warning(
            f"[identify_filter_operations] Validation errors detected, "
            f"may skip some operations"
        )
        # Keep operations as-is for now; FilterManager will handle errors

    # Ensure all current filters not mentioned are marked as MANTER
    # Handle both dict format (from persisted state) and list format (from parser)
    manter_op = filter_operations.get("MANTER", [])

    if isinstance(manter_op, dict):
        # Dict format: operations are already in {col: value} format
        for col in current_filters:
            if col not in detected_columns:
                if col not in manter_op:
                    manter_op[col] = current_filters[col]
    else:
        # List format: operations are in [col1, col2, ...] format
        for col in current_filters:
            if col not in detected_columns:
                if col not in manter_op:
                    manter_op.append(col)

    logger.debug(f"[identify_filter_operations] Final operations: {filter_operations}")

    return {
        **state,
        "filter_operations": filter_operations,
        "agent_tokens": state.get(
            "agent_tokens", {}
        ),  # CRITICAL: Preserve token tracking
    }


def apply_filter_operations(state: FilterGraphState) -> FilterGraphState:
    """
    Node 5: Apply CRUD operations to generate final filter set.

    Uses FilterManager to:
    - Apply ADICIONAR, ALTERAR, REMOVER, MANTER operations
    - Generate filter_final with consolidated filters
    - Update filter_history with timestamp

    Args:
        state: Current filter graph state

    Returns:
        Updated state with filter_final and filter_history
    """
    logger.info("[apply_filter_operations] Applying CRUD operations")

    filter_operations = state.get("filter_operations", {})
    current_filters = state.get("current_filters", {})
    parsed_entities = state.get("parsed_entities", {})
    filter_history = state.get("filter_history", [])

    try:
        # Instantiate manager per execution to avoid hidden shared state
        manager = FilterManager()

        # Convert operations format for FilterManager
        # FilterManager expects: {"ADICIONAR": {col: value}, "ALTERAR": {...}, ...}
        operations_dict = {"ADICIONAR": {}, "ALTERAR": {}, "REMOVER": {}, "MANTER": {}}

        # Check if LLM already returned dicts (new format) or lists (old format)
        adicionar_op = filter_operations.get("ADICIONAR", {})

        if isinstance(adicionar_op, dict):
            # New format: LLM returns dicts directly
            operations_dict["ADICIONAR"] = adicionar_op
            operations_dict["ALTERAR"] = filter_operations.get("ALTERAR", {})
            operations_dict["REMOVER"] = filter_operations.get("REMOVER", {})
            operations_dict["MANTER"] = filter_operations.get("MANTER", {})
            logger.debug("[apply_filter_operations] Using dict format from LLM")
        else:
            # Old format: LLM returns lists (fallback)
            logger.debug("[apply_filter_operations] Converting list format to dict")

            # Build ADICIONAR
            for col in filter_operations.get("ADICIONAR", []):
                if col in parsed_entities:
                    operations_dict["ADICIONAR"][col] = parsed_entities[col]["value"]

            # Build ALTERAR
            for col in filter_operations.get("ALTERAR", []):
                if col in parsed_entities:
                    operations_dict["ALTERAR"][col] = {
                        "from": current_filters.get(col),
                        "to": parsed_entities[col]["value"],
                    }

            # Build REMOVER
            for col in filter_operations.get("REMOVER", []):
                operations_dict["REMOVER"][col] = current_filters.get(col)

            # Build MANTER
            for col in filter_operations.get("MANTER", []):
                if col in current_filters:
                    operations_dict["MANTER"][col] = current_filters[col]

        # Apply operations
        filter_final = manager.apply_operations(current_filters, operations_dict)

        # FASE 1 - Etapa 1.1: Corrigir Formato de Filtros Temporais
        # Garantir que filtros temporais sao arrays, nao strings concatenadas
        for col, value in filter_final.items():
            if col == "Data" and isinstance(value, str) and ", " in value:
                # Converter string concatenada para array
                original_value = value
                filter_final[col] = [v.strip() for v in value.split(", ")]
                logger.info(
                    f"[apply_filter_operations] FASE 1.1: Converted Data filter from "
                    f"string to array: '{original_value}' -> {filter_final[col]}"
                )

        # Update history
        import datetime

        history_entry = {
            "timestamp": datetime.datetime.now().isoformat(),
            "query": state.get("query", ""),
            "operations": operations_dict,
            "filter_final": filter_final,
        }
        filter_history.append(history_entry)

        logger.info(
            f"[apply_filter_operations] Generated {len(filter_final)} final filters"
        )

        return {
            **state,
            "filter_final": filter_final,
            "filter_history": filter_history,
            "filter_operations": operations_dict,  # Update with formatted operations
        }

    except Exception as e:
        logger.error(f"[apply_filter_operations] Error: {str(e)}")
        return {
            **state,
            "filter_final": current_filters,  # Fallback to current filters
            "errors": state.get("errors", []) + [f"Apply error: {str(e)}"],
        }


def persist_filters(state: FilterGraphState) -> FilterGraphState:
    """
    Node 6: Persist filter state to storage.

    Saves:
    - filter_final
    - filter_history
    in session memory for use in the next query execution

    Args:
        state: Current filter graph state

    Returns:
        Unchanged state (side effect: file write)
    """
    logger.info("[persist_filters] Persisting filter state")

    filter_final = state.get("filter_final", {})
    filter_history = state.get("filter_history", [])

    logger.info(
        f"[persist_filters] Session-only isolation enabled: {len(filter_final)} filters in memory"
    )

    return {
        **state,
        # Garante que execuções subsequentes recebam o estado consolidado atual
        "current_filters": filter_final,
        "agent_tokens": state.get(
            "agent_tokens", {}
        ),  # CRITICAL: Preserve token tracking
    }


def format_filter_output(state: FilterGraphState) -> FilterGraphState:
    """
    Node 7: Format final output JSON.

    Generates structured output with:
    - CRUD operations (ADICIONAR, ALTERAR, REMOVER, MANTER)
    - filter_final
    - metadata (confidence, timestamp, errors)

    FASE 2 Enhancement: Calculates and stores filter_classifier execution time.

    Args:
        state: Current filter graph state

    Returns:
        Updated state with formatted output
    """
    logger.info("[format_filter_output] Formatting final output")

    # FASE 2: Calculate filter_classifier execution time
    start_time = state.get("_filter_classifier_start_time")
    if start_time:
        filter_execution_time = time.perf_counter() - start_time
        logger.info(
            f"[format_filter_output] filter_classifier execution time: {filter_execution_time:.4f}s"
        )
    else:
        filter_execution_time = 0.0
        logger.warning("[format_filter_output] No start time found, execution time=0.0")

    try:
        # Get formatter instance
        formatter = _get_filter_formatter()

        # Format output
        output = formatter.format_output(state)

        # CRITICAL: Keep filter_final at root level for graphic_classifier integration
        # The graphic_classifier expects state["filter_final"] not state["output"]["filter_final"]
        filter_final_value = output.get("filter_final", state.get("filter_final", {}))

        logger.info(
            f"[format_filter_output] Output formatted successfully: "
            f"{len(filter_final_value)} filters in filter_final"
        )
        logger.debug(
            f"[format_filter_output] filter_final content: {filter_final_value}"
        )

        return {
            **state,
            "output": output,
            "filter_final": filter_final_value,
            "filter_execution_time": filter_execution_time,  # FASE 2: Store execution time
        }

    except Exception as e:
        logger.error(f"[format_filter_output] Error: {str(e)}")

        # Fallback: create basic error output
        try:
            formatter = _get_filter_formatter()
            error_output = formatter.format_error_response(str(e))
        except Exception:
            # If formatter fails, create minimal error output
            from datetime import datetime

            error_output = {
                "ADICIONAR": {},
                "ALTERAR": {},
                "REMOVER": {},
                "MANTER": {},
                "filter_final": {},
                "metadata": {
                    "status": "error",
                    "error": str(e),
                    "confidence": 0.0,
                    "timestamp": datetime.now().isoformat(),
                    "columns_detected": [],
                    "errors": state.get("errors", []),
                },
            }

        return {
            **state,
            "output": error_output,
            "filter_final": error_output.get("filter_final", {}),
            "errors": state.get("errors", []) + [f"Format error: {str(e)}"],
        }
