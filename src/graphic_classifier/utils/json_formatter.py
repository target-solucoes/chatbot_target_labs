"""
JSON output formatting and validation utilities.

This module provides functions to format and validate the final JSON output
according to the optimized ChartOutput schema.
"""

import logging
import re
from pathlib import Path
from typing import Dict, Any, Optional, List, Union

from pydantic import ValidationError
from unidecode import unidecode

from src.shared_lib.models.schema import (
    ChartOutput,
    MetricSpec,
    DimensionSpec,
    SortSpec,
    VisualSpec,
    OutputSpec,
)
from src.graphic_classifier.core.settings import (
    VALID_CHART_TYPES,
    VALID_AGGREGATIONS,
    VALID_SORT_ORDERS,
    DATASET_PATH,
)
from src.graphic_classifier.tools.alias_mapper import AliasMapper
from src.graphic_classifier.tools.keyword_detector import detect_sort_order
from src.graphic_classifier.utils.dimension_filter_classifier import (
    classify_multi_value_field,
    is_date_range,
)
from src.graphic_classifier.utils.ranking_detector import extract_nested_ranking


logger = logging.getLogger(__name__)


_ALIAS_MAPPER: Optional[AliasMapper] = None

METRIC_KEYWORDS = [
    "valor",
    "venda",
    "vendas",
    "faturamento",
    "receita",
    "lucro",
    "quantidade",
    "qtd",
    "volume",
    "ticket",
    "fatura",
]

TITLE_PREFIXES = [
    "qual é ",
    "qual e ",
    "qual a ",
    "qual o ",
    "quais são ",
    "quais sao ",
    "quais os ",
    "quais as ",
    "me mostre ",
    "mostre ",
    "mostrar ",
    "exibir ",
    "exiba ",
    "gera ",
    "gerar ",
    "liste ",
    "listar ",
    "traga ",
    "forneça ",
    "forneca ",
]

DEFAULT_HISTOGRAM_BINS = 10


def _get_alias_mapper() -> Optional[AliasMapper]:
    """Get or initialize the AliasMapper instance for metric inference."""

    global _ALIAS_MAPPER

    if _ALIAS_MAPPER is None:
        try:
            _ALIAS_MAPPER = AliasMapper()
        except Exception as exc:
            logger.warning("Unable to initialize AliasMapper: %s", exc)
            _ALIAS_MAPPER = None

    return _ALIAS_MAPPER


def _prettify_label(value: Optional[str]) -> Optional[str]:
    """Convert a column or field name into a human-friendly label."""

    if not value:
        return None

    label = value.replace("_", " ").strip()
    if not label:
        return None

    return label[:1].upper() + label[1:]


def _infer_unit(column_name: Optional[str]) -> Optional[str]:
    """Infer the unit associated with a metric column name."""

    if not column_name:
        return None

    lower = column_name.lower()

    if any(
        keyword in lower
        for keyword in ["valor", "faturamento", "receita", "preço", "preco", "custo"]
    ):
        return "R$"

    if any(
        keyword in lower for keyword in ["percent", "taxa", "participa", "quota", "%"]
    ):
        return "%"

    if any(
        keyword in lower
        for keyword in ["quantidade", "qtd", "volume", "numero", "número", "contagem"]
    ):
        return "unidade"

    return None


def _deduplicate_preserve_order(values: List[str]) -> List[str]:
    """Remove duplicates while preserving ordering."""

    seen = set()
    result: List[str] = []

    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)

    return result


def _inject_compatibility_fields(data: Dict[str, Any]) -> Dict[str, Any]:
    """Add derived fields expected by legacy consumers/tests."""

    metrics = data.get("metrics")
    if isinstance(metrics, list) and metrics:
        first_metric = metrics[0]
        if isinstance(first_metric, dict):
            aggregation = first_metric.get("aggregation")
            if aggregation:
                data["aggregation"] = aggregation
    elif data.get("aggregation"):
        # Preserve previously inferred aggregation if present
        pass
    else:
        data.pop("aggregation", None)

    return data


def _normalize_metric(
    metric: Any, default_aggregation: str
) -> Optional[Dict[str, Any]]:
    """Normalize metric inputs into the MetricSpec structure."""

    if isinstance(metric, MetricSpec):
        metric_dict = metric.model_dump()
        metric_dict["aggregation"] = (
            metric_dict.get("aggregation") or default_aggregation
        )
        if not metric_dict.get("alias"):
            metric_dict["alias"] = _prettify_label(metric_dict.get("name"))
        if metric_dict.get("unit") is None:
            metric_dict["unit"] = _infer_unit(metric_dict.get("name"))
        return metric_dict

    if isinstance(metric, dict):
        name = metric.get("name")
        if not name:
            return None
        aggregation = metric.get("aggregation") or default_aggregation
        alias = metric.get("alias") or _prettify_label(name)
        unit = (
            metric.get("unit") if metric.get("unit") is not None else _infer_unit(name)
        )
        return {"name": name, "aggregation": aggregation, "alias": alias, "unit": unit}

    if isinstance(metric, str):
        name = metric
        return {
            "name": name,
            "aggregation": default_aggregation,
            "alias": _prettify_label(name),
            "unit": _infer_unit(name),
        }

    return None


def _normalize_dimension(dimension: Any) -> Optional[Dict[str, Any]]:
    """Normalize dimension inputs into the DimensionSpec structure."""

    if isinstance(dimension, DimensionSpec):
        return dimension.model_dump()

    if isinstance(dimension, dict):
        name = dimension.get("name")
        if not name:
            return None
        alias = dimension.get("alias") or _prettify_label(name)
        return {"name": name, "alias": alias}

    if isinstance(dimension, str):
        return {"name": dimension, "alias": _prettify_label(dimension)}

    return None


def _slugify_placeholder(key: Any) -> str:
    """Create a safe placeholder token from a filter key."""

    if key is None:
        return ""

    normalized = unidecode(str(key)).lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = normalized.strip("_")
    return normalized or str(key)


def _generate_summary_template(
    intent: Optional[str],
    top_n: Optional[int],
    metrics: List[Dict[str, Any]],
    dimensions: List[Dict[str, Any]],
    filters: Dict[str, Any],
) -> Optional[str]:
    """Build a summary template string for downstream agents."""

    if not metrics and not dimensions:
        return None

    metric_alias = (metrics[0].get("alias") if metrics else None) or (
        metrics[0].get("name") if metrics else "métrica"
    )
    dimension_alias = (dimensions[0].get("alias") if dimensions else None) or (
        dimensions[0].get("name") if dimensions else "categoria"
    )

    parts: List[str] = []

    if top_n:
        parts.append(
            f"Os {{top_n}} principais {dimension_alias.lower()} são apresentados ordenados por {metric_alias}."
        )
    else:
        parts.append(f"O gráfico apresenta {metric_alias} por {dimension_alias}.")

    if filters:
        placeholders = []
        for key in filters.keys():
            placeholder_key = _slugify_placeholder(key)
            if placeholder_key:
                placeholders.append(f"{{{placeholder_key}}}")
        if placeholders:
            parts.append(f"Filtros aplicados: {', '.join(placeholders)}.")

    if intent:
        parts.append(f"Intenção detectada: {intent}.")

    return " ".join(parts).strip()


def _generate_description(
    title: Optional[str],
    metrics: List[Dict[str, Any]],
    dimensions: List[Dict[str, Any]],
    filters: Dict[str, Any],
    data_source: Optional[str],
) -> Optional[str]:
    """Create a human-readable description summarizing the visualization."""

    components: List[str] = []

    if title:
        components.append(title.rstrip(".") + ".")

    if metrics and dimensions:
        metric_alias = metrics[0].get("alias") or metrics[0].get("name")
        dimension_alias = dimensions[0].get("alias") or dimensions[0].get("name")
        components.append(f"Apresenta {metric_alias} por {dimension_alias}.")
    elif metrics:
        metric_alias = metrics[0].get("alias") or metrics[0].get("name")
        components.append(f"Apresenta {metric_alias}.")

    if filters:
        filter_parts = []
        for key, value in filters.items():
            pretty_key = _prettify_label(str(key)) or str(key)
            filter_parts.append(f"{pretty_key}: {value}")
        components.append("Filtros aplicados: " + ", ".join(filter_parts) + ".")

    if data_source:
        components.append(f"Fonte de dados: {data_source}.")

    if not components:
        return None

    description = " ".join(components)
    return re.sub(r"\s+", " ", description).strip()


def _generate_title(query: str, provided: Optional[str]) -> Optional[str]:
    """Generate a title from the query if none is provided."""

    if provided:
        return provided

    cleaned = (query or "").strip()
    if not cleaned:
        return None

    cleaned = cleaned.rstrip(" ?!.,;")
    lower_cleaned = cleaned.lower()

    for prefix in TITLE_PREFIXES:
        if lower_cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix) :].lstrip()
            lower_cleaned = cleaned.lower()
            break

    if not cleaned:
        return None

    return cleaned[:1].upper() + cleaned[1:]


def _build_visual_config(
    chart_type: Optional[str],
    provided_visual: Optional[Dict[str, Any]],
    parsed_entities: Dict[str, Any],
) -> Dict[str, Any]:
    """Construct the visual configuration block."""

    provided_visual = provided_visual or {}

    palette = provided_visual.get("palette") or get_default_color_palette(chart_type)

    show_values = provided_visual.get("show_values")
    if show_values is None:
        show_values = chart_type in {
            "bar_horizontal",
            "bar_vertical",
            "bar_vertical_stacked",
            "bar_vertical_composed",
        }

    orientation = provided_visual.get("orientation")
    if orientation is None:
        if chart_type == "bar_horizontal":
            orientation = "horizontal"
        elif chart_type in {
            "bar_vertical",
            "bar_vertical_stacked",
            "bar_vertical_composed",
            "line",
            "line_composed",
        }:
            orientation = "vertical"

    stacked = provided_visual.get("stacked")
    if stacked is None and chart_type == "bar_vertical_stacked":
        stacked = True

    secondary_chart_type = provided_visual.get("secondary_chart_type")
    if secondary_chart_type is None:
        if chart_type == "line_composed":
            secondary_chart_type = None
        elif chart_type == "bar_vertical_composed":
            secondary_chart_type = "bar_vertical"

    bins = provided_visual.get("bins")
    if bins is None and isinstance(parsed_entities, dict):
        bins = parsed_entities.get("bins")
    if bins is None and chart_type == "histogram":
        bins = DEFAULT_HISTOGRAM_BINS

    return {
        "palette": palette,
        "show_values": show_values,
        "orientation": orientation,
        "stacked": stacked,
        "secondary_chart_type": secondary_chart_type,
        "bins": bins,
    }


def _build_sort_config(
    query: str,
    provided_sort: Optional[Any],
    metrics: List[Dict[str, Any]],
    top_n: Optional[int],
    parsed_entities: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Create a normalized sort configuration.

    FASE 3.1: Prioriza ranking_sort_order de parsed_entities quando disponível,
    garantindo que ranking operations detectadas upstream sejam respeitadas.

    CRITICAL: Prioriza sort_by de semantic mapping para compare_variation.
    """

    if isinstance(provided_sort, SortSpec):
        return provided_sort.model_dump()

    by = None
    order = None

    # CRITICAL: Priorizar sort_by de semantic mapping (variation para compare_variation)
    if parsed_entities and parsed_entities.get("sort_by"):
        by = parsed_entities["sort_by"]
        logger.info(f"[_build_sort_config] Using sort_by from semantic mapping: {by}")

    # FASE 3.1: Priorizar ranking_sort_order detectado upstream
    if parsed_entities and parsed_entities.get("ranking_sort_order"):
        order = parsed_entities["ranking_sort_order"]
        logger.info(
            f"[_build_sort_config] Using ranking_sort_order from upstream detection: {order}"
        )

    # Priorizar sort_order de semantic mapping (polarity)
    if not order and parsed_entities and parsed_entities.get("sort_order"):
        order = parsed_entities["sort_order"]
        logger.info(
            f"[_build_sort_config] Using sort_order from semantic mapping: {order}"
        )

    # Fallback para lógica existente
    if isinstance(provided_sort, dict):
        if not by:  # Não sobrescrever sort_by de semantic mapping
            by = provided_sort.get("by")
        if not order:  # Não sobrescrever ranking_sort_order
            order = provided_sort.get("order")
    elif isinstance(provided_sort, str):
        if not order:  # Não sobrescrever ranking_sort_order
            order = provided_sort if provided_sort in VALID_SORT_ORDERS else None

    # Detecção de ordem na query (apenas se não há ranking_sort_order)
    if not order:
        detected_order = detect_sort_order(query) if query else None
        order = order or detected_order

    # Default para rankings
    if top_n and not order:
        order = "desc"

    if order and order not in VALID_SORT_ORDERS:
        order = None

    # Se by ainda não foi definido (não é variation), usar métrica
    if not by and metrics:
        by = metrics[0].get("name") or metrics[0].get("alias")

    if not by and order:
        by = metrics[0].get("name") if metrics else None

    # Ensure order has a valid default when by is defined
    if by and not order:
        order = "asc"  # Default to ascending when sort field is specified

    if not by and not order:
        return None

    return {"by": by, "order": order}


def _infer_data_source(provided: Optional[str]) -> Optional[str]:
    """Infer the data source identifier."""

    if provided:
        return provided

    if DATASET_PATH:
        return Path(DATASET_PATH).stem

    return None


def _build_metrics(
    provided_metrics: Optional[Any],
    mapped_columns: Dict[str, Any],
    parsed_entities: Dict[str, Any],
    chart_type: Optional[str],
) -> List[Dict[str, Any]]:
    """
    Generate the metrics section using provided data and heuristics.

    FASE 1.3 ENHANCEMENT: Now validates and corrects aggregations using
    AggregationValidator to prevent absurd aggregations like MAX for sales.
    """

    aggregation = parsed_entities.get("aggregation")
    if aggregation not in VALID_AGGREGATIONS:
        aggregation = "sum"

    metrics: List[Dict[str, Any]] = []

    # Path 1: Use provided metrics if available
    if isinstance(provided_metrics, list) and provided_metrics:
        for metric in provided_metrics:
            normalized = _normalize_metric(metric, aggregation)
            if normalized:
                metrics.append(normalized)
    elif isinstance(provided_metrics, dict):
        normalized = _normalize_metric(provided_metrics, aggregation)
        if normalized:
            metrics.append(normalized)
    else:
        # Path 2: Infer metrics from mapped columns and hints
        alias_mapper = _get_alias_mapper()
        candidate_metrics: List[str] = []

        # Step 2a: Check mapped columns for metrics
        for _, column in (mapped_columns or {}).items():
            if not column:
                continue

            if alias_mapper and alias_mapper.is_metric_column(column):
                candidate_metrics.append(column)
                continue

            column_lower = column.lower()
            if any(keyword in column_lower for keyword in METRIC_KEYWORDS):
                candidate_metrics.append(column)

        # Step 2b: Use metric_hints to resolve additional metrics
        metric_hints = parsed_entities.get("metric_hints", [])
        if metric_hints and alias_mapper:
            for hint in metric_hints:
                # Try to resolve the hint to an actual column
                resolved = alias_mapper.resolve(hint)
                if resolved and resolved not in candidate_metrics:
                    # Verify it's actually a metric column
                    if alias_mapper.is_metric_column(resolved):
                        candidate_metrics.append(resolved)
                        logger.debug(f"Resolved metric hint '{hint}' to '{resolved}'")

        candidate_metrics = _deduplicate_preserve_order(candidate_metrics)

        # Step 2c: If still no metrics found, apply intelligent fallback
        if not candidate_metrics and chart_type and chart_type != "null":
            logger.info("No metrics detected, applying intelligent fallback")
            fallback_metric = _infer_default_metric(
                parsed_entities, alias_mapper, chart_type
            )
            if fallback_metric:
                candidate_metrics.append(fallback_metric)
                logger.info(f"Using fallback metric: {fallback_metric}")

        for column in candidate_metrics:
            normalized = _normalize_metric(column, aggregation)
            if normalized:
                metrics.append(normalized)

    # FASE 1.3: Validate and correct aggregations for all metrics
    query = parsed_entities.get("original_query", "")
    if query and metrics:
        from src.graphic_classifier.tools.aggregation_validator import (
            validate_and_correct_aggregation,
        )

        for metric in metrics:
            metric_name = metric.get("name")
            current_aggregation = metric.get("aggregation", "sum")

            try:
                validation_result = validate_and_correct_aggregation(
                    query=query,
                    metric_name=metric_name,
                    proposed_aggregation=current_aggregation,
                    parsed_entities=parsed_entities,
                )

                if validation_result["was_corrected"]:
                    logger.warning(
                        f"[_build_metrics] FASE 1.3: Corrected aggregation for '{metric_name}': "
                        f"{validation_result['original_aggregation']} → {validation_result['aggregation']}. "
                        f"Reason: {validation_result['reasoning']}"
                    )
                    metric["aggregation"] = validation_result["aggregation"]
                else:
                    logger.debug(
                        f"[_build_metrics] FASE 1.3: Aggregation '{current_aggregation}' "
                        f"for '{metric_name}' is valid"
                    )

            except Exception as e:
                logger.error(f"[_build_metrics] Error validating aggregation: {e}")

    return metrics


def _infer_default_metric(
    parsed_entities: Dict[str, Any],
    alias_mapper: Optional[AliasMapper],
    chart_type: Optional[str],
) -> Optional[str]:
    """
    Infer a default metric when none is explicitly detected.

    FASE 1.2 ENHANCEMENT: Now uses contextual MetricDetector to resolve
    ambiguities like "vendas" (quantity vs value).

    This function applies semantic analysis and heuristics to select
    the most appropriate metric based on context.

    Args:
        parsed_entities: Parsed query entities
        alias_mapper: AliasMapper instance for column resolution
        chart_type: Detected chart type

    Returns:
        Default metric column name or None
    """
    if not alias_mapper:
        return None

    # Get all available metrics from alias mapper
    all_metrics = alias_mapper.get_all_metrics()

    # Build dynamic set of valid metric columns: all numeric columns + virtual metrics
    # This replaces the hardcoded 'vendas' category that was dataset-specific
    try:
        from src.shared_lib.core.config import get_metric_columns

        valid_metric_columns = set(get_metric_columns())
    except Exception:
        valid_metric_columns = set()
    # Also include all virtual metrics defined in alias.yaml metrics section
    valid_metric_columns.update(all_metrics)

    # FASE 1.2: Use contextual metric detector for intelligent detection
    query = parsed_entities.get("original_query", "")

    if query:
        from src.graphic_classifier.tools.metric_detector import (
            detect_metric_from_query,
        )

        try:
            detection_result = detect_metric_from_query(
                query=query, parsed_entities=parsed_entities, alias_mapper=alias_mapper
            )

            detected_metric = detection_result["metric_name"]
            confidence = detection_result["confidence"]
            ambiguity_resolved = detection_result.get("ambiguity_resolved", False)

            # Verify metric exists in valid columns (numeric or virtual)
            if detected_metric and detected_metric in valid_metric_columns:
                logger.info(
                    f"[_infer_default_metric] FASE 1.2: Contextual detection: {detected_metric} "
                    f"(confidence={confidence:.2f}, ambiguity_resolved={ambiguity_resolved})"
                )
                return detected_metric
            elif detected_metric:
                logger.warning(
                    f"[_infer_default_metric] Detected metric '{detected_metric}' not in "
                    f"valid_metric_columns ({sorted(valid_metric_columns)}), "
                    f"falling back to legacy detection"
                )
        except Exception as e:
            logger.error(
                f"[_infer_default_metric] Error in contextual detection: {e}, using fallback"
            )

    # FALLBACK: Dynamic keyword-to-column matching from alias.yaml
    query_lower = query.lower() if query else ""

    # Construir implicit_patterns dinamicamente a partir de alias.yaml
    try:
        from src.shared_lib.core.config import build_keyword_to_column_map

        keyword_map = build_keyword_to_column_map()
        for keyword, col_name in keyword_map.items():
            if keyword in query_lower:
                # Verificar se a metrica existe nas colunas validas
                if col_name in valid_metric_columns:
                    logger.debug(
                        f"[_infer_default_metric] Dynamic keyword match: "
                        f"'{keyword}' -> '{col_name}'"
                    )
                    return col_name
    except Exception as e:
        logger.warning(f"[_infer_default_metric] Falha ao carregar keyword map: {e}")

    # Priority 2: Use aggregation hint
    aggregation = parsed_entities.get("aggregation")
    if aggregation == "count":
        # For count operations, prefer count-based metrics
        count_metrics = [
            m for m in all_metrics if "numero" in m.lower() or "compra" in m.lower()
        ]
        if count_metrics:
            return count_metrics[0]
    elif aggregation in ["sum", "avg"]:
        # For sum/avg, prefer the aggregation metric from config
        try:
            from src.shared_lib.core.config import get_aggregation_metric

            agg_metric = get_aggregation_metric()
            if agg_metric and agg_metric in valid_metric_columns:
                return agg_metric
        except Exception:
            pass

    # Priority 3: Chart type defaults - use default metric from alias.yaml
    try:
        from src.shared_lib.core.config import get_default_metric, get_metric_columns

        default_metric = get_default_metric()
        metric_columns = get_metric_columns()
        # Para histograma, preferir segunda metrica se existir
        if chart_type == "histogram" and len(metric_columns) > 1:
            hist_metric = metric_columns[1]
            if hist_metric in valid_metric_columns:
                return hist_metric
        # Para outros tipos de grafico, usar metrica padrao
        if chart_type in ("bar_horizontal", "bar_vertical", "pie", "line", "histogram"):
            if default_metric and default_metric in valid_metric_columns:
                return default_metric
    except Exception:
        pass

    # Priority 4: Universal fallback - first numeric column
    try:
        from src.shared_lib.core.config import get_metric_columns

        fallback_metrics = get_metric_columns()
        if fallback_metrics:
            return fallback_metrics[0]
    except Exception:
        pass

    # Last resort: any available metric (including virtual)
    if all_metrics:
        return all_metrics[0]

    return None


def _is_date_range_filter(value: Any) -> bool:
    """
    Detect if a filter value represents a date range.

    FASE 3.2: Delegando para módulo modular dimension_filter_classifier.
    Mantido como wrapper para compatibilidade.

    Args:
        value: Filter value to check

    Returns:
        True if value appears to be a date range filter
    """
    if isinstance(value, list):
        return is_date_range(value)
    elif isinstance(value, dict) and "between" in value:
        return True
    return False


def _is_comparison_filter(value: Any) -> bool:
    """
    Detect if a filter value represents a comparison (multiple distinct values).

    This is a scalable pattern-based detection that works for any column type,
    not just specific column names.

    Args:
        value: Filter value to check

    Returns:
        True if value appears to be a comparison filter (multiple values)
    """
    if isinstance(value, list):
        # Comparison filters have multiple distinct values (more than 1)
        # Date ranges have exactly 2 values and are date-like (handled separately)
        if len(value) > 2:
            return True
        elif len(value) == 2:
            # Could be comparison or date range - check if it's NOT a date range
            return not _is_date_range_filter(value)
    return False


def _infer_temporal_dimension_for_line_chart(
    filters: Optional[Dict[str, Any]],
    query: Optional[str],
    intent: Optional[str],
) -> Optional[str]:
    """
    Inferir dimensão temporal apropriada para line charts.

    REFACTORED: Agora ignora single-value filters não-temporais (ex: UF='SC')
    e foca APENAS em detectar contexto temporal.

    Estratégia de Inferência:
    1. Se há range filter em Data/Ano → usar Mes (agregação mensal)
    2. Se há filter único em Ano → usar Mes (evolução mensal dentro do ano)
    3. Se há filter em Mes → usar Data (evolução diária dentro do mês)
    4. Verificar keywords na query (histórico, evolução, etc.)
    5. Default → Mes (granularidade mais comum para análise temporal)

    IMPORTANTE: Single-value filters não-temporais (ex: UF_Cliente='SC')
    são IGNORADOS - eles devem permanecer como filters, não influenciam a
    escolha da dimensão temporal.

    Args:
        filters: Dicionário de filtros aplicados
        query: Query original do usuário
        intent: Intent classificado pelo LLM

    Returns:
        Nome da coluna temporal a ser usada como dimensão (ex: "Mes", "Data", "Ano")

    Examples:
        >>> _infer_temporal_dimension_for_line_chart(
        ...     filters={"Data": ["2016-01-01", "2016-12-31"], "UF_Cliente": "SC"},
        ...     query="histórico de vendas de SC em 2016",
        ...     intent="Show sales trends"
        ... )
        "Mes"  # Range filter em Data → agregar por Mes (ignora UF_Cliente)

        >>> _infer_temporal_dimension_for_line_chart(
        ...     filters={"Ano": 2015, "Cod_Cliente": "123"},
        ...     query="vendas do cliente 123 em 2015",
        ...     intent="Show sales"
        ... )
        "Mes"  # Ano específico → mostrar mês a mês (ignora Cod_Cliente)
    """

    # TEMPORAL_COLUMNS definition
    TEMPORAL_COLUMNS = [
        "Mes",
        "Ano",
        "Data",
        "Data_Venda",
        "Dia",
        "Trimestre",
        "Semestre",
    ]

    def _is_temporal_column(col_name: str) -> bool:
        """Check if column is temporal."""
        if not col_name:
            return False
        return col_name in TEMPORAL_COLUMNS or any(
            keyword in col_name.lower()
            for keyword in ["data", "mes", "ano", "dia", "trimestre", "semestre"]
        )

    if not filters:
        logger.debug(
            "[_infer_temporal_dimension] No filters, defaulting to 'Mes' for line chart"
        )
        return "Mes"

    # Filtrar apenas filtros temporais (ignorar UF, Cliente, etc.)
    temporal_filters = {
        col: value for col, value in filters.items() if _is_temporal_column(col)
    }

    if not temporal_filters:
        # Nenhum filtro temporal → usar default
        logger.info(
            "[_infer_temporal_dimension] No temporal filters found "
            "(non-temporal filters like UF, Cliente are ignored) → defaulting to 'Mes'"
        )
        return "Mes"

    # 1. Verificar range filter em Data (ex: 2016-01-01 to 2016-12-31)
    if "Data" in temporal_filters:
        data_value = temporal_filters["Data"]
        if isinstance(data_value, list) and len(data_value) == 2:
            # Range filter → agregação mensal é mais apropriada
            logger.info(
                "[_infer_temporal_dimension] Detected date range filter → using 'Mes' dimension"
            )
            return "Mes"
        # Valor único de data → não comum para line chart
        logger.debug(
            "[_infer_temporal_dimension] Single date filter detected → using 'Data'"
        )
        return "Data"

    # 2. Verificar filter em Ano (ex: 2015)
    if "Ano" in temporal_filters:
        ano_value = temporal_filters["Ano"]
        if isinstance(ano_value, list):
            # Múltiplos anos → comparar anos
            logger.info(
                "[_infer_temporal_dimension] Multiple years filter → using 'Ano' dimension"
            )
            return "Ano"
        else:
            # Ano único → mostrar evolução mensal dentro do ano
            logger.info(
                "[_infer_temporal_dimension] Single year filter → using 'Mes' dimension"
            )
            return "Mes"

    # 3. Verificar filter em Mes
    if "Mes" in temporal_filters:
        mes_value = temporal_filters["Mes"]
        if isinstance(mes_value, list):
            # Múltiplos meses → comparar meses
            logger.info(
                "[_infer_temporal_dimension] Multiple months filter → using 'Mes' dimension"
            )
            return "Mes"
        else:
            # Mês único → mostrar evolução diária (se relevante)
            logger.info(
                "[_infer_temporal_dimension] Single month filter → using 'Data' dimension"
            )
            return "Data"

    # 4. Verificar keywords temporais na query
    if query:
        query_lower = query.lower()
        temporal_keywords = {
            "mensal": "Mes",
            "mensalmente": "Mes",
            "mês a mês": "Mes",
            "mes a mes": "Mes",
            "por mês": "Mes",
            "por mes": "Mes",
            "anual": "Ano",
            "anualmente": "Ano",
            "ano a ano": "Ano",
            "por ano": "Ano",
            "diário": "Data",
            "diariamente": "Data",
            "dia a dia": "Data",
            "por dia": "Data",
        }

        for keyword, dimension in temporal_keywords.items():
            if keyword in query_lower:
                logger.info(
                    f"[_infer_temporal_dimension] Detected keyword '{keyword}' → using '{dimension}' dimension"
                )
                return dimension

    # 5. Default: Mes (granularidade mais comum)
    logger.debug(
        "[_infer_temporal_dimension] No specific temporal context, defaulting to 'Mes'"
    )
    return "Mes"


def _extract_dimensions_from_filters(
    filters: Dict[str, Any],
    chart_type: str,
    metric_names: set,
) -> List[str]:
    """
    Extrai dimensões potenciais dos filtros baseado no tipo de gráfico.

    Esta função implementa uma lógica clara e determinística para converter
    filtros em dimensões quando apropriado, especialmente para composed charts.

    Regras:
    - Para bar_vertical_composed/bar_vertical_stacked:
      * Filtros temporais com range (Data, Ano, Mes) → dimensão temporal
      * Filtros categóricos multi-valor (UF_Cliente, etc) → dimensão categórica
      * Maximum de 2 dimensões (requisito do chart type)

    - Para line_composed:
      * Similar ao bar_vertical_composed

    - Para outros chart types:
      * Retorna lista vazia (usa lógica padrão)

    Args:
        filters: Dicionário de filtros aplicados
        chart_type: Tipo de gráfico
        metric_names: Conjunto de nomes de métricas (para exclusão)

    Returns:
        Lista ordenada de nomes de colunas a serem usadas como dimensões

    Examples:
        >>> filters = {"Data": ["2015-01-01", "2015-02-28"], "UF_Cliente": ["SC", "PR"]}
        >>> _extract_dimensions_from_filters(filters, "bar_vertical_composed", set())
        ["Mes", "UF_Cliente"]  # Temporal first, then categorical

        >>> filters = {"Ano": [2015, 2025], "Des_Linha_Produto": ["A", "B", "C"]}
        >>> _extract_dimensions_from_filters(filters, "bar_vertical_composed", set())
        ["Ano", "Des_Linha_Produto"]
    """

    # Chart types que usam esta lógica
    if chart_type not in [
        "bar_vertical_composed",
        "bar_vertical_stacked",
        "line_composed",
    ]:
        return []

    TEMPORAL_COLUMNS = [
        "Mes",
        "Ano",
        "Data",
        "Data_Venda",
        "Dia",
        "Trimestre",
        "Semestre",
    ]

    def _is_temporal(col: str) -> bool:
        return col in TEMPORAL_COLUMNS or any(
            kw in col.lower()
            for kw in ["data", "mes", "ano", "dia", "trimestre", "semestre"]
        )

    def _is_date_range_filter(value: Any) -> bool:
        """Detecta se é um filtro de range temporal."""
        if isinstance(value, list) and len(value) == 2:
            # Check if both are dates or years
            if all(isinstance(v, int) and 1900 <= v <= 2100 for v in value):
                return True
            if all(isinstance(v, str) for v in value):
                # Check if date-like strings
                import re

                date_pattern = r"^\d{4}-\d{2}-\d{2}$"
                return all(re.match(date_pattern, v) for v in value)
        return False

    temporal_dimension = None
    categorical_dimensions = []

    # Processar filtros
    for col, value in filters.items():
        # Skip metrics
        if col in metric_names:
            continue

        # Processar filtros temporais
        if _is_temporal(col):
            # Se é range temporal (ex: Data: ["2015-01-01", "2015-02-28"])
            if _is_date_range_filter(value):
                # Converter para dimensão temporal apropriada
                if col == "Data" or col == "Data_Venda":
                    # Range de datas → agregar por Mes
                    temporal_dimension = "Mes"
                    logger.info(
                        f"[_extract_dimensions_from_filters] Converted date range filter '{col}' "
                        f"to temporal dimension 'Mes'"
                    )
                elif col == "Ano":
                    # Range de anos → usar Ano como dimensão
                    temporal_dimension = "Ano"
                    logger.info(
                        f"[_extract_dimensions_from_filters] Converted year range filter '{col}' "
                        f"to temporal dimension 'Ano'"
                    )
                elif col == "Mes":
                    # Range de meses → usar Mes como dimensão
                    temporal_dimension = "Mes"
                    logger.info(
                        f"[_extract_dimensions_from_filters] Converted month range filter '{col}' "
                        f"to temporal dimension 'Mes'"
                    )
            elif isinstance(value, list) and len(value) >= 2:
                # Multi-valor temporal (ex: Mes: [1, 2, 3])
                temporal_dimension = col
                logger.info(
                    f"[_extract_dimensions_from_filters] Multi-value temporal filter '{col}' "
                    f"converted to dimension"
                )

        # Processar filtros categóricos multi-valor
        elif isinstance(value, list) and len(value) >= 2:
            # Filtro com múltiplos valores → potencial dimensão categórica
            categorical_dimensions.append(col)
            logger.info(
                f"[_extract_dimensions_from_filters] Multi-value categorical filter '{col}' "
                f"identified as potential dimension"
            )

    # Montar lista de dimensões (temporal first, then categorical)
    result = []

    if temporal_dimension:
        result.append(temporal_dimension)

    # Para composed charts, precisamos de exatamente 2 dimensões
    required_dims = (
        2
        if chart_type
        in ["bar_vertical_composed", "bar_vertical_stacked", "line_composed"]
        else 1
    )

    # Adicionar dimensões categóricas até atingir o requisito
    for cat_dim in categorical_dimensions:
        if len(result) < required_dims:
            result.append(cat_dim)

    logger.debug(
        f"[_extract_dimensions_from_filters] Extracted {len(result)} dimensions from filters "
        f"for {chart_type}: {result}"
    )

    return result


def _build_dimensions(
    provided_dimensions: Optional[Any],
    mapped_columns: Dict[str, Any],
    metrics: List[Dict[str, Any]],
    filters: Optional[Dict[str, Any]] = None,
    intent: Optional[str] = None,
    chart_type: Optional[str] = None,
    query: Optional[str] = None,
    intent_config: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Generate the dimensions section using provided data and heuristics.

    REFACTORED: Lógica flexível que prioriza requisitos do chart type.

    Para line/line_composed charts:
    - SEMPRE garante dimensão temporal
    - Single value filters (ex: UF='SC') permanecem como filters
    - Multi-value filters podem ser dimensions SE não for line chart
    - Se intent_config.dimension_structure.series == None, usar apenas 1 dimensão (single_line)

    Para outros charts:
    - Usa lógica original de classificação

    Args:
        provided_dimensions: Dimensions fornecidas explicitamente
        mapped_columns: Mapeamento de colunas
        metrics: Lista de métricas
        filters: Dicionário de filtros
        intent: Intent classificado (para disambiguation)
        chart_type: Tipo de gráfico (para disambiguation)
        query: Query original (para keyword analysis)

    Returns:
        Lista de dimensions normalizadas
    """

    metric_names = {
        metric.get("name")
        for metric in metrics
        if isinstance(metric, dict) and metric.get("name")
    }

    # TEMPORAL_COLUMNS definition for temporal detection
    TEMPORAL_COLUMNS = [
        "Mes",
        "Ano",
        "Data",
        "Data_Venda",
        "Dia",
        "Trimestre",
        "Semestre",
    ]

    def _is_temporal_column(col_name: str) -> bool:
        """Check if column is temporal."""
        if not col_name:
            return False
        return col_name in TEMPORAL_COLUMNS or any(
            keyword in col_name.lower()
            for keyword in ["data", "mes", "ano", "dia", "trimestre", "semestre"]
        )

    # PHASE 5+: filter_classifier is the single source of truth for filters
    # Identificar colunas usadas em filtros e classificar:
    # - period_filter_columns: date ranges (sempre excluir)
    # - single_value_filter_columns: single values (excluir para non-composed charts)
    # - potential_dimension_filters: multi-value (apenas para line_composed)
    period_filter_columns = set()
    single_value_filter_columns = set()
    potential_dimension_filters = {}  # column -> values

    if isinstance(filters, dict):
        for col, value in filters.items():
            if _is_date_range_filter(value):
                # Date range filter - sempre excluir de dimensions
                period_filter_columns.add(col)
                logger.debug(
                    f"[_build_dimensions] Excluding '{col}' from dimensions "
                    f"(used as period/date range filter)"
                )
            elif isinstance(value, (str, int, float, bool)):
                # Single-value filter - SEMPRE excluir de dimensions
                # Exemplo: UF_Cliente='SC', Cliente='123' devem permanecer como filtros
                single_value_filter_columns.add(col)
                logger.debug(
                    f"[_build_dimensions] Excluding '{col}' from dimensions "
                    f"(single-value filter: {col}={value})"
                )
            elif isinstance(value, list) and len(value) == 1:
                # List com único valor - tratar como single-value filter
                single_value_filter_columns.add(col)
                logger.debug(
                    f"[_build_dimensions] Excluding '{col}' from dimensions "
                    f"(single-value list filter: {col}={value})"
                )
            elif isinstance(value, list) and len(value) >= 2:
                # Multi-value filter - pode ser dimension dependendo do chart type
                # Para line: NÃO usar como dimension (apenas temporal)
                # Para line_composed: PODE ser segunda dimensão (categórica)
                if chart_type == "line":
                    # Line simples: multi-value filters são sempre filters
                    logger.debug(
                        f"[_build_dimensions] Keeping '{col}={value}' as FILTER "
                        f"(line chart requires only temporal dimension)"
                    )
                elif chart_type == "line_composed":
                    # Line composed: multi-value filter pode ser segunda dimensão (categórica)
                    # Mas NÃO se for temporal (pois já teremos temporal como primeira dimensão)
                    if not _is_temporal_column(col):
                        potential_dimension_filters[col] = value
                        logger.info(
                            f"[_build_dimensions] Classified '{col}={value}' as POTENTIAL DIMENSION "
                            f"for line_composed (categorical dimension)"
                        )
                    else:
                        logger.debug(
                            f"[_build_dimensions] Keeping temporal '{col}={value}' as FILTER "
                            f"(line_composed uses single temporal as dimension)"
                        )
                elif chart_type not in ["line", "line_composed"]:
                    classification = classify_multi_value_field(
                        column=col,
                        values=value,
                        intent=intent,
                        chart_type=chart_type,
                        query_keywords=query.lower().split() if query else None,
                    )

                    if classification == "dimension":
                        potential_dimension_filters[col] = value
                        logger.info(
                            f"[_build_dimensions] Classified '{col}={value}' as DIMENSION "
                            f"(chart_type={chart_type})"
                        )
                    else:
                        logger.debug(
                            f"[_build_dimensions] Classified '{col}={value}' as FILTER "
                            f"(keeping as filter only)"
                        )

    dimensions: List[Dict[str, Any]] = []

    # 🆕 ESTRATÉGIA DIFERENCIADA: line vs line_composed
    if chart_type == "line":
        # LINE SIMPLES: Apenas 1 dimensão temporal

        # 1. Verificar se provided_dimensions contém dimensão temporal
        if isinstance(provided_dimensions, list) and provided_dimensions:
            for dimension in provided_dimensions:
                normalized = _normalize_dimension(dimension)
                if normalized:
                    dim_name = (
                        normalized.get("name")
                        if isinstance(normalized, dict)
                        else normalized
                    )
                    # Aceitar apenas temporais (não period filters ou single-value filters)
                    if (
                        _is_temporal_column(dim_name)
                        and dim_name not in period_filter_columns
                        and dim_name not in single_value_filter_columns
                    ):
                        dimensions.append(normalized)
                        logger.info(
                            f"[_build_dimensions] Using provided temporal dimension '{dim_name}'"
                        )
                        break  # Line chart needs only 1 dimension

        # 2. Se não tem dimension ainda, buscar em mapped_columns
        if not dimensions:
            for _, column in (mapped_columns or {}).items():
                if (
                    column
                    and column not in metric_names
                    and _is_temporal_column(column)
                    and column not in period_filter_columns
                    and column not in single_value_filter_columns
                ):
                    normalized = _normalize_dimension(column)
                    if normalized:
                        dimensions.append(normalized)
                        logger.info(
                            f"[_build_dimensions] Using mapped temporal dimension '{column}'"
                        )
                        break  # Line chart needs only 1 dimension

        # 3. Se AINDA não tem dimension, inferir automaticamente
        if not dimensions:
            temporal_dim = _infer_temporal_dimension_for_line_chart(
                filters=filters, query=query, intent=intent
            )
            if temporal_dim:
                normalized = _normalize_dimension(temporal_dim)
                if normalized:
                    dimensions.append(normalized)
                    logger.info(
                        f"[_build_dimensions] Auto-inferred temporal dimension '{temporal_dim}' "
                        f"for line chart"
                    )

    elif chart_type == "line_composed":
        # LINE COMPOSED: Decisão baseada em dimension_structure do intent
        #
        # LAYER 6 COMPLIANCE (graph_classifier_correction.md):
        # - Se dimension_structure.series == None → single_line (1 dimensão temporal)
        # - Se dimension_structure.series != None → multi_line (2 dimensões)
        #
        # Isso garante que históricos temporais simples (temporal_trend) usem
        # apenas 1 dimensão, enquanto comparações temporais usem 2 dimensões.

        # DEBUG: Log intent_config received
        logger.info(
            f"[_build_dimensions] line_composed: intent_config received = {intent_config}"
        )

        # Determinar se precisa de série categórica
        requires_series = True  # Default: multi_line (2 dimensões)

        if intent_config and isinstance(intent_config, dict):
            dim_structure = intent_config.get("dimension_structure", {})
            logger.info(
                f"[_build_dimensions] LAYER 6 CHECK: dim_structure={dim_structure}, "
                f"series={dim_structure.get('series')}, type={type(dim_structure.get('series'))}"
            )
            if isinstance(dim_structure, dict) and dim_structure.get("series") is None:
                requires_series = False
                logger.info(
                    f"[_build_dimensions] LAYER 6: dimension_structure.series=None → "
                    f"single_line variant (only temporal dimension)"
                )

        # PASSO 1: Garantir dimensão temporal (primeira - sempre obrigatória)
        temporal_dim_added = False

        # 1.1. Buscar em provided_dimensions
        if isinstance(provided_dimensions, list) and provided_dimensions:
            for dimension in provided_dimensions:
                normalized = _normalize_dimension(dimension)
                if normalized:
                    dim_name = (
                        normalized.get("name")
                        if isinstance(normalized, dict)
                        else normalized
                    )
                    if (
                        _is_temporal_column(dim_name)
                        and dim_name not in period_filter_columns
                        and dim_name not in single_value_filter_columns
                    ):
                        dimensions.append(normalized)
                        temporal_dim_added = True
                        logger.info(
                            f"[_build_dimensions] Using provided temporal dimension '{dim_name}' "
                            f"for line_composed"
                        )
                        break

        # 1.2. Buscar em mapped_columns
        if not temporal_dim_added:
            for _, column in (mapped_columns or {}).items():
                if (
                    column
                    and column not in metric_names
                    and _is_temporal_column(column)
                    and column not in period_filter_columns
                    and column not in single_value_filter_columns
                ):
                    normalized = _normalize_dimension(column)
                    if normalized:
                        dimensions.append(normalized)
                        temporal_dim_added = True
                        logger.info(
                            f"[_build_dimensions] Using mapped temporal dimension '{column}' "
                            f"for line_composed"
                        )
                        break

        # 1.3. Inferir temporal se necessário
        if not temporal_dim_added:
            temporal_dim = _infer_temporal_dimension_for_line_chart(
                filters=filters, query=query, intent=intent
            )
            if temporal_dim:
                normalized = _normalize_dimension(temporal_dim)
                if normalized:
                    dimensions.append(normalized)
                    temporal_dim_added = True
                    logger.info(
                        f"[_build_dimensions] Auto-inferred temporal dimension '{temporal_dim}' "
                        f"for line_composed"
                    )

        # PASSO 2: Adicionar dimensão categórica (segunda) - APENAS SE requires_series=True
        if requires_series:
            categorical_dim_added = False

            # 2.1. Buscar em provided_dimensions (não-temporais)
            if isinstance(provided_dimensions, list) and provided_dimensions:
                for dimension in provided_dimensions:
                    normalized = _normalize_dimension(dimension)
                    if normalized:
                        dim_name = (
                            normalized.get("name")
                            if isinstance(normalized, dict)
                            else normalized
                        )
                        # Adicionar dimensões não-temporais e não-filter-columns
                        if (
                            not _is_temporal_column(dim_name)
                            and dim_name not in period_filter_columns
                            and dim_name not in single_value_filter_columns
                            and dim_name not in metric_names
                        ):
                            if normalized not in dimensions:
                                dimensions.append(normalized)
                                categorical_dim_added = True
                                logger.info(
                                    f"[_build_dimensions] Using provided categorical dimension '{dim_name}' "
                                    f"for line_composed (multi_line variant)"
                                )
                                break  # Apenas 1 categórica

            # 2.2. Buscar em mapped_columns (não-temporais)
            if not categorical_dim_added:
                for _, column in (mapped_columns or {}).items():
                    if (
                        column
                        and column not in metric_names
                        and not _is_temporal_column(column)
                        and column not in period_filter_columns
                        and column not in single_value_filter_columns
                    ):
                        normalized = _normalize_dimension(column)
                        if normalized and normalized not in dimensions:
                            dimensions.append(normalized)
                            categorical_dim_added = True
                            logger.info(
                                f"[_build_dimensions] Using mapped categorical dimension '{column}' "
                                f"for line_composed (multi_line variant)"
                            )
                            break  # Apenas 1 categórica

            # 2.3. Buscar em potential_dimension_filters (multi-value filters)
            if not categorical_dim_added and potential_dimension_filters:
                for col, values in potential_dimension_filters.items():
                    if (
                        col not in period_filter_columns
                        and col not in single_value_filter_columns
                        and col not in metric_names
                        and not _is_temporal_column(col)
                    ):
                        normalized = _normalize_dimension(col)
                        if normalized and normalized not in dimensions:
                            dimensions.append(normalized)
                            categorical_dim_added = True
                            logger.info(
                                f"[_build_dimensions] Using multi-value filter '{col}' as categorical dimension "
                                f"for line_composed (multi_line variant, values: {values})"
                            )
                            break  # Apenas 1 categórica
        else:
            logger.info(
                f"[_build_dimensions] LAYER 6: Skipping categorical dimension - "
                f"single_line variant uses only temporal dimension. "
                f"Final dimensions: {[d.get('name') if isinstance(d, dict) else d for d in dimensions]}"
            )

    elif chart_type in ["bar_vertical_composed", "bar_vertical_stacked"]:
        # BAR VERTICAL COMPOSED/STACKED: 2 dimensões (temporal + categórica)
        #
        # Estratégia determinística:
        # 1. Extrair dimensões dos filtros (se disponíveis)
        # 2. Complementar com provided_dimensions ou mapped_columns
        # 3. Garantir exatamente 2 dimensões (requisito do chart type)

        logger.info(
            f"[_build_dimensions] Processing {chart_type} - requires exactly 2 dimensions"
        )

        # =============================================================
        # FASE 7: NESTED RANKING - ORDENACAO SEMANTICA DAS DIMENSOES
        # =============================================================
        # Para nested ranking, usar ordem explicita do intent_config
        # Isto garante que o grupo principal (estados) seja X-axis
        # e o subgrupo (clientes) seja o hue/stack

        if intent_config and isinstance(intent_config, dict):
            ordered_dims = intent_config.get("dimension_structure", {}).get(
                "ordered_dimensions"
            )

            if (
                ordered_dims
                and len(ordered_dims) >= 2
                and all(d is not None for d in ordered_dims)
            ):
                logger.info(
                    f"[_build_dimensions] NESTED RANKING: Using semantic dimension order: {ordered_dims}"
                )

                for dim_name in ordered_dims:
                    if dim_name and dim_name not in metric_names:
                        normalized = _normalize_dimension(dim_name)
                        if normalized and normalized not in dimensions:
                            dimensions.append(normalized)
                            logger.info(
                                f"[_build_dimensions] NESTED RANKING: Added dimension '{dim_name}'"
                            )

                if len(dimensions) == 2:
                    logger.info(
                        f"[_build_dimensions] NESTED RANKING complete: "
                        f"X-axis={dimensions[0].get('name') if isinstance(dimensions[0], dict) else dimensions[0]}, "
                        f"Stack={dimensions[1].get('name') if isinstance(dimensions[1], dict) else dimensions[1]}"
                    )
                    return dimensions  # Retornar com ordem semantica correta

        # PASSO 1: Extrair dimensões dos filtros (abordagem determinística)
        dimensions_from_filters = _extract_dimensions_from_filters(
            filters=filters or {}, chart_type=chart_type, metric_names=metric_names
        )

        # Adicionar dimensões extraídas dos filtros
        for dim_name in dimensions_from_filters:
            normalized = _normalize_dimension(dim_name)
            if normalized and normalized not in dimensions:
                dimensions.append(normalized)
                logger.info(
                    f"[_build_dimensions] Added dimension '{dim_name}' from filter analysis"
                )

        # PASSO 2: Complementar com provided_dimensions se necessário
        if len(dimensions) < 2 and isinstance(provided_dimensions, list):
            for dimension in provided_dimensions:
                if len(dimensions) >= 2:
                    break
                normalized = _normalize_dimension(dimension)
                if normalized and normalized not in dimensions:
                    dim_name = (
                        normalized.get("name")
                        if isinstance(normalized, dict)
                        else normalized
                    )
                    if (
                        dim_name not in period_filter_columns
                        and dim_name not in single_value_filter_columns
                    ):
                        dimensions.append(normalized)
                        logger.info(
                            f"[_build_dimensions] Added provided dimension '{dim_name}'"
                        )

        # PASSO 3: Complementar com mapped_columns se ainda necessário
        if len(dimensions) < 2:
            for _, column in (mapped_columns or {}).items():
                if len(dimensions) >= 2:
                    break
                if (
                    column
                    and column not in metric_names
                    and column not in period_filter_columns
                    and column not in single_value_filter_columns
                ):
                    normalized = _normalize_dimension(column)
                    if normalized and normalized not in dimensions:
                        dimensions.append(normalized)
                        logger.info(
                            f"[_build_dimensions] Added mapped dimension '{column}'"
                        )

        # PASSO 4: Último recurso - inferir dimensão temporal se ainda faltando
        if len(dimensions) < 2:
            # Verificar se já temos uma temporal
            has_temporal = any(
                _is_temporal_column(d.get("name") if isinstance(d, dict) else d)
                for d in dimensions
            )

            if not has_temporal:
                # Adicionar dimensão temporal inferida
                temporal_dim = _infer_temporal_dimension_for_line_chart(
                    filters=filters, query=query, intent=intent
                )
                if temporal_dim:
                    normalized = _normalize_dimension(temporal_dim)
                    if normalized and normalized not in dimensions:
                        dimensions.append(normalized)
                        logger.info(
                            f"[_build_dimensions] Inferred temporal dimension '{temporal_dim}' "
                            f"to complete requirements for {chart_type}"
                        )

        # Log final
        if len(dimensions) < 2:
            logger.warning(
                f"[_build_dimensions] {chart_type} requires 2 dimensions but only {len(dimensions)} found. "
                f"Available: filters={list((filters or {}).keys())}, "
                f"mapped_columns={list((mapped_columns or {}).keys())}"
            )

    else:
        # Para NON-LINE charts, usar lógica original (mais permissiva)

        if isinstance(provided_dimensions, list) and provided_dimensions:
            for dimension in provided_dimensions:
                normalized = _normalize_dimension(dimension)
                if normalized:
                    dim_name = (
                        normalized.get("name")
                        if isinstance(normalized, dict)
                        else normalized
                    )
                    if (
                        dim_name not in period_filter_columns
                        and dim_name not in single_value_filter_columns
                    ):
                        dimensions.append(normalized)
                    else:
                        logger.debug(
                            f"[_build_dimensions] Skipping dimension '{dim_name}' "
                            f"(used as filter)"
                        )
        elif isinstance(provided_dimensions, dict):
            normalized = _normalize_dimension(provided_dimensions)
            if normalized:
                dim_name = (
                    normalized.get("name")
                    if isinstance(normalized, dict)
                    else normalized
                )
                if (
                    dim_name not in period_filter_columns
                    and dim_name not in single_value_filter_columns
                ):
                    dimensions.append(normalized)
                else:
                    logger.debug(
                        f"[_build_dimensions] Skipping dimension '{dim_name}' "
                        f"(used as filter)"
                    )
        else:
            # Auto-generate dimensions from mapped columns
            for _, column in (mapped_columns or {}).items():
                if not column or column in metric_names:
                    continue

                # Skip columns used as any kind of filter
                if (
                    column in period_filter_columns
                    or column in single_value_filter_columns
                ):
                    logger.debug(
                        f"[_build_dimensions] Skipping auto-dimension '{column}' "
                        f"(used as filter)"
                    )
                    continue

                normalized = _normalize_dimension(column)
                if normalized and normalized not in dimensions:
                    dimensions.append(normalized)

            # Adicionar dimensions inferidas de comparison filters
            if not dimensions and potential_dimension_filters:
                for col, values in potential_dimension_filters.items():
                    if (
                        col not in period_filter_columns
                        and col not in single_value_filter_columns
                        and col not in metric_names
                    ):
                        normalized = _normalize_dimension(col)
                        if normalized and normalized not in dimensions:
                            dimensions.append(normalized)
                            logger.info(
                                f"[_build_dimensions] Added dimension '{col}' "
                                f"from classified comparison filter"
                            )
                            break  # Only add one comparison dimension

    logger.debug(
        f"[_build_dimensions] Final dimensions: {[d.get('name') if isinstance(d, dict) else d for d in dimensions]}"
    )

    return dimensions


def format_output(data: Dict[str, Any]) -> Dict[str, Any]:
    """Format and validate output data against the optimized ChartOutput schema."""

    chart_data = extract_chart_data(data)
    formatted_data = apply_defaults(chart_data)
    aggregation_hint = formatted_data.pop("_aggregation_hint", None)

    # LAYER 6: Preserve _intent_config before Pydantic validation
    # ChartOutput schema doesn't include this field, so we extract it before validation
    intent_config_preserved = formatted_data.pop("_intent_config", None)

    formatted_data = ensure_message_consistency(formatted_data)
    formatted_data = clean_none_values(formatted_data)

    try:
        output = ChartOutput(**formatted_data)
        logger.debug(
            "Output validated successfully: chart_type=%s intent=%s",
            output.chart_type,
            output.intent,
        )
        validated_dict = output.model_dump(exclude_none=False)
        if aggregation_hint and not validated_dict.get("aggregation"):
            validated_dict["aggregation"] = aggregation_hint

        result = _inject_compatibility_fields(validated_dict)

        # LAYER 6: Restore _intent_config for transformation pipeline
        if intent_config_preserved:
            result["_intent_config"] = intent_config_preserved

        return result

    except ValidationError as exc:
        logger.error("Output validation failed: %s", exc)
        raise


def extract_chart_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract relevant fields and preserve context for default inference."""

    if not isinstance(data, dict):
        return {}

    result: Dict[str, Any] = {
        "intent": data.get("intent"),
        "chart_type": data.get("chart_type"),
        "title": data.get("title"),
        "description": data.get("description"),
        "metrics": data.get("metrics"),
        "dimensions": data.get("dimensions"),
        "filters": data.get("filters"),
        "top_n": data.get("top_n"),
        "group_top_n": data.get("group_top_n"),  # Nested ranking support
        "sort": data.get("sort"),
        "visual": data.get("visual"),
        "data_source": data.get("data_source"),
        "output": data.get("output"),
        "message": data.get("message"),
    }

    parsed_entities = data.get("parsed_entities")
    if not isinstance(parsed_entities, dict):
        parsed_entities = {}

    mapped_columns = data.get("mapped_columns")
    if not isinstance(mapped_columns, dict):
        mapped_columns = {}

    result["_query"] = data.get("query", "")
    result["_parsed_entities"] = parsed_entities
    result["_mapped_columns"] = mapped_columns

    # FASE 2: Adicionar dimension_analysis e sort_analysis
    result["_dimension_analysis"] = data.get("dimension_analysis")
    result["_sort_analysis"] = data.get("sort_analysis")
    result["_intent_config"] = data.get("intent_config")

    return result


def apply_defaults(data: Dict[str, Any]) -> Dict[str, Any]:
    """Apply defaults and inferred values according to the optimized schema."""

    result = data.copy()

    query = result.pop("_query", "")
    parsed_entities = result.pop("_parsed_entities", {})
    mapped_columns = result.pop("_mapped_columns", {})

    # FASE 2: Extrair dimension_analysis e sort_analysis
    dimension_analysis = result.pop("_dimension_analysis", None)
    sort_analysis = result.pop("_sort_analysis", None)
    intent_config = result.pop("_intent_config", None)

    # PHASE 5+: filter_classifier is the SINGLE SOURCE OF TRUTH for all filters
    # graphic_classifier must NEVER add, modify, or merge filters from parsed_entities
    # Only use explicitly provided filters from filter_classifier (via filter_final)
    filters = result.get("filters")
    filters = filters if isinstance(filters, dict) else {}

    # DO NOT merge from parsed_entities - this causes conflicts with filter_classifier
    # filter_classifier already handles all filter normalization and validation
    result["filters"] = filters

    result["metrics"] = _build_metrics(
        result.get("metrics"), mapped_columns, parsed_entities, result.get("chart_type")
    )

    result["dimensions"] = _build_dimensions(
        result.get("dimensions"),
        mapped_columns,
        result["metrics"],
        filters,  # Pass filters to exclude period filter columns
        intent=result.get("intent"),  # FASE 3.2: Pass intent for classification
        chart_type=result.get("chart_type"),  # FASE 3.2: Pass chart_type
        query=query,  # FASE 3.2: Pass query for keyword analysis
        intent_config=intent_config,  # LAYER 6: Pass intent_config for dimension_structure
    )

    top_n = result.get("top_n")
    if top_n is None:
        potential_top_n = (
            parsed_entities.get("top_n") if isinstance(parsed_entities, dict) else None
        )
        if isinstance(potential_top_n, (int, float)):
            top_n = int(potential_top_n)
    elif isinstance(top_n, float):
        top_n = int(top_n)
    result["top_n"] = top_n

    # NESTED RANKING DETECTION: Detect "top N within top M" patterns
    # For queries like "top 3 clientes dos 5 maiores estados"
    # This should return M×N rows (e.g., 5 states × 3 clients = 15 rows)
    nested_ranking = extract_nested_ranking(query)
    if nested_ranking.get("is_nested"):
        # Override top_n with the subgroup limit (N)
        result["top_n"] = nested_ranking["top_n"]
        # Add group_top_n for the main group limit (M)
        result["group_top_n"] = nested_ranking["group_top_n"]

        logger.info(
            f"[apply_defaults] Nested ranking detected: "
            f"top {nested_ranking['group_top_n']} {nested_ranking.get('group_entity', 'groups')} × "
            f"top {nested_ranking['top_n']} {nested_ranking.get('subgroup_entity', 'items')} = "
            f"{nested_ranking['group_top_n'] * nested_ranking['top_n']} expected rows"
        )
    else:
        # No nested ranking - use default behavior
        result["group_top_n"] = None

    # FASE 2: Usar sort_analysis se disponivel
    if sort_analysis and sort_analysis.get("sort_config"):
        result["sort"] = sort_analysis["sort_config"]
        logger.debug(
            f"[apply_defaults] Using sort_analysis from FASE 2: "
            f"by={sort_analysis['sort_config']['by']}, order={sort_analysis['sort_config']['order']}"
        )
    else:
        result["sort"] = _build_sort_config(
            query,
            result.get("sort"),
            result["metrics"],
            result["top_n"],
            parsed_entities,  # FASE 3.1: Passar parsed_entities com ranking_sort_order
        )

    provided_visual = (
        result.get("visual") if isinstance(result.get("visual"), dict) else None
    )
    result["visual"] = _build_visual_config(
        result.get("chart_type"), provided_visual, parsed_entities
    )

    if not result.get("intent") and result.get("chart_type") in VALID_CHART_TYPES:
        result["intent"] = "unknown"

    data_source = _infer_data_source(result.get("data_source"))
    result["data_source"] = data_source

    title = _generate_title(query, result.get("title"))
    result["title"] = title
    result["description"] = _generate_description(
        title, result["metrics"], result["dimensions"], result["filters"], data_source
    )

    output_config = result.get("output")
    if isinstance(output_config, OutputSpec):
        output_config = output_config.model_dump()
    elif not isinstance(output_config, dict):
        output_config = {}

    output_type = output_config.get("type")
    if not output_type:
        output_type = (
            "chart_and_summary" if result.get("chart_type") else "summary_only"
        )

    summary_template = output_config.get("summary_template")
    if not summary_template:
        summary_template = _generate_summary_template(
            result.get("intent"),
            result.get("top_n"),
            result["metrics"],
            result["dimensions"],
            result["filters"],
        )

    result["output"] = {"type": output_type, "summary_template": summary_template}

    aggregation_hint: Optional[str] = None
    if result["metrics"]:
        first_metric = result["metrics"][0]
        if isinstance(first_metric, dict):
            aggregation_hint = first_metric.get("aggregation")
    else:
        candidate = (
            parsed_entities.get("aggregation")
            if isinstance(parsed_entities, dict)
            else None
        )
        if candidate in VALID_AGGREGATIONS:
            aggregation_hint = candidate
        elif result.get("chart_type") in VALID_CHART_TYPES:
            aggregation_hint = "sum"

    result["_aggregation_hint"] = aggregation_hint

    # LAYER 6: Preserve intent_config for downstream transformation pipeline
    # This ensures adjust_dimensions_by_chart_type can check for single_line variant
    if intent_config:
        result["_intent_config"] = intent_config

    return result


def ensure_message_consistency(data: Dict[str, Any]) -> Dict[str, Any]:
    """Guarantee that no-chart outputs include an explanatory message."""

    result = data.copy()

    if result.get("chart_type") is None:
        if not result.get("message"):
            result["message"] = "Nenhum gráfico necessário para esta consulta."
    else:
        # Remove empty messages for chart outputs
        if isinstance(result.get("message"), str) and not result["message"].strip():
            result["message"] = None

    return result


def clean_none_values(data: Dict[str, Any]) -> Dict[str, Any]:
    """Remove None values from optional fields while preserving required structure."""

    keep_none_fields = {
        "chart_type",
        "intent",
        "title",
        "description",
        "visual",
        "sort",
        "output",
        "data_source",
        "top_n",
        "group_top_n",  # NESTED RANKING: Preserve group_top_n even if None
        "metrics",
        "dimensions",
        "filters",
        "columns_detected",
    }

    cleaned: Dict[str, Any] = {}

    for key, value in data.items():
        if value is None and key not in keep_none_fields:
            continue
        cleaned[key] = value

    return cleaned


def get_default_color_palette(chart_type: Optional[str]) -> Optional[str]:
    """
    Get default color palette for a chart type.

    Args:
        chart_type: Chart type

    Returns:
        Default color palette name
    """
    palette_map = {
        "bar_horizontal": "Blues",
        "bar_vertical": "Greens",
        "bar_vertical_composed": "Set2",
        "line": "Greens",
        "line_composed": "tab10",
        "pie": "Set3",
        "bar_vertical_stacked": "Set2",
        "histogram": "Purples",
    }

    return palette_map.get(chart_type)


def infer_label_format(metrics: List[str]) -> Optional[str]:
    """
    Infer appropriate label format from metric names.

    Args:
        metrics: List of metric column names

    Returns:
        Inferred label format
    """
    if not metrics:
        return None

    # Check metric names for hints
    for metric in metrics:
        metric_lower = metric.lower()

        # Currency indicators
        if any(
            word in metric_lower
            for word in ["valor", "faturamento", "receita", "preco"]
        ):
            return "currency"

        # Percentage indicators
        if any(word in metric_lower for word in ["percentual", "taxa", "proporcao"]):
            return "percent"

        # Quantity indicators
        if any(word in metric_lower for word in ["quantidade", "qtd", "numero"]):
            return "integer"

    # Default to float for numeric values
    return "float"


def validate_chart_output(
    output: Union[Dict[str, Any], ChartOutput],
) -> Union[Dict[str, Any], tuple[bool, List[str]]]:
    """Validate output data against the optimized ChartOutput schema."""

    if isinstance(output, ChartOutput):
        errors = validate_chart_type_requirements(output)
        if output.chart_type is None and not output.message:
            errors.append("chart_type is None but message is missing")
        is_valid = len(errors) == 0
        return (is_valid, errors)

    # NESTED RANKING: Preserve group_top_n before re-processing
    # The output may have already been through format_output with group_top_n set
    preserved_group_top_n = output.get("group_top_n")

    # LAYER 6: Preserve _intent_config for downstream transformation pipeline
    preserved_intent_config = output.get("_intent_config")

    structured = extract_chart_data(output)
    formatted = apply_defaults(structured)
    aggregation_hint = formatted.pop("_aggregation_hint", None)

    # LAYER 6: Preserve _intent_config before ChartOutput validation
    # ChartOutput schema doesn't include _intent_config, so we need to restore it
    intent_config_from_formatted = formatted.pop("_intent_config", None)

    formatted = ensure_message_consistency(formatted)
    formatted = clean_none_values(formatted)

    # NESTED RANKING: Restore group_top_n if it was present and got lost
    if preserved_group_top_n is not None and formatted.get("group_top_n") is None:
        formatted["group_top_n"] = preserved_group_top_n
        logger.info(
            f"[validate_chart_output] Restored group_top_n={preserved_group_top_n}"
        )

    try:
        validated_output = ChartOutput(**formatted)
        chart_type_errors = validate_chart_type_requirements(validated_output)
        if chart_type_errors:
            logger.warning(
                "Chart output validated but contains semantic issues: %s",
                chart_type_errors,
            )
        validated_dict = validated_output.model_dump(exclude_none=False)
        if aggregation_hint and not validated_dict.get("aggregation"):
            validated_dict["aggregation"] = aggregation_hint

        # LAYER 6: Restore _intent_config for transformation pipeline
        final_intent_config = intent_config_from_formatted or preserved_intent_config
        if final_intent_config:
            validated_dict["_intent_config"] = final_intent_config

        return _inject_compatibility_fields(validated_dict)

    except ValidationError as exc:
        logger.error("Output validation failed: %s", exc)
        raise


def validate_chart_type_requirements(output: ChartOutput) -> List[str]:
    """Validate chart-type specific requirements."""

    errors: List[str] = []

    chart_type = output.chart_type
    metrics = output.metrics or []
    dimensions = output.dimensions or []

    if chart_type in {
        "bar_horizontal",
        "bar_vertical",
        "bar_vertical_composed",
        "bar_vertical_stacked",
    }:
        if not metrics:
            errors.append(f"{chart_type} requires at least one metric")
        if not dimensions:
            errors.append(f"{chart_type} requires at least one dimensão")

    if chart_type in {"line", "line_composed"}:
        if not metrics:
            errors.append(f"{chart_type} requires at least one metric")
        if not dimensions:
            errors.append(f"{chart_type} requires at least uma dimensão temporal")

    if chart_type == "pie":
        if not metrics or not dimensions:
            errors.append("pie requires at least one metric and one dimensão")

    if chart_type == "histogram":
        if not metrics:
            errors.append("histogram requires a metric for distribution analysis")
        visual_bins = output.visual.bins if output.visual else None
        if visual_bins is None:
            logger.info(
                "Histogram output without bins specified; renderer may apply defaults"
            )

    if output.top_n and (not output.sort or not output.sort.order):
        errors.append("top_n specified but sort configuration missing order")

    if chart_type and not metrics:
        errors.append(f"{chart_type} requires at least one metric")

    return errors


def format_for_rendering(output: ChartOutput) -> Dict[str, Any]:
    """Prepare a ChartOutput instance for JSON serialization by rendering agents."""

    data = output.model_dump(exclude_none=False)

    if "metrics" not in data:
        data["metrics"] = []

    if "dimensions" not in data:
        data["dimensions"] = []

    if "filters" not in data:
        data["filters"] = {}

    return data


def create_error_output(error_message: str, query: str = "") -> ChartOutput:
    """Create an error output when processing fails."""

    title = _generate_title(query, "Erro ao processar consulta")
    description = f"Erro ao processar query: {error_message}"

    return ChartOutput(
        intent="error",
        chart_type=None,
        title=title,
        description=description,
        metrics=[],
        dimensions=[],
        filters={},
        visual=VisualSpec(),
        data_source=_infer_data_source(None),
        output=OutputSpec(type="message", summary_template=description),
        message=description,
        columns_detected=None,
    )


def create_no_chart_output(
    reason: str,
    columns_detected: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
) -> ChartOutput:
    """Create output for queries that do not require a chart."""

    description = reason.rstrip(".") + "."

    return ChartOutput(
        intent="informational",
        chart_type=None,
        title=reason,
        description=description,
        metrics=[],
        dimensions=[],
        filters=filters or {},
        visual=VisualSpec(),
        data_source=_infer_data_source(None),
        output=OutputSpec(type="summary_only", summary_template=description),
        message=reason,
        columns_detected=columns_detected,
    )


def merge_outputs(base: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge two output dictionaries, with update taking precedence.

    Args:
        base: Base output dictionary
        update: Update dictionary with new values

    Returns:
        Merged dictionary
    """
    result = base.copy()

    for key, value in update.items():
        if value is not None:
            result[key] = value
        elif key not in result:
            result[key] = value

    return result


def prettify_output(output: ChartOutput, indent: int = 2) -> str:
    """
    Create a pretty-printed JSON string of the output.

    Args:
        output: ChartOutput instance
        indent: Number of spaces for indentation

    Returns:
        Pretty-printed JSON string
    """
    import json

    data = format_for_rendering(output)
    return json.dumps(data, indent=indent, ensure_ascii=False)
