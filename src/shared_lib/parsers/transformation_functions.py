"""
Individual Transformation Functions for Chart Spec Pipeline

Este modulo contem funcoes de transformacao independentes que podem
ser compostas em um pipeline para transformar ChartOutput specs.

Cada funcao e autonoma, testavel e documentada.

Column classifications (METRIC_COLUMNS, DIMENSION_COLUMNS, TEMPORAL_COLUMNS)
are loaded dynamically from alias.yaml via shared_lib.core.config,
ensuring zero hardcoded coupling to any specific dataset.

Referencia: planning_graphical_correction.md - Fase 3.3
"""

import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


# ========== Dynamic Column Loading from alias.yaml ==========


def _load_metric_columns() -> List[str]:
    """Load numeric columns from alias.yaml (replaces hardcoded METRIC_COLUMNS)."""
    try:
        from src.shared_lib.core.config import get_metric_columns

        cols = get_metric_columns()
        logger.debug(
            f"[transformation_functions] Loaded METRIC_COLUMNS from alias.yaml: {cols}"
        )
        return cols
    except Exception as e:
        logger.error(f"[transformation_functions] Failed to load metric columns: {e}")
        return []


def _load_dimension_columns() -> List[str]:
    """Load categorical columns from alias.yaml (replaces hardcoded DIMENSION_COLUMNS)."""
    try:
        from src.shared_lib.core.config import get_dimension_columns

        cols = get_dimension_columns()
        logger.debug(
            f"[transformation_functions] Loaded DIMENSION_COLUMNS from alias.yaml: {cols}"
        )
        return cols
    except Exception as e:
        logger.error(
            f"[transformation_functions] Failed to load dimension columns: {e}"
        )
        return []


def _load_temporal_columns() -> List[str]:
    """Load temporal columns from alias.yaml (replaces hardcoded TEMPORAL_COLUMNS)."""
    try:
        from src.shared_lib.core.config import get_temporal_columns

        cols = get_temporal_columns()
        logger.debug(
            f"[transformation_functions] Loaded TEMPORAL_COLUMNS from alias.yaml: {cols}"
        )
        return cols
    except Exception as e:
        logger.error(f"[transformation_functions] Failed to load temporal columns: {e}")
        return []


def _load_keyword_mappings() -> Dict[str, str]:
    """
    Build keyword->column mappings from alias.yaml.

    Replaces hardcoded keyword_mappings dict.
    Returns a dict of lowercase_keyword -> real_column_name.
    """
    try:
        from src.shared_lib.core.config import (
            build_keyword_to_column_map,
            get_metric_columns,
        )

        all_keywords = build_keyword_to_column_map()
        numeric_cols = set(get_metric_columns())
        # Filter to only keywords that map to numeric (metric) columns
        metric_keywords = {k: v for k, v in all_keywords.items() if v in numeric_cols}
        logger.debug(
            f"[transformation_functions] Built keyword_mappings with {len(metric_keywords)} entries"
        )
        return metric_keywords
    except Exception as e:
        logger.error(f"[transformation_functions] Failed to load keyword mappings: {e}")
        return {}


def _get_default_metric() -> Optional[str]:
    """Get the default metric (first numeric column) from alias.yaml."""
    try:
        from src.shared_lib.core.config import get_default_metric

        return get_default_metric()
    except Exception as e:
        logger.error(f"[transformation_functions] Failed to get default metric: {e}")
        return None


# Module-level lazy-loaded column lists
# These are populated on first access from alias.yaml
_METRIC_COLUMNS: Optional[List[str]] = None
_DIMENSION_COLUMNS: Optional[List[str]] = None
_TEMPORAL_COLUMNS: Optional[List[str]] = None


def _get_metric_columns() -> List[str]:
    global _METRIC_COLUMNS
    if _METRIC_COLUMNS is None:
        _METRIC_COLUMNS = _load_metric_columns()
    return _METRIC_COLUMNS


def _get_dimension_columns() -> List[str]:
    global _DIMENSION_COLUMNS
    if _DIMENSION_COLUMNS is None:
        _DIMENSION_COLUMNS = _load_dimension_columns()
    return _DIMENSION_COLUMNS


def _get_temporal_columns() -> List[str]:
    global _TEMPORAL_COLUMNS
    if _TEMPORAL_COLUMNS is None:
        _TEMPORAL_COLUMNS = _load_temporal_columns()
    return _TEMPORAL_COLUMNS


# Backward-compatible module-level names (now properties via lazy loading)
# Code that reads METRIC_COLUMNS etc. directly will still work via these accessors.
# However, internal functions should use _get_*_columns() for guaranteed freshness.


# ========== Chart Type Requirements ==========

CHART_TYPE_REQUIREMENTS = {
    "bar_horizontal": {
        "min_metrics": 1,
        "min_dimensions": 1,
        "max_dimensions": 1,
        "requires_temporal": False,
        "default_aggregation": "sum",
        "description": "Rankings and top-N comparisons",
    },
    "bar_vertical": {
        "min_metrics": 1,
        "min_dimensions": 1,
        "max_dimensions": 1,
        "requires_temporal": False,
        "default_aggregation": "sum",
        "description": "Direct comparisons between categories",
    },
    "bar_vertical_composed": {
        "min_metrics": 1,
        "min_dimensions": 2,
        "max_dimensions": 2,
        "requires_temporal": False,
        "default_aggregation": "sum",
        "description": "Grouped comparisons across periods or conditions",
    },
    "bar_vertical_stacked": {
        "min_metrics": 1,
        "min_dimensions": 2,
        "max_dimensions": 2,
        "requires_temporal": False,
        "default_aggregation": "sum",
        "description": "Composition of subcategories within main categories",
    },
    "line": {
        "min_metrics": 1,
        "min_dimensions": 1,
        "max_dimensions": 1,
        "requires_temporal": True,
        "default_aggregation": "sum",
        "description": "Temporal trends and series (or ordered numeric axes)",
    },
    "line_composed": {
        "min_metrics": 1,
        "min_dimensions": 2,
        "max_dimensions": 2,
        "requires_temporal": True,
        "default_aggregation": "sum",
        "description": "Multiple category trends over time (or ordered numeric axes)",
    },
    "pie": {
        "min_metrics": 1,
        "min_dimensions": 1,
        "max_dimensions": 1,
        "requires_temporal": False,
        "default_aggregation": "sum",
        "description": "Proportional composition and participation",
    },
    "histogram": {
        "min_metrics": 1,
        "min_dimensions": 0,
        "max_dimensions": 0,
        "requires_temporal": False,
        "default_aggregation": "count",
        "description": "Distribution of numeric values",
    },
}


# ========== Column Classifications (Dynamic from alias.yaml) ==========
# Module-level names populated at import time for backward compatibility.
# Internal functions use _get_*_columns() for guaranteed freshness.

METRIC_COLUMNS: List[str] = _get_metric_columns()
DIMENSION_COLUMNS: List[str] = _get_dimension_columns()
TEMPORAL_COLUMNS: List[str] = _get_temporal_columns()


# ========== Transformation 1: Infer Missing Metrics ==========


def infer_missing_metrics(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Garante que o spec tenha métricas suficientes para o tipo de gráfico.

    Se métricas estão faltando, tenta inferir de:
    1. parsed_entities (metric_hints)
    2. Palavras-chave da query
    3. Defaults do chart type

    Args:
        spec: Chart specification

    Returns:
        Spec com métricas inferidas (se necessário)
    """
    chart_type = spec.get("chart_type")

    if chart_type is None or chart_type == "null":
        return spec

    if chart_type not in CHART_TYPE_REQUIREMENTS:
        logger.warning(f"[infer_missing_metrics] Unknown chart type '{chart_type}'")
        return spec

    metrics = spec.get("metrics", [])
    requirements = CHART_TYPE_REQUIREMENTS[chart_type]
    min_metrics = requirements["min_metrics"]

    if len(metrics) >= min_metrics:
        logger.debug(
            f"[infer_missing_metrics] Sufficient metrics: {len(metrics)}/{min_metrics}"
        )
        return spec

    logger.warning(
        f"[infer_missing_metrics] Insufficient metrics for {chart_type}: "
        f"found {len(metrics)}, need {min_metrics}"
    )

    # Tentar inferir métrica
    inferred_metric = _infer_metric_from_context(spec)

    if inferred_metric:
        if not metrics:
            metrics = []

        metric_spec = {
            "name": inferred_metric,
            "aggregation": requirements["default_aggregation"],
            "alias": _prettify_label(inferred_metric),
        }

        metrics.append(metric_spec)
        spec["metrics"] = metrics

        logger.info(f"[infer_missing_metrics] Inferred metric: {inferred_metric}")
    else:
        logger.error(f"[infer_missing_metrics] Could not infer metric for {chart_type}")

    return spec


def _infer_metric_from_context(spec: Dict[str, Any]) -> Optional[str]:
    """Inferir metrica de parsed_entities, query, ou defaults.

    All column references are derived dynamically from alias.yaml.
    No hardcoded column names.
    """
    metric_columns = _get_metric_columns()
    parsed_entities = (
        spec.get("parsed_entities", {})
        if isinstance(spec.get("parsed_entities"), dict)
        else {}
    )

    # Prioridade 1: metric_hints
    metric_hints = parsed_entities.get("metric_hints", [])
    for hint in metric_hints:
        if hint in metric_columns:
            return hint

        # Tentar variacoes de case
        for col in metric_columns:
            if col.lower() == hint.lower():
                return col

    # Prioridade 2: Palavras-chave da query (from alias.yaml)
    query = spec.get("query", "").lower()
    keyword_mappings = _load_keyword_mappings()

    for keyword, metric in keyword_mappings.items():
        if keyword in query:
            return metric

    # Prioridade 3: Default metric (first numeric column from alias.yaml)
    default_metric = _get_default_metric()
    return default_metric


# ========== Transformation 2: Infer Temporal Dimensions ==========


def infer_temporal_dimensions(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Valida e infere dimensões temporais para line charts.

    Garante que line/line_composed tenham pelo menos uma dimensão temporal.

    Args:
        spec: Chart specification

    Returns:
        Spec com dimensão temporal inferida (se necessário)
    """
    chart_type = spec.get("chart_type")

    if chart_type is None or chart_type == "null":
        return spec

    if chart_type not in CHART_TYPE_REQUIREMENTS:
        return spec

    requirements = CHART_TYPE_REQUIREMENTS[chart_type]

    if not requirements.get("requires_temporal"):
        logger.debug(
            "[infer_temporal_dimensions] Chart type does not require temporal dimension"
        )
        return spec

    # Graceful degradation: if dataset has no temporal columns at all,
    # skip temporal enforcement — line charts can use non-temporal X-axis
    temporal_columns = _get_temporal_columns()
    if not temporal_columns:
        logger.info(
            f"[infer_temporal_dimensions] No temporal columns in alias.yaml. "
            f"Skipping temporal requirement for {chart_type} — "
            f"non-temporal dimension will be used for X-axis."
        )
        return spec

    dimensions = spec.get("dimensions", [])

    # Verificar se já tem dimensão temporal
    has_temporal = any(
        _is_temporal_column(d.get("name") if isinstance(d, dict) else d)
        for d in dimensions
    )

    if has_temporal:
        logger.debug("[infer_temporal_dimensions] Temporal dimension already present")
        return spec

    logger.warning(
        f"[infer_temporal_dimensions] {chart_type} requires temporal dimension but none found"
    )

    # Tentar inferir dimensão temporal
    temporal_dim = _infer_temporal_dimension_from_context(spec)

    if temporal_dim:
        dim_spec = {"name": temporal_dim, "alias": _prettify_label(temporal_dim)}

        # Inserir como primeira dimensão
        if not dimensions:
            dimensions = []
        dimensions.insert(0, dim_spec)

        spec["dimensions"] = dimensions
        logger.info(
            f"[infer_temporal_dimensions] Added temporal dimension: {temporal_dim}"
        )
    else:
        logger.error(
            f"[infer_temporal_dimensions] Could not infer temporal dimension for {chart_type}"
        )

    return spec


def _infer_temporal_dimension_from_context(spec: Dict[str, Any]) -> Optional[str]:
    """
    Inferir dimensao temporal ADEQUADA baseada no contexto.

    Se o dataset nao possui colunas temporais (temporal_columns vazio),
    retorna None imediatamente (graceful degradation).

    Args:
        spec: Chart specification com filters, query, chart_type

    Returns:
        Nome da coluna temporal apropriada, ou None se nao houver.
    """
    temporal_columns = _get_temporal_columns()

    # Se nao ha colunas temporais no dataset, retornar None imediatamente
    if not temporal_columns:
        logger.info(
            "[_infer_temporal_dimension] No temporal columns in alias.yaml, "
            "skipping temporal inference"
        )
        return None

    filters = spec.get("filters", {})
    chart_type = spec.get("chart_type")
    query = spec.get("query", "").lower()

    # 1. Analisar filters para detectar range filters
    for filter_col, filter_value in filters.items():
        if not _is_temporal_column(filter_col):
            continue

        is_range = isinstance(filter_value, list) and len(filter_value) == 2

        # Usar logica generica baseada nas colunas temporais disponiveis
        if is_range:
            # Para range filters, usar a primeira coluna temporal como dimensao
            logger.info(
                f"[_infer_temporal_dimension] Range filter on '{filter_col}' "
                f"-> using '{temporal_columns[0]}' as dimension"
            )
            return temporal_columns[0]

    # 2. Verificar query por keywords temporais (genericos)
    temporal_keywords_generic = [
        "mensal",
        "mensalmente",
        "mes",
        "historico",
        "evolucao",
        "tendencia",
        "anual",
        "anualmente",
        "ano",
        "data",
        "dia",
        "diario",
        "trimestre",
        "trimestral",
    ]

    for keyword in temporal_keywords_generic:
        if keyword in query:
            logger.info(
                f"[_infer_temporal_dimension] Detected keyword '{keyword}' "
                f"-> using '{temporal_columns[0]}' as dimension"
            )
            return temporal_columns[0]

    # 3. Default para line charts: primeira coluna temporal
    if chart_type in ["line", "line_composed"]:
        logger.debug(
            f"[_infer_temporal_dimension] Line chart -> "
            f"defaulting to '{temporal_columns[0]}'"
        )
        return temporal_columns[0]

    # 4. Fallback geral: primeira coluna temporal
    logger.debug(f"[_infer_temporal_dimension] Fallback -> '{temporal_columns[0]}'")
    return temporal_columns[0]


def _is_temporal_column(col_name: Optional[str]) -> bool:
    """Verifica se coluna e temporal (dynamic from alias.yaml)."""
    if not col_name:
        return False

    temporal_columns = _get_temporal_columns()
    return col_name in temporal_columns


# ========== Transformation 3: Normalize Aggregations ==========


def normalize_aggregations(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza funções de agregação baseadas no chart type.

    Garante que métricas tenham agregações apropriadas.

    Args:
        spec: Chart specification

    Returns:
        Spec com agregações normalizadas
    """
    chart_type = spec.get("chart_type")

    if chart_type is None or chart_type == "null":
        return spec

    if chart_type not in CHART_TYPE_REQUIREMENTS:
        return spec

    requirements = CHART_TYPE_REQUIREMENTS[chart_type]
    default_agg = requirements["default_aggregation"]

    metrics = spec.get("metrics", [])

    for metric in metrics:
        if isinstance(metric, dict):
            if not metric.get("aggregation"):
                metric["aggregation"] = default_agg
                logger.debug(
                    f"[normalize_aggregations] Set default aggregation '{default_agg}' "
                    f"for metric '{metric.get('name')}'"
                )

    return spec


# ========== Transformation 4: Adjust Dimensions by Chart Type ==========


def adjust_dimensions_by_chart_type(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ajusta número de dimensões para os requisitos do chart type.

    Adiciona ou remove dimensões conforme necessário.

    Args:
        spec: Chart specification

    Returns:
        Spec com dimensões ajustadas
    """
    chart_type = spec.get("chart_type")

    if chart_type is None or chart_type == "null":
        return spec

    if chart_type not in CHART_TYPE_REQUIREMENTS:
        return spec

    dimensions = spec.get("dimensions", [])
    requirements = CHART_TYPE_REQUIREMENTS[chart_type]

    min_dims = requirements["min_dimensions"]
    max_dims = requirements["max_dimensions"]
    requires_temporal = requirements["requires_temporal"]

    # LAYER 6: Check intent_config for single_line variant
    # When dimension_structure.series=None, line_composed uses only 1 dimension (temporal)
    intent_config = spec.get("_intent_config") or spec.get("intent_config")
    if chart_type == "line_composed" and intent_config:
        dim_structure = intent_config.get("dimension_structure", {})
        if isinstance(dim_structure, dict) and dim_structure.get("series") is None:
            # single_line variant: only temporal dimension needed
            min_dims = 1
            max_dims = 1
            logger.info(
                f"[adjust_dimensions_by_chart_type] LAYER 6: line_composed single_line variant "
                f"detected (series=None). Adjusted min_dims=1, max_dims=1"
            )

    # Caso 1: Poucas dimensões
    if len(dimensions) < min_dims:
        logger.warning(
            f"[adjust_dimensions_by_chart_type] Insufficient dimensions for {chart_type}: "
            f"found {len(dimensions)}, need {min_dims}"
        )

        inferred_dims = _infer_dimensions_from_context(
            spec, count=min_dims - len(dimensions), requires_temporal=requires_temporal
        )

        if inferred_dims:
            if not dimensions:
                dimensions = []
            dimensions.extend(inferred_dims)
            spec["dimensions"] = dimensions
            logger.info(
                f"[adjust_dimensions_by_chart_type] Inferred {len(inferred_dims)} dimensions: "
                f"{[d['name'] for d in inferred_dims]}"
            )

    # Caso 2: Muitas dimensões
    elif len(dimensions) > max_dims:
        logger.warning(
            f"[adjust_dimensions_by_chart_type] Too many dimensions for {chart_type}: "
            f"found {len(dimensions)}, max {max_dims}"
        )

        dimensions = _select_best_dimensions(dimensions, max_dims, requires_temporal)
        spec["dimensions"] = dimensions
        logger.info(
            f"[adjust_dimensions_by_chart_type] Reduced to {len(dimensions)} dimensions"
        )

    return spec


def _infer_dimensions_from_context(
    spec: Dict[str, Any], count: int, requires_temporal: bool
) -> List[Dict[str, Any]]:
    """Inferir dimensões faltantes de parsed_entities ou defaults."""
    inferred = []
    parsed_entities = (
        spec.get("parsed_entities", {})
        if isinstance(spec.get("parsed_entities"), dict)
        else {}
    )
    potential_columns = parsed_entities.get("potential_columns", [])

    # Se requer temporal, priorizar
    if requires_temporal:
        temporal_dim = _infer_temporal_dimension_from_context(spec)
        if temporal_dim:
            inferred.append(
                {"name": temporal_dim, "alias": _prettify_label(temporal_dim)}
            )
            count -= 1

    # Tentar usar potential_columns (dynamic)
    dimension_columns = _get_dimension_columns()
    for col_ref in potential_columns:
        if len(inferred) >= count:
            break

        if col_ref in dimension_columns and col_ref not in [
            d["name"] for d in inferred
        ]:
            inferred.append({"name": col_ref, "alias": _prettify_label(col_ref)})

    # Se ainda precisa mais, usar defaults (dynamic)
    if len(inferred) < count:
        for dim_col in dimension_columns:
            if len(inferred) >= count:
                break

            if dim_col not in [d["name"] for d in inferred]:
                inferred.append({"name": dim_col, "alias": _prettify_label(dim_col)})

    return inferred


def _select_best_dimensions(
    dimensions: List[Dict[str, Any]], max_count: int, requires_temporal: bool
) -> List[Dict[str, Any]]:
    """Seleciona as dimensões mais relevantes quando há muitas."""
    if len(dimensions) <= max_count:
        return dimensions

    selected = []

    # Prioridade 1: Dimensões temporais (se requeridas)
    if requires_temporal:
        for dim in dimensions:
            dim_name = dim.get("name") if isinstance(dim, dict) else dim
            if _is_temporal_column(dim_name):
                selected.append(dim)
                if len(selected) >= max_count:
                    return selected

    # Prioridade 2: Primeiras dimensões (mais relevantes)
    for dim in dimensions:
        if dim not in selected:
            selected.append(dim)
            if len(selected) >= max_count:
                return selected

    return selected


# ========== Transformation 5: Apply Chart-Specific Fixes ==========


def apply_chart_specific_fixes(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Aplica fixes específicos por chart type.

    Args:
        spec: Chart specification

    Returns:
        Spec com fixes aplicados
    """
    chart_type = spec.get("chart_type")

    if chart_type is None or chart_type == "null":
        return spec

    # Fix 1: Histogram
    if chart_type == "histogram":
        # Histogramas NÃO devem ter dimensions (binning é feito na métrica)
        if spec.get("dimensions"):
            logger.warning(
                "[apply_chart_specific_fixes] Histogram should not have dimensions, removing"
            )
            spec["dimensions"] = []

        # Garantir agregação count ou raw
        metrics = spec.get("metrics", [])
        if metrics and isinstance(metrics[0], dict):
            if metrics[0].get("aggregation") not in ["count", None]:
                logger.info(
                    "[apply_chart_specific_fixes] Changing histogram aggregation to 'count'"
                )
                metrics[0]["aggregation"] = "count"

    # Fix 2: Pie chart
    elif chart_type == "pie":
        # Pie charts devem ter exatamente 1 dimensão
        dimensions = spec.get("dimensions", [])
        if len(dimensions) > 1:
            logger.warning(
                f"[apply_chart_specific_fixes] Pie chart should have 1 dimension, "
                f"found {len(dimensions)}, keeping first"
            )
            spec["dimensions"] = dimensions[:1]

    # Fix 3: Composed/Stacked charts
    elif chart_type in [
        "bar_vertical_composed",
        "bar_vertical_stacked",
        "line_composed",
    ]:
        dimensions = spec.get("dimensions", [])

        # LAYER 6: Check for single_line variant in line_composed
        intent_config = spec.get("_intent_config") or spec.get("intent_config")
        is_single_line = False
        if chart_type == "line_composed" and intent_config:
            dim_structure = intent_config.get("dimension_structure", {})
            if isinstance(dim_structure, dict) and dim_structure.get("series") is None:
                is_single_line = True
                logger.info(
                    "[apply_chart_specific_fixes] LAYER 6: line_composed single_line variant "
                    "(series=None) - 1 dimension is valid"
                )

        if len(dimensions) < 2 and not is_single_line:
            logger.warning(
                f"[apply_chart_specific_fixes] {chart_type} needs 2 dimensions "
                f"but has {len(dimensions)}. May need to infer secondary dimension."
            )

    return spec


# ========== Utility Functions ==========


def _prettify_label(value: Optional[str]) -> Optional[str]:
    """Converte nome de coluna para label amigável."""
    if not value:
        return None

    label = value.replace("_", " ").strip()
    if not label:
        return None

    return label[:1].upper() + label[1:]
