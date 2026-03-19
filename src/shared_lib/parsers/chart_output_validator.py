"""
Validador de conformidade de ChartOutput com especificacoes.

Este modulo implementa validacao rigorosa de outputs contra as regras
definidas em CHART_TYPE_SPECS.md, garantindo que cada tipo de grafico
segue as especificacoes de metrics, dimensions e configuracoes visuais.
"""

from typing import List, Dict, Any, Tuple
import logging

logger = logging.getLogger(__name__)


# Especificacoes de requisitos por chart_type
CHART_TYPE_REQUIREMENTS = {
    "bar_horizontal": {
        "metrics_min": 1,
        "dimensions_exact": 1,
        "temporal_required": False,
        "default_aggregation": "sum",
        "default_sort": "desc",
    },
    "bar_vertical": {
        "metrics_min": 1,
        "dimensions_exact": 1,
        "temporal_required": False,
        "default_aggregation": "sum",
    },
    "bar_vertical_composed": {
        "metrics_min": 1,
        "dimensions_exact": 2,
        "temporal_required": False,
        "default_aggregation": "sum",
        "description": "Requires 2 dimensions for grouped comparison",
    },
    "bar_vertical_stacked": {
        "metrics_min": 1,
        "dimensions_exact": 2,
        "temporal_required": False,
        "default_aggregation": "sum",
        "visual_stacked": True,
        "description": "Requires 2 dimensions and visual.stacked=True",
    },
    "line": {
        "metrics_min": 1,
        "dimensions_exact": 1,
        "temporal_required": True,
        "default_aggregation": "sum",
        "default_sort": "asc",
        "description": "Requires exactly 1 temporal dimension",
    },
    "line_composed": {
        "metrics_min": 1,
        "dimensions_exact": 2,
        "temporal_required": True,
        "default_aggregation": "sum",
        "default_sort": "asc",
        "description": "Requires 2 dimensions (temporal + category)",
    },
    "pie": {
        "metrics_min": 1,
        "dimensions_exact": 1,
        "temporal_required": False,
        "default_aggregation": "sum",
        "default_sort": "desc",
    },
    "histogram": {
        "metrics_min": 1,
        "dimensions_exact": 0,
        "temporal_required": False,
        "default_aggregation": "count",
        "visual_bins": True,
        "description": "Requires 0 dimensions and visual.bins specified",
    },
}


# Colunas temporais reconhecidas
TEMPORAL_COLUMNS = ["Mes", "Ano", "Data", "Trimestre", "Semana", "Dia"]


def validate_chart_output_conformity(output: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Valida se ChartOutput esta conforme com specs do chart_type.

    Args:
        output: Dicionario com o output do graphical_classifier

    Returns:
        Tuple (is_valid, errors)
        - is_valid: True se conforme, False se ha violacoes
        - errors: Lista de mensagens de erro (vazia se valid)

    Examples:
        >>> output = {
        ...     "chart_type": "bar_vertical",
        ...     "metrics": [{"name": "Valor_Vendido", "aggregation": "sum"}],
        ...     "dimensions": [{"name": "UF_Cliente"}]
        ... }
        >>> is_valid, errors = validate_chart_output_conformity(output)
        >>> assert is_valid
    """
    errors = []

    chart_type = output.get("chart_type")

    if chart_type is None:
        # Null chart type tem regras especiais
        return validate_null_chart_type(output)

    if chart_type not in CHART_TYPE_REQUIREMENTS:
        errors.append(f"Unknown chart_type: '{chart_type}'")
        return False, errors

    req = CHART_TYPE_REQUIREMENTS[chart_type]

    # Validar metrics
    metrics = output.get("metrics", [])
    if len(metrics) < req["metrics_min"]:
        errors.append(
            f"{chart_type} requires at least {req['metrics_min']} metric(s), "
            f"got {len(metrics)}"
        )

    # Validar dimensions
    dimensions = output.get("dimensions", [])
    if "dimensions_exact" in req:
        expected_dims = req["dimensions_exact"]

        # LAYER 6: For line_composed, check for single_line variant
        intent_config = output.get("_intent_config") or output.get("intent_config")
        if chart_type == "line_composed" and intent_config:
            dim_structure = intent_config.get("dimension_structure", {})
            if isinstance(dim_structure, dict) and dim_structure.get("series") is None:
                # single_line variant: only 1 dimension required
                expected_dims = 1
                logger.debug(
                    f"[validate_chart_output_conformity] LAYER 6: line_composed single_line "
                    f"variant detected - expecting 1 dimension"
                )

        if len(dimensions) != expected_dims:
            errors.append(
                f"{chart_type} requires exactly {expected_dims} dimension(s), "
                f"got {len(dimensions)}. "
                f"Spec: {req.get('description', 'N/A')}"
            )

    # Validar temporal requirement
    if req.get("temporal_required"):
        has_temporal = any(dim.get("name") in TEMPORAL_COLUMNS for dim in dimensions)
        if not has_temporal:
            errors.append(
                f"{chart_type} requires temporal dimension "
                f"(one of: {', '.join(TEMPORAL_COLUMNS)}). "
                f"Got dimensions: {[d.get('name') for d in dimensions]}"
            )

    # Validar visual.stacked
    visual = output.get("visual", {})
    if req.get("visual_stacked"):
        if not visual.get("stacked"):
            errors.append(
                f"{chart_type} requires visual.stacked=True. "
                f"Got: {visual.get('stacked')}"
            )

    # Validar visual.bins
    if req.get("visual_bins"):
        if not visual.get("bins"):
            errors.append(
                f"{chart_type} requires visual.bins to be specified. "
                f"Got: {visual.get('bins')}"
            )

    # Validar default_aggregation (warning, nao erro)
    if metrics and isinstance(metrics, list) and "default_aggregation" in req:
        default_agg = req["default_aggregation"]
        actual_aggs = [m.get("aggregation") for m in metrics if isinstance(m, dict)]
        if actual_aggs and all(agg != default_agg for agg in actual_aggs):
            logger.warning(
                f"{chart_type} typically uses '{default_agg}' aggregation, "
                f"but got: {actual_aggs}"
            )

    # Validar default_sort (warning, nao erro)
    sort = output.get("sort", {})
    if sort and "default_sort" in req:
        default_order = req["default_sort"]
        actual_order = sort.get("order")
        if actual_order and actual_order != default_order:
            logger.warning(
                f"{chart_type} typically uses sort order '{default_order}', "
                f"but got: '{actual_order}'"
            )

    return len(errors) == 0, errors


def validate_null_chart_type(output: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Valida output com chart_type=None.

    Args:
        output: Output com chart_type=None

    Returns:
        Tuple (is_valid, errors)
    """
    errors = []

    # Deve ter mensagem explicando por que nao ha grafico
    if not output.get("message"):
        errors.append(
            "chart_type=None requires 'message' field explaining "
            "why no visualization is needed"
        )

    # Output type deve ser adequado
    output_config = output.get("output", {})
    output_type = output_config.get("type")

    if output_type not in ["summary_only", "text", None]:
        errors.append(
            f"chart_type=None should have output.type='summary_only' or 'text', "
            f"got '{output_type}'"
        )

    # Metrics e dimensions devem estar vazios ou ausentes
    metrics = output.get("metrics", [])
    dimensions = output.get("dimensions", [])

    if len(metrics) > 0:
        logger.warning(
            f"chart_type=None typically has empty metrics, "
            f"but got {len(metrics)} metric(s)"
        )

    if len(dimensions) > 0:
        logger.warning(
            f"chart_type=None typically has empty dimensions, "
            f"but got {len(dimensions)} dimension(s)"
        )

    return len(errors) == 0, errors


def validate_chart_output_schema(output: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Valida estrutura basica do schema ChartOutput.

    Verifica presenca de campos obrigatorios e tipos corretos.

    Args:
        output: Output a validar

    Returns:
        Tuple (is_valid, errors)
    """
    errors = []

    # Campos obrigatorios
    required_fields = ["chart_type", "metrics", "dimensions", "filters"]

    for field in required_fields:
        if field not in output:
            errors.append(f"Missing required field: '{field}'")

    # Validar tipos
    if "metrics" in output and not isinstance(output["metrics"], list):
        errors.append(f"Field 'metrics' must be list, got {type(output['metrics'])}")

    if "dimensions" in output and not isinstance(output["dimensions"], list):
        errors.append(
            f"Field 'dimensions' must be list, got {type(output['dimensions'])}"
        )

    if "filters" in output and not isinstance(output["filters"], dict):
        errors.append(f"Field 'filters' must be dict, got {type(output['filters'])}")

    # Validar estrutura de metrics
    for i, metric in enumerate(output.get("metrics", [])):
        if not isinstance(metric, dict):
            errors.append(f"Metric {i} must be dict, got {type(metric)}")
            continue

        if "name" not in metric:
            errors.append(f"Metric {i} missing required field 'name'")

        if "aggregation" not in metric:
            errors.append(f"Metric {i} missing required field 'aggregation'")

    # Validar estrutura de dimensions
    for i, dim in enumerate(output.get("dimensions", [])):
        if not isinstance(dim, dict):
            errors.append(f"Dimension {i} must be dict, got {type(dim)}")
            continue

        if "name" not in dim:
            errors.append(f"Dimension {i} missing required field 'name'")

    return len(errors) == 0, errors


def validate_full_conformity(output: Dict[str, Any]) -> Dict[str, Any]:
    """
    Valida conformidade completa de um output.

    Combina validacao de schema e validacao de conformidade com specs.

    Args:
        output: Output a validar

    Returns:
        Dict com resultado da validacao:
        {
            "is_valid": bool,
            "schema_valid": bool,
            "conformity_valid": bool,
            "errors": list,
            "warnings": list
        }
    """
    all_errors = []

    # Validar schema
    schema_valid, schema_errors = validate_chart_output_schema(output)
    all_errors.extend(schema_errors)

    # Validar conformidade com specs
    conformity_valid, conformity_errors = validate_chart_output_conformity(output)
    all_errors.extend(conformity_errors)

    return {
        "is_valid": schema_valid and conformity_valid,
        "schema_valid": schema_valid,
        "conformity_valid": conformity_valid,
        "errors": all_errors,
        "chart_type": output.get("chart_type"),
    }


# Funcao de conveniencia para uso em testes
def assert_chart_output_valid(output: Dict[str, Any], context: str = ""):
    """
    Assert que output e valido, lancando AssertionError com detalhes se nao for.

    Args:
        output: Output a validar
        context: Contexto adicional para mensagem de erro

    Raises:
        AssertionError: Se output nao for valido
    """
    result = validate_full_conformity(output)

    if not result["is_valid"]:
        error_msg = "ChartOutput validation failed"
        if context:
            error_msg += f" ({context})"
        error_msg += ":\n"
        error_msg += f"  chart_type: {result['chart_type']}\n"
        error_msg += f"  schema_valid: {result['schema_valid']}\n"
        error_msg += f"  conformity_valid: {result['conformity_valid']}\n"
        error_msg += "  errors:\n"
        for error in result["errors"]:
            error_msg += f"    - {error}\n"

        raise AssertionError(error_msg)
