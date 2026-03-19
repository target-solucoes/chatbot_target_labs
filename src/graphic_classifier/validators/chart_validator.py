"""
Chart Type Validator - Cross-Field Validation.

This module implements FASE 5 of the disambiguation improvement strategy,
providing cross-field validation to detect inconsistencies between
chart_type and data structure.

The validator checks:
1. Minimum/maximum dimension requirements per chart type
2. Temporal dimension requirements for time-based charts
3. Multi-value temporal filter requirements for composed charts
4. Conflicts between chart type and dimension/filter structure

When inconsistencies are detected with low confidence, the validator
can suggest corrections and trigger reclassification.

Reference: graph_classifier_diagnosis.md - FASE 5

Examples:
    >>> validator = ChartTypeValidator()
    >>> result = {
    ...     "chart_type": "line_composed",
    ...     "dimensions": [{"name": "Des_Linha_Produto"}],
    ...     "filters": {"Mes": "Janeiro"},
    ...     "confidence": 0.65
    ... }
    >>> is_valid, warnings = validator.validate(result)
    >>> is_valid
    False
    >>> warnings
    [
        'line_composed requires 2 dimensions (temporal + category), got 1.',
        'line_composed requires temporal dimension for time series.'
    ]
    >>> suggestion = validator.suggest_correction(result, warnings)
    >>> suggestion
    {
        'suggested_chart_type': 'bar_horizontal',
        'confidence': 0.75,
        'reason': 'Insufficient dimensions for multi-series chart (need 2, got 1)'
    }
"""

import logging
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger(__name__)


class ChartTypeValidator:
    """
    Validates consistency between chart_type and data structure.

    This validator implements FASE 5 validation rules that detect
    structural inconsistencies after classification. It provides both
    validation warnings and correction suggestions.

    Validation Rules:
    -----------------
    REFACTORED: Removed bar_vertical_composed (migrated to line_composed).

    Validation Rules per Chart Type:

    line_composed:
        - min_dimensions: 2
        - requires_temporal: True

    bar_vertical_stacked:
        - min_dimensions: 2
        - requires_composition: True

    line / line_composed:
        - min_dimensions: 1
        - requires_temporal: True

    bar_horizontal:
        - max_dimensions: 1
        - conflicts_with_multi_value_dimension: True

    Attributes:
        VALIDATION_RULES: Dict mapping chart types to validation requirements
        TEMPORAL_DIMS: List of temporal dimension names
        LOW_CONFIDENCE_THRESHOLD: Threshold for triggering correction (0.70)
    """

    # Lista de dimensões temporais conhecidas
    TEMPORAL_DIMS = ["Mes", "Ano", "Data", "Trimestre", "Semestre", "Dia", "Semana"]

    # Threshold para considerar confiança baixa
    LOW_CONFIDENCE_THRESHOLD = 0.70

    # Regras de validação por tipo de gráfico
    # REFACTORED: Removed bar_vertical_composed (migrated to line_composed)
    VALIDATION_RULES = {
        "bar_vertical_stacked": {
            "min_dimensions": 2,
            "requires_composition": True,
            "description": "Stacked bar chart for composition of subcategories",
        },
        "line": {
            "min_dimensions": 1,
            "requires_temporal": True,
            "description": "Line chart for single temporal trend",
        },
        "line_composed": {
            "min_dimensions": 2,
            "requires_temporal": True,
            "description": "Multi-line chart for multiple temporal trends",
        },
        "bar_horizontal": {
            "max_dimensions": 1,
            "conflicts_with_multi_value_dimension": True,
            "description": "Horizontal bar chart for simple ranking/top-N",
        },
        "bar_vertical": {
            "min_dimensions": 1,
            "description": "Vertical bar chart for direct category comparison",
        },
        "pie": {
            "min_dimensions": 1,
            "description": "Pie chart for proportional distribution",
        },
        "histogram": {
            "min_dimensions": 0,
            "description": "Histogram for value distribution",
        },
    }

    def __init__(self):
        """Initialize the chart type validator."""
        logger.info("[ChartTypeValidator] Initialized FASE 5 validator")

    def validate(self, result: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate resultado da classificação.

        Verifica se o chart_type classificado é consistente com a estrutura
        de dados (dimensions, filters, etc.). Detecta problemas como:
        # REFACTORED: Examples updated to reflect bar_vertical_composed removal
        # - bar_horizontal com multi-value dimension → line_composed (for temporal)
        # - line_composed com < 2 dimensions → bar_horizontal
        - bar_horizontal com dimension multi-valor
        - line sem dimension temporal
        - bar_vertical_stacked sem composição

        Args:
            result: Dicionário com resultado da classificação:
                {
                    "chart_type": str,
                    "dimensions": list[dict],
                    "filters": dict,
                    "confidence": float,
                    "intent": str
                }

        Returns:
            Tupla (is_valid, warnings):
                - is_valid: True se não há warnings, False caso contrário
                - warnings: Lista de mensagens de aviso

        Examples:
            >>> validator = ChartTypeValidator()
            >>> result = {
            ...     "chart_type": "line_composed",
            ...     "dimensions": [{"name": "Mes"}, {"name": "Produto"}],
            ...     "filters": {},
            ...     "confidence": 0.85
            ... }
            >>> is_valid, warnings = validator.validate(result)
            >>> is_valid
            False
            >>> len(warnings)
            2
        """
        chart_type = result.get("chart_type")

        # Se chart_type é None ou não tem regras, considerar válido
        if not chart_type or chart_type not in self.VALIDATION_RULES:
            return True, []

        rules = self.VALIDATION_RULES[chart_type]
        warnings: List[str] = []

        dimensions = result.get("dimensions", [])
        filters = result.get("filters", {})
        confidence = result.get("confidence", 0.0)

        # Check for nested ranking early (affects dimension validation)
        has_nested_ranking = (
            result.get("group_top_n") is not None and result.get("group_top_n") > 0
        )

        # LAYER 6: Check for single_line variant in line_composed
        intent_config = result.get("_intent_config") or result.get("intent_config")
        is_single_line = False
        if chart_type == "line_composed" and intent_config:
            dim_structure = intent_config.get("dimension_structure", {})
            if isinstance(dim_structure, dict) and dim_structure.get("series") is None:
                is_single_line = True
                logger.debug(
                    "[ChartTypeValidator] LAYER 6: line_composed single_line variant detected"
                )

        # Regra: min_dimensions
        if "min_dimensions" in rules:
            min_dims = rules["min_dimensions"]
            # For bar_vertical_stacked with nested ranking, only 1 dimension is OK
            if chart_type == "bar_vertical_stacked" and has_nested_ranking:
                min_dims = 1
            # LAYER 6: For line_composed single_line variant, only 1 dimension is OK
            if chart_type == "line_composed" and is_single_line:
                min_dims = 1

            if len(dimensions) < min_dims:
                warnings.append(
                    f"{chart_type} requires {min_dims} dimension(s), got {len(dimensions)}. "
                    f"Consider simpler chart type."
                )

        # Regra: max_dimensions
        if "max_dimensions" in rules:
            max_dims = rules["max_dimensions"]
            if len(dimensions) > max_dims:
                warnings.append(
                    f"{chart_type} should have max {max_dims} dimension(s), got {len(dimensions)}. "
                    f"Consider bar_vertical or line_composed for multi-dimensional data."
                )

        # Regra: requires_temporal
        if rules.get("requires_temporal", False):
            has_temporal = self._has_temporal_dimension(dimensions)
            if not has_temporal:
                dim_names = [d.get("name") for d in dimensions]
                warnings.append(
                    f"{chart_type} should have temporal dimension, got {dim_names}. "
                    f"Consider bar_vertical or pie."
                )

        # Regra: requires_multi_value_temporal
        if rules.get("requires_multi_value_temporal", False):
            has_multi_val = self._has_multi_value_temporal_filter(filters)
            if not has_multi_val:
                warnings.append(
                    f"{chart_type} requires 2+ temporal values for comparison. "
                    f"Consider bar_horizontal or bar_vertical."
                )

        # Regra: requires_composition
        if rules.get("requires_composition", False):
            # Verificar se há evidência de composição/nested ranking
            # has_nested_ranking already defined above
            has_multiple_dimensions = len(dimensions) >= 2

            if not has_nested_ranking and not has_multiple_dimensions:
                warnings.append(
                    f"{chart_type} requires composition structure (2+ dimensions or nested ranking). "
                    f"Consider bar_horizontal or bar_vertical."
                )

        # Regra: conflicts_with_multi_value_dimension
        if rules.get("conflicts_with_multi_value_dimension", False):
            conflicts = self._check_multi_value_dimension_conflict(dimensions, filters)
            if conflicts:
                for conflict_msg in conflicts:
                    warnings.append(conflict_msg)

        # Se confiança baixa + warnings, aumentar severidade
        if confidence < self.LOW_CONFIDENCE_THRESHOLD and len(warnings) > 0:
            warnings.append(
                f"Low confidence ({confidence:.2f}) + validation warnings = high risk of misclassification."
            )

        is_valid = len(warnings) == 0
        return is_valid, warnings

    def suggest_correction(
        self, result: Dict[str, Any], warnings: List[str]
    ) -> Optional[Dict[str, Any]]:
        """
        Sugere correção baseada em warnings detectados.

        Analisa os warnings e propõe um chart_type alternativo que melhor
        se adequa à estrutura de dados detectada.

        Args:
            result: Resultado original da classificação
            warnings: Lista de warnings detectados por validate()

        Returns:
            Dicionário com sugestão de correção ou None se não há sugestão:
            {
                "suggested_chart_type": str,
                "confidence": float,
                "reason": str
            }

        Examples:
            >>> validator = ChartTypeValidator()
            >>> result = {
            ...     "chart_type": "bar_horizontal",
            ...     "dimensions": [{"name": "UF_Cliente"}],
            ...     "filters": {"UF_Cliente": ["SP", "RJ"]}
            ... }
            >>> _, warnings = validator.validate(result)
            >>> suggestion = validator.suggest_correction(result, warnings)
            >>> suggestion["suggested_chart_type"]
            'bar_vertical'
        """
        if not warnings:
            return None

        chart_type = result.get("chart_type")
        dimensions = result.get("dimensions", [])
        filters = result.get("filters", {})

        # Correção: bar_horizontal com multi-value dimension
        if chart_type == "bar_horizontal":
            for dim in dimensions:
                dim_name = dim.get("name")
                if dim_name in filters and isinstance(filters[dim_name], list):
                    filter_values = filters[dim_name]
                    if len(filter_values) >= 2:
                        # Verificar se é temporal
                        if dim_name in self.TEMPORAL_DIMS:
                            return {
                                "suggested_chart_type": "line_composed",
                                "confidence": 0.85,
                                "reason": f"Multi-value temporal dimension '{dim_name}' ({filter_values}) "
                                f"indicates temporal comparison",
                            }
                        else:
                            return {
                                "suggested_chart_type": "bar_vertical",
                                "confidence": 0.80,
                                "reason": f"Multi-value dimension '{dim_name}' ({filter_values}) "
                                f"indicates direct comparison",
                            }

        # Note: bar_vertical_composed was deprecated and migrated to line_composed

        # Correção: line/line_composed sem temporal dimension
        if chart_type in ["line", "line_composed"]:
            has_temporal = self._has_temporal_dimension(dimensions)
            if not has_temporal:
                return {
                    "suggested_chart_type": "bar_vertical",
                    "confidence": 0.75,
                    "reason": "No temporal dimension detected, better suited for bar chart",
                }

        # Correção: bar_vertical_stacked sem composição clara
        if chart_type == "bar_vertical_stacked":
            if len(dimensions) < 2 and not result.get("group_top_n"):
                return {
                    "suggested_chart_type": "bar_horizontal",
                    "confidence": 0.75,
                    "reason": "Insufficient structure for stacked composition (need 2 dimensions or nested ranking)",
                }

        return None

    def _has_temporal_dimension(self, dimensions: List[Dict]) -> bool:
        """
        Verifica se há dimensão temporal na lista de dimensões.

        Args:
            dimensions: Lista de dicionários com dimensões

        Returns:
            True se há pelo menos uma dimensão temporal
        """
        for dim in dimensions:
            dim_name = dim.get("name", "")
            if dim_name in self.TEMPORAL_DIMS:
                return True
        return False

    def _has_multi_value_temporal_filter(self, filters: Dict[str, Any]) -> bool:
        """
        Verifica se há filtro temporal com múltiplos valores (2+).

        Args:
            filters: Dicionário de filtros

        Returns:
            True se há filtro temporal com 2+ valores
        """
        for key, value in filters.items():
            if key in self.TEMPORAL_DIMS:
                if isinstance(value, list) and len(value) >= 2:
                    return True
        return False

    def _check_multi_value_dimension_conflict(
        self, dimensions: List[Dict], filters: Dict[str, Any]
    ) -> List[str]:
        """
        Verifica conflitos entre bar_horizontal e dimensions com múltiplos valores.

        Args:
            dimensions: Lista de dimensões
            filters: Dicionário de filtros

        Returns:
            Lista de mensagens de conflito (vazia se não há conflitos)
        """
        conflicts = []

        for dim in dimensions:
            dim_name = dim.get("name")
            if dim_name in filters:
                filter_value = filters[dim_name]
                if isinstance(filter_value, list) and len(filter_value) >= 2:
                    conflicts.append(
                        f"bar_horizontal with multi-value dimension '{dim_name}' ({filter_value}). "
                        f"This looks like comparison. Consider bar_vertical or line_composed."
                    )

        return conflicts

    def get_validation_rules(self, chart_type: str) -> Optional[Dict[str, Any]]:
        """
        Retorna as regras de validação para um tipo específico de gráfico.

        Args:
            chart_type: Tipo do gráfico

        Returns:
            Dicionário com regras de validação ou None se não existem
        """
        return self.VALIDATION_RULES.get(chart_type)

    def get_all_chart_types(self) -> List[str]:
        """
        Retorna lista de todos os tipos de gráficos com regras de validação.

        Returns:
            Lista de tipos de gráficos
        """
        return list(self.VALIDATION_RULES.keys())
