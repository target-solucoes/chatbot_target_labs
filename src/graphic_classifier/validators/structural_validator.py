"""
Structural Validator - FASE 3: Validacoes Estruturais.

Este modulo implementa validacoes estruturais conforme especificado em
planning_graph_classifier_diagnosis.md - FASE 3, Etapa 3.3.

Checklist de Validacao (conforme planning):
1. filter_covers_periods - Todos os periodos no filtro
2. metric_matches_query - Metrica correta
3. aggregation_appropriate - Agregacao faz sentido
4. baseline_defined - Baseline para comparacao
5. sort_reflects_intent - Ordenacao correta

Acoes em Falha:
- filter_covers_periods: Expandir filtro automaticamente
- metric_matches_query: Alertar + sugerir
- aggregation_appropriate: Corrigir automaticamente
- baseline_defined: Inferir primeiro periodo
- sort_reflects_intent: Corrigir automaticamente
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# DEPRECATED TYPES (FASE 3)
# =============================================================================

DEPRECATED_CHART_TYPES = {
    "bar_vertical_composed": {
        "replacement": "line_composed",
        "reason": "Temporal comparisons now use line_composed (semantic type)",
        "phase": "FASE 3",
    },
    "line": {
        "replacement": "line_composed",
        "reason": "line is deprecated; use line_composed (visual variant decided by RenderSelector)",
        "phase": "FASE 2",
    },
}


# =============================================================================
# VALIDATION RESULT
# =============================================================================


class ValidationSeverity(Enum):
    """Severidade da validacao."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class ValidationIssue:
    """
    Representa um problema de validacao.

    Attributes:
        rule: Nome da regra de validacao
        severity: Severidade do problema
        message: Mensagem descritiva
        suggestion: Sugestao de correcao
        auto_fixable: Se pode ser corrigido automaticamente
        fix_applied: Se o fix foi aplicado
    """

    rule: str
    severity: ValidationSeverity
    message: str
    suggestion: Optional[str] = None
    auto_fixable: bool = False
    fix_applied: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionario."""
        return {
            "rule": self.rule,
            "severity": self.severity.value,
            "message": self.message,
            "suggestion": self.suggestion,
            "auto_fixable": self.auto_fixable,
            "fix_applied": self.fix_applied,
        }


@dataclass
class ValidationReport:
    """
    Relatorio completo de validacao.

    Attributes:
        is_valid: Se passou em todas as validacoes criticas
        issues: Lista de problemas encontrados
        fixes_applied: Numero de fixes automaticos aplicados
        summary: Resumo da validacao
    """

    is_valid: bool
    issues: List[ValidationIssue] = field(default_factory=list)
    fixes_applied: int = 0
    summary: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionario."""
        return {
            "is_valid": self.is_valid,
            "issues": [issue.to_dict() for issue in self.issues],
            "fixes_applied": self.fixes_applied,
            "summary": self.summary or self._generate_summary(),
        }

    def _generate_summary(self) -> str:
        """Gera resumo automatico."""
        total_issues = len(self.issues)
        errors = sum(1 for i in self.issues if i.severity == ValidationSeverity.ERROR)
        warnings = sum(
            1 for i in self.issues if i.severity == ValidationSeverity.WARNING
        )

        if total_issues == 0:
            return "Todas as validacoes passaram"

        return f"{total_issues} issues encontrados: {errors} errors, {warnings} warnings. {self.fixes_applied} fixes aplicados."


# =============================================================================
# DEPRECATED TYPE VALIDATOR (FASE 3)
# =============================================================================


class DeprecatedTypeValidator:
    """
    FASE 3: Valida e corrige tipos deprecated.

    Esta validação é uma camada de segurança (fail-safe) para garantir que
    tipos deprecated NUNCA cheguem ao executor. Se detectados, são
    automaticamente corrigidos e um log CRÍTICO é emitido.

    Tipos Deprecated:
    - bar_vertical_composed -> line_composed (FASE 3)
    - line -> line_composed (FASE 2)
    """

    @staticmethod
    def validate_and_fix(
        chart_type: Optional[str], state: Dict[str, Any]
    ) -> Tuple[Optional[str], List[ValidationIssue]]:
        """
        Valida se chart_type é deprecated e auto-corrige.

        Args:
            chart_type: Tipo de gráfico a validar
            state: State do grafo (para contexto de logging)

        Returns:
            (corrected_chart_type, issues)
        """
        issues = []

        if not chart_type:
            return chart_type, issues

        if chart_type in DEPRECATED_CHART_TYPES:
            deprecated_info = DEPRECATED_CHART_TYPES[chart_type]
            replacement = deprecated_info["replacement"]
            reason = deprecated_info["reason"]
            phase = deprecated_info["phase"]

            # Log CRÍTICO
            logger.critical(
                f"[DeprecatedTypeValidator] DEPRECATED TYPE DETECTED: '{chart_type}' "
                f"(should NEVER happen in {phase}). Auto-correcting to '{replacement}'."
            )

            # Criar issue
            issue = ValidationIssue(
                rule="deprecated_type_detection",
                severity=ValidationSeverity.ERROR,
                message=f"Deprecated chart type '{chart_type}' detected",
                suggestion=f"Auto-corrected to '{replacement}'. {reason}",
                auto_fixable=True,
                fix_applied=True,
            )
            issues.append(issue)

            # Retornar tipo corrigido
            return replacement, issues

        return chart_type, issues


# =============================================================================
# FILTER COVERAGE VALIDATOR
# =============================================================================


class FilterCoverageValidator:
    """
    Valida se o filtro cobre todos os periodos necessarios.

    Regra: filter_covers_periods
    Acao em falha: Expandir filtro automaticamente
    """

    @staticmethod
    def validate(
        filters: Dict[str, Any],
        temporal_comparison: Optional[Dict[str, Any]],
        intent_config: Optional[Dict[str, Any]],
    ) -> Tuple[bool, List[ValidationIssue], Dict[str, Any]]:
        """
        Valida cobertura de filtros temporais.

        Args:
            filters: Filtros atuais
            temporal_comparison: Comparacao temporal detectada
            intent_config: Configuracao do intent

        Returns:
            (is_valid, issues, fixed_filters)
        """
        issues = []
        fixed_filters = filters.copy()

        # Se nao ha comparacao temporal, filtro esta OK
        if not temporal_comparison or not temporal_comparison.get("is_comparison"):
            return True, issues, fixed_filters

        # Verificar se requer comparacao temporal
        if intent_config and intent_config.get("requires_temporal_comparison"):
            # Validar que filtro inclui TODOS os periodos
            periods = temporal_comparison.get("periods", [])
            filter_from_comparison = temporal_comparison.get("filter", {})

            if len(periods) >= 2:
                # Verificar se filtro temporal esta presente
                temporal_field = periods[0].get("field", "Mes")

                if temporal_field not in filters:
                    # ISSUE: Filtro temporal ausente
                    issue = ValidationIssue(
                        rule="filter_covers_periods",
                        severity=ValidationSeverity.ERROR,
                        message=f"Filtro temporal '{temporal_field}' ausente para comparacao entre {len(periods)} periodos",
                        suggestion=f"Adicionar filtro: {filter_from_comparison}",
                        auto_fixable=True,
                        fix_applied=False,
                    )
                    issues.append(issue)

                    # AUTO-FIX: Adicionar filtro
                    fixed_filters.update(filter_from_comparison)
                    issue.fix_applied = True

                    logger.info(
                        f"[FilterCoverageValidator] AUTO-FIX: Adicionado filtro temporal {filter_from_comparison}"
                    )

                else:
                    # Verificar se filtro cobre todos os periodos
                    current_filter_value = filters[temporal_field]
                    expected_values = [p.get("value") for p in periods]

                    # Se current_filter_value e string, converter para lista
                    if isinstance(current_filter_value, str):
                        current_values = [current_filter_value]
                    else:
                        current_values = current_filter_value

                    missing_periods = [
                        v for v in expected_values if v not in current_values
                    ]

                    if missing_periods:
                        # ISSUE: Filtro nao cobre todos os periodos
                        issue = ValidationIssue(
                            rule="filter_covers_periods",
                            severity=ValidationSeverity.WARNING,
                            message=f"Filtro temporal nao cobre todos os periodos: faltando {missing_periods}",
                            suggestion=f"Expandir filtro para incluir: {expected_values}",
                            auto_fixable=True,
                            fix_applied=False,
                        )
                        issues.append(issue)

                        # AUTO-FIX: Expandir filtro
                        fixed_filters[temporal_field] = expected_values
                        issue.fix_applied = True

                        logger.info(
                            f"[FilterCoverageValidator] AUTO-FIX: Expandido filtro temporal para {expected_values}"
                        )

        is_valid = not any(i.severity == ValidationSeverity.ERROR for i in issues)
        return is_valid, issues, fixed_filters


# =============================================================================
# METRIC INTENT VALIDATOR
# =============================================================================


class MetricIntentValidator:
    """
    Valida se a metrica corresponde ao intent.

    Regra: metric_matches_query
    Acao em falha: Alertar + sugerir

    EXPECTED_METRICS is built dynamically from alias.yaml numeric columns,
    making this validator dataset-agnostic.
    """

    # Populated on first use from alias.yaml
    _expected_metrics: Optional[Dict[str, List[str]]] = None

    @classmethod
    def _get_expected_metrics(cls) -> Dict[str, List[str]]:
        """
        Build EXPECTED_METRICS dynamically from alias.yaml numeric columns.

        All intents that expect metric validation use the full list of
        numeric columns from the current dataset.
        """
        if cls._expected_metrics is not None:
            return cls._expected_metrics

        try:
            from src.shared_lib.core.dataset_config import DatasetConfig

            numeric_cols = DatasetConfig.get_instance().numeric_columns
        except Exception:
            try:
                from src.shared_lib.core.config import get_metric_columns

                numeric_cols = get_metric_columns()
            except Exception:
                numeric_cols = []

        cls._expected_metrics = {
            "entity_ranking": list(numeric_cols),
            "temporal_comparison_analysis": list(numeric_cols),
            "temporal_trend": list(numeric_cols),
            "proportion_analysis": list(numeric_cols),
        }
        return cls._expected_metrics

    @staticmethod
    def validate(
        metrics: List[Dict[str, Any]], intent: str, query: str
    ) -> Tuple[bool, List[ValidationIssue]]:
        """
        Valida se metrica corresponde ao intent.

        Args:
            metrics: Lista de metricas
            intent: Intent detectado
            query: Query original

        Returns:
            (is_valid, issues)
        """
        issues = []

        if not metrics:
            issue = ValidationIssue(
                rule="metric_matches_query",
                severity=ValidationSeverity.ERROR,
                message="Nenhuma metrica especificada",
                suggestion="Adicionar metrica apropriada para o intent",
                auto_fixable=False,
            )
            issues.append(issue)
            return False, issues

        # Verificar se metrica faz sentido para o intent
        expected_metrics = MetricIntentValidator._get_expected_metrics().get(intent, [])

        if expected_metrics:
            metric_names = [m.get("name") for m in metrics]

            # Verificar se pelo menos uma metrica esperada esta presente
            has_expected = any(name in expected_metrics for name in metric_names)

            if not has_expected:
                issue = ValidationIssue(
                    rule="metric_matches_query",
                    severity=ValidationSeverity.WARNING,
                    message=f"Metrica pode nao corresponder ao intent '{intent}'",
                    suggestion=f"Metricas esperadas: {expected_metrics}",
                    auto_fixable=False,
                )
                issues.append(issue)

        is_valid = not any(i.severity == ValidationSeverity.ERROR for i in issues)
        return is_valid, issues


# =============================================================================
# DIMENSION STRUCTURE VALIDATOR
# =============================================================================


class DimensionStructureValidator:
    """
    Valida estrutura de dimensoes.

    Regra: dimensions_structure
    Acao em falha: Alertar
    """

    @staticmethod
    def validate(
        dimensions: List[Dict[str, Any]],
        chart_type: str,
        dimension_analysis: Optional[Dict[str, Any]],
    ) -> Tuple[bool, List[ValidationIssue]]:
        """
        Valida estrutura de dimensoes.

        Args:
            dimensions: Lista de dimensoes
            chart_type: Tipo de grafico
            dimension_analysis: Analise de dimensoes (FASE 2)

        Returns:
            (is_valid, issues)
        """
        issues = []

        # Validar numero de dimensoes por chart_type
        num_dims = len(dimensions)

        # FASE 2: line_composed now accepts 1+ dimensions (semantic type)
        # Visual variant (single_line vs multi_line) is decided by RenderSelector
        expected_dims = {
            "bar_vertical_stacked": 2,
            "line_composed": (1, 10),  # Min 1, Max 10 (flexible range)
            "bar_horizontal": 1,
            "bar_vertical": (1, 10),  # Min 1, Max 10
            "pie": 1,
        }

        if chart_type in expected_dims:
            expected = expected_dims[chart_type]

            # Handle both single value and range validation
            if isinstance(expected, tuple):
                min_dims, max_dims = expected
                if num_dims < min_dims or num_dims > max_dims:
                    issue = ValidationIssue(
                        rule="dimensions_structure",
                        severity=ValidationSeverity.WARNING,
                        message=f"Chart type '{chart_type}' espera {min_dims}-{max_dims} dimensoes, obtido {num_dims}",
                        suggestion=f"Ajustar numero de dimensoes para {min_dims}-{max_dims}",
                        auto_fixable=False,
                    )
                    issues.append(issue)
            else:
                if num_dims != expected:
                    issue = ValidationIssue(
                        rule="dimensions_structure",
                        severity=ValidationSeverity.WARNING,
                        message=f"Chart type '{chart_type}' espera {expected} dimensoes, obtido {num_dims}",
                        suggestion=f"Ajustar numero de dimensoes para {expected}",
                        auto_fixable=False,
                    )
                    issues.append(issue)

        # Validar roles (se dimension_analysis disponivel)
        if dimension_analysis and chart_type in [
            "bar_vertical_stacked",
            "line_composed",
        ]:
            # Verificar se tem primary e series definidos
            roles = {
                name: info.get("role") for name, info in dimension_analysis.items()
            }
            has_primary = "primary" in roles.values()
            has_series = "series" in roles.values()

            if not has_primary or not has_series:
                issue = ValidationIssue(
                    rule="dimensions_structure",
                    severity=ValidationSeverity.INFO,
                    message=f"Roles de dimensao nao definidos completamente (primary={has_primary}, series={has_series})",
                    suggestion="Dimension roles serao inferidos pelo formatter",
                    auto_fixable=True,
                )
                issues.append(issue)

        is_valid = not any(i.severity == ValidationSeverity.ERROR for i in issues)
        return is_valid, issues


# =============================================================================
# SORT INTENT VALIDATOR
# =============================================================================


class SortIntentValidator:
    """
    Valida se a ordenacao reflete o intent.

    Regra: sort_reflects_intent
    Acao em falha: Corrigir automaticamente
    """

    @staticmethod
    def validate(
        sort_config: Optional[Dict[str, Any]],
        intent: str,
        intent_config: Optional[Dict[str, Any]],
        sort_analysis: Optional[Dict[str, Any]],
    ) -> Tuple[bool, List[ValidationIssue], Optional[Dict[str, Any]]]:
        """
        Valida ordenacao vs intent.

        Args:
            sort_config: Configuracao de sort
            intent: Intent detectado
            intent_config: Configuracao do intent
            sort_analysis: Analise de sort (FASE 2)

        Returns:
            (is_valid, issues, fixed_sort)
        """
        issues = []
        fixed_sort = sort_config.copy() if sort_config else {}

        # Se tem sort_analysis, usar como fonte de verdade
        if sort_analysis and sort_analysis.get("sort_config"):
            expected_sort = sort_analysis["sort_config"]

            # Se sort_config nao existe ou e diferente, corrigir
            if not sort_config or sort_config != expected_sort:
                issue = ValidationIssue(
                    rule="sort_reflects_intent",
                    severity=ValidationSeverity.WARNING,
                    message=f"Sort config nao reflete intent '{intent}'",
                    suggestion=f"Usar sort: {expected_sort}",
                    auto_fixable=True,
                    fix_applied=False,
                )
                issues.append(issue)

                # AUTO-FIX: Aplicar sort correto
                fixed_sort = expected_sort
                issue.fix_applied = True

                logger.info(
                    f"[SortIntentValidator] AUTO-FIX: Aplicado sort correto {expected_sort}"
                )

        is_valid = not any(i.severity == ValidationSeverity.ERROR for i in issues)
        return is_valid, issues, fixed_sort


# =============================================================================
# BASELINE VALIDATOR
# =============================================================================


class BaselineValidator:
    """
    Valida se baseline esta definido para comparacoes.

    Regra: baseline_defined
    Acao em falha: Inferir primeiro periodo
    """

    @staticmethod
    def validate(
        temporal_comparison: Optional[Dict[str, Any]],
        calculated_field_spec: Optional[Dict[str, Any]],
    ) -> Tuple[bool, List[ValidationIssue]]:
        """
        Valida baseline para comparacao.

        Args:
            temporal_comparison: Comparacao temporal
            calculated_field_spec: Spec de campo calculado

        Returns:
            (is_valid, issues)
        """
        issues = []

        # Se ha comparacao temporal, verificar baseline
        if temporal_comparison and temporal_comparison.get("is_comparison"):
            baseline = temporal_comparison.get("baseline")

            if not baseline:
                issue = ValidationIssue(
                    rule="baseline_defined",
                    severity=ValidationSeverity.WARNING,
                    message="Baseline nao definido para comparacao temporal",
                    suggestion="Baseline sera inferido como primeiro periodo",
                    auto_fixable=True,
                )
                issues.append(issue)

        # Se ha campo calculado, verificar baseline
        if calculated_field_spec:
            baseline = calculated_field_spec.get("baseline")

            if not baseline:
                issue = ValidationIssue(
                    rule="baseline_defined",
                    severity=ValidationSeverity.INFO,
                    message="Baseline nao definido em calculated_field_spec",
                    suggestion="Baseline sera inferido automaticamente",
                    auto_fixable=True,
                )
                issues.append(issue)

        is_valid = not any(i.severity == ValidationSeverity.ERROR for i in issues)
        return is_valid, issues


# =============================================================================
# STRUCTURAL VALIDATOR (Main Interface)
# =============================================================================


class StructuralValidator:
    """
    Validador estrutural principal.

    Interface unificada para todas as validacoes estruturais da FASE 3.
    """

    def __init__(self):
        self.filter_validator = FilterCoverageValidator()
        self.metric_validator = MetricIntentValidator()
        self.dimension_validator = DimensionStructureValidator()
        self.sort_validator = SortIntentValidator()
        self.baseline_validator = BaselineValidator()

    def validate(
        self, output: Dict[str, Any], state: Dict[str, Any]
    ) -> ValidationReport:
        """
        Executa todas as validacoes estruturais.

        Args:
            output: Output JSON gerado
            state: State do grafo com metadados

        Returns:
            ValidationReport
        """
        all_issues = []
        fixes_applied = 0

        # Extrair campos necessarios
        filters = output.get("filters", {})
        metrics = output.get("metrics", [])
        dimensions = output.get("dimensions", [])
        sort_config = output.get("sort")
        chart_type = output.get("chart_type")
        intent = state.get("intent", "")

        # =====================================================================
        # VALIDAÇÃO 0: DEPRECATED TYPE DETECTION (FASE 3 - CRITICAL)
        # =====================================================================
        # Esta validação DEVE executar PRIMEIRO para garantir que nenhum
        # tipo deprecated seja processado pelas validações subsequentes
        corrected_chart_type, issues = DeprecatedTypeValidator.validate_and_fix(
            chart_type, state
        )
        all_issues.extend(issues)

        if corrected_chart_type != chart_type:
            output["chart_type"] = corrected_chart_type
            chart_type = corrected_chart_type
            fixes_applied += 1

            logger.warning(
                f"[StructuralValidator] Auto-corrected deprecated type: "
                f"{output.get('chart_type')} -> {corrected_chart_type}"
            )

        # Extrair metadados da FASE 2 e 3
        temporal_comparison = state.get("temporal_comparison")
        intent_config = state.get("intent_config")
        dimension_analysis = state.get("dimension_analysis")
        sort_analysis = state.get("sort_analysis")
        calculated_field_spec = state.get("calculated_field_spec")

        # Validacao 1: Filter Coverage
        is_valid, issues, fixed_filters = self.filter_validator.validate(
            filters, temporal_comparison, intent_config
        )
        all_issues.extend(issues)
        fixes_applied += sum(1 for i in issues if i.fix_applied)

        if fixed_filters != filters:
            output["filters"] = fixed_filters

        # Validacao 2: Metric Intent Match
        query = state.get("query", "")
        is_valid, issues = self.metric_validator.validate(metrics, intent, query)
        all_issues.extend(issues)

        # Validacao 3: Dimension Structure
        is_valid, issues = self.dimension_validator.validate(
            dimensions, chart_type, dimension_analysis
        )
        all_issues.extend(issues)

        # Validacao 4: Sort Intent Match
        is_valid, issues, fixed_sort = self.sort_validator.validate(
            sort_config, intent, intent_config, sort_analysis
        )
        all_issues.extend(issues)
        fixes_applied += sum(1 for i in issues if i.fix_applied)

        if fixed_sort != sort_config:
            output["sort"] = fixed_sort

        # Validacao 5: Baseline Defined
        is_valid, issues = self.baseline_validator.validate(
            temporal_comparison, calculated_field_spec
        )
        all_issues.extend(issues)

        # Gerar relatorio
        has_errors = any(i.severity == ValidationSeverity.ERROR for i in all_issues)
        is_valid = not has_errors

        report = ValidationReport(
            is_valid=is_valid, issues=all_issues, fixes_applied=fixes_applied
        )

        logger.info(
            f"[StructuralValidator] Validation complete: "
            f"is_valid={is_valid}, issues={len(all_issues)}, fixes={fixes_applied}"
        )

        return report


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def validate_structure(output: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Helper function para validar estrutura.

    Args:
        output: Output JSON
        state: State do grafo

    Returns:
        Relatorio de validacao (dict)
    """
    validator = StructuralValidator()
    report = validator.validate(output, state)
    return report.to_dict()
