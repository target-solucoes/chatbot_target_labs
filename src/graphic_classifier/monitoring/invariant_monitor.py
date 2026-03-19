"""
FASE 4: Monitoramento de Invariantes em Produção

Este módulo implementa o monitoramento em tempo de execução das invariantes
semânticas. Ele detecta e loga violações durante a execução do sistema,
permitindo identificar problemas que escaparam dos testes.

INVARIANTES MONITORADAS:
- I1: Temporal → line_composed
- I2: Negative polarity → asc sort
- I3: Tipo semântico (line_composed, não line)
- I4: Null safety (factual queries)
- I5: Requisitos estruturais

CASOS DE USO:
1. Detectar alucinações do LLM que violam invariantes
2. Alertar sobre heurísticas downstream que contradizem semantic anchor
3. Coletar métricas de violações para análise
4. (Futuro) Implementar fallbacks automáticos

INTEGRAÇÃO:
- Usado em runtime validators
- Logs enviados para sistema de monitoramento
- Métricas exportadas para analytics

Referências:
- graph_classifier_correction.md (Seção: Fase 4 - Monitoramento)
"""

import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime

logger = logging.getLogger(__name__)


# ============================================================================
# INVARIANT VIOLATION TYPES
# ============================================================================


class InvariantType(str, Enum):
    """Tipos de invariantes monitoradas."""

    I1_TEMPORAL = "I1_temporal_comparison"  # temporal → line_composed
    I2_POLARITY = "I2_polarity_sort"  # negative → asc
    I3_TYPE_CONSISTENCY = "I3_type_consistency"  # line_composed não line
    I4_NULL_SAFETY = "I4_null_safety"  # factual → None + message
    I5_STRUCTURAL = "I5_structural_requirements"  # line_composed → temporal dim


class SeverityLevel(str, Enum):
    """Níveis de severidade de violações."""

    CRITICAL = "CRITICAL"  # Bloqueio imediato, sistema não pode prosseguir
    HIGH = "HIGH"  # Resultado incorreto, mas sistema pode continuar
    MEDIUM = "MEDIUM"  # Potencial problema, requer investigação
    LOW = "LOW"  # Aviso, não afeta resultado


# ============================================================================
# VIOLATION RECORD
# ============================================================================


@dataclass
class InvariantViolation:
    """
    Registro de violação de invariante.

    Fields:
        invariant: Tipo da invariante violada
        severity: Nível de severidade
        message: Descrição detalhada da violação
        context: Contexto da query/state quando violação ocorreu
        timestamp: Momento da violação
        auto_fixed: Se a violação foi corrigida automaticamente
        fix_description: Descrição da correção aplicada (se houver)
    """

    invariant: InvariantType
    severity: SeverityLevel
    message: str
    context: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    auto_fixed: bool = False
    fix_description: Optional[str] = None

    def to_log_dict(self) -> Dict[str, Any]:
        """Converte para dicionário para logging estruturado."""
        return {
            "invariant": self.invariant.value,
            "severity": self.severity.value,
            "message": self.message,
            "context": self.context,
            "timestamp": self.timestamp.isoformat(),
            "auto_fixed": self.auto_fixed,
            "fix_description": self.fix_description,
        }


# ============================================================================
# INVARIANT MONITOR
# ============================================================================


class InvariantMonitor:
    """
    Monitor de invariantes em tempo de execução.

    Este monitor valida que as invariantes semânticas estão sendo
    respeitadas durante a execução do sistema. Ele detecta violações
    e loga alertas para análise.

    Usage:
        monitor = InvariantMonitor()

        # Validar mapeamento semântico
        violations = monitor.validate_semantic_mapping(
            semantic_anchor=anchor,
            chart_family="line_composed",
            sort_order="asc"
        )

        if violations:
            for v in violations:
                logger.warning(f"Invariant violation: {v.message}")
    """

    def __init__(self):
        """Initialize monitor."""
        self.violations: List[InvariantViolation] = []
        logger.info("[InvariantMonitor] Initialized for runtime validation")

    def validate_semantic_mapping(
        self,
        semantic_anchor: Any,  # SemanticAnchor
        chart_family: Optional[str],
        sort_order: Optional[str],
        requires_temporal_dimension: bool = False,
    ) -> List[InvariantViolation]:
        """
        Valida se um mapeamento semântico respeita todas as invariantes.

        Args:
            semantic_anchor: Semantic anchor extraído
            chart_family: Chart family mapeado
            sort_order: Ordenação aplicada
            requires_temporal_dimension: Se dimensão temporal é requerida

        Returns:
            Lista de violações detectadas (vazia se tudo OK)

        Example:
            >>> violations = monitor.validate_semantic_mapping(
            ...     semantic_anchor=anchor,
            ...     chart_family="pie",  # ERRADO para temporal
            ...     sort_order="desc",
            ...     requires_temporal_dimension=False
            ... )
            >>> len(violations) > 0  # True - violação detectada
        """
        violations = []

        # Get anchor fields
        semantic_goal = getattr(semantic_anchor, "semantic_goal", None)
        comparison_axis = getattr(semantic_anchor, "comparison_axis", None)
        polarity = getattr(semantic_anchor, "polarity", None)

        # ====================================================================
        # I1: TEMPORAL → line_composed
        # ====================================================================

        if comparison_axis == "temporal":
            if chart_family != "line_composed":
                violation = InvariantViolation(
                    invariant=InvariantType.I1_TEMPORAL,
                    severity=SeverityLevel.CRITICAL,
                    message=(
                        f"I1 VIOLATED: Temporal comparison_axis mapped to "
                        f"'{chart_family}' instead of 'line_composed'"
                    ),
                    context={
                        "semantic_goal": semantic_goal,
                        "comparison_axis": comparison_axis,
                        "chart_family": chart_family,
                        "query": getattr(semantic_anchor, "entity_scope", ""),
                    },
                )
                violations.append(violation)
                self.violations.append(violation)

                logger.error(
                    f"[Invariant Violation] {violation.invariant.value}: {violation.message}"
                )

        # ====================================================================
        # I2: NEGATIVE POLARITY → asc
        # ====================================================================

        if polarity == "negative":
            if sort_order != "asc":
                violation = InvariantViolation(
                    invariant=InvariantType.I2_POLARITY,
                    severity=SeverityLevel.HIGH,
                    message=(
                        f"I2 VIOLATED: Negative polarity mapped to "
                        f"sort_order='{sort_order}' instead of 'asc'"
                    ),
                    context={
                        "semantic_goal": semantic_goal,
                        "polarity": polarity,
                        "sort_order": sort_order,
                        "expected": "asc",
                    },
                )
                violations.append(violation)
                self.violations.append(violation)

                logger.warning(
                    f"[Invariant Violation] {violation.invariant.value}: {violation.message}"
                )

        # ====================================================================
        # I3: TYPE CONSISTENCY - Deprecated types
        # ====================================================================

        if chart_family in ["line", "bar_vertical_composed"]:
            violation = InvariantViolation(
                invariant=InvariantType.I3_TYPE_CONSISTENCY,
                severity=SeverityLevel.CRITICAL,
                message=(
                    f"I3 VIOLATED: Deprecated chart_family '{chart_family}' detected. "
                    f"Use 'line_composed' instead."
                ),
                context={
                    "chart_family": chart_family,
                    "replacement": "line_composed",
                    "semantic_goal": semantic_goal,
                },
            )
            violations.append(violation)
            self.violations.append(violation)

            logger.error(
                f"[Invariant Violation] {violation.invariant.value}: {violation.message}"
            )

        # ====================================================================
        # I4: NULL SAFETY - Factual queries
        # ====================================================================

        if semantic_goal == "factual":
            if chart_family is not None:
                violation = InvariantViolation(
                    invariant=InvariantType.I4_NULL_SAFETY,
                    severity=SeverityLevel.MEDIUM,
                    message=(
                        f"I4 WARNING: Factual query mapped to chart_family='{chart_family}'. "
                        f"Expected None (textual response)."
                    ),
                    context={
                        "semantic_goal": semantic_goal,
                        "chart_family": chart_family,
                        "expected": None,
                    },
                )
                violations.append(violation)
                self.violations.append(violation)

                logger.warning(
                    f"[Invariant Violation] {violation.invariant.value}: {violation.message}"
                )

        # ====================================================================
        # I5: STRUCTURAL - line_composed requires temporal
        # ====================================================================

        if chart_family == "line_composed":
            if not requires_temporal_dimension:
                violation = InvariantViolation(
                    invariant=InvariantType.I5_STRUCTURAL,
                    severity=SeverityLevel.HIGH,
                    message=(
                        f"I5 VIOLATED: line_composed without temporal dimension. "
                        f"This chart family REQUIRES temporal dimension."
                    ),
                    context={
                        "chart_family": chart_family,
                        "requires_temporal_dimension": requires_temporal_dimension,
                        "comparison_axis": comparison_axis,
                    },
                )
                violations.append(violation)
                self.violations.append(violation)

                logger.error(
                    f"[Invariant Violation] {violation.invariant.value}: {violation.message}"
                )

        # Log summary
        if violations:
            logger.warning(
                f"[InvariantMonitor] Detected {len(violations)} violation(s) "
                f"for semantic_goal={semantic_goal}"
            )
        else:
            logger.debug(
                f"[InvariantMonitor] All invariants validated successfully "
                f"for semantic_goal={semantic_goal}"
            )

        return violations

    def validate_downstream_override(
        self,
        semantic_anchor: Any,
        proposed_chart_family: str,
        override_source: str,
    ) -> Optional[InvariantViolation]:
        """
        Valida se uma heurística downstream está tentando contradizer o semantic anchor.

        Este método detecta quando componentes downstream (como o decision tree)
        tentam alterar o chart_family de forma que viole invariantes.

        Args:
            semantic_anchor: Semantic anchor original
            proposed_chart_family: Chart family proposto por downstream
            override_source: Nome do componente que propôs override

        Returns:
            InvariantViolation se houver conflito, None se OK

        Example:
            >>> # Semantic anchor diz "line_composed", decision tree propõe "pie"
            >>> violation = monitor.validate_downstream_override(
            ...     semantic_anchor=anchor,
            ...     proposed_chart_family="pie",
            ...     override_source="DecisionTree"
            ... )
            >>> assert violation is not None  # Conflito detectado
        """
        comparison_axis = getattr(semantic_anchor, "comparison_axis", None)

        # I1: Temporal não pode ser sobrescrito para tipo não-temporal
        if comparison_axis == "temporal":
            if proposed_chart_family != "line_composed":
                violation = InvariantViolation(
                    invariant=InvariantType.I1_TEMPORAL,
                    severity=SeverityLevel.CRITICAL,
                    message=(
                        f"DOWNSTREAM OVERRIDE VIOLATION: {override_source} attempted to "
                        f"override temporal comparison to '{proposed_chart_family}'. "
                        f"I1 invariant requires 'line_composed'."
                    ),
                    context={
                        "comparison_axis": comparison_axis,
                        "proposed_chart": proposed_chart_family,
                        "override_source": override_source,
                        "expected": "line_composed",
                    },
                )

                self.violations.append(violation)

                logger.error(
                    f"[Invariant Violation] {override_source} violated I1: "
                    f"temporal → {proposed_chart_family} (rejected)"
                )

                return violation

        # I3: Deprecated types não podem ser propostos
        if proposed_chart_family in ["line", "bar_vertical_composed"]:
            violation = InvariantViolation(
                invariant=InvariantType.I3_TYPE_CONSISTENCY,
                severity=SeverityLevel.CRITICAL,
                message=(
                    f"DOWNSTREAM OVERRIDE VIOLATION: {override_source} proposed "
                    f"deprecated chart_family '{proposed_chart_family}'. "
                    f"Use 'line_composed' instead."
                ),
                context={
                    "proposed_chart": proposed_chart_family,
                    "override_source": override_source,
                    "replacement": "line_composed",
                },
            )

            self.violations.append(violation)

            logger.error(
                f"[Invariant Violation] {override_source} proposed deprecated type: "
                f"{proposed_chart_family}"
            )

            return violation

        return None

    def get_violation_summary(self) -> Dict[str, int]:
        """
        Retorna resumo de violações por tipo.

        Returns:
            Dicionário com contagem de violações por invariante

        Example:
            >>> summary = monitor.get_violation_summary()
            >>> print(summary)
            {
                "I1_temporal_comparison": 2,
                "I2_polarity_sort": 1,
                "I3_type_consistency": 0,
                ...
            }
        """
        summary = {inv_type.value: 0 for inv_type in InvariantType}

        for violation in self.violations:
            summary[violation.invariant.value] += 1

        return summary

    def get_critical_violations(self) -> List[InvariantViolation]:
        """
        Retorna apenas violações CRITICAL.

        Returns:
            Lista de violações críticas

        Example:
            >>> critical = monitor.get_critical_violations()
            >>> if critical:
            ...     logger.error(f"Found {len(critical)} CRITICAL violations")
        """
        return [v for v in self.violations if v.severity == SeverityLevel.CRITICAL]

    def clear_violations(self):
        """Limpa histórico de violações."""
        logger.info(
            f"[InvariantMonitor] Clearing {len(self.violations)} violation records"
        )
        self.violations.clear()

    def export_violations_for_analytics(self) -> List[Dict[str, Any]]:
        """
        Exporta violações em formato para analytics.

        Returns:
            Lista de dicionários com dados de violações

        Usage:
            >>> violations_data = monitor.export_violations_for_analytics()
            >>> analytics_client.send_metrics(violations_data)
        """
        return [v.to_log_dict() for v in self.violations]


# ============================================================================
# GLOBAL MONITOR INSTANCE
# ============================================================================

# Singleton monitor para uso global
_global_monitor: Optional[InvariantMonitor] = None


def get_invariant_monitor() -> InvariantMonitor:
    """
    Retorna instância global do monitor de invariantes.

    Returns:
        InvariantMonitor: Instância singleton

    Example:
        >>> from src.graphic_classifier.monitoring.invariant_monitor import get_invariant_monitor
        >>> monitor = get_invariant_monitor()
        >>> violations = monitor.validate_semantic_mapping(...)
    """
    global _global_monitor

    if _global_monitor is None:
        _global_monitor = InvariantMonitor()
        logger.info("[InvariantMonitor] Global monitor initialized")

    return _global_monitor


# ============================================================================
# DECORATOR FOR AUTOMATIC VALIDATION
# ============================================================================


def validate_invariants(func):
    """
    Decorator para validar invariantes automaticamente em métodos.

    Este decorator pode ser aplicado a métodos que retornam um
    SemanticMappingResult para validar automaticamente as invariantes.

    Usage:
        @validate_invariants
        def map(self, anchor: SemanticAnchor) -> SemanticMappingResult:
            # ... implementação ...
            return result

    O decorator irá:
    1. Executar o método normalmente
    2. Validar o resultado contra as invariantes
    3. Logar violações (se houver)
    4. Retornar o resultado original
    """

    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)

        # Extrair anchor do primeiro argumento (self é args[0])
        if len(args) >= 2:
            anchor = args[1]
            monitor = get_invariant_monitor()

            violations = monitor.validate_semantic_mapping(
                semantic_anchor=anchor,
                chart_family=result.chart_family,
                sort_order=result.sort_order,
                requires_temporal_dimension=result.requires_temporal_dimension,
            )

            if violations:
                critical_count = sum(
                    1 for v in violations if v.severity == SeverityLevel.CRITICAL
                )
                logger.warning(
                    f"[{func.__name__}] Detected {len(violations)} invariant violations "
                    f"({critical_count} CRITICAL)"
                )

        return result

    return wrapper
