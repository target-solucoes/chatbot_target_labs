"""
Calculated Field Builder - FASE 3: Campos Calculados e Validacoes.

Este modulo implementa o sistema de campos calculados conforme especificado em
planning_graph_classifier_diagnosis.md - FASE 3, Etapa 3.1.

O sistema permite calcular automaticamente:
1. Variacao absoluta (current - baseline)
2. Variacao percentual ((current - baseline) / baseline * 100)
3. Diferenca entre valores
4. Taxa de crescimento
5. Baseline dinamico baseado em periodos temporais

Schema de Campos Calculados:
{
  "calculated_fields": [
    {
      "name": "variation_percent",
      "type": "growth_rate",
      "formula": "((target - baseline) / baseline) * 100",
      "dependencies": ["baseline_value", "target_value"],
      "format": "+#.0%;-#.0%",
      "metric": "Valor_Vendido",
      "baseline": {"period": "maio", "year": 2016},
      "target": {"period": "junho", "year": 2016}
    }
  ]
}
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# ENUMS AND TYPES
# =============================================================================

class CalculatedFieldType(Enum):
    """Tipos de campos calculados suportados."""
    VARIATION_ABSOLUTE = "variation_absolute"  # current - baseline
    VARIATION_PERCENT = "variation_percent"    # ((current - baseline) / baseline) * 100
    GROWTH_RATE = "growth_rate"                # Same as VARIATION_PERCENT
    DIFFERENCE = "difference"                  # value_A - value_B
    RATIO = "ratio"                           # value_A / value_B


class BaselineStrategy(Enum):
    """Estrategias para definir baseline."""
    FIRST_PERIOD = "first_period"      # Primeiro periodo da serie
    PREVIOUS_PERIOD = "previous_period"  # Periodo anterior
    SPECIFIED = "specified"              # Periodo especificado manualmente
    AVERAGE = "average"                  # Media dos periodos


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class PeriodSpec:
    """
    Especificacao de um periodo temporal.

    Attributes:
        value: Valor do periodo (ex: "maio", "2016-05", "Q1")
        field: Campo temporal (ex: "Mes", "Ano", "Trimestre")
        year: Ano associado (opcional)
    """
    value: str
    field: str
    year: Optional[int] = None

    def to_filter(self) -> Dict[str, Any]:
        """Converte para formato de filtro."""
        filter_dict = {self.field: self.value}
        if self.year:
            filter_dict["Ano"] = self.year
        return filter_dict


@dataclass
class CalculatedFieldSpec:
    """
    Especificacao completa de um campo calculado.

    Attributes:
        name: Nome do campo calculado
        type: Tipo de calculo
        formula: Formula matematica
        metric: Metrica base para o calculo
        baseline: Especificacao do periodo baseline
        target: Especificacao do periodo target
        dependencies: Campos necessarios para o calculo
        format: Formato de apresentacao
        unit: Unidade do resultado
    """
    name: str
    type: CalculatedFieldType
    formula: str
    metric: str
    baseline: Optional[PeriodSpec] = None
    target: Optional[PeriodSpec] = None
    dependencies: List[str] = field(default_factory=list)
    format: Optional[str] = None
    unit: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionario."""
        result = {
            "name": self.name,
            "type": self.type.value,
            "formula": self.formula,
            "metric": self.metric,
            "dependencies": self.dependencies,
        }

        if self.baseline:
            result["baseline"] = {
                "period": self.baseline.value,
                "field": self.baseline.field,
                "year": self.baseline.year
            }

        if self.target:
            result["target"] = {
                "period": self.target.value,
                "field": self.target.field,
                "year": self.target.year
            }

        if self.format:
            result["format"] = self.format

        if self.unit:
            result["unit"] = self.unit

        return result


# =============================================================================
# VARIATION CALCULATOR
# =============================================================================

class VariationCalculator:
    """
    Calculador de variacao absoluta.

    Calcula: current_value - baseline_value
    """

    @staticmethod
    def build_spec(
        metric: str,
        baseline: PeriodSpec,
        target: PeriodSpec,
        unit: Optional[str] = None
    ) -> CalculatedFieldSpec:
        """
        Constroi especificacao de variacao absoluta.

        Args:
            metric: Nome da metrica
            baseline: Periodo baseline
            target: Periodo target
            unit: Unidade do resultado

        Returns:
            CalculatedFieldSpec
        """
        field_name = f"{metric}_variation_absolute"
        formula = f"{metric}[target] - {metric}[baseline]"

        dependencies = [
            f"{metric}[{baseline.value}]",
            f"{metric}[{target.value}]"
        ]

        return CalculatedFieldSpec(
            name=field_name,
            type=CalculatedFieldType.VARIATION_ABSOLUTE,
            formula=formula,
            metric=metric,
            baseline=baseline,
            target=target,
            dependencies=dependencies,
            format="+#,##0.00;-#,##0.00",
            unit=unit
        )

    @staticmethod
    def calculate(baseline_value: float, target_value: float) -> float:
        """
        Calcula variacao absoluta.

        Args:
            baseline_value: Valor baseline
            target_value: Valor target

        Returns:
            Variacao absoluta
        """
        if baseline_value is None or target_value is None:
            raise ValueError("baseline_value and target_value cannot be None")

        return target_value - baseline_value


# =============================================================================
# GROWTH RATE CALCULATOR
# =============================================================================

class GrowthRateCalculator:
    """
    Calculador de taxa de crescimento (variacao percentual).

    Calcula: ((current_value - baseline_value) / baseline_value) * 100
    """

    @staticmethod
    def build_spec(
        metric: str,
        baseline: PeriodSpec,
        target: PeriodSpec
    ) -> CalculatedFieldSpec:
        """
        Constroi especificacao de taxa de crescimento.

        Args:
            metric: Nome da metrica
            baseline: Periodo baseline
            target: Periodo target

        Returns:
            CalculatedFieldSpec
        """
        field_name = f"{metric}_growth_rate"
        formula = f"(({metric}[target] - {metric}[baseline]) / {metric}[baseline]) * 100"

        dependencies = [
            f"{metric}[{baseline.value}]",
            f"{metric}[{target.value}]"
        ]

        return CalculatedFieldSpec(
            name=field_name,
            type=CalculatedFieldType.GROWTH_RATE,
            formula=formula,
            metric=metric,
            baseline=baseline,
            target=target,
            dependencies=dependencies,
            format="+#.0%;-#.0%",
            unit="%"
        )

    @staticmethod
    def calculate(baseline_value: float, target_value: float) -> float:
        """
        Calcula taxa de crescimento.

        Args:
            baseline_value: Valor baseline
            target_value: Valor target

        Returns:
            Taxa de crescimento em percentual

        Raises:
            ValueError: Se baseline_value for zero
        """
        if baseline_value is None or target_value is None:
            raise ValueError("baseline_value and target_value cannot be None")

        if baseline_value == 0:
            raise ValueError("baseline_value cannot be zero for growth_rate calculation")

        return ((target_value - baseline_value) / baseline_value) * 100


# =============================================================================
# DIFFERENCE CALCULATOR
# =============================================================================

class DifferenceCalculator:
    """
    Calculador de diferenca simples.

    Calcula: value_A - value_B
    """

    @staticmethod
    def build_spec(
        metric: str,
        period_a: PeriodSpec,
        period_b: PeriodSpec,
        unit: Optional[str] = None
    ) -> CalculatedFieldSpec:
        """
        Constroi especificacao de diferenca.

        Args:
            metric: Nome da metrica
            period_a: Primeiro periodo
            period_b: Segundo periodo
            unit: Unidade do resultado

        Returns:
            CalculatedFieldSpec
        """
        field_name = f"{metric}_difference"
        formula = f"{metric}[A] - {metric}[B]"

        dependencies = [
            f"{metric}[{period_a.value}]",
            f"{metric}[{period_b.value}]"
        ]

        return CalculatedFieldSpec(
            name=field_name,
            type=CalculatedFieldType.DIFFERENCE,
            formula=formula,
            metric=metric,
            baseline=period_b,  # period_b e o baseline
            target=period_a,    # period_a e o target
            dependencies=dependencies,
            format="+#,##0.00;-#,##0.00",
            unit=unit
        )

    @staticmethod
    def calculate(value_a: float, value_b: float) -> float:
        """
        Calcula diferenca.

        Args:
            value_a: Primeiro valor
            value_b: Segundo valor

        Returns:
            Diferenca
        """
        if value_a is None or value_b is None:
            raise ValueError("value_a and value_b cannot be None")

        return value_a - value_b


# =============================================================================
# BASELINE DETECTOR
# =============================================================================

class BaselineDetector:
    """
    Detector de baseline dinamico.

    Identifica automaticamente qual periodo deve ser usado como baseline
    baseado na query e nos periodos detectados.
    """

    @staticmethod
    def detect_from_query(
        query: str,
        periods: List[PeriodSpec],
        strategy: BaselineStrategy = BaselineStrategy.FIRST_PERIOD
    ) -> Optional[PeriodSpec]:
        """
        Detecta baseline a partir da query.

        Args:
            query: Query do usuario
            periods: Lista de periodos detectados
            strategy: Estrategia de deteccao

        Returns:
            PeriodSpec do baseline ou None
        """
        if not periods:
            return None

        # Estrategia: primeiro periodo
        if strategy == BaselineStrategy.FIRST_PERIOD:
            return periods[0]

        # Estrategia: periodo anterior
        if strategy == BaselineStrategy.PREVIOUS_PERIOD:
            # Assumir que os periodos estao ordenados
            # O baseline e o penultimo periodo
            return periods[-2] if len(periods) >= 2 else periods[0]

        # Estrategia: especificado (detectar keywords)
        if strategy == BaselineStrategy.SPECIFIED:
            # Detectar keywords como "de", "desde", "a partir de"
            query_lower = query.lower()

            if "de " in query_lower:
                # Extrair periodo apos "de"
                # Exemplo: "de maio para junho" -> baseline = maio
                # Isso ja foi processado em periods, entao baseline e o primeiro
                return periods[0]

        return periods[0]  # Default: primeiro periodo

    @staticmethod
    def detect_target(
        periods: List[PeriodSpec],
        baseline: PeriodSpec
    ) -> Optional[PeriodSpec]:
        """
        Detecta periodo target baseado no baseline.

        Args:
            periods: Lista de periodos
            baseline: Periodo baseline

        Returns:
            PeriodSpec do target ou None
        """
        if len(periods) < 2:
            return None

        # Target e o periodo que nao e o baseline
        for period in periods:
            if period.value != baseline.value or period.field != baseline.field:
                return period

        # Se todos sao iguais, target e o ultimo
        return periods[-1]


# =============================================================================
# CALCULATED FIELD BUILDER (Main Interface)
# =============================================================================

class CalculatedFieldBuilder:
    """
    Builder principal para campos calculados.

    Interface unificada para construir especificacoes de campos calculados
    baseado no intent, sort_config e periodos temporais detectados.
    """

    def __init__(self):
        self.variation_calculator = VariationCalculator()
        self.growth_rate_calculator = GrowthRateCalculator()
        self.difference_calculator = DifferenceCalculator()
        self.baseline_detector = BaselineDetector()

    def build_from_sort_analysis(
        self,
        sort_analysis: Dict[str, Any],
        metric: str,
        periods: List[PeriodSpec],
        unit: Optional[str] = None
    ) -> Optional[CalculatedFieldSpec]:
        """
        Constroi campo calculado a partir do sort_analysis.

        Args:
            sort_analysis: Resultado do SortManager
            metric: Nome da metrica
            periods: Lista de periodos temporais
            unit: Unidade da metrica

        Returns:
            CalculatedFieldSpec ou None
        """
        if not sort_analysis or not sort_analysis.get("requires_calculated_field"):
            return None

        calculated_field_type = sort_analysis.get("calculated_field_type")

        if not calculated_field_type or len(periods) < 2:
            logger.warning(
                f"[CalculatedFieldBuilder] Cannot build calculated field: "
                f"type={calculated_field_type}, periods={len(periods)}"
            )
            return None

        # Detectar baseline e target
        baseline = self.baseline_detector.detect_from_query(
            "", periods, BaselineStrategy.FIRST_PERIOD
        )
        target = self.baseline_detector.detect_target(periods, baseline)

        if not baseline or not target:
            logger.warning(
                f"[CalculatedFieldBuilder] Could not detect baseline/target"
            )
            return None

        # Construir spec baseado no tipo
        if calculated_field_type == "variation":
            spec = self.variation_calculator.build_spec(
                metric, baseline, target, unit
            )
        elif calculated_field_type in ["growth_rate", "variation_percent"]:
            spec = self.growth_rate_calculator.build_spec(
                metric, baseline, target
            )
        elif calculated_field_type == "difference":
            spec = self.difference_calculator.build_spec(
                metric, baseline, target, unit
            )
        else:
            logger.warning(
                f"[CalculatedFieldBuilder] Unknown calculated_field_type: {calculated_field_type}"
            )
            return None

        logger.info(
            f"[CalculatedFieldBuilder] Built calculated field: "
            f"name={spec.name}, type={spec.type.value}, "
            f"baseline={baseline.value}, target={target.value}"
        )

        return spec

    def build_from_intent_config(
        self,
        intent_config: Dict[str, Any],
        metric: str,
        periods: List[PeriodSpec],
        unit: Optional[str] = None
    ) -> Optional[CalculatedFieldSpec]:
        """
        Constroi campo calculado a partir do intent_config.

        Args:
            intent_config: Configuracao do intent
            metric: Nome da metrica
            periods: Lista de periodos temporais
            unit: Unidade da metrica

        Returns:
            CalculatedFieldSpec ou None
        """
        if not intent_config or not intent_config.get("requires_calculated_fields"):
            return None

        if len(periods) < 2:
            logger.warning(
                f"[CalculatedFieldBuilder] Cannot build calculated field: "
                f"requires 2+ periods, got {len(periods)}"
            )
            return None

        # Detectar baseline e target
        baseline = self.baseline_detector.detect_from_query(
            "", periods, BaselineStrategy.FIRST_PERIOD
        )
        target = self.baseline_detector.detect_target(periods, baseline)

        if not baseline or not target:
            return None

        # Para intents de comparacao temporal, usar variacao absoluta como padrao
        spec = self.variation_calculator.build_spec(
            metric, baseline, target, unit
        )

        logger.info(
            f"[CalculatedFieldBuilder] Built calculated field from intent_config: "
            f"name={spec.name}, type={spec.type.value}"
        )

        return spec


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def build_calculated_field_spec(
    sort_analysis: Dict[str, Any],
    metric: str,
    periods: List[Dict[str, Any]],
    unit: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Helper function para construir spec de campo calculado.

    Args:
        sort_analysis: Resultado do SortManager
        metric: Nome da metrica
        periods: Lista de periodos (dicts)
        unit: Unidade da metrica

    Returns:
        Dict com spec ou None
    """
    # Converter periods de dict para PeriodSpec
    period_specs = []
    for p in periods:
        if isinstance(p, dict):
            period_spec = PeriodSpec(
                value=p.get("value", ""),
                field=p.get("field", ""),
                year=p.get("year")
            )
            period_specs.append(period_spec)

    builder = CalculatedFieldBuilder()
    spec = builder.build_from_sort_analysis(sort_analysis, metric, period_specs, unit)

    return spec.to_dict() if spec else None


def calculate_variation_absolute(baseline: float, target: float) -> float:
    """Helper para calcular variacao absoluta."""
    return VariationCalculator.calculate(baseline, target)


def calculate_growth_rate(baseline: float, target: float) -> float:
    """Helper para calcular taxa de crescimento."""
    return GrowthRateCalculator.calculate(baseline, target)


def calculate_difference(value_a: float, value_b: float) -> float:
    """Helper para calcular diferenca."""
    return DifferenceCalculator.calculate(value_a, value_b)
