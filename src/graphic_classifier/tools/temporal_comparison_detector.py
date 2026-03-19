"""
Temporal Comparison Detector - FASE 3: Sistema de Comparacao Temporal.

Este modulo implementa a deteccao e processamento de comparacoes temporais
conforme especificado em planning_graph_classifier_diagnosis.md - FASE 3, Etapa 3.2.

O sistema detecta:
1. Padroes "entre X e Y", "de X para Y"
2. Periodos especificos (maio 2016, junho 2016)
3. Valida se periodos sao consecutivos
4. Gera filtros corretos (TODOS os periodos, nao apenas o ultimo)

Schema de Comparacao Temporal:
{
  "temporal_comparison": {
    "is_comparison": true,
    "pattern": "de_para",
    "periods": [
      {"value": "maio", "field": "Mes", "year": 2016},
      {"value": "junho", "field": "Mes", "year": 2016}
    ],
    "is_consecutive": true,
    "baseline": {"value": "maio", "field": "Mes", "year": 2016},
    "target": {"value": "junho", "field": "Mes", "year": 2016},
    "filter": {"Mes": ["maio", "junho"], "Ano": 2016}
  }
}
"""

import re
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class TemporalPeriod:
    """
    Representa um periodo temporal.

    Attributes:
        value: Valor do periodo (ex: "maio", "2016-05", "Q1")
        field: Campo temporal (ex: "Mes", "Ano", "Trimestre")
        year: Ano associado (opcional)
        month_index: Indice do mes (1-12) para validacao consecutiva
    """
    value: str
    field: str
    year: Optional[int] = None
    month_index: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionario."""
        result = {"value": self.value, "field": self.field}
        if self.year:
            result["year"] = self.year
        if self.month_index:
            result["month_index"] = self.month_index
        return result


@dataclass
class TemporalComparison:
    """
    Representa uma comparacao temporal detectada.

    Attributes:
        is_comparison: Se e uma comparacao temporal
        pattern: Tipo de pattern ("entre_e", "de_para", "vs", etc.)
        periods: Lista de periodos envolvidos
        is_consecutive: Se os periodos sao consecutivos
        baseline: Periodo baseline
        target: Periodo target
        filter: Filtro gerado (incluindo TODOS os periodos)
    """
    is_comparison: bool
    pattern: Optional[str] = None
    periods: List[TemporalPeriod] = field(default_factory=list)
    is_consecutive: bool = False
    baseline: Optional[TemporalPeriod] = None
    target: Optional[TemporalPeriod] = None
    filter: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionario."""
        return {
            "is_comparison": self.is_comparison,
            "pattern": self.pattern,
            "periods": [p.to_dict() for p in self.periods],
            "is_consecutive": self.is_consecutive,
            "baseline": self.baseline.to_dict() if self.baseline else None,
            "target": self.target.to_dict() if self.target else None,
            "filter": self.filter
        }


# =============================================================================
# MONTH MAPPER
# =============================================================================

class MonthMapper:
    """Mapeamento de meses para indices."""

    MONTHS_PT = {
        "janeiro": 1,
        "fevereiro": 2,
        "marco": 3,
        "abril": 4,
        "maio": 5,
        "junho": 6,
        "julho": 7,
        "agosto": 8,
        "setembro": 9,
        "outubro": 10,
        "novembro": 11,
        "dezembro": 12,
    }

    MONTHS_PT_ABBR = {
        "jan": 1,
        "fev": 2,
        "mar": 3,
        "abr": 4,
        "mai": 5,
        "jun": 6,
        "jul": 7,
        "ago": 8,
        "set": 9,
        "out": 10,
        "nov": 11,
        "dez": 12,
    }

    @classmethod
    def get_month_index(cls, month_name: str) -> Optional[int]:
        """Retorna indice do mes (1-12)."""
        month_lower = month_name.lower()

        # Tentar nome completo
        if month_lower in cls.MONTHS_PT:
            return cls.MONTHS_PT[month_lower]

        # Tentar abreviacao
        if month_lower in cls.MONTHS_PT_ABBR:
            return cls.MONTHS_PT_ABBR[month_lower]

        # Tentar numero
        try:
            index = int(month_name)
            if 1 <= index <= 12:
                return index
        except ValueError:
            pass

        return None

    @classmethod
    def are_consecutive(cls, month1: str, month2: str) -> bool:
        """Verifica se dois meses sao consecutivos."""
        idx1 = cls.get_month_index(month1)
        idx2 = cls.get_month_index(month2)

        if idx1 is None or idx2 is None:
            return False

        # Consecutivos: diferenca de 1
        return abs(idx2 - idx1) == 1


# =============================================================================
# TEMPORAL PATTERN DETECTOR
# =============================================================================

class TemporalPatternDetector:
    """
    Detector de padroes de comparacao temporal.

    Detecta patterns como:
    - "entre X e Y"
    - "de X para Y"
    - "X vs Y"
    - "comparar X com Y"
    """

    # Patterns regex
    PATTERNS = {
        "entre_e": [
            r"entre\s+(\w+)\s+e\s+(\w+)",
            r"entre\s+(\w+)\s+de\s+(\d{4})\s+e\s+(\w+)\s+de\s+(\d{4})",
        ],
        "de_para": [
            r"de\s+(\w+)\s+para\s+(\w+)",
            r"de\s+(\w+)\s+de\s+(\d{4})\s+para\s+(\w+)\s+de\s+(\d{4})",
        ],
        "vs": [
            r"(\w+)\s+vs\s+(\w+)",
            r"(\w+)\s+versus\s+(\w+)",
        ],
        "comparar": [
            r"comparar\s+(\w+)\s+com\s+(\w+)",
            r"comparar\s+(\w+)\s+e\s+(\w+)",
            r"comparando\s+(\w+)\s+e\s+(\w+)",
            r"comparando\s+(\w+)\s+com\s+(\w+)",
        ],
    }

    @staticmethod
    def detect(query: str) -> Optional[Tuple[str, List[str]]]:
        """
        Detecta pattern de comparacao temporal.

        Args:
            query: Query do usuario

        Returns:
            (pattern_name, [extracted_periods]) ou None
        """
        query_lower = query.lower()

        for pattern_name, regexes in TemporalPatternDetector.PATTERNS.items():
            for regex in regexes:
                match = re.search(regex, query_lower)
                if match:
                    periods = list(match.groups())
                    logger.debug(
                        f"[TemporalPatternDetector] Pattern '{pattern_name}' detected: {periods}"
                    )
                    return pattern_name, periods

        return None


# =============================================================================
# TEMPORAL PERIOD EXTRACTOR
# =============================================================================

class TemporalPeriodExtractor:
    """
    Extrator de periodos temporais da query.

    Extrai periodos com contexto completo (valor, campo, ano).
    """

    @staticmethod
    def extract(query: str, parsed_entities: Optional[Dict] = None) -> List[TemporalPeriod]:
        """
        Extrai periodos temporais da query.

        Args:
            query: Query do usuario
            parsed_entities: Entidades parseadas (opcional)

        Returns:
            Lista de TemporalPeriod
        """
        periods = []

        # Obter anos do parsed_entities
        years = []
        if parsed_entities and "years" in parsed_entities:
            years = parsed_entities["years"]

        # Extrair meses
        months_found = TemporalPeriodExtractor._extract_months(query)

        # Criar TemporalPeriod para cada mes
        for i, month in enumerate(months_found):
            year = years[i] if i < len(years) else (years[0] if years else None)
            month_index = MonthMapper.get_month_index(month)

            period = TemporalPeriod(
                value=month,
                field="Mes",
                year=year,
                month_index=month_index
            )
            periods.append(period)

        logger.debug(
            f"[TemporalPeriodExtractor] Extracted {len(periods)} periods: "
            f"{[f'{p.value}/{p.year}' for p in periods]}"
        )

        return periods

    @staticmethod
    def _extract_months(query: str) -> List[str]:
        """Extrai nomes de meses da query."""
        from unidecode import unidecode

        query_lower = query.lower()
        query_normalized = unidecode(query_lower)
        months_found = []

        # Lista de meses em portugues
        all_months = list(MonthMapper.MONTHS_PT.keys())

        for month in all_months:
            # Verificar tanto na query original quanto na normalizada (sem acentos)
            if month in query_lower or month in query_normalized:
                months_found.append(month)

        return months_found


# =============================================================================
# TEMPORAL COMPARISON DETECTOR (Main Interface)
# =============================================================================

class TemporalComparisonDetector:
    """
    Detector principal de comparacoes temporais.

    Interface unificada para:
    1. Detectar pattern de comparacao
    2. Extrair periodos
    3. Validar consecutividade
    4. Gerar filtros corretos (TODOS os periodos)
    """

    def __init__(self):
        self.pattern_detector = TemporalPatternDetector()
        self.period_extractor = TemporalPeriodExtractor()
        self.month_mapper = MonthMapper()

    def detect(
        self,
        query: str,
        parsed_entities: Optional[Dict] = None
    ) -> TemporalComparison:
        """
        Detecta comparacao temporal na query.

        Args:
            query: Query do usuario
            parsed_entities: Entidades parseadas

        Returns:
            TemporalComparison
        """
        # Detectar pattern
        pattern_result = self.pattern_detector.detect(query)

        if not pattern_result:
            # Nao e uma comparacao temporal explicita
            return TemporalComparison(is_comparison=False)

        pattern_name, _ = pattern_result

        # Extrair periodos
        periods = self.period_extractor.extract(query, parsed_entities)

        if len(periods) < 2:
            logger.warning(
                f"[TemporalComparisonDetector] Pattern detected but < 2 periods found"
            )
            return TemporalComparison(is_comparison=False)

        # Validar consecutividade (para meses)
        is_consecutive = self._check_consecutive(periods)

        # Determinar baseline e target
        baseline = periods[0]
        target = periods[1] if len(periods) >= 2 else periods[0]

        # Gerar filtro (INCLUINDO TODOS OS PERIODOS)
        filter_dict = self._generate_filter(periods)

        comparison = TemporalComparison(
            is_comparison=True,
            pattern=pattern_name,
            periods=periods,
            is_consecutive=is_consecutive,
            baseline=baseline,
            target=target,
            filter=filter_dict
        )

        logger.info(
            f"[TemporalComparisonDetector] Comparison detected: "
            f"pattern={pattern_name}, periods={len(periods)}, "
            f"consecutive={is_consecutive}, filter={filter_dict}"
        )

        return comparison

    def _check_consecutive(self, periods: List[TemporalPeriod]) -> bool:
        """Verifica se periodos sao consecutivos."""
        if len(periods) < 2:
            return False

        # Verificar apenas os dois primeiros periodos
        period1 = periods[0]
        period2 = periods[1]

        if period1.field == "Mes" and period2.field == "Mes":
            return self.month_mapper.are_consecutive(period1.value, period2.value)

        # Para outros tipos de periodo, nao validar
        return False

    def _generate_filter(self, periods: List[TemporalPeriod]) -> Dict[str, Any]:
        """
        Gera filtro incluindo TODOS os periodos.

        IMPORTANTE: Este e o fix para o problema do diagnostico.
        Anteriormente, apenas o ultimo periodo era filtrado.
        Agora, TODOS os periodos sao incluidos no filtro.
        """
        if not periods:
            return {}

        # Agrupar periodos por campo
        periods_by_field = {}
        years = set()

        for period in periods:
            field = period.field
            if field not in periods_by_field:
                periods_by_field[field] = []

            periods_by_field[field].append(period.value)

            if period.year:
                years.add(period.year)

        # Construir filtro
        filter_dict = {}

        for field, values in periods_by_field.items():
            if len(values) == 1:
                filter_dict[field] = values[0]
            else:
                # Multiplos valores: usar lista (IN clause)
                filter_dict[field] = values

        # Adicionar ano se todos os periodos tem o mesmo ano
        if len(years) == 1:
            filter_dict["Ano"] = list(years)[0]

        logger.debug(
            f"[TemporalComparisonDetector] Generated filter: {filter_dict}"
        )

        return filter_dict


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def detect_temporal_comparison(
    query: str,
    parsed_entities: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Helper function para detectar comparacao temporal.

    Args:
        query: Query do usuario
        parsed_entities: Entidades parseadas

    Returns:
        Dicionario com resultado da deteccao
    """
    detector = TemporalComparisonDetector()
    comparison = detector.detect(query, parsed_entities)
    return comparison.to_dict()


def generate_temporal_filter(
    periods: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Helper function para gerar filtro temporal.

    Args:
        periods: Lista de periodos (dicts)

    Returns:
        Filtro temporal
    """
    temporal_periods = []
    for p in periods:
        if isinstance(p, dict):
            period = TemporalPeriod(
                value=p.get("value", ""),
                field=p.get("field", "Mes"),
                year=p.get("year"),
                month_index=p.get("month_index")
            )
            temporal_periods.append(period)

    detector = TemporalComparisonDetector()
    return detector._generate_filter(temporal_periods)
