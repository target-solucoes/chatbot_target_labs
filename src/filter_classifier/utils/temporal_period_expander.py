"""
Temporal Period Expander - FASE 1.1 (Etapas 1.1.1 e 1.1.2)

Este modulo implementa a logica para expandir filtros temporais que cobrem
multiplos periodos mencionados em queries de comparacao.

Problema Original:
- Query: "maio de 2016 para junho de 2016"
- Filtro atual: ["2016-06-01", "2016-06-30"]  # Apenas junho
- Filtro esperado: ["2016-05-01", "2016-06-30"]  # Maio E junho

Referencia: planning_graph_classifier_diagnosis.md - FASE 1, Etapa 1.1
"""

import re
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)


# Mapeamento de meses em portugues
MONTH_NAMES_PT = {
    "janeiro": 1,
    "jan": 1,
    "fevereiro": 2,
    "fev": 2,
    "marco": 3,
    "mar": 3,
    "março": 3,
    "abril": 4,
    "abr": 4,
    "maio": 5,
    "mai": 5,
    "junho": 6,
    "jun": 6,
    "julho": 7,
    "jul": 7,
    "agosto": 8,
    "ago": 8,
    "setembro": 9,
    "set": 9,
    "outubro": 10,
    "out": 10,
    "novembro": 11,
    "nov": 11,
    "dezembro": 12,
    "dez": 12,
}


class TemporalPeriodExpander:
    """
    Expande filtros temporais para cobrir todos os periodos mencionados.

    Esta classe implementa a logica descrita em planning_graph_classifier_diagnosis.md
    FASE 1 - Etapas 1.1.1 e 1.1.2:
    - Parser de periodos multiplos
    - Logica de expansao de filtros temporais
    """

    def __init__(self):
        """Inicializa o expansor com padroes de deteccao."""

        # Padroes de comparacao temporal
        self.comparison_patterns = [
            # Formato: "de X para Y"
            r"de\s+(\w+(?:\s+de)?\s+\d{4})\s+(?:para|a)\s+(\w+(?:\s+de)?\s+\d{4})",
            # Formato: "entre X e Y"
            r"entre\s+(\w+(?:\s+de)?\s+\d{4})\s+e\s+(\w+(?:\s+de)?\s+\d{4})",
            # Formato: "X vs Y" ou "X versus Y"
            r"(\w+(?:\s+de)?\s+\d{4})\s+(?:vs|versus)\s+(\w+(?:\s+de)?\s+\d{4})",
            # Formato: "comparar X com Y"
            r"comparar\s+(\w+(?:\s+de)?\s+\d{4})\s+com\s+(\w+(?:\s+de)?\s+\d{4})",
            # FASE 1 - NOVOS PADRÕES: Variações sem ano repetido
            # "de maio para junho de 2016" ou "de maio para junho 2016"
            r"de\s+(\w+)\s+(?:para|a|até)\s+(\w+)\s+(?:de\s+)?(\d{4})",
            # "entre maio e junho de 2016"
            r"entre\s+(\w+)\s+e\s+(\w+)\s+de\s+(\d{4})",
            # "comparar maio com junho de 2016"
            r"comparar\s+(\w+)\s+com\s+(\w+)\s+de\s+(\d{4})",
            # "maio vs junho de 2016" ou "maio versus junho de 2016"
            r"(\w+)\s+(?:vs|versus)\s+(\w+)\s+de\s+(\d{4})",
        ]

    def extract_temporal_periods(self, query: str) -> List[Tuple[int, int]]:
        """
        Extrai todos os periodos temporais (mes, ano) mencionados na query.

        Args:
            query: Query do usuario

        Returns:
            Lista de tuplas (mes, ano) detectadas

        Examples:
            >>> expander = TemporalPeriodExpander()
            >>> expander.extract_temporal_periods("maio de 2016 para junho de 2016")
            [(5, 2016), (6, 2016)]
        """
        periods = []
        query_lower = query.lower()

        # Detectar padroes de comparacao primeiro
        for pattern in self.comparison_patterns:
            matches = re.finditer(pattern, query_lower)
            for match in matches:
                period1_str = match.group(1)
                period2_str = match.group(2)

                # FASE 1 - CORREÇÃO: Verificar se há um terceiro grupo (ano compartilhado)
                shared_year = None
                if match.lastindex >= 3:
                    try:
                        shared_year = int(match.group(3))
                    except (ValueError, IndexError):
                        pass

                period1 = self._parse_period_string(
                    period1_str, default_year=shared_year
                )
                period2 = self._parse_period_string(
                    period2_str, default_year=shared_year
                )

                if period1:
                    periods.append(period1)
                if period2:
                    periods.append(period2)

        # Se nao encontrou padroes de comparacao, buscar mencoes individuais
        if not periods:
            periods = self._extract_individual_periods(query_lower)

        # Remover duplicatas preservando ordem
        seen = set()
        unique_periods = []
        for period in periods:
            if period not in seen:
                seen.add(period)
                unique_periods.append(period)

        logger.debug(
            f"[TemporalPeriodExpander] Extracted {len(unique_periods)} periods: {unique_periods}"
        )
        return unique_periods

    def _parse_period_string(
        self, period_str: str, default_year: Optional[int] = None
    ) -> Optional[Tuple[int, int]]:
        """
        Parse uma string de periodo (ex: "maio de 2016", "jun 2016", "maio") em (mes, ano).

        Args:
            period_str: String contendo mes e opcionalmente ano
            default_year: Ano padrão a usar se não encontrado na string

        Returns:
            Tupla (mes, ano) ou None se nao puder parsear
        """
        period_str = period_str.strip()

        # Extrair ano (4 digitos)
        year_match = re.search(r"(\d{4})", period_str)
        if year_match:
            year = int(year_match.group(1))
        elif default_year:
            year = default_year
        else:
            return None

        # Extrair mes (nome em portugues)
        month = None
        for month_name, month_num in MONTH_NAMES_PT.items():
            if month_name in period_str:
                month = month_num
                break

        if not month:
            return None

        return (month, year)

    def _extract_individual_periods(self, query: str) -> List[Tuple[int, int]]:
        """
        Extrai periodos individuais mencionados (sem padroes de comparacao).

        Args:
            query: Query normalizada

        Returns:
            Lista de tuplas (mes, ano)
        """
        periods = []

        # Buscar mencoes de mes + ano
        # Formato: "maio 2016", "maio de 2016"
        pattern = r"(" + "|".join(MONTH_NAMES_PT.keys()) + r")(?:\s+de)?\s+(\d{4})"
        matches = re.finditer(pattern, query)

        for match in matches:
            month_name = match.group(1)
            year = int(match.group(2))
            month = MONTH_NAMES_PT[month_name]
            periods.append((month, year))

        return periods

    def expand_date_filter(
        self, current_filter: Any, query: str
    ) -> Optional[List[str]]:
        """
        Expande filtro de data para cobrir todos os periodos mencionados na query.

        Esta funcao implementa a Etapa 1.1.2 do planejamento:
        "Criar logica de expansao de filtros temporais"

        Args:
            current_filter: Filtro atual (pode ser lista, dict, ou valor unico)
            query: Query original do usuario

        Returns:
            Lista com [data_inicio, data_fim] expandida ou None se nao for necessario

        Examples:
            >>> expander = TemporalPeriodExpander()
            >>> expander.expand_date_filter(
            ...     ["2016-06-01", "2016-06-30"],
            ...     "maio de 2016 para junho de 2016"
            ... )
            ["2016-05-01", "2016-06-30"]
        """
        # Extrair periodos mencionados na query
        periods = self.extract_temporal_periods(query)

        if not periods or len(periods) < 2:
            # Nao ha comparacao de multiplos periodos
            logger.debug("[TemporalPeriodExpander] No multi-period comparison detected")
            return None

        # Determinar range completo
        min_month, min_year = min(periods, key=lambda p: (p[1], p[0]))
        max_month, max_year = max(periods, key=lambda p: (p[1], p[0]))

        # Construir data de inicio (primeiro dia do mes inicial)
        start_date = datetime(min_year, min_month, 1)
        start_date_str = start_date.strftime("%Y-%m-%d")

        # Construir data de fim (ultimo dia do mes final)
        # Adicionar 1 mes e subtrair 1 dia para obter ultimo dia do mes
        end_date = (
            datetime(max_year, max_month, 1)
            + relativedelta(months=1)
            - timedelta(days=1)
        )
        end_date_str = end_date.strftime("%Y-%m-%d")

        expanded_filter = [start_date_str, end_date_str]

        logger.info(
            f"[TemporalPeriodExpander] Expanded filter from {periods} "
            f"to [{start_date_str}, {end_date_str}]"
        )

        return expanded_filter

    def validate_period_coverage(
        self, date_filter: List[str], query: str
    ) -> Dict[str, Any]:
        """
        Valida se o filtro de data cobre todos os periodos mencionados na query.

        Esta funcao implementa a Etapa 1.1.3 do planejamento:
        "Validar cobertura completa de periodos mencionados"

        Args:
            date_filter: Filtro de data atual [start, end]
            query: Query original

        Returns:
            Dict com resultado da validacao:
            {
                "is_valid": bool,
                "missing_periods": List[Tuple[int, int]],
                "covered_periods": List[Tuple[int, int]],
                "confidence": float
            }
        """
        result = {
            "is_valid": True,
            "missing_periods": [],
            "covered_periods": [],
            "confidence": 1.0,
        }

        # Extrair periodos mencionados
        mentioned_periods = self.extract_temporal_periods(query)

        if not mentioned_periods:
            logger.debug("[TemporalPeriodExpander] No periods mentioned in query")
            return result

        # Parse filtro de data
        if not date_filter or len(date_filter) != 2:
            result["is_valid"] = False
            result["confidence"] = 0.0
            result["missing_periods"] = mentioned_periods
            return result

        try:
            filter_start = datetime.strptime(date_filter[0], "%Y-%m-%d")
            filter_end = datetime.strptime(date_filter[1], "%Y-%m-%d")
        except (ValueError, IndexError) as e:
            logger.error(f"[TemporalPeriodExpander] Invalid date filter format: {e}")
            result["is_valid"] = False
            result["confidence"] = 0.0
            return result

        # Verificar cobertura de cada periodo mencionado
        for month, year in mentioned_periods:
            period_start = datetime(year, month, 1)
            period_end = (
                datetime(year, month, 1) + relativedelta(months=1) - timedelta(days=1)
            )

            # Verificar se periodo esta dentro do range do filtro
            if filter_start <= period_start and period_end <= filter_end:
                result["covered_periods"].append((month, year))
            else:
                result["missing_periods"].append((month, year))
                result["is_valid"] = False

        # Calcular confianca baseado na cobertura
        if mentioned_periods:
            coverage_ratio = len(result["covered_periods"]) / len(mentioned_periods)
            result["confidence"] = coverage_ratio

        logger.info(
            f"[TemporalPeriodExpander] Validation: "
            f"is_valid={result['is_valid']}, "
            f"coverage={len(result['covered_periods'])}/{len(mentioned_periods)}, "
            f"confidence={result['confidence']:.2f}"
        )

        return result


def expand_temporal_filters(filters: Dict[str, Any], query: str) -> Dict[str, Any]:
    """
    Funcao helper para expandir filtros temporais em um dicionario de filtros.

    Esta funcao pode ser integrada no workflow existente do filter_classifier.

    Args:
        filters: Dicionario de filtros atual
        query: Query original

    Returns:
        Dicionario de filtros com expansao temporal aplicada

    Examples:
        >>> filters = {"Data": ["2016-06-01", "2016-06-30"]}
        >>> query = "maio de 2016 para junho de 2016"
        >>> expand_temporal_filters(filters, query)
        {"Data": ["2016-05-01", "2016-06-30"]}
    """
    if not filters or "Data" not in filters:
        return filters

    expander = TemporalPeriodExpander()

    # Tentar expandir filtro de Data
    expanded = expander.expand_date_filter(filters["Data"], query)

    if expanded:
        logger.info(
            f"[expand_temporal_filters] Expanded Data filter: "
            f"{filters['Data']} -> {expanded}"
        )
        filters["Data"] = expanded

    return filters
