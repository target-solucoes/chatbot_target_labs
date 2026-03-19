"""
RelativeTemporalResolver - Resolução de referências temporais relativas.

Este módulo detecta e resolve referências temporais relativas em queries de usuários,
como "último mês", "últimos 3 trimestres", etc., convertendo-as em valores concretos
baseados na data máxima do dataset.

Arquitetura:
1. Detecta padrões de referências temporais relativas usando regex
2. Busca data máxima do dataset (com cache)
3. Calcula períodos correspondentes
4. Substitui referências na query por valores concretos
5. Retorna query resolvida + filtros extraídos

Example:
    Query: "vendas do último mês"
    → Detecta "último mês"
    → max_date = 2016-06-30
    → Resolve para "junho de 2016"
    → Query resolvida: "vendas de junho de 2016"
    → Filtros: {"Mes": "junho", "Ano": 2016}
"""

import re
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

from src.filter_classifier.utils.dataset_max_date_cache import (
    DatasetMaxDateCache,
    MaxDateInfo,
)

logger = logging.getLogger(__name__)


# Mapeamentos de nomes de períodos em português
# Reutiliza estrutura do temporal_period_expander
MONTH_NAMES_PT = {
    1: "janeiro",
    2: "fevereiro",
    3: "março",
    4: "abril",
    5: "maio",
    6: "junho",
    7: "julho",
    8: "agosto",
    9: "setembro",
    10: "outubro",
    11: "novembro",
    12: "dezembro",
}

QUARTER_NAMES_PT = {
    1: "primeiro trimestre",
    2: "segundo trimestre",
    3: "terceiro trimestre",
    4: "quarto trimestre",
}

SEMESTER_NAMES_PT = {1: "primeiro semestre", 2: "segundo semestre"}

BIMESTER_NAMES_PT = {
    1: "primeiro bimestre",
    2: "segundo bimestre",
    3: "terceiro bimestre",
    4: "quarto bimestre",
    5: "quinto bimestre",
    6: "sexto bimestre",
}


@dataclass
class ResolverResult:
    """
    Resultado da resolução de referências temporais.

    Attributes:
        resolved_query: Query com referências substituídas por valores concretos
        detected_references: Lista de referências temporais detectadas na query original
        resolved_filters: Dicionário de filtros resolvidos {coluna: valor}
        has_relative_references: Se a query continha referências relativas
        metadata: Informações adicionais sobre a resolução
    """

    resolved_query: str
    detected_references: List[str] = field(default_factory=list)
    resolved_filters: Dict[str, Any] = field(default_factory=dict)
    has_relative_references: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


class RelativeTemporalResolver:
    """
    Resolve referências temporais relativas em queries de usuários.

    Esta classe detecta padrões como "último mês", "últimos 3 trimestres", etc.,
    busca a data máxima do dataset, calcula os períodos correspondentes e substitui
    as referências por valores concretos.

    Features:
    - Suporta múltiplas granularidades (mês, trimestre, bimestre, semestre, ano)
    - Suporta períodos singulares e múltiplos ("último mês" vs "últimos 3 meses")
    - Cache de data máxima para eficiência
    - Zero overhead para queries sem referências relativas
    - Extensível via padrões regex

    Args:
        dataset_path: Caminho para o dataset (para buscar data máxima)
        alias_mapper: AliasMapper para identificar colunas temporais (opcional)

    Example:
        >>> resolver = RelativeTemporalResolver(dataset_path="data/dataset.parquet")
        >>> result = resolver.resolve_query("vendas do último mês")
        >>> print(result.resolved_query)  # "vendas de junho de 2016"
        >>> print(result.resolved_filters)  # {"Mes": "junho", "Ano": 2016}
    """

    # Padrões regex para detecção de referências temporais relativas
    RELATIVE_PATTERNS = [
        # Padrão 1: Último período singular
        (
            r"(?:últim[ao]|ultim[ao]|last)\s+(mes|mês|trimestre|semestre|ano|bimestre)(?:\s+disponível|\s+disponivel)?",
            "last_period",
        ),
        (
            r"(mes|mês|trimestre|semestre|ano|bimestre)\s+(?:mais\s+recente)",
            "last_period",
        ),
        (
            r"(?:dados|informações|informacoes)\s+(?:mais\s+recentes?|disponíveis|disponiveis)",
            "most_recent_data",
        ),
        # Padrão 2: Últimos N períodos
        (
            r"(?:últimos|últimas|ultimos|ultimas|last)\s+(\d+)\s+(meses|trimestres|semestres|anos|bimestres)(?:\s+disponíveis|\s+disponiveis)?",
            "last_n_periods",
        ),
        # Padrão 3: Mês/ano/trimestre passado/recente
        (
            r"(?:mês|mes|ano|trimestre|bimestre|semestre)\s+(?:passado|anterior)",
            "previous_period",
        ),
        # Padrão 4: Este período
        (r"(?:este|esta|current)\s+(mes|mês|ano|trimestre)", "current_period"),
    ]

    def __init__(
        self,
        dataset_path: str,
        alias_mapper: Optional[Any] = None,
    ):
        """
        Inicializa o resolver com cache de data máxima.

        Args:
            dataset_path: Caminho para o dataset
            alias_mapper: AliasMapper para identificar colunas temporais (opcional)
        """
        self.dataset_path = dataset_path
        self.alias_mapper = alias_mapper
        self.cache = DatasetMaxDateCache()
        logger.info("[RelativeTemporalResolver] Initialized")

    def resolve_query(self, query: str) -> ResolverResult:
        """
        Resolve referências temporais relativas em uma query.

        Args:
            query: Query original do usuário

        Returns:
            ResolverResult com query resolvida e filtros extraídos
        """
        logger.info(f"[RelativeTemporalResolver] Processing query: {query}")

        detected_references = []
        resolved_filters = {}
        resolved_query = query

        # Buscar data máxima do dataset uma vez
        try:
            max_date_info = self.cache.get_max_date(self.dataset_path)
            if max_date_info is None:
                # Dataset has no temporal column - skip temporal resolution gracefully
                logger.info(
                    "[RelativeTemporalResolver] No temporal column in dataset, "
                    "skipping relative temporal resolution"
                )
                return ResolverResult(
                    resolved_query=query,
                    has_relative_references=False,
                    metadata={"skipped": "no_temporal_column"},
                )
            logger.debug(
                f"[RelativeTemporalResolver] Max date from dataset: {max_date_info.max_date.date()}"
            )
        except Exception as e:
            logger.error(f"[RelativeTemporalResolver] Failed to get max date: {str(e)}")
            return ResolverResult(
                resolved_query=query,
                has_relative_references=False,
                metadata={"error": str(e)},
            )

        # Processar cada padrão
        for pattern, pattern_type in self.RELATIVE_PATTERNS:
            match = re.search(pattern, resolved_query, re.IGNORECASE)
            if match:
                logger.info(
                    f"[RelativeTemporalResolver] Detected pattern '{pattern_type}': {match.group(0)}"
                )

                # Processar baseado no tipo de padrão
                if pattern_type == "last_period":
                    result = self._resolve_last_period(match, max_date_info)
                elif pattern_type == "last_n_periods":
                    result = self._resolve_last_n_periods(match, max_date_info)
                elif pattern_type == "previous_period":
                    result = self._resolve_previous_period(match, max_date_info)
                elif pattern_type == "current_period":
                    result = self._resolve_current_period(match, max_date_info)
                elif pattern_type == "most_recent_data":
                    result = self._resolve_most_recent_data(match, max_date_info)
                else:
                    continue

                # Substituir na query
                if result:
                    detected_references.append(match.group(0))
                    resolved_query = resolved_query.replace(
                        match.group(0), result["replacement_text"]
                    )
                    resolved_filters.update(result["filters"])

                    logger.info(
                        f"[RelativeTemporalResolver] Resolved: '{match.group(0)}' -> '{result['replacement_text']}'"
                    )

        has_references = len(detected_references) > 0

        if has_references:
            logger.info(
                f"[RelativeTemporalResolver] Resolved query: '{query}' -> '{resolved_query}'"
            )

        return ResolverResult(
            resolved_query=resolved_query,
            detected_references=detected_references,
            resolved_filters=resolved_filters,
            has_relative_references=has_references,
            metadata={
                "max_date": max_date_info.max_date,
                "pattern_count": len(detected_references),
            },
        )

    def _resolve_last_period(
        self, match: re.Match, max_date_info: MaxDateInfo
    ) -> Optional[Dict[str, Any]]:
        """
        Resolve 'último [período]'.

        Args:
            match: Regex match object
            max_date_info: Informações de data máxima

        Returns:
            Dict com replacement_text e filters, ou None se falhar
        """
        from datetime import datetime
        import calendar

        granularity = match.group(1).lower()  # "mes", "mês", "trimestre", etc.

        if granularity in ["mes", "mês"]:
            # Último mês - usar range de datas do mês
            month_name = MONTH_NAMES_PT[max_date_info.max_month]
            year = max_date_info.max_year
            month = max_date_info.max_month

            # Calcular primeiro e último dia do mês
            first_day = datetime(year, month, 1)
            last_day_num = calendar.monthrange(year, month)[1]
            last_day = datetime(year, month, last_day_num)

            return {
                "replacement_text": f"{month_name} de {year}",
                "filters": self._get_discrete_filters(year=year, month=month),
            }

        elif granularity == "trimestre":
            # Último trimestre
            quarter = max_date_info.max_quarter
            year = max_date_info.max_year

            return {
                "replacement_text": f"{QUARTER_NAMES_PT[quarter]} de {year}",
                "filters": {"Trimestre": quarter, "Ano": year},
            }

        elif granularity == "semestre":
            # Último semestre
            semester = max_date_info.max_semester
            year = max_date_info.max_year

            return {
                "replacement_text": f"{SEMESTER_NAMES_PT[semester]} de {year}",
                "filters": {"Semestre": semester, "Ano": year},
            }

        elif granularity == "bimestre":
            # Último bimestre
            bimester = max_date_info.max_bimester
            year = max_date_info.max_year

            return {
                "replacement_text": f"{BIMESTER_NAMES_PT[bimester]} de {year}",
                "filters": {"Bimestre": bimester, "Ano": year},
            }

        elif granularity == "ano":
            # Último ano
            year = max_date_info.max_year

            return {
                "replacement_text": str(year),
                "filters": self._get_discrete_filters(year=year),
            }

        return None

    def _resolve_last_n_periods(
        self, match: re.Match, max_date_info: MaxDateInfo
    ) -> Optional[Dict[str, Any]]:
        """
        Resolve 'últimos N [períodos]'.

        Args:
            match: Regex match object
            max_date_info: Informações de data máxima

        Returns:
            Dict com replacement_text e filters, ou None se falhar
        """
        n = int(match.group(1))  # Número de períodos
        granularity = match.group(2).lower()  # "meses", "trimestres", etc.

        # Calcular períodos
        periods = self._calculate_last_n_periods(granularity, max_date_info, n)

        if not periods:
            return None

        # Formatar replacement text e filtros
        if granularity == "meses":
            # Ex: últimos 3 meses → ["junho", "maio", "abril"] de 2016
            months = [MONTH_NAMES_PT[p[0]] for p in periods]
            years = list(set(p[1] for p in periods))

            if len(years) == 1:
                replacement_text = f"{', '.join(months)} de {years[0]}"
                filters = {
                    "Mes": [m.capitalize() for m in months],
                    "Ano": years[0],
                }
            else:
                replacement_text = f"{', '.join(months)}"
                filters = {"Mes": [m.capitalize() for m in months]}

            return {"replacement_text": replacement_text, "filters": filters}

        elif granularity == "trimestres":
            quarters = [p[0] for p in periods]
            years = list(set(p[1] for p in periods))

            if len(years) == 1:
                replacement_text = (
                    f"trimestres {', '.join(map(str, quarters))} de {years[0]}"
                )
                filters = {"Trimestre": quarters, "Ano": years[0]}
            else:
                replacement_text = f"trimestres {', '.join(map(str, quarters))}"
                filters = {"Trimestre": quarters}

            return {"replacement_text": replacement_text, "filters": filters}

        # Similar para semestres, bimestres, anos...

        return None

    def _resolve_previous_period(
        self, match: re.Match, max_date_info: MaxDateInfo
    ) -> Optional[Dict[str, Any]]:
        """
        Resolve '[período] passado' ou '[período] anterior'.

        Args:
            match: Regex match object
            max_date_info: Informações de data máxima

        Returns:
            Dict com replacement_text e filters, ou None se falhar
        """
        full_match = match.group(0).lower()

        if "mês" in full_match or "mes" in full_match:
            # Mês anterior
            prev_month = max_date_info.max_month - 1
            prev_year = max_date_info.max_year

            if prev_month == 0:
                prev_month = 12
                prev_year -= 1

            month_name = MONTH_NAMES_PT[prev_month]

            return {
                "replacement_text": f"{month_name} de {prev_year}",
                "filters": self._get_discrete_filters(year=prev_year, month=prev_month),
            }

        elif "ano" in full_match:
            # Ano anterior
            prev_year = max_date_info.max_year - 1

            return {
                "replacement_text": str(prev_year),
                "filters": self._get_discrete_filters(year=prev_year),
            }

        # Similar para trimestre, semestre, bimestre...

        return None

    def _resolve_current_period(
        self, match: re.Match, max_date_info: MaxDateInfo
    ) -> Optional[Dict[str, Any]]:
        """
        Resolve 'este [período]' ou 'current [period]'.

        Args:
            match: Regex match object
            max_date_info: Informações de data máxima

        Returns:
            Dict com replacement_text e filters, ou None se falhar
        """
        granularity = match.group(1).lower()

        if granularity in ["mes", "mês"]:
            # Este mês
            month_name = MONTH_NAMES_PT[max_date_info.max_month]
            year = max_date_info.max_year

            return {
                "replacement_text": f"{month_name} de {year}",
                "filters": self._get_discrete_filters(year=year, month=max_date_info.max_month),
            }

        elif granularity == "ano":
            # Este ano
            year = max_date_info.max_year

            return {
                "replacement_text": str(year),
                "filters": self._get_discrete_filters(year=year),
            }

        # Similar para trimestre...

        return None

    def _get_discrete_filters(
        self, year: int, month: Optional[int] = None, quarter: Optional[int] = None,
        semester: Optional[int] = None, bimester: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Build temporal filters using discrete columns from temporal_mapping.

        The temporal_mapping.derived_columns in alias.yaml maps:
            column_name -> semantic_type  (e.g. ano: year, mes: month)
        So the KEYS are the actual dataset column names.

        We build a reverse lookup: semantic_type -> column_name
        to find which column corresponds to each temporal granularity.
        """
        derived = {}
        if self.alias_mapper and hasattr(self.alias_mapper, "aliases"):
            derived = (
                self.alias_mapper.aliases
                .get("temporal_mapping", {})
                .get("derived_columns", {})
            )

        # Build reverse map: semantic_type -> column_name
        # e.g. {"year": "ano", "month": "mes", "year_month": "ano_mes"}
        type_to_col = {v: k for k, v in derived.items()}

        col_ano = type_to_col.get("year", "ano")
        col_mes = type_to_col.get("month", "mes")
        col_trim = type_to_col.get("quarter", "trimestre")
        col_sem = type_to_col.get("semester", "semestre")
        col_bim = type_to_col.get("bimester", "bimestre")
        col_ano_mes = type_to_col.get("year_month")

        filters = {col_ano: year}
        if month is not None:
            filters[col_mes] = month
            if col_ano_mes is not None:
                filters[col_ano_mes] = int(f"{year}{month:02d}")
        elif quarter is not None:
            filters[col_trim] = quarter
        elif semester is not None:
            filters[col_sem] = semester
        elif bimester is not None:
            filters[col_bim] = bimester

        return filters

    def _resolve_most_recent_data(self, match: re.Match, max_date_info: MaxDateInfo) -> Optional[Dict[str, Any]]:
        month_name = MONTH_NAMES_PT[max_date_info.max_month]
        year = max_date_info.max_year
        filters = self._get_discrete_filters(year=year, month=max_date_info.max_month)
        return {
            "replacement_text": f"{month_name} de {year}",
            "filters": filters
        }

    def _calculate_last_n_periods(
        self, granularity: str, max_date_info: MaxDateInfo, n: int
    ) -> List[Tuple[int, int]]:
        """
        Calcula os últimos N períodos de uma granularidade.

        Args:
            granularity: "meses", "trimestres", "bimestres", "semestres", "anos"
            max_date_info: Informações de data máxima
            n: Número de períodos

        Returns:
            Lista de tuplas (período, ano)
        """
        periods = []

        if granularity == "meses":
            current_month = max_date_info.max_month
            current_year = max_date_info.max_year

            for i in range(n):
                periods.append((current_month, current_year))
                current_month -= 1
                if current_month == 0:
                    current_month = 12
                    current_year -= 1

        elif granularity == "trimestres":
            current_quarter = max_date_info.max_quarter
            current_year = max_date_info.max_year

            for i in range(n):
                periods.append((current_quarter, current_year))
                current_quarter -= 1
                if current_quarter == 0:
                    current_quarter = 4
                    current_year -= 1

        elif granularity == "bimestres":
            current_bimester = max_date_info.max_bimester
            current_year = max_date_info.max_year

            for i in range(n):
                periods.append((current_bimester, current_year))
                current_bimester -= 1
                if current_bimester == 0:
                    current_bimester = 6
                    current_year -= 1

        elif granularity == "semestres":
            current_semester = max_date_info.max_semester
            current_year = max_date_info.max_year

            for i in range(n):
                periods.append((current_semester, current_year))
                current_semester -= 1
                if current_semester == 0:
                    current_semester = 2
                    current_year -= 1

        elif granularity == "anos":
            current_year = max_date_info.max_year

            for i in range(n):
                periods.append((current_year, current_year))
                current_year -= 1

        return periods
