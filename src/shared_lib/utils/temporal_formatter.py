"""
Temporal Formatter - Utilitario para formatacao de valores temporais.

Este modulo centraliza a logica de formatacao e ordenacao de valores temporais
(meses, anos, trimestres, etc.) para uso em visualizacoes de graficos.

Fornece funcoes para:
- Detectar se uma dimensao e temporal
- Formatar valores temporais para exibicao legivel
- Ordenar valores temporais cronologicamente

Uso tipico:
    >>> from src.shared_lib.utils.temporal_formatter import format_temporal_value
    >>> format_temporal_value(1, "Mes")
    'janeiro'
    >>> format_temporal_value(2015, "Ano")
    '2015'
"""

import logging
from typing import Any, Union
from datetime import datetime
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


# ============================================================================
# MAPEAMENTOS DE VALORES TEMPORAIS
# ============================================================================

# Mapeamento: numero do mes -> nome do mes em portugues
MONTH_NUMBER_TO_NAME = {
    1: "janeiro",
    2: "fevereiro",
    3: "marco",
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

# Mapeamento: abreviacao do mes -> nome completo do mes
MONTH_ABBR_TO_NAME = {
    "jan": "janeiro",
    "fev": "fevereiro",
    "mar": "marco",
    "abr": "abril",
    "mai": "maio",
    "jun": "junho",
    "jul": "julho",
    "ago": "agosto",
    "set": "setembro",
    "out": "outubro",
    "nov": "novembro",
    "dez": "dezembro",
}

# Mapeamento: numero do mes -> abreviacao de 3 letras (estilo Plotly: Jan, Feb, Mar)
# Usado quando exibindo mes + ano no formato "Feb 2015"
MONTH_NUMBER_TO_ABBR_EN = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}

# Mapeamento: numero do trimestre -> formato de exibicao
QUARTER_NUMBER_TO_NAME = {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}

# Mapeamento: numero do semestre -> formato de exibicao
SEMESTER_NUMBER_TO_NAME = {1: "1º Semestre", 2: "2º Semestre"}


# ============================================================================
# FUNCOES DE DETECCAO
# ============================================================================


def is_temporal_dimension(dimension_name: str) -> bool:
    """
    Detecta se uma dimensao e temporal baseado no nome.

    Verifica se o nome da dimensao contem palavras-chave que indicam
    que se trata de uma dimensao temporal (mes, ano, trimestre, etc.).

    Args:
        dimension_name: Nome da dimensao a verificar

    Returns:
        True se a dimensao for temporal, False caso contrario

    Examples:
        >>> is_temporal_dimension("Mes")
        True
        >>> is_temporal_dimension("Ano")
        True
        >>> is_temporal_dimension("Produto")
        False
        >>> is_temporal_dimension("UF_Cliente")
        False
    """
    if not dimension_name:
        return False

    dimension_lower = dimension_name.lower()

    # Lista de indicadores temporais
    temporal_indicators = [
        "mes",
        "month",
        "ano",
        "year",
        "trimestre",
        "quarter",
        "semestre",
        "semester",
        "data",
        "date",
        "periodo",
        "period",
    ]

    return any(indicator in dimension_lower for indicator in temporal_indicators)


# ============================================================================
# FUNCOES DE FORMATACAO
# ============================================================================


def format_temporal_value(
    value: Any, dimension_name: str, year_value: Any = None
) -> str:
    """
    Formata valor temporal para exibicao legivel.

    Aplica formatacao apropriada baseado no tipo de dimensao temporal:
    - Mes: numero -> nome do mes ("janeiro", "fevereiro", etc.)
           Se year_value fornecido: "janeiro/2015"
    - Ano: mantem como string
    - Trimestre: numero -> "Q1", "Q2", etc.
               Se year_value fornecido: "Q1/2015"
    - Semestre: numero -> "1º Semestre", "2º Semestre"
               Se year_value fornecido: "1º Sem/2015"
    - Data/Timestamp: formata como "mes/ano"

    Args:
        value: Valor a formatar (int, str, datetime, etc.)
        dimension_name: Nome da dimensao temporal
        year_value: Valor do ano para incluir no formato (opcional)

    Returns:
        String formatada para exibicao

    Examples:
        >>> format_temporal_value(1, "Mes")
        'janeiro'
        >>> format_temporal_value(1, "Mes", 2015)
        'janeiro/2015'
        >>> format_temporal_value(2015, "Ano")
        '2015'
        >>> format_temporal_value(1, "Trimestre", 2024)
        'Q1/2024'
    """
    try:
        dimension_lower = dimension_name.lower()

        # IMPORTANTE: Verificar termos mais específicos primeiro
        # (trimestre/semestre antes de mes, pois contêm "mes" como substring)

        # Trimestre
        if "trimestre" in dimension_lower or "quarter" in dimension_lower:
            formatted = _format_quarter(value)
            return f"{formatted}/{year_value}" if year_value else formatted

        # Semestre
        elif "semestre" in dimension_lower or "semester" in dimension_lower:
            formatted = _format_semester(value)
            # Abreviar "Semestre" para "Sem" quando ano incluído
            if year_value:
                formatted = formatted.replace("Semestre", "Sem")
                return f"{formatted}/{year_value}"
            return formatted

        # Mes (verificar depois de trimestre/semestre)
        elif "mes" in dimension_lower or "month" in dimension_lower:
            if isinstance(value, (pd.Timestamp, datetime)):
                # Valores de timestamp já têm ano embutido
                return _format_month_with_year(value.month, value.year)

            if year_value:
                # Com ano: usar abreviação inglesa (padrão Plotly: "Feb 2015")
                return _format_month_with_year(value, year_value)

            # Sem ano: usar abreviação inglesa (padrão Plotly: "Feb")
            return _format_month(value)

        # Ano
        elif "ano" in dimension_lower or "year" in dimension_lower:
            return _format_year(value)

        # Data/Timestamp
        elif "data" in dimension_lower or "date" in dimension_lower:
            return _format_date(value)

        # Fallback: retornar como string
        else:
            logger.debug(
                f"Dimensao temporal '{dimension_name}' nao reconhecida, "
                f"retornando valor como string"
            )
            return str(value)

    except Exception as e:
        # Em caso de erro, retornar valor original como string
        logger.warning(
            f"Erro ao formatar valor temporal '{value}' "
            f"para dimensao '{dimension_name}': {e}. "
            f"Retornando valor original."
        )
        return str(value)


def _format_month(value: Any) -> str:
    """
    Formata valor de mes usando abreviacao em ingles (padrao Plotly).

    Esta funcao SEMPRE retorna abreviacoes em ingles (Jan, Feb, Mar, etc.)
    para consistencia com o padrao Plotly, mesmo quando o ano nao esta disponivel.

    Args:
        value: Valor do mes (numero 1-12, string, etc.)

    Returns:
        Abreviacao em ingles (Jan, Feb, Mar, etc.)

    Examples:
        >>> _format_month(2)
        'Feb'
        >>> _format_month(3)
        'Mar'
    """
    # Se e numero inteiro (incluindo numpy int64, int32, etc.)
    if isinstance(value, (int, float, np.integer, np.floating)):
        month_num = int(value)
        if month_num in MONTH_NUMBER_TO_ABBR_EN:
            return MONTH_NUMBER_TO_ABBR_EN[month_num]
        else:
            logger.warning(f"Numero de mes invalido: {month_num}")
            return str(value)

    # Se e string (pode ser abreviacao)
    elif isinstance(value, str):
        value_lower = value.lower().strip()
        # Tentar converter para numero primeiro
        try:
            month_num = int(value)
            if month_num in MONTH_NUMBER_TO_ABBR_EN:
                return MONTH_NUMBER_TO_ABBR_EN[month_num]
        except ValueError:
            pass

    # Fallback
    return str(value)


def _format_month_with_year(value: Any, year_value: Any) -> str:
    """
    Formata valor de mes com ano no formato padrao Plotly: "Feb 2015".

    Args:
        value: Valor do mes (numero 1-12)
        year_value: Valor do ano (ex: 2015)

    Returns:
        String formatada como "Feb 2015", "Mar 2015", etc.

    Examples:
        >>> _format_month_with_year(2, 2015)
        'Feb 2015'
        >>> _format_month_with_year(3, 2015)
        'Mar 2015'
    """
    # Converter para numero inteiro (suporta np.int64)
    if isinstance(value, (int, float, np.integer, np.floating)):
        month_num = int(value)
        if month_num in MONTH_NUMBER_TO_ABBR_EN:
            month_abbr = MONTH_NUMBER_TO_ABBR_EN[month_num]
            return f"{month_abbr} {year_value}"
        else:
            logger.warning(f"Numero de mes invalido: {month_num}")
            return f"{value} {year_value}"

    # Se e string, tentar converter
    elif isinstance(value, str):
        try:
            month_num = int(value)
            if month_num in MONTH_NUMBER_TO_ABBR_EN:
                month_abbr = MONTH_NUMBER_TO_ABBR_EN[month_num]
                return f"{month_abbr} {year_value}"
        except ValueError:
            pass

    # Fallback
    return f"{value} {year_value}"


def _format_year(value: Any) -> str:
    """Formata valor de ano."""
    # Simplesmente retornar como string
    return str(value)


def _format_quarter(value: Any) -> str:
    """Formata valor de trimestre."""
    if isinstance(value, (int, float, np.integer, np.floating)):
        quarter_num = int(value)
        if quarter_num in QUARTER_NUMBER_TO_NAME:
            return QUARTER_NUMBER_TO_NAME[quarter_num]

    # Fallback
    return str(value)


def _format_semester(value: Any) -> str:
    """Formata valor de semestre."""
    if isinstance(value, (int, float, np.integer, np.floating)):
        semester_num = int(value)
        if semester_num in SEMESTER_NUMBER_TO_NAME:
            return SEMESTER_NUMBER_TO_NAME[semester_num]

    # Fallback
    return str(value)


def _format_date(value: Any) -> str:
    """
    Formata valor de data/timestamp no formato padrao Plotly: "Feb 2015".

    Args:
        value: Data/Timestamp (pandas.Timestamp, datetime, etc.)

    Returns:
        String formatada como "Feb 2015", "Mar 2015", etc.

    Examples:
        >>> _format_date(pd.Timestamp('2015-02-01'))
        'Feb 2015'
        >>> _format_date(datetime(2015, 3, 1))
        'Mar 2015'
    """
    # Se e Timestamp ou datetime
    if isinstance(value, (pd.Timestamp, datetime)):
        # Extrair mes e ano
        month_num = value.month
        year_value = value.year

        # Usar abreviacao inglesa (padrao Plotly: "Feb 2015")
        month_abbr = MONTH_NUMBER_TO_ABBR_EN.get(month_num, str(month_num))
        return f"{month_abbr} {year_value}"

    # Fallback
    return str(value)


# ============================================================================
# FUNCOES DE ORDENACAO
# ============================================================================


def get_temporal_sort_key(value: Any, dimension_name: str) -> Union[int, float, str]:
    """
    Retorna chave para ordenacao cronologica de valores temporais.

    Esta funcao converte valores temporais em uma chave numerica que
    permite ordenacao cronologica correta (em vez de alfabetica).

    Args:
        value: Valor temporal a converter
        dimension_name: Nome da dimensao temporal

    Returns:
        Chave de ordenacao (int, float, ou str)

    Examples:
        >>> get_temporal_sort_key(1, "Mes")  # janeiro
        1
        >>> get_temporal_sort_key("jan", "Mes")
        1
        >>> get_temporal_sort_key(2015, "Ano")
        2015
    """
    try:
        dimension_lower = dimension_name.lower()

        # IMPORTANTE: Verificar termos mais específicos primeiro
        # (trimestre/semestre antes de mes)

        # Trimestre
        if "trimestre" in dimension_lower or "quarter" in dimension_lower:
            return _get_quarter_sort_key(value)

        # Semestre
        elif "semestre" in dimension_lower or "semester" in dimension_lower:
            return _get_semester_sort_key(value)

        # Mes (verificar depois de trimestre/semestre)
        elif "mes" in dimension_lower or "month" in dimension_lower:
            return _get_month_sort_key(value)

        # Ano
        elif "ano" in dimension_lower or "year" in dimension_lower:
            return _get_year_sort_key(value)

        # Data/Timestamp
        elif "data" in dimension_lower or "date" in dimension_lower:
            return _get_date_sort_key(value)

        # Fallback: retornar valor original
        else:
            return value

    except Exception as e:
        logger.warning(
            f"Erro ao obter chave de ordenacao para '{value}' "
            f"na dimensao '{dimension_name}': {e}. "
            f"Retornando valor original."
        )
        return value


def _get_month_sort_key(value: Any) -> int:
    """Retorna chave de ordenacao para mes."""
    # Se ja e numero (incluindo numpy types)
    if isinstance(value, (int, float, np.integer, np.floating)):
        return int(value)

    # Se e string (abreviacao)
    if isinstance(value, str):
        value_lower = value.lower().strip()
        # Procurar no mapeamento de abreviacoes
        for abbr, month_name in MONTH_ABBR_TO_NAME.items():
            if value_lower == abbr:
                # Encontrar numero do mes
                for num, name in MONTH_NUMBER_TO_NAME.items():
                    if name == month_name:
                        return num
        # Tentar converter direto
        try:
            return int(value)
        except ValueError:
            pass

    # Fallback: retornar 0 (vai para o inicio)
    return 0


def _get_year_sort_key(value: Any) -> int:
    """Retorna chave de ordenacao para ano."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0


def _get_quarter_sort_key(value: Any) -> int:
    """Retorna chave de ordenacao para trimestre."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0


def _get_semester_sort_key(value: Any) -> int:
    """Retorna chave de ordenacao para semestre."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0


def _get_date_sort_key(value: Any) -> Union[float, int]:
    """Retorna chave de ordenacao para data/timestamp."""
    # Se e Timestamp ou datetime, converter para timestamp Unix
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.timestamp()

    # Tentar converter string para datetime
    if isinstance(value, str):
        try:
            dt = pd.to_datetime(value)
            return dt.timestamp()
        except Exception:
            pass

    # Fallback
    return 0


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "is_temporal_dimension",
    "format_temporal_value",
    "get_temporal_sort_key",
    "MONTH_NUMBER_TO_NAME",
    "MONTH_ABBR_TO_NAME",
    "MONTH_NUMBER_TO_ABBR_EN",
    "QUARTER_NUMBER_TO_NAME",
    "SEMESTER_NUMBER_TO_NAME",
]
