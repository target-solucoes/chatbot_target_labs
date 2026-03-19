"""
Modulo para sanitizacao de chart_type extraido do LLM.

Este modulo implementa a correcao critica para Issue #1, garantindo que
valores retornados pelo LLM com texto descritivo sejam sanitizados para
os literais exatos esperados pelo schema ChartOutput.

Examples:
    >>> sanitize_chart_type("bar_vertical (direct comparison)")
    'bar_vertical'

    >>> sanitize_chart_type("line")
    'line'

    >>> sanitize_chart_type("invalid_type")
    None
"""

import re
import logging
from typing import Optional, Literal

logger = logging.getLogger(__name__)


# Type hint para chart types validos
# FASE 2: "line" removed - use "line_composed" with render_variant instead
ChartTypeLiteral = Literal[
    "bar_horizontal",
    "bar_vertical",
    "bar_vertical_composed",
    "line_composed",
    "pie",
    "bar_vertical_stacked",
    "histogram",
]

# Lista de valores permitidos para chart_type
# FASE 2: "line" removed - use "line_composed" with render_variant instead
ALLOWED_CHART_TYPES = [
    "bar_horizontal",
    "bar_vertical",
    "bar_vertical_composed",
    "line_composed",
    "pie",
    "bar_vertical_stacked",
    "histogram",
    "null",
    "none",
    "n/a",
]


def sanitize_chart_type(raw_value: str) -> Optional[str]:
    """
    Sanitiza o valor de chart_type extraido do LLM.

    Remove texto descritivo entre parenteses, espacos extras,
    e valida contra lista de valores permitidos.

    Esta funcao implementa a correcao critica para o Issue #1, onde o LLM
    pode retornar valores como "bar_vertical (direct comparison)" ao inves
    do literal exato "bar_vertical", causando falhas de validacao Pydantic.

    Args:
        raw_value: Valor bruto extraido do LLM

    Returns:
        Chart type sanitizado ou None se invalido

    Examples:
        >>> sanitize_chart_type("bar_vertical (direct comparison)")
        'bar_vertical'

        >>> sanitize_chart_type("line")
        'line'

        >>> sanitize_chart_type("invalid_type")
        None

        >>> sanitize_chart_type("null")
        None
    """
    if not raw_value:
        return None

    # Normalizar: lowercase e trim
    normalized = raw_value.strip().lower()

    # Remover texto entre parenteses e tudo depois
    # Ex: "bar_vertical (comparison)" -> "bar_vertical"
    # Ex: "bar_vertical comparison" -> "bar_vertical"
    sanitized = re.split(r"[\s(]", normalized)[0]

    # Validar contra lista permitida
    if sanitized in ALLOWED_CHART_TYPES:
        # Converter "null", "none", "n/a" para None
        if sanitized in ["null", "none", "n/a"]:
            return None
        return sanitized

    # Se nao esta na lista, logar warning e retornar None
    logger.warning(
        f"[sanitize_chart_type] Invalid chart_type after sanitization: "
        f"raw='{raw_value}' -> sanitized='{sanitized}'. "
        f"Allowed values: {ALLOWED_CHART_TYPES}"
    )
    return None


def validate_chart_type_format(value: str) -> bool:
    """
    Valida se o chart_type esta no formato correto (sem descricoes).

    Esta funcao detecta se o valor ainda contem texto descritivo que
    deveria ter sido removido, indicando que o LLM nao esta seguindo
    as instrucoes do prompt corretamente.

    Args:
        value: Valor a validar

    Returns:
        True se formato correto, False se contem texto extra

    Examples:
        >>> validate_chart_type_format("bar_vertical")
        True

        >>> validate_chart_type_format("bar_vertical (comparison)")
        False

        >>> validate_chart_type_format("line chart")
        False
    """
    if not value:
        return True  # None e valido

    # Verificar se contem espacos ou parenteses (indica texto descritivo)
    if " " in value or "(" in value:
        return False

    return True
