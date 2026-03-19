"""
Ranking Operation Detector - Upstream Detection Module

Este módulo implementa a detecção preventiva de ranking operations
(top N, bottom N, maiores, menores, etc.) ANTES da criação de filtros.

Conforme especificado em planning_graphical_correction.md - Fase 3.1:
- Detectar ranking operations no query_parser (upstream)
- Prevenir criação de filtros inválidos
- Módulo dedicado e reutilizável

Referência: GRAPHICAL_CLASSIFIER_DIAGNOSIS.md - Issue #3
"""

import re
import logging
from typing import Dict, Any, Tuple

logger = logging.getLogger(__name__)


# Ranking patterns com grupos de captura para extrair número e tipo
RANKING_PATTERNS = {
    # Pattern: (regex_pattern, sort_order, ranking_type)
    r"\btop\s+(\d+)\b": ("desc", "top"),
    r"\bbottom\s+(\d+)\b": ("asc", "bottom"),
    r"\b(\d+)\s+maiores\b": ("desc", "top"),
    r"\b(\d+)\s+menores\b": ("asc", "bottom"),
    r"\b(\d+)\s+primeiros\b": ("desc", "top"),
    r"\b(\d+)\s+últimos\b": ("asc", "bottom"),
    r"\b(\d+)\s+ultimos\b": ("asc", "bottom"),
    r"\bprimeiro(?:s)?\s+(\d+)\b": ("desc", "top"),
    r"\búltimo(?:s)?\s+(\d+)\b": ("asc", "bottom"),
    r"\bultimo(?:s)?\s+(\d+)\b": ("asc", "bottom"),
    r"\bmelhores\s+(\d+)\b": ("desc", "top"),
    r"\b(\d+)\s+melhores\b": ("desc", "top"),
    r"\bpiores\s+(\d+)\b": ("asc", "bottom"),
    r"\b(\d+)\s+piores\b": ("asc", "bottom"),
    r"\bmaior(?:es)?\s+(\d+)\b": ("desc", "top"),
    r"\bmenor(?:es)?\s+(\d+)\b": ("asc", "bottom"),
}


def extract_ranking_info(query: str) -> Dict[str, Any]:
    """
    Extrai informações de ranking do query (top N, bottom N).

    Esta função detecta operações de ranking UPSTREAM no fluxo,
    prevenindo que sejam criados filtros inválidos downstream.

    Args:
        query: Query do usuário em linguagem natural

    Returns:
        Dict com:
            - top_n (int): Número de registros a retornar
            - sort_order (str): 'desc' para top/maiores, 'asc' para bottom/menores
            - ranking_type (str): 'top' ou 'bottom'
            - matched_pattern (str): Pattern que fez o match (para debug)
            - original_text (str): Texto original que fez match

    Examples:
        >>> extract_ranking_info("top 5 produtos")
        {'top_n': 5, 'sort_order': 'desc', 'ranking_type': 'top', ...}

        >>> extract_ranking_info("10 menores clientes")
        {'top_n': 10, 'sort_order': 'asc', 'ranking_type': 'bottom', ...}

        >>> extract_ranking_info("vendas por estado")
        {}  # Sem ranking operation detectada
    """
    if not query:
        return {}

    query_lower = query.lower().strip()

    # Tentar cada pattern
    for pattern, (sort_order, ranking_type) in RANKING_PATTERNS.items():
        match = re.search(pattern, query_lower, re.IGNORECASE)

        if match:
            try:
                # Extrair número do primeiro grupo de captura
                n = int(match.group(1))

                result = {
                    "top_n": n,
                    "sort_order": sort_order,
                    "ranking_type": ranking_type,
                    "matched_pattern": pattern,
                    "original_text": match.group(0),
                }

                logger.info(
                    f"[extract_ranking_info] Detected ranking operation: "
                    f"top_n={n}, order={sort_order}, type={ranking_type}, "
                    f"matched='{match.group(0)}'"
                )

                return result

            except (ValueError, IndexError) as e:
                logger.warning(
                    f"[extract_ranking_info] Failed to extract number from match: "
                    f"{match.group(0)} - {e}"
                )
                continue

    # Nenhum ranking operation detectado
    logger.debug("[extract_ranking_info] No ranking operation detected in query")
    return {}


def is_ranking_filter_value(value: Any) -> bool:
    """
    Detecta se um valor de filtro é na verdade uma ranking operation.

    Esta função é usada como safety check downstream para detectar
    filtros inválidos que escaparam da detecção upstream.

    Args:
        value: Valor a verificar (pode ser string, int, lista, etc)

    Returns:
        True se o valor parece ser uma ranking operation, False caso contrário

    Examples:
        >>> is_ranking_filter_value("top 10")
        True

        >>> is_ranking_filter_value(["top 5", "bottom 3"])
        True

        >>> is_ranking_filter_value("SC")
        False

        >>> is_ranking_filter_value(2015)
        False
    """
    if not value:
        return False

    # Se é lista, verificar cada item
    if isinstance(value, list):
        return any(is_ranking_filter_value(item) for item in value)

    # Converter para string e normalizar
    value_str = str(value).lower().strip()

    # Padrões simplificados para detecção rápida
    simple_patterns = [
        r"^top\s+\d+$",
        r"^bottom\s+\d+$",
        r"^\d+\s+primeiros$",
        r"^\d+\s+ultimos$",
        r"^\d+\s+maiores$",
        r"^\d+\s+menores$",
        r"^primeiro\s+\d+$",
        r"^ultimo\s+\d+$",
        r"^\d+\s+melhores$",
        r"^\d+\s+piores$",
    ]

    for pattern in simple_patterns:
        if re.match(pattern, value_str):
            logger.warning(
                f"[is_ranking_filter_value] Detected ranking operation in filter value: "
                f"'{value}' (pattern: {pattern})"
            )
            return True

    return False


def validate_no_ranking_in_filters(filters: Dict[str, Any]) -> Tuple[bool, list]:
    """
    Valida que não há ranking operations nos filtros.

    Esta função é usada como validação final antes de processar filtros,
    garantindo que nenhuma ranking operation escapou da detecção upstream.

    Args:
        filters: Dicionário de filtros a validar

    Returns:
        Tuple (is_valid, invalid_filters):
            - is_valid (bool): True se não há ranking operations
            - invalid_filters (list): Lista de filtros inválidos detectados

    Examples:
        >>> validate_no_ranking_in_filters({"UF": ["SC", "PR"]})
        (True, [])

        >>> validate_no_ranking_in_filters({"Cod_Cliente": ["top 10"]})
        (False, [{'key': 'Cod_Cliente', 'value': ['top 10']}])
    """
    invalid_filters = []

    for key, value in filters.items():
        if is_ranking_filter_value(value):
            invalid_filters.append({"key": key, "value": value})
            logger.error(
                f"[validate_no_ranking_in_filters] INVALID: Ranking operation found in filter: "
                f"{key}={value}. Use top_n parameter instead."
            )

    is_valid = len(invalid_filters) == 0

    if is_valid:
        logger.debug(
            "[validate_no_ranking_in_filters] All filters valid (no ranking operations)"
        )
    else:
        logger.warning(
            f"[validate_no_ranking_in_filters] Found {len(invalid_filters)} invalid filters "
            f"with ranking operations"
        )

    return is_valid, invalid_filters


def get_ranking_keywords() -> list:
    """
    Retorna lista de keywords que indicam ranking operations.

    Útil para análise de query e detecção de intent.

    Returns:
        Lista de keywords em português e inglês
    """
    return [
        "top",
        "bottom",
        "ranking",
        "maiores",
        "menores",
        "maior",
        "menor",
        "melhores",
        "piores",
        "melhor",
        "pior",
        "primeiros",
        "últimos",
        "primeiro",
        "último",
        "ultimos",
        "ultimo",  # sem acento também
    ]


def has_ranking_keywords(query: str) -> bool:
    """
    Verifica se o query contém keywords de ranking.

    Útil para detecção rápida antes de aplicar regex patterns.

    Args:
        query: Query do usuário

    Returns:
        True se contém keywords de ranking

    Examples:
        >>> has_ranking_keywords("top 10 produtos")
        True

        >>> has_ranking_keywords("vendas por estado")
        False
    """
    if not query:
        return False

    query_lower = query.lower()
    keywords = get_ranking_keywords()

    return any(keyword in query_lower for keyword in keywords)


def extract_nested_ranking(query: str) -> Dict[str, Any]:
    """
    Detecta operações de nested ranking (top N dentro dos top M).

    Exemplos de queries com nested ranking:
    - "top 3 clientes dos 5 maiores estados"
    - "5 produtos mais vendidos nos 10 melhores clientes"
    - "maiores 3 vendedores dos 7 maiores estados"

    Args:
        query: Query do usuário em linguagem natural

    Returns:
        Dict com:
            - is_nested (bool): True se detectou nested ranking
            - top_n (int): Número de subgrupos (ex: 3 clientes)
            - group_top_n (int): Número de grupos principais (ex: 5 estados)
            - subgroup_entity (str): Entidade do subgrupo (ex: "clientes")
            - group_entity (str): Entidade do grupo (ex: "estados")
            - sort_order (str): Ordem de classificação ('desc' padrão)

    Examples:
        >>> extract_nested_ranking("top 3 clientes dos 5 maiores estados")
        {
            'is_nested': True,
            'top_n': 3,
            'group_top_n': 5,
            'subgroup_entity': 'clientes',
            'group_entity': 'estados',
            'sort_order': 'desc'
        }

        >>> extract_nested_ranking("top 10 produtos")
        {'is_nested': False}
    """
    if not query:
        return {"is_nested": False}

    query_lower = query.lower().strip()

    # Padrões de nested ranking:
    # Formato: "top N <entity1> dos/para M maiores <entity2>"
    # Variações: dentro dos, nos, para os, do(s), da(s), que mais..., mais...
    nested_patterns = [
        # "N <entity> para os M maiores <entity>"
        # Ex: "top 3 produtos para os 5 maiores estados", "3 produtos para os 5 maiores estados"
        r"(?:top\s+)?(\d+)\s+(\w+)\s+para\s+(?:os?|as?)\s+(\d+)\s+(?:maiores?|menores?|melhores?|piores?)\s+(\w+)",
        # "N <entity> que mais ... nos/para M maiores <entity>"
        # Ex: "3 produtos que mais venderam nos 5 maiores estados"
        r"(\d+)\s+(\w+)\s+(?:que\s+mais\s+\w+|mais\s+\w+)\s+(?:nos?|nas?|dos?|das?|para\s+os?|para\s+as?|dentro\s+dos?|dentro\s+das?)\s+(\d+)\s+(?:maiores?|menores?|melhores?|piores?)\s+(\w+)",
        # top N <entity> dos/para M maiores/menores <entity>
        r"(?:top|maiores?|melhores?)\s+(\d+)\s+(\w+)\s+(?:dos?|das?|nos?|nas?|para\s+os?|para\s+as?|dentro\s+dos?|dentro\s+das?)\s+(\d+)\s+(?:maiores?|menores?|melhores?|piores?)\s+(\w+)",
        # N maiores <entity> dos M maiores <entity>
        r"(\d+)\s+(?:maiores?|menores?|melhores?|piores?)\s+(\w+)\s+(?:dos?|das?|nos?|nas?|para\s+os?|para\s+as?|dentro\s+dos?|dentro\s+das?)\s+(\d+)\s+(?:maiores?|menores?|melhores?|piores?)\s+(\w+)",
        # top N <entity> nos top M <entity>
        r"(?:top)\s+(\d+)\s+(\w+)\s+(?:nos?|nas?|dos?|das?|para\s+os?|para\s+as?)\s+(?:top)\s+(\d+)\s+(\w+)",
    ]

    for pattern in nested_patterns:
        match = re.search(pattern, query_lower, re.IGNORECASE)

        if match:
            try:
                # Extrair grupos
                groups = match.groups()

                if len(groups) == 4:
                    top_n = int(groups[0])
                    subgroup_entity = groups[1].strip()
                    group_top_n = int(groups[2])
                    group_entity = groups[3].strip()

                    result = {
                        "is_nested": True,
                        "top_n": top_n,
                        "group_top_n": group_top_n,
                        "subgroup_entity": subgroup_entity,
                        "group_entity": group_entity,
                        "sort_order": "desc",  # Default para nested ranking
                        "matched_pattern": pattern,
                        "original_text": match.group(0),
                    }

                    logger.info(
                        f"[extract_nested_ranking] Detected NESTED ranking: "
                        f"top_n={top_n} ({subgroup_entity}) within "
                        f"group_top_n={group_top_n} ({group_entity}), "
                        f"matched='{match.group(0)}'"
                    )

                    return result

            except (ValueError, IndexError) as e:
                logger.warning(
                    f"[extract_nested_ranking] Failed to parse nested ranking from: "
                    f"{match.group(0)} - {e}"
                )
                continue

    logger.debug("[extract_nested_ranking] No nested ranking detected")
    return {"is_nested": False}


def map_nested_ranking_to_columns(nested_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mapeia entidades do nested ranking para colunas reais usando AliasMapper.

    Para "top 3 clientes dos 5 maiores estados":
    - group_entity="estados" -> group_column="UF_Cliente"  (via alias.yaml)
    - subgroup_entity="clientes" -> subgroup_column="Cod_Cliente" (via alias.yaml)

    Usa o arquivo data/mappings/alias.yaml para mapeamento dinamico.

    Args:
        nested_result: Resultado de extract_nested_ranking()

    Returns:
        Dict com campos adicionais:
            - group_column (str): Nome da coluna do grupo principal
            - subgroup_column (str): Nome da coluna do subgrupo

    Example:
        >>> result = extract_nested_ranking("top 3 clientes dos 5 maiores estados")
        >>> mapped = map_nested_ranking_to_columns(result)
        >>> mapped['group_column']
        'UF_Cliente'
        >>> mapped['subgroup_column']
        'Cod_Cliente'
    """
    if not nested_result.get("is_nested"):
        return nested_result

    # Importar AliasMapper - lazy import para evitar circular dependency
    from src.graphic_classifier.tools.alias_mapper import AliasMapper

    result = nested_result.copy()

    try:
        mapper = AliasMapper()

        group_entity = result.get("group_entity", "").lower()
        subgroup_entity = result.get("subgroup_entity", "").lower()

        # Usar AliasMapper.resolve() para mapear entidades para colunas
        result["group_column"] = mapper.resolve(group_entity)
        result["subgroup_column"] = mapper.resolve(subgroup_entity)

        logger.info(
            f"[map_nested_ranking_to_columns] Mapped via AliasMapper: "
            f"group='{group_entity}' -> '{result['group_column']}', "
            f"subgroup='{subgroup_entity}' -> '{result['subgroup_column']}'"
        )

    except Exception as e:
        logger.warning(
            f"[map_nested_ranking_to_columns] AliasMapper error: {e}. "
            f"Entities not mapped to columns."
        )
        result["group_column"] = None
        result["subgroup_column"] = None

    return result
