"""
Query Mention Validator

Valida se os valores de filtros detectados pelo LLM estao REALMENTE presentes
na query original do usuario, prevenindo inferencias indevidas.
"""

import re
import logging
from typing import Dict, Any, Tuple, List
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)


class QueryMentionValidator:
    """
    Validador que verifica se valores de filtros foram explicitamente
    mencionados na query do usuario.

    Este validador previne que o LLM infira valores baseado em:
    - Associacao semantica ("produto" -> "PRODUTOS REVENDA")
    - Conhecimento geral
    - Valores de queries anteriores
    """

    # Threshold de similaridade para fuzzy matching
    FUZZY_SIMILARITY_THRESHOLD = 0.85

    # Valores que podem ser inferidos legitimamente (datas, ranges)
    ALLOWED_INFERRED_PATTERNS = [
        r'^\d{4}-\d{2}-\d{2}$',  # Data ISO: 2015-01-01
        r'^\d{4}$',              # Ano: 2015
        r'^\d{1,2}$',            # Mes/Dia: 1, 12
    ]

    @staticmethod
    def validate_filters(
        detected_filters: Dict[str, Any],
        query: str
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Valida se os filtros detectados estao presentes na query.

        Args:
            detected_filters: Filtros detectados pelo LLM
            query: Query original do usuario

        Returns:
            Tuple[Dict, Dict]: (filtros_validos, filtros_rejeitados)
        """
        valid_filters = {}
        rejected_filters = {}

        query_normalized = QueryMentionValidator._normalize_text(query)

        for column, filter_spec in detected_filters.items():
            # Extrair valor
            if isinstance(filter_spec, dict):
                value = filter_spec.get("value")
                operator = filter_spec.get("operator", "=")
            else:
                value = filter_spec
                operator = "="

            # Validar se valor esta presente
            is_mentioned, reason = QueryMentionValidator._is_value_mentioned(
                value, query_normalized, column
            )

            if is_mentioned:
                valid_filters[column] = filter_spec
                logger.debug(
                    f"[QueryMentionValidator] ACEITO: {column}={value} "
                    f"(encontrado na query)"
                )
            else:
                rejected_filters[column] = {
                    "value": value,
                    "operator": operator,
                    "reason": reason,
                    "query": query
                }
                logger.warning(
                    f"[QueryMentionValidator] REJEITADO: {column}={value} "
                    f"(Motivo: {reason})"
                )

        if rejected_filters:
            logger.info(
                f"[QueryMentionValidator] Total rejeitados: {len(rejected_filters)} | "
                f"Validos: {len(valid_filters)}"
            )

        return valid_filters, rejected_filters

    @staticmethod
    def _normalize_text(text: str) -> str:
        """
        Normaliza texto para comparacao (case-insensitive, sem acentos).

        Args:
            text: Texto original

        Returns:
            str: Texto normalizado
        """
        # Lowercase
        normalized = text.lower()

        # Remover acentos comuns
        accent_map = {
            'á': 'a', 'à': 'a', 'ã': 'a', 'â': 'a',
            'é': 'e', 'ê': 'e',
            'í': 'i',
            'ó': 'o', 'õ': 'o', 'ô': 'o',
            'ú': 'u', 'ü': 'u',
            'ç': 'c'
        }
        for accented, plain in accent_map.items():
            normalized = normalized.replace(accented, plain)

        return normalized

    @staticmethod
    def _is_value_mentioned(
        value: Any,
        query_normalized: str,
        column: str
    ) -> Tuple[bool, str]:
        """
        Verifica se o valor foi mencionado na query.

        Args:
            value: Valor do filtro
            query_normalized: Query normalizada
            column: Nome da coluna

        Returns:
            Tuple[bool, str]: (foi_mencionado, razao_se_nao)
        """
        # Caso 1: Valor None ou vazio
        if value is None or value == "" or value == []:
            return False, "Valor vazio ou None"

        # Caso 2: Lista de valores
        if isinstance(value, list):
            # Para listas, pelo menos um valor deve estar presente
            for v in value:
                is_mentioned, _ = QueryMentionValidator._is_single_value_mentioned(
                    v, query_normalized, column
                )
                if is_mentioned:
                    # Se pelo menos um valor foi mencionado, aceita a lista
                    return True, ""

            # Nenhum valor da lista foi mencionado
            return False, f"Nenhum valor da lista {value} encontrado na query"

        # Caso 3: Valor unico
        return QueryMentionValidator._is_single_value_mentioned(
            value, query_normalized, column
        )

    @staticmethod
    def _is_single_value_mentioned(
        value: Any,
        query_normalized: str,
        column: str
    ) -> Tuple[bool, str]:
        """
        Verifica se um valor unico foi mencionado na query.

        Args:
            value: Valor do filtro (string, int, float)
            query_normalized: Query normalizada
            column: Nome da coluna

        Returns:
            Tuple[bool, str]: (foi_mencionado, razao_se_nao)
        """
        # Permitir valores inferidos para colunas temporais (datas formatadas)
        if column in ["Data", "Ano", "Mes", "Trimestre", "Semestre", "Dia"]:
            # Verificar se e padrao permitido (ex: "2015-01-01")
            if isinstance(value, str):
                for pattern in QueryMentionValidator.ALLOWED_INFERRED_PATTERNS:
                    if re.match(pattern, value):
                        logger.debug(
                            f"[QueryMentionValidator] Valor temporal inferido permitido: {value}"
                        )
                        return True, ""

        # Converter valor para string e normalizar
        value_str = str(value)
        value_normalized = QueryMentionValidator._normalize_text(value_str)

        # Busca 1: Exact match (case-insensitive)
        if value_normalized in query_normalized:
            return True, ""

        # Busca 2: Busca por tokens (palavras individuais)
        # Ex: "PRODUTOS REVENDA" -> verifica se "produtos" E "revenda" estao presentes
        tokens = value_normalized.split()
        if len(tokens) > 1:
            # Valor composto (ex: "PRODUTOS REVENDA")
            all_tokens_found = all(token in query_normalized for token in tokens)
            if all_tokens_found:
                # IMPORTANTE: Mesmo que todos os tokens estejam presentes,
                # verificar se NAO e apenas termo generico
                # Ex: query="produtos", valor="PRODUTOS REVENDA" -> NAO aceitar
                if len(tokens) == 2 and tokens[0] in ["produtos", "produto", "clientes", "cliente"]:
                    # Se primeiro token e generico, rejeitar
                    return False, f"Valor '{value}' parece ser inferencia a partir de termo generico '{tokens[0]}'"
                return True, ""

        # Busca 3: Fuzzy matching (para casos de typo ou variacao)
        similarity = SequenceMatcher(None, value_normalized, query_normalized).ratio()
        if similarity > QueryMentionValidator.FUZZY_SIMILARITY_THRESHOLD:
            return True, ""

        # Busca 4: Verificar variações comuns (plural/singular)
        variations = QueryMentionValidator._get_variations(value_normalized)
        for variation in variations:
            if variation in query_normalized:
                logger.debug(
                    f"[QueryMentionValidator] Encontrado variacao '{variation}' "
                    f"para valor '{value}'"
                )
                return True, ""

        # Valor NAO encontrado
        return False, f"Valor '{value}' nao encontrado na query (inferencia indevida)"

    @staticmethod
    def _get_variations(value: str) -> List[str]:
        """
        Gera variacoes comuns de um valor (plural/singular, etc).

        Args:
            value: Valor original

        Returns:
            List[str]: Lista de variacoes
        """
        variations = [value]

        # Plural -> Singular (remove 's' final)
        if value.endswith('s') and len(value) > 3:
            variations.append(value[:-1])

        # Singular -> Plural (adiciona 's')
        if not value.endswith('s'):
            variations.append(value + 's')

        # Remover/adicionar acentos em algumas palavras comuns
        # (ja normalizado, mas manter para referencia)

        return variations

    @staticmethod
    def validate_and_log(
        detected_filters: Dict[str, Any],
        query: str
    ) -> Dict[str, Any]:
        """
        Valida filtros e retorna apenas os validos, logando os rejeitados.

        Metodo de conveniencia que retorna apenas os filtros validos.

        Args:
            detected_filters: Filtros detectados
            query: Query original

        Returns:
            Dict: Apenas filtros validos
        """
        valid, rejected = QueryMentionValidator.validate_filters(
            detected_filters, query
        )

        if rejected:
            logger.warning(
                f"[QueryMentionValidator] Filtros inferidos indevidamente foram rejeitados: "
                f"{list(rejected.keys())}"
            )
            for col, info in rejected.items():
                logger.warning(
                    f"  - {col}: '{info['value']}' nao foi mencionado em '{info['query']}'"
                )

        return valid
