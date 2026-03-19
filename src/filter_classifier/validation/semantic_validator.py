"""
Semantic Validator for Filter Classifier

Valida filtros para garantir que apenas valores categoricos sejam aceitos,
rejeitando valores numericos que possam ter vindo de termos de ranking.
"""

import re
import logging
from typing import Dict, Any, List, Tuple

logger = logging.getLogger(__name__)


class SemanticValidator:
    """Validador semantico de filtros para prevenir aplicacao de termos quantitativos/ranking"""

    # Termos de ranking que indicam que a query NAO deve gerar filtros numericos
    RANKING_TERMS = [
        r"\btop\s+\d+",  # top 5, top 10
        r"\bos\s+\d+\s+maiores",  # os 5 maiores
        r"\bos\s+\d+\s+menores",  # os 3 menores
        r"\bos\s+\d+\s+melhores",  # os 5 melhores
        r"\bos\s+\d+\s+piores",  # os 3 piores
        r"\b\d+\s+maiores",  # 5 maiores, 10 maiores
        r"\b\d+\s+menores",  # 3 menores
        r"\b\d+\s+melhores",  # 5 melhores
        r"\b\d+\s+piores",  # 3 piores
        r"\b\d+\s+primeiros",  # 5 primeiros
        r"\b\d+\s+ultimos",  # 10 ultimos
        r"\bmelhores\s+\d+",  # melhores 5
        r"\bpiores\s+\d+",  # piores 3
        r"\bprincipais\s+\d+",  # principais 10
        r"\bentre\s+os\s+\d+",  # entre os 5
    ]

    # Colunas que devem SEMPRE ter valores categoricos (nunca numericos)
    # Carregadas dinamicamente de alias.yaml em _load_column_sets()
    CATEGORICAL_COLUMNS = set()

    # Colunas pontuais que devem ser removidas em queries de ranking
    # Inclui metricas numericas - carregadas dinamicamente
    PONTUAL_COLUMNS = set()

    # Colunas temporais (para aceitar datas como filtro)
    _TEMPORAL_COLUMNS = set()

    _columns_loaded = False

    @classmethod
    def _load_column_sets(cls):
        """Carrega conjuntos de colunas dinamicamente de alias.yaml."""
        if cls._columns_loaded:
            return
        try:
            from src.shared_lib.core.config import (
                get_dimension_columns,
                get_metric_columns,
                get_temporal_columns,
            )

            cls.CATEGORICAL_COLUMNS = set(get_dimension_columns())
            cls.PONTUAL_COLUMNS = set(get_metric_columns())
            cls._TEMPORAL_COLUMNS = set(get_temporal_columns())
            # Adicionar colunas virtuais temporais se existirem
            if cls._TEMPORAL_COLUMNS:
                cls._TEMPORAL_COLUMNS.update({"Ano", "Mes", "Trimestre", "Semestre"})
            cls._columns_loaded = True
            logger.debug(
                f"[SemanticValidator] Colunas carregadas: "
                f"{len(cls.CATEGORICAL_COLUMNS)} categoricas, "
                f"{len(cls.PONTUAL_COLUMNS)} pontuais, "
                f"{len(cls._TEMPORAL_COLUMNS)} temporais"
            )
        except Exception as e:
            logger.warning(
                f"[SemanticValidator] Falha ao carregar colunas de alias.yaml: {e}"
            )
            cls._columns_loaded = True  # Evitar retentativas

    @staticmethod
    def detect_ranking_terms(query: str) -> Tuple[bool, List[str]]:
        """
        Detecta presenca de termos de ranking na query.

        Args:
            query: Query do usuario

        Returns:
            Tuple[bool, List[str]]: (tem_ranking, lista_de_termos_encontrados)
        """
        query_lower = query.lower()
        found_terms = []

        for pattern in SemanticValidator.RANKING_TERMS:
            matches = re.findall(pattern, query_lower)
            if matches:
                found_terms.extend(matches)

        has_ranking = len(found_terms) > 0

        if has_ranking:
            logger.debug(
                f"[SemanticValidator] Termos de ranking detectados: {found_terms}"
            )

        return has_ranking, found_terms

    @staticmethod
    def validate_non_quantitative_filters(
        detected_filters: Dict[str, Any], query: str = ""
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Valida filtros para garantir que apenas valores categoricos sejam aceitos.
        Remove filtros com valores numericos isolados.

        Args:
            detected_filters: Filtros detectados pelo LLM
            query: Query original (para detectar ranking)

        Returns:
            Tuple[Dict, Dict]: (filtros_validos, filtros_rejeitados)
        """
        valid_filters = {}
        rejected_filters = {}

        # Carregar colunas dinamicamente (lazy)
        SemanticValidator._load_column_sets()

        # Detectar se query contem termos de ranking
        has_ranking, ranking_terms = SemanticValidator.detect_ranking_terms(query)

        for column, filter_spec in detected_filters.items():
            # Extrair valor do filtro
            if isinstance(filter_spec, dict):
                value = filter_spec.get("value")
                operator = filter_spec.get("operator", "=")
            else:
                value = filter_spec
                operator = "="

            # Validar tipo do valor
            is_valid, rejection_reason = SemanticValidator._validate_filter_value(
                column, value, operator, has_ranking
            )

            if is_valid:
                valid_filters[column] = filter_spec
            else:
                rejected_filters[column] = {
                    "value": value,
                    "operator": operator,
                    "reason": rejection_reason,
                    "ranking_detected": has_ranking,
                }
                logger.warning(
                    f"[SemanticValidator] Filtro rejeitado: {column}={value} "
                    f"(Motivo: {rejection_reason})"
                )

        if rejected_filters:
            logger.info(
                f"[SemanticValidator] Total de filtros rejeitados: {len(rejected_filters)} "
                f"| Validos: {len(valid_filters)}"
            )

        return valid_filters, rejected_filters

    @staticmethod
    def _validate_filter_value(
        column: str, value: Any, operator: str, has_ranking: bool
    ) -> Tuple[bool, str]:
        """
        Valida se o valor do filtro e aceitavel (categorico).

        Args:
            column: Nome da coluna
            value: Valor do filtro
            operator: Operador (=, >, <, between, etc)
            has_ranking: Se a query contem termos de ranking

        Returns:
            Tuple[bool, str]: (e_valido, razao_rejeicao)
        """
        # Regra 1: Valores None ou vazios sao invalidos
        if value is None or value == "" or value == []:
            return False, "Valor vazio ou None"

        # Regra 2: Valores numericos isolados (int, float) sao SEMPRE rejeitados
        if isinstance(value, (int, float)):
            return False, f"Valor numerico isolado ({type(value).__name__})"

        # Regra 3: Listas de numeros sao rejeitadas
        if isinstance(value, list):
            # Verificar se e lista de numeros
            if all(isinstance(v, (int, float)) for v in value):
                return False, "Lista de valores numericos"

            # Verificar se e lista vazia
            if len(value) == 0:
                return False, "Lista vazia"

            # Lista de strings e aceitavel (ex: ["SP", "RJ", "MG"])
            if all(isinstance(v, str) for v in value):
                return True, ""

        # Regra 4: Strings sao aceitaveis (valores categoricos)
        if isinstance(value, str):
            # Carregar colunas dinamicamente (lazy)
            SemanticValidator._load_column_sets()

            # Verificar se e coluna temporal - aceitar datas
            if column in SemanticValidator._TEMPORAL_COLUMNS:
                return True, ""

            # Verificar se e coluna categorica
            if column in SemanticValidator.CATEGORICAL_COLUMNS:
                return True, ""

            # Se nao e coluna conhecida, aceitar string por padrao
            # (pode ser nome de produto, cliente, etc)
            return True, ""

        # Regra 5: Colunas pontuais devem ser rejeitadas se query tem ranking
        if column in SemanticValidator.PONTUAL_COLUMNS and has_ranking:
            return False, f"Coluna pontual ({column}) em query de ranking"

        # Regra 6: Se chegou aqui e nao passou pelas validacoes acima,
        # e provavel que seja tipo complexo (dict, etc) - rejeitar
        return False, f"Tipo de valor nao suportado ({type(value).__name__})"

    @staticmethod
    def validate_filter_type_consistency(
        column: str, value: Any, dataset_columns: Dict[str, str]
    ) -> Tuple[bool, str]:
        """
        Valida se o tipo do valor e compativel com o tipo da coluna no dataset.

        Args:
            column: Nome da coluna
            value: Valor do filtro
            dataset_columns: Mapa de colunas do dataset com seus tipos

        Returns:
            Tuple[bool, str]: (e_consistente, mensagem_erro)
        """
        # Se coluna nao existe no dataset, nao podemos validar
        if column not in dataset_columns:
            return True, ""  # Aceitar (sera validado em outro lugar)

        column_type = dataset_columns[column]

        # Validar tipos basicos
        if column_type in ["VARCHAR", "TEXT", "STRING"]:
            # Aceitar strings ou listas de strings
            if isinstance(value, str):
                return True, ""
            if isinstance(value, list) and all(isinstance(v, str) for v in value):
                return True, ""
            return (
                False,
                f"Coluna {column} espera STRING, recebeu {type(value).__name__}",
            )

        if column_type in ["INTEGER", "BIGINT", "INT"]:
            # Para colunas numericas, aceitar apenas em contextos especificos
            # (ja foi validado em _validate_filter_value)
            if isinstance(value, (int, float)):
                # Verificar se e coluna de ID (aceitavel)
                if "Cod_" in column or "_ID" in column:
                    return True, ""
                return False, f"Valor numerico nao aceitavel para coluna {column}"
            return (
                False,
                f"Coluna {column} espera INTEGER, recebeu {type(value).__name__}",
            )

        if column_type in ["DATE", "TIMESTAMP", "DATETIME"]:
            # Aceitar strings de data
            if isinstance(value, str):
                return True, ""
            if isinstance(value, list) and all(isinstance(v, str) for v in value):
                return True, ""
            return False, f"Coluna {column} espera DATE, recebeu {type(value).__name__}"

        # Tipo nao reconhecido - aceitar por padrao
        return True, ""

    @staticmethod
    def should_remove_pontual_filters(
        current_filters: Dict[str, Any], query: str
    ) -> List[str]:
        """
        Identifica filtros pontuais que devem ser removidos baseado na query.

        Args:
            current_filters: Filtros atualmente ativos
            query: Query do usuario

        Returns:
            List[str]: Lista de colunas que devem ser removidas
        """
        to_remove = []

        # Detectar se query contem termos de ranking
        has_ranking, _ = SemanticValidator.detect_ranking_terms(query)

        if not has_ranking:
            return to_remove

        # Carregar colunas dinamicamente (lazy)
        SemanticValidator._load_column_sets()

        # Remover filtros pontuais se query tem ranking
        for column in current_filters.keys():
            if column in SemanticValidator.PONTUAL_COLUMNS:
                to_remove.append(column)
                logger.debug(
                    f"[SemanticValidator] Marcando filtro pontual para remocao: {column} "
                    f"(query contem termos de ranking)"
                )

        return to_remove
