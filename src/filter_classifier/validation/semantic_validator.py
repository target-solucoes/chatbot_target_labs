"""
Semantic Validator for Filter Classifier

Simplified in Phase 3 to focus exclusively on ranking term detection.
All value/type validation is now handled by ValueCatalog and PreMatchEngine.
"""

import re
import logging
from typing import Dict, Any, List, Tuple

logger = logging.getLogger(__name__)


class SemanticValidator:
    """Validates filter semantics: detects ranking terms and pontual filter removal."""

    # Termos de ranking que indicam que a query NAO deve gerar filtros numericos
    RANKING_TERMS = [
        r"\btop\s+\d+",
        r"\bos\s+\d+\s+maiores",
        r"\bos\s+\d+\s+menores",
        r"\bos\s+\d+\s+melhores",
        r"\bos\s+\d+\s+piores",
        r"\b\d+\s+maiores",
        r"\b\d+\s+menores",
        r"\b\d+\s+melhores",
        r"\b\d+\s+piores",
        r"\b\d+\s+primeiros",
        r"\b\d+\s+ultimos",
        r"\bmelhores\s+\d+",
        r"\bpiores\s+\d+",
        r"\bprincipais\s+\d+",
        r"\bentre\s+os\s+\d+",
    ]

    # Column sets loaded lazily from alias.yaml
    PONTUAL_COLUMNS = set()
    _columns_loaded = False

    @classmethod
    def _load_column_sets(cls):
        """Carrega conjuntos de colunas dinamicamente de alias.yaml."""
        if cls._columns_loaded:
            return
        try:
            from src.shared_lib.core.config import get_metric_columns
            cls.PONTUAL_COLUMNS = set(get_metric_columns())
            cls._columns_loaded = True
        except Exception as e:
            logger.warning(f"[SemanticValidator] Falha ao carregar colunas: {e}")
            cls._columns_loaded = True

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
            logger.debug(f"[SemanticValidator] Termos de ranking detectados: {found_terms}")

        return has_ranking, found_terms

    @staticmethod
    def should_remove_pontual_filters(
        current_filters: Dict[str, Any], query: str
    ) -> List[str]:
        """
        Identifica filtros pontuais que devem ser removidos em queries de ranking.

        Args:
            current_filters: Filtros atualmente ativos
            query: Query do usuario

        Returns:
            List[str]: Lista de colunas que devem ser removidas
        """
        has_ranking, _ = SemanticValidator.detect_ranking_terms(query)
        if not has_ranking:
            return []

        SemanticValidator._load_column_sets()

        to_remove = []
        for column in current_filters:
            if column in SemanticValidator.PONTUAL_COLUMNS:
                to_remove.append(column)
                logger.debug(
                    f"[SemanticValidator] Marcando filtro pontual para remocao: {column}"
                )

        return to_remove
