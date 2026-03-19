"""
DatasetMaxDateCache - Cache LRU para data máxima de datasets.

Este módulo fornece cache eficiente para informações de data máxima de datasets,
evitando múltiplas queries custosas ao banco de dados.

Estratégia:
- Usa DuckDB para queries diretas no Parquet (zero data loading)
- Cache LRU para até 10 datasets
- Extrai múltiplas granularidades em uma única query
"""

import logging
import pandas as pd
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Union, Optional

logger = logging.getLogger(__name__)


@dataclass
class MaxDateInfo:
    """
    Informações sobre a data máxima de um dataset.

    Attributes:
        max_date: datetime da data máxima encontrada
        max_year: Ano máximo (int)
        max_month: Mês máximo (1-12)
        max_quarter: Trimestre máximo (1-4)
        max_semester: Semestre máximo (1-2)
        max_bimester: Bimestre máximo (1-6)
        dataset_path: Caminho do dataset consultado
    """
    max_date: datetime
    max_year: int
    max_month: int
    max_quarter: int
    max_semester: int
    max_bimester: int


class DatasetMaxDateCache:
    """
    Cache LRU para data máxima de datasets.

    Esta classe fornece acesso eficiente à data máxima de um dataset,
    utilizando DuckDB para queries rápidas sem carregar dados em memória.

    Features:
    - Cache LRU com até 10 datasets
    - Query direta via DuckDB (zero data loading)
    - Cálculo automático de granularidades (mês, trimestre, semestre)
    - Thread-safe para uso em aplicações multi-threaded
    - Graceful degradation: retorna None se dataset não tem coluna temporal

    Example:
        >>> cache = DatasetMaxDateCache()
        >>> info = cache.get_max_date("data/datasets/dataset.parquet")
        >>> if info:
        ...     print(info.max_date)
    """

    def __init__(self):
        """Initialize cache."""
        self.logger = logging.getLogger(__name__)
        logger.info("[DatasetMaxDateCache] Initialized")

    @staticmethod
    def _get_temporal_column() -> Optional[str]:
        """
        Get the temporal column name from alias.yaml.

        Returns:
            First temporal column name, or None if no temporal columns exist.
        """
        try:
            from src.shared_lib.core.config import get_temporal_columns
            temporal_cols = get_temporal_columns()
            return temporal_cols[0] if temporal_cols else None
        except Exception:
            return None

    @lru_cache(maxsize=10)
    def get_max_date(self, dataset_path: str) -> Optional["MaxDateInfo"]:
        """
        Busca e cacheia informações sobre a data máxima no dataset.

        Args:
            dataset_path: Caminho para o arquivo parquet/csv do dataset

        Returns:
            MaxDateInfo com informações sobre data máxima, ou None se
            o dataset não possui coluna temporal configurada em alias.yaml.

        Raises:
            ValueError: Se falhar ao executar a query (dataset corrompido, etc.)
        """
        # Check if dataset has temporal columns configured
        temporal_col = self._get_temporal_column()
        if not temporal_col:
            logger.info(
                "[DatasetMaxDateCache] No temporal column configured in alias.yaml. "
                "Skipping max date extraction."
            )
            return None

        logger.info(
            f"[DatasetMaxDateCache] Fetching max date from {dataset_path} "
            f"(temporal column: {temporal_col})"
        )

        try:
            import duckdb

            result = duckdb.query(
                f"""
                WITH max_date_row AS (
                    SELECT MAX("{temporal_col}") as max_date FROM '{dataset_path}'
                )
                SELECT
                    max_date,
                    YEAR(max_date) as max_year,
                    MONTH(max_date) as max_month,
                    QUARTER(max_date) as max_quarter
                FROM max_date_row
                """
            ).to_df()

            if result.loc[0, "max_date"] is None or pd.isna(result.loc[0, "max_date"]):
                logger.warning(
                    f"[DatasetMaxDateCache] Temporal column '{temporal_col}' has no valid dates"
                )
                return None

            max_date = pd.to_datetime(result.loc[0, "max_date"])
            max_year = int(result.loc[0, "max_year"])
            max_month = int(result.loc[0, "max_month"])
            max_quarter = int(result.loc[0, "max_quarter"])
            max_semester = 1 if max_month <= 6 else 2
            max_bimester = (max_month - 1) // 2 + 1

            logger.info(
                f"[DatasetMaxDateCache] Fetched max date from dataset: "
                f"{max_date.date()} (Year: {max_year}, Month: {max_month})"
            )

            return MaxDateInfo(
                max_date=max_date,
                max_year=max_year,
                max_month=max_month,
                max_quarter=max_quarter,
                max_semester=max_semester,
                max_bimester=max_bimester,
            )

        except Exception as e:
            logger.error(f"[DatasetMaxDateCache] Error fetching max date: {str(e)}")
            raise ValueError(f"Failed to fetch max date from dataset: {str(e)}")


# Global cache instance
_cache_instance = DatasetMaxDateCache()


def get_max_date_info(dataset_path: str) -> Optional[MaxDateInfo]:
    """
    Função conveniente para obter informações de data máxima do dataset.

    Args:
        dataset_path: Caminho para o arquivo do dataset.

    Returns:
        MaxDateInfo com informações da data máxima, ou None se
        o dataset não possui coluna temporal configurada.
    """
    return _cache_instance.get_max_date(dataset_path)
