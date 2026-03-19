"""
Metadata cache for non_graph_executor.

This module implements intelligent metadata caching with lazy loading
that differentiates between global and filtered metadata.
"""

import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

try:
    import duckdb

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class MetadataCache:
    """
    Cache inteligente de metadados do dataset.

    Estratégia:
    - Global metadata: calculado uma vez na primeira solicitação (lazy loading)
    - Filtered metadata: calculado on-demand com hash de filtros como chave
    - Invalidação automática ao detectar mudança no dataset (via mtime)

    Performance:
    - Lazy loading evita computação desnecessária na inicialização
    - Cache diferenciado global/filtrado evita invalidações desnecessárias
    - Uma única conexão DuckDB por computação para otimizar performance

    Example:
        >>> cache = MetadataCache("data/dataset.parquet")
        >>> # Primeira chamada: computa metadados
        >>> global_meta = cache.get_global_metadata()
        >>> # Segunda chamada: retorna cache (instantâneo)
        >>> global_meta2 = cache.get_global_metadata()
        >>> # Metadados filtrados
        >>> filtered_meta = cache.get_filtered_metadata({"Ano": 2015})
    """

    def __init__(self, data_source: str):
        """
        Initialize metadata cache.

        Args:
            data_source: Path to dataset file (parquet, csv, etc)

        Raises:
            FileNotFoundError: Se dataset não existe
            RuntimeError: Se DuckDB não está disponível
        """
        if not DUCKDB_AVAILABLE:
            raise RuntimeError(
                "DuckDB is required for MetadataCache. Install with: pip install duckdb"
            )

        self.data_source = data_source

        # Validar que dataset existe
        if not Path(data_source).exists():
            raise FileNotFoundError(f"Dataset not found: {data_source}")

        # Cache de metadados globais (None = não computado ainda - lazy loading)
        self._global_cache: Optional[Dict[str, Any]] = None

        # Cache de metadados filtrados (key = hash de filtros)
        self._filtered_cache: Dict[str, Dict[str, Any]] = {}

        # Timestamp de modificação do dataset para invalidação
        self._dataset_mtime: float = self._get_dataset_mtime()

        logger.info(f"MetadataCache initialized for: {data_source}")
        logger.debug(f"Initial dataset mtime: {self._dataset_mtime}")

    def get_global_metadata(self) -> Dict[str, Any]:
        """
        Retorna metadados globais do dataset com lazy loading.

        Na primeira chamada, computa os metadados via DuckDB.
        Chamadas subsequentes retornam cache (instantâneo).
        Verifica invalidação antes de retornar cache.

        Returns:
            Dict contendo:
                - shape: {rows: int, cols: int}
                - columns: List[str]
                - dtypes: Dict[str, str] (coluna -> tipo DuckDB)
                - null_counts: Dict[str, int]
                - unique_counts: Dict[str, int] (apenas colunas VARCHAR)
                - numeric_stats: Dict[str, Dict] (apenas colunas numéricas)

        Example:
            >>> metadata = cache.get_global_metadata()
            >>> print(f"Dataset has {metadata['shape']['rows']} rows")
            >>> print(f"Columns: {metadata['columns']}")
        """
        # Verificar se cache precisa ser invalidado
        if self._check_invalidation():
            logger.info("Dataset modified detected, invalidating cache")
            self.invalidate_cache()

        # Se cache existe, retornar (lazy loading)
        if self._global_cache is not None:
            logger.debug("Returning cached global metadata")
            return self._global_cache

        # Cache vazio: computar metadados
        logger.info("Computing global metadata (first time)")
        self._global_cache = self._compute_global_metadata()

        return self._global_cache

    def get_filtered_metadata(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Retorna metadados para subset filtrado do dataset.

        Usa hash dos filtros como chave de cache.
        Se cache existe para este conjunto de filtros, retorna cache.
        Senão, computa metadados aplicando filtros via WHERE clause.

        Args:
            filters: Dict de filtros a aplicar
                Formatos suportados:
                - {"Ano": 2015} → WHERE Ano = 2015
                - {"UF": ["SP", "RJ"]} → WHERE UF IN ('SP', 'RJ')
                - {"Valor": {"operator": ">=", "value": 1000}}

        Returns:
            Dict com mesma estrutura de get_global_metadata(),
            mas calculado sobre subset filtrado

        Example:
            >>> filters = {"Ano": 2015, "UF": "SP"}
            >>> metadata = cache.get_filtered_metadata(filters)
            >>> print(f"Filtered dataset has {metadata['shape']['rows']} rows")
        """
        # Gerar hash único para conjunto de filtros
        filter_hash = self._hash_filters(filters)

        # Verificar se já existe em cache
        if filter_hash in self._filtered_cache:
            logger.debug(
                f"Returning cached filtered metadata (hash: {filter_hash[:8]})"
            )
            return self._filtered_cache[filter_hash]

        # Computar metadados filtrados
        logger.info(f"Computing filtered metadata for filters: {filters}")
        filtered_metadata = self._compute_filtered_metadata(filters)

        # Adicionar ao cache
        self._filtered_cache[filter_hash] = filtered_metadata

        return filtered_metadata

    def get_exact_column_names(self) -> list[str]:
        """
        Retorna nomes EXATOS das colunas do dataset.

        Reutiliza get_global_metadata() que ja tem cache, evitando
        overhead adicional. Usa nomes exatos como aparecem no schema
        do dataset (case-sensitive).

        Returns:
            Lista de nomes de colunas exatamente como no dataset

        Example:
            >>> cache = MetadataCache("data.parquet")
            >>> columns = cache.get_exact_column_names()
            >>> print(columns)
            ['VALOR_VENDIDO', 'MUNICIPIO_CLIENTE', 'Ano']
        """
        metadata = self.get_global_metadata()
        return metadata.get("columns", [])

    def invalidate_cache(self) -> None:
        """
        Invalida todos os caches (global e filtrados).

        Chamado quando detecta mudança no dataset ou manualmente.
        Atualiza timestamp de modificação do dataset.
        """
        logger.info("Invalidating all caches")
        self._global_cache = None
        self._filtered_cache.clear()
        self._dataset_mtime = self._get_dataset_mtime()
        logger.debug(f"Cache invalidated, new mtime: {self._dataset_mtime}")

    def _compute_global_metadata(self) -> Dict[str, Any]:
        """
        Computa metadados globais via queries DuckDB.

        Executa múltiplas queries em uma única conexão para otimizar performance:
        1. Shape (count rows)
        2. Schema (DESCRIBE para colunas e tipos)
        3. Null counts (para cada coluna)
        4. Unique counts (apenas colunas VARCHAR/TEXT)
        5. Numeric stats (apenas colunas numéricas)

        Returns:
            Dict com todos os metadados computados
        """
        logger.debug("Starting global metadata computation")

        try:
            # Usar conexão efêmera (abrir, executar, fechar)
            conn = duckdb.connect()

            # Query 1: Shape (total de linhas)
            row_count_query = f"SELECT COUNT(*) as total FROM '{self.data_source}'"
            row_count_result = conn.execute(row_count_query).fetchone()
            total_rows = row_count_result[0] if row_count_result else 0

            # Query 2: Schema (colunas e tipos)
            describe_query = f"DESCRIBE SELECT * FROM '{self.data_source}'"
            schema_result = conn.execute(describe_query).fetchall()

            # Processar schema
            columns = []
            dtypes = {}
            for row in schema_result:
                col_name = row[0]
                col_type = row[1]
                columns.append(col_name)
                dtypes[col_name] = col_type

            total_cols = len(columns)

            logger.debug(f"Dataset shape: {total_rows} rows x {total_cols} cols")

            # Query 3: Null counts (para cada coluna)
            null_counts = {}
            for col in columns:
                null_query = (
                    f"SELECT COUNT(*) FROM '{self.data_source}' WHERE \"{col}\" IS NULL"
                )
                null_result = conn.execute(null_query).fetchone()
                null_counts[col] = null_result[0] if null_result else 0

            logger.debug(f"Computed null counts for {len(columns)} columns")

            # Query 4: Unique counts (apenas colunas VARCHAR/TEXT)
            unique_counts = {}
            for col, dtype in dtypes.items():
                if "VARCHAR" in dtype.upper() or "TEXT" in dtype.upper():
                    unique_query = (
                        f"SELECT COUNT(DISTINCT \"{col}\") FROM '{self.data_source}'"
                    )
                    unique_result = conn.execute(unique_query).fetchone()
                    unique_counts[col] = unique_result[0] if unique_result else 0

            logger.debug(
                f"Computed unique counts for {len(unique_counts)} categorical columns"
            )

            # Query 5: Numeric stats (apenas colunas numéricas)
            numeric_stats = {}
            numeric_types = [
                "INTEGER",
                "BIGINT",
                "DOUBLE",
                "DECIMAL",
                "FLOAT",
                "NUMERIC",
            ]

            for col, dtype in dtypes.items():
                # Verificar se tipo é numérico
                is_numeric = any(nt in dtype.upper() for nt in numeric_types)

                if is_numeric:
                    stats_query = f"""
                        SELECT 
                            MIN(\"{col}\") as min_val,
                            MAX(\"{col}\") as max_val,
                            AVG(\"{col}\") as mean_val,
                            MEDIAN(\"{col}\") as median_val
                        FROM '{self.data_source}'
                    """
                    stats_result = conn.execute(stats_query).fetchone()

                    if stats_result:
                        numeric_stats[col] = {
                            "min": stats_result[0],
                            "max": stats_result[1],
                            "mean": stats_result[2],
                            "median": stats_result[3],
                        }

            logger.debug(
                f"Computed numeric stats for {len(numeric_stats)} numeric columns"
            )

            # Fechar conexão
            conn.close()

            # Montar resultado final
            metadata = {
                "shape": {"rows": total_rows, "cols": total_cols},
                "columns": columns,
                "dtypes": dtypes,
                "null_counts": null_counts,
                "unique_counts": unique_counts,
                "numeric_stats": numeric_stats,
            }

            logger.info("Global metadata computation completed successfully")
            return metadata

        except Exception as e:
            logger.error(f"Error computing global metadata: {e}")
            raise RuntimeError(f"Failed to compute global metadata: {e}") from e

    def _compute_filtered_metadata(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Computa metadados para subset filtrado via WHERE clause.

        Similar a _compute_global_metadata, mas adiciona WHERE clause
        em todas as queries baseado nos filtros fornecidos.

        Args:
            filters: Dict de filtros a aplicar

        Returns:
            Dict com mesma estrutura de _compute_global_metadata
        """
        logger.debug(f"Starting filtered metadata computation with filters: {filters}")

        try:
            # Construir WHERE clause
            where_clause = self._build_where_clause(filters)

            # Usar conexão efêmera
            conn = duckdb.connect()

            # Query 1: Shape (total de linhas filtradas)
            row_count_query = f"SELECT COUNT(*) as total FROM '{self.data_source}' WHERE {where_clause}"
            row_count_result = conn.execute(row_count_query).fetchone()
            total_rows = row_count_result[0] if row_count_result else 0

            # Query 2: Schema (colunas e tipos - sem WHERE pois estrutura não muda)
            describe_query = f"DESCRIBE SELECT * FROM '{self.data_source}'"
            schema_result = conn.execute(describe_query).fetchall()

            # Processar schema
            columns = []
            dtypes = {}
            for row in schema_result:
                col_name = row[0]
                col_type = row[1]
                columns.append(col_name)
                dtypes[col_name] = col_type

            total_cols = len(columns)

            logger.debug(
                f"Filtered dataset shape: {total_rows} rows x {total_cols} cols"
            )

            # Query 3: Null counts (com WHERE)
            null_counts = {}
            for col in columns:
                null_query = f"SELECT COUNT(*) FROM '{self.data_source}' WHERE {where_clause} AND \"{col}\" IS NULL"
                null_result = conn.execute(null_query).fetchone()
                null_counts[col] = null_result[0] if null_result else 0

            # Query 4: Unique counts (com WHERE, apenas VARCHAR/TEXT)
            unique_counts = {}
            for col, dtype in dtypes.items():
                if "VARCHAR" in dtype.upper() or "TEXT" in dtype.upper():
                    unique_query = f"SELECT COUNT(DISTINCT \"{col}\") FROM '{self.data_source}' WHERE {where_clause}"
                    unique_result = conn.execute(unique_query).fetchone()
                    unique_counts[col] = unique_result[0] if unique_result else 0

            # Query 5: Numeric stats (com WHERE, apenas colunas numéricas)
            numeric_stats = {}
            numeric_types = [
                "INTEGER",
                "BIGINT",
                "DOUBLE",
                "DECIMAL",
                "FLOAT",
                "NUMERIC",
            ]

            for col, dtype in dtypes.items():
                is_numeric = any(nt in dtype.upper() for nt in numeric_types)

                if is_numeric:
                    stats_query = f"""
                        SELECT 
                            MIN(\"{col}\") as min_val,
                            MAX(\"{col}\") as max_val,
                            AVG(\"{col}\") as mean_val,
                            MEDIAN(\"{col}\") as median_val
                        FROM '{self.data_source}'
                        WHERE {where_clause}
                    """
                    stats_result = conn.execute(stats_query).fetchone()

                    if stats_result:
                        numeric_stats[col] = {
                            "min": stats_result[0],
                            "max": stats_result[1],
                            "mean": stats_result[2],
                            "median": stats_result[3],
                        }

            # Fechar conexão
            conn.close()

            # Montar resultado final
            metadata = {
                "shape": {"rows": total_rows, "cols": total_cols},
                "columns": columns,
                "dtypes": dtypes,
                "null_counts": null_counts,
                "unique_counts": unique_counts,
                "numeric_stats": numeric_stats,
                "filters_applied": filters,
            }

            logger.info("Filtered metadata computation completed successfully")
            return metadata

        except Exception as e:
            logger.error(f"Error computing filtered metadata: {e}")
            raise RuntimeError(f"Failed to compute filtered metadata: {e}") from e

    def _build_where_clause(self, filters: Dict[str, Any]) -> str:
        """
        Constrói WHERE clause a partir de dict de filtros.

        Suporta múltiplos formatos:
        - Equality: {"Ano": 2015} → "Ano = 2015"
        - IN clause: {"UF": ["SP", "RJ"]} → "UF IN ('SP', 'RJ')"
        - Operator: {"Valor": {"operator": ">=", "value": 1000}} → "Valor >= 1000"
        - Between: {"Data": {"between": [start, end]}} → "Data BETWEEN start AND end"

        Args:
            filters: Dict de filtros

        Returns:
            WHERE clause string (sem palavra "WHERE")

        Example:
            >>> clause = cache._build_where_clause({"Ano": 2015, "UF": ["SP", "RJ"]})
            >>> print(clause)
            "Ano = 2015 AND UF IN ('SP', 'RJ')"
        """
        if not filters:
            return "1=1"  # WHERE sem filtros

        conditions = []

        for col, value in filters.items():
            # Escapar nome da coluna com aspas duplas
            col_escaped = f'"{col}"'

            # Case 1: IN clause (lista de valores)
            if isinstance(value, list):
                # Escapar valores string com aspas simples
                values_escaped = []
                for v in value:
                    if isinstance(v, str):
                        v_escaped = v.replace("'", "''")  # Escape aspas simples
                        values_escaped.append(f"'{v_escaped}'")
                    else:
                        values_escaped.append(str(v))

                values_str = ", ".join(values_escaped)
                conditions.append(f"{col_escaped} IN ({values_str})")

            # Case 2: Operator dict
            elif isinstance(value, dict):
                if "operator" in value:
                    operator = value["operator"]
                    val = value["value"]

                    # Escapar valor se string
                    if isinstance(val, str):
                        val_escaped = val.replace("'", "''")
                        val_str = f"'{val_escaped}'"
                    else:
                        val_str = str(val)

                    conditions.append(f"{col_escaped} {operator} {val_str}")

                # Case 3: BETWEEN
                elif "between" in value:
                    start, end = value["between"]

                    # Escapar valores se string
                    if isinstance(start, str):
                        start_escaped = start.replace("'", "''")
                        start_str = f"'{start_escaped}'"
                    else:
                        start_str = str(start)

                    if isinstance(end, str):
                        end_escaped = end.replace("'", "''")
                        end_str = f"'{end_escaped}'"
                    else:
                        end_str = str(end)

                    conditions.append(
                        f"{col_escaped} BETWEEN {start_str} AND {end_str}"
                    )

            # Case 4: Equality (valor simples)
            else:
                # Escapar valor se string
                if isinstance(value, str):
                    value_escaped = value.replace("'", "''")
                    value_str = f"'{value_escaped}'"
                else:
                    value_str = str(value)

                conditions.append(f"{col_escaped} = {value_str}")

        # Combinar condições com AND
        where_clause = " AND ".join(conditions)

        return where_clause

    def _hash_filters(self, filters: Dict[str, Any]) -> str:
        """
        Gera hash MD5 único para conjunto de filtros.

        Ordena chaves do dict para garantir consistência:
        {"B": 2, "A": 1} e {"A": 1, "B": 2} geram mesmo hash.

        Args:
            filters: Dict de filtros

        Returns:
            Hash MD5 hexadecimal (32 caracteres)

        Example:
            >>> hash1 = cache._hash_filters({"Ano": 2015, "UF": "SP"})
            >>> hash2 = cache._hash_filters({"UF": "SP", "Ano": 2015})
            >>> assert hash1 == hash2  # Mesma ordem lógica
        """
        # Ordenar chaves para consistência
        sorted_filters = dict(sorted(filters.items()))

        # Serializar para JSON (determinístico)
        filters_json = json.dumps(sorted_filters, sort_keys=True)

        # Gerar hash MD5
        hash_obj = hashlib.md5(filters_json.encode("utf-8"))
        hash_hex = hash_obj.hexdigest()

        return hash_hex

    def _check_invalidation(self) -> bool:
        """
        Verifica se dataset foi modificado desde último cache.

        Compara timestamp de modificação atual com timestamp armazenado.

        Returns:
            True se dataset foi modificado (cache deve ser invalidado)
            False se dataset não mudou (cache ainda válido)
        """
        current_mtime = self._get_dataset_mtime()

        # Se mtime mudou, cache deve ser invalidado
        if current_mtime != self._dataset_mtime:
            logger.debug(
                f"Dataset modification detected: "
                f"cached={self._dataset_mtime}, current={current_mtime}"
            )
            return True

        return False

    def _get_dataset_mtime(self) -> float:
        """
        Obtém timestamp de modificação do dataset.

        Returns:
            Timestamp de modificação (float)
        """
        try:
            return os.path.getmtime(self.data_source)
        except OSError as e:
            logger.warning(f"Could not get mtime for {self.data_source}: {e}")
            return 0.0
