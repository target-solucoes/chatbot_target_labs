"""
Utilitário para limitar e agregar categorias em datasets de visualização.

Este módulo implementa a lógica de limitação de categorias para gráficos,
garantindo que visualizações não fiquem sobrecarregadas com muitas categorias.

Características:
- Limita número de categorias exibidas
- Agrega categorias excedentes em "OUTROS" (para pie charts)
- Preserva ordenação e totais
- Reutilizável em qualquer tipo de gráfico
"""

from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class CategoryLimiter:
    """
    Classe responsável por limitar e agregar categorias em datasets.

    Esta classe implementa a lógica central de limitação de categorias,
    permitindo que gráficos exibam apenas as N categorias mais relevantes
    (baseado em uma métrica de ordenação) e opcionalmente agregue as
    demais em uma categoria "OUTROS".

    Uso típico:
        1. Gráficos de barras: Mostra top N, descarta o resto
        2. Gráficos de pizza: Mostra top N, agrega resto em "OUTROS"

    Exemplo:
        >>> limiter = CategoryLimiter(max_categories=5)
        >>> data = [
        ...     {"produto": "A", "vendas": 1000},
        ...     {"produto": "B", "vendas": 800},
        ...     # ... mais produtos
        ... ]
        >>> limited = limiter.limit_categories(
        ...     data=data,
        ...     category_column="produto",
        ...     metric_column="vendas",
        ...     create_others=True
        ... )
    """

    def __init__(self, max_categories: int = 15):
        """
        Inicializa o limitador de categorias.

        Args:
            max_categories: Número máximo de categorias a exibir

        Raises:
            ValueError: Se max_categories < 1
        """
        if max_categories < 1:
            raise ValueError(f"max_categories deve ser >= 1, recebeu {max_categories}")

        self.max_categories = max_categories
        self.logger = logger

    def limit_categories(
        self,
        data: List[Dict[str, Any]],
        category_column: str,
        metric_column: str,
        create_others: bool = False,
        others_label: str = "OUTROS",
        ascending: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Limita o número de categorias no dataset.

        Processo:
        1. Converte dados para DataFrame
        2. Ordena por métrica (padrão: descendente)
        3. Seleciona top N categorias
        4. Se create_others=True e há categorias excedentes:
           - Agrega valores das categorias excedentes
           - Adiciona linha "OUTROS" ao resultado
        5. Retorna dados limitados + metadados

        Args:
            data: Lista de dicionários com os dados
            category_column: Nome da coluna de categoria
            metric_column: Nome da coluna de métrica (para ordenação)
            create_others: Se True, cria categoria agregada "OUTROS"
            others_label: Label da categoria agregada (padrão: "OUTROS")
            ascending: Se True, ordena ascendente; False ordena descendente

        Returns:
            Tupla contendo:
            - Lista de dicionários com dados limitados
            - Dicionário de metadados com informações sobre a limitação

        Raises:
            ValueError: Se colunas não existem nos dados

        Exemplo:
            >>> limiter = CategoryLimiter(max_categories=3)
            >>> data = [
            ...     {"cat": "A", "val": 100},
            ...     {"cat": "B", "val": 90},
            ...     {"cat": "C", "val": 80},
            ...     {"cat": "D", "val": 70},
            ...     {"cat": "E", "val": 60}
            ... ]
            >>> result, meta = limiter.limit_categories(
            ...     data, "cat", "val", create_others=True
            ... )
            >>> len(result)
            4  # A, B, C + OUTROS
            >>> meta["categories_excluded"]
            2  # D e E foram agregados em OUTROS
        """
        # Validar entrada
        if not data:
            self.logger.warning("Dataset vazio - retornando vazio")
            return [], {
                "original_count": 0,
                "limited_count": 0,
                "categories_excluded": 0,
                "others_created": False,
                "others_value": 0,
            }

        # Converter para DataFrame
        df = pd.DataFrame(data)

        # Validar colunas
        if category_column not in df.columns:
            raise ValueError(
                f"Coluna de categoria '{category_column}' não encontrada. "
                f"Colunas disponíveis: {list(df.columns)}"
            )
        if metric_column not in df.columns:
            raise ValueError(
                f"Coluna de métrica '{metric_column}' não encontrada. "
                f"Colunas disponíveis: {list(df.columns)}"
            )

        original_count = len(df)

        # Se já está dentro do limite, retornar sem modificações
        if original_count <= self.max_categories:
            self.logger.debug(
                f"Dataset tem {original_count} categorias, "
                f"dentro do limite de {self.max_categories}"
            )
            return data, {
                "original_count": original_count,
                "limited_count": original_count,
                "categories_excluded": 0,
                "others_created": False,
                "others_value": 0,
            }

        # Ordenar por métrica
        df_sorted = df.sort_values(by=metric_column, ascending=ascending).reset_index(
            drop=True
        )

        # Separar top N e excedentes
        df_top = df_sorted.head(self.max_categories)
        df_excluded = df_sorted.iloc[self.max_categories :]

        categories_excluded = len(df_excluded)

        self.logger.info(
            f"Limitando de {original_count} para {self.max_categories} categorias. "
            f"{categories_excluded} categorias excedentes."
        )

        # Preparar resultado
        result_data = df_top.to_dict("records")

        # Criar categoria "OUTROS" se solicitado
        others_created = False
        others_value = 0

        if create_others and categories_excluded > 0:
            # Somar valores das categorias excedentes
            others_value = df_excluded[metric_column].sum()

            # Criar registro "OUTROS"
            others_record = {category_column: others_label}

            # Adicionar todas as outras colunas
            for col in df.columns:
                if col == category_column:
                    continue  # Já adicionado
                elif col == metric_column:
                    others_record[col] = others_value
                else:
                    # Para outras colunas, usar valor padrão (None ou 0)
                    # Dependendo do tipo
                    if pd.api.types.is_numeric_dtype(df[col]):
                        others_record[col] = 0
                    else:
                        others_record[col] = None

            result_data.append(others_record)
            others_created = True

            self.logger.info(
                f"Categoria '{others_label}' criada com {categories_excluded} "
                f"categorias agregadas (valor total: {others_value:,.2f})"
            )

        # Metadados
        metadata = {
            "original_count": original_count,
            "limited_count": len(result_data),
            "categories_excluded": categories_excluded,
            "others_created": others_created,
            "others_value": float(others_value) if others_created else 0,
            "others_label": others_label if others_created else None,
            "max_categories": self.max_categories,
        }

        return result_data, metadata

    def should_limit(self, data: List[Dict[str, Any]]) -> bool:
        """
        Verifica se o dataset precisa ser limitado.

        Args:
            data: Lista de dicionários com os dados

        Returns:
            True se len(data) > max_categories, False caso contrário

        Exemplo:
            >>> limiter = CategoryLimiter(max_categories=10)
            >>> limiter.should_limit([{"a": 1}] * 5)
            False
            >>> limiter.should_limit([{"a": 1}] * 15)
            True
        """
        return len(data) > self.max_categories

    def get_top_categories(
        self,
        data: List[Dict[str, Any]],
        category_column: str,
        metric_column: str,
        ascending: bool = False,
    ) -> List[str]:
        """
        Retorna lista com os nomes das top N categorias.

        Útil para logging e debugging.

        Args:
            data: Lista de dicionários com os dados
            category_column: Nome da coluna de categoria
            metric_column: Nome da coluna de métrica
            ascending: Ordenação

        Returns:
            Lista com nomes das top N categorias

        Exemplo:
            >>> limiter = CategoryLimiter(max_categories=3)
            >>> data = [
            ...     {"cat": "A", "val": 100},
            ...     {"cat": "B", "val": 90},
            ...     {"cat": "C", "val": 80},
            ...     {"cat": "D", "val": 70}
            ... ]
            >>> limiter.get_top_categories(data, "cat", "val")
            ['A', 'B', 'C']
        """
        if not data:
            return []

        df = pd.DataFrame(data)
        df_sorted = df.sort_values(by=metric_column, ascending=ascending)
        top_df = df_sorted.head(self.max_categories)

        return top_df[category_column].tolist()


def apply_category_limit(
    data: List[Dict[str, Any]],
    category_column: str,
    metric_column: str,
    max_categories: int = 15,
    create_others: bool = False,
    others_label: str = "OUTROS",
    ascending: bool = False,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Função de conveniência para limitar categorias em uma única chamada.

    Esta é uma função wrapper que cria um CategoryLimiter e aplica
    a limitação em uma única operação.

    Args:
        data: Lista de dicionários com os dados
        category_column: Nome da coluna de categoria
        metric_column: Nome da coluna de métrica
        max_categories: Número máximo de categorias
        create_others: Se deve criar categoria "OUTROS"
        others_label: Label da categoria agregada
        ascending: Ordenação

    Returns:
        Tupla (dados_limitados, metadados)

    Exemplo:
        >>> data = [{"cat": "A", "val": 100}, ...]
        >>> limited, meta = apply_category_limit(
        ...     data, "cat", "val", max_categories=5
        ... )
    """
    limiter = CategoryLimiter(max_categories=max_categories)
    return limiter.limit_categories(
        data=data,
        category_column=category_column,
        metric_column=metric_column,
        create_others=create_others,
        others_label=others_label,
        ascending=ascending,
    )


__all__ = ["CategoryLimiter", "apply_category_limit"]
