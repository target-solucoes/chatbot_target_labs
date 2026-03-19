"""
Base calculator for insight generation.

This module provides the abstract base class that all chart-type-specific
calculators must implement. It ensures a consistent interface and provides
generic helper methods for common calculations.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class BaseCalculator(ABC):
    """
    Abstract base class for all chart-type-specific calculators.

    Each calculator is responsible for extracting numeric insights from
    processed data based on the chart type. Calculators must be completely
    generic and avoid hardcoding column names.

    Subclasses must implement:
        - calculate(df, config): Main calculation logic

    Subclasses can use helper methods:
        - _get_total(df, value_col): Calculate sum of a column
        - _get_top_n(df, label_col, value_col, n): Get top N rows
        - _get_percentage(part, total): Calculate percentage
        - _safe_divide(numerator, denominator): Division with zero handling
    """

    @abstractmethod
    def calculate(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate chart-type-specific metrics from data.

        This method must be implemented by all subclasses. It should extract
        relevant metrics from the DataFrame without hardcoding column names.
        All column references must come from the config parameter.

        Args:
            df: DataFrame with processed data from analytics_executor
            config: Configuration extracted from chart_spec/analytics_result
                Expected keys (vary by chart type):
                    - dimension_cols: List[str] - Dimension column names
                    - metric_cols: List[str] - Metric column names
                    - aggregation: str - Aggregation function used (sum, avg, etc.)
                    - top_n: int - Number of top items (for rankings)
                    - filters: Dict - Applied filters
                    - ...other chart-specific configs

        Returns:
            Dictionary with calculated metrics. Structure varies by chart type
            but should always include:
                - Calculated numeric values
                - Percentages and ratios
                - Column references (label_col, value_col)
                - Context metadata (total_items, etc.)

        Example return for ranking calculator:
            {
                "total": 12000.0,
                "top_n": 5,
                "sum_top_n": 8000.0,
                "concentracao_top_n_pct": 66.67,
                "lider_valor": 3000.0,
                "segundo_valor": 2500.0,
                "gap_percentual": 20.0,
                "label_col": "categoria",
                "value_col": "valor",
                "total_items": 20
            }

        Raises:
            ValueError: If required config keys are missing
            KeyError: If specified columns don't exist in DataFrame
        """
        pass

    # ========== Generic Helper Methods ==========

    def _get_total(self, df: pd.DataFrame, value_col: str) -> float:
        """
        Calculate the total sum of a numeric column.

        Args:
            df: DataFrame containing the column
            value_col: Name of the column to sum

        Returns:
            Sum of all values in the column

        Raises:
            KeyError: If column doesn't exist in DataFrame

        Example:
            >>> total = self._get_total(df, "vendas")
            >>> # Returns: 125000.50
        """
        if value_col not in df.columns:
            raise KeyError(f"Column '{value_col}' not found in DataFrame")

        return float(df[value_col].sum())

    def _get_top_n(
        self,
        df: pd.DataFrame,
        label_col: str,
        value_col: str,
        n: int
    ) -> pd.DataFrame:
        """
        Get top N rows sorted by a value column.

        Args:
            df: DataFrame to extract from
            label_col: Name of the label/dimension column
            value_col: Name of the value/metric column
            n: Number of top rows to return

        Returns:
            DataFrame with top N rows, sorted descending by value_col

        Raises:
            KeyError: If columns don't exist in DataFrame

        Example:
            >>> top5 = self._get_top_n(df, "produto", "receita", 5)
            >>> # Returns DataFrame with 5 products with highest revenue
        """
        if label_col not in df.columns:
            raise KeyError(f"Column '{label_col}' not found in DataFrame")
        if value_col not in df.columns:
            raise KeyError(f"Column '{value_col}' not found in DataFrame")

        return df.nlargest(n, value_col)

    def _get_percentage(
        self,
        part: float,
        total: float,
        decimal_places: int = 2
    ) -> float:
        """
        Calculate percentage with safe division.

        Args:
            part: Numerator value
            total: Denominator value (total)
            decimal_places: Number of decimal places to round to

        Returns:
            Percentage value (0-100 scale), or 0.0 if total is zero

        Example:
            >>> pct = self._get_percentage(250, 1000)
            >>> # Returns: 25.0
            >>> pct = self._get_percentage(100, 0)
            >>> # Returns: 0.0 (safe division)
        """
        if total == 0:
            logger.warning("Division by zero in percentage calculation")
            return 0.0

        percentage = (part / total) * 100
        return round(percentage, decimal_places)

    def _safe_divide(
        self,
        numerator: float,
        denominator: float,
        default: float = 0.0,
        decimal_places: Optional[int] = None
    ) -> float:
        """
        Perform division with zero-handling.

        Args:
            numerator: Value to divide
            denominator: Value to divide by
            default: Value to return if denominator is zero
            decimal_places: If provided, round result to this many places

        Returns:
            Result of division, or default if denominator is zero

        Example:
            >>> ratio = self._safe_divide(1500, 1000)
            >>> # Returns: 1.5
            >>> ratio = self._safe_divide(1500, 0, default=1.0)
            >>> # Returns: 1.0 (default)
        """
        if denominator == 0:
            logger.warning(
                f"Division by zero: {numerator} / {denominator}, "
                f"returning default: {default}"
            )
            return default

        result = numerator / denominator

        if decimal_places is not None:
            result = round(result, decimal_places)

        return result

    def _get_full_dataset_total(
        self,
        config: Dict[str, Any],
        metric_col: str,
        fallback_df: Optional[pd.DataFrame] = None
    ) -> Optional[float]:
        """
        Extrai o total global do dataset filtrado a partir do metadata.

        Quando há um top_n no chart_spec, o analytics_executor calcula o total
        do dataset completo usando duas queries e passa via metadata.
        Este método extrai esse valor, lidando com mapeamento de nomes
        (alias vs nome original da coluna).

        Args:
            config: Configuração contendo metadata
            metric_col: Nome da coluna métrica no DataFrame (pode ser alias)
            fallback_df: DataFrame para usar como fallback se metadata não disponível

        Returns:
            Total global da métrica, ou None se não disponível

        Example:
            >>> # Caso 1: Nome exato encontrado
            >>> total = self._get_full_dataset_total(config, "Valor_Vendido", df)
            >>> # Retorna: 814948699.45 (do metadata)
            >>>
            >>> # Caso 2: Alias usado, precisa mapear para nome original
            >>> total = self._get_full_dataset_total(config, "Vendas", df)
            >>> # Busca metadata.metrics, encontra "Valor_Vendido", retorna total
        """
        metadata = config.get("metadata", {})
        full_dataset_totals = metadata.get("full_dataset_totals", {})

        # TENTATIVA 1: Buscar usando o nome da coluna diretamente
        if metric_col in full_dataset_totals:
            total = float(full_dataset_totals[metric_col])
            logger.info(
                f"Using full dataset total from metadata for '{metric_col}': {total}"
            )
            return total

        # TENTATIVA 2: Mapear alias → nome original usando metadata
        # O metadata contém a lista de metrics com name (original) e alias
        # Precisamos encontrar qual métrica tem este alias e pegar seu name
        metrics = metadata.get("metrics", [])
        original_name = None

        for metric in metrics:
            if isinstance(metric, dict):
                # Verificar se metric_col corresponde ao alias
                if metric.get("alias") == metric_col:
                    original_name = metric.get("name")
                    break
                # Verificar se metric_col corresponde ao nome (já é o original)
                elif metric.get("name") == metric_col:
                    original_name = metric_col
                    break
            elif isinstance(metric, str):
                # Se metric é string simples, tratar como nome
                if metric == metric_col:
                    original_name = metric
                    break

        # Se encontrou mapeamento, tentar buscar com nome original
        if original_name and original_name in full_dataset_totals:
            total = float(full_dataset_totals[original_name])
            logger.info(
                f"Using full dataset total from metadata for '{metric_col}' "
                f"(mapped from alias to '{original_name}'): {total}"
            )
            return total

        # TENTATIVA 3: Buscar todas as chaves do full_dataset_totals e ver se há match
        # (útil para variações de underscores/espaços)
        for key in full_dataset_totals.keys():
            # Comparar ignorando case e substituindo _ por espaço
            normalized_key = key.lower().replace("_", " ")
            normalized_col = metric_col.lower().replace("_", " ")

            if normalized_key == normalized_col:
                total = float(full_dataset_totals[key])
                logger.info(
                    f"Using full dataset total from metadata for '{metric_col}' "
                    f"(normalized match with '{key}'): {total}"
                )
                return total

        # FALLBACK: Usar DataFrame se disponível (com warning)
        if fallback_df is not None:
            total = self._get_total(fallback_df, metric_col)
            logger.warning(
                f"Full dataset total not in metadata for '{metric_col}', "
                f"using DataFrame sum: {total}. "
                f"This may be incorrect if top_n filtering is applied. "
                f"Available totals in metadata: {list(full_dataset_totals.keys())}"
            )
            return total

        # Nenhuma fonte disponível
        logger.error(
            f"Cannot determine full dataset total for '{metric_col}': "
            f"not in metadata and no fallback DataFrame provided. "
            f"Available totals in metadata: {list(full_dataset_totals.keys())}"
        )
        return None

    def _validate_config(self, config: Dict[str, Any], required_keys: List[str]) -> None:
        """
        Validate that required keys exist in config.

        Args:
            config: Configuration dictionary to validate
            required_keys: List of required key names

        Raises:
            ValueError: If any required keys are missing

        Example:
            >>> self._validate_config(config, ["dimension_cols", "metric_cols"])
            >>> # Raises ValueError if keys are missing
        """
        missing_keys = [key for key in required_keys if key not in config]

        if missing_keys:
            raise ValueError(
                f"Missing required config keys: {missing_keys}. "
                f"Provided keys: {list(config.keys())}"
            )

    def _validate_columns(self, df: pd.DataFrame, columns: List[str]) -> None:
        """
        Validate that columns exist in DataFrame.

        Args:
            df: DataFrame to check
            columns: List of column names that must exist

        Raises:
            KeyError: If any columns are missing

        Example:
            >>> self._validate_columns(df, ["cliente", "valor"])
            >>> # Raises KeyError if columns don't exist
        """
        missing_cols = [col for col in columns if col not in df.columns]

        if missing_cols:
            raise KeyError(
                f"Columns not found in DataFrame: {missing_cols}. "
                f"Available columns: {list(df.columns)}"
            )
