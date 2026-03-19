"""
DistributionCalculator for pie chart type

Calculates metrics for distribution/proportion visualizations:
- Concentration analysis
- Fragmentation metrics
- Balance and diversity indicators
"""
import logging
from typing import Dict, Any
import pandas as pd
import numpy as np
from .base import BaseCalculator

logger = logging.getLogger(__name__)


class DistributionCalculator(BaseCalculator):
    """
    Calculator for pie (distribution) visualizations

    This calculator analyzes distribution data to extract insights about:
    - Market concentration (Herfindahl index)
    - Fragmentation and diversity
    - Balance vs dominance patterns

    Example:
        >>> calculator = DistributionCalculator()
        >>> df = pd.DataFrame({
        ...     "category": ["A", "B", "C", "D"],
        ...     "share": [400, 300, 200, 100]
        ... })
        >>> config = {
        ...     "dimension_cols": ["category"],
        ...     "metric_cols": ["share"]
        ... }
        >>> result = calculator.calculate(df, config)
        >>> result["herfindahl_index"]
        3400.0
    """

    def calculate(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate distribution-specific metrics

        Args:
            df: DataFrame with distribution data
            config: Configuration dict with:
                - dimension_cols: List[str] - Category column
                - metric_cols: List[str] - Value column

        Returns:
            Dict with distribution metrics:
                - total: Total sum
                - n_categories: Number of categories
                - maior_categoria_valor: Largest segment value
                - maior_categoria_label: Largest segment label
                - maior_categoria_pct: Largest segment percentage
                - menor_categoria_valor: Smallest segment value
                - menor_categoria_label: Smallest segment label
                - menor_categoria_pct: Smallest segment percentage
                - herfindahl_index: HHI (sum of squared percentages)
                - concentracao_nivel: Concentration level descriptor
                - diversidade_score: Diversity score (inverse of concentration)
                - equilibrio_score: Balance score
                - fragmentacao_pct: % of categories with < 5% share
                - top3_concentracao_pct: Top 3 concentration
                - label_col: Name of category column
                - value_col: Name of value column

        Raises:
            ValueError: If config is invalid or required columns missing
        """
        # Validate config
        self._validate_config(config, ["dimension_cols", "metric_cols"])

        # Extract column names
        label_col = config["dimension_cols"][0]
        value_col = config["metric_cols"][0]

        # Validate columns exist
        self._validate_columns(df, [label_col, value_col])

        # Validate data is not empty
        if len(df) == 0:
            logger.warning("Empty DataFrame provided to DistributionCalculator")
            return self._empty_result(label_col, value_col)

        # Sort by value descending
        df_sorted = df.sort_values(value_col, ascending=False).reset_index(drop=True)

        # Calculate total (usar metadata se disponível, senão fallback para DataFrame)
        total = self._get_full_dataset_total(config, value_col, fallback_df=df_sorted)

        # Se não conseguiu obter total de nenhuma fonte, retornar resultado vazio
        if total is None:
            logger.error("Cannot calculate metrics without total value")
            return self._empty_result(label_col, value_col)

        if total == 0:
            logger.warning("Total sum is zero in DistributionCalculator")
            return self._empty_result(label_col, value_col)

        # Calculate percentages for each category
        percentages = (df_sorted[value_col] / total * 100).values

        # Largest segment
        maior_valor = float(df_sorted[value_col].iloc[0])
        maior_label = df_sorted[label_col].iloc[0]
        maior_pct = float(percentages[0])

        # Smallest segment
        menor_valor = float(df_sorted[value_col].iloc[-1])
        menor_label = df_sorted[label_col].iloc[-1]
        menor_pct = float(percentages[-1])

        # Herfindahl-Hirschman Index (HHI)
        # Sum of squared percentages (0-10000 scale)
        herfindahl_index = float(np.sum(percentages ** 2))

        # Concentration level based on HHI
        if herfindahl_index > 2500:
            concentracao_nivel = "alta"
        elif herfindahl_index > 1500:
            concentracao_nivel = "moderada"
        else:
            concentracao_nivel = "baixa"

        # Diversity score (normalized, 0-100)
        # Perfect diversity (all equal) = 100, complete concentration = 0
        n_categories = len(df_sorted)
        perfect_hhi = 10000 / n_categories  # HHI if all equal
        if n_categories == 1:
            diversidade_score = 0.0  # Single category = no diversity
        else:
            diversidade_score = max(0, 100 - (herfindahl_index - perfect_hhi) / (10000 - perfect_hhi) * 100)

        # Balance score (coefficient of variation inverted)
        cv = (np.std(percentages) / np.mean(percentages)) * 100 if np.mean(percentages) > 0 else 0
        equilibrio_score = max(0, 100 - cv)

        # Fragmentation: % of categories with less than 5% share
        small_segments = np.sum(percentages < 5)
        fragmentacao_pct = (small_segments / n_categories) * 100

        # Top 3 concentration
        top3_pct = float(np.sum(percentages[:min(3, len(percentages))]))

        result = {
            # Basic metrics
            "total": total,
            "n_categories": n_categories,

            # Largest segment
            "maior_categoria_valor": maior_valor,
            "maior_categoria_label": maior_label,
            "maior_categoria_pct": maior_pct,

            # Smallest segment
            "menor_categoria_valor": menor_valor,
            "menor_categoria_label": menor_label,
            "menor_categoria_pct": menor_pct,

            # Concentration metrics
            "herfindahl_index": herfindahl_index,
            "concentracao_nivel": concentracao_nivel,
            "top3_concentracao_pct": top3_pct,

            # Diversity and balance
            "diversidade_score": diversidade_score,
            "equilibrio_score": equilibrio_score,
            "fragmentacao_pct": fragmentacao_pct,

            # Context
            "label_col": label_col,
            "value_col": value_col,
        }

        logger.debug(f"DistributionCalculator completed: {n_categories} categories, "
                    f"HHI={herfindahl_index:.2f}, concentration={concentracao_nivel}")

        return result

    def _empty_result(self, label_col: str, value_col: str) -> Dict[str, Any]:
        """Return empty/zero result structure"""
        return {
            "total": 0.0,
            "n_categories": 0,
            "maior_categoria_valor": 0.0,
            "maior_categoria_label": "N/A",
            "maior_categoria_pct": 0.0,
            "menor_categoria_valor": 0.0,
            "menor_categoria_label": "N/A",
            "menor_categoria_pct": 0.0,
            "herfindahl_index": 0.0,
            "concentracao_nivel": "baixa",
            "top3_concentracao_pct": 0.0,
            "diversidade_score": 100.0,
            "equilibrio_score": 100.0,
            "fragmentacao_pct": 0.0,
            "label_col": label_col,
            "value_col": value_col,
        }
