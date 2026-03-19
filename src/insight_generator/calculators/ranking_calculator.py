"""
RankingCalculator for bar_horizontal chart type

Calculates metrics for ranking visualizations:
- Concentration analysis (Top N vs universe)
- Gap analysis (leader vs competitors)
- Distribution metrics
- Competitive dynamics
"""
import logging
from typing import Dict, Any
import pandas as pd
from .base import BaseCalculator

logger = logging.getLogger(__name__)


class RankingCalculator(BaseCalculator):
    """
    Calculator for bar_horizontal (ranking) visualizations

    This calculator analyzes ranking data to extract insights about:
    - Market concentration (Top N performance)
    - Competitive gaps (leader advantage)
    - Distribution patterns
    - Relative performance metrics

    Example:
        >>> calculator = RankingCalculator()
        >>> df = pd.DataFrame({
        ...     "product": ["A", "B", "C", "D", "E"],
        ...     "revenue": [1000, 800, 600, 400, 200]
        ... })
        >>> config = {
        ...     "dimension_cols": ["product"],
        ...     "metric_cols": ["revenue"],
        ...     "top_n": 3
        ... }
        >>> result = calculator.calculate(df, config)
        >>> result["concentracao_top_n_pct"]
        80.0
    """

    def calculate(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate ranking-specific metrics

        Args:
            df: DataFrame with ranking data
            config: Configuration dict with:
                - dimension_cols: List[str] - Column(s) for categories/labels
                - metric_cols: List[str] - Column(s) for values
                - top_n: int (optional) - Number of top items to analyze (default: 5)

        Returns:
            Dict with ranking metrics:
                - total: Total sum of values
                - top_n: Number of top items analyzed
                - sum_top_n: Sum of top N values
                - concentracao_top_n_pct: Percentage concentration in top N
                - top3_sum: Sum of top 3 values
                - concentracao_top3_pct: Top 3 concentration relative to top N
                - lider_valor: Value of #1 position
                - segundo_valor: Value of #2 position
                - gap_absoluto: Absolute difference between #1 and #2
                - gap_percentual: Percentage gap (#1 vs #2)
                - multiplo_lider_segundo: Multiple between leader and second
                - peso_lider_total_pct: Leader's weight in total
                - lider_label: Label of leader
                - segundo_label: Label of second position
                - label_col: Name of label column (for transparency)
                - value_col: Name of value column (for transparency)
                - total_items: Total number of items in ranking

        Raises:
            ValueError: If config is invalid or required columns missing
        """
        # Validate config
        self._validate_config(config, ["dimension_cols", "metric_cols"])

        # Extract column names from config (generic, no hardcoding)
        label_col = config["dimension_cols"][0]
        value_col = config["metric_cols"][0]
        top_n = config.get("top_n", 5)

        # Validate columns exist
        self._validate_columns(df, [label_col, value_col])

        # Validate data is not empty
        if len(df) == 0:
            logger.warning("Empty DataFrame provided to RankingCalculator")
            return self._empty_result(label_col, value_col, top_n)

        # Sort by value descending (ranking order)
        df_sorted = df.sort_values(value_col, ascending=False).reset_index(drop=True)

        # Calculate total (usar metadata se disponível, senão fallback para DataFrame)
        total = self._get_full_dataset_total(config, value_col, fallback_df=df_sorted)

        # Se não conseguiu obter total de nenhuma fonte, retornar resultado vazio
        if total is None:
            logger.error("Cannot calculate metrics without total value")
            return self._empty_result(label_col, value_col, top_n)

        # Handle edge case: total is zero
        if total == 0:
            logger.warning("Total sum is zero in RankingCalculator")
            return self._empty_result(label_col, value_col, top_n)

        # Get top N items
        actual_top_n = min(top_n, len(df_sorted))
        top_df = self._get_top_n(df_sorted, label_col, value_col, actual_top_n)
        sum_top_n = top_df[value_col].sum()

        # Get top 3 (for additional analysis)
        actual_top_3 = min(3, len(df_sorted))
        top3_sum = df_sorted[value_col].head(actual_top_3).sum()

        # Leader and second place analysis
        lider_valor = float(df_sorted[value_col].iloc[0])
        lider_label = df_sorted[label_col].iloc[0]

        # Handle case with only one item
        if len(df_sorted) < 2:
            segundo_valor = 0.0
            segundo_label = "N/A"
            gap_absoluto = lider_valor
            gap_percentual = 0.0
            multiplo_lider_segundo = 0.0
        else:
            segundo_valor = float(df_sorted[value_col].iloc[1])
            segundo_label = df_sorted[label_col].iloc[1]
            gap_absoluto = lider_valor - segundo_valor
            gap_percentual = self._get_percentage(gap_absoluto, segundo_valor)
            multiplo_lider_segundo = self._safe_divide(lider_valor, segundo_valor)

        # Build comprehensive result
        result = {
            # Totals
            "total": total,
            "total_items": len(df_sorted),

            # Top N metrics
            "top_n": actual_top_n,
            "sum_top_n": sum_top_n,
            "concentracao_top_n_pct": self._get_percentage(sum_top_n, total),

            # Top 3 metrics
            "top3_sum": top3_sum,
            "concentracao_top3_pct": self._get_percentage(top3_sum, total),

            # Leader analysis
            "lider_valor": lider_valor,
            "lider_label": lider_label,
            "peso_lider_total_pct": self._get_percentage(lider_valor, total),

            # Competitive gap analysis
            "segundo_valor": segundo_valor,
            "segundo_label": segundo_label,
            "gap_absoluto": gap_absoluto,
            "gap_percentual": gap_percentual,
            "multiplo_lider_segundo": multiplo_lider_segundo,

            # Context (for transparency and debugging)
            "label_col": label_col,
            "value_col": value_col,

            # Additional insights
            "tail_sum": total - sum_top_n,  # Value outside top N
            "tail_pct": self._get_percentage(total - sum_top_n, total),
            "avg_top_n": self._safe_divide(sum_top_n, actual_top_n),
            "avg_total": self._safe_divide(total, len(df_sorted)),
        }

        logger.debug(f"RankingCalculator completed: {actual_top_n} items, "
                    f"concentration={result['concentracao_top_n_pct']:.2f}%")

        return result

    def _empty_result(self, label_col: str, value_col: str, top_n: int) -> Dict[str, Any]:
        """Return empty/zero result structure"""
        return {
            "total": 0.0,
            "total_items": 0,
            "top_n": top_n,
            "sum_top_n": 0.0,
            "concentracao_top_n_pct": 0.0,
            "top3_sum": 0.0,
            "concentracao_top3_pct": 0.0,
            "lider_valor": 0.0,
            "lider_label": "N/A",
            "peso_lider_total_pct": 0.0,
            "segundo_valor": 0.0,
            "segundo_label": "N/A",
            "gap_absoluto": 0.0,
            "gap_percentual": 0.0,
            "multiplo_lider_segundo": 0.0,
            "label_col": label_col,
            "value_col": value_col,
            "tail_sum": 0.0,
            "tail_pct": 0.0,
            "avg_top_n": 0.0,
            "avg_total": 0.0,
        }
