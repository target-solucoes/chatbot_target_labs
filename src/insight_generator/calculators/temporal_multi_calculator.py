"""
TemporalMultiCalculator for line_composed chart type

Calculates metrics for multi-line temporal visualizations:
- Convergence/divergence analysis
- Temporal correlation between series
- Relative performance trends
"""

import logging
from typing import Dict, Any
import pandas as pd
import numpy as np
from .base import BaseCalculator

logger = logging.getLogger(__name__)


class TemporalMultiCalculator(BaseCalculator):
    """
    Calculator for line_composed (multi-line temporal) visualizations
    """

    def calculate(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate multi-line temporal metrics

        Args:
            df: DataFrame with multi-series temporal data
            config: Must include 'series_col' for series identification

        Returns:
            Dict with multi-temporal metrics
        """
        self._validate_config(config, ["dimension_cols", "metric_cols"])

        time_col = config["dimension_cols"][0]
        value_col = config["metric_cols"][0]

        # Series column detection with smart fallback
        # Priority: config["series_col"] > second dimension > first non-temporal categorical column
        series_col = config.get("series_col")

        if not series_col:
            # Try to use second dimension if available
            if len(config["dimension_cols"]) >= 2:
                series_col = config["dimension_cols"][1]
                logger.info(
                    f"[TemporalMultiCalculator] Using second dimension '{series_col}' as series_col"
                )
            else:
                # Find any categorical column that's not the time column
                for col in df.columns:
                    if (
                        col != time_col
                        and col != value_col
                        and not pd.api.types.is_numeric_dtype(df[col])
                    ):
                        series_col = col
                        logger.info(
                            f"[TemporalMultiCalculator] Auto-detected series_col: '{series_col}'"
                        )
                        break

        if not series_col:
            # Ultimate fallback: treat as single series
            logger.warning(
                "[TemporalMultiCalculator] No series_col found, treating as single series"
            )
            return self._single_series_fallback(df, time_col, value_col)

        self._validate_columns(df, [time_col, value_col, series_col])

        if len(df) == 0:
            return self._empty_result(time_col, value_col, series_col)

        # Sort by time
        df_sorted = df.sort_values(time_col).reset_index(drop=True)

        # Get unique series
        series_list = df_sorted[series_col].unique()
        n_series = len(series_list)

        # Pivot to get series as columns
        try:
            pivot = df_sorted.pivot_table(
                values=value_col, index=time_col, columns=series_col, fill_value=0
            )
        except Exception as e:
            logger.warning(f"Could not pivot data: {e}")
            return self._empty_result(time_col, value_col, series_col)

        # Calculate correlation between series
        correlacao_media = 0.0
        if n_series >= 2:
            corr_matrix = pivot.corr()
            mask = np.triu(np.ones_like(corr_matrix, dtype=bool), k=1)
            if mask.sum() > 0:
                correlacao_media = float(corr_matrix.where(mask).stack().mean())

        # Convergence/divergence (std of differences over time)
        if n_series >= 2:
            spread = pivot.std(axis=1)
            spread_inicial = float(spread.iloc[0]) if len(spread) > 0 else 0
            spread_final = float(spread.iloc[-1]) if len(spread) > 0 else 0

            if spread_inicial > 0:
                convergencia_pct = (
                    (spread_inicial - spread_final) / spread_inicial
                ) * 100
            else:
                convergencia_pct = 0.0

            if convergencia_pct > 20:
                padrao = "convergente"
            elif convergencia_pct < -20:
                padrao = "divergente"
            else:
                padrao = "paralelo"
        else:
            spread_inicial = 0.0
            spread_final = 0.0
            convergencia_pct = 0.0
            padrao = "unico"

        # Leader series (highest final value)
        final_values = pivot.iloc[-1] if len(pivot) > 0 else pd.Series()
        if len(final_values) > 0:
            lider_serie = final_values.idxmax()
            lider_valor_final = float(final_values.max())
        else:
            lider_serie = "N/A"
            lider_valor_final = 0.0

        return {
            "n_series": n_series,
            "n_periods": len(pivot),
            "correlacao_media": correlacao_media,
            "spread_inicial": spread_inicial,
            "spread_final": spread_final,
            "convergencia_pct": convergencia_pct,
            "padrao": padrao,
            "lider_serie": lider_serie,
            "lider_valor_final": lider_valor_final,
            "time_col": time_col,
            "value_col": value_col,
            "series_col": series_col,
        }

    def _empty_result(
        self, time_col: str, value_col: str, series_col: str
    ) -> Dict[str, Any]:
        return {
            "n_series": 0,
            "n_periods": 0,
            "correlacao_media": 0.0,
            "spread_inicial": 0.0,
            "spread_final": 0.0,
            "convergencia_pct": 0.0,
            "padrao": "unico",
            "lider_serie": "N/A",
            "lider_valor_final": 0.0,
            "time_col": time_col,
            "value_col": value_col,
            "series_col": series_col,
        }

    def _single_series_fallback(
        self, df: pd.DataFrame, time_col: str, value_col: str
    ) -> Dict[str, Any]:
        """
        Fallback for when no series column is detected (single-series temporal data).

        Calculates basic temporal metrics without multi-series analysis.
        """
        if len(df) == 0:
            return self._empty_result(time_col, value_col, "single_series")

        # Sort by time
        df_sorted = df.sort_values(time_col).reset_index(drop=True)

        # Calculate basic temporal metrics
        valor_inicial = float(df_sorted[value_col].iloc[0])
        valor_final = float(df_sorted[value_col].iloc[-1])

        # Variation
        if valor_inicial != 0:
            variacao_pct = ((valor_final - valor_inicial) / abs(valor_inicial)) * 100
        else:
            variacao_pct = 0.0

        return {
            "n_series": 1,
            "n_periods": len(df_sorted),
            "correlacao_media": 1.0,  # Perfect correlation with itself
            "spread_inicial": 0.0,
            "spread_final": 0.0,
            "convergencia_pct": 0.0,
            "padrao": "unico",
            "lider_serie": "single_series",
            "lider_valor_final": valor_final,
            "valor_inicial": valor_inicial,
            "valor_final": valor_final,
            "variacao_pct": variacao_pct,
            "time_col": time_col,
            "value_col": value_col,
            "series_col": "single_series",
        }
