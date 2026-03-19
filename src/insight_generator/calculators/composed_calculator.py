"""
ComposedCalculator for bar_vertical_composed chart type

Calculates metrics for multi-series bar chart visualizations:
- Series dominance analysis
- Cross-series variation
- Correlation between series
"""
import logging
from typing import Dict, Any
import pandas as pd
import numpy as np
from .base import BaseCalculator

logger = logging.getLogger(__name__)


class ComposedCalculator(BaseCalculator):
    """
    Calculator for bar_vertical_composed (multi-series bar) visualizations
    """

    def calculate(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate composed bar metrics

        Args:
            df: DataFrame with multi-series data
            config: Must include 'series_col' for series grouping

        Returns:
            Dict with multi-series metrics
        """
        self._validate_config(config, ["dimension_cols", "metric_cols"])

        label_col = config["dimension_cols"][0]
        value_col = config["metric_cols"][0]
        series_col = config.get("series_col", label_col)

        self._validate_columns(df, [label_col, value_col, series_col])

        if len(df) == 0:
            return self._empty_result(label_col, value_col, series_col)

        # Group by series
        series_totals = df.groupby(series_col)[value_col].sum()
        n_series = len(series_totals)

        # Dominant series
        dominant_series = series_totals.idxmax()
        dominant_valor = float(series_totals.max())

        # Total (usar metadata se disponível para cálculos corretos de porcentagem)
        full_total = self._get_full_dataset_total(config, value_col, fallback_df=df)
        if full_total is None:
            logger.error("Cannot calculate metrics without total value")
            return self._empty_result(label_col, value_col, series_col)

        total_all_series = float(series_totals.sum())  # Total do subset (para referência)
        dominancia_pct = self._get_percentage(dominant_valor, full_total)

        # Series variation
        series_mean = float(series_totals.mean())
        series_std = float(series_totals.std())
        series_cv = self._get_percentage(series_std, series_mean) if series_mean != 0 else 0.0

        # Correlation between series (if multiple dimensions)
        correlacao_media = 0.0
        if len(df[label_col].unique()) > 1 and n_series >= 2:
            pivot = df.pivot_table(values=value_col, index=label_col, columns=series_col, fill_value=0)
            if pivot.shape[1] >= 2:
                corr_matrix = pivot.corr()
                # Average correlation (excluding diagonal)
                mask = np.triu(np.ones_like(corr_matrix, dtype=bool), k=1)
                correlacao_media = float(corr_matrix.where(mask).stack().mean())

        return {
            "n_series": n_series,
            "n_categories": len(df[label_col].unique()),
            "total_all_series": total_all_series,
            "dominant_series": dominant_series,
            "dominant_valor": dominant_valor,
            "dominancia_pct": dominancia_pct,
            "series_mean": series_mean,
            "series_std": series_std,
            "series_cv": series_cv,
            "correlacao_media": correlacao_media,
            "label_col": label_col,
            "value_col": value_col,
            "series_col": series_col,
        }

    def _empty_result(self, label_col: str, value_col: str, series_col: str) -> Dict[str, Any]:
        return {
            "n_series": 0,
            "n_categories": 0,
            "total_all_series": 0.0,
            "dominant_series": "N/A",
            "dominant_valor": 0.0,
            "dominancia_pct": 0.0,
            "series_mean": 0.0,
            "series_std": 0.0,
            "series_cv": 0.0,
            "correlacao_media": 0.0,
            "label_col": label_col,
            "value_col": value_col,
            "series_col": series_col,
        }
