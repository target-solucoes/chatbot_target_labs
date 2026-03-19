"""
StackedCalculator for bar_vertical_stacked chart type

Calculates metrics for stacked bar visualizations:
- Contribution analysis per stack
- Proportion evolution
- Stack composition patterns
"""
import logging
from typing import Dict, Any
import pandas as pd
import numpy as np
from .base import BaseCalculator

logger = logging.getLogger(__name__)


class StackedCalculator(BaseCalculator):
    """
    Calculator for bar_vertical_stacked visualizations
    """

    def calculate(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate stacked bar metrics

        Args:
            df: DataFrame with stacked data
            config: Must include 'stack_col' for stack grouping

        Returns:
            Dict with stacked metrics
        """
        self._validate_config(config, ["dimension_cols", "metric_cols"])

        label_col = config["dimension_cols"][0]
        value_col = config["metric_cols"][0]
        stack_col = config.get("stack_col", label_col)

        self._validate_columns(df, [label_col, value_col, stack_col])

        if len(df) == 0:
            return self._empty_result(label_col, value_col, stack_col)

        # Calculate totals per category (stack height)
        category_totals = df.groupby(label_col)[value_col].sum()

        # Calculate contribution per stack component
        stack_totals = df.groupby(stack_col)[value_col].sum()

        # Total (usar metadata se disponível para cálculos corretos de porcentagem)
        full_total = self._get_full_dataset_total(config, value_col, fallback_df=df)
        if full_total is None:
            logger.error("Cannot calculate metrics without total value")
            return self._empty_result(label_col, value_col, stack_col)

        total_overall = float(df[value_col].sum())  # Total do subset (para referência)

        # Dominant stack component
        dominant_stack = stack_totals.idxmax()
        dominant_contribution = float(stack_totals.max())
        dominant_pct = self._get_percentage(dominant_contribution, full_total)

        # Stack component analysis
        n_stacks = len(stack_totals)
        stack_mean = float(stack_totals.mean())
        stack_std = float(stack_totals.std())

        # Contribution balance
        stack_contributions_pct = (stack_totals / total_overall * 100).values
        balance_score = 100 - float(np.std(stack_contributions_pct))

        return {
            "n_categories": len(category_totals),
            "n_stacks": n_stacks,
            "total_overall": total_overall,
            "dominant_stack": dominant_stack,
            "dominant_contribution": dominant_contribution,
            "dominant_pct": dominant_pct,
            "stack_mean": stack_mean,
            "stack_std": stack_std,
            "balance_score": max(0, balance_score),
            "label_col": label_col,
            "value_col": value_col,
            "stack_col": stack_col,
        }

    def _empty_result(self, label_col: str, value_col: str, stack_col: str) -> Dict[str, Any]:
        return {
            "n_categories": 0,
            "n_stacks": 0,
            "total_overall": 0.0,
            "dominant_stack": "N/A",
            "dominant_contribution": 0.0,
            "dominant_pct": 0.0,
            "stack_mean": 0.0,
            "stack_std": 0.0,
            "balance_score": 100.0,
            "label_col": label_col,
            "value_col": value_col,
            "stack_col": stack_col,
        }
