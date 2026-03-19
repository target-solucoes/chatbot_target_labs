"""
HistogramCalculator for histogram chart type

Calculates metrics for distribution/frequency visualizations:
- Skewness and kurtosis
- Outlier detection
- Distributional shape analysis
"""
import logging
from typing import Dict, Any
import pandas as pd
import numpy as np
from scipy import stats
from .base import BaseCalculator

logger = logging.getLogger(__name__)


class HistogramCalculator(BaseCalculator):
    """
    Calculator for histogram (distribution) visualizations

    Analyzes frequency distributions to extract insights about:
    - Shape (skewness, kurtosis)
    - Outliers
    - Concentration patterns
    """

    def calculate(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate histogram-specific metrics

        Args:
            df: DataFrame with distribution data
            config: Configuration dict

        Returns:
            Dict with distribution metrics
        """
        self._validate_config(config, ["metric_cols"])

        value_col = config["metric_cols"][0]

        self._validate_columns(df, [value_col])

        if len(df) == 0:
            return self._empty_result(value_col)

        values = df[value_col].dropna().values

        if len(values) == 0:
            return self._empty_result(value_col)

        # Basic statistics
        media = float(np.mean(values))
        mediana = float(np.median(values))
        moda_values = stats.mode(values, keepdims=True)
        moda = float(moda_values.mode[0]) if len(moda_values.mode) > 0 else media
        desvio_padrao = float(np.std(values))

        # Shape metrics
        skewness = float(stats.skew(values))
        kurtosis = float(stats.kurtosis(values))

        # Skewness interpretation
        if abs(skewness) < 0.5:
            assimetria = "simetrica"
        elif skewness > 0:
            assimetria = "assimetrica_positiva"
        else:
            assimetria = "assimetrica_negativa"

        # Kurtosis interpretation
        if kurtosis > 1:
            achatamento = "leptocurtica"
        elif kurtosis < -1:
            achatamento = "platicurtica"
        else:
            achatamento = "mesocurtica"

        # Outlier detection (IQR method)
        q1 = float(np.percentile(values, 25))
        q3 = float(np.percentile(values, 75))
        iqr = q3 - q1
        outlier_lower = q1 - 1.5 * iqr
        outlier_upper = q3 + 1.5 * iqr
        outliers = values[(values < outlier_lower) | (values > outlier_upper)]
        n_outliers = len(outliers)

        # Range
        valor_minimo = float(np.min(values))
        valor_maximo = float(np.max(values))
        amplitude = valor_maximo - valor_minimo

        return {
            "n_valores": len(values),
            "media": media,
            "mediana": mediana,
            "moda": moda,
            "desvio_padrao": desvio_padrao,
            "valor_minimo": valor_minimo,
            "valor_maximo": valor_maximo,
            "amplitude": amplitude,
            "q1": q1,
            "q3": q3,
            "iqr": iqr,
            "skewness": skewness,
            "assimetria": assimetria,
            "kurtosis": kurtosis,
            "achatamento": achatamento,
            "n_outliers": n_outliers,
            "outlier_pct": (n_outliers / len(values)) * 100,
            "value_col": value_col,
        }

    def _empty_result(self, value_col: str) -> Dict[str, Any]:
        return {
            "n_valores": 0,
            "media": 0.0,
            "mediana": 0.0,
            "moda": 0.0,
            "desvio_padrao": 0.0,
            "valor_minimo": 0.0,
            "valor_maximo": 0.0,
            "amplitude": 0.0,
            "q1": 0.0,
            "q3": 0.0,
            "iqr": 0.0,
            "skewness": 0.0,
            "assimetria": "simetrica",
            "kurtosis": 0.0,
            "achatamento": "mesocurtica",
            "n_outliers": 0,
            "outlier_pct": 0.0,
            "value_col": value_col,
        }
