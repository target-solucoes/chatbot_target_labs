"""
TemporalCalculator for line chart type

Calculates metrics for temporal/time series visualizations:
- Trend analysis (direction, slope)
- Period variation (growth/decline %)
- Acceleration/deceleration
- Volatility and stability
"""

import logging
from typing import Dict, Any
import pandas as pd
import numpy as np
from .base import BaseCalculator

logger = logging.getLogger(__name__)


class TemporalCalculator(BaseCalculator):
    """
    Calculator for line (temporal) visualizations

    This calculator analyzes time series data to extract insights about:
    - Temporal trends (upward, downward, stable)
    - Period-over-period variations
    - Acceleration patterns
    - Volatility and consistency

    Example:
        >>> calculator = TemporalCalculator()
        >>> df = pd.DataFrame({
        ...     "month": pd.date_range("2015-01", periods=6, freq="ME"),
        ...     "sales": [100, 110, 115, 130, 135, 150]
        ... })
        >>> config = {
        ...     "dimension_cols": ["month"],
        ...     "metric_cols": ["sales"]
        ... }
        >>> result = calculator.calculate(df, config)
        >>> result["tendencia"]
        'crescente'
    """

    def calculate(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate temporal-specific metrics

        Args:
            df: DataFrame with temporal data (should be sorted by time)
            config: Configuration dict with:
                - dimension_cols: List[str] - Temporal column (date, month, etc.)
                - metric_cols: List[str] - Value column to analyze

        Returns:
            Dict with temporal metrics:
                - total_periods: Number of time periods
                - valor_inicial: First value in series
                - valor_final: Last value in series
                - variacao_absoluta: Absolute change (final - initial)
                - variacao_percentual: Percentage change
                - valor_minimo: Minimum value in series
                - valor_maximo: Maximum value in series
                - amplitude: Range (max - min)
                - media: Average value
                - mediana: Median value
                - desvio_padrao: Standard deviation
                - coeficiente_variacao: Coefficient of variation (CV %)
                - tendencia: Trend direction ('crescente', 'decrescente', 'estavel')
                - inclinacao: Slope from linear regression
                - r_squared: R² from linear regression
                - volatilidade: Volatility measure
                - max_variacao_periodo: Largest period-over-period change
                - aceleracao: Acceleration indicator
                - consistencia: Consistency score
                - time_col: Name of temporal column
                - value_col: Name of value column

        Raises:
            ValueError: If config is invalid or required columns missing
        """
        # Validate config
        self._validate_config(config, ["dimension_cols", "metric_cols"])

        # Extract column names
        time_col = config["dimension_cols"][0]
        value_col = config["metric_cols"][0]

        # Validate columns exist
        self._validate_columns(df, [time_col, value_col])

        # Validate data is not empty
        if len(df) == 0:
            logger.warning("Empty DataFrame provided to TemporalCalculator")
            return self._empty_result(time_col, value_col)

        # Ensure sorted by time
        df_sorted = df.sort_values(time_col).reset_index(drop=True)

        # Extract values
        values = df_sorted[value_col].values
        n_periods = len(values)

        # Basic temporal metrics
        valor_inicial = float(values[0])
        valor_final = float(values[-1])
        variacao_absoluta = valor_final - valor_inicial
        variacao_percentual = self._get_percentage(variacao_absoluta, valor_inicial)

        # Statistical metrics
        valor_minimo = float(np.min(values))
        valor_maximo = float(np.max(values))
        amplitude = valor_maximo - valor_minimo
        media = float(np.mean(values))
        mediana = float(np.median(values))
        desvio_padrao = float(np.std(values))
        coeficiente_variacao = (
            self._get_percentage(desvio_padrao, media) if media != 0 else 0.0
        )

        # Trend analysis via linear regression
        trend_metrics = self._calculate_trend(values)

        # Period-over-period analysis
        variacao_periodo = np.diff(values)
        max_variacao_periodo = (
            float(np.max(np.abs(variacao_periodo)))
            if len(variacao_periodo) > 0
            else 0.0
        )

        # Volatility (standard deviation of period changes)
        volatilidade = (
            float(np.std(variacao_periodo)) if len(variacao_periodo) > 0 else 0.0
        )

        # Acceleration (trend in the changes)
        aceleracao = self._calculate_acceleration(variacao_periodo)

        # Consistency score (lower CV = more consistent)
        consistencia = max(0, 100 - abs(coeficiente_variacao))

        result = {
            # Basic metrics
            "total_periods": n_periods,
            "valor_inicial": valor_inicial,
            "valor_final": valor_final,
            "variacao_absoluta": variacao_absoluta,
            "variacao_percentual": variacao_percentual,
            # Range metrics
            "valor_minimo": valor_minimo,
            "valor_maximo": valor_maximo,
            "amplitude": amplitude,
            # Statistical metrics
            "media": media,
            "mediana": mediana,
            "desvio_padrao": desvio_padrao,
            "coeficiente_variacao": coeficiente_variacao,
            # Trend metrics
            "tendencia": trend_metrics["tendencia"],
            "inclinacao": trend_metrics["inclinacao"],
            "r_squared": trend_metrics["r_squared"],
            # Variation analysis
            "max_variacao_periodo": max_variacao_periodo,
            "volatilidade": volatilidade,
            "aceleracao": aceleracao,
            "consistencia": consistencia,
            # Context
            "time_col": time_col,
            "value_col": value_col,
        }

        logger.debug(
            f"TemporalCalculator completed: {n_periods} periods, "
            f"trend={result['tendencia']}, var={variacao_percentual:.2f}%"
        )

        return result

    def _calculate_trend(self, values: np.ndarray) -> Dict[str, Any]:
        """
        Calculate trend using linear regression

        Args:
            values: Array of values

        Returns:
            Dict with trend direction, slope, and R²
        """
        n = len(values)
        if n < 2:
            return {"tendencia": "estavel", "inclinacao": 0.0, "r_squared": 0.0}

        # Linear regression: y = ax + b
        x = np.arange(n)
        slope, intercept = np.polyfit(x, values, 1)

        # Calculate R²
        y_pred = slope * x + intercept
        ss_res = np.sum((values - y_pred) ** 2)
        ss_tot = np.sum((values - np.mean(values)) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0.0

        # Determine trend direction
        # Use percentage change to determine significance
        avg_value = np.mean(values)
        slope_pct = (slope / avg_value * 100) if avg_value != 0 else 0

        if abs(slope_pct) < 1:  # Less than 1% per period
            tendencia = "estavel"
        elif slope > 0:
            tendencia = "crescente"
        else:
            tendencia = "decrescente"

        return {
            "tendencia": tendencia,
            "inclinacao": float(slope),
            "r_squared": float(r_squared),
        }

    def _calculate_acceleration(self, variacao_periodo: np.ndarray) -> str:
        """
        Calculate acceleration/deceleration in trend

        Args:
            variacao_periodo: Period-over-period changes

        Returns:
            Acceleration descriptor
        """
        if len(variacao_periodo) < 2:
            return "estavel"

        # Calculate second derivative (change in changes)
        second_diff = np.diff(variacao_periodo)

        # Average acceleration
        avg_accel = np.mean(second_diff)

        # Determine acceleration type
        if abs(avg_accel) < 0.1:
            return "estavel"
        elif avg_accel > 0:
            return "acelerando"
        else:
            return "desacelerando"

    def _empty_result(self, time_col: str, value_col: str) -> Dict[str, Any]:
        """Return empty/zero result structure"""
        return {
            "total_periods": 0,
            "valor_inicial": 0.0,
            "valor_final": 0.0,
            "variacao_absoluta": 0.0,
            "variacao_percentual": 0.0,
            "valor_minimo": 0.0,
            "valor_maximo": 0.0,
            "amplitude": 0.0,
            "media": 0.0,
            "mediana": 0.0,
            "desvio_padrao": 0.0,
            "coeficiente_variacao": 0.0,
            "tendencia": "estavel",
            "inclinacao": 0.0,
            "r_squared": 0.0,
            "max_variacao_periodo": 0.0,
            "volatilidade": 0.0,
            "aceleracao": "estavel",
            "consistencia": 100.0,
            "time_col": time_col,
            "value_col": value_col,
        }
