"""
ComparisonCalculator for bar_vertical chart type

Calculates metrics for comparison visualizations:
- Amplitude and range analysis
- Dispersion and variability
- Outlier detection
"""
import logging
from typing import Dict, Any
import pandas as pd
import numpy as np
from .base import BaseCalculator

logger = logging.getLogger(__name__)


class ComparisonCalculator(BaseCalculator):
    """
    Calculator for bar_vertical (comparison) visualizations

    Analyzes comparative data to extract insights about:
    - Range and amplitude
    - Statistical dispersion
    - Outliers and anomalies
    - Relative performance

    Example:
        >>> calculator = ComparisonCalculator()
        >>> df = pd.DataFrame({
        ...     "quarter": ["Q1", "Q2", "Q3", "Q4"],
        ...     "revenue": [100, 150, 120, 180]
        ... })
        >>> config = {
        ...     "dimension_cols": ["quarter"],
        ...     "metric_cols": ["revenue"]
        ... }
        >>> result = calculator.calculate(df, config)
    """

    def calculate(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate comparison-specific metrics

        Args:
            df: DataFrame with comparison data
            config: Configuration dict

        Returns:
            Dict with comparison metrics
        """
        self._validate_config(config, ["dimension_cols", "metric_cols"])

        label_col = config["dimension_cols"][0]
        value_col = config["metric_cols"][0]

        self._validate_columns(df, [label_col, value_col])

        if len(df) == 0:
            return self._empty_result(label_col, value_col)

        values = df[value_col].values

        # Basic stats
        valor_maximo = float(np.max(values))
        valor_minimo = float(np.min(values))
        amplitude = valor_maximo - valor_minimo
        media = float(np.mean(values))
        mediana = float(np.median(values))
        desvio_padrao = float(np.std(values))

        # Coefficient of variation
        coeficiente_variacao = self._get_percentage(desvio_padrao, media) if media != 0 else 0.0

        # Identify max and min
        idx_max = int(np.argmax(values))
        idx_min = int(np.argmin(values))
        maior_label = df[label_col].iloc[idx_max]
        menor_label = df[label_col].iloc[idx_min]

        # Dispersion level
        if coeficiente_variacao < 10:
            dispersao_nivel = "baixa"
        elif coeficiente_variacao < 30:
            dispersao_nivel = "moderada"
        else:
            dispersao_nivel = "alta"

        # Outlier detection (simple IQR method)
        q1 = float(np.percentile(values, 25))
        q3 = float(np.percentile(values, 75))
        iqr = q3 - q1
        outlier_lower = q1 - 1.5 * iqr
        outlier_upper = q3 + 1.5 * iqr
        n_outliers = int(np.sum((values < outlier_lower) | (values > outlier_upper)))

        return {
            "n_items": len(df),
            "valor_maximo": valor_maximo,
            "maior_label": maior_label,
            "valor_minimo": valor_minimo,
            "menor_label": menor_label,
            "amplitude": amplitude,
            "media": media,
            "mediana": mediana,
            "desvio_padrao": desvio_padrao,
            "coeficiente_variacao": coeficiente_variacao,
            "dispersao_nivel": dispersao_nivel,
            "q1": q1,
            "q3": q3,
            "iqr": iqr,
            "n_outliers": n_outliers,
            "label_col": label_col,
            "value_col": value_col,
        }

    def _empty_result(self, label_col: str, value_col: str) -> Dict[str, Any]:
        return {
            "n_items": 0,
            "valor_maximo": 0.0,
            "maior_label": "N/A",
            "valor_minimo": 0.0,
            "menor_label": "N/A",
            "amplitude": 0.0,
            "media": 0.0,
            "mediana": 0.0,
            "desvio_padrao": 0.0,
            "coeficiente_variacao": 0.0,
            "dispersao_nivel": "baixa",
            "q1": 0.0,
            "q3": 0.0,
            "iqr": 0.0,
            "n_outliers": 0,
            "label_col": label_col,
            "value_col": value_col,
        }
