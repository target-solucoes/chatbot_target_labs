"""
Atomic Metric Modules for Composable Insight Generation.

FASE 2 Implementation - Metric Composer

This module provides atomic, reusable metric calculation modules that can be
composed based on intent rather than chart type. Each module calculates a
specific class of metrics independent of visualization.

Architecture:
    - MetricModule: Abstract base for all metric modules
    - VariationModule: Calculates delta, growth_rate, absolute_change
    - ConcentrationModule: Calculates HHI, Top N share, Gini coefficient
    - GapModule: Calculates competitive gaps between positions
    - TemporalModule: Calculates trend, volatility, seasonality
    - DistributionModule: Calculates statistical distribution metrics
    - ComparativeModule: Calculates ratios and benchmarks

Key Principles:
    1. No hardcoded column names - all from config
    2. No chart_type dependencies
    3. Pure computation - no text generation
    4. Composable and reusable across intents
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class MetricModule(ABC):
    """
    Abstract base class for atomic metric modules.

    Each module is responsible for calculating a specific family of metrics
    that can be composed with other modules based on intent.
    """

    @abstractmethod
    def calculate(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate module-specific metrics.

        Args:
            df: DataFrame with processed data
            config: Configuration with column mappings and context
                Required keys:
                    - dimension_cols: List[str] - Dimension column names
                    - metric_cols: List[str] - Metric column names
                Optional keys (module-specific):
                    - top_n: int - Number of top items
                    - time_col: str - Temporal column name
                    - aggregation: str - Aggregation function used

        Returns:
            Dictionary with calculated metrics specific to this module
        """
        pass

    def _safe_divide(
        self, numerator: float, denominator: float, default: float = 0.0
    ) -> float:
        """Safe division with zero handling."""
        try:
            if denominator == 0 or pd.isna(denominator):
                return default
            result = numerator / denominator
            return default if pd.isna(result) else result
        except Exception:
            return default

    def _get_total(self, df: pd.DataFrame, value_col: str) -> float:
        """Calculate total sum of a column."""
        if value_col not in df.columns:
            return 0.0
        return float(df[value_col].sum())


class VariationModule(MetricModule):
    """
    Calculates variation/delta metrics between periods or entities.

    Metrics:
        - absolute_change: Difference between two values
        - growth_rate: Percentage change
        - period_comparison: Multi-period comparison
        - acceleration: Rate of change of change
    """

    def calculate(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate variation metrics.

        Required config:
            - metric_cols: List[str] - Value columns to analyze
            - dimension_cols: List[str] - Grouping dimensions

        Optional config:
            - time_col: str - Temporal dimension for period-over-period
            - comparison_periods: List[str] - Specific periods to compare

        Returns:
            {
                "variation_type": str,
                "deltas": List[Dict] - List of {entity, from_value, to_value, delta, growth_rate}
                "total_variation": float,
                "variation_pct": float,
                "max_increase": Dict,
                "max_decrease": Dict,
                "median_variation": float
            }
        """
        metrics = {
            "variation_type": "not_calculated",
            "deltas": [],
            "total_variation": 0.0,
            "variation_pct": 0.0,
            "max_increase": {},
            "max_decrease": {},
            "median_variation": 0.0,
        }

        try:
            if df.empty or not config.get("metric_cols"):
                return metrics

            value_col = config["metric_cols"][0]
            dimension_cols = config.get("dimension_cols", [])
            time_col = config.get("time_col")

            # Check if temporal variation is possible
            if time_col and time_col in df.columns:
                metrics.update(
                    self._calculate_temporal_variation(
                        df, value_col, time_col, dimension_cols
                    )
                )
            else:
                # Categorical variation (between entities)
                metrics.update(
                    self._calculate_categorical_variation(df, value_col, dimension_cols)
                )

        except Exception as e:
            logger.error(f"Error in VariationModule: {e}")

        return metrics

    def _calculate_temporal_variation(
        self, df: pd.DataFrame, value_col: str, time_col: str, dimension_cols: List[str]
    ) -> Dict[str, Any]:
        """Calculate variation across time periods."""
        result = {"variation_type": "temporal", "deltas": []}

        try:
            # Sort by time
            df_sorted = df.sort_values(time_col)

            if len(dimension_cols) > 0:
                # Variation per entity across time
                entity_col = dimension_cols[0]
                for entity in df_sorted[entity_col].unique():
                    entity_data = df_sorted[df_sorted[entity_col] == entity]
                    if len(entity_data) >= 2:
                        first_value = entity_data.iloc[0][value_col]
                        last_value = entity_data.iloc[-1][value_col]
                        delta = last_value - first_value
                        growth_rate = self._safe_divide(delta, first_value, 0.0) * 100

                        result["deltas"].append(
                            {
                                "entity": entity,
                                "from_value": float(first_value),
                                "to_value": float(last_value),
                                "delta": float(delta),
                                "growth_rate": float(growth_rate),
                                "from_period": str(entity_data.iloc[0][time_col]),
                                "to_period": str(entity_data.iloc[-1][time_col]),
                            }
                        )
            else:
                # Overall temporal variation
                if len(df_sorted) >= 2:
                    first_value = df_sorted.iloc[0][value_col]
                    last_value = df_sorted.iloc[-1][value_col]
                    delta = last_value - first_value
                    growth_rate = self._safe_divide(delta, first_value, 0.0) * 100

                    result["deltas"].append(
                        {
                            "entity": "total",
                            "from_value": float(first_value),
                            "to_value": float(last_value),
                            "delta": float(delta),
                            "growth_rate": float(growth_rate),
                        }
                    )

            # Aggregate statistics
            if result["deltas"]:
                deltas_values = [d["delta"] for d in result["deltas"]]
                growth_rates = [d["growth_rate"] for d in result["deltas"]]

                result["total_variation"] = float(np.sum(deltas_values))
                result["median_variation"] = float(np.median(deltas_values))
                result["variation_pct"] = float(np.mean(growth_rates))

                # Max increase/decrease
                max_idx = np.argmax(deltas_values)
                min_idx = np.argmin(deltas_values)
                result["max_increase"] = (
                    result["deltas"][max_idx] if deltas_values[max_idx] > 0 else {}
                )
                result["max_decrease"] = (
                    result["deltas"][min_idx] if deltas_values[min_idx] < 0 else {}
                )

        except Exception as e:
            logger.error(f"Error calculating temporal variation: {e}")

        return result

    def _calculate_categorical_variation(
        self, df: pd.DataFrame, value_col: str, dimension_cols: List[str]
    ) -> Dict[str, Any]:
        """Calculate variation between categories (e.g., gap analysis)."""
        result = {"variation_type": "categorical", "deltas": []}

        try:
            if len(df) >= 2:
                # Calculate deltas between consecutive items (assumes sorted by value)
                df_sorted = df.sort_values(value_col, ascending=False)

                for i in range(len(df_sorted) - 1):
                    curr_value = df_sorted.iloc[i][value_col]
                    next_value = df_sorted.iloc[i + 1][value_col]
                    delta = curr_value - next_value

                    entity_label = "unknown"
                    if dimension_cols and dimension_cols[0] in df_sorted.columns:
                        entity_label = str(df_sorted.iloc[i][dimension_cols[0]])

                    result["deltas"].append(
                        {
                            "entity": entity_label,
                            "position": i + 1,
                            "value": float(curr_value),
                            "next_value": float(next_value),
                            "delta": float(delta),
                            "delta_pct": float(
                                self._safe_divide(delta, next_value, 0.0) * 100
                            ),
                        }
                    )

                # Aggregate stats
                if result["deltas"]:
                    deltas_values = [d["delta"] for d in result["deltas"]]
                    result["total_variation"] = float(np.sum(deltas_values))
                    result["median_variation"] = float(np.median(deltas_values))
                    result["max_increase"] = (
                        result["deltas"][0] if result["deltas"] else {}
                    )

        except Exception as e:
            logger.error(f"Error calculating categorical variation: {e}")

        return result


class ConcentrationModule(MetricModule):
    """
    Calculates concentration and market power metrics.

    Metrics:
        - hhi: Herfindahl-Hirschman Index
        - top_n_share: Percentage share of top N entities
        - gini_coefficient: Inequality measure
        - cumulative_distribution: Lorenz curve data
    """

    def calculate(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate concentration metrics.

        Required config:
            - metric_cols: List[str] - Value columns
            - dimension_cols: List[str] - Entity dimensions

        Optional config:
            - top_n: int - Number of top entities (default: 5)

        Returns:
            {
                "hhi": float - Herfindahl Index (0-10000)
                "top_n": int,
                "top_n_share": float - Percentage (0-100)
                "top_n_values": List[float],
                "gini_coefficient": float - (0-1)
                "concentration_level": str - "high"/"medium"/"low"
                "cumulative_shares": List[float] - For Lorenz curve
            }
        """
        metrics = {
            "hhi": 0.0,
            "top_n": 0,
            "top_n_share": 0.0,
            "top_n_values": [],
            "gini_coefficient": 0.0,
            "concentration_level": "unknown",
            "cumulative_shares": [],
        }

        try:
            if df.empty or not config.get("metric_cols"):
                return metrics

            value_col = config["metric_cols"][0]
            top_n = config.get("top_n", 5)

            # Sort by value descending
            df_sorted = df.sort_values(value_col, ascending=False)
            total = self._get_total(df, value_col)

            if total == 0:
                return metrics

            # Top N share
            top_n_df = df_sorted.head(top_n)
            top_n_total = self._get_total(top_n_df, value_col)
            top_n_share = (top_n_total / total) * 100

            metrics["top_n"] = top_n
            metrics["top_n_share"] = float(top_n_share)
            metrics["top_n_values"] = top_n_df[value_col].tolist()

            # HHI (Herfindahl-Hirschman Index)
            market_shares = (df_sorted[value_col] / total) * 100
            hhi = (market_shares**2).sum()
            metrics["hhi"] = float(hhi)

            # Concentration level interpretation
            if hhi > 2500:
                metrics["concentration_level"] = "high"
            elif hhi > 1500:
                metrics["concentration_level"] = "medium"
            else:
                metrics["concentration_level"] = "low"

            # Gini coefficient
            values_sorted = df_sorted[value_col].values
            metrics["gini_coefficient"] = float(self._calculate_gini(values_sorted))

            # Cumulative shares for Lorenz curve
            cumulative_sum = 0
            cumulative_shares = []
            for value in df_sorted[value_col]:
                cumulative_sum += value
                cumulative_shares.append((cumulative_sum / total) * 100)
            metrics["cumulative_shares"] = cumulative_shares

        except Exception as e:
            logger.error(f"Error in ConcentrationModule: {e}")

        return metrics

    def _calculate_gini(self, values: np.ndarray) -> float:
        """Calculate Gini coefficient."""
        try:
            if len(values) == 0:
                return 0.0

            sorted_values = np.sort(values)
            n = len(sorted_values)
            cumsum = np.cumsum(sorted_values)

            # Gini formula
            gini = (2 * np.sum((np.arange(1, n + 1)) * sorted_values)) / (
                n * np.sum(sorted_values)
            ) - (n + 1) / n
            return max(0.0, min(1.0, gini))  # Clamp between 0 and 1
        except Exception:
            return 0.0


class GapModule(MetricModule):
    """
    Calculates competitive gaps between entities.

    Metrics:
        - leader_gap: Distance from leader to second place
        - position_gaps: Gaps between all consecutive positions
        - gap_to_average: Distance from average
        - outlier_gaps: Unusually large gaps
    """

    def calculate(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate gap metrics.

        Required config:
            - metric_cols: List[str] - Value columns
            - dimension_cols: List[str] - Entity dimensions

        Returns:
            {
                "leader_value": float,
                "second_value": float,
                "leader_gap_absolute": float,
                "leader_gap_percentage": float,
                "position_gaps": List[Dict] - Gaps between consecutive positions
                "average_value": float,
                "max_gap": Dict,
                "min_gap": Dict
            }
        """
        metrics = {
            "leader_value": 0.0,
            "second_value": 0.0,
            "leader_gap_absolute": 0.0,
            "leader_gap_percentage": 0.0,
            "position_gaps": [],
            "average_value": 0.0,
            "max_gap": {},
            "min_gap": {},
        }

        try:
            if df.empty or not config.get("metric_cols"):
                return metrics

            value_col = config["metric_cols"][0]
            dimension_cols = config.get("dimension_cols", [])

            # Sort by value descending
            df_sorted = df.sort_values(value_col, ascending=False)

            if len(df_sorted) >= 2:
                leader_value = df_sorted.iloc[0][value_col]
                second_value = df_sorted.iloc[1][value_col]

                metrics["leader_value"] = float(leader_value)
                metrics["second_value"] = float(second_value)
                metrics["leader_gap_absolute"] = float(leader_value - second_value)
                metrics["leader_gap_percentage"] = float(
                    self._safe_divide(leader_value - second_value, second_value, 0.0)
                    * 100
                )

            # Calculate gaps between all consecutive positions
            for i in range(len(df_sorted) - 1):
                curr_value = df_sorted.iloc[i][value_col]
                next_value = df_sorted.iloc[i + 1][value_col]
                gap = curr_value - next_value
                gap_pct = self._safe_divide(gap, next_value, 0.0) * 100

                entity_label = "unknown"
                if dimension_cols and dimension_cols[0] in df_sorted.columns:
                    entity_label = str(df_sorted.iloc[i][dimension_cols[0]])

                metrics["position_gaps"].append(
                    {
                        "position": i + 1,
                        "entity": entity_label,
                        "value": float(curr_value),
                        "next_value": float(next_value),
                        "gap_absolute": float(gap),
                        "gap_percentage": float(gap_pct),
                    }
                )

            # Average value
            metrics["average_value"] = float(df_sorted[value_col].mean())

            # Max and min gaps
            if metrics["position_gaps"]:
                gaps_abs = [g["gap_absolute"] for g in metrics["position_gaps"]]
                max_idx = np.argmax(gaps_abs)
                min_idx = np.argmin(gaps_abs)
                metrics["max_gap"] = metrics["position_gaps"][max_idx]
                metrics["min_gap"] = metrics["position_gaps"][min_idx]

        except Exception as e:
            logger.error(f"Error in GapModule: {e}")

        return metrics


class TemporalModule(MetricModule):
    """
    Calculates temporal trend and pattern metrics.

    Metrics:
        - trend_direction: "increasing"/"decreasing"/"stable"
        - trend_strength: Linear regression slope
        - volatility: Standard deviation / mean
        - momentum: Recent vs overall trend
        - seasonality_detected: Boolean flag
    """

    def calculate(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate temporal metrics.

        Required config:
            - metric_cols: List[str] - Value columns
            - time_col: str - Temporal dimension

        Returns:
            {
                "trend_direction": str - "increasing"/"decreasing"/"stable"
                "trend_strength": float - Slope value
                "trend_r_squared": float - Fit quality (0-1)
                "volatility": float - Coefficient of variation
                "momentum": float - Recent vs overall change
                "avg_value": float,
                "min_value": float,
                "max_value": float,
                "total_variation": float,
                "periods_count": int
            }
        """
        metrics = {
            "trend_direction": "unknown",
            "trend_strength": 0.0,
            "trend_r_squared": 0.0,
            "volatility": 0.0,
            "momentum": 0.0,
            "avg_value": 0.0,
            "min_value": 0.0,
            "max_value": 0.0,
            "total_variation": 0.0,
            "periods_count": 0,
        }

        try:
            if df.empty or not config.get("metric_cols") or not config.get("time_col"):
                return metrics

            value_col = config["metric_cols"][0]
            time_col = config["time_col"]

            if time_col not in df.columns:
                logger.warning(f"Time column '{time_col}' not found in DataFrame")
                return metrics

            # Sort by time
            df_sorted = df.sort_values(time_col).reset_index(drop=True)
            values = df_sorted[value_col].values
            n = len(values)

            metrics["periods_count"] = n
            metrics["avg_value"] = float(np.mean(values))
            metrics["min_value"] = float(np.min(values))
            metrics["max_value"] = float(np.max(values))

            if n < 2:
                return metrics

            # Total variation (first to last)
            metrics["total_variation"] = float(values[-1] - values[0])

            # Linear trend analysis
            x = np.arange(n)
            slope, r_squared = self._calculate_linear_trend(x, values)

            metrics["trend_strength"] = float(slope)
            metrics["trend_r_squared"] = float(r_squared)

            # Trend direction
            if abs(slope) < 0.01 * np.mean(values):  # Less than 1% per period
                metrics["trend_direction"] = "stable"
            elif slope > 0:
                metrics["trend_direction"] = "increasing"
            else:
                metrics["trend_direction"] = "decreasing"

            # Volatility (coefficient of variation)
            std_dev = np.std(values)
            mean_val = np.mean(values)
            metrics["volatility"] = float(self._safe_divide(std_dev, mean_val, 0.0))

            # Momentum (recent half vs overall trend)
            if n >= 4:
                recent_half = n // 2
                recent_slope, _ = self._calculate_linear_trend(
                    np.arange(recent_half), values[-recent_half:]
                )
                metrics["momentum"] = float(self._safe_divide(recent_slope, slope, 1.0))

        except Exception as e:
            logger.error(f"Error in TemporalModule: {e}")

        return metrics

    def _calculate_linear_trend(self, x: np.ndarray, y: np.ndarray) -> tuple:
        """Calculate linear regression slope and R²."""
        try:
            if len(x) < 2:
                return 0.0, 0.0

            # Linear regression
            coeffs = np.polyfit(x, y, 1)
            slope = coeffs[0]

            # R² calculation
            y_pred = np.polyval(coeffs, x)
            ss_res = np.sum((y - y_pred) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0.0

            return slope, max(0.0, min(1.0, r_squared))
        except Exception:
            return 0.0, 0.0


class DistributionModule(MetricModule):
    """
    Calculates statistical distribution metrics.

    Metrics:
        - mean, median, mode
        - std_dev, variance
        - percentiles (25th, 50th, 75th)
        - outliers (IQR method)
        - skewness, kurtosis
    """

    def calculate(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate distribution metrics.

        Required config:
            - metric_cols: List[str] - Value columns

        Returns:
            {
                "mean": float,
                "median": float,
                "std_dev": float,
                "variance": float,
                "min": float,
                "max": float,
                "percentile_25": float,
                "percentile_75": float,
                "iqr": float,
                "outliers_count": int,
                "outliers": List[float],
                "skewness": float,
                "distribution_shape": str - "symmetric"/"right_skewed"/"left_skewed"
            }
        """
        metrics = {
            "mean": 0.0,
            "median": 0.0,
            "std_dev": 0.0,
            "variance": 0.0,
            "min": 0.0,
            "max": 0.0,
            "percentile_25": 0.0,
            "percentile_75": 0.0,
            "iqr": 0.0,
            "outliers_count": 0,
            "outliers": [],
            "skewness": 0.0,
            "distribution_shape": "unknown",
        }

        try:
            if df.empty or not config.get("metric_cols"):
                return metrics

            value_col = config["metric_cols"][0]
            values = df[value_col].dropna().values

            if len(values) == 0:
                return metrics

            # Basic statistics
            metrics["mean"] = float(np.mean(values))
            metrics["median"] = float(np.median(values))
            metrics["std_dev"] = float(np.std(values))
            metrics["variance"] = float(np.var(values))
            metrics["min"] = float(np.min(values))
            metrics["max"] = float(np.max(values))

            # Percentiles
            metrics["percentile_25"] = float(np.percentile(values, 25))
            metrics["percentile_75"] = float(np.percentile(values, 75))
            metrics["iqr"] = metrics["percentile_75"] - metrics["percentile_25"]

            # Outliers detection (IQR method)
            lower_bound = metrics["percentile_25"] - 1.5 * metrics["iqr"]
            upper_bound = metrics["percentile_75"] + 1.5 * metrics["iqr"]
            outliers = values[(values < lower_bound) | (values > upper_bound)]
            metrics["outliers_count"] = len(outliers)
            metrics["outliers"] = outliers.tolist()

            # Skewness
            if len(values) >= 3 and metrics["std_dev"] > 0:
                skewness = np.mean(
                    ((values - metrics["mean"]) / metrics["std_dev"]) ** 3
                )
                metrics["skewness"] = float(skewness)

                # Distribution shape
                if abs(skewness) < 0.5:
                    metrics["distribution_shape"] = "symmetric"
                elif skewness > 0:
                    metrics["distribution_shape"] = "right_skewed"
                else:
                    metrics["distribution_shape"] = "left_skewed"

        except Exception as e:
            logger.error(f"Error in DistributionModule: {e}")

        return metrics


class ComparativeModule(MetricModule):
    """
    Calculates comparative and benchmark metrics.

    Metrics:
        - entity_vs_average: Each entity's deviation from average
        - entity_vs_leader: Each entity's distance from leader
        - relative_index: Normalized scores (0-100)
        - performance_bands: Categorization (high/medium/low)
    """

    def calculate(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate comparative metrics.

        Required config:
            - metric_cols: List[str] - Value columns
            - dimension_cols: List[str] - Entity dimensions

        Returns:
            {
                "comparisons": List[Dict] - Per entity comparison
                "average_value": float,
                "leader_value": float,
                "benchmark_type": str,
                "above_average_count": int,
                "below_average_count": int
            }
        """
        metrics = {
            "comparisons": [],
            "average_value": 0.0,
            "leader_value": 0.0,
            "benchmark_type": "average",
            "above_average_count": 0,
            "below_average_count": 0,
        }

        try:
            if df.empty or not config.get("metric_cols"):
                return metrics

            value_col = config["metric_cols"][0]
            dimension_cols = config.get("dimension_cols", [])

            # Calculate benchmarks
            average = df[value_col].mean()
            leader = df[value_col].max()

            metrics["average_value"] = float(average)
            metrics["leader_value"] = float(leader)

            # Per-entity comparisons
            for idx, row in df.iterrows():
                entity_value = row[value_col]

                entity_label = "unknown"
                if dimension_cols and dimension_cols[0] in df.columns:
                    entity_label = str(row[dimension_cols[0]])

                deviation_from_avg = entity_value - average
                distance_from_leader = leader - entity_value
                relative_to_avg_pct = (
                    self._safe_divide(deviation_from_avg, average, 0.0) * 100
                )
                relative_to_leader_pct = (
                    self._safe_divide(entity_value, leader, 0.0) * 100
                )

                # Performance band
                if entity_value >= average * 1.2:
                    performance_band = "high"
                elif entity_value >= average * 0.8:
                    performance_band = "medium"
                else:
                    performance_band = "low"

                metrics["comparisons"].append(
                    {
                        "entity": entity_label,
                        "value": float(entity_value),
                        "deviation_from_average": float(deviation_from_avg),
                        "relative_to_average_pct": float(relative_to_avg_pct),
                        "distance_from_leader": float(distance_from_leader),
                        "relative_to_leader_pct": float(relative_to_leader_pct),
                        "performance_band": performance_band,
                    }
                )

                # Count above/below average
                if entity_value > average:
                    metrics["above_average_count"] += 1
                elif entity_value < average:
                    metrics["below_average_count"] += 1

        except Exception as e:
            logger.error(f"Error in ComparativeModule: {e}")

        return metrics
