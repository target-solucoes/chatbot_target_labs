"""
Temporal Analyzer Module

Analyzes temporal data granularity and determines decomposition requirements.
Implements intelligent DATE_TRUNC logic for hybrid temporal strategy.
"""

from enum import Enum
from typing import Optional, Tuple, Dict, Any
from functools import lru_cache
import logging
import pandas as pd

from src.shared_lib.models.schema import AnalyticsInputSpec, DimensionSpec

logger = logging.getLogger(__name__)


class TemporalGranularity(Enum):
    """
    Temporal data granularity levels.

    Used to determine the resolution of temporal data and whether
    decomposition is needed.
    """

    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    SEMESTER = "semester"
    YEAR = "year"
    UNKNOWN = "unknown"


class TemporalAnalyzer:
    """
    Analyzes temporal dimensions and determines decomposition requirements.

    This class implements the hybrid strategy: it determines whether to preserve
    detail rows or apply DATE_TRUNC based on data characteristics and query intent.
    """

    # Granularity hierarchy (finer to coarser)
    GRANULARITY_HIERARCHY = [
        TemporalGranularity.SECOND,
        TemporalGranularity.MINUTE,
        TemporalGranularity.HOUR,
        TemporalGranularity.DAY,
        TemporalGranularity.WEEK,
        TemporalGranularity.MONTH,
        TemporalGranularity.QUARTER,
        TemporalGranularity.SEMESTER,
        TemporalGranularity.YEAR,
    ]

    # Column name patterns for granularity detection
    GRANULARITY_PATTERNS = {
        TemporalGranularity.SECOND: ["segundo", "second", "seg"],
        TemporalGranularity.MINUTE: ["minuto", "minute", "min"],
        TemporalGranularity.HOUR: ["hora", "hour", "hr"],
        TemporalGranularity.DAY: ["dia", "day", "date", "data"],
        TemporalGranularity.WEEK: ["semana", "week", "wk"],
        TemporalGranularity.MONTH: ["mes", "month", "mon", "mês"],
        TemporalGranularity.QUARTER: ["trimestre", "quarter", "qtr"],
        TemporalGranularity.SEMESTER: ["semestre", "semester", "sem"],
        TemporalGranularity.YEAR: ["ano", "year", "yr"],
    }

    # DATE_TRUNC function templates
    DATE_TRUNC_TEMPLATES = {
        TemporalGranularity.SECOND: "DATE_TRUNC('second', {column})",
        TemporalGranularity.MINUTE: "DATE_TRUNC('minute', {column})",
        TemporalGranularity.HOUR: "DATE_TRUNC('hour', {column})",
        TemporalGranularity.DAY: "DATE_TRUNC('day', {column})",
        TemporalGranularity.WEEK: "DATE_TRUNC('week', {column})",
        TemporalGranularity.MONTH: "DATE_TRUNC('month', {column})",
        TemporalGranularity.QUARTER: "DATE_TRUNC('quarter', {column})",
        TemporalGranularity.YEAR: "DATE_TRUNC('year', {column})",
    }

    def __init__(self):
        """Initialize temporal analyzer with caching."""
        self._granularity_cache: Dict[str, TemporalGranularity] = {}

    def detect_dimension_granularity(
        self, dimension: DimensionSpec
    ) -> TemporalGranularity:
        """
        Detect granularity level from dimension name.

        Args:
            dimension: Dimension specification

        Returns:
            TemporalGranularity enum value

        Examples:
            >>> analyzer = TemporalAnalyzer()
            >>> dim = DimensionSpec(name="Mes")
            >>> analyzer.detect_dimension_granularity(dim)
            TemporalGranularity.MONTH
        """
        # Check cache
        cache_key = dimension.name.lower()
        if cache_key in self._granularity_cache:
            return self._granularity_cache[cache_key]

        dim_lower = dimension.name.lower()

        # Match against patterns
        for granularity, patterns in self.GRANULARITY_PATTERNS.items():
            if any(pattern in dim_lower for pattern in patterns):
                logger.debug(
                    f"Detected granularity {granularity.value} for dimension '{dimension.name}'"
                )
                self._granularity_cache[cache_key] = granularity
                return granularity

        # Default to UNKNOWN
        logger.warning(
            f"Could not determine granularity for dimension '{dimension.name}', "
            f"defaulting to UNKNOWN"
        )
        self._granularity_cache[cache_key] = TemporalGranularity.UNKNOWN
        return TemporalGranularity.UNKNOWN

    def detect_data_granularity(
        self, df: pd.DataFrame, temporal_column: str
    ) -> TemporalGranularity:
        """
        Detect actual granularity of temporal data in DataFrame.

        Analyzes the temporal column to determine its finest resolution.

        Args:
            df: DataFrame containing temporal data
            temporal_column: Name of temporal column to analyze

        Returns:
            TemporalGranularity enum value
        """
        if temporal_column not in df.columns:
            logger.warning(
                f"Temporal column '{temporal_column}' not found in DataFrame"
            )
            return TemporalGranularity.UNKNOWN

        col_data = df[temporal_column]

        # Convert to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(col_data):
            try:
                col_data = pd.to_datetime(col_data)
            except Exception as e:
                logger.error(f"Failed to convert column to datetime: {e}")
                return TemporalGranularity.UNKNOWN

        # Drop NaT values
        col_data = col_data.dropna()
        if len(col_data) == 0:
            return TemporalGranularity.UNKNOWN

        # Sample for performance (analyze max 1000 rows)
        if len(col_data) > 1000:
            col_data = col_data.sample(1000, random_state=42)

        # Check for time components
        has_seconds = (col_data.dt.second != 0).any()
        has_minutes = (col_data.dt.minute != 0).any()
        has_hours = (col_data.dt.hour != 0).any()

        # Determine finest granularity present
        if has_seconds:
            return TemporalGranularity.SECOND
        elif has_minutes:
            return TemporalGranularity.MINUTE
        elif has_hours:
            return TemporalGranularity.HOUR
        else:
            # Date-only data, check spacing between dates
            unique_dates = col_data.dt.date.nunique()
            date_range = (col_data.max() - col_data.min()).days

            if date_range == 0:
                return TemporalGranularity.DAY

            avg_spacing = date_range / max(unique_dates - 1, 1)

            if avg_spacing <= 1.5:
                return TemporalGranularity.DAY
            elif avg_spacing <= 10:
                return TemporalGranularity.WEEK
            elif avg_spacing <= 45:
                return TemporalGranularity.MONTH
            elif avg_spacing <= 180:
                return TemporalGranularity.QUARTER
            else:
                return TemporalGranularity.YEAR

    def needs_decomposition(
        self,
        dimension_granularity: TemporalGranularity,
        data_granularity: TemporalGranularity,
    ) -> bool:
        """
        Determine if temporal decomposition (DATE_TRUNC) is needed.

        Decomposition is needed when:
        - Data granularity is finer than dimension granularity
        - Example: Data is daily but dimension is Mes (month)

        Args:
            dimension_granularity: Requested granularity from dimension
            data_granularity: Actual granularity in data

        Returns:
            True if DATE_TRUNC should be applied

        Examples:
            >>> analyzer = TemporalAnalyzer()
            >>> analyzer.needs_decomposition(
            ...     TemporalGranularity.MONTH,
            ...     TemporalGranularity.DAY
            ... )
            True
        """
        if dimension_granularity == TemporalGranularity.UNKNOWN:
            return False

        if data_granularity == TemporalGranularity.UNKNOWN:
            return False

        # Get hierarchy positions
        try:
            dim_idx = self.GRANULARITY_HIERARCHY.index(dimension_granularity)
            data_idx = self.GRANULARITY_HIERARCHY.index(data_granularity)

            # Need decomposition if data is finer than dimension
            needs_it = data_idx < dim_idx

            logger.info(
                f"Decomposition analysis: dimension={dimension_granularity.value}, "
                f"data={data_granularity.value}, needs_decomposition={needs_it}"
            )

            return needs_it

        except ValueError as e:
            logger.error(f"Error comparing granularities: {e}")
            return False

    def build_temporal_expression(
        self,
        dimension: DimensionSpec,
        target_granularity: TemporalGranularity,
        source_column: str,
    ) -> str:
        """
        Build DATE_TRUNC expression for temporal decomposition.

        Args:
            dimension: Dimension specification
            target_granularity: Target granularity level
            source_column: Source column name to apply DATE_TRUNC to

        Returns:
            SQL expression string with DATE_TRUNC

        Examples:
            >>> analyzer = TemporalAnalyzer()
            >>> dim = DimensionSpec(name="Mes")
            >>> expr = analyzer.build_temporal_expression(
            ...     dim, TemporalGranularity.MONTH, "Data"
            ... )
            >>> print(expr)
            DATE_TRUNC('month', "Data")
        """
        template = self.DATE_TRUNC_TEMPLATES.get(target_granularity)

        if not template:
            logger.warning(
                f"No DATE_TRUNC template for granularity {target_granularity.value}, "
                f"using column as-is"
            )
            return f'"{source_column}"'

        # Escape column name
        escaped_column = f'"{source_column}"'
        expression = template.format(column=escaped_column)

        logger.debug(f"Built temporal expression for {dimension.name}: {expression}")

        return expression

    def analyze_spec_temporal_requirements(
        self, spec: AnalyticsInputSpec, df: Optional[pd.DataFrame] = None
    ) -> Dict[str, Any]:
        """
        Analyze temporal requirements for entire spec.

        This is the main entry point for determining temporal decomposition needs.

        Args:
            spec: Analytics input specification
            df: Optional DataFrame for data-based analysis

        Returns:
            Dictionary with analysis results:
            {
                'needs_decomposition': bool,
                'temporal_dimensions': List[DimensionSpec],
                'decomposition_specs': Dict[str, Dict]
            }
        """
        result = {
            "needs_decomposition": False,
            "temporal_dimensions": [],
            "decomposition_specs": {},
        }

        if not spec.dimensions:
            return result

        # Identify temporal dimensions
        temporal_dims = self._get_temporal_dimensions(spec.dimensions)
        result["temporal_dimensions"] = temporal_dims

        if not temporal_dims:
            return result

        # Analyze each temporal dimension
        for dim in temporal_dims:
            dim_granularity = self.detect_dimension_granularity(dim)

            decomposition_needed = False
            data_granularity = TemporalGranularity.UNKNOWN

            # If DataFrame provided, analyze data
            if df is not None:
                # Try to find corresponding column in data
                # Could be exact match or need alias resolution
                if dim.name in df.columns:
                    data_granularity = self.detect_data_granularity(df, dim.name)
                    decomposition_needed = self.needs_decomposition(
                        dim_granularity, data_granularity
                    )

            result["decomposition_specs"][dim.name] = {
                "dimension_granularity": dim_granularity,
                "data_granularity": data_granularity,
                "needs_decomposition": decomposition_needed,
                "target_expression": self.build_temporal_expression(
                    dim, dim_granularity, dim.name
                )
                if decomposition_needed
                else None,
            }

            if decomposition_needed:
                result["needs_decomposition"] = True

        logger.info(
            f"Temporal analysis complete: {len(temporal_dims)} temporal dimension(s), "
            f"decomposition_needed={result['needs_decomposition']}"
        )

        return result

    def find_temporal_column(self, df: pd.DataFrame) -> Optional[str]:
        """
        Automatically discover the temporal column in a DataFrame.

        Uses alias.yaml temporal configuration as the primary source.
        If alias.yaml has no temporal columns configured, returns None
        immediately (graceful degradation for non-temporal datasets).

        Args:
            df: DataFrame to analyze

        Returns:
            Name of temporal column if found, None otherwise
        """
        # Priority 0: Check alias.yaml configuration
        try:
            from src.shared_lib.core.config import get_temporal_columns

            configured_temporal = get_temporal_columns()
        except Exception:
            configured_temporal = []

        if not configured_temporal:
            logger.info(
                "No temporal columns configured in alias.yaml. Dataset is non-temporal."
            )
            return None

        # Priority 1: Check configured temporal columns from alias.yaml
        for col_name in configured_temporal:
            if col_name in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col_name]):
                    logger.debug(f"Found temporal column from alias.yaml: {col_name}")
                    return col_name
                try:
                    pd.to_datetime(df[col_name].dropna().head(5))
                    logger.debug(
                        f"Found convertible temporal column from alias.yaml: {col_name}"
                    )
                    return col_name
                except (ValueError, TypeError):
                    pass

        # Priority 2: Fallback - scan for datetime dtypes
        for col_name in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col_name]):
                logger.debug(f"Found temporal column by dtype: {col_name}")
                return col_name

        logger.warning("No temporal column found in DataFrame")
        return None

    def infer_target_granularity(
        self, dimension_name: str, chart_type: Optional[str] = None
    ) -> TemporalGranularity:
        """
        Infer the target temporal granularity based on dimension name and chart type.

        This method handles semantic dimension names like "Mes", "Ano" that don't
        exist as columns but indicate the desired aggregation level.

        IMPORTANT: When dimension is the actual temporal column name (like "Data"),
        we infer MONTH granularity for line charts to show historical trends,
        not raw daily detail.

        Args:
            dimension_name: Name of the dimension (e.g., "Mes", "Ano", "Data")
            chart_type: Optional chart type for context (e.g., "line", "bar_vertical")

        Returns:
            Inferred TemporalGranularity

        Examples:
            >>> analyzer = TemporalAnalyzer()
            >>> analyzer.infer_target_granularity("Mes")
            TemporalGranularity.MONTH
            >>> analyzer.infer_target_granularity("Ano")
            TemporalGranularity.YEAR
            >>> analyzer.infer_target_granularity("Data", chart_type="line")
            TemporalGranularity.MONTH  # Infers monthly aggregation for historical trends
        """
        dim_lower = dimension_name.lower()

        # SPECIAL CASE: Handle actual temporal column dimension FIRST
        # The temporal column name is resolved dynamically from DatasetConfig/alias.yaml.
        # When the dimension IS the temporal column itself, infer granularity from chart type.
        _temporal_col_names = set()
        try:
            from src.shared_lib.core.dataset_config import DatasetConfig

            tc = DatasetConfig.get_instance().temporal_columns
            _temporal_col_names = {c.lower() for c in tc}
        except Exception:
            try:
                from src.shared_lib.core.config import get_temporal_columns

                _temporal_col_names = {c.lower() for c in get_temporal_columns()}
            except Exception:
                pass
        # Also include generic temporal names for backward compatibility
        _temporal_col_names.update({"date"})

        if dim_lower in _temporal_col_names:
            # For historical trend charts (line, line_composed), default to MONTH
            if chart_type and chart_type in ["line", "line_composed", "area"]:
                logger.info(
                    f"Dimension '{dimension_name}' with chart_type '{chart_type}': "
                    f"inferring MONTH for historical trend aggregation"
                )
                return TemporalGranularity.MONTH

            # For other chart types, preserve day-level detail
            logger.debug(
                f"Dimension '{dimension_name}' with chart_type '{chart_type}': inferring DAY"
            )
            return TemporalGranularity.DAY

        # Direct pattern matching for semantic dimensions (Mes, Ano, Trimestre, etc.)
        for granularity, patterns in self.GRANULARITY_PATTERNS.items():
            # Skip DAY patterns for temporal column names handled above
            if granularity == TemporalGranularity.DAY:
                # Only match day-specific keywords, not generic "data"
                day_specific = ["dia", "day"]
                if any(pattern in dim_lower for pattern in day_specific):
                    logger.debug(
                        f"Inferred granularity {granularity.value} from dimension '{dimension_name}'"
                    )
                    return granularity
            else:
                if any(pattern in dim_lower for pattern in patterns):
                    logger.debug(
                        f"Inferred granularity {granularity.value} from dimension '{dimension_name}'"
                    )
                    return granularity

        # Default to UNKNOWN
        logger.warning(
            f"Could not infer granularity for dimension '{dimension_name}', "
            f"defaulting to UNKNOWN"
        )
        return TemporalGranularity.UNKNOWN

    @staticmethod
    def _get_temporal_dimensions(dimensions: list) -> list:
        """
        Extract temporal dimensions from dimension list.

        Args:
            dimensions: List of dimension specifications

        Returns:
            List of temporal dimensions
        """
        temporal_keywords = {
            "data",
            "date",
            "datetime",
            "timestamp",
            "ano",
            "year",
            "mes",
            "month",
            "dia",
            "day",
            "trimestre",
            "quarter",
            "semestre",
            "semester",
            "semana",
            "week",
            "hora",
            "hour",
            "periodo",
            "period",
        }

        temporal_dims = []
        for dim in dimensions:
            dim_name = dim.name.lower()
            if any(keyword in dim_name for keyword in temporal_keywords):
                temporal_dims.append(dim)

        return temporal_dims


# Singleton instance for caching
_temporal_analyzer_instance: Optional[TemporalAnalyzer] = None


def get_temporal_analyzer() -> TemporalAnalyzer:
    """
    Get singleton instance of TemporalAnalyzer.

    Returns:
        TemporalAnalyzer instance
    """
    global _temporal_analyzer_instance
    if _temporal_analyzer_instance is None:
        _temporal_analyzer_instance = TemporalAnalyzer()
    return _temporal_analyzer_instance
