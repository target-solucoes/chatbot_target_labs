"""
Pydantic schemas and TypedDict definitions for the agent.

This module defines the data structures for:
- Input queries
- Output JSON format
- Internal graph state
"""

from typing import TypedDict, Optional, List, Dict, Any, Literal, TYPE_CHECKING, Union
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from dataclasses import dataclass
from datetime import datetime

if TYPE_CHECKING:
    import pandas as pd
else:
    try:
        import pandas as pd
    except ImportError:
        pd = None


# ============================================================================
# OUTPUT SCHEMA (Pydantic)
# ============================================================================

# FASE 2: "line" removed - use "line_composed" with render_variant instead
ChartTypeLiteral = Literal[
    "bar_horizontal",
    "bar_vertical",
    "bar_vertical_composed",
    "line_composed",
    "pie",
    "bar_vertical_stacked",
    "histogram",
]


def _prettify_label(value: Optional[str]) -> Optional[str]:
    """Convert a snake_case identifier into a human-friendly label."""

    if not value:
        return None

    label = value.replace("_", " ").strip()
    if not label:
        return None

    return label[:1].upper() + label[1:]


def _infer_metric_unit(column_name: Optional[str]) -> Optional[str]:
    """Infer a default unit for a metric based on its name."""

    if not column_name:
        return None

    lower = column_name.lower()

    if any(
        keyword in lower
        for keyword in ["valor", "faturamento", "receita", "preço", "preco", "custo"]
    ):
        return "R$"

    if any(
        keyword in lower for keyword in ["percent", "taxa", "participa", "quota", "%"]
    ):
        return "%"

    if any(
        keyword in lower
        for keyword in ["quantidade", "qtd", "volume", "numero", "número", "contagem"]
    ):
        return "unidade"

    return None


# ============================================================================
# FILTER SPECIFICATIONS (Typed Filters)
# ============================================================================


class OperatorFilter(BaseModel):
    """Filter with comparison operator."""

    operator: Literal[">=", "<=", ">", "<", "=", "!="] = Field(
        ..., description="Comparison operator"
    )

    value: Union[int, float, str, bool] = Field(
        ..., description="Value to compare against"
    )


class BetweenFilter(BaseModel):
    """Filter for range queries (BETWEEN)."""

    between: List[Union[int, float, str]] = Field(
        ...,
        min_length=2,
        max_length=2,
        description="Range boundaries [min, max] for BETWEEN clause",
    )


# Union type for filter values supporting multiple formats
FilterValue = Union[
    str,  # Simple equality: {"Ano": "2015"}
    int,  # Simple equality: {"Ano": 2015}
    float,  # Simple equality: {"Valor": 1000.50}
    bool,  # Simple equality: {"Ativo": true}
    List[Union[str, int, float, bool]],  # IN clause: {"UF": ["SP", "RJ", "MG"]}
    OperatorFilter,  # Comparison: {"Valor": {"operator": ">=", "value": 1000}}
    BetweenFilter,  # Range: {"Data": {"between": ["2015-01-01", "2015-12-31"]}}
]


class MetricSpec(BaseModel):
    """Specification for a metric in the optimized JSON output."""

    name: str = Field(..., min_length=1, description="Column name of the metric")

    aggregation: Literal[
        "sum", "avg", "count", "min", "max", "median", "std", "var"
    ] = Field(default="sum", description="Aggregation function applied to the metric")

    alias: Optional[str] = Field(
        default=None, description="Friendly name for the metric"
    )

    unit: Optional[str] = Field(
        default=None, description="Measurement unit associated with the metric"
    )

    @model_validator(mode="after")
    def populate_defaults(self) -> "MetricSpec":
        """Fill optional fields with sensible defaults when not provided."""

        if not self.alias:
            self.alias = _prettify_label(self.name)

        if self.unit is None:
            self.unit = _infer_metric_unit(self.name)

        return self


class DimensionSpec(BaseModel):
    """Specification for a dimension/categorical grouping."""

    name: str = Field(..., min_length=1, description="Column name of the dimension")

    alias: Optional[str] = Field(
        default=None, description="Friendly name for the dimension"
    )

    temporal_granularity: Optional[Literal["day", "month", "quarter", "year"]] = Field(
        default=None, description="Temporal granularity for time-based dimensions"
    )

    @model_validator(mode="after")
    def populate_alias_and_temporal(self) -> "DimensionSpec":
        """Ensure a human-friendly alias is always available and infer temporal granularity."""

        if not self.alias:
            self.alias = _prettify_label(self.name)

        # Auto-detect temporal granularity based on common column names
        if self.temporal_granularity is None:
            name_lower = self.name.lower()
            if name_lower in ["dia", "day", "data", "date"]:
                self.temporal_granularity = "day"
            elif name_lower in ["mes", "month", "mês"]:
                self.temporal_granularity = "month"
            elif name_lower in ["trimestre", "quarter"]:
                self.temporal_granularity = "quarter"
            elif name_lower in ["ano", "year"]:
                self.temporal_granularity = "year"

        return self


class SortSpec(BaseModel):
    """Sorting configuration for ordered outputs."""

    by: Optional[str] = Field(default=None, description="Field used for sorting")

    order: Optional[Literal["asc", "desc"]] = Field(
        default=None, description="Sorting order"
    )

    @field_validator("order")
    @classmethod
    def validate_order(cls, v: Optional[str]) -> Optional[str]:
        """Validate that order is either 'asc' or 'desc'."""
        if v is not None and v not in ["asc", "desc"]:
            raise ValueError("Order must be 'asc' or 'desc'")
        return v


class VisualSpec(BaseModel):
    """Visual configuration for the chart."""

    palette: Optional[str] = Field(
        default=None, description="Color palette for the visualization"
    )

    show_values: bool = Field(
        default=False, description="Whether values should be displayed on the chart"
    )

    orientation: Optional[Literal["horizontal", "vertical"]] = Field(
        default=None, description="Chart orientation when applicable"
    )

    stacked: Optional[bool] = Field(
        default=None, description="Whether the chart is stacked"
    )

    secondary_chart_type: Optional[ChartTypeLiteral] = Field(
        default=None, description="Secondary chart type for composed visualizations"
    )

    bins: Optional[int] = Field(
        default=None, ge=2, description="Number of bins for histograms"
    )


class OutputSpec(BaseModel):
    """Output specification that downstream agents can use."""

    type: str = Field(
        default="chart",
        description="Type of output expected (e.g., chart, chart_and_summary)",
    )

    summary_template: Optional[str] = Field(
        default=None, description="Template used to generate textual summaries"
    )


class ChartOutput(BaseModel):
    """
    Schema for the optimized JSON output returned by the agent.

    The schema includes all information required for downstream agents to
    render charts and generate textual explanations without reprocessing the
    natural language query.
    """

    intent: Optional[str] = Field(
        default=None, description="Identified intent for the query"
    )

    chart_type: Optional[ChartTypeLiteral] = Field(
        default=None,
        description="Type of chart to render. None if no chart is required.",
    )

    title: Optional[str] = Field(default=None, description="Title of the visualization")

    description: Optional[str] = Field(
        default=None, description="Description of the visualization"
    )

    metrics: List[MetricSpec] = Field(
        default_factory=list, description="List of metric specifications"
    )

    dimensions: List[DimensionSpec] = Field(
        default_factory=list, description="List of dimension specifications"
    )

    filters: Dict[str, FilterValue] = Field(
        default_factory=dict,
        description="Explicitly extracted filters for data selection. Supports multiple formats: simple equality, IN clause (list), operator-based comparison, and BETWEEN ranges.",
    )

    top_n: Optional[int] = Field(
        default=None,
        ge=1,
        description="Number of records to display for ranking queries",
    )

    group_top_n: Optional[int] = Field(
        default=None,
        ge=1,
        description="Number of top groups for nested ranking queries (e.g., 'top 3 clients from top 5 states' → group_top_n=5, top_n=3)",
    )

    sort: Optional[SortSpec] = Field(
        default=None, description="Sorting configuration for the output"
    )

    visual: VisualSpec = Field(
        default_factory=VisualSpec, description="Visual configuration for the chart"
    )

    data_source: Optional[str] = Field(
        default=None, description="Dataset or table used to generate the visualization"
    )

    output: OutputSpec = Field(
        default_factory=OutputSpec,
        description="Output configuration for downstream agents",
    )

    message: Optional[str] = Field(
        default=None,
        description="Optional message for cases where no chart is required",
    )

    requires_tabular_data: bool = Field(
        default=False,
        description="Indica se usuário solicitou dados tabulares explicitamente",
    )

    @model_validator(mode="after")
    def validate_chart_requirements(self) -> "ChartOutput":
        """
        Validate cross-field requirements based on chart_type.

        This validator ensures that dimensions, metrics, and other fields
        conform to the requirements of each specific chart type.
        """

        # Skip validation if chart_type is None (no visualization required)
        if self.chart_type is None:
            return self

        chart_type = self.chart_type
        num_dimensions = len(self.dimensions)
        num_metrics = len(self.metrics)

        # Define temporal dimension names for validation
        temporal_dimension_names = [
            "mes",
            "mês",
            "ano",
            "year",
            "month",
            "data",
            "date",
            "dia",
            "day",
            "trimestre",
            "quarter",
        ]

        # Check if any dimension is temporal (by name or explicit flag)
        has_temporal_dimension = any(
            d.temporal_granularity is not None
            or d.name.lower() in temporal_dimension_names
            for d in self.dimensions
        )

        # Validate metrics requirement (all charts except null need at least 1 metric)
        if num_metrics < 1 and chart_type != "histogram":
            raise ValueError(
                f"Chart type '{chart_type}' requires at least 1 metric, but {num_metrics} were provided"
            )

        # Chart-specific validations
        if chart_type == "bar_horizontal":
            if num_dimensions != 1:
                raise ValueError(
                    f"bar_horizontal requires exactly 1 dimension, but {num_dimensions} were provided"
                )

        elif chart_type == "bar_vertical":
            if num_dimensions != 1:
                raise ValueError(
                    f"bar_vertical requires exactly 1 dimension, but {num_dimensions} were provided"
                )

        elif chart_type == "bar_vertical_composed":
            if num_dimensions != 2:
                raise ValueError(
                    f"bar_vertical_composed requires exactly 2 dimensions, but {num_dimensions} were provided"
                )

        elif chart_type == "bar_vertical_stacked":
            if num_dimensions != 2:
                raise ValueError(
                    f"bar_vertical_stacked requires exactly 2 dimensions, but {num_dimensions} were provided"
                )
            if not self.visual.stacked:
                # Auto-correct stacked flag
                self.visual.stacked = True

        elif chart_type == "line_composed":
            # FASE 2: line_composed now accepts 1+ dimensions (semantic type)
            # Visual variant (single_line vs multi_line) decided by RenderSelector
            if num_dimensions < 1:
                raise ValueError(
                    f"line_composed requires at least 1 dimension, but {num_dimensions} were provided"
                )
            # Check that at least the first dimension is temporal
            first_dim = self.dimensions[0] if self.dimensions else None
            if first_dim:
                is_first_temporal = (
                    first_dim.temporal_granularity is not None
                    or first_dim.name.lower() in temporal_dimension_names
                )
                if not is_first_temporal:
                    raise ValueError(
                        f"line_composed requires the first dimension to be temporal, but got: {first_dim.name}"
                    )
            else:
                raise ValueError(
                    f"line_composed requires at least 1 temporal dimension, but none were found"
                )

        elif chart_type == "pie":
            if num_dimensions != 1:
                raise ValueError(
                    f"pie requires exactly 1 dimension, but {num_dimensions} were provided"
                )

        elif chart_type == "histogram":
            if num_dimensions != 0:
                raise ValueError(
                    f"histogram must have 0 dimensions, but {num_dimensions} were provided"
                )
            # Histogram typically uses count aggregation
            if num_metrics > 0 and self.metrics[0].aggregation not in ["count", "sum"]:
                # This is just a warning, not an error - we'll log it if needed
                pass

        return self

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "intent": "top_clients_by_city",
                    "chart_type": "bar_horizontal",
                    "title": "Top 3 clientes de Joinville",
                    "description": "Ranking dos 3 principais clientes de Joinville pelo valor total de vendas.",
                    "metrics": [
                        {
                            "name": "valor_vendas",
                            "aggregation": "sum",
                            "alias": "total_vendas",
                            "unit": "R$",
                        }
                    ],
                    "dimensions": [{"name": "cliente_nome", "alias": "Cliente"}],
                    "filters": {"cidade": "Joinville"},
                    "top_n": 3,
                    "sort": {"by": "total_vendas", "order": "desc"},
                    "visual": {
                        "palette": "Blues",
                        "show_values": True,
                        "orientation": "horizontal",
                        "stacked": False,
                        "secondary_chart_type": None,
                        "bins": None,
                    },
                    "data_source": "clientes_vendas",
                    "output": {
                        "type": "chart_and_summary",
                        "summary_template": "Os {top_n} principais clientes de {cidade} são apresentados no gráfico, ordenados por {metric_alias}.",
                    },
                }
            ]
        }
    )


# ============================================================================
# INPUT SCHEMA (Pydantic)
# ============================================================================


class QueryInput(BaseModel):
    """Schema for input queries."""

    query: str = Field(
        ..., description="Natural language query for chart classification", min_length=1
    )

    context: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional context information (e.g., user preferences, dataset info)",
    )

    @field_validator("query")
    @classmethod
    def validate_query_not_whitespace_only(cls, v: str) -> str:
        """Ensure query is not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError("Query cannot be empty or whitespace-only")
        return v

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {"query": "top 5 produtos mais vendidos", "context": None},
                {
                    "query": "evolução das vendas por mês em 2016",
                    "context": {"year": 2025},
                },
            ]
        }
    )


# ============================================================================
# GRAPH STATE (TypedDict)
# ============================================================================


class GraphState(TypedDict, total=False):
    """
    State passed between workflow nodes in the LangGraph.

    This TypedDict defines all the fields that can be present in the state
    as it flows through the graph nodes.
    """

    # Input
    query: str
    """Original user query"""

    # FASE 1: Semantic-First Architecture
    semantic_anchor: Optional[Dict[str, Any]]
    """Semantic anchor extracted by LLM (FIRST LAYER - pure intent)"""

    semantic_validation: Optional[Dict[str, Any]]
    """Semantic validation result (consistency check)"""

    semantic_mapping: Optional[Dict[str, Any]]
    """Semantic mapping result (anchor -> chart family)"""

    # Parsing results
    parsed_entities: Dict[str, Any]
    """Extracted entities from query (numbers, dates, categories)"""

    detected_keywords: List[str]
    """Keywords detected that indicate chart type or operations"""

    # Classification results
    intent: str
    """Classified user intent (e.g., 'ranking', 'temporal_trend', 'comparison')"""

    chart_type: Optional[str]
    """Selected chart type based on intent"""

    confidence: float
    """Confidence score for the classification (0.0 to 1.0)"""

    intent_config: Optional[Dict[str, Any]]
    """FASE 2: Configuration associated with the detected intent (from IntentClassifier)"""

    level_used: Optional[int]
    """FASE 2/3: Level of classification used (0=Intent, 1=Detection, 2=Context, 3=LLM)"""

    query_context: Optional[Dict[str, Any]]
    """FASE 2: Context extracted from query (temporal, comparison, dimensions, etc.)"""

    agent_tokens: Optional[Dict[str, Dict[str, int]]]
    """Token usage by agent (for tracking LLM usage)"""

    # Column mapping
    columns_mentioned: List[str]
    """Raw column references extracted from query"""

    mapped_columns: Dict[str, str]
    """Mapping from query terms to actual column names"""

    # Dataset validation
    data_source: Optional[str]
    """Path to the data source file"""

    available_columns: Optional[List[str]]
    """List of columns actually available in the dataset"""

    # Output
    output: Dict[str, Any]
    """Final JSON output (will be validated against ChartOutput schema)"""

    # Error handling
    errors: List[str]
    """List of error messages encountered during processing"""

    # Phase 2 - Analytics Executor fields
    executor_input: Optional[Dict[str, Any]]
    """Input specification for executor agent (from Phase 1 output)"""

    executor_output: Optional[Dict[str, Any]]
    """Output from executor agent (ready for Plotly)"""

    execution_time: Optional[float]
    """Execution time for analytics operations (in seconds)"""

    engine_used: Optional[str]
    """Execution engine used (DuckDB or Pandas)"""

    # Phase 0 - Filter Classifier fields (Phase 5 integration)
    filter_history: List[Dict[str, Any]]
    """Historical record of filters applied in previous queries"""

    current_filters: Dict[str, Any]
    """Currently active filters from previous session"""

    filter_operations: Dict[str, Any]
    """CRUD operations identified in current query (ADICIONAR, ALTERAR, REMOVER, MANTER)"""

    filter_final: Dict[str, Any]
    """Final consolidated filters after applying all operations"""

    detected_filter_columns: List[str]
    """Column names detected as filter targets in the query"""

    filter_confidence: float
    """Confidence score for filter detection (0.0 to 1.0)"""

    requires_tabular_data: bool
    """Flag indicando solicitação de dados tabulares"""

    non_graph_output: Optional[Dict[str, Any]]
    """Output do non_graph_executor (para queries não-gráficas)"""

    # Phase 6 - Fallback System fields
    fallback_result: Optional[Dict[str, Any]]
    """Result from fallback manager (degradation or routing decision)"""

    fallback_triggered: bool
    """Flag indicating if fallback degradation occurred"""

    fallback_message: Optional[str]
    """User-facing message when fallback is triggered"""

    redirect_to: Optional[str]
    """Target agent for routing (e.g., 'non_graph_executor')"""

    redirect_payload: Optional[Dict[str, Any]]
    """Complete payload for routing to another agent"""


# ============================================================================
# HELPER SCHEMAS
# ============================================================================


class ParsedEntity(BaseModel):
    """Schema for a parsed entity from the query."""

    type: Literal["number", "date", "category", "operator", "column_reference"]
    value: Any
    position: Optional[int] = None

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {"type": "number", "value": 5, "position": 4},
                {"type": "operator", "value": "top", "position": 0},
            ]
        }
    )


class KeywordMatch(BaseModel):
    """Schema for a matched keyword."""

    keyword: str
    category: str
    chart_type_hint: Optional[str] = None
    confidence: float = Field(ge=0.0, le=1.0)

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "keyword": "ranking",
                    "category": "chart_indicator",
                    "chart_type_hint": "bar_horizontal",
                    "confidence": 0.95,
                }
            ]
        }
    )


class ColumnMapping(BaseModel):
    """Schema for a column mapping result."""

    query_term: str
    mapped_column: Optional[str]
    confidence: float = Field(ge=0.0, le=1.0)
    method: Literal["exact", "fuzzy", "semantic"]

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "query_term": "revenue",
                    "mapped_column": "TotalRevenue",
                    "confidence": 1.0,
                    "method": "exact",
                }
            ]
        }
    )


# ============================================================================
# PHASE 2 - ANALYTICS EXECUTOR SCHEMAS
# ============================================================================

# Aggregation types supported by the analytics executor
AggregationType = Literal["sum", "avg", "count", "min", "max", "median", "std", "var"]


class AnalyticsInputSpec(BaseModel):
    """
    Complete input specification for analytics executor agent.

    This schema is compatible with ChartOutput from Phase 1, allowing seamless
    integration between the classifier and executor agents.
    """

    intent: Optional[str] = Field(
        default=None, description="Identified intent for the query"
    )

    chart_type: str = Field(..., description="Type of chart to generate")

    metrics: List[MetricSpec] = Field(
        ..., min_length=1, description="List of metrics to aggregate"
    )

    dimensions: List[DimensionSpec] = Field(
        ..., description="List of dimensions for grouping"
    )

    filters: Dict[str, Any] = Field(
        default_factory=dict, description="Filter conditions to apply"
    )

    sort: Optional[SortSpec] = Field(default=None, description="Sort specification")

    top_n: Optional[int] = Field(
        default=None, ge=1, description="Limit to top N results"
    )

    data_source: str = Field(..., description="Data source identifier")

    visual_config: Dict[str, Any] = Field(
        default_factory=dict, description="Visual configuration parameters"
    )

    @classmethod
    def from_chart_output(cls, chart_output: ChartOutput) -> "AnalyticsInputSpec":
        """
        Create AnalyticsInputSpec from ChartOutput (Phase 1 output).

        This method enables seamless conversion from Phase 1 output format
        to Phase 2 input format.

        Args:
            chart_output: ChartOutput instance from Phase 1

        Returns:
            AnalyticsInputSpec instance ready for executor
        """
        if not chart_output.chart_type:
            raise ValueError(
                "ChartOutput must have a chart_type to convert to AnalyticsInputSpec"
            )

        if not chart_output.metrics:
            raise ValueError("ChartOutput must have at least one metric")

        if not chart_output.data_source:
            raise ValueError("ChartOutput must have a data_source")

        # Extract visual config from VisualSpec
        visual_config = {}
        if chart_output.visual:
            visual_config = {
                "palette": chart_output.visual.palette,
                "show_values": chart_output.visual.show_values,
                "orientation": chart_output.visual.orientation,
                "stacked": chart_output.visual.stacked,
                "secondary_chart_type": chart_output.visual.secondary_chart_type,
                "bins": chart_output.visual.bins,
            }
            # Remove None values
            visual_config = {k: v for k, v in visual_config.items() if v is not None}

        return cls(
            intent=chart_output.intent,
            chart_type=chart_output.chart_type,
            metrics=chart_output.metrics,
            dimensions=chart_output.dimensions,
            filters=chart_output.filters,
            sort=chart_output.sort,
            top_n=chart_output.top_n,
            data_source=chart_output.data_source,
            visual_config=visual_config,
        )

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "intent": "entity_ranking",
                    "chart_type": "bar_horizontal",
                    "metrics": [
                        {
                            "name": "MetricColumn",
                            "aggregation": "sum",
                            "alias": "Total Metric",
                        }
                    ],
                    "dimensions": [{"name": "CategoryColumn", "alias": "Category"}],
                    "filters": {},
                    "sort": {"by": "MetricColumn", "order": "desc"},
                    "top_n": 5,
                    "data_source": "dataset",
                    "visual_config": {
                        "palette": "Blues",
                        "show_values": True,
                        "orientation": "horizontal",
                    },
                }
            ]
        }
    )


class ExecutionMetadata(BaseModel):
    """Execution metadata for analytics operations."""

    engine: str = Field(..., description="Engine used: DuckDB or Pandas")

    execution_time: float = Field(..., ge=0.0, description="Execution time in seconds")

    timestamp: str = Field(..., description="Execution timestamp in ISO format")

    row_count: int = Field(..., ge=0, description="Number of rows returned")

    filters_applied: Dict[str, Any] = Field(
        default_factory=dict, description="Filters that were applied during execution"
    )

    @model_validator(mode="after")
    def populate_timestamp(self) -> "ExecutionMetadata":
        """Populate timestamp if not provided."""
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()
        return self


class PlotlyConfig(BaseModel):
    """Plotly-specific configuration for chart generation."""

    x: Optional[str] = Field(default=None, description="X-axis column name")

    y: Optional[str] = Field(default=None, description="Y-axis column name")

    color: Optional[str] = Field(default=None, description="Color grouping column name")

    orientation: Optional[Literal["h", "v"]] = Field(
        default=None,
        description="Chart orientation: 'h' for horizontal, 'v' for vertical",
    )

    title: Optional[str] = Field(default=None, description="Chart title")

    markers: Optional[bool] = Field(
        default=None, description="Whether to show markers on line charts"
    )


class AnalyticsOutput(BaseModel):
    """
    Complete output from analytics executor agent.

    This schema defines the structured output ready for Plotly visualization.
    """

    status: Literal["success", "error"] = Field(..., description="Execution status")

    data: List[Dict[str, Any]] = Field(
        ..., description="Processed data as list of dictionaries"
    )

    metadata: Dict[str, Any] = Field(
        ..., description="Execution metadata including chart type, dimensions, metrics"
    )

    execution: ExecutionMetadata = Field(
        ..., description="Execution details and performance metrics"
    )

    plotly_config: PlotlyConfig = Field(
        ..., description="Plotly configuration for chart generation"
    )

    error: Optional[Dict[str, str]] = Field(
        default=None, description="Error information if status is 'error'"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "status": "success",
                    "data": [
                        {"Category": "Group A", "Total Metric": 350000.50},
                        {"Category": "Group B", "Total Metric": 310000.75},
                    ],
                    "metadata": {
                        "chart_type": "bar_horizontal",
                        "dimensions": ["Category"],
                        "metrics": ["Total Metric"],
                        "row_count": 2,
                        "palette": "Blues",
                        "show_values": True,
                        "orientation": "horizontal",
                        "filters_applied": {},
                    },
                    "execution": {
                        "engine": "DuckDB",
                        "execution_time": 1.83,
                        "timestamp": "2025-11-02T10:30:45.123Z",
                        "row_count": 2,
                        "filters_applied": {},
                    },
                    "plotly_config": {
                        "x": "Total Metric",
                        "y": "Category",
                        "color": None,
                        "orientation": "h",
                        "title": "Top Categories by Total Metric",
                        "markers": None,
                    },
                    "error": None,
                }
            ]
        }
    )


# ============================================================================
# EXECUTION RESULT (Dataclass for internal use)
# ============================================================================


@dataclass
class ExecutionResult:
    """
    Internal dataclass for query execution results.

    This is used within the execution engine to pass results between
    components before final formatting.
    """

    data: Any  # pd.DataFrame - using Any to avoid import issues at module level
    """Processed DataFrame result"""

    engine: str
    """Engine used: 'DuckDB' or 'Pandas'"""

    execution_time: float
    """Execution time in seconds"""

    error: Optional[str] = None
    """Error message if execution had issues (but still succeeded with fallback)"""

    def __post_init__(self):
        """Validate fields after initialization."""
        if self.engine not in ["DuckDB", "Pandas"]:
            raise ValueError(
                f"Engine must be 'DuckDB' or 'Pandas', got '{self.engine}'"
            )
        if self.execution_time < 0:
            raise ValueError(
                f"Execution time must be non-negative, got {self.execution_time}"
            )
