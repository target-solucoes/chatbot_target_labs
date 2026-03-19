"""
Pipeline Result Module

Encapsulates results from pipeline execution with Rich display support.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
from datetime import datetime
from rich.panel import Panel
from rich.table import Table
from rich.syntax import Syntax
from rich import box
import json
import pandas as pd
import numpy as np


def json_serializer(obj):
    """
    Custom JSON serializer for objects not serializable by default json code.

    Handles:
    - pandas Timestamp
    - numpy datetime64
    - datetime objects
    - numpy int/float types
    - other numpy types
    """
    if isinstance(obj, (pd.Timestamp, np.datetime64)):
        return obj.isoformat() if pd.notna(obj) else None
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, (np.integer, np.floating)):
        return obj.item()
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif pd.isna(obj):
        return None
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


@dataclass
class PipelineResult:
    """
    Enhanced result container for pipeline execution.

    Encapsulates results from all three agents (filter, classifier, executor)
    with Rich display support for interactive visualization.
    """

    # Query and status
    query: str
    status: str  # 'success', 'partial', 'error'
    timestamp: datetime = field(default_factory=datetime.now)

    # Phase results
    filter_result: Optional[Dict[str, Any]] = None
    classifier_result: Optional[Dict[str, Any]] = None
    executor_result: Optional[Dict[str, Any]] = None
    plotly_result: Optional[Dict[str, Any]] = None

    # Timing breakdown
    filter_time: float = 0.0
    classifier_time: float = 0.0
    executor_time: float = 0.0
    plotly_time: float = 0.0
    total_time: float = 0.0

    # Extracted metadata
    active_filters: Dict[str, Any] = field(default_factory=dict)
    chart_type: Optional[str] = None
    intent: Optional[str] = None
    confidence: float = 0.0
    data: List[Dict[str, Any]] = field(default_factory=list)
    row_count: int = 0
    engine_used: Optional[str] = None
    sql_query: Optional[str] = None

    # Error tracking
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "query": self.query,
            "status": self.status,
            "timestamp": self.timestamp.isoformat(),
            "filter_result": self.filter_result,
            "classifier_result": self.classifier_result,
            "executor_result": self.executor_result,
            "plotly_result": self.plotly_result,
            "timing": {
                "filter": self.filter_time,
                "classifier": self.classifier_time,
                "executor": self.executor_time,
                "plotly": self.plotly_time,
                "total": self.total_time,
            },
            "metadata": {
                "active_filters": self.active_filters,
                "chart_type": self.chart_type,
                "intent": self.intent,
                "confidence": self.confidence,
                "row_count": self.row_count,
                "engine_used": self.engine_used,
            },
            "data": self.data,
            "sql_query": self.sql_query,
            "errors": self.errors,
        }

    def to_panel(self, show_data_sample: bool = True, max_rows: int = 5) -> Panel:
        """
        Create a Rich Panel for display.

        Args:
            show_data_sample: Whether to show data sample
            max_rows: Maximum rows to show in data sample

        Returns:
            Rich Panel object
        """
        # Status icon
        status_icons = {"success": "✓", "partial": "⚠", "error": "✗"}
        icon = status_icons.get(self.status, "?")

        # Build content
        content_parts = []

        # Header
        content_parts.append(
            f"[bold]{icon} {self.status.upper()}[/bold] | "
            f"{self.total_time:.0f}ms | {self.engine_used or 'N/A'}"
        )
        content_parts.append(f"\n[dim]Query:[/dim] {self.query}\n")

        # Phase breakdown
        if self.filter_time > 0:
            filter_pct = (
                (self.filter_time / self.total_time * 100) if self.total_time > 0 else 0
            )
            content_parts.append(
                f"[cyan]PHASE 0: Filter Classifier[/cyan] ({self.filter_time:.0f}ms - {filter_pct:.1f}%)\n"
                f"  Active Filters: {len(self.active_filters)}\n"
            )

        if self.classifier_time > 0:
            classifier_pct = (
                (self.classifier_time / self.total_time * 100)
                if self.total_time > 0
                else 0
            )
            content_parts.append(
                f"[yellow]PHASE 1: Graphic Classifier[/yellow] ({self.classifier_time:.0f}ms - {classifier_pct:.1f}%)\n"
                f"  Chart: {self.chart_type or 'N/A'}\n"
                f"  Intent: {self.intent or 'N/A'}\n"
                f"  Confidence: {self.confidence:.2f}\n"
            )

        if self.executor_time > 0:
            executor_pct = (
                (self.executor_time / self.total_time * 100)
                if self.total_time > 0
                else 0
            )
            content_parts.append(
                f"[green]PHASE 2: Analytics Executor[/green] ({self.executor_time:.0f}ms - {executor_pct:.1f}%)\n"
                f"  Rows: {self.row_count}\n"
                f"  Engine: {self.engine_used or 'N/A'}\n"
            )

        # Errors
        if self.errors:
            content_parts.append(f"\n[red bold]Errors:[/red bold]")
            for error in self.errors:
                content_parts.append(f"  [red]• {error}[/red]")

        # Data sample
        if show_data_sample and self.data and self.row_count > 0:
            content_parts.append(
                f"\n[bold]Data Sample[/bold] (showing {min(max_rows, len(self.data))} of {self.row_count} rows)"
            )

        content = "\n".join(content_parts)

        return Panel(
            content,
            title=f"Pipeline Result #{id(self) % 10000}",
            border_style="green" if self.status == "success" else "red",
            box=box.ROUNDED,
        )

    def to_table(self, max_rows: int = 10) -> Optional[Table]:
        """
        Create a Rich Table for data display.

        Args:
            max_rows: Maximum rows to display

        Returns:
            Rich Table object or None if no data
        """
        if not self.data:
            return None

        table = Table(
            title=f"Results ({self.row_count} rows)",
            box=box.SIMPLE,
            show_header=True,
            header_style="bold cyan",
        )

        # Add columns from first row
        if self.data:
            for col in self.data[0].keys():
                table.add_column(col, style="white")

            # Add rows (limited)
            for row in self.data[:max_rows]:
                table.add_row(*[str(v) for v in row.values()])

        return table

    def get_sql_syntax(self) -> Optional[Syntax]:
        """
        Get SQL query as Rich Syntax object.

        Returns:
            Rich Syntax object or None if no SQL
        """
        if not self.sql_query:
            return None

        return Syntax(
            self.sql_query, "sql", theme="monokai", line_numbers=True, word_wrap=True
        )

    def get_json_display(self, part: str = "all") -> str:
        """
        Get JSON representation for display.

        Args:
            part: Which part to display ('all', 'filter', 'classifier', 'executor')

        Returns:
            Formatted JSON string
        """
        if part == "filter" and self.filter_result:
            return json.dumps(
                self.filter_result,
                indent=2,
                ensure_ascii=False,
                default=json_serializer,
            )
        elif part == "classifier" and self.classifier_result:
            return json.dumps(
                self.classifier_result,
                indent=2,
                ensure_ascii=False,
                default=json_serializer,
            )
        elif part == "executor" and self.executor_result:
            return json.dumps(
                self.executor_result,
                indent=2,
                ensure_ascii=False,
                default=json_serializer,
            )
        else:
            return json.dumps(
                self.to_dict(), indent=2, ensure_ascii=False, default=json_serializer
            )

    @property
    def has_data(self) -> bool:
        """Check if result has data."""
        return len(self.data) > 0

    @property
    def is_success(self) -> bool:
        """Check if execution was successful."""
        return self.status == "success"

    @property
    def is_error(self) -> bool:
        """Check if execution had errors."""
        return self.status == "error"
