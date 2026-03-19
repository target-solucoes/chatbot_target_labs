"""
Session Statistics Module

Tracks and displays statistics for an interactive pipeline session.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List
from collections import defaultdict, Counter
from rich.table import Table
from rich import box


@dataclass
class SessionStatistics:
    """
    Tracks statistics for an interactive pipeline session.

    Monitors query execution, timing, success rates, and operation counts
    across all three pipeline agents.
    """

    session_start: datetime = field(default_factory=datetime.now)

    # Query counters
    total_queries: int = 0
    successful_queries: int = 0
    failed_queries: int = 0

    # Timing aggregates
    total_time: float = 0.0
    filter_time: float = 0.0
    classifier_time: float = 0.0
    executor_time: float = 0.0

    # Chart type distribution
    chart_types: Counter = field(default_factory=Counter)

    # Filter operations (CRUD)
    filter_operations: Dict[str, int] = field(
        default_factory=lambda: {
            "ADICIONAR": 0,
            "ALTERAR": 0,
            "REMOVER": 0,
            "MANTER": 0,
        }
    )

    # Engine usage
    engines_used: Counter = field(default_factory=Counter)

    # Active filters count
    active_filters_count: int = 0

    def record_query(
        self,
        success: bool,
        total_time: float,
        filter_time: float = 0.0,
        classifier_time: float = 0.0,
        executor_time: float = 0.0,
        plotly_time: float = 0.0,
        chart_type: str = None,
        engine: str = None,
        filter_ops: Dict[str, int] = None,
    ):
        """
        Record a query execution.

        Args:
            success: Whether query succeeded
            total_time: Total execution time in seconds
            filter_time: Filter phase time
            classifier_time: Classifier phase time
            executor_time: Executor phase time
            plotly_time: Plotly generation phase time
            chart_type: Chart type generated
            engine: Engine used (DuckDB, Pandas)
            filter_ops: Filter operations performed
        """
        self.total_queries += 1

        if success:
            self.successful_queries += 1
        else:
            self.failed_queries += 1

        # Timing
        self.total_time += total_time
        self.filter_time += filter_time
        self.classifier_time += classifier_time
        self.executor_time += executor_time

        # Chart types
        if chart_type:
            self.chart_types[chart_type] += 1

        # Engines
        if engine:
            self.engines_used[engine] += 1

        # Filter operations
        if filter_ops:
            for op, count in filter_ops.items():
                if op in self.filter_operations:
                    self.filter_operations[op] += count

    def update_active_filters(self, count: int):
        """Update count of active filters."""
        self.active_filters_count = count

    @property
    def session_duration(self) -> timedelta:
        """Get session duration."""
        return datetime.now() - self.session_start

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_queries == 0:
            return 0.0
        return (self.successful_queries / self.total_queries) * 100

    @property
    def average_time(self) -> float:
        """Calculate average query time in milliseconds."""
        if self.total_queries == 0:
            return 0.0
        return (self.total_time / self.total_queries) * 1000

    @property
    def queries_per_minute(self) -> float:
        """Calculate queries per minute."""
        duration_minutes = self.session_duration.total_seconds() / 60
        if duration_minutes == 0:
            return 0.0
        return self.total_queries / duration_minutes

    def get_phase_breakdown(self) -> Dict[str, Dict[str, float]]:
        """
        Get timing breakdown by phase.

        Returns:
            Dict with absolute times and percentages
        """
        if self.total_time == 0:
            return {}

        return {
            "filter": {
                "time_ms": self.filter_time * 1000,
                "percentage": (self.filter_time / self.total_time) * 100,
            },
            "classifier": {
                "time_ms": self.classifier_time * 1000,
                "percentage": (self.classifier_time / self.total_time) * 100,
            },
            "executor": {
                "time_ms": self.executor_time * 1000,
                "percentage": (self.executor_time / self.total_time) * 100,
            },
        }

    def to_table(self) -> Table:
        """
        Create a Rich Table for display.

        Returns:
            Rich Table with session statistics
        """
        table = Table(
            title=f"Session Statistics (Duration: {self._format_duration(self.session_duration)})",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold cyan",
        )

        table.add_column("Metric", style="white", width=30)
        table.add_column("Value", justify="right", style="yellow")

        # Query stats
        table.add_row("Total Queries", str(self.total_queries))
        table.add_row(
            "Successful",
            f"{self.successful_queries} ({self.success_rate:.1f}%)",
            style="green",
        )
        table.add_row(
            "Failed",
            str(self.failed_queries),
            style="red" if self.failed_queries > 0 else "dim",
        )
        table.add_row("", "")  # Spacer

        # Timing stats
        table.add_row("Average Time", f"{self.average_time:.0f}ms")
        table.add_row("Total Time", f"{self.total_time:.2f}s")
        table.add_row("Queries/minute", f"{self.queries_per_minute:.1f}")
        table.add_row("", "")  # Spacer

        # Phase breakdown
        if self.total_queries > 0:
            breakdown = self.get_phase_breakdown()
            if breakdown:
                table.add_row("[bold]Phase Breakdown:[/bold]", "")
                for phase, stats in breakdown.items():
                    table.add_row(
                        f"  {phase.capitalize()}",
                        f"{stats['time_ms']:.0f}ms ({stats['percentage']:.1f}%)",
                    )
                table.add_row("", "")  # Spacer

        # Chart types
        if self.chart_types:
            table.add_row("[bold]Chart Types:[/bold]", "")
            for chart_type, count in self.chart_types.most_common(5):
                table.add_row(f"  {chart_type}", str(count))
            table.add_row("", "")  # Spacer

        # Filter operations
        if any(count > 0 for count in self.filter_operations.values()):
            table.add_row("[bold]Filter Operations:[/bold]", "")
            for op, count in self.filter_operations.items():
                if count > 0:
                    table.add_row(f"  {op}", str(count))
            table.add_row("", "")  # Spacer

        # Active filters
        table.add_row("Active Filters", str(self.active_filters_count))

        return table

    def _format_duration(self, duration: timedelta) -> str:
        """Format duration as human-readable string."""
        total_seconds = int(duration.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60

        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        else:
            return f"{minutes:02d}:{seconds:02d}"

    def reset(self):
        """Reset all statistics."""
        self.session_start = datetime.now()
        self.total_queries = 0
        self.successful_queries = 0
        self.failed_queries = 0
        self.total_time = 0.0
        self.filter_time = 0.0
        self.classifier_time = 0.0
        self.executor_time = 0.0
        self.chart_types.clear()
        self.filter_operations = {
            "ADICIONAR": 0,
            "ALTERAR": 0,
            "REMOVER": 0,
            "MANTER": 0,
        }
        self.engines_used.clear()
        self.active_filters_count = 0

    def to_dict(self) -> Dict:
        """Convert statistics to dictionary."""
        return {
            "session_start": self.session_start.isoformat(),
            "session_duration": str(self.session_duration),
            "total_queries": self.total_queries,
            "successful_queries": self.successful_queries,
            "failed_queries": self.failed_queries,
            "success_rate": self.success_rate,
            "average_time_ms": self.average_time,
            "total_time": self.total_time,
            "queries_per_minute": self.queries_per_minute,
            "phase_breakdown": self.get_phase_breakdown(),
            "chart_types": dict(self.chart_types),
            "filter_operations": self.filter_operations,
            "engines_used": dict(self.engines_used),
            "active_filters_count": self.active_filters_count,
        }
