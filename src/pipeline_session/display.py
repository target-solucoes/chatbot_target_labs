"""
Display Module

Rich display helpers for interactive pipeline session.
"""

from typing import Dict, List, Any, Optional
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.syntax import Syntax
from rich.json import JSON
from rich import box
from rich.text import Text
import json
import pandas as pd
import numpy as np
from datetime import datetime, date


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
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, (np.integer, np.floating)):
        return obj.item()
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif pd.isna(obj):
        return None
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class DisplayHelper:
    """
    Helper class for Rich display formatting.

    Provides consistent, beautiful formatting for all pipeline outputs.
    """

    def __init__(self, console: Optional[Console] = None):
        """
        Initialize display helper.

        Args:
            console: Rich Console instance (creates new if None)
        """
        self.console = console or Console()

    def show_welcome(self):
        """Display welcome banner."""
        welcome_text = """
[bold cyan]╔═══════════════════════════════════════════════════════════╗[/bold cyan]
[bold cyan]║[/bold cyan]     [bold white]Interactive Multi-Agent Pipeline Session[/bold white]           [bold cyan]║[/bold cyan]
[bold cyan]╚═══════════════════════════════════════════════════════════╝[/bold cyan]

[yellow]Four Integrated Agents:[/yellow]
  • [cyan]Filter Classifier[/cyan] - Conversational filter management
  • [yellow]Graphic Classifier[/yellow] - Query intent classification
  • [green]Analytics Executor[/green] - SQL execution & data processing
  • [magenta]Plotly Generator[/magenta] - Interactive chart visualization

[dim]Type your query or use /help for commands[/dim]
"""
        self.console.print(Panel(welcome_text, border_style="cyan", box=box.DOUBLE))

    def show_result(
        self,
        result: "PipelineResult",
        show_data: bool = True,
        max_rows: int = 10,
        show_full_agent_output: bool = True,
    ):
        """
        Display pipeline result with Rich formatting.

        Args:
            result: PipelineResult to display
            show_data: Whether to show data table
            max_rows: Maximum rows to display
            show_full_agent_output: Whether to show complete agent outputs (default: True)
        """
        # Main result panel
        panel = result.to_panel(show_data_sample=False)
        self.console.print("\n")
        self.console.print(panel)

        # Show Filter Classifier output (if available and enabled)
        if show_full_agent_output and result.filter_result:
            self.console.print("\n")
            filter_panel = Panel(
                JSON(
                    json.dumps(
                        result.filter_result,
                        ensure_ascii=False,
                        indent=2,
                        default=json_serializer,
                    )
                ),
                title="PHASE 0: Filter Classifier - Complete Output",
                border_style="blue",
                box=box.ROUNDED,
            )
            self.console.print(filter_panel)

        # Show Graphic Classifier output (if available and enabled)
        if show_full_agent_output and result.classifier_result:
            self.console.print("\n")
            classifier_panel = Panel(
                JSON(
                    json.dumps(
                        result.classifier_result,
                        ensure_ascii=False,
                        indent=2,
                        default=json_serializer,
                    )
                ),
                title="PHASE 1: Graphic Classifier - Complete Output",
                border_style="magenta",
                box=box.ROUNDED,
            )
            self.console.print(classifier_panel)

        # Show Analytics Executor output (if available and enabled)
        if show_full_agent_output and result.executor_result:
            self.console.print("\n")
            executor_panel = Panel(
                JSON(
                    json.dumps(
                        result.executor_result,
                        ensure_ascii=False,
                        indent=2,
                        default=json_serializer,
                    )
                ),
                title="PHASE 2: Analytics Executor - Complete Output",
                border_style="cyan",
                box=box.ROUNDED,
            )
            self.console.print(executor_panel)

        # Show Plotly Generator output (if available and enabled)
        if show_full_agent_output and result.plotly_result:
            self.console.print("\n")
            # Filter out the 'figure' key as it's too large to display
            plotly_display = {
                k: v for k, v in result.plotly_result.items() if k != "figure"
            }
            # Convert file_path to string if it's a Path object
            if "file_path" in plotly_display and plotly_display["file_path"]:
                plotly_display["file_path"] = str(plotly_display["file_path"])

            plotly_panel = Panel(
                JSON(
                    json.dumps(
                        plotly_display,
                        ensure_ascii=False,
                        indent=2,
                        default=json_serializer,
                    )
                ),
                title="PHASE 3: Plotly Generator - Complete Output",
                border_style="bright_magenta",
                box=box.ROUNDED,
            )
            self.console.print(plotly_panel)

        # Data table (if requested and available)
        if show_data and result.has_data:
            table = result.to_table(max_rows=max_rows)
            if table:
                self.console.print("\n")
                self.console.print(table)

        # Show available commands hint
        if result.has_data:
            self.console.print(
                "\n[dim]Commands: /sql (show query) | /export (save data) | /full-output (toggle agent details) | /help (all commands)[/dim]"
            )

    def show_filters(self, filters: Dict[str, Any]):
        """
        Display active filters.

        Args:
            filters: Dictionary of active filters
        """
        if not filters:
            self.console.print("[dim]No active filters[/dim]")
            return

        panel = Panel(
            JSON(json.dumps(filters, ensure_ascii=False, default=json_serializer)),
            title=f"Active Filters ({len(filters)})",
            border_style="cyan",
            box=box.ROUNDED,
        )
        self.console.print(panel)

    def show_crud_operations(self, operations: Dict[str, List[Dict]]):
        """
        Display CRUD operations with tables.

        Args:
            operations: Dictionary of operations (ADICIONAR, ALTERAR, etc.)
        """
        operation_styles = {
            "ADICIONAR": ("green", "➕"),
            "ALTERAR": ("yellow", "✏"),
            "REMOVER": ("red", "🗑"),
            "MANTER": ("blue", "✓"),
        }

        for op_name, filters in operations.items():
            if not filters:
                continue

            style, icon = operation_styles.get(op_name, ("white", "•"))

            self.console.print(f"\n[bold {style}]{icon} {op_name}:[/bold {style}]")

            if filters:
                table = Table(
                    box=box.SIMPLE, show_header=True, header_style=f"bold {style}"
                )
                table.add_column("Column", style="white")
                table.add_column("Operator", style="yellow")
                table.add_column("Value", style="cyan")

                for f in filters:
                    col = f.get("column", "N/A")
                    op = f.get("operator", "=")
                    val = str(f.get("value", "N/A"))
                    table.add_row(col, op, val)

                self.console.print(table)

    def show_sql(self, sql_query: str):
        """
        Display SQL query with syntax highlighting.

        Args:
            sql_query: SQL query string
        """
        syntax = Syntax(
            sql_query, "sql", theme="monokai", line_numbers=True, word_wrap=True
        )

        panel = Panel(syntax, title="SQL Query", border_style="green", box=box.ROUNDED)
        self.console.print(panel)

    def show_json(self, data: Any, title: str = "JSON Output"):
        """
        Display JSON data with formatting.

        Args:
            data: Data to display as JSON
            title: Panel title
        """
        if isinstance(data, str):
            json_str = data
        else:
            json_str = json.dumps(
                data, indent=2, ensure_ascii=False, default=json_serializer
            )

        panel = Panel(JSON(json_str), title=title, border_style="cyan", box=box.ROUNDED)
        self.console.print(panel)

    def show_error(self, error_msg: str, details: Optional[str] = None):
        """
        Display error message.

        Args:
            error_msg: Main error message
            details: Additional error details
        """
        content = f"[bold red]Error:[/bold red] {error_msg}"
        if details:
            content += f"\n\n[dim]{details}[/dim]"

        panel = Panel(content, title="Error", border_style="red", box=box.ROUNDED)
        self.console.print(panel)

    def show_warning(self, warning_msg: str):
        """
        Display warning message.

        Args:
            warning_msg: Warning message
        """
        self.console.print(f"[yellow]⚠ {warning_msg}[/yellow]")

    def show_success(self, success_msg: str):
        """
        Display success message.

        Args:
            success_msg: Success message
        """
        self.console.print(f"[green]✓ {success_msg}[/green]")

    def show_info(self, info_msg: str):
        """
        Display info message.

        Args:
            info_msg: Info message
        """
        self.console.print(f"[cyan]ℹ {info_msg}[/cyan]")

    def show_columns(self, columns: List[str], dtypes: Optional[Dict[str, str]] = None):
        """
        Display dataset columns.

        Args:
            columns: List of column names
            dtypes: Optional dictionary of column name -> dtype
        """
        table = Table(
            title=f"Dataset Columns ({len(columns)})",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold cyan",
        )

        table.add_column("#", style="dim", width=4)
        table.add_column("Column Name", style="white")
        if dtypes:
            table.add_column("Type", style="yellow")

        for i, col in enumerate(columns, 1):
            if dtypes and col in dtypes:
                table.add_row(str(i), col, dtypes[col])
            else:
                table.add_row(str(i), col)

        self.console.print(table)

    def show_schema(self, schema_info: Dict[str, Any]):
        """
        Display full dataset schema.

        Args:
            schema_info: Dictionary with schema information
        """
        table = Table(
            title="Dataset Schema",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold cyan",
        )

        table.add_column("Column", style="white")
        table.add_column("Type", style="yellow")
        table.add_column("Sample", style="dim")

        for col, info in schema_info.items():
            dtype = info.get("dtype", "unknown")
            sample = str(info.get("sample", "N/A"))
            if len(sample) > 50:
                sample = sample[:47] + "..."
            table.add_row(col, dtype, sample)

        self.console.print(table)

    def show_history(self, history: List["PipelineResult"], max_items: int = 20):
        """
        Display query history.

        Args:
            history: List of PipelineResult objects
            max_items: Maximum items to show
        """
        if not history:
            self.console.print("[dim]No query history[/dim]")
            return

        table = Table(
            title=f"Query History (last {min(len(history), max_items)})",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold cyan",
        )

        table.add_column("#", style="dim", width=4, justify="right")
        table.add_column("Query", style="white", no_wrap=False)
        table.add_column("Status", style="yellow", width=10)
        table.add_column("Time", style="green", width=10, justify="right")
        table.add_column("Chart", style="cyan", width=15)
        table.add_column("Rows", style="magenta", width=8, justify="right")

        # Show last N items
        display_items = history[-max_items:]
        for i, result in enumerate(display_items, 1):
            # Truncate long queries
            query_text = result.query
            if len(query_text) > 60:
                query_text = query_text[:57] + "..."

            # Status with icon
            status_icon = "✓" if result.is_success else "✗"
            status_style = "green" if result.is_success else "red"

            table.add_row(
                str(i),
                query_text,
                f"[{status_style}]{status_icon}[/{status_style}] {result.status}",
                f"{result.total_time * 1000:.0f}ms",
                result.chart_type or "N/A",
                str(result.row_count) if result.row_count > 0 else "-",
            )

        self.console.print(table)

    def show_comparison(
        self, result1: "PipelineResult", result2: "PipelineResult", idx1: int, idx2: int
    ):
        """
        Display comparison of two results.

        Args:
            result1: First result
            result2: Second result
            idx1: First result index
            idx2: Second result index
        """
        table = Table(
            title=f"Comparison: Query {idx1} vs Query {idx2}",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold cyan",
        )

        table.add_column("Metric", style="white", width=20)
        table.add_column(f"Query {idx1}", style="yellow")
        table.add_column(f"Query {idx2}", style="green")
        table.add_column("Diff", style="magenta")

        # Queries
        q1_short = (
            result1.query[:40] + "..." if len(result1.query) > 40 else result1.query
        )
        q2_short = (
            result2.query[:40] + "..." if len(result2.query) > 40 else result2.query
        )
        table.add_row("Query", q1_short, q2_short, "-")

        # Status
        table.add_row(
            "Status",
            result1.status,
            result2.status,
            "✓" if result1.status == result2.status else "✗",
        )

        # Chart type
        table.add_row(
            "Chart Type",
            result1.chart_type or "N/A",
            result2.chart_type or "N/A",
            "✓" if result1.chart_type == result2.chart_type else "✗",
        )

        # Rows
        row_diff = result2.row_count - result1.row_count
        row_diff_str = f"{row_diff:+d}" if row_diff != 0 else "="
        table.add_row(
            "Rows", str(result1.row_count), str(result2.row_count), row_diff_str
        )

        # Time
        time_diff = (result2.total_time - result1.total_time) * 1000
        time_diff_str = f"{time_diff:+.0f}ms" if abs(time_diff) > 1 else "≈"
        table.add_row(
            "Time",
            f"{result1.total_time * 1000:.0f}ms",
            f"{result2.total_time * 1000:.0f}ms",
            time_diff_str,
        )

        # Engine
        table.add_row(
            "Engine",
            result1.engine_used or "N/A",
            result2.engine_used or "N/A",
            "✓" if result1.engine_used == result2.engine_used else "✗",
        )

        # Confidence
        conf_diff = result2.confidence - result1.confidence
        conf_diff_str = f"{conf_diff:+.2f}" if abs(conf_diff) > 0.01 else "≈"
        table.add_row(
            "Confidence",
            f"{result1.confidence:.2f}",
            f"{result2.confidence:.2f}",
            conf_diff_str,
        )

        self.console.print(table)

    def show_timing_breakdown(self, result: "PipelineResult"):
        """
        Display timing breakdown for a result.

        Args:
            result: PipelineResult with timing information
        """
        if result.total_time == 0:
            self.console.print("[yellow]No timing data available[/yellow]")
            return

        table = Table(
            title="Timing Breakdown",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold cyan",
        )

        table.add_column("Phase", style="white", width=20)
        table.add_column("Time", style="yellow", justify="right", width=12)
        table.add_column("Percentage", style="green", justify="right", width=12)

        phases = [
            ("Filter Classifier", result.filter_time, "cyan"),
            ("Graphic Classifier", result.classifier_time, "yellow"),
            ("Analytics Executor", result.executor_time, "green"),
        ]

        for phase_name, phase_time, color in phases:
            if phase_time > 0:
                pct = phase_time / result.total_time * 100
                table.add_row(
                    f"[{color}]{phase_name}[/{color}]",
                    f"{phase_time * 1000:.0f}ms",
                    f"{pct:.1f}%",
                )

        table.add_row(
            "[bold]TOTAL[/bold]",
            f"[bold]{result.total_time * 1000:.0f}ms[/bold]",
            "[bold]100.0%[/bold]",
        )

        self.console.print(table)

    def clear(self):
        """Clear the console."""
        self.console.clear()

    def print(self, *args, **kwargs):
        """Wrapper for console.print."""
        self.console.print(*args, **kwargs)
