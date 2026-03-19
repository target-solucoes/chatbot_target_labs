"""
Command Handlers Module

Handles special commands for interactive pipeline session.
"""

from typing import Dict, Callable, Optional, Any
from pathlib import Path
import json
from rich.console import Console


class CommandHandler:
    """
    Handles special commands in interactive session.

    Provides handlers for 15+ commands including filter management,
    pipeline control, data inspection, output export, and debugging.
    """

    def __init__(self, session: 'InteractivePipelineSession'):
        """
        Initialize command handler.

        Args:
            session: Reference to the InteractivePipelineSession
        """
        self.session = session
        self.console = Console()

        # Command registry
        self.commands: Dict[str, Callable] = {
            # Filter commands
            '/reset': self.cmd_reset,
            '/show': self.cmd_show_filters,
            '/filters': self.cmd_show_filters,  # Alias

            # Pipeline control
            '/mode': self.cmd_toggle_mode,
            '/enable-filters': self.cmd_enable_filters,
            '/disable-filters': self.cmd_disable_filters,
            '/enable-executor': self.cmd_enable_executor,
            '/disable-executor': self.cmd_disable_executor,

            # Data inspection
            '/data': self.cmd_show_data,
            '/columns': self.cmd_show_columns,
            '/schema': self.cmd_show_schema,

            # Output export
            '/export': self.cmd_export,
            '/save': self.cmd_export,  # Alias

            # History and replay
            '/history': self.cmd_show_history,
            '/replay': self.cmd_replay,
            '/compare': self.cmd_compare,

            # Stats and debug
            '/stats': self.cmd_show_stats,
            '/timing': self.cmd_show_timing,
            '/debug': self.cmd_toggle_debug,
            '/sql': self.cmd_show_sql,
            '/full-output': self.cmd_toggle_full_output,

            # Help and control
            '/help': self.cmd_help,
            '/clear': self.cmd_clear,
            '/exit': self.cmd_exit,
            '/quit': self.cmd_exit,  # Alias
        }

    def handle(self, command_str: str) -> bool:
        """
        Handle a command.

        Args:
            command_str: Command string starting with '/'

        Returns:
            bool: False if should exit, True to continue
        """
        parts = command_str.strip().split(maxsplit=1)
        cmd = parts[0].lower()
        args = parts[1] if len(parts) > 1 else ''

        if cmd in self.commands:
            return self.commands[cmd](args)
        else:
            self.console.print(f"[red]Unknown command: {cmd}[/red]")
            self.console.print("Type [cyan]/help[/cyan] for available commands")
            return True

    # ========================================================================
    # FILTER COMMANDS
    # ========================================================================

    def cmd_reset(self, args: str) -> bool:
        """Reset all filters."""
        if self.session.filter_agent:
            self.session.filter_agent.clear_filters()
            self.console.print("[green]All filters cleared[/green]")
        else:
            self.console.print("[yellow]Filter classifier not enabled[/yellow]")
        return True

    def cmd_show_filters(self, args: str) -> bool:
        """Show active filters."""
        if self.session.filter_agent:
            filters = self.session.filter_agent.get_active_filters()
            if filters:
                self.console.print("[bold cyan]Active Filters:[/bold cyan]")
                self.console.print_json(data=filters)
            else:
                self.console.print("[dim]No active filters[/dim]")
        else:
            self.console.print("[yellow]Filter classifier not enabled[/yellow]")
        return True

    # ========================================================================
    # PIPELINE CONTROL COMMANDS
    # ========================================================================

    def cmd_toggle_mode(self, args: str) -> bool:
        """Toggle between integrated and sequential mode."""
        self.session.config.use_integrated = not self.session.config.use_integrated
        mode = "integrated" if self.session.config.use_integrated else "sequential"
        self.console.print(f"[green]Switched to {mode} mode[/green]")
        return True

    def cmd_enable_filters(self, args: str) -> bool:
        """Enable filter classifier."""
        self.session.config.enable_filter_classifier = True
        self.console.print("[green]Filter classifier enabled[/green]")
        return True

    def cmd_disable_filters(self, args: str) -> bool:
        """Disable filter classifier."""
        self.session.config.enable_filter_classifier = False
        self.console.print("[yellow]Filter classifier disabled[/yellow]")
        return True

    def cmd_enable_executor(self, args: str) -> bool:
        """Enable analytics executor."""
        self.session.config.enable_executor = True
        self.console.print("[green]Analytics executor enabled[/green]")
        return True

    def cmd_disable_executor(self, args: str) -> bool:
        """Disable analytics executor."""
        self.session.config.enable_executor = False
        self.console.print("[yellow]Analytics executor disabled (classification only)[/yellow]")
        return True

    # ========================================================================
    # DATA INSPECTION COMMANDS
    # ========================================================================

    def cmd_show_data(self, args: str) -> bool:
        """Show data path."""
        self.console.print(f"[cyan]Data source:[/cyan] {self.session.config.data_path}")
        return True

    def cmd_show_columns(self, args: str) -> bool:
        """Show dataset columns."""
        from src.analytics_executor.data.data_loader import DataLoader

        try:
            loader = DataLoader()
            df = loader.load(self.session.config.data_path)

            self.console.print(f"[bold cyan]Dataset Columns ({len(df.columns)} total):[/bold cyan]")
            for i, col in enumerate(df.columns, 1):
                dtype = str(df[col].dtype)
                self.console.print(f"  {i:2d}. {col:30s} [{dtype}]")
        except Exception as e:
            self.console.print(f"[red]Error loading dataset: {e}[/red]")

        return True

    def cmd_show_schema(self, args: str) -> bool:
        """Show full dataset schema with sample values."""
        from src.analytics_executor.data.data_loader import DataLoader

        try:
            loader = DataLoader()
            df = loader.load(self.session.config.data_path)

            self.console.print(f"[bold cyan]Dataset Schema[/bold cyan]")
            self.console.print(f"Rows: {len(df):,}")
            self.console.print(f"Columns: {len(df.columns)}\n")

            from rich.table import Table
            table = Table(show_header=True, header_style="bold cyan")
            table.add_column("Column", style="white")
            table.add_column("Type", style="yellow")
            table.add_column("Sample", style="dim")

            for col in df.columns:
                dtype = str(df[col].dtype)
                sample = str(df[col].iloc[0]) if len(df) > 0 else "N/A"
                if len(sample) > 50:
                    sample = sample[:47] + "..."
                table.add_row(col, dtype, sample)

            self.console.print(table)
        except Exception as e:
            self.console.print(f"[red]Error loading dataset: {e}[/red]")

        return True

    # ========================================================================
    # EXPORT COMMANDS
    # ========================================================================

    def cmd_export(self, args: str) -> bool:
        """
        Export last result.

        Usage: /export [json|csv|plotly] <path>
        """
        if not args:
            self.console.print("[yellow]Usage: /export [json|csv|plotly] <path>[/yellow]")
            return True

        parts = args.split(maxsplit=1)
        if len(parts) < 2:
            self.console.print("[yellow]Please specify format and path[/yellow]")
            return True

        format_type = parts[0].lower()
        path = parts[1]

        if not self.session.last_result:
            self.console.print("[yellow]No result to export[/yellow]")
            return True

        try:
            if format_type == 'json':
                self._export_json(path)
            elif format_type == 'csv':
                self._export_csv(path)
            elif format_type == 'plotly':
                self._export_plotly(path)
            else:
                self.console.print(f"[red]Unknown format: {format_type}[/red]")
                self.console.print("Supported formats: json, csv, plotly")
        except Exception as e:
            self.console.print(f"[red]Export failed: {e}[/red]")

        return True

    def _export_json(self, path: str):
        """Export result as JSON."""
        result_dict = self.session.last_result.to_dict()
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(result_dict, f, indent=2, ensure_ascii=False, default=str)
        self.console.print(f"[green]Exported to {path}[/green]")

    def _export_csv(self, path: str):
        """Export data as CSV."""
        import pandas as pd
        if self.session.last_result.data:
            df = pd.DataFrame(self.session.last_result.data)
            df.to_csv(path, index=False, encoding='utf-8')
            self.console.print(f"[green]Exported {len(df)} rows to {path}[/green]")
        else:
            self.console.print("[yellow]No data to export[/yellow]")

    def _export_plotly(self, path: str):
        """Export Plotly config as JSON."""
        if self.session.last_result.executor_result:
            plotly_config = self.session.last_result.executor_result.get('plotly_config')
            if plotly_config:
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(plotly_config, f, indent=2, ensure_ascii=False)
                self.console.print(f"[green]Exported Plotly config to {path}[/green]")
            else:
                self.console.print("[yellow]No Plotly config available[/yellow]")
        else:
            self.console.print("[yellow]No executor result available[/yellow]")

    # ========================================================================
    # HISTORY COMMANDS
    # ========================================================================

    def cmd_show_history(self, args: str) -> bool:
        """Show query history."""
        if not self.session.query_history:
            self.console.print("[dim]No query history[/dim]")
            return True

        from rich.table import Table
        table = Table(title="Query History", show_header=True, header_style="bold cyan")
        table.add_column("#", style="dim", width=4)
        table.add_column("Query", style="white")
        table.add_column("Status", style="yellow", width=10)
        table.add_column("Time", style="green", width=10)
        table.add_column("Chart", style="cyan", width=15)

        # Show last 20 queries
        for i, result in enumerate(self.session.query_history[-20:], 1):
            query_short = result.query[:60] + "..." if len(result.query) > 60 else result.query
            status_icon = "✓" if result.is_success else "✗"
            table.add_row(
                str(i),
                query_short,
                f"{status_icon} {result.status}",
                f"{result.total_time*1000:.0f}ms",
                result.chart_type or "N/A"
            )

        self.console.print(table)
        return True

    def cmd_replay(self, args: str) -> bool:
        """Replay a query from history."""
        if not args.isdigit():
            self.console.print("[yellow]Usage: /replay <number>[/yellow]")
            return True

        index = int(args) - 1
        if 0 <= index < len(self.session.query_history):
            query = self.session.query_history[index].query
            self.console.print(f"[cyan]Replaying:[/cyan] {query}")
            self.session.process_query(query)
        else:
            self.console.print(f"[red]Invalid index. History has {len(self.session.query_history)} queries[/red]")

        return True

    def cmd_compare(self, args: str) -> bool:
        """Compare two results from history."""
        parts = args.split()
        if len(parts) != 2 or not all(p.isdigit() for p in parts):
            self.console.print("[yellow]Usage: /compare <number1> <number2>[/yellow]")
            return True

        idx1, idx2 = int(parts[0]) - 1, int(parts[1]) - 1
        if not (0 <= idx1 < len(self.session.query_history) and 0 <= idx2 < len(self.session.query_history)):
            self.console.print(f"[red]Invalid indices. History has {len(self.session.query_history)} queries[/red]")
            return True

        r1 = self.session.query_history[idx1]
        r2 = self.session.query_history[idx2]

        from rich.table import Table
        table = Table(title="Comparison", show_header=True, header_style="bold cyan")
        table.add_column("Metric", style="white")
        table.add_column(f"Query {idx1+1}", style="yellow")
        table.add_column(f"Query {idx2+1}", style="green")

        table.add_row("Query", r1.query[:40] + "...", r2.query[:40] + "...")
        table.add_row("Status", r1.status, r2.status)
        table.add_row("Chart Type", r1.chart_type or "N/A", r2.chart_type or "N/A")
        table.add_row("Rows", str(r1.row_count), str(r2.row_count))
        table.add_row("Time", f"{r1.total_time*1000:.0f}ms", f"{r2.total_time*1000:.0f}ms")
        table.add_row("Engine", r1.engine_used or "N/A", r2.engine_used or "N/A")

        self.console.print(table)
        return True

    # ========================================================================
    # STATS AND DEBUG COMMANDS
    # ========================================================================

    def cmd_show_stats(self, args: str) -> bool:
        """Show session statistics."""
        table = self.session.statistics.to_table()
        self.console.print(table)
        return True

    def cmd_show_timing(self, args: str) -> bool:
        """Show timing breakdown."""
        if not self.session.last_result:
            self.console.print("[yellow]No result available[/yellow]")
            return True

        from rich.table import Table
        table = Table(title="Timing Breakdown", show_header=True, header_style="bold cyan")
        table.add_column("Phase", style="white")
        table.add_column("Time", style="yellow", justify="right")
        table.add_column("Percentage", style="green", justify="right")

        total = self.session.last_result.total_time
        if total > 0:
            phases = [
                ("Filter", self.session.last_result.filter_time),
                ("Classifier", self.session.last_result.classifier_time),
                ("Executor", self.session.last_result.executor_time),
            ]

            for phase, time in phases:
                pct = (time / total * 100) if total > 0 else 0
                table.add_row(phase, f"{time*1000:.0f}ms", f"{pct:.1f}%")

            table.add_row("[bold]TOTAL[/bold]", f"[bold]{total*1000:.0f}ms[/bold]", "[bold]100.0%[/bold]")

            self.console.print(table)
        else:
            self.console.print("[yellow]No timing data available[/yellow]")

        return True

    def cmd_toggle_debug(self, args: str) -> bool:
        """Toggle debug mode."""
        self.session.config.debug_mode = not self.session.config.debug_mode
        status = "enabled" if self.session.config.debug_mode else "disabled"
        self.console.print(f"[green]Debug mode {status}[/green]")
        return True

    def cmd_toggle_full_output(self, args: str) -> bool:
        """Toggle full agent output display."""
        self.session.show_full_agent_output = not self.session.show_full_agent_output
        status = "enabled" if self.session.show_full_agent_output else "disabled"
        mode = "Complete JSON outputs" if self.session.show_full_agent_output else "Summary only"
        self.console.print(f"[green]Full agent output {status}[/green]")
        self.console.print(f"[dim]Mode: {mode}[/dim]")
        return True

    def cmd_show_sql(self, args: str) -> bool:
        """Show last SQL query."""
        if not self.session.last_result or not self.session.last_result.sql_query:
            self.console.print("[yellow]No SQL query available[/yellow]")
            return True

        syntax = self.session.last_result.get_sql_syntax()
        if syntax:
            self.console.print("\n[bold cyan]SQL Query:[/bold cyan]")
            self.console.print(syntax)

        return True

    # ========================================================================
    # HELP AND CONTROL COMMANDS
    # ========================================================================

    def cmd_help(self, args: str) -> bool:
        """Show help message."""
        from rich.panel import Panel

        help_text = """
[bold cyan]Available Commands:[/bold cyan]

[yellow]Filter Management:[/yellow]
  /reset              - Clear all filters
  /show, /filters     - Show active filters

[yellow]Pipeline Control:[/yellow]
  /mode               - Toggle integrated/sequential mode
  /enable-filters     - Enable filter classifier
  /disable-filters    - Disable filter classifier
  /enable-executor    - Enable analytics executor
  /disable-executor   - Disable executor (classification only)

[yellow]Data Inspection:[/yellow]
  /data               - Show data source path
  /columns            - List dataset columns
  /schema             - Show full dataset schema

[yellow]Export:[/yellow]
  /export json <path> - Export result as JSON
  /export csv <path>  - Export data as CSV
  /export plotly <path> - Export Plotly config

[yellow]History:[/yellow]
  /history            - Show query history
  /replay <N>         - Re-execute query N from history
  /compare <N> <M>    - Compare two results

[yellow]Stats & Debug:[/yellow]
  /stats              - Show session statistics
  /timing             - Show timing breakdown
  /debug              - Toggle debug mode
  /sql                - Show last SQL query
  /full-output        - Toggle full agent output display

[yellow]Control:[/yellow]
  /help               - Show this help
  /clear              - Clear screen
  /exit, /quit        - Exit session
"""

        panel = Panel(help_text, title="Interactive Pipeline Help", border_style="cyan")
        self.console.print(panel)
        return True

    def cmd_clear(self, args: str) -> bool:
        """Clear screen."""
        self.console.clear()
        return True

    def cmd_exit(self, args: str) -> bool:
        """Exit session."""
        return False
