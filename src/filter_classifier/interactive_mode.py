"""
Interactive CLI mode for filter_classifier agent.

This module provides an interactive terminal interface for testing and
using the filter_classifier agent in isolation. Features include:
- Rich terminal UI with colored output
- Special commands (/reset, /show, /stats, /exit, etc.)
- Real-time filter visualization
- Performance metrics tracking
- Query history
"""

import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.json import JSON
from rich.live import Live
from rich.layout import Layout
from rich.syntax import Syntax
from rich.text import Text
from rich.prompt import Prompt
from rich import box

from src.filter_classifier.agent import FilterClassifierAgent
from src.filter_classifier.core.settings import ALIAS_PATH, DATASET_PATH


console = Console()


class InteractiveFilterClassifier:
    """
    Interactive CLI interface for filter_classifier agent.
    
    Provides a rich terminal UI for testing filter classification with:
    - Real-time filter visualization
    - Command history
    - Performance metrics
    - Special commands
    """
    
    def __init__(self):
        """Initialize the interactive session."""
        self.agent: Optional[FilterClassifierAgent] = None
        self.query_history: List[Dict[str, Any]] = []
        self.session_start: datetime = datetime.now()
        self.total_queries: int = 0
        self.total_time: float = 0.0
        
    def initialize_agent(self) -> bool:
        """
        Initialize the filter_classifier agent.
        
        Returns:
            True if initialization successful, False otherwise
        """
        try:
            with console.status("[bold green]Inicializando agente filter_classifier..."):
                self.agent = FilterClassifierAgent(
                    alias_path=ALIAS_PATH,
                    dataset_path=DATASET_PATH,
                    setup_logs=False
                )
            
            console.print("[green]✓[/green] Agente inicializado com sucesso!\n")
            return True
            
        except Exception as e:
            console.print(f"[red]✗[/red] Erro ao inicializar agente: {str(e)}\n")
            return False
    
    def display_welcome(self):
        """Display welcome banner and instructions."""
        welcome_text = Text()
        welcome_text.append("Filter Classifier ", style="bold blue")
        welcome_text.append("- Modo Interativo", style="bold white")
        
        help_text = (
            "[cyan]Comandos Especiais:[/cyan]\n"
            "  [yellow]/help[/yellow]   - Mostra ajuda completa\n"
            "  [yellow]/reset[/yellow]  - Limpa todos os filtros\n"
            "  [yellow]/show[/yellow]   - Mostra filtros ativos\n"
            "  [yellow]/stats[/yellow]  - Exibe estatísticas\n"
            "  [yellow]/history[/yellow] - Histórico de queries\n"
            "  [yellow]/clear[/yellow]  - Limpa a tela\n"
            "  [yellow]/exit[/yellow]   - Sair (ou /quit)\n\n"
            "[dim]Digite suas perguntas naturalmente ou use comandos com /[/dim]"
        )
        
        panel = Panel(
            help_text,
            title=welcome_text,
            border_style="blue",
            box=box.DOUBLE
        )
        
        console.print(panel)
        console.print()
    
    def display_help(self):
        """Display detailed help information."""
        help_panel = Panel(
            "[bold cyan]Ajuda - Filter Classifier Interactive Mode[/bold cyan]\n\n"
            "[yellow]Comandos Disponíveis:[/yellow]\n"
            "  [green]/help[/green]     - Exibe esta ajuda\n"
            "  [green]/reset[/green]    - Remove todos os filtros da sessão\n"
            "  [green]/show[/green]     - Mostra os filtros atualmente ativos\n"
            "  [green]/stats[/green]    - Estatísticas da sessão (queries, tempo, operações)\n"
            "  [green]/history[/green]  - Histórico de queries executadas\n"
            "  [green]/clear[/green]    - Limpa a tela do terminal\n"
            "  [green]/exit[/green]     - Encerra o modo interativo\n"
            "  [green]/quit[/green]     - Mesmo que /exit\n\n"
            "[yellow]Exemplos de Queries:[/yellow]\n"
            "  [dim]• Mostre vendas de SP[/dim]\n"
            "  [dim]• E do ano 2020[/dim]\n"
            "  [dim]• Agora para RJ[/dim]\n"
            "  [dim]• Remova o filtro de estado[/dim]\n"
            "  [dim]• Vendas entre 2015 e 2020[/dim]\n\n"
            "[yellow]Operações CRUD:[/yellow]\n"
            "  • [green]ADICIONAR[/green] - Adiciona novos filtros\n"
            "  • [blue]ALTERAR[/blue]   - Modifica filtros existentes\n"
            "  • [red]REMOVER[/red]    - Remove filtros\n"
            "  • [yellow]MANTER[/yellow]    - Mantém filtros inalterados\n",
            border_style="cyan"
        )
        console.print(help_panel)
    
    def display_filters(self, filters: Dict[str, Any]):
        """
        Display active filters in a formatted panel.
        
        Args:
            filters: Dictionary of active filters
        """
        if not filters:
            console.print(Panel(
                "[dim]Nenhum filtro ativo[/dim]",
                title="[cyan]Filtros Ativos[/cyan]",
                border_style="cyan"
            ))
            return
        
        # Create formatted JSON view
        json_view = JSON.from_data(filters)
        
        panel = Panel(
            json_view,
            title=f"[cyan]Filtros Ativos[/cyan] [dim]({len(filters)} filtro(s))[/dim]",
            border_style="cyan"
        )
        
        console.print(panel)
    
    def display_output(self, output: Dict[str, Any], elapsed_time: float):
        """
        Display query output with rich formatting.
        
        Args:
            output: Output from filter_classifier
            elapsed_time: Execution time in seconds
        """
        console.print()  # Blank line
        
        # Extract components
        filter_final = output.get("filter_final", {})
        metadata = output.get("metadata", {})
        confidence = metadata.get("confidence", 0.0)
        columns_detected = metadata.get("columns_detected", [])
        status = metadata.get("status", "unknown")
        errors = metadata.get("errors", [])
        
        # Status indicator
        status_icon = "✓" if status == "success" else "⚠" if status == "partial" else "✗"
        status_color = "green" if status == "success" else "yellow" if status == "partial" else "red"
        
        # Create main result panel
        result_text = ""
        
        # Filter final
        if filter_final:
            result_text += "[bold cyan]Filtros Finais:[/bold cyan]\n"
            for col, val in filter_final.items():
                result_text += f"  • [yellow]{col}[/yellow]: {val}\n"
        else:
            result_text += "[dim]Nenhum filtro final[/dim]\n"
        
        # CRUD operations
        crud_ops = []
        for op in ["ADICIONAR", "ALTERAR", "REMOVER", "MANTER"]:
            op_data = output.get(op, {})
            if op_data:
                color = {"ADICIONAR": "green", "ALTERAR": "blue", "REMOVER": "red", "MANTER": "yellow"}[op]
                crud_ops.append(f"[{color}]{op}[/{color}]: {len(op_data)} filtro(s)")
        
        if crud_ops:
            result_text += f"\n[bold]Operações:[/bold]\n  "
            result_text += " | ".join(crud_ops) + "\n"
        
        # Metadata
        result_text += f"\n[dim]Confiança: {confidence:.2%} | Tempo: {elapsed_time:.3f}s[/dim]"
        
        if columns_detected:
            result_text += f"\n[dim]Colunas detectadas: {', '.join(columns_detected)}[/dim]"
        
        # Display result panel
        panel = Panel(
            result_text,
            title=f"[{status_color}]{status_icon} Resultado[/{status_color}]",
            border_style=status_color
        )
        console.print(panel)
        
        # Display detailed CRUD operations if any
        has_operations = any(output.get(op) for op in ["ADICIONAR", "ALTERAR", "REMOVER"])
        if has_operations:
            self._display_crud_operations(output)
        
        # Display errors if any
        if errors:
            error_panel = Panel(
                "\n".join(f"• {err}" for err in errors),
                title="[red]Erros/Avisos[/red]",
                border_style="red"
            )
            console.print(error_panel)
        
        console.print()  # Blank line
    
    def _display_crud_operations(self, output: Dict[str, Any]):
        """Display detailed CRUD operations in a table."""
        has_ops = False
        
        for operation in ["ADICIONAR", "ALTERAR", "REMOVER"]:
            op_data = output.get(operation, {})
            if not op_data:
                continue
            
            has_ops = True
            color = {"ADICIONAR": "green", "ALTERAR": "blue", "REMOVER": "red"}[operation]
            
            # Create table for this operation
            table = Table(
                title=f"[{color}]{operation}[/{color}]",
                border_style=color,
                show_header=True,
                header_style=f"bold {color}"
            )
            
            if operation == "ALTERAR":
                table.add_column("Coluna", style="yellow")
                table.add_column("De", style="dim")
                table.add_column("Para", style="bright_white")
                
                for col, change in op_data.items():
                    if isinstance(change, dict) and "from" in change and "to" in change:
                        table.add_row(col, str(change["from"]), str(change["to"]))
                    else:
                        table.add_row(col, "—", str(change))
            else:
                table.add_column("Coluna", style="yellow")
                table.add_column("Valor", style="bright_white")
                
                for col, val in op_data.items():
                    table.add_row(col, str(val))
            
            console.print(table)
    
    def display_statistics(self):
        """Display session statistics."""
        if not self.agent:
            console.print("[red]Agente não inicializado[/red]")
            return
        
        stats = self.agent.get_statistics()
        
        # Create statistics table
        table = Table(
            title="[bold cyan]Estatísticas da Sessão[/bold cyan]",
            border_style="cyan",
            show_header=True,
            header_style="bold cyan"
        )
        
        table.add_column("Métrica", style="yellow", width=30)
        table.add_column("Valor", justify="right", style="bright_white")
        
        # Session info
        session_duration = datetime.now() - self.session_start
        duration_str = str(session_duration).split('.')[0]  # Remove microseconds
        
        table.add_row("Duração da sessão", duration_str)
        table.add_row("Queries processadas", str(stats["query_count"]))
        table.add_row("Erros", str(stats.get("error_count", 0)))
        
        # Average time
        if self.total_queries > 0:
            avg_time = self.total_time / self.total_queries
            table.add_row("Tempo médio", f"{avg_time:.3f}s")
        
        # CRUD operations
        operations = stats.get("filter_operations", {})
        table.add_row("", "")  # Blank row
        table.add_row("[bold]Operações CRUD", "", style="dim")
        table.add_row("  ADICIONAR", str(operations.get("ADICIONAR", 0)), style="green")
        table.add_row("  ALTERAR", str(operations.get("ALTERAR", 0)), style="blue")
        table.add_row("  REMOVER", str(operations.get("REMOVER", 0)), style="red")
        table.add_row("  MANTER", str(operations.get("MANTER", 0)), style="yellow")
        
        # Session info
        session_info = stats.get("session_info", {})
        if session_info:
            table.add_row("", "")  # Blank row
            table.add_row("[bold]Sessão de Filtros", "", style="dim")
            table.add_row("  Filtros ativos", str(session_info.get("active_filters", 0)))
            has_session = session_info.get("has_active_session", False)
            table.add_row("  Sessão ativa", "Sim" if has_session else "Não")
        
        console.print(table)
    
    def display_history(self):
        """Display query history."""
        if not self.query_history:
            console.print("[dim]Nenhuma query no histórico[/dim]")
            return
        
        table = Table(
            title=f"[bold cyan]Histórico de Queries[/bold cyan] [dim]({len(self.query_history)} queries)[/dim]",
            border_style="cyan",
            show_header=True,
            header_style="bold cyan"
        )
        
        table.add_column("#", justify="right", style="dim", width=4)
        table.add_column("Query", style="bright_white", width=50)
        table.add_column("Filtros", justify="center", style="yellow", width=8)
        table.add_column("Tempo", justify="right", style="green", width=8)
        
        for i, entry in enumerate(self.query_history[-20:], 1):  # Last 20 queries
            query = entry["query"]
            if len(query) > 47:
                query = query[:44] + "..."
            
            filter_count = entry.get("filter_count", 0)
            exec_time = entry.get("execution_time", 0)
            
            table.add_row(
                str(i),
                query,
                str(filter_count),
                f"{exec_time:.3f}s"
            )
        
        console.print(table)
    
    def process_query(self, query: str):
        """
        Process a user query through the filter_classifier.
        
        Args:
            query: User query to process
        """
        if not self.agent:
            console.print("[red]Erro: Agente não inicializado[/red]")
            return
        
        # Execute query
        start_time = time.perf_counter()
        
        try:
            output = self.agent.classify_filters(query)
            elapsed_time = time.perf_counter() - start_time
            
            # Update statistics
            self.total_queries += 1
            self.total_time += elapsed_time
            
            # Add to history
            filter_count = len(output.get("filter_final", {}))
            self.query_history.append({
                "query": query,
                "filter_count": filter_count,
                "execution_time": elapsed_time,
                "timestamp": datetime.now()
            })
            
            # Display output
            self.display_output(output, elapsed_time)
            
        except Exception as e:
            elapsed_time = time.perf_counter() - start_time
            
            # Display error
            error_panel = Panel(
                f"[red]Erro ao processar query:[/red]\n{str(e)}",
                title="[red]✗ Erro[/red]",
                border_style="red"
            )
            console.print(error_panel)
            
            # Still update statistics
            self.total_queries += 1
            self.total_time += elapsed_time
    
    def handle_command(self, command: str) -> bool:
        """
        Handle special commands.
        
        Args:
            command: Command to execute (e.g., '/reset', '/show')
            
        Returns:
            True to continue, False to exit
        """
        command = command.lower().strip()
        
        if command in ["/exit", "/quit"]:
            return False
        
        elif command == "/help":
            self.display_help()
        
        elif command == "/reset":
            if self.agent:
                self.agent.clear_filters()
                console.print("[green]✓[/green] Filtros limpos com sucesso!\n")
            else:
                console.print("[red]Erro: Agente não inicializado[/red]\n")
        
        elif command == "/show":
            if self.agent:
                filters = self.agent.get_active_filters()
                self.display_filters(filters)
            else:
                console.print("[red]Erro: Agente não inicializado[/red]")
        
        elif command == "/stats":
            self.display_statistics()
        
        elif command == "/history":
            self.display_history()
        
        elif command == "/clear":
            console.clear()
            self.display_welcome()
        
        else:
            console.print(f"[yellow]⚠[/yellow] Comando desconhecido: {command}")
            console.print("[dim]Use /help para ver comandos disponíveis[/dim]\n")
        
        return True
    
    def run(self):
        """Run the interactive session."""
        # Display welcome
        self.display_welcome()
        
        # Initialize agent
        if not self.initialize_agent():
            console.print("[red]Falha ao inicializar. Encerrando...[/red]")
            sys.exit(1)
        
        # Main loop
        try:
            while True:
                # Get input
                try:
                    query = Prompt.ask(
                        "\n[bold cyan]filter>[/bold cyan]",
                        default=""
                    ).strip()
                except EOFError:
                    # Handle Ctrl+D
                    break
                
                if not query:
                    continue
                
                # Check if it's a command
                if query.startswith("/"):
                    should_continue = self.handle_command(query)
                    if not should_continue:
                        break
                else:
                    # Process as query
                    self.process_query(query)
        
        except KeyboardInterrupt:
            console.print("\n\n[yellow]Interrompido pelo usuário[/yellow]")
        
        finally:
            # Display final statistics
            console.print("\n[bold cyan]Sessão Encerrada[/bold cyan]")
            if self.total_queries > 0:
                self.display_statistics()
            
            console.print("\n[dim]Obrigado por usar o Filter Classifier![/dim]\n")


def main():
    """Main entry point for interactive mode."""
    session = InteractiveFilterClassifier()
    session.run()


if __name__ == "__main__":
    main()

