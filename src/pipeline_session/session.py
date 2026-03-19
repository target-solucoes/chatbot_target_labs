"""
Interactive Pipeline Session Module

Main class for interactive multi-agent pipeline sessions.
"""

from dataclasses import dataclass
from typing import Optional, List
from pathlib import Path
from datetime import datetime
import time

from rich.console import Console
from rich.prompt import Prompt

from src.pipeline_session.result import PipelineResult
from src.pipeline_session.statistics import SessionStatistics
from src.pipeline_session.commands import CommandHandler
from src.pipeline_session.display import DisplayHelper


@dataclass
class PipelineConfig:
    """Configuration for pipeline execution."""

    data_path: str
    use_integrated: bool = True
    enable_filter_classifier: bool = True
    enable_executor: bool = True
    enable_plotly: bool = False
    save_png: bool = False
    debug_mode: bool = False
    verbose: bool = False


class InteractivePipelineSession:
    """
    Interactive session for complete multi-agent pipeline.

    Provides CLI interface with Rich displays for testing and validation
    of the three-agent system (filter, classifier, executor).

    Inspired by run_interactive_filter.py but expanded for full pipeline.
    """

    def __init__(
        self,
        data_path: Optional[str] = None,
        use_integrated: bool = True,
        enable_filter_classifier: bool = True,
        enable_executor: bool = True,
        enable_plotly: bool = False,
        save_png: bool = False,
        verbose: bool = False,
    ):
        """
        Initialize interactive pipeline session.

        Args:
            data_path: Path to dataset
            use_integrated: Use integrated workflow (vs sequential)
            enable_filter_classifier: Enable filter classifier phase
            enable_executor: Enable analytics executor phase
            enable_plotly: Enable plotly chart generation phase
            save_png: Save charts as PNG (requires kaleido)
            verbose: Enable verbose logging
        """
        # Configuration
        self.config = PipelineConfig(
            data_path=data_path or self._get_default_data_path(),
            use_integrated=use_integrated,
            enable_filter_classifier=enable_filter_classifier,
            enable_executor=enable_executor,
            enable_plotly=enable_plotly,
            save_png=save_png,
            verbose=verbose,
        )

        # Display and UI
        self.console = Console()
        self.display = DisplayHelper(self.console)
        self.command_handler = CommandHandler(self)

        # Session state
        self.query_history: List[PipelineResult] = []
        self.statistics = SessionStatistics()
        self.last_result: Optional[PipelineResult] = None
        self.show_full_agent_output: bool = (
            True  # Show complete agent outputs by default
        )

        # Agents (initialized lazily)
        self.filter_agent = None
        self.graphic_agent = None
        self.executor_agent = None
        self.plotly_agent = None

        # Pipeline orchestrator
        self._orchestrator = None

    def _get_default_data_path(self) -> str:
        """Get default data path from centralized config."""
        from src.shared_lib.core.config import get_dataset_path
        return get_dataset_path()

    def initialize_agents(self) -> bool:
        """
        Initialize all agents (filter, classifier, executor, plotly).

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.console.print("[cyan]Initializing agents...[/cyan]")

            # Filter Classifier (optional)
            if self.config.enable_filter_classifier:
                from src.filter_classifier.agent import FilterClassifierAgent

                self.filter_agent = FilterClassifierAgent()
                self.console.print("[green]✓ Filter Classifier initialized[/green]")

            # Graphic Classifier (always needed)
            from src.graphic_classifier.agent import GraphicClassifierAgent

            self.graphic_agent = GraphicClassifierAgent()
            self.console.print("[green]✓ Graphic Classifier initialized[/green]")

            # Analytics Executor (optional)
            if self.config.enable_executor:
                from src.analytics_executor.agent import AnalyticsExecutorAgent

                self.executor_agent = AnalyticsExecutorAgent(
                    default_data_path=self.config.data_path  # Correct parameter name
                )
                self.console.print("[green]✓ Analytics Executor initialized[/green]")

            # Plotly Generator (optional)
            if self.config.enable_plotly:
                from src.plotly_generator.plotly_generator_agent import (
                    PlotlyGeneratorAgent,
                )

                self.plotly_agent = PlotlyGeneratorAgent(
                    save_html=True, save_png=self.config.save_png
                )
                self.console.print("[green]✓ Plotly Generator initialized[/green]")

            self.console.print(
                "[bold green]All agents initialized successfully![/bold green]\n"
            )
            return True

        except Exception as e:
            self.display.show_error(f"Failed to initialize agents: {e}")
            if self.config.debug_mode:
                import traceback

                self.console.print(traceback.format_exc())
            return False

    def run(self):
        """
        Run the interactive session.

        Main entry point for the interactive CLI.
        """
        # Initialize agents
        if not self.initialize_agents():
            return

        # Show welcome message
        self.display.show_welcome()
        self.display.show_info(f"Data source: {self.config.data_path}")
        self.display.show_info(
            f"Mode: {'Integrated' if self.config.use_integrated else 'Sequential'} | "
            f"Filter: {'ON' if self.config.enable_filter_classifier else 'OFF'} | "
            f"Executor: {'ON' if self.config.enable_executor else 'OFF'}"
        )
        self.console.print()

        # Main loop
        while True:
            try:
                # Get user input
                query = Prompt.ask("\n[bold cyan]pipeline>[/bold cyan]")

                if not query.strip():
                    continue

                # Handle commands
                if query.startswith("/"):
                    should_continue = self.command_handler.handle(query)
                    if not should_continue:
                        break
                else:
                    # Process query
                    self.process_query(query)

            except KeyboardInterrupt:
                self.console.print("\n[yellow]Use /exit to quit[/yellow]")
                continue
            except EOFError:
                break
            except Exception as e:
                self.display.show_error(f"Unexpected error: {e}")
                if self.config.debug_mode:
                    import traceback

                    self.console.print(traceback.format_exc())

        # Shutdown
        self.shutdown()

    def process_query(self, query: str):
        """
        Process a user query through the pipeline.

        Args:
            query: User query string
        """
        start_time = time.time()

        try:
            if self.config.use_integrated:
                result = self._process_integrated(query)
            else:
                result = self._process_sequential(query)

            # Display result
            self.display.show_result(
                result,
                show_data=True,
                max_rows=10,
                show_full_agent_output=self.show_full_agent_output,
            )

            # Update statistics
            self._update_statistics(result)

            # Store in history
            self.query_history.append(result)
            self.last_result = result

        except Exception as e:
            # Create error result
            total_time = time.time() - start_time
            result = PipelineResult(
                query=query, status="error", total_time=total_time, errors=[str(e)]
            )

            self.display.show_error(f"Query processing failed: {e}")
            if self.config.debug_mode:
                import traceback

                self.console.print(traceback.format_exc())

            # Still track the error
            self.query_history.append(result)
            self.last_result = result
            self._update_statistics(result)

    def _process_integrated(self, query: str) -> PipelineResult:
        """
        Process query using integrated workflow.

        Args:
            query: User query

        Returns:
            PipelineResult
        """
        from src.pipeline_orchestrator import run_integrated_pipeline

        # Execute integrated pipeline
        pipeline_result = run_integrated_pipeline(
            query=query,
            include_filter_classifier=self.config.enable_filter_classifier,
            include_executor=self.config.enable_executor,
            data_path=self.config.data_path,
        )

        # Extract timing and results
        result = PipelineResult(
            query=query,
            status=pipeline_result.status,
            filter_result=pipeline_result.filter_output
            if hasattr(pipeline_result, "filter_output")
            else None,
            classifier_result=pipeline_result.classifier_output,
            executor_result=pipeline_result.executor_output
            if self.config.enable_executor
            else None,
            active_filters=pipeline_result.active_filters or {},
            chart_type=pipeline_result.chart_type,
            intent=pipeline_result.intent,
            confidence=pipeline_result.confidence,
            data=pipeline_result.data or [],
            row_count=len(pipeline_result.data) if pipeline_result.data else 0,
            engine_used=pipeline_result.engine_used,
            sql_query=pipeline_result.executor_output.get("sql_query")
            if pipeline_result.executor_output
            else None,
            errors=pipeline_result.errors or [],
            total_time=pipeline_result.execution_time,
        )

        # Estimate phase times (integrated pipeline doesn't break down timing)
        # Use heuristics: ~20% filter, ~25% classifier, ~55% executor
        if self.config.enable_filter_classifier:
            result.filter_time = result.total_time * 0.20
            result.classifier_time = result.total_time * 0.25
            result.executor_time = result.total_time * 0.55
        else:
            result.classifier_time = result.total_time * 0.35
            result.executor_time = result.total_time * 0.65

        # Phase 3: Plotly Generator (optional, runs after integrated pipeline)
        if self.config.enable_plotly and self.plotly_agent and result.executor_result:
            if result.executor_result.get("status") == "success":
                plotly_start = time.time()
                try:
                    plotly_output = self.plotly_agent.generate(
                        chart_spec=result.classifier_result,
                        analytics_result=result.executor_result,
                    )
                    result.plotly_result = plotly_output
                    result.plotly_time = time.time() - plotly_start

                    # Update total time
                    result.total_time += result.plotly_time

                    # Log chart generation
                    if plotly_output.get("status") == "success" and plotly_output.get(
                        "file_path"
                    ):
                        self.console.print(
                            f"[green]✓ Chart saved to: {plotly_output['file_path']}[/green]"
                        )

                except Exception as e:
                    result.plotly_time = time.time() - plotly_start
                    result.total_time += result.plotly_time
                    result.errors.append(f"Plotly generation error: {e}")
                    if self.config.debug_mode:
                        import traceback

                        self.console.print(traceback.format_exc())

        return result

    def _process_sequential(self, query: str) -> PipelineResult:
        """
        Process query using sequential workflow.

        Args:
            query: User query

        Returns:
            PipelineResult
        """
        result = PipelineResult(query=query, status="success")
        errors = []

        try:
            # Phase 0: Filter Classifier (optional)
            filter_start = time.time()
            if self.config.enable_filter_classifier and self.filter_agent:
                try:
                    filter_output = self.filter_agent.classify_filters(query)
                    result.filter_result = filter_output
                    result.active_filters = filter_output.get("filter_final", {})
                    result.filter_time = time.time() - filter_start
                except Exception as e:
                    errors.append(f"Filter classifier error: {e}")
                    result.filter_time = time.time() - filter_start

            # Phase 1: Graphic Classifier
            classifier_start = time.time()
            try:
                classifier_output = self.graphic_agent.classify(query)
                result.classifier_result = classifier_output
                result.chart_type = classifier_output.get("chart_type")
                result.intent = classifier_output.get("intent")
                result.confidence = classifier_output.get("confidence", 0.0)

                # Merge filters if we have them
                if result.active_filters:
                    if "filters" not in classifier_output:
                        classifier_output["filters"] = {}
                    classifier_output["filters"].update(result.active_filters)

                result.classifier_time = time.time() - classifier_start
            except Exception as e:
                errors.append(f"Graphic classifier error: {e}")
                result.classifier_time = time.time() - classifier_start
                result.status = "error"

            # Phase 2: Analytics Executor (optional)
            if (
                self.config.enable_executor
                and self.executor_agent
                and result.classifier_result
            ):
                executor_start = time.time()
                try:
                    executor_output = self.executor_agent.execute(
                        chart_spec=result.classifier_result,
                        data_path=self.config.data_path,
                    )

                    result.executor_result = executor_output
                    result.data = executor_output.get("data", [])
                    result.row_count = len(result.data)
                    result.engine_used = executor_output.get("engine_used")
                    result.sql_query = executor_output.get("sql_query")
                    result.executor_time = time.time() - executor_start

                    # Check for errors in executor output
                    if executor_output.get("status") == "error":
                        result.status = "error"
                        if "error" in executor_output:
                            errors.append(
                                f"Executor error: {executor_output['error'].get('message', 'Unknown error')}"
                            )

                except Exception as e:
                    errors.append(f"Analytics executor error: {e}")
                    result.executor_time = time.time() - executor_start
                    result.status = "error"

            # Phase 3: Plotly Generator (optional)
            if (
                self.config.enable_plotly
                and self.plotly_agent
                and result.executor_result
            ):
                if result.executor_result.get("status") == "success":
                    plotly_start = time.time()
                    try:
                        plotly_output = self.plotly_agent.generate(
                            chart_spec=result.classifier_result,
                            analytics_result=result.executor_result,
                        )
                        result.plotly_result = plotly_output
                        result.plotly_time = time.time() - plotly_start

                        # Log chart generation
                        if plotly_output.get(
                            "status"
                        ) == "success" and plotly_output.get("file_path"):
                            self.console.print(
                                f"[green]✓ Chart saved to: {plotly_output['file_path']}[/green]"
                            )

                    except Exception as e:
                        errors.append(f"Plotly generation error: {e}")
                        result.plotly_time = time.time() - plotly_start
                        if self.config.debug_mode:
                            import traceback

                            self.console.print(traceback.format_exc())

            # Set final status
            if errors:
                result.status = "partial" if result.data else "error"
                result.errors = errors

            result.total_time = (
                result.filter_time
                + result.classifier_time
                + result.executor_time
                + result.plotly_time
            )

        except Exception as e:
            result.status = "error"
            result.errors.append(f"Pipeline error: {e}")

        return result

    def _update_statistics(self, result: PipelineResult):
        """
        Update session statistics.

        Args:
            result: PipelineResult to record
        """
        # Extract filter operations if available
        filter_ops = {}
        if result.filter_result:
            for op in ["ADICIONAR", "ALTERAR", "REMOVER", "MANTER"]:
                if op in result.filter_result:
                    filter_ops[op] = len(result.filter_result[op])

        # Record query
        self.statistics.record_query(
            success=result.is_success,
            total_time=result.total_time,
            filter_time=result.filter_time,
            classifier_time=result.classifier_time,
            executor_time=result.executor_time,
            plotly_time=result.plotly_time,
            chart_type=result.chart_type,
            engine=result.engine_used,
            filter_ops=filter_ops,
        )

        # Update active filters count
        self.statistics.update_active_filters(len(result.active_filters))

    def shutdown(self):
        """Shutdown session gracefully."""
        self.console.print("\n")
        self.display.show_info("Shutting down session...")

        # Show final statistics
        if self.statistics.total_queries > 0:
            self.console.print()
            table = self.statistics.to_table()
            self.console.print(table)

        self.console.print(
            "\n[bold green]Thank you for using the Interactive Pipeline![/bold green]"
        )

    def export_session(self, path: str):
        """
        Export session history to file.

        Args:
            path: File path for export
        """
        import json

        session_data = {
            "config": {
                "data_path": self.config.data_path,
                "use_integrated": self.config.use_integrated,
                "enable_filter_classifier": self.config.enable_filter_classifier,
                "enable_executor": self.config.enable_executor,
            },
            "statistics": self.statistics.to_dict(),
            "history": [result.to_dict() for result in self.query_history],
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(session_data, f, indent=2, ensure_ascii=False, default=str)

        self.display.show_success(f"Session exported to {path}")


def main():
    """
    Main entry point for interactive pipeline session.

    Can be called directly or imported.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Interactive Multi-Agent Pipeline Session"
    )
    parser.add_argument("--data", "-d", help="Path to dataset")
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Use sequential mode (default: integrated)",
    )
    parser.add_argument(
        "--no-filters", action="store_true", help="Disable filter classifier"
    )
    parser.add_argument(
        "--no-executor", action="store_true", help="Disable analytics executor"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    # Create and run session
    session = InteractivePipelineSession(
        data_path=args.data,
        use_integrated=not args.sequential,
        enable_filter_classifier=not args.no_filters,
        enable_executor=not args.no_executor,
        verbose=args.verbose,
    )

    session.run()


if __name__ == "__main__":
    main()
