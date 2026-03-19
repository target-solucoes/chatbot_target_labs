# -*- coding: utf-8 -*-
"""
Progressive Display Manager for Streamlit Chatbot

Handles real-time progressive rendering of pipeline outputs as they become available.
Supports both formatter_output (graphical) and non_graph_output (textual) responses.
"""

import streamlit as st
from typing import Dict, Any
from dataclasses import dataclass
from streamlit_app.display_components import (
    render_executive_summary,
    render_plotly_chart,
    render_insights,
    render_next_steps,
    render_data_table,
    render_error,
    render_non_graph_response,
)


@dataclass
class ProgressiveContainers:
    """Container for all pre-allocated Streamlit placeholders"""

    status_container: Any = None
    title_container: Any = None
    filters_container: Any = None
    chart_container: Any = None
    processing_spinner_container: Any = None  # NEW: spinner after chart
    insights_container: Any = None
    next_steps_container: Any = None
    data_container: Any = None


class ProgressiveRenderer:
    """
    Manages progressive rendering of pipeline outputs

    Pre-allocates containers in the correct order and updates them
    as data becomes available from the streaming pipeline.
    """

    def __init__(self, parent_container=None):
        """
        Initialize progressive renderer

        Args:
            parent_container: Optional Streamlit container to render within
        """
        self.container = parent_container if parent_container else st
        self.containers = ProgressiveContainers()
        self._sections_rendered = {
            "status": False,
            "title": False,
            "filters": False,
            "chart": False,
            "insights": False,
            "next_steps": False,
            "data": False,
        }
        self._spinner_active = False
        self._current_spinner_message = None
        self._allocate_containers()

    def _allocate_containers(self) -> None:
        """Pre-allocate all containers in correct order"""
        # Status/Loading indicator
        self.containers.status_container = self.container.empty()

        # Title and introduction
        self.containers.title_container = self.container.empty()

        # Filters
        self.containers.filters_container = self.container.empty()

        # Chart
        self.containers.chart_container = self.container.empty()

        # Processing spinner (appears after chart while generating rest)
        self.containers.processing_spinner_container = self.container.empty()

        # Insights
        self.containers.insights_container = self.container.empty()

        # Next steps
        self.containers.next_steps_container = self.container.empty()

        # Data table
        self.containers.data_container = self.container.empty()

        # Metadata/Debug container removed per user request

    def update_status(self, message: str, is_complete: bool = False) -> None:
        """
        Update status message

        Args:
            message: Status message to display
            is_complete: Whether processing is complete
        """
        with self.containers.status_container:
            if is_complete:
                st.success(f"{message}")
            else:
                with st.spinner(message):
                    st.empty()
        self._sections_rendered["status"] = True

    def update_title(self, executive_summary: Dict) -> None:
        """
        Update title and introduction section

        Args:
            executive_summary: Executive summary dict from formatter
        """
        with self.containers.title_container.container():
            render_executive_summary(executive_summary)
        self._sections_rendered["title"] = True

    def update_filters(self, filters: Dict[str, Any]) -> None:
        """
        Update filters section

        Args:
            filters: Active filters dict
        """
        # Filters rendering removed per user request
        return

    def update_chart(self, chart_data: Dict) -> None:
        """
        Update chart section

        Args:
            chart_data: Chart dict from visualization
        """
        with self.containers.chart_container.container():
            st.markdown("")
            render_plotly_chart(chart_data)
        self._sections_rendered["chart"] = True

        # Clear chart generation spinner and show insights generation spinner
        self.show_processing_spinner("✨ Analisando dados e gerando insights...")

    def update_insights(
        self,
        insights: Dict,
        resposta: str = "",
        dados_destacados: list = None,
    ) -> None:
        """
        Update insights section

        Args:
            insights: Insights dict from formatter
            resposta: FASE 2 native response text
            dados_destacados: FASE 2 native key findings list
        """
        with self.containers.insights_container.container():
            render_insights(
                insights,
                resposta=resposta,
                dados_destacados=dados_destacados,
            )
        self._sections_rendered["insights"] = True

    def update_next_steps(self, next_steps: Dict) -> None:
        """
        Update next steps section

        Args:
            next_steps: Next steps dict from formatter
        """
        with self.containers.next_steps_container.container():
            render_next_steps(next_steps)
        self._sections_rendered["next_steps"] = True

    def update_data_table(self, data: Dict) -> None:
        """
        Update data table section

        Args:
            data: Data dict from formatter
        """
        with self.containers.data_container.container():
            render_data_table(data)
        self._sections_rendered["data"] = True

    def render_error(self, error_msg: str) -> None:
        """
        Render error in status container

        Args:
            error_msg: Error message to display
        """
        with self.containers.status_container:
            render_error(error_msg)
        self._sections_rendered["status"] = True

    def show_processing_spinner(self, message: str) -> None:
        """
        Show processing spinner with animated message

        Args:
            message: Message to display with spinner
        """
        # Only update if message changed to avoid flickering
        if self._current_spinner_message != message:
            self._current_spinner_message = message
            self._spinner_active = True
            # Clear previous content
            self.containers.processing_spinner_container.empty()
            # Show new spinner message with custom styling
            with self.containers.processing_spinner_container:
                st.markdown(
                    f'<div style="padding: 15px; background: linear-gradient(135deg, #fff3e0 0%, #ffe0b2 100%); '
                    f"border-left: 4px solid #ff9800; border-radius: 8px; margin: 10px 0; "
                    f"box-shadow: 0 2px 8px rgba(0,0,0,0.1); "
                    f'animation: pulse 2s ease-in-out infinite;">'
                    f'<span style="font-size: 1.1em; color: #e65100; font-weight: 500;">'
                    f"⏳ {message}</span></div>"
                    f"<style>@keyframes pulse {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.85; }} }}</style>",
                    unsafe_allow_html=True,
                )

    def clear_processing_spinner(self) -> None:
        """Clear the processing spinner container"""
        if self._spinner_active:
            self.containers.processing_spinner_container.empty()
            self._spinner_active = False
            self._current_spinner_message = None

    def clear_status(self) -> None:
        """Clear the status container"""
        self.containers.status_container.empty()

    def is_section_rendered(self, section: str) -> bool:
        """
        Check if a section has been rendered

        Args:
            section: Section name to check

        Returns:
            True if section has been rendered
        """
        return self._sections_rendered.get(section, False)

    def get_rendered_sections(self) -> Dict[str, bool]:
        """
        Get dict of all sections and their render status

        Returns:
            Dict mapping section names to render status
        """
        return self._sections_rendered.copy()


class StreamingDisplayManager:
    """
    High-level manager for streaming display updates

    Coordinates progressive rendering based on pipeline state updates.
    """

    def __init__(self, renderer: ProgressiveRenderer):
        """
        Initialize streaming display manager

        Args:
            renderer: ProgressiveRenderer instance
        """
        self.renderer = renderer
        self._last_state = {}
        self._processing_started = False

    def process_pipeline_state(self, state: Dict[str, Any]) -> None:
        """
        Process pipeline state and update relevant sections

        Handles both formatter_output and non_graph_output responses.

        Args:
            state: Current pipeline state from workflow.stream()
        """
        # Show initial spinner when processing first starts
        if not self._processing_started and state:
            self._processing_started = True
            self.renderer.show_processing_spinner("🔍 Analisando requisição...")

        # Check for non_graph_output (textual response path)
        if "non_graph_output" in state and state["non_graph_output"]:
            self._process_non_graph_output(state["non_graph_output"])
            return  # Non-graph path ends here

        # Check for filter classifier output
        # Filters rendering removed per user request
        # if "filter_final" in state and not self.renderer.is_section_rendered("filters"):
        #     filters = state.get("filter_final", {})
        #     if filters:
        #         self.renderer.update_status("Filtros detectados...", is_complete=False)
        #         self.renderer.update_filters(filters)

        # Check for graphic classifier output
        if "classifier_output" in state and state["classifier_output"]:
            if not self.renderer.is_section_rendered("chart"):
                classifier = state["classifier_output"]
                chart_type = classifier.get("chart_type", "gráfico")
                # Show spinner for chart generation
                self.renderer.show_processing_spinner(f"📊 Gerando {chart_type}...")

        # Check for executor output
        if "executor_output" in state and state["executor_output"]:
            if not self.renderer.is_section_rendered("chart"):
                executor = state["executor_output"]
                data_rows = executor.get("data", [])
                if data_rows:
                    # Update spinner message during data processing
                    self.renderer.show_processing_spinner(
                        f"📈 Processando {len(data_rows)} registros..."
                    )

        # Check for plotly output
        if "plotly_output" in state and state["plotly_output"]:
            plotly_data = state["plotly_output"]
            chart_html = plotly_data.get("html")
            if chart_html and not self.renderer.is_section_rendered("chart"):
                # Update spinner for rendering phase
                self.renderer.show_processing_spinner("🎨 Renderizando visualização...")
                chart_data = {
                    "html": chart_html,
                    "caption": plotly_data.get("description", ""),
                }
                self.renderer.update_chart(chart_data)

        # Check for insight output
        if "insight_result" in state and state["insight_result"]:
            if not self.renderer.is_section_rendered("insights"):
                # Update processing spinner with current stage
                if self.renderer.is_section_rendered("chart"):
                    self.renderer.show_processing_spinner(
                        "✨ Analisando dados e gerando insights..."
                    )
                else:
                    self.renderer.update_status(
                        "Gerando insights...", is_complete=False
                    )

        # Check for formatter output (final)
        if "formatter_output" in state and state["formatter_output"]:
            self._process_formatter_output(state["formatter_output"])

        self._last_state = state

    def _process_non_graph_output(self, non_graph_output: Dict) -> None:
        """
        Process non-graph executor output and display summary

        For non_graph_output, we show only the summary field in a clean format.

        Args:
            non_graph_output: Complete non_graph JSON output
        """
        if non_graph_output.get("status") == "error":
            error = non_graph_output.get("error", "Erro desconhecido")
            self.renderer.render_error(str(error))
            return

        # Clear processing spinner
        self.renderer.clear_processing_spinner()

        # Render the non-graph response (summary only)
        with self.renderer.containers.title_container.container():
            render_non_graph_response(non_graph_output)

        # Mark completion
        metadata = non_graph_output.get("metadata", {})
        total_time = metadata.get("total_execution_time", 0)
        self.renderer.update_status(
            f"✅ Concluído em {total_time:.2f}s", is_complete=True
        )

        # Clear status after showing success briefly
        import time

        time.sleep(0.5)
        self.renderer.clear_status()

    def _process_formatter_output(self, formatter_output: Dict) -> None:
        """
        Process complete formatter output and update all sections

        Args:
            formatter_output: Complete formatter JSON output
        """
        if formatter_output.get("status") == "error":
            error = formatter_output.get("error", "Erro desconhecido")
            self.renderer.render_error(str(error))
            return

        # Update all sections from formatter output
        executive_summary = formatter_output.get("executive_summary", {})
        if executive_summary and not self.renderer.is_section_rendered("title"):
            self.renderer.update_title(executive_summary)

            # Filters rendering removed per user request
            # filters_applied = executive_summary.get("filters_applied", {})
            # if filters_applied and not self.renderer.is_section_rendered("filters"):
            #     self.renderer.update_filters(filters_applied)

        # Update chart if not already rendered from plotly_output
        if not self.renderer.is_section_rendered("chart"):
            visualization = formatter_output.get("visualization", {})
            chart = visualization.get("chart", {})
            if chart:
                self.renderer.update_chart(chart)

        # Update insights
        insights = formatter_output.get("insights", {})
        resposta = formatter_output.get("resposta", "")
        dados_destacados = formatter_output.get("dados_destacados", [])
        has_insights_content = insights or resposta
        if has_insights_content and not self.renderer.is_section_rendered("insights"):
            # Update spinner message for insights rendering
            if self.renderer.is_section_rendered("chart"):
                self.renderer.show_processing_spinner("📝 Formatando insights...")
            # FASE 5B: Pass native fields for direct rendering
            self.renderer.update_insights(
                insights,
                resposta=resposta,
                dados_destacados=dados_destacados,
            )

        # Update next steps
        next_steps = formatter_output.get("next_steps", {})
        if next_steps and not self.renderer.is_section_rendered("next_steps"):
            # Update spinner message for next steps
            if self.renderer.is_section_rendered("chart"):
                self.renderer.show_processing_spinner(
                    "🎯 Preparando próximos passos..."
                )
            self.renderer.update_next_steps(next_steps)

        # Update data table
        data = formatter_output.get("data", {})
        if data and not self.renderer.is_section_rendered("data"):
            # Update spinner message for data table
            if self.renderer.is_section_rendered("chart"):
                self.renderer.show_processing_spinner(
                    "📊 Organizando tabela de dados..."
                )
            self.renderer.update_data_table(data)

        # Metadata rendering removed per user request

        # Clear processing spinner when complete
        self.renderer.clear_processing_spinner()

        # Mark as complete
        metadata = formatter_output.get("metadata", {})
        total_time = metadata.get("total_execution_time", 0)
        self.renderer.update_status(
            f"✅ Concluído em {total_time:.2f}s", is_complete=True
        )

        # Clear status after showing success briefly
        import time

        time.sleep(0.5)
        self.renderer.clear_status()

    def get_last_state(self) -> Dict[str, Any]:
        """
        Get the last processed pipeline state

        Returns:
            Last pipeline state dict
        """
        return self._last_state.copy()
