"""
Pipeline Session Module

Interactive session management for the complete multi-agent pipeline.
Provides a CLI interface with Rich display for testing and validation.
"""

from src.pipeline_session.session import InteractivePipelineSession
from src.pipeline_session.result import PipelineResult
from src.pipeline_session.statistics import SessionStatistics

__all__ = [
    'InteractivePipelineSession',
    'PipelineResult',
    'SessionStatistics',
]
