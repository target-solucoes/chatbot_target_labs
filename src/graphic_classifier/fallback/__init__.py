"""
Fallback System for Graph Classifier
Implements intelligent degradation and routing when visualization fails.
"""

from .fallback_manager import FallbackManager
from .message_generator import NullMessageGenerator

__all__ = ["FallbackManager", "NullMessageGenerator"]
