"""
Formatter Agent - Phase 4
=========================

The formatter agent is responsible for consolidating outputs from all previous agents
and generating a structured, API-first JSON output with:
- Executive summary (title + introduction) via LLM
- Synthesized insights narrative via LLM
- Strategic next steps recommendations via LLM
- Formatted data tables and visualizations
- Complete metadata

Version: 1.0.0
"""

from .agent import run_formatter

__all__ = ["run_formatter"]
__version__ = "1.0.0"
