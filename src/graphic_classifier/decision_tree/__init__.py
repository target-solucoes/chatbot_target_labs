"""
Decision Tree Classification System for Chart Type Detection.

This module implements FASE 3 of the disambiguation improvement strategy,
providing a three-level decision tree that reduces dependency on LLM calls
and increases deterministic classification accuracy.

Architecture:
- Level 1 (Detection): High-confidence pattern matching (0.90-0.95)
- Level 2 (Context Analysis): Context-based disambiguation (0.75-0.90)
- Level 3 (Fallback): LLM-based classification for ambiguous cases

Expected coverage:
- Level 1: 40-50% of queries
- Level 2: 30-40% of queries
- Level 3 (LLM): 10-30% of queries

Reference: graph_classifier_diagnosis.md - FASE 3
"""

from src.graphic_classifier.decision_tree.classifier import DecisionTreeClassifier
from src.graphic_classifier.decision_tree.level1_detection import Level1Detector
from src.graphic_classifier.decision_tree.level2_context import Level2Analyzer

__all__ = [
    "DecisionTreeClassifier",
    "Level1Detector",
    "Level2Analyzer",
]
