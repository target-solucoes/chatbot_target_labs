"""
MetricComposer - Intent-based Metric Composition System.

FASE 2 Implementation - Metric Composer

This module implements the core composition logic that selects and combines
metric modules based on enriched intent rather than chart type.

Key Principles:
    1. Intent-driven: base_intent determines which modules to use
    2. Composable: Multiple modules can be combined for complex analysis
    3. Extensible: New modules can be added to MODULE_REGISTRY
    4. Pure computation: No text generation, no LLM calls

Architecture:
    MODULE_REGISTRY: Maps module names to classes
    INTENT_MODULE_MAPPING: Maps intents to required modules
    MetricComposer: Orchestrates module selection and execution
"""

from typing import Dict, Any, List, Type, Set
import pandas as pd
import logging

from .metric_modules import (
    MetricModule,
    VariationModule,
    ConcentrationModule,
    GapModule,
    TemporalModule,
    DistributionModule,
    ComparativeModule,
)
from ..core.intent_enricher import EnrichedIntent, Polarity, TemporalFocus

logger = logging.getLogger(__name__)


# ============================================================================
# MODULE REGISTRY - Central registry of all available metric modules
# ============================================================================

MODULE_REGISTRY: Dict[str, Type[MetricModule]] = {
    "variation": VariationModule,
    "concentration": ConcentrationModule,
    "gap": GapModule,
    "temporal": TemporalModule,
    "distribution": DistributionModule,
    "comparative": ComparativeModule,
}


# ============================================================================
# INTENT MODULE MAPPING - Defines which modules are used for each intent
# ============================================================================

INTENT_MODULE_MAPPING: Dict[str, Set[str]] = {
    # Ranking analysis: concentration + gap + comparative
    "ranking": {"concentration", "gap", "comparative"},
    # Variation analysis: variation + gap (for before/after comparison)
    "variation": {"variation", "gap", "comparative"},
    # Trend analysis: temporal + variation (for momentum)
    "trend": {"temporal", "variation"},
    # Comparison: comparative + gap + distribution
    "comparison": {"comparative", "gap", "distribution"},
    # Composition: concentration + distribution
    "composition": {"concentration", "distribution"},
    # Distribution: distribution + comparative
    "distribution": {"distribution", "comparative"},
    # Temporal (explicit): temporal + variation
    "temporal": {"temporal", "variation"},
}


# ============================================================================
# POLARITY ENRICHMENT - Additional modules based on polarity
# ============================================================================

POLARITY_ADDITIONAL_MODULES: Dict[Polarity, Set[str]] = {
    Polarity.POSITIVE: {"gap"},  # Emphasize growth gaps
    Polarity.NEGATIVE: {"gap", "comparative"},  # Emphasize decline impact
    Polarity.NEUTRAL: set(),  # No additional modules
}


# ============================================================================
# TEMPORAL FOCUS ENRICHMENT - Additional modules based on temporal focus
# ============================================================================

TEMPORAL_FOCUS_MODULES: Dict[TemporalFocus, Set[str]] = {
    TemporalFocus.SINGLE_PERIOD: set(),
    TemporalFocus.PERIOD_OVER_PERIOD: {"variation", "gap"},
    TemporalFocus.TIME_SERIES: {"temporal", "variation"},
    TemporalFocus.SEASONALITY: {"temporal", "distribution"},
}


# ============================================================================
# MetricComposer - Main composition orchestrator
# ============================================================================


class MetricComposer:
    """
    Composes metrics based on enriched intent.

    This class is the central orchestrator that:
    1. Receives EnrichedIntent from Phase 1
    2. Selects appropriate metric modules
    3. Executes modules with proper configuration
    4. Aggregates results into unified metrics dictionary

    Usage:
        composer = MetricComposer()
        metrics = composer.compose(df, enriched_intent, config)
    """

    def __init__(self):
        """Initialize composer with module registry."""
        self.module_registry = MODULE_REGISTRY
        self.intent_mapping = INTENT_MODULE_MAPPING

    def compose(
        self, df: pd.DataFrame, enriched_intent: EnrichedIntent, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Main composition method - selects and executes metric modules.

        Args:
            df: DataFrame with processed data from analytics_executor
            enriched_intent: EnrichedIntent from Phase 1
            config: Configuration with column mappings and context

        Returns:
            Unified dictionary with all calculated metrics:
            {
                "modules_used": List[str],
                "variation": {...},      # From VariationModule
                "concentration": {...},  # From ConcentrationModule
                "gap": {...},           # From GapModule
                "temporal": {...},      # From TemporalModule
                "distribution": {...},  # From DistributionModule
                "comparative": {...},   # From ComparativeModule
                "metadata": {
                    "intent": str,
                    "polarity": str,
                    "temporal_focus": str,
                    "modules_count": int
                }
            }
        """
        result = {
            "modules_used": [],
            "metadata": {
                "intent": enriched_intent.base_intent,
                "polarity": enriched_intent.polarity.value,
                "temporal_focus": enriched_intent.temporal_focus.value,
                "modules_count": 0,
            },
        }

        try:
            # Step 1: Select modules based on intent
            selected_modules = self._select_modules(enriched_intent)

            logger.info(
                f"MetricComposer: Selected {len(selected_modules)} modules for intent '{enriched_intent.base_intent}': {selected_modules}"
            )

            # Step 2: Execute each module
            for module_name in selected_modules:
                module_result = self._execute_module(module_name, df, config)
                result[module_name] = module_result
                result["modules_used"].append(module_name)

            result["metadata"]["modules_count"] = len(selected_modules)

            logger.info(
                f"MetricComposer: Successfully composed metrics using {len(selected_modules)} modules"
            )

        except Exception as e:
            logger.error(f"Error in MetricComposer.compose: {e}", exc_info=True)

        return result

    def _select_modules(self, enriched_intent: EnrichedIntent) -> Set[str]:
        """
        Select metric modules based on enriched intent.

        Selection logic:
        1. Start with base modules from INTENT_MODULE_MAPPING
        2. Add modules based on polarity
        3. Add modules based on temporal_focus
        4. Add explicitly suggested modules from enriched_intent

        Args:
            enriched_intent: EnrichedIntent with semantic metadata

        Returns:
            Set of module names to execute
        """
        selected = set()

        # Base modules from intent
        base_intent = enriched_intent.base_intent.lower()
        if base_intent in self.intent_mapping:
            selected.update(self.intent_mapping[base_intent])
        else:
            # Default fallback for unknown intents
            logger.warning(f"Unknown intent '{base_intent}', using default modules")
            selected.update({"comparative", "distribution"})

        # Add modules based on polarity
        polarity_modules = POLARITY_ADDITIONAL_MODULES.get(
            enriched_intent.polarity, set()
        )
        selected.update(polarity_modules)

        # Add modules based on temporal focus
        temporal_modules = TEMPORAL_FOCUS_MODULES.get(
            enriched_intent.temporal_focus, set()
        )
        selected.update(temporal_modules)

        # Add modules from suggested_metrics (if they map to module names)
        for suggested in enriched_intent.suggested_metrics:
            # Map metric names to modules
            if "variation" in suggested.lower() or "delta" in suggested.lower():
                selected.add("variation")
            if "concentration" in suggested.lower() or "hhi" in suggested.lower():
                selected.add("concentration")
            if "gap" in suggested.lower():
                selected.add("gap")
            if "trend" in suggested.lower() or "temporal" in suggested.lower():
                selected.add("temporal")
            if "distribution" in suggested.lower() or "percentile" in suggested.lower():
                selected.add("distribution")

        # Ensure all selected modules exist in registry
        valid_modules = {m for m in selected if m in self.module_registry}
        invalid_modules = selected - valid_modules

        if invalid_modules:
            logger.warning(f"Invalid modules filtered out: {invalid_modules}")

        return valid_modules

    def _execute_module(
        self, module_name: str, df: pd.DataFrame, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute a single metric module.

        Args:
            module_name: Name of module from MODULE_REGISTRY
            df: DataFrame with data
            config: Configuration dictionary

        Returns:
            Dictionary with module-specific metrics
        """
        try:
            # Get module class from registry
            module_class = self.module_registry.get(module_name)
            if not module_class:
                logger.error(f"Module '{module_name}' not found in registry")
                return {}

            # Instantiate and execute
            module_instance = module_class()
            result = module_instance.calculate(df, config)

            logger.debug(f"Module '{module_name}' executed successfully")
            return result

        except Exception as e:
            logger.error(f"Error executing module '{module_name}': {e}", exc_info=True)
            return {}

    def get_available_modules(self) -> List[str]:
        """Return list of all available module names."""
        return list(self.module_registry.keys())

    def is_module_available(self, module_name: str) -> bool:
        """Check if a module is available in registry."""
        return module_name in self.module_registry


# ============================================================================
# Convenience Functions
# ============================================================================


def compose_metrics(
    df: pd.DataFrame, enriched_intent: EnrichedIntent, config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Convenience function for metric composition.

    Args:
        df: DataFrame with processed data
        enriched_intent: EnrichedIntent from Phase 1
        config: Configuration dictionary

    Returns:
        Composed metrics dictionary
    """
    composer = MetricComposer()
    return composer.compose(df, enriched_intent, config)


def get_modules_for_intent(intent: str) -> Set[str]:
    """
    Get the list of modules that would be used for a given intent.

    Useful for debugging and documentation.

    Args:
        intent: Base intent string (e.g., "ranking", "variation")

    Returns:
        Set of module names
    """
    return INTENT_MODULE_MAPPING.get(intent.lower(), set())


# ============================================================================
# Module Registry Management (for future extensions)
# ============================================================================


def register_module(name: str, module_class: Type[MetricModule]) -> None:
    """
    Register a new metric module dynamically.

    Args:
        name: Module name (e.g., "custom_metric")
        module_class: Class inheriting from MetricModule
    """
    if not issubclass(module_class, MetricModule):
        raise ValueError(f"Module class must inherit from MetricModule")

    MODULE_REGISTRY[name] = module_class
    logger.info(f"Registered new module: {name}")


def unregister_module(name: str) -> None:
    """
    Unregister a metric module.

    Args:
        name: Module name to remove
    """
    if name in MODULE_REGISTRY:
        del MODULE_REGISTRY[name]
        logger.info(f"Unregistered module: {name}")
