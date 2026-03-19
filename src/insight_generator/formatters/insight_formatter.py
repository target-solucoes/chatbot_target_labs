"""
Insight formatter for structuring final output.

This module formats raw insights into the structured output format.
"""

from typing import Dict, Any, List
from datetime import datetime


class InsightFormatter:
    """Formats insights into structured output."""

    @staticmethod
    def format_output(
        insights: List[Dict[str, Any]],
        chart_type: str,
        calculation_time: float,
        llm_model: str,
        transparency_validated: bool,
        status: str = "success",
        error: str = None,
    ) -> Dict[str, Any]:
        """
        Formats insights into final output structure.

        Args:
            insights: List of insight dictionaries
            chart_type: Type of chart
            calculation_time: Time taken for calculations
            llm_model: Model used for generation
            transparency_validated: Whether transparency validation passed
            status: Status of generation (success/error)
            error: Error message if any

        Returns:
            Formatted output dictionary
        """
        return {
            "status": status,
            "chart_type": chart_type,
            "insights": insights,
            "metadata": {
                "calculation_time": calculation_time,
                "metrics_count": len(insights),
                "llm_model": llm_model,
                "timestamp": datetime.now().isoformat(),
                "transparency_validated": transparency_validated,
            },
            "error": error,
        }

    @staticmethod
    def parse_llm_response(
        llm_response: str, chart_type: str, metrics: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Parses LLM response into structured insights.

        Args:
            llm_response: Raw response from LLM
            chart_type: Type of chart for context
            metrics: Metrics used in calculation

        Returns:
            List of structured insight dictionaries
        """
        insights = []

        # Split by lines and process
        lines = llm_response.strip().split("\n")
        current_insight = None

        for line in lines:
            line = line.strip()

            # Skip empty lines
            if not line:
                continue

            # Detect title (bold markdown or numbered bullet)
            if (
                line.startswith("**")
                or line.startswith("-")
                or line.startswith(str(len(insights) + 1))
            ):
                # Save previous insight
                if current_insight:
                    insights.append(current_insight)

                # Extract title and content
                if "**" in line:
                    # Format: **Title:** Content or - **Title:** Content
                    parts = line.split("**")
                    if len(parts) >= 3:
                        title = parts[1].strip().rstrip(":")
                        content = "**".join(parts[2:]).strip()

                        current_insight = {
                            "title": title,
                            "content": content if content else title,
                            "metrics": metrics,
                            "confidence": 0.8,
                            "chart_context": chart_type,
                        }
                else:
                    # Simple bullet or numbered
                    current_insight = {
                        "title": "Insight",
                        "content": line.lstrip("-")
                        .lstrip("0123456789")
                        .lstrip(".")
                        .strip(),
                        "metrics": metrics,
                        "confidence": 0.8,
                        "chart_context": chart_type,
                    }
            elif current_insight:
                # Continuation of current insight
                current_insight["content"] += " " + line

        # Add last insight
        if current_insight:
            insights.append(current_insight)

        # Limit to 5 insights
        return insights[:5]
