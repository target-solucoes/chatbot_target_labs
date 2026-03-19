"""
DataTableFormatter - Formats data tables in markdown and HTML
===============================================================

Responsible for:
- Converting raw data into formatted tables
- Generating both markdown and HTML representations
- Limiting rows for display (with full count tracking)
- Formatting cells with appropriate number formatting
- Handling empty data gracefully
"""

import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class DataTableFormatter:
    """
    Formats data tables into markdown and HTML representations.

    This formatter takes raw data from the analytics executor and creates
    human-readable table formats suitable for reports, documentation,
    and web display.

    Features:
    - Smart cell formatting (numbers, dates, strings)
    - Row limiting with total count tracking
    - Graceful empty data handling
    - Both markdown and HTML output
    """

    def format(self, data: List[Dict[str, Any]], max_rows: int = 10) -> Dict[str, Any]:
        """
        Format data into table representations.

        Args:
            data: List of dictionaries representing data rows
            max_rows: Maximum number of rows to display (default: 10)

        Returns:
            Dictionary containing:
            {
                "markdown": str,       # Markdown table representation
                "html": str,           # HTML table representation
                "headers": List[str],  # Column headers
                "rows": List[List],    # Formatted data rows
                "total_rows": int,     # Total number of rows in data
                "showing_rows": int    # Number of rows being displayed
            }
        """
        if not data:
            logger.info("No data provided to DataTableFormatter, returning empty table")
            return self._empty_table()

        # Extract headers from first row
        headers = list(data[0].keys())
        logger.debug(f"Extracted {len(headers)} headers: {headers}")

        # Limit rows for display
        limited_data = data[:max_rows]
        rows = [[row.get(h) for h in headers] for row in limited_data]

        # Generate markdown representation
        markdown = self._generate_markdown(headers, rows)

        # Generate HTML representation
        html = self._generate_html(headers, rows)

        result = {
            "markdown": markdown,
            "html": html,
            "headers": headers,
            "rows": rows,
            "total_rows": len(data),
            "showing_rows": len(limited_data),
        }

        logger.info(
            f"Formatted table with {result['showing_rows']}/{result['total_rows']} rows, "
            f"{len(headers)} columns"
        )

        return result

    def _generate_markdown(self, headers: List[str], rows: List[List[Any]]) -> str:
        """
        Generate markdown table representation.

        Args:
            headers: List of column header names
            rows: List of data rows

        Returns:
            Formatted markdown table string
        """
        lines = []

        # Header row
        header_row = "| " + " | ".join(headers) + " |"
        lines.append(header_row)

        # Separator row
        separator = "|" + "|".join(["---"] * len(headers)) + "|"
        lines.append(separator)

        # Data rows
        for row in rows:
            formatted_row = [self._format_cell(cell) for cell in row]
            data_row = "| " + " | ".join(formatted_row) + " |"
            lines.append(data_row)

        return "\n".join(lines)

    def _generate_html(self, headers: List[str], rows: List[List[Any]]) -> str:
        """
        Generate HTML table representation.

        Args:
            headers: List of column header names
            rows: List of data rows

        Returns:
            Formatted HTML table string
        """
        html = ['<table class="data-table">']

        # Header
        html.append("  <thead>")
        html.append("    <tr>")
        for h in headers:
            html.append(f"      <th>{self._escape_html(str(h))}</th>")
        html.append("    </tr>")
        html.append("  </thead>")

        # Body
        html.append("  <tbody>")
        for row in rows:
            html.append("    <tr>")
            for cell in row:
                formatted_cell = self._escape_html(self._format_cell(cell))
                html.append(f"      <td>{formatted_cell}</td>")
            html.append("    </tr>")
        html.append("  </tbody>")

        html.append("</table>")
        return "\n".join(html)

    def _format_cell(self, value: Any) -> str:
        """
        Format individual cell value with appropriate formatting.

        Args:
            value: Cell value to format

        Returns:
            Formatted string representation
        """
        if value is None:
            return ""
        elif isinstance(value, float):
            # Format floats with 2 decimal places and thousand separators
            return f"{value:,.2f}"
        elif isinstance(value, int):
            # Format integers with thousand separators
            return f"{value:,}"
        else:
            # Convert to string for other types
            return str(value)

    def _escape_html(self, text: str) -> str:
        """
        Escape HTML special characters to prevent XSS.

        Args:
            text: Text to escape

        Returns:
            HTML-safe string
        """
        if not text:
            return ""

        escape_map = {
            "&": "&amp;",
            "<": "&lt;",
            ">": "&gt;",
            '"': "&quot;",
            "'": "&#39;",
        }

        for char, escaped in escape_map.items():
            text = text.replace(char, escaped)

        return text

    def _empty_table(self) -> Dict[str, Any]:
        """
        Return empty table structure when no data is available.

        Returns:
            Dictionary with empty table representations
        """
        return {
            "markdown": "*Nenhum dado disponível*",
            "html": "<p>Nenhum dado disponível</p>",
            "headers": [],
            "rows": [],
            "total_rows": 0,
            "showing_rows": 0,
        }
