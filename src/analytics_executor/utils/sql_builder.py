"""
SQL Builder utility for safe SQL query construction.

This module provides utilities for:
- Escaping SQL identifiers (column names, table names)
- Formatting SQL values (strings, numbers, dates, None)
- Building safe SQL queries without injection vulnerabilities
"""

from typing import Any
from datetime import date, datetime
import logging

logger = logging.getLogger(__name__)


class SQLBuilder:
    """
    Utility class for building safe SQL queries.

    Provides methods to escape identifiers and format values properly
    to prevent SQL injection and ensure correct syntax.
    """

    @staticmethod
    def escape_identifier(identifier: str) -> str:
        """
        Escape SQL identifier (column name, table name, alias).

        Wraps identifier in double quotes and escapes any internal quotes.
        This is the standard way to handle identifiers in SQL that may
        contain special characters or reserved words.

        Args:
            identifier: Column name, table name, or alias to escape

        Returns:
            str: Escaped identifier wrapped in double quotes

        Examples:
            >>> SQLBuilder.escape_identifier("sales_amount")
            '"sales_amount"'

            >>> SQLBuilder.escape_identifier("customer name")
            '"customer name"'

            >>> SQLBuilder.escape_identifier('product"type')
            '"product""type"'
        """
        if not identifier:
            raise ValueError("Identifier cannot be empty")

        # Escape internal double quotes by doubling them
        escaped = identifier.replace('"', '""')

        # Wrap in double quotes
        return f'"{escaped}"'

    @staticmethod
    def format_value(value: Any) -> str:
        """
        Format a Python value as a SQL literal.

        Handles different data types appropriately:
        - None → NULL
        - Strings → 'escaped string'
        - Numbers → raw number
        - Booleans → TRUE/FALSE
        - Dates/Datetimes → 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'

        Args:
            value: Python value to format

        Returns:
            str: SQL-safe formatted value

        Examples:
            >>> SQLBuilder.format_value(None)
            'NULL'

            >>> SQLBuilder.format_value("O'Brien")
            "'O''Brien'"

            >>> SQLBuilder.format_value(42)
            '42'

            >>> SQLBuilder.format_value(True)
            'TRUE'

            >>> SQLBuilder.format_value(date(2025, 1, 15))
            "'2025-01-15'"
        """
        # NULL handling
        if value is None:
            return "NULL"

        # Boolean handling (before numeric, as bool is subclass of int)
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"

        # Numeric handling (int, float)
        if isinstance(value, (int, float)):
            return str(value)

        # Date/Datetime handling
        if isinstance(value, datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"

        if isinstance(value, date):
            return f"'{value.strftime('%Y-%m-%d')}'"

        # String handling (default)
        # Escape single quotes by doubling them
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"

        # Fallback: convert to string and treat as string
        logger.warning(
            f"Unknown type {type(value)} for value {value}, converting to string"
        )
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    @staticmethod
    def build_in_clause(column: str, values: list) -> str:
        """
        Build an IN clause for SQL WHERE conditions.

        Args:
            column: Column name (will be escaped)
            values: List of values to include in IN clause

        Returns:
            str: SQL IN clause

        Examples:
            >>> SQLBuilder.build_in_clause("status", ["active", "pending"])
            '"status" IN (\'active\', \'pending\')'

            >>> SQLBuilder.build_in_clause("id", [1, 2, 3])
            '"id" IN (1, 2, 3)'
        """
        if not values:
            raise ValueError("IN clause requires at least one value")

        escaped_col = SQLBuilder.escape_identifier(column)
        formatted_values = ", ".join(SQLBuilder.format_value(v) for v in values)

        return f"{escaped_col} IN ({formatted_values})"

    @staticmethod
    def build_between_clause(column: str, start_value: Any, end_value: Any) -> str:
        """
        Build a BETWEEN clause for SQL WHERE conditions.

        Used for range queries, especially temporal ranges.
        BETWEEN is inclusive on both ends.

        Args:
            column: Column name (will be escaped)
            start_value: Start of range (inclusive)
            end_value: End of range (inclusive)

        Returns:
            str: SQL BETWEEN clause

        Examples:
            >>> SQLBuilder.build_between_clause("Data", "2015-02-01", "2015-02-28")
            '"Data" BETWEEN \'2015-02-01\' AND \'2015-02-28\''

            >>> SQLBuilder.build_between_clause("Valor", 100, 500)
            '"Valor" BETWEEN 100 AND 500'

            >>> SQLBuilder.build_between_clause("Ano", 2014, 2016)
            '"Ano" BETWEEN 2014 AND 2016'
        """
        escaped_col = SQLBuilder.escape_identifier(column)
        formatted_start = SQLBuilder.format_value(start_value)
        formatted_end = SQLBuilder.format_value(end_value)

        return f"{escaped_col} BETWEEN {formatted_start} AND {formatted_end}"

    @staticmethod
    def build_comparison(column: str, operator: str, value: Any) -> str:
        """
        Build a comparison expression for SQL WHERE conditions.

        Args:
            column: Column name (will be escaped)
            operator: SQL operator (=, !=, <, >, <=, >=, LIKE, etc.)
            value: Value to compare against

        Returns:
            str: SQL comparison expression

        Examples:
            >>> SQLBuilder.build_comparison("age", ">=", 18)
            '"age" >= 18'

            >>> SQLBuilder.build_comparison("name", "LIKE", "%Smith%")
            '"name" LIKE \'%Smith%\''
        """
        valid_operators = {
            "=",
            "!=",
            "<>",
            "<",
            ">",
            "<=",
            ">=",
            "LIKE",
            "ILIKE",
            "NOT LIKE",
            "NOT ILIKE",
            "IS",
            "IS NOT",
        }

        op_upper = operator.strip().upper()
        if op_upper not in valid_operators:
            raise ValueError(
                f"Invalid operator '{operator}'. Valid operators: {valid_operators}"
            )

        escaped_col = SQLBuilder.escape_identifier(column)

        # Special handling for IS NULL / IS NOT NULL
        if op_upper in ("IS", "IS NOT"):
            if value is None or str(value).upper() == "NULL":
                return f"{escaped_col} {op_upper} NULL"
            else:
                raise ValueError(
                    f"Operator {op_upper} requires NULL value, got {value}"
                )

        formatted_value = SQLBuilder.format_value(value)
        return f"{escaped_col} {op_upper} {formatted_value}"
