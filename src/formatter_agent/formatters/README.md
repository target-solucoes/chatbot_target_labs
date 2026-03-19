# Formatters Module - Phase 4

This module provides data formatting and output assembly capabilities for the formatter agent.

## Overview

The formatters module is responsible for:
1. Converting raw data into formatted tables (markdown and HTML)
2. Assembling the final structured JSON output
3. Calculating statistics and quality metrics
4. Formatting filters and metadata

## Components

### DataTableFormatter

Formats data tables into markdown and HTML representations.

**Usage:**
```python
from src.formatter_agent.formatters import DataTableFormatter

formatter = DataTableFormatter()

data = [
    {"Cliente": "Cliente A", "Faturamento": 4500000},
    {"Cliente": "Cliente B", "Faturamento": 2800000}
]

result = formatter.format(data, max_rows=10)

# result contains:
# - markdown: Markdown table string
# - html: HTML table string
# - headers: List of column names
# - rows: List of formatted rows
# - total_rows: Total number of rows in data
# - showing_rows: Number of rows being displayed
```

**Features:**
- Smart number formatting with thousand separators
- HTML XSS prevention through character escaping
- Unicode support
- Configurable row limiting
- Graceful handling of empty data and missing values

### OutputAssembler

Assembles the complete JSON output structure from all formatter components.

**Usage:**
```python
from src.formatter_agent.formatters import OutputAssembler

assembler = OutputAssembler()

output = assembler.assemble(
    parsed_inputs=parsed_inputs,
    executive_summary=executive_summary,
    synthesized_insights=synthesized_insights,
    next_steps=next_steps,
    formatted_table=formatted_table,
    execution_times=execution_times
)

# output contains complete JSON structure:
# - status
# - format_version
# - timestamp
# - executive_summary
# - visualization
# - insights
# - next_steps
# - data
# - metadata
```

**Features:**
- Complete JSON structure assembly
- Statistics calculation (total, mean, median, std)
- Data completeness calculation
- Filter formatting (simple, between, operator, list)
- Chart caption generation
- Transparency score calculation
- Execution time tracking

## Output Structure

```json
{
  "status": "success",
  "format_version": "1.0.0",
  "timestamp": "2025-11-20T...",
  "executive_summary": {
    "title": "...",
    "subtitle": "...",
    "introduction": "...",
    "query_original": "...",
    "chart_type": "...",
    "filters_applied": {}
  },
  "visualization": {
    "chart": {
      "type": "...",
      "html": "...",
      "file_path": "...",
      "config": {},
      "caption": "..."
    },
    "data_context": {
      "total_records": 0,
      "records_displayed": 0,
      "aggregation": "...",
      "date_range": null
    }
  },
  "insights": {
    "narrative": "...",
    "key_findings": [],
    "detailed_insights": [],
    "transparency": {
      "formulas_validated": false,
      "transparency_score": 0.0
    }
  },
  "next_steps": {
    "strategic_actions": [],
    "suggested_analyses": []
  },
  "data": {
    "summary_table": {
      "markdown": "...",
      "html": "...",
      "headers": [],
      "rows": [],
      "total_rows": 0,
      "showing_rows": 0
    },
    "raw_data": [],
    "statistics": {
      "total": 0.0,
      "mean": 0.0,
      "median": 0.0,
      "std": 0.0
    }
  },
  "metadata": {
    "pipeline_version": "v06_formatter",
    "agents_executed": [],
    "total_execution_time": 0.0,
    "formatter_execution_time": 0.0,
    "llm_calls": {},
    "data_quality": {
      "completeness": 0.0,
      "filters_count": 0,
      "engine_used": "..."
    }
  }
}
```

## Testing

Comprehensive test suite available in `tests/tests_formatter_agent/test_formatters/`:

- `test_data_table_formatter.py`: 23 tests for DataTableFormatter
- `test_output_assembler.py`: 32 tests for OutputAssembler
- `test_integration.py`: 10 integration tests

**Run tests:**
```bash
pytest tests/tests_formatter_agent/test_formatters/ -v
```

**Test coverage:** 100% of implemented functionality

## Examples

### Example 1: Format a Simple Table

```python
from src.formatter_agent.formatters import DataTableFormatter

formatter = DataTableFormatter()

data = [
    {"Product": "Widget A", "Sales": 125000.50},
    {"Product": "Widget B", "Sales": 98500.75},
    {"Product": "Widget C", "Sales": 76300.00}
]

result = formatter.format(data)

print(result["markdown"])
# | Product | Sales |
# |---|---|
# | Widget A | 125,000.50 |
# | Widget B | 98,500.75 |
# | Widget C | 76,300.00 |
```

### Example 2: Complete Assembly Workflow

```python
from src.formatter_agent.formatters import DataTableFormatter, OutputAssembler

# Step 1: Format table
formatter = DataTableFormatter()
data = [...]  # your data
formatted_table = formatter.format(data)

# Step 2: Assemble output
assembler = OutputAssembler()
output = assembler.assemble(
    parsed_inputs={...},
    executive_summary={...},
    synthesized_insights={...},
    next_steps={...},
    formatted_table=formatted_table,
    execution_times={...}
)

# Step 3: Use output
print(f"Status: {output['status']}")
print(f"Title: {output['executive_summary']['title']}")
print(f"Key Findings: {len(output['insights']['key_findings'])}")
```

## Schema Validation

Output complies with `FormatterOutputSchema` defined in `src/formatter_agent/models/formatter_schemas.py`.

Validate output:
```python
from src.formatter_agent.models.formatter_schemas import FormatterOutputSchema

# Validate (will raise ValidationError if invalid)
schema = FormatterOutputSchema(**output)
```

## Security

### XSS Prevention

DataTableFormatter automatically escapes HTML special characters:
- `<` → `&lt;`
- `>` → `&gt;`
- `&` → `&amp;`
- `"` → `&quot;`
- `'` → `&#39;`

This prevents XSS attacks when displaying user-provided data in HTML contexts.

## Performance

- **DataTableFormatter**: O(n) for n rows
- **OutputAssembler**: O(n) for statistics calculation
- **Memory**: Minimal - processes data in single pass

Typical execution times:
- Format 100 rows: ~5ms
- Assemble complete output: ~10ms

## Dependencies

- `typing`: Type annotations
- `statistics`: Statistical calculations
- `datetime`: Timestamp generation
- `logging`: Logging support

No external dependencies required.

## Logging

Both formatters include comprehensive logging:

```python
import logging

# Configure logging level
logging.getLogger("src.formatter_agent.formatters").setLevel(logging.INFO)
```

Log messages include:
- Table formatting operations
- Assembly operations
- Statistics calculations
- Warning for missing/invalid data

## Error Handling

### Graceful Degradation

Both formatters handle edge cases gracefully:

**Empty Data:**
```python
result = formatter.format([])
# Returns valid structure with empty indicators
```

**Missing Values:**
```python
data = [{"A": 1, "B": None}]
result = formatter.format(data)
# None values formatted as empty strings
```

**Invalid Input:**
```python
# OutputAssembler provides defaults for missing fields
# No exceptions raised - returns valid output
```

## Best Practices

1. **Always validate inputs** before passing to assembler
2. **Use max_rows** to limit table size for large datasets
3. **Check output status** before using assembled data
4. **Log execution times** for performance monitoring
5. **Validate schema** in production to catch structural issues

## Migration Notes

When upgrading from earlier versions:
- Output structure version is tracked in `format_version` field
- Check `format_version` to handle different schema versions
- Backward compatibility maintained for v1.0.0

## Support

For issues or questions:
1. Check test files for usage examples
2. Review inline documentation (docstrings)
3. Consult FASE4_IMPLEMENTATION_SUMMARY.md for detailed specifications

## License

Part of the formatter agent project. Internal use only.
