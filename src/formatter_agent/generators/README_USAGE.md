# Formatter Agent - Phase 3 Generators Quick Reference

## Installation & Setup

```python
from langchain_openai import ChatOpenAI
from src.formatter_agent.core.settings import get_llm_config
from src.formatter_agent.generators import (
    ExecutiveSummaryGenerator,
    InsightSynthesizer,
    NextStepsGenerator
)

# Initialize LLM
llm_config = get_llm_config()
llm = ChatOpenAI(**llm_config)

# Initialize generators
exec_gen = ExecutiveSummaryGenerator(llm)
insight_synth = InsightSynthesizer(llm)
next_gen = NextStepsGenerator(llm)
```

## Usage Examples

### 1. Executive Summary Generator

```python
# Prepare inputs
parsed_inputs = {
    "query": "top 5 clientes de SP por faturamento",
    "chart_type": "bar_horizontal",
    "filters": {"UF_Cliente": "SP"},
    "chart_spec": {
        "metrics": [{"name": "Valor", "alias": "Faturamento", "aggregation": "sum"}],
        "dimensions": [{"name": "Des_Cliente", "alias": "Cliente"}],
        "top_n": 5
    },
    "data": [
        {"Des_Cliente": "Cliente A", "Faturamento": 4500000},
        {"Des_Cliente": "Cliente B", "Faturamento": 2800000},
        # ... more data
    ]
}

handler_context = {
    "chart_type_description": "ranking de clientes por faturamento",
    "analysis_focus": "concentração"
}

# Generate
result = exec_gen.generate(parsed_inputs, handler_context)

# Access results
print(result["title"])              # Max 80 chars
print(result["introduction"])       # 50-300 chars
print(result["subtitle"])           # Original query
print(result["filters_applied_description"])
print(result["_fallback_used"])     # False if LLM succeeded
```

### 2. Insight Synthesizer

```python
# Prepare insights
insights = [
    {
        "title": "Concentração Crítica no Top 3",
        "interpretation": "Os três principais clientes representam 68.3%...",
        "formula": "4.5M + 2.8M + 2.5M / 13M → 68.3%",
        "confidence": 0.95
    },
    # ... more insights
]

# Synthesize
result = insight_synth.synthesize(insights, parsed_inputs)

# Access results
print(result["narrative"])                  # 200-500 chars
print(result["key_findings"])              # 3-5 bullets (max 120 chars each)
print(result["detailed_insights"])         # Processed insights with metadata
print(result["transparency_validated"])    # True if all have formulas
print(result["_fallback_used"])
```

### 3. Next Steps Generator

```python
# Prepare synthesized insights (from previous step)
synthesized_insights = {
    "narrative": "A análise revela concentração crítica...",
    "key_findings": [
        "Top 3 clientes concentram 68.3% do faturamento",
        "Líder possui vantagem de 60.7%...",
        # ...
    ]
}

# Generate
result = next_gen.generate(synthesized_insights, parsed_inputs)

# Access results
for action in result["strategic_actions"]:  # 2-3 actions
    print(f"[{action['priority']}] {action['action']}")
    print(f"Rationale: {action['rationale']}")

for analysis in result["suggested_analyses"]:  # 2-3 analyses
    print(f"Query: {analysis['query']}")
    print(f"Description: {analysis['description']}")
    print(f"Filters: {analysis['filters']}")

print(result["_fallback_used"])
```

## Complete Pipeline Example

```python
# Step 1: Executive Summary
exec_result = exec_gen.generate(parsed_inputs, handler_context)

# Step 2: Insight Synthesis
insight_result = insight_synth.synthesize(insights, parsed_inputs)

# Step 3: Next Steps
next_result = next_gen.generate(insight_result, parsed_inputs)

# Consolidated output
output = {
    "executive_summary": {
        "title": exec_result["title"],
        "introduction": exec_result["introduction"],
        "filters_applied": exec_result["filters_applied_description"]
    },
    "insights": {
        "narrative": insight_result["narrative"],
        "key_findings": insight_result["key_findings"],
        "transparency_validated": insight_result["transparency_validated"]
    },
    "next_steps": {
        "strategic_actions": next_result["strategic_actions"],
        "suggested_analyses": next_result["suggested_analyses"]
    }
}
```

## Error Handling

All generators have automatic fallback:

```python
try:
    result = exec_gen.generate(parsed_inputs, handler_context)
    
    if result["_fallback_used"]:
        print("Warning: LLM failed, using template fallback")
    else:
        print("Success: LLM-generated content")
        
except Exception as e:
    print(f"Critical error: {e}")
    # This should rarely happen as generators have internal error handling
```

## Testing

### Run Unit Tests
```bash
# All generator tests
pytest tests/tests_formatter_agent/test_generators/ -v -k "not integration"

# Specific generator
pytest tests/tests_formatter_agent/test_generators/test_executive_summary.py -v
pytest tests/tests_formatter_agent/test_generators/test_insight_synthesizer.py -v
pytest tests/tests_formatter_agent/test_generators/test_next_steps_generator.py -v
```

### Run Integration Tests (with real LLM)
```bash
# Requires OPENAI_API_KEY environment variable
pytest -v -m integration tests/tests_formatter_agent/test_generators/test_integration.py

# With performance benchmarks
pytest -v -m "integration and slow" tests/tests_formatter_agent/test_generators/test_integration.py
```

## Configuration

### Customize LLM Settings
```python
from src.formatter_agent.core.settings import get_llm_config

# Custom configuration
config = get_llm_config(
    model="gpt-5-nano-2025-08-07",
    temperature=0.5,  # More creative
    max_tokens=2000    # More tokens
)
llm = ChatOpenAI(**config)
```

### Adjust Retry Behavior
```python
from src.formatter_agent.core.settings import get_retry_config

retry_config = get_retry_config()
# Returns: {"max_attempts": 2, "delay": 1.0}

# Modify in settings.py:
# DEFAULT_RETRY_ATTEMPTS = 3
# DEFAULT_RETRY_DELAY = 2.0
```

## Output Schemas

### ExecutiveSummary
```python
{
    "title": str,              # Max 80 chars
    "introduction": str,       # 50-300 chars
    "subtitle": str,           # Query original
    "filters_applied_description": str,
    "_fallback_used": bool
}
```

### SynthesizedInsights
```python
{
    "narrative": str,          # 200-500 chars
    "key_findings": [str],     # 3-5 items, max 120 chars each
    "detailed_insights": [     # Processed insights
        {
            "title": str,
            "interpretation": str,
            "formula": str,
            "confidence": float,
            "category": str,   # concentração, gap_competitivo, tendência, diversidade, geral
            "has_formula": bool
        }
    ],
    "transparency_validated": bool,
    "_fallback_used": bool
}
```

### NextSteps
```python
{
    "strategic_actions": [     # 2-3 items
        {
            "action": str,     # Max 150 chars
            "rationale": str,  # Max 200 chars
            "priority": str    # high|medium|low
        }
    ],
    "suggested_analyses": [    # 2-3 items
        {
            "query": str,      # Max 100 chars
            "description": str,# Max 150 chars
            "filters": dict
        }
    ],
    "_fallback_used": bool
}
```

## Common Issues & Solutions

### Issue: LLM timeout
**Solution:** Increase timeout in settings.py:
```python
DEFAULT_LLM_TIMEOUT = 60  # Instead of 30
```

### Issue: Validation errors
**Solution:** Check Pydantic schema constraints. Enable detailed logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Issue: Fallback always triggered
**Solution:** Verify:
1. OpenAI API key is set: `os.getenv("OPENAI_API_KEY")`
2. Model name is correct: `gpt-4o-2015-11-20`
3. JSON response format is enabled (automatic in settings)

### Issue: Character limit violations
**Solution:** Constraints are enforced by Pydantic. If needed, adjust in `formatter_schemas.py`:
```python
title: str = Field(..., max_length=100)  # Increased from 80
```

## Performance Tips

1. **Reuse LLM instance:** Create once, use multiple times
2. **Batch processing:** Process multiple analyses in sequence without recreating generators
3. **Monitor fallbacks:** Track `_fallback_used` to identify quality issues
4. **Parallel execution:** Phase 4+ will enable async/parallel LLM calls

## Support & Documentation

- **Phase 3 Summary:** `FASE3_IMPLEMENTATION_SUMMARY.md`
- **Planning Document:** `planning_formatter.md`
- **Schemas Reference:** `src/formatter_agent/models/formatter_schemas.py`
- **Settings Reference:** `src/formatter_agent/core/settings.py`
- **Tests:** `tests/tests_formatter_agent/test_generators/`
