import pandas as pd
import json
from typing import Any, Dict, List

def format_sql(sql_query: str) -> str:
    """Format SQL for better readability if needed."""
    if not sql_query:
        return "-- No SQL Query executed"
    return sql_query.strip()

def format_dataframe(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Safely convert a list of dicts to a pandas DataFrame."""
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)

def format_json(data: Any) -> str:
    """Format an object to an indented JSON string safely."""
    try:
        return json.dumps(data, indent=2, ensure_ascii=False, default=str)
    except Exception:
        return str(data)

def format_tokens(agent_tokens_dict: Dict[str, Dict[str, int]]) -> List[Dict[str, Any]]:
    """Format agent tokens for display as a table."""
    records = []
    total_in = 0
    total_out = 0
    for agent, metrics in agent_tokens_dict.items():
        inp = metrics.get('input_tokens', 0)
        outp = metrics.get('output_tokens', 0)
        records.append({
            "Agent": agent, 
            "Model": metrics.get('model_name', 'N/A'),
            "Input Tokens": inp, 
            "Output Tokens": outp,
            "Total": inp + outp
        })
        total_in += inp
        total_out += outp
    
    if records:
        records.append({
            "Agent": "**TOTAL**", 
            "Model": "-",
            "Input Tokens": total_in, 
            "Output Tokens": total_out,
            "Total": total_in + total_out
        })
    return records
