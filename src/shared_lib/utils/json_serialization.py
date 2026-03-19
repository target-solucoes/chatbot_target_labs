"""Shared JSON serialization helpers.

The pipeline frequently needs to persist or display structured outputs that may
contain Python objects not supported by the stdlib `json` module (notably
`datetime`).

This module provides:
- `sanitize_for_json`: recursively converts objects into JSON-serializable types
- `json_default`: a `json.dumps(default=...)` compatible hook
- `json_dumps`: convenience wrapper around `json.dumps` using the default hook

Design goals:
- Deterministic and safe (no crashes at output boundaries)
- Prefer ISO-8601 for date/time values
- Keep dependencies optional (numpy/pandas/pydantic handled when installed)
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Dict


def sanitize_for_json(obj: Any) -> Any:
    """Recursively convert *obj* to JSON-serializable primitives.

    Converts:
    - datetime/date -> ISO-8601 strings
    - timedelta -> total seconds (float)
    - Path -> str
    - Decimal -> float (or int when exact)
    - Enum -> value
    - numpy/pandas/pydantic types when available

    Falls back to `str(obj)` for unknown objects.
    """

    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    if isinstance(obj, timedelta):
        return obj.total_seconds()

    if isinstance(obj, Path):
        return str(obj)

    if isinstance(obj, Decimal):
        # Preserve integers when possible (e.g. Decimal('3'))
        if obj == obj.to_integral_value():
            return int(obj)
        return float(obj)

    if isinstance(obj, Enum):
        return sanitize_for_json(obj.value)

    if isinstance(obj, (list, tuple, set)):
        return [sanitize_for_json(v) for v in obj]

    if isinstance(obj, dict):
        return {str(k): sanitize_for_json(v) for k, v in obj.items()}

    # Optional: pydantic BaseModel
    try:
        from pydantic import BaseModel  # type: ignore

        if isinstance(obj, BaseModel):
            # model_dump in v2; dict in v1
            if hasattr(obj, "model_dump"):
                return sanitize_for_json(obj.model_dump())  # type: ignore[attr-defined]
            return sanitize_for_json(obj.dict())
    except Exception:
        pass

    # Optional: numpy
    try:
        import numpy as np  # type: ignore

        if isinstance(obj, np.datetime64):
            # Convert to ISO if possible
            try:
                return str(obj.astype("datetime64[ms]"))
            except Exception:
                return str(obj)

        if isinstance(obj, np.ndarray):
            return [sanitize_for_json(v) for v in obj.tolist()]

        if isinstance(obj, np.generic):
            return sanitize_for_json(obj.item())
    except Exception:
        pass

    # Optional: pandas
    try:
        import pandas as pd  # type: ignore

        if isinstance(obj, pd.Timestamp):
            return obj.isoformat() if pd.notna(obj) else None

        if hasattr(pd, "isna") and pd.isna(obj):  # type: ignore[arg-type]
            return None
    except Exception:
        pass

    # Best-effort fallback
    return str(obj)


def json_default(obj: Any) -> Any:
    """Hook for `json.dumps(default=...)`.

    This function is called only for objects `json` doesn't know how to encode.
    """

    return sanitize_for_json(obj)


def json_dumps(data: Any, **kwargs: Any) -> str:
    """`json.dumps` wrapper using `json_default` by default."""

    import json

    if "default" not in kwargs:
        kwargs["default"] = json_default
    return json.dumps(data, **kwargs)


__all__ = [
    "sanitize_for_json",
    "json_default",
    "json_dumps",
]
