"""Serialize BookMeta to JSON for programmatic consumption."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from dbook.models import BookMeta


def book_to_dict(book: BookMeta) -> dict[str, Any]:
    """Convert BookMeta to a JSON-serializable dict.

    Handles datetime serialization and tuple conversion.
    """
    result = asdict(book)
    converted = _deep_convert(result)
    assert isinstance(converted, dict)  # asdict always returns a dict  # noqa: S101

    # Add FK graph if available (not part of the dataclass)
    fk_graph = getattr(book, "_fk_graph", None)
    if fk_graph:
        converted["fk_graph"] = fk_graph.to_dict()

    return converted


def _deep_convert(obj: Any) -> Any:
    """Recursively convert non-JSON-safe types."""
    if isinstance(obj, dict):
        return {k: _deep_convert(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_deep_convert(v) for v in obj]
    if isinstance(obj, tuple):
        return [_deep_convert(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, set):
        return sorted([_deep_convert(v) for v in obj])
    return obj


def book_to_json(book: BookMeta, indent: int = 2) -> str:
    """Serialize BookMeta to JSON string."""
    return json.dumps(book_to_dict(book), indent=indent, ensure_ascii=False)


def save_book_json(book: BookMeta, path: str | Path) -> None:
    """Save BookMeta as a JSON file."""
    Path(path).write_text(book_to_json(book))


def load_book_json(path: str | Path) -> dict[str, Any]:
    """Load a BookMeta JSON file. Returns raw dict (not BookMeta object)."""
    return json.loads(Path(path).read_text())
