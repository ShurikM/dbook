"""Schema hash computation for change detection."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from dbook.models import TableMeta


def compute_table_hash(table: TableMeta) -> str:
    """Compute a SHA256 hash of a table's schema for change detection.

    The hash is computed from a canonical representation of the table's
    structure: columns (sorted by name), primary key, foreign keys,
    indexes, and table comment. Data (row counts, sample data) is
    excluded -- only structural changes trigger a hash change.
    """
    canonical = _canonical_representation(table)
    serialized = json.dumps(canonical, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _canonical_representation(table: TableMeta) -> dict[str, Any]:
    """Build a canonical dict for hashing. Sorted and deterministic."""
    columns_list: list[dict[str, str | bool | None]] = [
        {
            "name": col.name,
            "type": col.type,
            "nullable": col.nullable,
            "default": col.default,
            "is_primary_key": col.is_primary_key,
        }
        for col in table.columns
    ]

    fk_list: list[dict[str, str | list[str]]] = [
        {
            "columns": sorted(fk.columns),
            "referred_table": fk.referred_table,
            "referred_columns": sorted(fk.referred_columns),
        }
        for fk in table.foreign_keys
    ]

    idx_list: list[dict[str, list[str] | bool]] = [
        {
            "columns": sorted(idx.columns),
            "unique": idx.unique,
        }
        for idx in table.indexes
    ]

    return {
        "columns": sorted(
            columns_list,
            key=lambda c: str(c["name"]),
        ),
        "primary_key": sorted(table.primary_key),
        "foreign_keys": sorted(
            fk_list,
            key=lambda fk: (str(fk["referred_table"]), str(fk["columns"])),
        ),
        "indexes": sorted(
            idx_list,
            key=lambda idx: str(idx["columns"]),
        ),
        "comment": table.comment,
    }
