"""Generate checksums.json from BookMeta."""

from __future__ import annotations

import json

from dbook.models import BookMeta


def generate_checksums(book: BookMeta) -> str:
    """Generate a checksums.json for all tables in the book.

    Parameters
    ----------
    book : BookMeta
        The introspected database metadata.

    Returns
    -------
    str
        JSON string of schema hashes keyed by qualified table name.
    """
    checksums: dict[str, str] = {}

    for schema_name, schema in sorted(book.schemas.items()):
        for table_name, table in sorted(schema.tables.items()):
            qualified = f"{schema_name}.{table_name}"
            checksums[qualified] = table.schema_hash

    return json.dumps(checksums, indent=2, sort_keys=True)
