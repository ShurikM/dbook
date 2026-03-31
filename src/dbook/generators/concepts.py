"""Build concepts.json — compact term-to-table/column index.

The index is optimised for minimal token consumption. Table references
use the format ``schemas/<schema>/<table>.md`` so an agent can
``read_file`` them directly. Column references list only the bare
column name (deduplicated) to keep the file small.

Very short terms (<=2 chars) and ubiquitous structural terms are
filtered out to reduce noise.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict

from dbook.models import BookMeta

# Terms that are too generic to be useful in a concept index.
# Includes structural, temporal, and common DB-field terms.
_NOISE_TERMS: frozenset[str] = frozenset({
    "the", "and", "for", "not", "null", "key", "default",
    "type", "value", "data",
    # Common structural/temporal terms
    "created", "updated", "name", "status", "description",
    "active", "amount", "date",
    # Generic field patterns
    "code", "method", "page", "steps", "unit", "percentage",
    "count", "rate", "total", "max", "avg", "last", "four",
    "from", "until", "valid", "uses", "issued", "paid",
    "processed", "quantity", "price", "category", "card",
    # Relational / FK-derived terms
    "contact", "conversion", "discount", "event",
    "invoice", "password", "product", "role",
})


def _split_name(name: str) -> list[str]:
    """Split a name into component terms.

    Handles: snake_case, camelCase, PascalCase, kebab-case.
    """
    # Replace separators with spaces
    result = re.sub(r'[-_]', ' ', name)
    # Split camelCase/PascalCase
    result = re.sub(r'([a-z])([A-Z])', r'\1 \2', result)
    result = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1 \2', result)
    # Split and lowercase
    return [t.lower().strip() for t in result.split() if t.strip()]


def generate_concepts(book: BookMeta) -> dict[str, dict[str, list[str]]]:
    """Generate concept index by splitting table/column names into terms.

    Parameters
    ----------
    book : BookMeta
        The introspected database metadata.

    Returns
    -------
    dict[str, dict[str, list[str]]]
        Mapping of term to ``{"tables": [...], "columns": [...]}``.
    """
    # term -> {tables: set, columns: set}
    table_sets: dict[str, set[str]] = defaultdict(set)
    column_sets: dict[str, set[str]] = defaultdict(set)

    # Collect terms from table-name prefixes (high-level domain concepts)
    table_prefixes: set[str] = set()

    for schema_name, schema in book.schemas.items():
        for table_name, table in schema.tables.items():
            table_terms = _split_name(table_name)
            table_path = f"schemas/{schema_name}/{table_name}.md"

            # Only keep the first term from table names (the domain prefix)
            # e.g. "auth" from auth_users, "billing" from billing_orders
            if table_terms:
                prefix = table_terms[0]
                if len(prefix) > 2 and prefix not in _NOISE_TERMS:
                    table_sets[prefix].add(table_path)
                    table_prefixes.add(prefix)

            # Index single-word column names only (e.g. "email",
            # "phone", "token") — they are the highest-signal terms.
            for col in table.columns:
                col_terms = _split_name(col.name)
                if len(col_terms) == 1:
                    term = col_terms[0]
                    if len(term) > 2 and term not in _NOISE_TERMS:
                        column_sets[term].add(col.name)
                        table_sets[term].add(table_path)
                # Also index the head term for multi-word columns
                # that start with a distinctive word.
                elif col_terms:
                    head = col_terms[0]
                    if len(head) > 2 and head not in _NOISE_TERMS:
                        column_sets[head].add(col.name)
                        table_sets[head].add(table_path)

    # Build output: keep table-prefix terms (always) and column
    # terms that appear in only 1 table (highly distinctive).
    # For prefix terms with many tables, point to the manifest instead
    # to keep the index compact.
    all_terms = sorted(table_sets.keys() | column_sets.keys())
    result: dict[str, dict[str, list[str]]] = {}
    for term in all_terms:
        tables = sorted(table_sets.get(term, set()))
        if term not in table_prefixes and len(tables) > 1:
            continue
        # For prefix terms with 3+ tables, point to schema manifest
        if term in table_prefixes and len(tables) >= 3:
            # Extract schema name from first table path
            schema = tables[0].split("/")[1]
            tables = [f"schemas/{schema}/_manifest.md"]
        result[term] = {
            "tables": tables,
            "columns": sorted(column_sets.get(term, set())),
        }

    return result


def generate_concepts_json(book: BookMeta) -> str:
    """Generate concepts.json string.

    Parameters
    ----------
    book : BookMeta
        The introspected database metadata.

    Returns
    -------
    str
        Compact JSON string of concept mappings.
    """
    concepts = generate_concepts(book)
    return json.dumps(concepts, separators=(",", ":"), ensure_ascii=False)
