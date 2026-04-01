"""Build concepts.json — term-to-table/column index.

The index maps every meaningful term extracted from table and column
names to the specific table files and qualified column names where
that term appears.  This allows an agent to look up any term and
immediately find the relevant tables and columns.

Only single-character terms and pure numeric strings are filtered out.
Common terms like "id", "name", "email", "user", "created", "updated"
are intentionally kept because they are the most frequently searched.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict

from dbook.models import BookMeta


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


def _is_noise(term: str) -> bool:
    """Return True only for single-char terms and pure numbers."""
    if len(term) <= 1:
        return True
    if term.isdigit():
        return True
    return False


def generate_concepts(book: BookMeta) -> dict[str, dict[str, list[str]]]:
    """Generate concept index by splitting table/column names into terms.

    Parameters
    ----------
    book : BookMeta
        The introspected database metadata.

    Returns
    -------
    dict[str, dict[str, list[str]]]
        Mapping of term to ``{"tables": [...], "columns": [...], "aliases": []}``.
    """
    # term -> {tables: set, columns: set}
    table_sets: dict[str, set[str]] = defaultdict(set)
    column_sets: dict[str, set[str]] = defaultdict(set)

    for schema_name, schema in book.schemas.items():
        for table_name, table in schema.tables.items():
            table_path = f"schemas/{schema_name}/{table_name}.md"

            # Extract ALL terms from table name
            table_terms = _split_name(table_name)
            for term in table_terms:
                if not _is_noise(term):
                    table_sets[term].add(table_path)

            # Extract ALL terms from column names
            for col in table.columns:
                col_terms = _split_name(col.name)
                qualified_name = f"{table_name}.{col.name}"
                for term in col_terms:
                    if not _is_noise(term):
                        column_sets[term].add(qualified_name)
                        table_sets[term].add(table_path)

    # Build output
    all_terms = sorted(table_sets.keys() | column_sets.keys())
    result: dict[str, dict[str, list[str]]] = {}
    for term in all_terms:
        tables = sorted(table_sets.get(term, set()))
        columns = sorted(column_sets.get(term, set()))
        result[term] = {
            "tables": tables,
            "columns": columns,
            "aliases": [],
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
        JSON string of concept mappings with indent=2.
    """
    concepts = generate_concepts(book)
    return json.dumps(concepts, indent=2, ensure_ascii=False)
