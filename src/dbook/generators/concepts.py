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

    # Inject LLM-generated aliases if available
    alias_map = getattr(book, '_concept_aliases', None)
    if alias_map:
        for term, aliases in alias_map.items():
            if term in result and isinstance(aliases, list):
                result[term]["aliases"] = aliases

    return result


def generate_compact_lookup(concepts: dict, max_terms: int = 20, max_columns: int = 5) -> str:
    """Generate a compact markdown table of top concepts for embedding in NAVIGATION.md."""
    # Sort by number of table references (most connected terms first)
    sorted_terms = sorted(
        concepts.items(),
        key=lambda x: len(x[1].get("tables", [])) + len(x[1].get("columns", [])),
        reverse=True,
    )[:max_terms]

    if not sorted_terms:
        return ""

    lines = []
    lines.append("| Term | Tables | Key Columns |")
    lines.append("|------|--------|-------------|")

    for term, data in sorted_terms:
        tables = data.get("tables", [])
        columns = data.get("columns", [])

        # Shorten table paths: "schemas/default/auth_users.md" -> "auth_users"
        short_tables = []
        for t in tables[:3]:
            name = t.rsplit("/", 1)[-1].replace(".md", "")
            short_tables.append(name)
        tables_str = ", ".join(short_tables)
        if len(tables) > 3:
            tables_str += f" +{len(tables) - 3}"

        # Shorten column refs: "default.auth_users.email" -> "auth_users.email"
        short_cols = []
        for c in columns[:max_columns]:
            parts = c.split(".")
            if len(parts) >= 3:
                short_cols.append(f"{parts[-2]}.{parts[-1]}")
            else:
                short_cols.append(c)
        cols_str = ", ".join(short_cols)
        if len(columns) > max_columns:
            cols_str += f" +{len(columns) - max_columns}"

        lines.append(f"| {term} | {tables_str} | {cols_str} |")

    return "\n".join(lines)


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
