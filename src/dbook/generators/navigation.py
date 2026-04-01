"""Generate NAVIGATION.md — the L0 catalog entry point."""

from __future__ import annotations

from dbook.models import BookMeta


def generate_navigation(
    book: BookMeta,
    concepts: dict | None = None,
) -> str:
    """Generate NAVIGATION.md content from BookMeta.

    Parameters
    ----------
    book : BookMeta
        The introspected database metadata.
    concepts : dict | None
        Concept index from ``generate_concepts(book)``.  When provided the
        top terms are embedded as a compact "Quick Lookup" table.
    """
    lines = []
    lines.append(f"# Database Book: {book.dialect}")
    lines.append("")

    # Schemas table
    lines.append("## Schemas")
    lines.append("")
    lines.append("| Schema | Tables | Total Rows | Description |")
    lines.append("|--------|--------|------------|-------------|")
    for schema_name, schema in sorted(book.schemas.items()):
        table_count = len(schema.tables)
        total_rows = sum(t.row_count or 0 for t in schema.tables.values())
        description = schema.narrative if schema.narrative else "-"
        lines.append(f"| {schema_name} | {table_count} | {total_rows:,} | {description} |")
    lines.append("")

    # Quick Reference — key tables per schema
    lines.append("## Quick Reference")
    lines.append("")
    for schema_name, schema in sorted(book.schemas.items()):
        table_names = sorted(schema.tables.keys())
        lines.append(f"- **{schema_name}**: {', '.join(table_names)}")
    lines.append("")

    # Sensitivity summary (only if any PII detected)
    has_pii = any(
        col.pii_type
        for schema in book.schemas.values()
        for table in schema.tables.values()
        for col in table.columns
    )
    if has_pii:
        lines.append("## Sensitivity Overview")
        lines.append("")
        lines.append("| Schema | PII Columns | Types Detected |")
        lines.append("|--------|-------------|----------------|")
        for schema_name, schema in sorted(book.schemas.items()):
            pii_cols = [
                col for table in schema.tables.values()
                for col in table.columns if col.pii_type
            ]
            if pii_cols:
                types = sorted(t for c in pii_cols if (t := c.pii_type) is not None)
                lines.append(f"| {schema_name} | {len(pii_cols)} | {', '.join(types)} |")
        lines.append("")

    # Quick Lookup — inline concept index for ALL database sizes
    if concepts:
        from dbook.generators.concepts import generate_compact_lookup

        # Count total tables to determine max_terms
        total_tables = sum(
            len(schema.tables)
            for schema in book.schemas.values()
        )

        # For small DBs (<20 tables): show ALL terms; for large DBs: top 30
        if total_tables < 20:
            max_terms = len(concepts)
        else:
            max_terms = 30

        lookup_table = generate_compact_lookup(
            concepts, max_terms=max_terms, max_columns=3,
        )
        if lookup_table:
            lines.append("## Quick Lookup")
            lines.append("")
            lines.append(lookup_table)
            lines.append("")

    # How to navigate
    lines.append("## How to Navigate")
    lines.append("")
    lines.append("1. Read this file for overview and term lookup")
    lines.append("2. Read schemas/{name}/_manifest.md for schema details")
    lines.append("3. Read schemas/{name}/{table}.md for full table metadata")
    lines.append("")

    return "\n".join(lines)
