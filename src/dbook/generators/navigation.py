"""Generate NAVIGATION.md — the L0 catalog entry point."""

from __future__ import annotations

from dbook.models import BookMeta


def generate_navigation(
    book: BookMeta,
    concepts: dict | None = None,
    has_concepts_file: bool = False,
) -> str:
    """Generate NAVIGATION.md content from BookMeta.

    Parameters
    ----------
    book : BookMeta
        The introspected database metadata.
    concepts : dict | None
        Concept index from ``generate_concepts(book)``.  When provided the
        top terms are embedded as a compact "Quick Lookup" table.
    has_concepts_file : bool
        Whether concepts.json will be written alongside this file (large DB).
        Controls the "How to Navigate" instructions.
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

    # Quick Lookup — inline concept index
    if concepts:
        from dbook.generators.concepts import generate_compact_lookup

        # For large DBs, show fewer terms (concepts.json has the rest)
        max_terms = 10 if has_concepts_file else 20
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
    if has_concepts_file:
        # Large DB — concepts.json exists as a separate file
        lines.append("1. Read this file for overview and term lookup")
        lines.append("2. If the term you need isn't in Quick Lookup above, check `concepts.json`")
        lines.append("3. `schemas/{s}/{table}.md` — table detail")
    else:
        # Small DB — everything is in this file
        lines.append("1. Read this file for overview and term lookup")
        lines.append("2. `schemas/{s}/{table}.md` — table detail")
    lines.append("")

    return "\n".join(lines)
