"""Generate NAVIGATION.md — the L0 catalog entry point."""

from __future__ import annotations

from dbook.models import BookMeta


def generate_navigation(book: BookMeta) -> str:
    """Generate NAVIGATION.md content from BookMeta."""
    lines = []
    lines.append(f"# Database Book: {book.dialect}")
    lines.append("")

    # Schemas list
    lines.append("## Schemas")
    lines.append("")
    for schema_name, schema in sorted(book.schemas.items()):
        table_count = len(schema.tables)
        total_rows = sum(t.row_count or 0 for t in schema.tables.values())
        lines.append(f"- **{schema_name}**: {table_count} tables, {total_rows:,} rows")

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

    # How to navigate
    lines.append("## How to Navigate")
    lines.append("")
    lines.append("1. `concepts.json` — find terms")
    lines.append("2. `schemas/{s}/_manifest.md` — schema overview")
    lines.append("3. `schemas/{s}/{table}.md` — table detail")
    lines.append("")

    return "\n".join(lines)
