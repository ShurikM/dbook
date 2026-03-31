"""Generate _manifest.md — schema-level overview."""

from __future__ import annotations

from dbook.models import SchemaMeta


def generate_manifest(schema: SchemaMeta) -> str:
    """Generate _manifest.md content for a schema."""
    lines = []
    lines.append(f"# Schema: {schema.name}")

    if schema.narrative:
        lines.append(f"\n{schema.narrative}")

    lines.append("")
    lines.append("## Tables")
    lines.append("")
    lines.append("| Table | Columns | Rows | Primary Key | Foreign Keys | Description |")
    lines.append("|-------|---------|------|-------------|-------------|-------------|")

    for table_name, table in sorted(schema.tables.items()):
        col_count = len(table.columns)
        rows = f"{table.row_count:,}" if table.row_count is not None else "?"
        pk = ", ".join(table.primary_key) if table.primary_key else "-"
        fk_count = len(table.foreign_keys)
        desc = table.summary if table.summary else "-"
        lines.append(f"| [{table_name}]({table_name}.md) | {col_count} | {rows} | {pk} | {fk_count} | {desc} |")

    lines.append("")

    # Relationships section
    relationships = []
    for table_name, table in sorted(schema.tables.items()):
        for fk in table.foreign_keys:
            ref_schema = fk.referred_schema or schema.name
            relationships.append(
                f"- {table_name}.{', '.join(fk.columns)} → {ref_schema}.{fk.referred_table}.{', '.join(fk.referred_columns)}"
            )

    if relationships:
        lines.append("## Relationships")
        lines.append("")
        for rel in relationships:
            lines.append(rel)
        lines.append("")

    return "\n".join(lines)
