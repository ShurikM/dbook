"""Generate per-table .md files — full table metadata."""

from __future__ import annotations

from dbook.models import BookMeta, TableMeta


def generate_table(table: TableMeta, book: BookMeta | None = None) -> str:
    """Generate table .md content."""
    lines = []

    # Header with inline summary
    lines.append(f"# {table.name}")
    row_info = f" ({table.row_count:,} rows)" if table.row_count is not None else ""
    pk_info = f" PK: {', '.join(table.primary_key)}" if table.primary_key else ""
    lines.append(f"{len(table.columns)} cols{row_info}{pk_info}")
    lines.append("")

    # Columns table — compact format
    lines.append("## Columns")
    lines.append("")

    has_pii = any(col.pii_type for col in table.columns)
    if has_pii:
        lines.append("| Column | Type | Null | PK | PII |")
        lines.append("|--------|------|------|----|-----|")
    else:
        lines.append("| Column | Type | Null | PK |")
        lines.append("|--------|------|------|----|")

    for col in table.columns:
        nullable = "Y" if col.nullable else "N"
        pk = "Y" if col.is_primary_key else ""

        if has_pii:
            pii = col.pii_type if col.pii_type else ""
            lines.append(f"| {col.name} | {col.type} | {nullable} | {pk} | {pii} |")
        else:
            lines.append(f"| {col.name} | {col.type} | {nullable} | {pk} |")

    lines.append("")

    # Foreign Keys
    if table.foreign_keys:
        lines.append("## Foreign Keys")
        lines.append("")
        lines.append("| Column | References |")
        lines.append("|--------|-----------|")
        for fk in table.foreign_keys:
            ref_schema = f"{fk.referred_schema}." if fk.referred_schema else ""
            cols = ", ".join(fk.columns)
            ref = f"{ref_schema}{fk.referred_table}.{', '.join(fk.referred_columns)}"
            lines.append(f"| {cols} | {ref} |")
        lines.append("")

    # Indexes
    if table.indexes:
        lines.append("## Indexes")
        lines.append("")
        lines.append("| Name | Columns | Unique |")
        lines.append("|------|---------|--------|")
        for idx in table.indexes:
            name = idx.name if idx.name else "-"
            cols = ", ".join(idx.columns)
            unique = "yes" if idx.unique else "no"
            lines.append(f"| {name} | {cols} | {unique} |")
        lines.append("")

    # Sample Data (1 row, first 4 columns, truncated values)
    if table.sample_data:
        lines.append("## Sample Data")
        lines.append("")
        all_cols = list(table.sample_data[0].keys())
        cols = all_cols[:4]
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("|" + "|".join(["---"] * len(cols)) + "|")
        row = table.sample_data[0]
        vals = []
        for c in cols:
            v = str(row.get(c, ""))
            if len(v) > 16:
                v = v[:13] + "..."
            vals.append(v)
        lines.append("| " + " | ".join(vals) + " |")
        lines.append("")

    # Referenced By
    if book:
        refs = _find_references(table.name, table.schema, book)
        if refs:
            lines.append("## Referenced By")
            lines.append("")
            for ref in refs:
                lines.append(f"- {ref}")
            lines.append("")

    return "\n".join(lines)


def _mechanical_summary(table: TableMeta) -> str:
    """Generate a mechanical summary from structural data."""
    parts = [f"Table '{table.name}'"]
    if table.row_count is not None:
        parts.append(f"with {table.row_count:,} rows")
    parts.append(f"and {len(table.columns)} columns")
    if table.primary_key:
        parts.append(f"(PK: {', '.join(table.primary_key)})")
    if table.foreign_keys:
        targets = [fk.referred_table for fk in table.foreign_keys]
        parts.append(f"references {', '.join(targets)}")
    if table.indexes:
        parts.append(f"with {len(table.indexes)} index(es)")
    return " ".join(parts) + "."


def _find_references(table_name: str, schema: str | None, book: BookMeta) -> list[str]:
    """Find all tables that reference this table via foreign keys."""
    refs = []
    for s_name, s_meta in book.schemas.items():
        for t_name, t_meta in s_meta.tables.items():
            for fk in t_meta.foreign_keys:
                if fk.referred_table == table_name:
                    cols = ", ".join(fk.columns)
                    refs.append(f"{t_name}.{cols}")
    return sorted(refs)
