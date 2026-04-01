"""Generate per-table .md files — full table metadata."""

from __future__ import annotations

from dbook.models import BookMeta, TableMeta


def _estimate_table_tokens(table: TableMeta) -> int:
    """Estimate tokens for a table's .md file without generating it."""
    base = 80  # header + section headers
    base += len(table.columns) * 25  # ~25 tokens per column row
    base += len(table.foreign_keys) * 20  # ~20 per FK row
    base += len(table.indexes) * 15  # ~15 per index row
    if table.sample_data:
        base += len(table.sample_data) * len(table.columns) * 5  # sample rows
    base += 30  # referenced-by section
    return base


def generate_table(table: TableMeta, book: BookMeta | None = None) -> str:
    """Generate table .md content."""
    lines = []

    # Header with summary (LLM if available, otherwise mechanical)
    tok_estimate = _estimate_table_tokens(table)
    lines.append(f"# {table.name} (~{tok_estimate} tok)")
    lines.append("")
    if table.summary:
        lines.append(table.summary)
    else:
        lines.append(_mechanical_summary(table))
    lines.append("")

    # Columns table — full format
    lines.append("## Columns")
    lines.append("")

    has_pii = any(col.pii_type for col in table.columns)
    if has_pii:
        lines.append("| Column | Type | Nullable | Default | PK | Comment | PII | Sensitivity |")
        lines.append("|--------|------|----------|---------|----|---------|----|-------------|")
    else:
        lines.append("| Column | Type | Nullable | Default | PK | Comment |")
        lines.append("|--------|------|----------|---------|----|---------| ")

    for col in table.columns:
        nullable = "YES" if col.nullable else "NO"
        default = str(col.default) if col.default is not None else ""
        pk = "PK" if col.is_primary_key else ""
        comment = col.comment if col.comment else ""
        # Use column purpose from LLM enrichment if no explicit comment
        if not comment and table.column_purposes and col.name in table.column_purposes:
            comment = table.column_purposes[col.name]

        if has_pii:
            pii = col.pii_type if col.pii_type else ""
            sensitivity = col.sensitivity if col.sensitivity and col.sensitivity != "none" else ""
            lines.append(f"| {col.name} | {col.type} | {nullable} | {default} | {pk} | {comment} | {pii} | {sensitivity} |")
        else:
            lines.append(f"| {col.name} | {col.type} | {nullable} | {default} | {pk} | {comment} |")

    lines.append("")

    # Primary Key
    if table.primary_key:
        lines.append("## Primary Key")
        lines.append("")
        lines.append(f"`{', '.join(table.primary_key)}`")
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

    # Sample Data — up to 5 rows with ALL columns, truncated at 40 chars
    if table.sample_data:
        lines.append("## Sample Data")
        lines.append("")
        all_cols = list(table.sample_data[0].keys())
        lines.append("| " + " | ".join(all_cols) + " |")
        lines.append("|" + "|".join(["---"] * len(all_cols)) + "|")
        for row in table.sample_data[:5]:
            vals = []
            for c in all_cols:
                v = str(row.get(c, ""))
                if len(v) > 40:
                    v = v[:37] + "..."
                vals.append(v)
            lines.append("| " + " | ".join(vals) + " |")
        lines.append("")

    # Related Tables (bidirectional FK navigation)
    if book:
        outgoing = _outgoing_references(table)
        incoming = _find_references(table.name, table.schema, book)
        if outgoing or incoming:
            lines.append("## Related Tables")
            lines.append("")
            if outgoing:
                lines.append("**References (outgoing):**")
                for ref_table, ref_col in outgoing:
                    lines.append(f"- \u2192 {ref_table} via {ref_col} ([{ref_table}.md]({ref_table}.md))")
                lines.append("")
            if incoming:
                lines.append("**Referenced By (incoming):**")
                for ref in incoming:
                    # ref is like "schema.table.col" — extract table name for link
                    parts = ref.split(".")
                    if len(parts) >= 2:
                        ref_table_name = parts[1]
                        lines.append(f"- \u2190 {ref} ([{ref_table_name}.md]({ref_table_name}.md))")
                    else:
                        lines.append(f"- \u2190 {ref}")
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
    fk_count = len(table.foreign_keys) if table.foreign_keys else 0
    parts.append(f"with {fk_count} foreign keys")
    idx_count = len(table.indexes) if table.indexes else 0
    parts.append(f"and {idx_count} index(es).")
    return " ".join(parts)


def _outgoing_references(table: TableMeta) -> list[tuple[str, str]]:
    """Return list of (referred_table, via_column) for outgoing FKs."""
    result: list[tuple[str, str]] = []
    for fk in table.foreign_keys:
        cols = ", ".join(fk.columns)
        result.append((fk.referred_table, cols))
    return result


def _find_references(table_name: str, schema: str | None, book: BookMeta) -> list[str]:
    """Find all tables that reference this table via foreign keys."""
    refs = []
    for s_name, s_meta in book.schemas.items():
        for t_name, t_meta in s_meta.tables.items():
            for fk in t_meta.foreign_keys:
                if fk.referred_table == table_name:
                    cols = ", ".join(fk.columns)
                    refs.append(f"{s_name}.{t_name}.{cols}")
    return sorted(refs)
