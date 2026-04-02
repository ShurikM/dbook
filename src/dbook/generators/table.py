"""Generate per-table .md files — full table metadata."""

from __future__ import annotations

from dbook.generators.metrics import generate_metrics
from dbook.models import BookMeta, TableMeta

# --- Semantic FK description helpers ---

FK_SEMANTIC_PATTERNS = {
    "user": "the user/customer",
    "account": "the customer account",
    "customer": "the customer",
    "order": "the parent order",
    "product": "the product",
    "invoice": "the associated invoice",
    "payment": "the payment record",
    "category": "the category",
    "session": "the user session",
    "ticket": "the support ticket",
    "role": "the assigned role",
    "warehouse": "the warehouse location",
    "shipment": "the shipment",
    "subscription": "the subscription",
    "cart": "the shopping cart",
    "discount": "the discount applied",
}


def _fk_description(fk_columns: tuple[str, ...], referred_table: str) -> str:
    """Generate a mechanical semantic description for a FK relationship."""
    table_lower = referred_table.lower()
    for pattern, desc in FK_SEMANTIC_PATTERNS.items():
        if pattern in table_lower:
            return desc
    return f"the {referred_table.replace('_', ' ')}"


def _incoming_fk_description(source_table: str, fk_columns: tuple[str, ...]) -> str:
    """Describe what an incoming FK represents."""
    table_lower = source_table.lower()
    if "item" in table_lower:
        return "line items in this record"
    if "payment" in table_lower:
        return "payments for this record"
    if "invoice" in table_lower:
        return "invoices for this record"
    if "session" in table_lower:
        return "sessions linked to this record"
    if "review" in table_lower:
        return "reviews of this record"
    if "comment" in table_lower or "message" in table_lower:
        return "messages/comments on this record"
    if "image" in table_lower or "photo" in table_lower:
        return "images for this record"
    return f"{source_table} records referencing this"


# --- Values column helper ---

def _column_values_display(col_name: str, table: TableMeta) -> str:
    """Show enum values for enum-like columns, '-' for others."""
    if col_name in table.enum_values:
        vals = table.enum_values[col_name]
        if len(vals) <= 5:
            return ", ".join(vals)
        return ", ".join(vals[:4]) + f" +{len(vals) - 4}"
    return "-"


# --- Example queries ---

def _sql_example(label: str, *parts: str) -> str:
    """Build an example query string for documentation (not executed)."""
    sql = "".join(parts)
    return label + ": `" + sql + "`"


def _generate_example_queries(table: TableMeta) -> list[str]:  # noqa: S608
    """Generate template-based example queries from table structure."""
    queries: list[str] = []
    t = table.name

    # PK lookup
    if table.primary_key:
        pk = table.primary_key[0]
        queries.append(_sql_example(
            "By " + pk,
            "SELECT * FROM ", t, " WHERE ", pk, " = ?",
        ))

    # FK join (first FK only)
    if table.foreign_keys:
        fk = table.foreign_keys[0]
        fk_col = fk.columns[0]
        ref_table = fk.referred_table
        ref_col = fk.referred_columns[0]
        queries.append(_sql_example(
            "With " + ref_table,
            "SELECT * FROM ", t, " JOIN ", ref_table,
            " ON ", t, ".", fk_col, " = ", ref_table, ".", ref_col,
        ))

    # Status/enum filter (if enum values exist)
    for col_name, values in table.enum_values.items():
        if len(values) <= 10:
            vals_str = ", ".join("'" + v + "'" for v in values[:5])
            queries.append(_sql_example(
                "By " + col_name,
                "SELECT * FROM ", t, " WHERE ", col_name, " IN (", vals_str, ")",
            ))
        break  # Only first enum column

    # Date range (if date column exists)
    date_cols = [
        c for c in table.columns
        if any(d in c.type.upper() for d in ["DATE", "TIME", "STAMP"])
    ]
    if date_cols:
        dc = date_cols[0].name
        queries.append(_sql_example(
            "Recent",
            "SELECT * FROM ", t, " WHERE ", dc,
            " > NOW() - INTERVAL '7 days' ORDER BY ", dc, " DESC",
        ))

    # Aggregation (if numeric column + group-by candidate exist)
    fk_col_names = {
        fk.columns[0] for fk in table.foreign_keys if fk.columns
    }
    numeric_cols = [
        c for c in table.columns
        if any(n in c.type.upper() for n in ["INT", "FLOAT", "DECIMAL", "NUMERIC", "REAL"])
        and not c.is_primary_key
        and c.name not in fk_col_names
    ]
    if numeric_cols and table.enum_values:
        num_col = numeric_cols[0].name
        group_col = list(table.enum_values.keys())[0]
        queries.append(_sql_example(
            "Aggregate",
            "SELECT ", group_col, ", SUM(", num_col, ") FROM ", t,
            " GROUP BY ", group_col,
        ))

    return queries[:5]  # Max 5 examples


# --- Token estimation ---

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


# --- Main generator ---

def generate_table(table: TableMeta, book: BookMeta | None = None) -> str:
    """Generate table .md content."""
    lines: list[str] = []

    # Header with summary (LLM if available, otherwise mechanical)
    tok_estimate = _estimate_table_tokens(table)
    lines.append(f"# {table.name} (~{tok_estimate} tok)")
    lines.append("")
    if table.summary:
        lines.append(table.summary)
    else:
        lines.append(_mechanical_summary(table))
    lines.append("")

    # Build FK lookup: col_name -> referred_table for description enrichment
    fk_lookup: dict[str, str] = {}
    for fk in table.foreign_keys:
        for col in fk.columns:
            fk_lookup[col] = fk.referred_table

    # Columns table — full format with Values column
    lines.append("## Columns")
    lines.append("")

    has_pii = any(col.pii_type for col in table.columns)

    if has_pii:
        lines.append("| Column | Type | Null | PK | Description | Values | PII | Sensitivity |")
        lines.append("|--------|------|------|----|-----------  |--------|-----|-------------|")
    else:
        lines.append("| Column | Type | Null | PK | Description | Values |")
        lines.append("|--------|------|------|----|-----------  |--------|")

    for col in table.columns:
        nullable = "YES" if col.nullable else "NO"
        pk = "PK" if col.is_primary_key else ""

        # Build description: FK reference + comment/purpose
        desc_parts: list[str] = []
        if col.name in fk_lookup:
            ref_table = fk_lookup[col.name]
            fk_desc = _fk_description((), ref_table)
            desc_parts.append(f"\u2192 {ref_table} ({fk_desc})")
        if col.comment:
            desc_parts.append(col.comment)
        elif table.column_purposes and col.name in table.column_purposes:
            desc_parts.append(table.column_purposes[col.name])
        description = "; ".join(desc_parts) if desc_parts else ""

        values_display = _column_values_display(col.name, table)

        if has_pii:
            pii = col.pii_type if col.pii_type else ""
            sensitivity = col.sensitivity if col.sensitivity and col.sensitivity != "none" else ""
            lines.append(
                f"| {col.name} | {col.type} | {nullable} | {pk}"
                f" | {description} | {values_display} | {pii} | {sensitivity} |"
            )
        else:
            lines.append(
                f"| {col.name} | {col.type} | {nullable} | {pk}"
                f" | {description} | {values_display} |"
            )

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

    # Related Tables (bidirectional FK navigation with semantic descriptions)
    if book:
        outgoing = _outgoing_references(table)
        incoming = _find_incoming_references(table.name, table.schema, book)
        if outgoing or incoming:
            lines.append("## Related Tables")
            lines.append("")
            if outgoing:
                lines.append("**Outgoing:**")
                for fk in table.foreign_keys:
                    cols = ", ".join(fk.columns)
                    desc = _fk_description(fk.columns, fk.referred_table)
                    lines.append(
                        f"- \u2192 {fk.referred_table} via {cols} \u2014 {desc}"
                    )
                lines.append("")
            if incoming:
                lines.append("**Incoming:**")
                for src_table, fk_cols_str, src_table_name in incoming:
                    desc = _incoming_fk_description(src_table_name, ())
                    lines.append(f"- \u2190 {src_table}.{fk_cols_str} \u2014 {desc}")
                lines.append("")

    # Example Queries
    examples = _generate_example_queries(table)
    if examples:
        lines.append("## Example Queries")
        lines.append("")
        for ex in examples:
            lines.append(f"- {ex}")
        lines.append("")

    # Auto-detected Metrics
    metrics = generate_metrics(table)
    if metrics:
        lines.append("## Metrics")
        lines.append("")
        for m in metrics:
            lines.append(f"- {m}")
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


def _find_incoming_references(
    table_name: str, schema: str | None, book: BookMeta,
) -> list[tuple[str, str, str]]:
    """Find all tables that reference this table via foreign keys.

    Returns list of (qualified_table, fk_columns_str, raw_table_name).
    """
    refs: list[tuple[str, str, str]] = []
    for s_name, s_meta in book.schemas.items():
        for t_name, t_meta in s_meta.tables.items():
            for fk in t_meta.foreign_keys:
                if fk.referred_table == table_name:
                    cols = ", ".join(fk.columns)
                    refs.append((f"{s_name}.{t_name}", cols, t_name))
    return sorted(refs)


# Keep the old name as a compatibility alias used by other modules
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
