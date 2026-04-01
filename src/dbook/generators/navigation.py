"""Generate NAVIGATION.md — the L0 catalog entry point."""

from __future__ import annotations

from dbook.models import BookMeta


def generate_navigation(book: BookMeta) -> str:
    """Generate NAVIGATION.md content from BookMeta.

    Parameters
    ----------
    book : BookMeta
        The introspected database metadata.
    """
    lines = []
    total_tables = sum(len(s.tables) for s in book.schemas.values())
    lines.append(f"# Database Book: {book.dialect}")
    lines.append(f"Compiled: {book.compiled_at:%Y-%m-%dT%H:%M:%SZ} | Mode: {book.mode}")
    lines.append("")

    # Compact table overview
    lines.append(f"## Tables ({total_tables})")
    lines.append("")
    lines.append("| Table | Rows | Key Columns | References |")
    lines.append("|-------|------|-------------|------------|")

    for _schema_name, schema in sorted(book.schemas.items()):
        for table_name, table in sorted(schema.tables.items()):
            rows = f"{table.row_count:,}" if table.row_count is not None else "-"
            key_cols = _key_columns(table, max_cols=6)
            refs = _references(table)
            lines.append(f"| {table_name} | {rows} | {key_cols} | {refs} |")

    lines.append("")

    # PII sensitivity summary (if any PII detected)
    pii_tables: list[tuple[str, list[str]]] = []
    for _schema_name, schema in sorted(book.schemas.items()):
        for table_name, table in sorted(schema.tables.items()):
            pii_col_names = [col.name for col in table.columns if col.pii_type]
            if pii_col_names:
                pii_tables.append((table_name, pii_col_names))

    if pii_tables:
        parts = [f"{tname} ({', '.join(cols)})" for tname, cols in pii_tables]
        lines.append(f"\u26a0 PII detected: {', '.join(parts)}")
        lines.append("")

    # Navigate instructions
    lines.append("## Navigate")
    lines.append("1. Scan the table above to find what you need")
    lines.append("2. Read `schemas/{schema}/{table}.md` for full details")
    lines.append("")

    return "\n".join(lines)


def _key_columns(table, max_cols: int = 6) -> str:
    """Select up to max_cols key columns, prioritized.

    Priority: PK columns -> FK columns -> PII-marked -> commented -> by position.
    Skip the PK "id" if there are more interesting columns to show.
    """
    pk_set = set(table.primary_key) if table.primary_key else set()
    fk_cols: set[str] = set()
    for fk in table.foreign_keys:
        for c in fk.columns:
            fk_cols.add(c)

    pii_cols = {col.name for col in table.columns if col.pii_type}
    commented_cols = {col.name for col in table.columns if col.comment}

    selected: list[str] = []
    seen: set[str] = set()

    def _add(name: str) -> None:
        if name not in seen and len(selected) < max_cols:
            selected.append(name)
            seen.add(name)

    # 1. PK columns
    for col in table.columns:
        if col.name in pk_set:
            _add(col.name)

    # 2. FK columns
    for col in table.columns:
        if col.name in fk_cols:
            _add(col.name)

    # 3. PII-marked columns
    for col in table.columns:
        if col.name in pii_cols:
            _add(col.name)

    # 4. Commented columns
    for col in table.columns:
        if col.name in commented_cols:
            _add(col.name)

    # 5. Remaining by position
    for col in table.columns:
        _add(col.name)

    # If "id" is the only PK and there are more interesting columns, drop it
    if (
        len(selected) > max_cols
        and "id" in selected
        and pk_set == {"id"}
        and len(selected) > 1
    ):
        selected.remove("id")

    return ", ".join(selected[:max_cols])


def _references(table) -> str:
    """Comma-separated list of referred tables from foreign keys."""
    if not table.foreign_keys:
        return "-"
    referred = []
    seen: set[str] = set()
    for fk in table.foreign_keys:
        if fk.referred_table not in seen:
            referred.append(fk.referred_table)
            seen.add(fk.referred_table)
    return ", ".join(referred) if referred else "-"
