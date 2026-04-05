"""Generate NAVIGATION.md — the L0 catalog entry point."""

from __future__ import annotations

from dbook.models import BookMeta, TableMeta


# Business-term aliases for mechanical descriptions (base mode).
# Maps table name substrings to common business-language synonyms.
MECHANICAL_ALIASES: dict[str, str] = {
    # Shopping
    "cart": "shopping cart, basket, checkout",
    "order": "purchase, buy, transaction, order history",
    "order_item": "line item, also bought, frequently bought together, recommendation",
    # Billing
    "payment": "transaction, charge, settlement",
    "invoice": "bill, statement, receipt",
    "refund": "money back, return refund, chargeback",
    "subscription": "recurring billing, membership, plan renewal",
    "promotion": "discount, promo code, coupon, voucher, deal",
    "gift_card": "gift card, store credit, gift certificate",
    # Product
    "product": "item, merchandise, goods, SKU, top product, popular items, bestseller",
    "category": "department, product type, classification",
    "review": "star rating, customer review, product rating, feedback, bestseller, popular",
    "inventory": "stock level, availability, in stock, warehouse inventory",
    "image": "product photo, picture, gallery",
    # Customer
    "account": "customer account, user profile, member, verify identity, account verification, security check, customer history",
    "address": "shipping address, delivery address, location, mailing",
    "payment_method": "credit card, saved card, billing method",
    "preference": "settings, notification, language preference",
    # Fulfillment
    "shipment": "delivery tracking, shipping status, carrier, where is my order",
    "return": "return request, RMA, send back",
    "warehouse": "fulfillment center, distribution center",
    "picking_list": "warehouse picking, fulfillment, pack and ship",
    "shipping_rate": "delivery cost, shipping fee, postage",
    # Support
    "ticket": "support case, support cases, support request, support requests, customer history, open case, open cases, help request",
    "ticket_message": "support reply, case comment, conversation",
    "faq": "knowledge base, help article, self-service, how to",
    # Analytics
    "page_view": "page visit, browsing, traffic",
    "search_query": "search history, what people search, search log",
    "click_event": "user interaction, click tracking, engagement",
    "conversion_funnel": "conversion rate, search funnel, purchase conversion, funnel analysis, drop-off",
    "daily_metric": "KPI, dashboard, business metrics, performance",
    "ab_test": "experiment, A/B test, variant test, split test",
}


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


def _mechanical_description(table: TableMeta) -> str:
    """Generate a description that includes business-term aliases."""
    name_lower = table.name.lower()

    # Find matching aliases
    aliases: list[str] = []
    for pattern, alias_text in MECHANICAL_ALIASES.items():
        if pattern in name_lower:
            aliases.append(alias_text)

    # Also detect patterns from column names
    col_names_str = " ".join(c.name for c in table.columns).lower()
    aliases_found = " ".join(aliases).lower()
    if "rating" in col_names_str and "rating" not in aliases_found:
        aliases.append("star rating, product rating")
    if "conversion" in col_names_str and "conversion" not in aliases_found:
        aliases.append("conversion rate")
    if "tracking" in col_names_str and "tracking" not in aliases_found:
        aliases.append("tracking, shipment tracking")

    # Build description from column types
    parts: list[str] = []
    col_names = [c.name for c in table.columns]

    # Detect key patterns
    if any("email" in c for c in col_names):
        parts.append("email")
    if any("status" in c for c in col_names):
        parts.append("status tracking")
    if any("price" in c or "amount" in c or "total" in c for c in col_names):
        parts.append("financial data")
    if any("created_at" in c or "date" in c for c in col_names):
        parts.append("time-series")

    desc = f"{len(table.columns)} cols, {table.row_count or '?'} rows"
    if table.foreign_keys:
        refs = [fk.referred_table for fk in table.foreign_keys]
        desc += f", refs: {', '.join(refs[:2])}"
    if aliases:
        desc += f" | {', '.join(aliases)}"
    if parts:
        desc += f" [{', '.join(parts[:3])}]"

    return desc


def generate_navigation(
    book: BookMeta,
    user_metrics: list | None = None,
) -> str:
    """Generate NAVIGATION.md content from BookMeta.

    Parameters
    ----------
    book : BookMeta
        The introspected database metadata.
    user_metrics : list | None
        Optional list of MetricDefinition objects from metrics.yaml.
    """
    lines = []
    total_tables = sum(len(s.tables) for s in book.schemas.values())
    lines.append(f"# Database Book: {book.dialect}")
    lines.append(f"Compiled: {book.compiled_at:%Y-%m-%dT%H:%M:%SZ} | Mode: {book.mode}")
    lines.append("")

    # Compact table overview
    lines.append(f"## Tables ({total_tables})")
    lines.append("")
    lines.append("| Table | Rows | Key Columns | References | Domain | Description | ~Tok |")
    lines.append("|-------|------|-------------|------------|--------|-------------|------|")

    for schema_name, schema in sorted(book.schemas.items()):
        for table_name, table in sorted(schema.tables.items()):
            qualified_name = f"{schema_name}.{table_name}"
            rows = f"{table.row_count:,}" if table.row_count is not None else "-"
            key_cols = _key_columns(table, max_cols=6)
            refs = _references(table)
            desc = _description(table, book)
            tok = _estimate_table_tokens(table)
            domain = table.domain or "-"
            lines.append(f"| {qualified_name} | {rows} | {key_cols} | {refs} | {domain} | {desc} | {tok} |")

    lines.append("")

    # PII sensitivity summary (if any PII detected)
    pii_tables: list[tuple[str, list[str]]] = []
    for schema_name, schema in sorted(book.schemas.items()):
        for table_name, table in sorted(schema.tables.items()):
            pii_col_names = [col.name for col in table.columns if col.pii_type]
            if pii_col_names:
                pii_tables.append((f"{schema_name}.{table_name}", pii_col_names))

    if pii_tables:
        parts = [f"{tname} ({', '.join(cols)})" for tname, cols in pii_tables]
        lines.append(f"\u26a0 PII detected: {', '.join(parts)}")
        lines.append("")

    # Lineage section
    from dbook.generators.lineage import generate_lineage

    lineage = generate_lineage(book)
    if lineage:
        lines.append(lineage)

    # User-defined business metrics
    if user_metrics:
        lines.append("## Business Metrics (user-defined)")
        lines.append("")
        lines.append("| Metric | SQL | Description |")
        lines.append("|--------|-----|-------------|")
        for metric in user_metrics:
            lines.append(f"| {metric.name} | `{metric.sql}` | {metric.description} |")
        lines.append("")

    # Common query patterns (multi-table join paths)
    patterns = _query_patterns(book)
    if patterns:
        lines.append("## Common Query Patterns")
        for p in patterns:
            lines.append(f"- {p}")
        lines.append("")

    # Navigate instructions
    lines.append("## Navigate")
    lines.append("1. Scan the table above — names are schema-qualified (e.g. `catalog.inventory`)")
    lines.append("2. Check the `~Tok` column to budget your read")
    lines.append("3. Read `schemas/{schema}/{table}.md` for full details")
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


def _description(table: TableMeta, book: BookMeta | None = None, max_len: int = 160) -> str:
    """Return table description for the NAVIGATION.md overview.

    In base mode, uses _mechanical_description with business-term aliases.
    In LLM mode, uses the LLM-generated semantic summary (longer limit to
    preserve business-term vocabulary).
    """
    if book and book.mode in ("llm", "full"):
        desc = table.summary or "-"
        # LLM summaries carry richer business vocabulary; allow more room
        effective_max = max(max_len, 200)
    else:
        desc = _mechanical_description(table)
        effective_max = max_len
    if len(desc) > effective_max:
        desc = desc[: effective_max - 1] + "\u2026"
    return desc


def _query_patterns(book: BookMeta, max_patterns: int = 8) -> list[str]:
    """Find multi-table FK chains (length 2-3 hops) for common join guidance.

    Returns concise lines like:
      ``payments → invoices (invoice_id) → orders (order_id) → users (user_id)``
    """
    # Build schema-qualified FK adjacency: child -> [(parent_qualified, fk_col)]
    # "child references parent" means child has FK pointing to parent.
    adj: dict[str, list[tuple[str, str]]] = {}  # qualified_child -> [(qualified_parent, col)]
    all_qualified: set[str] = set()

    for schema_name, schema in book.schemas.items():
        for table_name, table in schema.tables.items():
            qname = f"{schema_name}.{table_name}"
            all_qualified.add(qname)
            adj.setdefault(qname, [])
            for fk in table.foreign_keys:
                ref_schema = fk.referred_schema or schema_name
                ref_qualified = f"{ref_schema}.{fk.referred_table}"
                col = ", ".join(fk.columns)
                adj[qname].append((ref_qualified, col))

    # Find chains of length >= 2 hops by DFS from every table
    chains: list[list[tuple[str, str]]] = []  # each chain: [(table, fk_col), ...]

    for start in all_qualified:
        parents = adj.get(start, [])
        if not parents:
            continue
        for parent, col in parents:
            # 2-hop: start --(col)--> parent --(col2)--> grandparent
            grandparents = adj.get(parent, [])
            for gp, col2 in grandparents:
                if gp == start:
                    continue  # skip cycles
                chain2: list[tuple[str, str]] = [
                    (start, col), (parent, col2), (gp, ""),
                ]
                chains.append(chain2)
                # 3-hop: extend one more level
                for ggp, col3 in adj.get(gp, []):
                    if ggp in (start, parent):
                        continue
                    chain3: list[tuple[str, str]] = [
                        (start, col), (parent, col2), (gp, col3), (ggp, ""),
                    ]
                    chains.append(chain3)

    if not chains:
        return []

    # Score: longer chains first, then alphabetical for stability
    chains.sort(key=lambda c: (-len(c), c[0][0]))

    # Deduplicate: keep only longest chain for each table-set, and remove
    # chains whose table-set is a strict subset of a longer chain.
    seen_sets: set[frozenset[str]] = set()
    unique: list[list[tuple[str, str]]] = []
    for chain in chains:
        tables = frozenset(t for t, _ in chain)
        if tables not in seen_sets:
            seen_sets.add(tables)
            unique.append(chain)

    # Remove chains that are strict subsets of longer chains
    table_sets = [frozenset(t for t, _ in c) for c in unique]
    filtered: list[list[tuple[str, str]]] = []
    for i, chain in enumerate(unique):
        ts = table_sets[i]
        is_subset = any(
            ts < other for j, other in enumerate(table_sets) if j != i
        )
        if not is_subset:
            filtered.append(chain)

    # Format each chain
    result: list[str] = []
    for chain in filtered[:max_patterns]:
        parts: list[str] = []
        for i, (table, _col) in enumerate(chain):
            if i == 0:
                parts.append(table)
            else:
                parts.append(f"{table} ({chain[i - 1][1]})")
        result.append(" \u2192 ".join(parts))

    return result
