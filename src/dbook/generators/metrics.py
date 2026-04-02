"""Auto-detect common metrics from table structure."""

from __future__ import annotations

from dbook.models import TableMeta


def _metric(label: str, *sql_parts: str) -> str:  # noqa: S608
    """Build a metric string for documentation (not executed)."""
    sql = "".join(sql_parts)
    return label + ": `" + sql + "`"


def _classify_columns(table: TableMeta) -> tuple[list, list, list, list]:
    """Classify columns into numeric, date, enum, and boolean categories.

    Returns (numeric_cols, date_cols, enum_cols, bool_cols).
    """
    numeric_cols = []
    date_cols = []
    enum_cols = []
    bool_cols = []

    fk_column_names: set[str] = set()
    for fk in table.foreign_keys:
        for col_name in fk.columns:
            fk_column_names.add(col_name)

    for col in table.columns:
        if col.is_primary_key:
            continue

        type_upper = col.type.upper()

        # Numeric (non-FK, non-PK)
        if any(
            tok in type_upper
            for tok in ["INT", "FLOAT", "DECIMAL", "NUMERIC", "REAL", "DOUBLE"]
        ):
            if col.name not in fk_column_names:
                numeric_cols.append(col)

        # Date/time
        if any(tok in type_upper for tok in ["DATE", "TIME", "STAMP"]):
            date_cols.append(col)

        # Boolean
        if "BOOL" in type_upper:
            bool_cols.append(col)

        # Enum (has known values)
        if col.name in table.enum_values:
            enum_cols.append(col)

    return numeric_cols, date_cols, enum_cols, bool_cols


def _sum_metrics(table: TableMeta, numeric_cols: list) -> list[str]:
    """SUM metrics for revenue/amount/total/quantity columns."""
    metrics: list[str] = []
    t = table.name

    sum_keywords = {
        "total", "amount", "revenue", "price", "cost", "fee",
        "balance", "subtotal", "tax", "shipping_cost",
        "refund_amount", "unit_price",
    }
    sum_patterns = ["total", "amount", "revenue", "price", "cost"]
    qty_keywords = {"quantity", "count", "qty", "num", "number"}
    qty_patterns = ["quantity", "count"]

    seen: set[str] = set()
    for col in numeric_cols:
        lower = col.name.lower()
        is_sum = lower in sum_keywords or any(p in lower for p in sum_patterns)
        is_qty = lower in qty_keywords or any(p in lower for p in qty_patterns)
        if (is_sum or is_qty) and col.name not in seen:
            seen.add(col.name)
            label = col.name.replace("_", " ").title()
            metrics.append(_metric(
                "Total " + label,
                "SELECT SUM(", col.name, ") FROM ", t,
            ))

    return metrics


def _enum_metrics(table: TableMeta, enum_cols: list) -> list[str]:
    """COUNT by enum/status columns with value filters."""
    metrics: list[str] = []
    t = table.name

    for col in enum_cols:
        values = table.enum_values[col.name]
        label = col.name.replace("_", " ").title()
        metrics.append(_metric(
            "Count by " + label,
            "SELECT ", col.name, ", COUNT(*) FROM ", t,
            " GROUP BY ", col.name,
        ))
        for val in values[:3]:
            metrics.append(_metric(
                label + " = '" + val + "'",
                "SELECT COUNT(*) FROM ", t,
                " WHERE ", col.name, " = '", val, "'",
            ))

    return metrics


def _cross_metrics(
    table: TableMeta,
    numeric_cols: list,
    date_cols: list,
    enum_cols: list,
    bool_cols: list,
) -> list[str]:
    """Cross-column metrics: numeric x enum, numeric x date, bool, FK."""
    metrics: list[str] = []
    t = table.name

    # SUM numeric GROUP BY enum
    if numeric_cols and enum_cols:
        num = numeric_cols[0]
        enum = enum_cols[0]
        num_label = num.name.replace("_", " ").title()
        enum_label = enum.name.replace("_", " ").title()
        metrics.append(_metric(
            num_label + " by " + enum_label,
            "SELECT ", enum.name, ", SUM(", num.name, ") FROM ", t,
            " GROUP BY ", enum.name,
        ))

    # Time series (numeric + date)
    if numeric_cols and date_cols:
        num = numeric_cols[0]
        date_col = date_cols[0]
        num_label = num.name.replace("_", " ").title()
        metrics.append(_metric(
            num_label + " over time",
            "SELECT DATE(", date_col.name, "), SUM(", num.name,
            ") FROM ", t, " GROUP BY DATE(", date_col.name, ")",
        ))

    # Boolean counts
    for col in bool_cols:
        label = col.name.replace("_", " ").replace("is ", "").title()
        metrics.append(_metric(
            "Active " + label,
            "SELECT COUNT(*) FROM ", t,
            " WHERE ", col.name, " = true",
        ))

    # Per-FK aggregation
    if numeric_cols and table.foreign_keys:
        num = numeric_cols[0]
        fk = table.foreign_keys[0]
        fk_col = fk.columns[0]
        ref = fk.referred_table
        num_label = num.name.replace("_", " ").title()
        metrics.append(_metric(
            num_label + " per " + ref,
            "SELECT ", fk_col, ", SUM(", num.name, ") FROM ", t,
            " GROUP BY ", fk_col,
        ))

    return metrics


def generate_metrics(table: TableMeta) -> list[str]:
    """Auto-detect metrics from column patterns, types, enums, and FKs.

    Returns list of metric strings like:
    - "Total revenue: `SELECT SUM(total) FROM orders`"
    - "Count by status: `SELECT status, COUNT(*) FROM orders GROUP BY status`"
    """
    numeric_cols, date_cols, enum_cols, bool_cols = _classify_columns(table)

    metrics: list[str] = []
    metrics.extend(_sum_metrics(table, numeric_cols))
    metrics.extend(_enum_metrics(table, enum_cols))
    metrics.extend(_cross_metrics(
        table, numeric_cols, date_cols, enum_cols, bool_cols,
    ))

    # Cap at 10 metrics
    return metrics[:10]
