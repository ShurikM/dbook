"""Automatic domain classification for database tables."""

from __future__ import annotations

DOMAIN_PATTERNS: dict[str, list[str]] = {
    "auth": [
        "user", "account", "session", "role", "permission",
        "token", "password", "login", "auth",
    ],
    "billing": [
        "invoice", "payment", "charge", "subscription", "plan",
        "price", "billing", "refund", "credit",
    ],
    "orders": [
        "order", "cart", "checkout", "purchase", "shipment",
        "delivery", "return",
    ],
    "catalog": [
        "product", "category", "item", "inventory", "stock",
        "sku", "brand", "review",
    ],
    "analytics": [
        "event", "metric", "funnel", "cohort", "analytics",
        "tracking", "page_view", "click", "conversion", "ab_test",
    ],
    "support": [
        "ticket", "faq", "knowledge", "support", "feedback",
        "escalation", "agent",
    ],
    "warehouse": [
        "warehouse", "location", "picking", "shipping",
        "fulfillment", "supplier",
    ],
    "financial": [
        "revenue", "cost", "budget", "expense", "tax",
        "ledger", "transaction",
    ],
    "hr": [
        "employee", "department", "salary", "leave", "attendance",
    ],
    "content": [
        "article", "post", "comment", "media", "document", "page",
    ],
}


def detect_domain(table_name: str, column_names: list[str]) -> str:
    """Detect the business domain of a table from its name and columns."""
    name_lower = table_name.lower()
    cols_lower = " ".join(c.lower() for c in column_names)

    scores: dict[str, int] = {}
    for domain, keywords in DOMAIN_PATTERNS.items():
        score = 0
        for kw in keywords:
            if kw in name_lower:
                score += 3  # Table name match is strongest signal
            if kw in cols_lower:
                score += 1  # Column name match
        if score > 0:
            scores[domain] = score

    if scores:
        return max(scores, key=scores.get)  # type: ignore[arg-type]
    return "general"
