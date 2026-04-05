#!/usr/bin/env python3
"""
Seed a Postgres database with realistic e-commerce data for the dbook benchmark.

Inserts golden records (deterministic, scenario-specific) first, then bulk-generates
random rows using Faker to reach the target volumes defined in seed_config.yaml.

Usage:
    python seed/seed_data.py --db-url postgresql://bench:bench@localhost:5433/benchdb
    python seed/seed_data.py --config seed/seed_config.yaml --db-url <url>
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg2  # type: ignore[import-untyped]
import psycopg2.errors  # type: ignore[import-untyped]
import psycopg2.extras  # type: ignore[import-untyped]
import psycopg2.sql  # type: ignore[import-untyped]
import yaml  # type: ignore[import-untyped]
from faker import Faker  # type: ignore[import-untyped]

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FAKER_SEED = 42
BATCH_SIZE = 1000
COPY_THRESHOLD = 50_000  # Use COPY protocol above this row count

# Status enum mappings per table
STATUS_ENUMS: dict[str, dict[str, list[str]]] = {
    "customers.accounts": {
        "status": ["active", "inactive", "suspended", "closed"],
        "tier": ["standard", "prime", "business"],
    },
    "orders.carts": {
        "status": ["active", "abandoned", "converted"],
    },
    "orders.orders": {
        "status": ["pending", "processing", "shipped", "delivered", "cancelled"],
    },
    "orders.order_items": {
        "status": ["pending", "shipped", "delivered", "returned"],
    },
    "orders.shipments": {
        "status": ["preparing", "shipped", "in_transit", "delivered", "failed"],
    },
    "orders.returns": {
        "status": ["requested", "approved", "received", "refunded", "denied"],
    },
    "billing.invoices": {
        "status": ["draft", "sent", "paid", "overdue", "cancelled"],
    },
    "billing.payments": {
        "status": ["pending", "completed", "failed", "refunded"],
    },
    "billing.refunds": {
        "status": ["pending", "processed", "denied"],
    },
    "billing.subscriptions": {
        "status": ["active", "paused", "cancelled", "expired"],
        "billing_cycle": ["monthly", "annual"],
    },
    "billing.gift_cards": {
        "status": ["active", "redeemed", "expired", "disabled"],
    },
    "billing.promotions": {
        "type": ["percentage", "fixed_amount"],
    },
    "billing.payment_disputes": {
        "status": ["open", "under_review", "won", "lost", "resolved"],
        "dispute_type": ["chargeback", "inquiry", "fraud"],
    },
    "billing.ledger_entries": {
        "entry_type": ["charge", "payment", "refund", "credit", "adjustment"],
    },
    "billing.credit_notes": {
        "status": ["draft", "issued", "applied"],
    },
    "support.tickets": {
        "status": ["open", "in_progress", "waiting", "resolved", "closed"],
        "priority": ["low", "medium", "high", "urgent"],
        "category": ["billing", "shipping", "product", "account", "technical", "other"],
        "channel": ["email", "chat", "phone", "web"],
    },
    "support.agents": {
        "status": ["active", "offline", "busy"],
    },
    "warehouse.purchase_orders": {
        "status": ["draft", "submitted", "confirmed", "received", "cancelled"],
    },
    "warehouse.transfers": {
        "status": ["pending", "in_transit", "completed", "cancelled"],
    },
    "warehouse.picking_lists": {
        "status": ["pending", "picking", "packed", "shipped"],
    },
}

CARRIERS = ["UPS", "FedEx", "USPS", "DHL"]

WAREHOUSE_CODES = ["SEA", "LAX", "ORD", "JFK", "DFW", "MIA", "ATL", "DEN"]
WAREHOUSE_CITIES = [
    ("Seattle", "WA"),
    ("Los Angeles", "CA"),
    ("Chicago", "IL"),
    ("New York", "NY"),
    ("Dallas", "TX"),
    ("Miami", "FL"),
    ("Atlanta", "GA"),
    ("Denver", "CO"),
]

# Insertion phases -- FK-dependency order
INSERTION_PHASES = [
    # Phase 1: Root tables
    [
        "customers.accounts", "catalog.brands", "catalog.categories",
        "warehouse.warehouses", "support.teams", "support.sla_policies",
        "billing.promotions", "billing.tax_rates",
    ],
    # Phase 2
    [
        "customers.addresses", "customers.payment_methods", "customers.preferences",
        "customers.wishlists", "customers.login_history", "catalog.products",
        "warehouse.locations", "warehouse.suppliers", "support.agents",
        "support.macros", "support.faq_articles", "support.knowledge_base",
    ],
    # Phase 3
    [
        "catalog.product_images", "catalog.reviews", "catalog.product_attributes",
        "catalog.price_history", "catalog.product_tags", "catalog.related_products",
        "billing.billing_accounts", "billing.subscriptions", "billing.gift_cards",
    ],
    # Phase 4
    [
        "orders.carts", "orders.orders", "analytics.sessions",
    ],
    # Phase 5
    [
        "orders.cart_items", "orders.order_items", "orders.shipments",
        "orders.order_status_history", "orders.order_notes", "orders.saved_for_later",
        "customers.wishlist_items", "warehouse.stock", "catalog.inventory",
        "warehouse.purchase_orders", "warehouse.transfers", "warehouse.picking_lists",
        "support.tickets",
    ],
    # Phase 6
    [
        "billing.invoices", "orders.returns", "orders.order_coupons",
        "warehouse.purchase_order_items", "warehouse.transfer_items",
        "support.ticket_messages", "support.escalations", "support.ticket_tags",
        "billing.subscription_payments", "warehouse.shipping_rates",
    ],
    # Phase 7
    [
        "billing.payments", "billing.credit_notes",
    ],
    # Phase 8
    [
        "billing.refunds", "billing.gift_card_transactions",
        "billing.payment_disputes", "billing.ledger_entries",
    ],
    # Phase 9: Analytics
    [
        "analytics.page_views", "analytics.search_queries", "analytics.click_events",
        "analytics.conversion_funnels", "analytics.daily_metrics", "analytics.ab_tests",
        "analytics.recommendations", "analytics.product_impressions",
        "analytics.cohort_analysis",
    ],
]

# Column lists per table (excluding 'id' which is SERIAL)
TABLE_COLUMNS: dict[str, list[str]] = {
    "customers.accounts": [
        "email", "phone", "name", "password_hash", "status", "tier",
        "created_at", "updated_at",
    ],
    "customers.addresses": [
        "account_id", "label", "street", "city", "state", "zip", "country",
        "is_default", "created_at",
    ],
    "customers.payment_methods": [
        "account_id", "type", "provider", "card_last_four", "expiry",
        "billing_address_id", "is_default", "created_at",
    ],
    "customers.preferences": [
        "account_id", "language", "currency", "timezone",
        "notification_email", "notification_sms", "notification_push",
    ],
    "customers.wishlists": [
        "account_id", "name", "is_public", "created_at",
    ],
    "customers.wishlist_items": [
        "wishlist_id", "product_id", "added_at", "priority",
    ],
    "customers.login_history": [
        "account_id", "ip_address", "user_agent", "device_type",
        "login_at", "success",
    ],
    "catalog.brands": [
        "name", "logo_url", "description",
    ],
    "catalog.categories": [
        "name", "slug", "parent_id", "level", "path", "is_active",
    ],
    "catalog.products": [
        "asin", "title", "description", "brand_id", "category_id",
        "price", "compare_at_price", "weight", "status", "created_at",
    ],
    "catalog.product_images": [
        "product_id", "url", "position", "alt_text", "is_primary",
    ],
    "catalog.reviews": [
        "product_id", "account_id", "rating", "title", "body",
        "verified", "helpful_count", "created_at",
    ],
    "catalog.product_attributes": [
        "product_id", "attribute_name", "attribute_value",
    ],
    "catalog.price_history": [
        "product_id", "price", "changed_at", "changed_by",
    ],
    "catalog.product_tags": [
        "product_id", "tag",
    ],
    "catalog.related_products": [
        "product_id", "related_product_id", "relationship_type",
    ],
    "catalog.inventory": [
        "product_id", "warehouse_id", "quantity", "reserved",
        "reorder_point", "last_restocked_at",
    ],
    "orders.carts": [
        "account_id", "session_id", "status", "created_at",
        "updated_at", "converted_at",
    ],
    "orders.cart_items": [
        "cart_id", "product_id", "quantity", "unit_price", "added_at",
    ],
    "orders.orders": [
        "account_id", "shipping_address_id", "billing_address_id",
        "payment_method_id", "subtotal", "tax", "shipping_cost",
        "discount_amount", "total", "status", "promotion_id",
        "placed_at", "updated_at",
    ],
    "orders.order_items": [
        "order_id", "product_id", "quantity", "unit_price", "status",
    ],
    "orders.shipments": [
        "order_id", "carrier", "tracking_number", "estimated_delivery",
        "shipped_at", "delivered_at", "status",
    ],
    "orders.returns": [
        "order_item_id", "reason_code", "reason_detail", "status",
        "refund_amount", "requested_at", "processed_at",
    ],
    "orders.order_status_history": [
        "order_id", "old_status", "new_status", "changed_at", "changed_by",
    ],
    "orders.order_notes": [
        "order_id", "note_type", "body", "created_by", "created_at",
    ],
    "orders.saved_for_later": [
        "account_id", "product_id", "saved_at",
    ],
    "orders.order_coupons": [
        "order_id", "promotion_id", "discount_amount",
    ],
    "billing.promotions": [
        "code", "name", "type", "value", "min_order", "max_discount",
        "max_uses", "used_count", "valid_from", "valid_until", "is_active",
    ],
    "billing.tax_rates": [
        "jurisdiction", "state", "rate", "effective_from", "effective_until",
    ],
    "billing.billing_accounts": [
        "account_id", "billing_email", "company_name", "tax_id", "net_terms",
    ],
    "billing.invoices": [
        "order_id", "account_id", "invoice_number", "amount", "tax",
        "total", "status", "issued_at", "due_at", "paid_at",
    ],
    "billing.payments": [
        "invoice_id", "payment_method_id", "amount", "currency",
        "processor_ref", "status", "processed_at", "failure_reason",
    ],
    "billing.refunds": [
        "payment_id", "return_id", "amount", "reason", "status",
        "processed_at", "processor_ref",
    ],
    "billing.subscriptions": [
        "account_id", "plan", "price", "billing_cycle", "status",
        "started_at", "cancelled_at", "next_billing_at", "auto_renew",
    ],
    "billing.subscription_payments": [
        "subscription_id", "amount", "period_start", "period_end",
        "status", "processed_at",
    ],
    "billing.gift_cards": [
        "code", "balance", "original_amount", "purchaser_id",
        "recipient_email", "status", "created_at", "expires_at",
    ],
    "billing.gift_card_transactions": [
        "gift_card_id", "order_id", "amount", "transaction_type", "created_at",
    ],
    "billing.credit_notes": [
        "invoice_id", "amount", "reason", "status", "issued_at",
    ],
    "billing.payment_disputes": [
        "payment_id", "dispute_type", "amount", "status",
        "opened_at", "resolved_at", "resolution",
    ],
    "billing.ledger_entries": [
        "account_id", "entry_type", "amount", "balance_after",
        "reference_type", "reference_id", "created_at",
    ],
    "analytics.sessions": [
        "session_id", "account_id", "started_at", "ended_at",
        "page_count", "device_type", "utm_source", "utm_medium",
    ],
    "analytics.page_views": [
        "account_id", "session_id", "page_type", "page_id",
        "referrer", "user_agent", "ip_address", "device_type", "created_at",
    ],
    "analytics.search_queries": [
        "account_id", "query_text", "results_count", "clicked_product_id",
        "category_filter", "min_price", "max_price", "created_at",
    ],
    "analytics.click_events": [
        "account_id", "session_id", "event_type", "element_id",
        "page_url", "product_id", "created_at",
    ],
    "analytics.conversion_funnels": [
        "name", "steps_json", "created_at", "updated_at",
    ],
    "analytics.daily_metrics": [
        "date", "metric_name", "metric_value", "dimension", "dimension_value",
    ],
    "analytics.ab_tests": [
        "name", "variant", "metric_name", "metric_value", "sample_size",
        "significance", "started_at", "ended_at",
    ],
    "analytics.recommendations": [
        "account_id", "product_id", "algorithm", "score", "shown_at", "clicked",
    ],
    "analytics.product_impressions": [
        "product_id", "session_id", "position", "page_type", "clicked",
        "created_at",
    ],
    "analytics.cohort_analysis": [
        "cohort_date", "cohort_size", "period", "retention_rate", "revenue",
    ],
    "warehouse.warehouses": [
        "name", "code", "city", "state", "country", "capacity", "is_active",
    ],
    "warehouse.locations": [
        "warehouse_id", "aisle", "shelf", "bin", "zone",
    ],
    "warehouse.stock": [
        "product_id", "location_id", "quantity", "last_counted_at",
    ],
    "warehouse.suppliers": [
        "name", "contact_email", "phone", "lead_time_days", "rating",
    ],
    "warehouse.purchase_orders": [
        "supplier_id", "warehouse_id", "status", "total_cost",
        "ordered_at", "expected_at", "received_at",
    ],
    "warehouse.purchase_order_items": [
        "purchase_order_id", "product_id", "quantity", "unit_cost",
    ],
    "warehouse.transfers": [
        "from_warehouse_id", "to_warehouse_id", "status",
        "initiated_at", "completed_at",
    ],
    "warehouse.transfer_items": [
        "transfer_id", "product_id", "quantity",
    ],
    "warehouse.shipping_rates": [
        "carrier", "service_level", "weight_min", "weight_max", "zone", "rate",
    ],
    "warehouse.picking_lists": [
        "order_id", "warehouse_id", "status", "assigned_to",
        "created_at", "completed_at",
    ],
    "support.teams": [
        "name", "description", "is_active",
    ],
    "support.agents": [
        "team_id", "name", "email", "status", "max_concurrent_tickets",
    ],
    "support.sla_policies": [
        "name", "priority", "first_response_hours", "resolution_hours", "is_active",
    ],
    "support.tickets": [
        "account_id", "order_id", "category", "priority", "status",
        "subject", "channel", "assigned_agent_id", "created_at",
        "updated_at", "resolved_at",
    ],
    "support.ticket_messages": [
        "ticket_id", "sender_type", "sender_id", "body",
        "is_internal", "created_at",
    ],
    "support.faq_articles": [
        "category", "title", "body", "helpful_count", "view_count",
        "created_at", "updated_at",
    ],
    "support.escalations": [
        "ticket_id", "from_agent_id", "to_agent_id", "reason", "escalated_at",
    ],
    "support.macros": [
        "name", "category", "template_body", "is_active", "usage_count",
    ],
    "support.knowledge_base": [
        "title", "slug", "content", "category", "parent_id",
        "view_count", "created_at", "updated_at",
    ],
    "support.ticket_tags": [
        "ticket_id", "tag",
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

fake = Faker()
Faker.seed(FAKER_SEED)
rng = fake.random  # Faker's internal seeded Random instance

# Per-table counters for unique value generation (avoids unique violations)
_unique_counters: dict[str, int] = {}


def _next_unique(prefix: str) -> int:
    """Return next monotonic counter for a given prefix."""
    _unique_counters[prefix] = _unique_counters.get(prefix, 0) + 1
    return _unique_counters[prefix]


def _next_invoice_number() -> str:
    return f"INV-{_next_unique('invoice'):06d}"


def ts() -> datetime:
    """Random aware datetime in the last year."""
    return fake.date_time_between("-1y", "now", tzinfo=timezone.utc)


def money(lo: float = 5.0, hi: float = 500.0) -> float:
    return round(rng.uniform(lo, hi), 2)


# ---------------------------------------------------------------------------
# ID tracker
# ---------------------------------------------------------------------------

class IDTracker:
    """Track known IDs for each table so FK columns can reference them."""

    def __init__(self) -> None:
        self._ids: dict[str, list[int]] = {}
        self._session_ids: list[str] = []

    def add(self, table: str, row_id: int) -> None:
        self._ids.setdefault(table, []).append(row_id)

    def add_many(self, table: str, ids: list[int]) -> None:
        self._ids.setdefault(table, []).extend(ids)

    def get(self, table: str) -> list[int]:
        return self._ids.get(table, [])

    def pick(self, table: str) -> int:
        ids = self._ids.get(table, [])
        if not ids:
            raise ValueError(f"No IDs tracked for {table}")
        return rng.choice(ids)

    def pick_or_none(self, table: str, null_pct: float = 0.2) -> int | None:
        if rng.random() < null_pct or not self._ids.get(table):
            return None
        return self.pick(table)

    def count(self, table: str) -> int:
        return len(self._ids.get(table, []))

    def add_session_id(self, sid: str) -> None:
        self._session_ids.append(sid)

    def pick_session_id(self) -> str:
        if not self._session_ids:
            return fake.uuid4()
        return rng.choice(self._session_ids)


tracker = IDTracker()


# ---------------------------------------------------------------------------
# FK resolution
# ---------------------------------------------------------------------------

FK_MAP: dict[str, str] = {
    "account_id": "customers.accounts",
    "billing_address_id": "customers.addresses",
    "shipping_address_id": "customers.addresses",
    "payment_method_id": "customers.payment_methods",
    "brand_id": "catalog.brands",
    "category_id": "catalog.categories",
    "product_id": "catalog.products",
    "related_product_id": "catalog.products",
    "clicked_product_id": "catalog.products",
    "order_id": "orders.orders",
    "order_item_id": "orders.order_items",
    "cart_id": "orders.carts",
    "promotion_id": "billing.promotions",
    "invoice_id": "billing.invoices",
    "payment_id": "billing.payments",
    "return_id": "orders.returns",
    "subscription_id": "billing.subscriptions",
    "gift_card_id": "billing.gift_cards",
    "warehouse_id": "warehouse.warehouses",
    "location_id": "warehouse.locations",
    "supplier_id": "warehouse.suppliers",
    "from_warehouse_id": "warehouse.warehouses",
    "to_warehouse_id": "warehouse.warehouses",
    "purchase_order_id": "warehouse.purchase_orders",
    "transfer_id": "warehouse.transfers",
    "team_id": "support.teams",
    "assigned_agent_id": "support.agents",
    "from_agent_id": "support.agents",
    "to_agent_id": "support.agents",
    "ticket_id": "support.tickets",
    "wishlist_id": "customers.wishlists",
    "purchaser_id": "customers.accounts",
}

NULLABLE_FKS = {
    "shipping_address_id", "billing_address_id", "payment_method_id",
    "promotion_id", "return_id", "assigned_agent_id", "from_agent_id",
    "to_agent_id", "order_id", "clicked_product_id", "purchaser_id",
}


def _resolve_fk(table: str, col: str) -> int | None:
    """Resolve a foreign-key column to a known parent ID."""
    if col == "parent_id":
        if table == "catalog.categories":
            return tracker.pick_or_none("catalog.categories", null_pct=0.3)
        if table == "support.knowledge_base":
            return tracker.pick_or_none("support.knowledge_base", null_pct=0.5)
        return None

    parent_table = FK_MAP.get(col)
    if not parent_table:
        return rng.randint(1, 1000)

    if col in NULLABLE_FKS:
        return tracker.pick_or_none(parent_table, null_pct=0.15)

    ids = tracker.get(parent_table)
    if not ids:
        return 1
    return rng.choice(ids)


# ---------------------------------------------------------------------------
# Row generator
# ---------------------------------------------------------------------------

def generate_row(table: str, columns: list[str]) -> dict:
    """Generate a single random row for a table."""
    row: dict = {}
    enums = STATUS_ENUMS.get(table, {})
    for col in columns:
        row[col] = _generate_value(table, col, enums, row)
    return row


def _generate_value(table: str, col: str, enums: dict, row: dict):  # noqa: C901 PLR0911 PLR0912
    """Generate a single column value based on name/type heuristics."""

    # --- Enum columns ---
    if col in enums:
        return rng.choice(enums[col])

    # --- FK columns ---
    if col.endswith("_id"):
        return _resolve_fk(table, col)

    # --- Specific column names ---
    if col == "email":
        # Must be unique per-table — use counter suffix
        n = _next_unique(f"{table}.email")
        return f"user_{n}@example.com"
    if col in ("billing_email", "contact_email", "recipient_email"):
        n = _next_unique(f"{table}.{col}")
        return f"{col.replace('_', '')}_{n}@example.com"
    if col == "phone":
        return fake.phone_number()[:20]
    if col == "password_hash":
        return fake.sha256()
    if col == "ip_address":
        return fake.ipv4()
    if col == "asin":
        n = _next_unique(f"{table}.asin")
        return f"B{n:09d}"
    if col == "card_last_four":
        return f"{rng.randint(1000, 9999)}"
    if col == "expiry":
        return f"{rng.randint(2025, 2030)}-{rng.randint(1, 12):02d}"
    if col == "currency":
        return "USD"
    if col == "session_id":
        return f"sess-{fake.uuid4()}"
    if col == "slug":
        n = _next_unique(f"{table}.slug")
        return f"slug-{n}"
    if col == "tracking_number":
        return fake.bothify("1Z###??#########")
    if col == "carrier":
        return rng.choice(CARRIERS)
    if col == "service_level":
        return rng.choice(["ground", "express", "overnight", "two_day"])
    if col == "country":
        return "US"
    if col == "code":
        n = _next_unique(f"{table}.code")
        if table == "billing.gift_cards":
            return f"GC-{n:06d}"
        return f"PROMO{n:04d}"
    if col == "invoice_number":
        return _next_invoice_number()
    if col == "processor_ref":
        return fake.uuid4()[:24]
    if col == "resolution":
        return rng.choice(["accepted", "rejected", None])
    if col == "algorithm":
        return rng.choice(["collaborative", "content_based", "hybrid", "trending", "popular"])
    if col == "variant":
        return rng.choice(["control", "variant_a", "variant_b"])
    if col == "relationship_type":
        return rng.choice(["similar", "complementary", "bundle", "accessory"])
    if col == "sender_type":
        return rng.choice(["customer", "agent", "system"])
    if col == "sender_id":
        return tracker.pick_or_none("customers.accounts", null_pct=0.3)
    if col == "reason_code":
        return rng.choice(["defective", "wrong_item", "not_as_described", "changed_mind", "late_delivery"])
    if col == "note_type":
        return rng.choice(["internal", "customer", "system"])
    if col == "page_type":
        return rng.choice(["product", "category", "search", "cart", "checkout", "home"])
    if col == "page_id":
        return str(rng.randint(1, 10000))
    if col == "element_id":
        return fake.bothify("btn-???-####")
    if col == "event_type":
        return rng.choice(["click", "add_to_cart", "wishlist", "share", "compare"])
    if col == "page_url":
        return fake.url()
    if col == "referrer":
        return fake.url() if rng.random() > 0.3 else None
    if col == "user_agent":
        return fake.user_agent()[:200]
    if col == "device_type":
        return rng.choice(["desktop", "mobile", "tablet"])
    if col == "utm_source":
        return rng.choice(["google", "facebook", "email", "direct", None])
    if col == "utm_medium":
        return rng.choice(["cpc", "organic", "social", "email", None])
    if col == "metric_name":
        return rng.choice(["revenue", "orders", "sessions", "conversion_rate", "aov", "cart_abandonment"])
    if col == "dimension":
        return rng.choice(["channel", "device", "region", None])
    if col == "dimension_value":
        return rng.choice(["desktop", "mobile", "organic", "paid", "east", "west", None])
    if col == "attribute_name":
        return rng.choice(["color", "size", "material", "weight", "warranty", "brand"])
    if col == "attribute_value":
        return rng.choice(["red", "blue", "XL", "cotton", "2 years", "lightweight", "heavy-duty"])
    if col == "tag":
        return rng.choice([
            "sale", "new", "bestseller", "clearance", "limited", "premium", "eco",
            "urgent", "vip", "follow-up", "escalated", "refund",
        ])
    if col == "label":
        return rng.choice(["home", "work", "billing", "shipping", "other"])
    if col == "type":
        if table == "customers.payment_methods":
            return rng.choice(["credit_card", "debit_card", "paypal", "bank_transfer"])
        return rng.choice(["percentage", "fixed_amount"])
    if col == "provider":
        return rng.choice(["visa", "mastercard", "amex", "paypal", "stripe"])
    if col == "language":
        return rng.choice(["en", "es", "fr", "de", "ru"])
    if col == "timezone":
        return rng.choice([
            "America/New_York", "America/Chicago", "America/Denver",
            "America/Los_Angeles", "Europe/London",
        ])
    if col == "zone":
        if table == "warehouse.locations":
            return rng.choice(["A", "B", "C", "D", "E"])
        return rng.choice(["zone_1", "zone_2", "zone_3", "zone_4", "zone_5"])
    if col == "aisle":
        return rng.choice(list("ABCDEFGH"))
    if col == "shelf":
        return str(rng.randint(1, 20))
    if col == "bin":
        return str(rng.randint(1, 50))
    if col == "jurisdiction":
        return fake.state()
    if col == "state":
        return fake.state_abbr()
    if col == "plan":
        return rng.choice([
            "Basic Monthly", "Premium Monthly", "Basic Annual",
            "Premium Annual", "Enterprise",
        ])
    if col == "reference_type":
        return rng.choice(["order", "payment", "refund", "subscription", None])
    if col == "reference_id":
        return rng.randint(1, 50000)
    if col == "transaction_type":
        return rng.choice(["load", "redeem", "refund"])
    if col == "category":
        if table == "support.tickets":
            return rng.choice(STATUS_ENUMS["support.tickets"]["category"])
        return rng.choice(["general", "billing", "shipping", "product", "technical", "account"])
    if col == "channel":
        return rng.choice(STATUS_ENUMS["support.tickets"]["channel"])
    if col == "category_filter":
        return rng.choice(["electronics", "clothing", "home", "sports", None])
    if col == "assigned_to":
        return fake.name()
    if col == "tax_id":
        return fake.bothify("##-#######")
    if col == "company_name":
        return fake.company()[:200]
    if col == "net_terms":
        return rng.choice([0, 15, 30, 60])
    if col == "path":
        return f"/cat/{rng.randint(1, 10)}/{rng.randint(1, 50)}"
    if col == "level":
        return rng.randint(0, 3)
    if col == "period":
        return rng.randint(0, 12)

    # --- steps_json (JSONB) ---
    if col == "steps_json":
        return json.dumps({"steps": [fake.sentence()[:40] for _ in range(rng.randint(2, 5))]})

    # --- Timestamps ---
    nullable_ts = {
        "updated_at", "cancelled_at", "delivered_at", "resolved_at",
        "processed_at", "completed_at", "ended_at", "converted_at",
        "paid_at", "received_at", "last_counted_at", "last_restocked_at",
    }
    if col.endswith("_at"):
        if col in nullable_ts and rng.random() < 0.3:
            return None
        return ts()

    if col in ("estimated_delivery", "expected_at"):
        return fake.date_between("+1d", "+30d")
    if col in ("date", "cohort_date"):
        return fake.date_between("-1y", "today")
    if col in ("effective_from", "period_start"):
        return fake.date_between("-2y", "today")
    if col in ("effective_until", "period_end"):
        return fake.date_between("today", "+2y") if rng.random() > 0.3 else None
    if col == "valid_from":
        return fake.date_time_between("-6M", "now", tzinfo=timezone.utc)
    if col == "valid_until":
        return fake.date_time_between("now", "+6M", tzinfo=timezone.utc) if rng.random() > 0.2 else None

    # --- Money/numeric ---
    money_cols = {
        "price", "amount", "total", "subtotal", "unit_price", "unit_cost",
        "total_cost", "compare_at_price", "refund_amount", "discount_amount",
        "shipping_cost", "min_order", "max_discount", "original_amount",
        "balance", "balance_after", "revenue",
    }
    if col in money_cols:
        return money()
    if col == "tax":
        return money(0, 50)
    if col == "rate":
        if table == "billing.tax_rates":
            return round(rng.uniform(0.0, 0.12), 4)
        if table == "warehouse.shipping_rates":
            return money(3, 80)
        return round(rng.uniform(0.01, 0.15), 4)
    if col == "value":
        return money(5, 100)
    if col == "significance":
        return round(rng.uniform(0.0, 1.0), 4)
    if col == "score":
        return round(rng.uniform(0.0, 1.0), 4)
    if col == "metric_value":
        return round(rng.uniform(0.0, 100000.0), 4)
    if col == "retention_rate":
        return round(rng.uniform(0.0, 1.0), 4)
    if col in ("weight", "weight_min", "weight_max"):
        return round(rng.uniform(0.1, 50.0), 2)
    if col in ("min_price", "max_price"):
        return money(1, 1000) if rng.random() > 0.4 else None
    if col == "capacity":
        return rng.randint(1000, 100000)

    # --- Integers ---
    if col in ("quantity", "reserved"):
        return rng.randint(0, 200)
    if col in ("count", "page_count"):
        return rng.randint(1, 50)
    if col == "reorder_point":
        return rng.randint(5, 50)
    if col == "max_uses":
        return rng.randint(100, 10000) if rng.random() > 0.3 else None
    if col == "used_count":
        return rng.randint(0, 500)
    if col == "rating":
        if table == "warehouse.suppliers":
            return round(rng.uniform(1.0, 5.0), 2)
        return rng.randint(1, 5)
    if col == "position":
        return rng.randint(0, 10)
    if col in ("helpful_count", "view_count", "usage_count"):
        return rng.randint(0, 500)
    if col == "results_count":
        return rng.randint(0, 200)
    if col == "first_response_hours":
        return rng.choice([1, 2, 4, 8, 24])
    if col == "resolution_hours":
        return rng.choice([4, 8, 24, 48, 72])
    if col == "lead_time_days":
        return rng.randint(1, 45)
    if col == "max_concurrent_tickets":
        return rng.randint(5, 20)
    if col == "sample_size":
        return rng.randint(100, 50000)
    if col == "cohort_size":
        return rng.randint(50, 5000)
    if col == "priority":
        if table == "customers.wishlist_items":
            return rng.randint(0, 5)
        return rng.choice(STATUS_ENUMS.get(table, {}).get("priority", ["medium"]))

    # --- Booleans ---
    bool_cols = {
        "success", "verified", "clicked", "auto_renew",
        "notification_email", "notification_sms", "notification_push",
        "is_internal",
    }
    if col.startswith("is_") or col in bool_cols:
        return rng.choice([True, False])

    # --- Text fields ---
    if col in ("description", "body", "content", "template_body", "reason",
               "reason_detail", "failure_reason"):
        return fake.paragraph()
    if col in ("title", "subject"):
        return fake.sentence()[:200]
    if col == "name":
        person_tables = {"customers.accounts", "support.agents"}
        if table in person_tables:
            return fake.name()
        return fake.catch_phrase()[:100]
    if col == "street":
        return fake.street_address()
    if col == "city":
        return fake.city()
    if col == "zip":
        return fake.zipcode()
    if col in ("url", "logo_url"):
        return fake.url()
    if col == "alt_text":
        return fake.sentence()[:200]
    if col == "old_status":
        return rng.choice(["pending", "processing", "shipped", None])
    if col == "new_status":
        return rng.choice(["processing", "shipped", "delivered", "cancelled"])
    if col in ("changed_by", "created_by"):
        return rng.choice(["system", "admin", fake.name()])
    if col == "query_text":
        return fake.sentence()[:100]

    # Fallback
    return fake.pystr(max_chars=20)


# ---------------------------------------------------------------------------
# Golden record insertion
# ---------------------------------------------------------------------------

def insert_golden_records(conn) -> None:
    """Insert specific records that benchmark scenarios reference."""
    cur = conn.cursor()

    now = datetime.now(timezone.utc)

    # ---------------------------------------------------------------
    # 8 Warehouses
    # ---------------------------------------------------------------
    for i, (code, (city, state)) in enumerate(
        zip(WAREHOUSE_CODES, WAREHOUSE_CITIES), start=1
    ):
        cur.execute(
            """INSERT INTO warehouse.warehouses
                   (id, name, code, city, state, country, capacity, is_active)
               VALUES (%s, %s, %s, %s, %s, 'US', %s, true)""",
            (i, f"{city} Fulfillment Center", code, city, state,
             rng.randint(10000, 80000)),
        )
        tracker.add("warehouse.warehouses", i)

    # ---------------------------------------------------------------
    # Accounts
    # ---------------------------------------------------------------
    cur.execute(
        """INSERT INTO customers.accounts
               (id, email, phone, name, password_hash, status, tier, created_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (4521, "alice@example.com", "+1-555-0101", "Alice Johnson",
         fake.sha256(), "active", "prime", now - timedelta(days=400)),
    )
    tracker.add("customers.accounts", 4521)

    cur.execute(
        """INSERT INTO customers.accounts
               (id, email, phone, name, password_hash, status, tier, created_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (1200, "bob.smith@example.com", "+1-555-0202", "Bob Smith",
         fake.sha256(), "active", "standard", now - timedelta(days=300)),
    )
    tracker.add("customers.accounts", 1200)

    # Addresses for golden accounts
    cur.execute(
        """INSERT INTO customers.addresses
               (id, account_id, label, street, city, state, zip, country, is_default)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (1, 4521, "home", "123 Main St", "Seattle", "WA", "98101", "US", True),
    )
    tracker.add("customers.addresses", 1)

    cur.execute(
        """INSERT INTO customers.addresses
               (id, account_id, label, street, city, state, zip, country, is_default)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (2, 1200, "home", "456 Oak Ave", "Portland", "OR", "97201", "US", True),
    )
    tracker.add("customers.addresses", 2)

    # Payment methods
    cur.execute(
        """INSERT INTO customers.payment_methods
               (id, account_id, type, provider, card_last_four, expiry, is_default)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (1, 4521, "credit_card", "visa", "4242", "2027-12", True),
    )
    tracker.add("customers.payment_methods", 1)

    cur.execute(
        """INSERT INTO customers.payment_methods
               (id, account_id, type, provider, card_last_four, expiry, is_default)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (2, 1200, "credit_card", "mastercard", "1234", "2028-06", True),
    )
    tracker.add("customers.payment_methods", 2)

    # ---------------------------------------------------------------
    # Brands + Categories
    # ---------------------------------------------------------------
    cur.execute(
        """INSERT INTO catalog.brands (id, name, logo_url, description)
           VALUES (%s, %s, %s, %s)""",
        (1, "Sony", "https://sony.com/logo.png", "Sony Corporation"),
    )
    tracker.add("catalog.brands", 1)

    cur.execute(
        """INSERT INTO catalog.categories (id, name, slug, parent_id, level, path, is_active)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (1, "Electronics", "electronics", None, 0, "/electronics", True),
    )
    tracker.add("catalog.categories", 1)

    cur.execute(
        """INSERT INTO catalog.categories (id, name, slug, parent_id, level, path, is_active)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (2, "Headphones", "headphones", 1, 1, "/electronics/headphones", True),
    )
    tracker.add("catalog.categories", 2)

    # ---------------------------------------------------------------
    # Product id=234
    # ---------------------------------------------------------------
    cur.execute(
        """INSERT INTO catalog.products
               (id, asin, title, description, brand_id, category_id,
                price, compare_at_price, weight, status, created_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (234, "B00X4WHP5E", "Sony WH-1000XM5", "Premium noise-cancelling headphones",
         1, 2, 348.00, 399.99, 0.55, "active", now - timedelta(days=180)),
    )
    tracker.add("catalog.products", 234)

    # Extra products for cart items
    for pid, asin_val, title in [
        (235, "B00Y5THP6F", "Anker USB-C Cable 3-pack"),
        (236, "B00Z6UJP7G", "Logitech MX Keys"),
    ]:
        cur.execute(
            """INSERT INTO catalog.products
                   (id, asin, title, description, brand_id, category_id,
                    price, status, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (pid, asin_val, title, fake.paragraph(), 1, 1,
             round(rng.uniform(15, 200), 2), "active", now - timedelta(days=90)),
        )
        tracker.add("catalog.products", pid)

    # ---------------------------------------------------------------
    # Brand: JBL (for wireless headphones)
    # ---------------------------------------------------------------
    cur.execute(
        """INSERT INTO catalog.brands (id, name, logo_url, description)
           VALUES (%s, %s, %s, %s)""",
        (2, "JBL", "https://jbl.com/logo.png", "JBL Audio"),
    )
    tracker.add("catalog.brands", 2)

    # ---------------------------------------------------------------
    # Wireless headphones products for S5 scenario (price < $100)
    # ---------------------------------------------------------------
    cur.execute(
        """INSERT INTO catalog.products
               (id, asin, title, description, brand_id, category_id,
                price, compare_at_price, weight, status, created_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (501, "B01WHPJBL1", "JBL Tune 520BT Wireless Headphones",
         "On-ear wireless Bluetooth headphones with JBL Pure Bass sound",
         2, 2, 49.95, 59.99, 0.15, "active", now - timedelta(days=120)),
    )
    tracker.add("catalog.products", 501)

    cur.execute(
        """INSERT INTO catalog.products
               (id, asin, title, description, brand_id, category_id,
                price, compare_at_price, weight, status, created_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (502, "B02WHPSNX2", "Sony WH-CH520 Wireless Headphones",
         "Lightweight on-ear wireless headphones with 50-hour battery",
         1, 2, 79.99, 99.99, 0.14, "active", now - timedelta(days=90)),
    )
    tracker.add("catalog.products", 502)

    # ---------------------------------------------------------------
    # Reviews for wireless headphones (S5 scenario)
    # ---------------------------------------------------------------
    reviews_golden = [
        (5001, 501, 4521, 5, "Great value wireless headphones",
         "Amazing sound quality for the price. Battery lasts forever.", True, 12),
        (5002, 501, 1200, 4, "Solid budget pick",
         "Comfortable fit, good bass. Minor Bluetooth lag.", True, 5),
        (5003, 502, 4521, 4, "Lightweight and long battery",
         "Perfect for commuting. Sound is clear but not audiophile-level.", True, 8),
        (5004, 502, 1200, 5, "Best under $100",
         "Sony quality at a great price. Very comfortable for long sessions.", True, 15),
    ]
    for rev_id, prod_id, acct_id, rating, title, body, verified, helpful in reviews_golden:
        cur.execute(
            """INSERT INTO catalog.reviews
                   (id, product_id, account_id, rating, title, body,
                    verified, helpful_count, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (rev_id, prod_id, acct_id, rating, title, body, verified, helpful,
             now - timedelta(days=rng.randint(5, 60))),
        )
        tracker.add("catalog.reviews", rev_id)

    # ---------------------------------------------------------------
    # Promotion SPRING25
    # ---------------------------------------------------------------
    cur.execute(
        """INSERT INTO billing.promotions
               (id, code, name, type, value, min_order, max_discount,
                max_uses, used_count, valid_from, valid_until, is_active)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (1, "SPRING25", "Spring 25% Off", "percentage", 25.00, 50.00, 100.00,
         1000, 142, now - timedelta(days=30), now + timedelta(days=60), True),
    )
    tracker.add("billing.promotions", 1)

    # ---------------------------------------------------------------
    # Orders for account 4521
    # ---------------------------------------------------------------
    orders_golden = [
        (34567, 4521, 149.99, 12.74, 6.00, 0.00, 168.73, "shipped", 5),
        (45678, 4521, 89.99, 7.65, 0.00, 0.00, 97.64, "delivered", 15),
        (78234, 4521, 189.99, 16.14, 6.00, 0.00, 212.13, "pending", 1),
    ]
    for oid, aid, sub, tax, ship, disc, total, status, days_ago in orders_golden:
        cur.execute(
            """INSERT INTO orders.orders
                   (id, account_id, shipping_address_id, billing_address_id,
                    payment_method_id, subtotal, tax, shipping_cost,
                    discount_amount, total, status, placed_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (oid, aid, 1, 1, 1, sub, tax, ship, disc, total, status,
             now - timedelta(days=days_ago)),
        )
        tracker.add("orders.orders", oid)

    # 3 orders for account 1200 (for invoices)
    for oid, days_ago, status, total in [
        (90001, 80, "delivered", 120.50),
        (90002, 50, "delivered", 85.30),
        (90003, 20, "shipped", 210.00),
    ]:
        cur.execute(
            """INSERT INTO orders.orders
                   (id, account_id, shipping_address_id, billing_address_id,
                    payment_method_id, subtotal, tax, shipping_cost,
                    discount_amount, total, status, placed_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (oid, 1200, 2, 2, 2, round(total * 0.85, 2), round(total * 0.08, 2),
             round(total * 0.07, 2), 0.00, total, status,
             now - timedelta(days=days_ago)),
        )
        tracker.add("orders.orders", oid)

    # ---------------------------------------------------------------
    # Shipments
    # ---------------------------------------------------------------
    cur.execute(
        """INSERT INTO orders.shipments
               (id, order_id, carrier, tracking_number,
                estimated_delivery, shipped_at, delivered_at, status)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
        (1, 34567, "UPS", "1Z999AA10123456784",
         (now + timedelta(days=2)).date(), now - timedelta(days=3), None, "in_transit"),
    )
    tracker.add("orders.shipments", 1)

    cur.execute(
        """INSERT INTO orders.shipments
               (id, order_id, carrier, tracking_number,
                estimated_delivery, shipped_at, delivered_at, status)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
        (2, 45678, "FedEx", "7489302756",
         (now - timedelta(days=10)).date(), now - timedelta(days=13),
         now - timedelta(days=10), "delivered"),
    )
    tracker.add("orders.shipments", 2)

    # ---------------------------------------------------------------
    # Order item for order 45678 -- product 234, $89.99
    # ---------------------------------------------------------------
    cur.execute(
        """INSERT INTO orders.order_items (id, order_id, product_id, quantity, unit_price, status)
           VALUES (%s,%s,%s,%s,%s,%s)""",
        (1, 45678, 234, 1, 89.99, "delivered"),
    )
    tracker.add("orders.order_items", 1)

    # ---------------------------------------------------------------
    # Invoice 9001 (order 34567, paid) + 2 payments
    # ---------------------------------------------------------------
    cur.execute(
        """INSERT INTO billing.invoices
               (id, order_id, account_id, invoice_number, amount, tax, total,
                status, issued_at, due_at, paid_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (9001, 34567, 4521, "INV-009001", 149.99, 18.74, 168.73,
         "paid", datetime(2026, 3, 14, tzinfo=timezone.utc),
         now, datetime(2026, 3, 15, tzinfo=timezone.utc)),
    )
    tracker.add("billing.invoices", 9001)

    cur.execute(
        """INSERT INTO billing.payments
               (id, invoice_id, payment_method_id, amount, currency,
                processor_ref, status, processed_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
        (7001, 9001, 1, 89.99, "USD", "ch_abc123", "completed",
         datetime(2026, 3, 15, tzinfo=timezone.utc)),
    )
    tracker.add("billing.payments", 7001)

    cur.execute(
        """INSERT INTO billing.payments
               (id, invoice_id, payment_method_id, amount, currency,
                processor_ref, status, processed_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
        (7002, 9001, 1, 78.74, "USD", "ch_def456", "completed",
         datetime(2026, 3, 15, tzinfo=timezone.utc)),
    )
    tracker.add("billing.payments", 7002)

    # ---------------------------------------------------------------
    # Invoice 9002 (order 78234, paid) + 2 payments  — C5 "charged twice"
    # Order 78234 placed 2026-04-03, total $212.13
    # ---------------------------------------------------------------
    cur.execute(
        """INSERT INTO billing.invoices
               (id, order_id, account_id, invoice_number, amount, tax, total,
                status, issued_at, due_at, paid_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (9002, 78234, 4521, "INV-009002", 189.99, 22.14, 212.13,
         "paid",
         datetime(2026, 4, 3, 10, 0, 0, tzinfo=timezone.utc),
         datetime(2026, 4, 17, 10, 0, 0, tzinfo=timezone.utc),
         datetime(2026, 4, 3, 10, 5, 0, tzinfo=timezone.utc)),
    )
    tracker.add("billing.invoices", 9002)

    # First charge — legitimate payment
    cur.execute(
        """INSERT INTO billing.payments
               (id, invoice_id, payment_method_id, amount, currency,
                processor_ref, status, processed_at, failure_reason)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (7003, 9002, 1, 212.13, "USD", "ch_dup_aaa111",
         "completed",
         datetime(2026, 4, 3, 10, 5, 0, tzinfo=timezone.utc),
         None),
    )
    tracker.add("billing.payments", 7003)

    # Second charge — duplicate (the "charged twice" problem)
    cur.execute(
        """INSERT INTO billing.payments
               (id, invoice_id, payment_method_id, amount, currency,
                processor_ref, status, processed_at, failure_reason)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (7004, 9002, 1, 212.13, "USD", "ch_dup_bbb222",
         "completed",
         datetime(2026, 4, 3, 10, 5, 12, tzinfo=timezone.utc),
         None),
    )
    tracker.add("billing.payments", 7004)

    # ---------------------------------------------------------------
    # 3 invoices for account 1200 (Jan/Feb/Mar 2026)
    # ---------------------------------------------------------------
    invoice_data = [
        (9101, 90001, "INV-009101", 102.43, 9.64, 120.50, "paid",
         datetime(2026, 1, 15, tzinfo=timezone.utc),
         datetime(2026, 2, 15, tzinfo=timezone.utc),
         datetime(2026, 1, 20, tzinfo=timezone.utc)),
        (9102, 90002, "INV-009102", 72.51, 6.82, 85.30, "sent",
         datetime(2026, 2, 12, tzinfo=timezone.utc),
         datetime(2026, 3, 12, tzinfo=timezone.utc),
         None),
        (9103, 90003, "INV-009103", 178.50, 15.12, 210.00, "overdue",
         datetime(2026, 3, 5, tzinfo=timezone.utc),
         datetime(2026, 3, 20, tzinfo=timezone.utc),
         None),
    ]
    for inv_id, o_id, inv_num, amt, tax, total, st, issued, due, paid in invoice_data:
        cur.execute(
            """INSERT INTO billing.invoices
                   (id, order_id, account_id, invoice_number, amount, tax, total,
                    status, issued_at, due_at, paid_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (inv_id, o_id, 1200, inv_num, amt, tax, total, st, issued, due, paid),
        )
        tracker.add("billing.invoices", inv_id)

    # ---------------------------------------------------------------
    # Subscription id=42
    # ---------------------------------------------------------------
    cur.execute(
        """INSERT INTO billing.subscriptions
               (id, account_id, plan, price, billing_cycle, status,
                started_at, next_billing_at, auto_renew)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (42, 4521, "Premium Annual", 139.00, "annual", "active",
         now - timedelta(days=200), now + timedelta(days=165), True),
    )
    tracker.add("billing.subscriptions", 42)

    # ---------------------------------------------------------------
    # Gift card GC-4455 + 3 transactions
    # ---------------------------------------------------------------
    cur.execute(
        """INSERT INTO billing.gift_cards
               (id, code, balance, original_amount, purchaser_id,
                recipient_email, status, created_at, expires_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (1, "GC-4455", 42.50, 100.00, 4521, "friend@example.com", "active",
         now - timedelta(days=60), now + timedelta(days=305)),
    )
    tracker.add("billing.gift_cards", 1)

    gc_txns = [
        (1, 1, None, 100.00, "load", now - timedelta(days=60)),
        (2, 1, None, -45.00, "redeem", now - timedelta(days=30)),
        (3, 1, None, -12.50, "redeem", now - timedelta(days=10)),
    ]
    for txn_id, gc_id, o_id, amt, txn_type, created in gc_txns:
        cur.execute(
            """INSERT INTO billing.gift_card_transactions
                   (id, gift_card_id, order_id, amount, transaction_type, created_at)
               VALUES (%s,%s,%s,%s,%s,%s)""",
            (txn_id, gc_id, o_id, amt, txn_type, created),
        )
        tracker.add("billing.gift_card_transactions", txn_id)

    # ---------------------------------------------------------------
    # Cart id=9876 + 3 cart items
    # ---------------------------------------------------------------
    cur.execute(
        """INSERT INTO orders.carts (id, account_id, session_id, status, created_at)
           VALUES (%s,%s,%s,%s,%s)""",
        (9876, 4521, "sess-golden-4521", "active", now - timedelta(hours=3)),
    )
    tracker.add("orders.carts", 9876)

    cart_items = [
        (1, 9876, 234, 1, 348.00, now - timedelta(hours=3)),
        (2, 9876, 235, 2, 15.99, now - timedelta(hours=2)),
        (3, 9876, 236, 1, 119.99, now - timedelta(hours=1)),
    ]
    for ci_id, c_id, p_id, qty, price, added in cart_items:
        cur.execute(
            """INSERT INTO orders.cart_items
                   (id, cart_id, product_id, quantity, unit_price, added_at)
               VALUES (%s,%s,%s,%s,%s,%s)""",
            (ci_id, c_id, p_id, qty, price, added),
        )
        tracker.add("orders.cart_items", ci_id)

    # ---------------------------------------------------------------
    # Support: teams + agents + SLA
    # ---------------------------------------------------------------
    cur.execute(
        """INSERT INTO support.teams (id, name, description, is_active)
           VALUES (%s,%s,%s,%s)""",
        (1, "Customer Support", "General customer support team", True),
    )
    tracker.add("support.teams", 1)

    cur.execute(
        """INSERT INTO support.agents (id, team_id, name, email, status, max_concurrent_tickets)
           VALUES (%s,%s,%s,%s,%s,%s)""",
        (1, 1, "Agent Smith", "agent.smith@support.com", "active", 15),
    )
    tracker.add("support.agents", 1)

    cur.execute(
        """INSERT INTO support.sla_policies
               (id, name, priority, first_response_hours, resolution_hours, is_active)
           VALUES (%s,%s,%s,%s,%s,%s)""",
        (1, "High Priority SLA", "high", 2, 24, True),
    )
    tracker.add("support.sla_policies", 1)

    # ---------------------------------------------------------------
    # Tickets: 8901 + 2 more for account 4521 in last 2 weeks
    # ---------------------------------------------------------------
    tickets_golden = [
        (8901, 4521, 34567, "shipping", "high", "open",
         "Where is my order #34567?", "web", 1, now - timedelta(days=2)),
        (8902, 4521, 45678, "product", "medium", "resolved",
         "Product quality concern", "email", 1, now - timedelta(days=10)),
        (8903, 4521, None, "billing", "low", "in_progress",
         "Invoice question", "chat", 1, now - timedelta(days=5)),
    ]
    for t_id, a_id, o_id, cat, pri, st, subj, ch, agent, created in tickets_golden:
        cur.execute(
            """INSERT INTO support.tickets
                   (id, account_id, order_id, category, priority, status,
                    subject, channel, assigned_agent_id, created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (t_id, a_id, o_id, cat, pri, st, subj, ch, agent, created),
        )
        tracker.add("support.tickets", t_id)

    # ---------------------------------------------------------------
    # Locations + Inventory for product 234 across 3 warehouses
    # ---------------------------------------------------------------
    for loc_id, wh_id in [(1, 1), (2, 3), (3, 5)]:
        cur.execute(
            """INSERT INTO warehouse.locations (id, warehouse_id, aisle, shelf, bin, zone)
               VALUES (%s,%s,%s,%s,%s,%s)""",
            (loc_id, wh_id, "A", "1", str(loc_id), "A"),
        )
        tracker.add("warehouse.locations", loc_id)

    for inv_id, wh_id, qty in [(1, 1, 45), (2, 3, 120), (3, 5, 30)]:
        cur.execute(
            """INSERT INTO catalog.inventory
                   (id, product_id, warehouse_id, quantity, reserved,
                    reorder_point, last_restocked_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (inv_id, 234, wh_id, qty, rng.randint(0, 10), 10,
             now - timedelta(days=rng.randint(1, 30))),
        )
        tracker.add("catalog.inventory", inv_id)

    conn.commit()
    log.info("Golden records inserted.")


# ---------------------------------------------------------------------------
# Sequence reset
# ---------------------------------------------------------------------------

def reset_sequences(conn) -> None:
    """Reset all SERIAL sequences to be above the max ID in each table."""
    cur = conn.cursor()
    all_tables = [t for phase in INSERTION_PHASES for t in phase]
    for full_table in all_tables:
        schema_name, table_name = full_table.split(".")
        try:
            cur.execute(
                psycopg2.sql.SQL("SELECT MAX(id) FROM {}.{}").format(
                    psycopg2.sql.Identifier(schema_name),
                    psycopg2.sql.Identifier(table_name),
                )
            )
            row = cur.fetchone()
            max_id = row[0] if row else None
            if max_id is not None:
                cur.execute(
                    psycopg2.sql.SQL("SELECT setval({}, %s)").format(
                        psycopg2.sql.Literal(f"{schema_name}.{table_name}_id_seq"),
                    ),
                    (max_id,),
                )
        except Exception:
            conn.rollback()
            continue
    conn.commit()
    log.info("Sequences reset.")


# ---------------------------------------------------------------------------
# COPY protocol for large tables
# ---------------------------------------------------------------------------

def _escape_copy_value(val) -> str:
    """Escape a value for PostgreSQL COPY TSV format."""
    if val is None:
        return "\\N"
    if isinstance(val, bool):
        return "t" if val else "f"
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, date):
        return val.isoformat()
    s = str(val)
    s = s.replace("\\", "\\\\")
    s = s.replace("\t", "\\t")
    s = s.replace("\n", "\\n")
    s = s.replace("\r", "\\r")
    return s


def _build_qualified_table(full_table: str) -> psycopg2.sql.Composed:
    """Build a schema-qualified SQL identifier from 'schema.table' string."""
    schema_name, table_name = full_table.split(".")
    return psycopg2.sql.SQL("{}.{}").format(
        psycopg2.sql.Identifier(schema_name),
        psycopg2.sql.Identifier(table_name),
    )


def _insert_copy(conn, full_table: str, columns: list[str], rows: list[dict]) -> list[int]:
    """Insert rows using PostgreSQL COPY protocol (high performance).
    Falls back to batch insert on failure. Returns new IDs."""
    if not rows:
        return []
    buf = io.StringIO()
    for row in rows:
        vals = [_escape_copy_value(row[c]) for c in columns]
        buf.write("\t".join(vals) + "\n")
    buf.seek(0)
    col_ids = psycopg2.sql.SQL(", ").join(psycopg2.sql.Identifier(c) for c in columns)
    copy_sql = psycopg2.sql.SQL(
        "COPY {tbl} ({cols}) FROM STDIN WITH (FORMAT text, NULL '\\N')"
    ).format(tbl=_build_qualified_table(full_table), cols=col_ids)
    cur = conn.cursor()
    try:
        cur.copy_expert(copy_sql.as_string(conn), buf)
        conn.commit()
        return []  # IDs will be fetched by caller via SELECT
    except (psycopg2.errors.UniqueViolation, psycopg2.IntegrityError) as e:
        conn.rollback()
        log.warning("  COPY failed for %s (%s), falling back to batch insert...",
                     full_table, type(e).__name__)
        return _insert_batch(conn, full_table, columns, rows)


def _insert_batch(conn, full_table: str, columns: list[str], rows: list[dict]) -> list[int]:
    """Insert rows in batches. On batch failure, falls back to row-by-row. Returns new IDs."""
    if not rows:
        return []
    new_ids: list[int] = []
    failed_count = 0
    col_ids = psycopg2.sql.SQL(", ").join(psycopg2.sql.Identifier(c) for c in columns)
    placeholders = psycopg2.sql.SQL(", ").join(psycopg2.sql.Placeholder() for _ in columns)
    stmt = psycopg2.sql.SQL("INSERT INTO {tbl} ({cols}) VALUES ({phs}) RETURNING id").format(
        tbl=_build_qualified_table(full_table),
        cols=col_ids,
        phs=placeholders,
    )

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        try:
            cur = conn.cursor()
            for row in batch:
                vals = tuple(row[c] for c in columns)
                cur.execute(stmt, vals)
                result = cur.fetchone()
                rid = result[0] if result else None
                if rid is not None:
                    new_ids.append(rid)
            conn.commit()
        except (psycopg2.errors.UniqueViolation, psycopg2.IntegrityError):
            conn.rollback()
            # Fall back to row-by-row insertion for this batch
            for row in batch:
                try:
                    cur = conn.cursor()
                    vals = tuple(row[c] for c in columns)
                    cur.execute(stmt, vals)
                    result = cur.fetchone()
                    rid = result[0] if result else None
                    if rid is not None:
                        new_ids.append(rid)
                    conn.commit()
                except (psycopg2.errors.UniqueViolation, psycopg2.IntegrityError):
                    conn.rollback()
                    failed_count += 1
                except Exception:
                    conn.rollback()
                    failed_count += 1

    if failed_count > 0:
        log.warning("  %s: %d inserted, %d failed (individual insert fallback)",
                     full_table, len(new_ids), failed_count)
    else:
        log.info("  %s: %d inserted successfully", full_table, len(new_ids))
    return new_ids


# ---------------------------------------------------------------------------
# Per-table seeding
# ---------------------------------------------------------------------------

def _reset_sequence(conn, full_table: str) -> None:
    """Reset SERIAL sequence for a table to be above the max existing ID."""
    schema_name, table_name = full_table.split(".")
    try:
        cur = conn.cursor()
        cur.execute(
            psycopg2.sql.SQL("SELECT MAX(id) FROM {}.{}").format(
                psycopg2.sql.Identifier(schema_name),
                psycopg2.sql.Identifier(table_name),
            )
        )
        row = cur.fetchone()
        max_id = row[0] if row else None
        if max_id is not None:
            cur.execute(
                psycopg2.sql.SQL("SELECT setval({}, %s)").format(
                    psycopg2.sql.Literal(f"{schema_name}.{table_name}_id_seq"),
                ),
                (max_id,),
            )
        conn.commit()
    except Exception:
        conn.rollback()


def _refresh_tracker_from_db(conn, full_table: str) -> None:
    """Replace tracker IDs for a table with actual IDs from the database."""
    cur = conn.cursor()
    cur.execute(
        psycopg2.sql.SQL("SELECT id FROM {tbl}").format(
            tbl=_build_qualified_table(full_table),
        )
    )
    all_ids = [r[0] for r in cur.fetchall()]
    # Replace tracker state with actual DB state
    tracker._ids[full_table] = all_ids


def _generate_account_rows(remaining: int, columns: list[str]) -> list[dict]:
    """Generate account rows with explicit unique IDs to avoid SERIAL conflicts.

    Golden record IDs (4521, 1200) are skipped. IDs start from 1.
    Each row gets a guaranteed-unique email based on its ID.
    """
    golden_ids = {4521, 1200}
    rows: list[dict] = []
    next_id = 0
    for i in range(remaining):
        next_id += 1
        while next_id in golden_ids:
            next_id += 1
        row = generate_row("customers.accounts", columns)
        row["id"] = next_id
        # Override email with ID-based unique email to guarantee no conflicts
        row["email"] = f"user_{next_id}@example.com"
        rows.append(row)
    return rows


def _insert_account_rows(conn, full_table: str, columns: list[str], rows: list[dict]) -> None:
    """Insert account rows with explicit IDs. On batch failure, fall back to row-by-row."""
    if not rows:
        return
    col_ids = psycopg2.sql.SQL(", ").join(psycopg2.sql.Identifier(c) for c in columns)
    placeholders = psycopg2.sql.SQL(", ").join(psycopg2.sql.Placeholder() for _ in columns)
    stmt = psycopg2.sql.SQL(
        "INSERT INTO {tbl} ({cols}) VALUES ({phs})"
    ).format(
        tbl=_build_qualified_table(full_table),
        cols=col_ids,
        phs=placeholders,
    )
    success_count = 0
    failed_count = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        try:
            cur = conn.cursor()
            for row in batch:
                vals = tuple(row[c] for c in columns)
                cur.execute(stmt, vals)
            conn.commit()
            success_count += len(batch)
        except (psycopg2.errors.UniqueViolation, psycopg2.IntegrityError):
            conn.rollback()
            # Fall back to row-by-row for this batch
            for row in batch:
                try:
                    cur = conn.cursor()
                    vals = tuple(row[c] for c in columns)
                    cur.execute(stmt, vals)
                    conn.commit()
                    success_count += 1
                except (psycopg2.errors.UniqueViolation, psycopg2.IntegrityError):
                    conn.rollback()
                    failed_count += 1
                except Exception:
                    conn.rollback()
                    failed_count += 1
        except Exception:
            conn.rollback()
            # Fall back to row-by-row for this batch
            for row in batch:
                try:
                    cur = conn.cursor()
                    vals = tuple(row[c] for c in columns)
                    cur.execute(stmt, vals)
                    conn.commit()
                    success_count += 1
                except Exception:
                    conn.rollback()
                    failed_count += 1
    log.info("  %s: %d inserted, %d failed", full_table, success_count, failed_count)


def seed_table(conn, full_table: str, target_volume: int) -> None:
    """Generate and insert bulk random rows for a table."""
    columns = TABLE_COLUMNS.get(full_table)
    if columns is None:
        log.warning("No column definition for %s, skipping.", full_table)
        return

    golden_count = tracker.count(full_table)
    remaining = max(0, target_volume - golden_count)

    if remaining == 0:
        log.info("Seeding %s: 0 additional rows (golden=%d)... done", full_table, golden_count)
        return

    t0 = time.time()
    log.info("Seeding %s: %d rows (golden=%d)...", full_table, remaining, golden_count)

    # --- Special handling for customers.accounts ---
    # Use explicit IDs to avoid SERIAL conflicts with golden records at 1200, 4521
    if full_table == "customers.accounts":
        rows = _generate_account_rows(remaining, columns)
        insert_cols = ["id"] + columns
        _insert_account_rows(conn, full_table, insert_cols, rows)
        _reset_sequence(conn, full_table)
        _refresh_tracker_from_db(conn, full_table)
        elapsed = time.time() - t0
        log.info("Seeding %s: done (%.1fs)", full_table, elapsed)
        return

    # Generate rows
    rows = [generate_row(full_table, columns) for _ in range(remaining)]

    # Insert
    if remaining >= COPY_THRESHOLD:
        fallback_ids = _insert_copy(conn, full_table, columns, rows)
        if fallback_ids:
            # COPY failed and fell back to batch insert; IDs already returned
            tracker.add_many(full_table, fallback_ids)
    else:
        new_ids = _insert_batch(conn, full_table, columns, rows)
        tracker.add_many(full_table, new_ids)

    # Always refresh tracker from DB to get accurate, complete ID list
    _refresh_tracker_from_db(conn, full_table)

    # Reset sequence after explicit or bulk inserts
    _reset_sequence(conn, full_table)

    # Track session_id strings for analytics cross-references
    if full_table == "analytics.sessions":
        cur = conn.cursor()
        cur.execute("SELECT session_id FROM analytics.sessions")
        for r in cur.fetchall():
            tracker.add_session_id(r[0])

    elapsed = time.time() - t0
    log.info("Seeding %s: done (%.1fs)", full_table, elapsed)


# ---------------------------------------------------------------------------
# CLI + main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed the dbook benchmark Postgres database with realistic e-commerce data.",
    )
    parser.add_argument(
        "--db-url",
        default="postgresql://bench:bench@localhost:5433/benchdb",
        help="PostgreSQL connection string (default: postgresql://bench:bench@localhost:5433/benchdb)",
    )
    parser.add_argument(
        "--config",
        default="seed/seed_config.yaml",
        help="Path to seed_config.yaml (default: seed/seed_config.yaml)",
    )
    return parser.parse_args()


def load_config(config_path: str) -> dict[str, int]:
    """Load table volumes from YAML config."""
    path = Path(config_path)
    if not path.exists():
        # Try relative to script directory
        path = Path(__file__).parent / Path(config_path).name
    if not path.exists():
        log.error("Cannot find config file at %s", config_path)
        sys.exit(1)
    with open(path) as f:
        data = yaml.safe_load(f)
    return data.get("volumes", {})


def connect_db(db_url: str):
    """Connect to PostgreSQL."""
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = False
        return conn
    except psycopg2.OperationalError as e:
        log.error("Cannot connect to database: %s", e)
        sys.exit(1)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_args()
    volumes = load_config(args.config)
    conn = connect_db(args.db_url)

    total_start = time.time()
    log.info("=" * 60)
    log.info("dbook benchmark seeder")
    log.info("Database: %s", args.db_url)
    log.info("Config:   %s", args.config)
    log.info("Total target rows: %s", f"{sum(volumes.values()):,}")
    log.info("=" * 60)

    # Step 1: Golden records
    log.info("[Phase 0] Golden records")
    insert_golden_records(conn)
    reset_sequences(conn)

    # Step 2: Bulk seed in FK-dependency order
    for phase_idx, phase_tables in enumerate(INSERTION_PHASES, start=1):
        log.info("[Phase %d]", phase_idx)
        for full_table in phase_tables:
            target = volumes.get(full_table, 0)
            if target == 0:
                continue
            seed_table(conn, full_table, target)

    # Verification
    log.info("=" * 60)
    log.info("Verification Summary")
    log.info("=" * 60)
    log.info("%-40s %8s %8s %5s", "Schema.Table", "Target", "Actual", "%")
    log.info("-" * 63)
    cur = conn.cursor()
    total_rows = 0
    total_target = 0
    for full_table in sorted(volumes.keys()):
        try:
            schema_name, table_name = full_table.split(".")
            cur.execute(
                psycopg2.sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                    psycopg2.sql.Identifier(schema_name),
                    psycopg2.sql.Identifier(table_name),
                )
            )
            row = cur.fetchone()
            count = row[0] if row else 0
            target = volumes[full_table]
            pct = round(count / target * 100) if target > 0 else 0
            log.info("%-40s %8d %8d %4d%%", full_table, target, count, pct)
            total_rows += count
            total_target += target
        except Exception as e:
            log.error("%-40s ERROR: %s", full_table, e)
            conn.rollback()

    log.info("-" * 63)
    total_pct = round(total_rows / total_target * 100) if total_target > 0 else 0
    log.info("%-40s %8d %8d %4d%%", "TOTAL", total_target, total_rows, total_pct)
    log.info("=" * 60)

    elapsed = time.time() - total_start
    log.info("Elapsed: %.1fs", elapsed)
    log.info("Done.")
    conn.close()


if __name__ == "__main__":
    main()
