# ruff: noqa: S311
"""Amazon-like e-commerce database fixture for realistic benchmarking.

~40 tables across 7 schema prefixes: customers, catalog, orders, billing,
analytics, warehouse, support.  Uses SQLAlchemy Table/Column/MetaData API
(not ORM) for programmatic generation.  SQLite-compatible (schema prefixes
baked into table names).
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest  # type: ignore[import-untyped]
from sqlalchemy import (  # type: ignore[import-untyped]
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    event,
)


# ---------------------------------------------------------------------------
# Deterministic PRNG (not used for security — test data only)
# ---------------------------------------------------------------------------

class _DetRng:
    """Deterministic PRNG for test data (not used for security)."""

    def __init__(self, seed: int = 42) -> None:
        self._state = seed

    def _next(self) -> int:
        self._state = (self._state * 1664525 + 1013904223) & 0xFFFFFFFF
        return self._state

    def randint(self, lo: int, hi: int) -> int:
        return lo + self._next() % (hi - lo + 1)

    def uniform(self, lo: float, hi: float) -> float:
        return lo + (self._next() / 0xFFFFFFFF) * (hi - lo)

    def choice(self, seq: list | tuple):  # noqa: ANN001
        return seq[self._next() % len(seq)]


# ---------------------------------------------------------------------------
# Schema definition (~40 tables)
# ---------------------------------------------------------------------------

def _build_amazon_metadata() -> MetaData:
    """Define ~40 Amazon-like tables using the Table/Column API."""
    meta = MetaData()

    # ===== customers (4 tables) ============================================

    Table(
        "customers_accounts", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("email", String(255), nullable=False, unique=True),
        Column("phone", String(20), nullable=True),
        Column("name", String(150), nullable=False),
        Column("password_hash", String(255), nullable=False),
        Column("status", String(20), nullable=False, default="active"),
        Column("created_at", DateTime, nullable=False),
        Index("idx_ca_email", "email", unique=True),
        Index("idx_ca_status", "status"),
    )

    Table(
        "customers_addresses", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("account_id", Integer, ForeignKey("customers_accounts.id"), nullable=False),
        Column("street", String(255), nullable=False),
        Column("city", String(100), nullable=False),
        Column("state", String(50), nullable=True),
        Column("zip", String(20), nullable=True),
        Column("country", String(2), nullable=False, default="US"),
        Column("is_default", Boolean, default=False),
        Index("idx_addr_account", "account_id"),
    )

    Table(
        "customers_payment_methods", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("account_id", Integer, ForeignKey("customers_accounts.id"), nullable=False),
        Column("type", String(30), nullable=False),
        Column("card_last_four", String(4), nullable=True),
        Column("expiry", String(7), nullable=True),
        Column("is_default", Boolean, default=False),
        Index("idx_pm_account", "account_id"),
    )

    Table(
        "customers_preferences", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("account_id", Integer, ForeignKey("customers_accounts.id"), nullable=False, unique=True),
        Column("language", String(5), default="en"),
        Column("currency", String(3), default="USD"),
        Column("notification_prefs", Text, nullable=True),
    )

    # ===== catalog (5 tables) ==============================================

    Table(
        "catalog_categories", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(100), nullable=False),
        Column("parent_id", Integer, ForeignKey("catalog_categories.id"), nullable=True),
        Column("level", Integer, default=0),
        Column("path", String(500), nullable=True),
    )

    Table(
        "catalog_products", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("asin", String(20), nullable=False, unique=True),
        Column("title", String(300), nullable=False),
        Column("description", Text, nullable=True),
        Column("brand", String(100), nullable=True),
        Column("category_id", Integer, ForeignKey("catalog_categories.id"), nullable=True),
        Column("price", Float, nullable=False),
        Column("weight", Float, nullable=True),
        Index("idx_cp_category", "category_id"),
        Index("idx_cp_brand", "brand"),
    )

    Table(
        "catalog_product_images", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("product_id", Integer, ForeignKey("catalog_products.id"), nullable=False),
        Column("url", String(500), nullable=False),
        Column("position", Integer, default=0),
        Column("alt_text", String(200), nullable=True),
        Index("idx_cpi_product", "product_id"),
    )

    Table(
        "catalog_reviews", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("product_id", Integer, ForeignKey("catalog_products.id"), nullable=False),
        Column("account_id", Integer, ForeignKey("customers_accounts.id"), nullable=False),
        Column("rating", Integer, nullable=False),
        Column("title", String(200), nullable=True),
        Column("body", Text, nullable=True),
        Column("verified", Boolean, default=False),
        Column("created_at", DateTime, nullable=False),
        Index("idx_cr_product", "product_id"),
        Index("idx_cr_account", "account_id"),
        CheckConstraint("rating >= 1 AND rating <= 5", name="ck_review_rating"),
    )

    Table(
        "catalog_inventory", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("product_id", Integer, ForeignKey("catalog_products.id"), nullable=False),
        Column("warehouse_id", Integer, ForeignKey("warehouse_warehouses.id"), nullable=False),
        Column("quantity", Integer, nullable=False, default=0),
        Column("reserved", Integer, nullable=False, default=0),
        Column("reorder_point", Integer, default=10),
        Index("idx_ci_product", "product_id"),
        Index("idx_ci_warehouse", "warehouse_id"),
    )

    # ===== orders (6 tables) ===============================================

    Table(
        "orders_carts", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("account_id", Integer, ForeignKey("customers_accounts.id"), nullable=False),
        Column("status", String(20), nullable=False, default="active"),
        Column("created_at", DateTime, nullable=False),
        Column("updated_at", DateTime, nullable=True),
        Index("idx_oc_account", "account_id"),
    )

    Table(
        "orders_cart_items", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("cart_id", Integer, ForeignKey("orders_carts.id"), nullable=False),
        Column("product_id", Integer, ForeignKey("catalog_products.id"), nullable=False),
        Column("quantity", Integer, nullable=False, default=1),
        Column("unit_price", Float, nullable=False),
        Index("idx_oci_cart", "cart_id"),
        Index("idx_oci_product", "product_id"),
    )

    Table(
        "orders_orders", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("account_id", Integer, ForeignKey("customers_accounts.id"), nullable=False),
        Column("shipping_address_id", Integer, ForeignKey("customers_addresses.id"), nullable=True),
        Column("payment_method_id", Integer, ForeignKey("customers_payment_methods.id"), nullable=True),
        Column("subtotal", Float, nullable=False),
        Column("tax", Float, nullable=False, default=0),
        Column("shipping_cost", Float, nullable=False, default=0),
        Column("total", Float, nullable=False),
        Column("status", String(20), nullable=False, default="pending"),
        Column("placed_at", DateTime, nullable=False),
        Index("idx_oo_account", "account_id"),
        Index("idx_oo_status", "status"),
        Index("idx_oo_placed", "placed_at"),
        CheckConstraint("total >= 0", name="ck_order_total_positive"),
    )

    Table(
        "orders_order_items", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("order_id", Integer, ForeignKey("orders_orders.id"), nullable=False),
        Column("product_id", Integer, ForeignKey("catalog_products.id"), nullable=False),
        Column("quantity", Integer, nullable=False, default=1),
        Column("unit_price", Float, nullable=False),
        Column("status", String(20), nullable=False, default="pending"),
        Index("idx_ooi_order", "order_id"),
        Index("idx_ooi_product", "product_id"),
    )

    Table(
        "orders_shipments", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("order_id", Integer, ForeignKey("orders_orders.id"), nullable=False),
        Column("carrier", String(50), nullable=True),
        Column("tracking_number", String(100), nullable=True),
        Column("shipped_at", DateTime, nullable=True),
        Column("delivered_at", DateTime, nullable=True),
        Column("status", String(20), nullable=False, default="preparing"),
        Index("idx_os_order", "order_id"),
    )

    Table(
        "orders_returns", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("order_item_id", Integer, ForeignKey("orders_order_items.id"), nullable=False),
        Column("reason", Text, nullable=True),
        Column("status", String(20), nullable=False, default="requested"),
        Column("refund_amount", Float, nullable=True),
        Column("created_at", DateTime, nullable=False),
        Index("idx_or_order_item", "order_item_id"),
    )

    # ===== billing (7 tables) ==============================================

    Table(
        "billing_invoices", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("order_id", Integer, ForeignKey("orders_orders.id"), nullable=False),
        Column("account_id", Integer, ForeignKey("customers_accounts.id"), nullable=False),
        Column("amount", Float, nullable=False),
        Column("tax", Float, nullable=False, default=0),
        Column("total", Float, nullable=False),
        Column("status", String(20), nullable=False, default="draft"),
        Column("issued_at", DateTime, nullable=True),
        Column("due_at", DateTime, nullable=True),
        Index("idx_bi_order", "order_id"),
        Index("idx_bi_account", "account_id"),
    )

    Table(
        "billing_payments", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("invoice_id", Integer, ForeignKey("billing_invoices.id"), nullable=False),
        Column("payment_method_id", Integer, ForeignKey("customers_payment_methods.id"), nullable=True),
        Column("amount", Float, nullable=False),
        Column("currency", String(3), default="USD"),
        Column("processor_ref", String(100), nullable=True),
        Column("status", String(20), nullable=False, default="pending"),
        Column("processed_at", DateTime, nullable=True),
        Index("idx_bp_invoice", "invoice_id"),
    )

    Table(
        "billing_refunds", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("payment_id", Integer, ForeignKey("billing_payments.id"), nullable=False),
        Column("return_id", Integer, ForeignKey("orders_returns.id"), nullable=True),
        Column("amount", Float, nullable=False),
        Column("reason", Text, nullable=True),
        Column("status", String(20), nullable=False, default="pending"),
        Column("processed_at", DateTime, nullable=True),
        Index("idx_br_payment", "payment_id"),
        Index("idx_br_return", "return_id"),
    )

    Table(
        "billing_subscriptions", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("account_id", Integer, ForeignKey("customers_accounts.id"), nullable=False),
        Column("plan", String(50), nullable=False),
        Column("price", Float, nullable=False),
        Column("billing_cycle", String(20), nullable=False, default="monthly"),
        Column("status", String(20), nullable=False, default="active"),
        Column("started_at", DateTime, nullable=False),
        Column("next_billing_at", DateTime, nullable=True),
        Index("idx_bs_account", "account_id"),
    )

    Table(
        "billing_subscription_payments", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("subscription_id", Integer, ForeignKey("billing_subscriptions.id"), nullable=False),
        Column("amount", Float, nullable=False),
        Column("period_start", DateTime, nullable=False),
        Column("period_end", DateTime, nullable=False),
        Column("status", String(20), nullable=False, default="paid"),
        Index("idx_bsp_sub", "subscription_id"),
    )

    Table(
        "billing_gift_cards", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("code", String(20), nullable=False, unique=True),
        Column("balance", Float, nullable=False),
        Column("original_amount", Float, nullable=False),
        Column("purchaser_id", Integer, ForeignKey("customers_accounts.id"), nullable=True),
        Column("recipient_email", String(255), nullable=True),
        Column("status", String(20), nullable=False, default="active"),
        Index("idx_bgc_code", "code", unique=True),
    )

    Table(
        "billing_promotions", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("code", String(50), nullable=False, unique=True),
        Column("type", String(20), nullable=False),
        Column("value", Float, nullable=False),
        Column("min_order", Float, nullable=True),
        Column("max_uses", Integer, nullable=True),
        Column("used_count", Integer, default=0),
        Column("valid_from", DateTime, nullable=False),
        Column("valid_until", DateTime, nullable=True),
    )

    # ===== analytics (6 tables) ============================================

    Table(
        "analytics_page_views", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("account_id", Integer, ForeignKey("customers_accounts.id"), nullable=True),
        Column("session_id", String(100), nullable=True),
        Column("page_type", String(50), nullable=True),
        Column("page_id", String(100), nullable=True),
        Column("referrer", String(500), nullable=True),
        Column("user_agent", Text, nullable=True),
        Column("ip_address", String(45), nullable=True),
        Column("created_at", DateTime, nullable=False),
        Index("idx_apv_account", "account_id"),
        Index("idx_apv_session", "session_id"),
    )

    Table(
        "analytics_search_queries", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("account_id", Integer, ForeignKey("customers_accounts.id"), nullable=True),
        Column("query_text", String(500), nullable=False),
        Column("results_count", Integer, nullable=True),
        Column("clicked_product_id", Integer, ForeignKey("catalog_products.id"), nullable=True),
        Column("created_at", DateTime, nullable=False),
        Index("idx_asq_account", "account_id"),
    )

    Table(
        "analytics_click_events", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("account_id", Integer, ForeignKey("customers_accounts.id"), nullable=True),
        Column("session_id", String(100), nullable=True),
        Column("event_type", String(50), nullable=False),
        Column("element_id", String(100), nullable=True),
        Column("page_url", String(500), nullable=True),
        Column("created_at", DateTime, nullable=False),
        Index("idx_ace_account", "account_id"),
    )

    Table(
        "analytics_conversion_funnels", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(100), nullable=False),
        Column("steps_json", Text, nullable=True),
        Column("created_at", DateTime, nullable=False),
    )

    Table(
        "analytics_daily_metrics", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("date", DateTime, nullable=False),
        Column("metric_name", String(100), nullable=False),
        Column("metric_value", Float, nullable=False),
        Column("dimension", String(50), nullable=True),
        Column("dimension_value", String(100), nullable=True),
        Index("idx_adm_date", "date"),
    )

    Table(
        "analytics_ab_tests", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(100), nullable=False),
        Column("variant", String(50), nullable=False),
        Column("metric_name", String(100), nullable=False),
        Column("metric_value", Float, nullable=False),
        Column("sample_size", Integer, nullable=True),
        Column("significance", Float, nullable=True),
    )

    # ===== warehouse (3 tables) ============================================

    Table(
        "warehouse_warehouses", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(100), nullable=False),
        Column("code", String(10), nullable=False, unique=True),
        Column("city", String(100), nullable=True),
        Column("state", String(50), nullable=True),
        Column("country", String(2), nullable=False, default="US"),
        Column("capacity", Integer, nullable=True),
    )

    Table(
        "warehouse_picking_lists", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("order_id", Integer, ForeignKey("orders_orders.id"), nullable=False),
        Column("warehouse_id", Integer, ForeignKey("warehouse_warehouses.id"), nullable=False),
        Column("status", String(20), nullable=False, default="pending"),
        Column("assigned_to", String(100), nullable=True),
        Column("created_at", DateTime, nullable=False),
        Index("idx_wpl_order", "order_id"),
        Index("idx_wpl_warehouse", "warehouse_id"),
    )

    Table(
        "warehouse_shipping_rates", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("carrier", String(50), nullable=False),
        Column("service_level", String(50), nullable=False),
        Column("weight_min", Float, nullable=False),
        Column("weight_max", Float, nullable=False),
        Column("zone", String(20), nullable=True),
        Column("rate", Float, nullable=False),
    )

    # ===== support (3 tables) ==============================================

    Table(
        "support_tickets", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("account_id", Integer, ForeignKey("customers_accounts.id"), nullable=False),
        Column("order_id", Integer, ForeignKey("orders_orders.id"), nullable=True),
        Column("category", String(50), nullable=True),
        Column("priority", String(20), nullable=False, default="medium"),
        Column("status", String(20), nullable=False, default="open"),
        Column("subject", String(255), nullable=False),
        Column("created_at", DateTime, nullable=False),
        Column("resolved_at", DateTime, nullable=True),
        Index("idx_st_account", "account_id"),
        Index("idx_st_order", "order_id"),
        Index("idx_st_status", "status"),
    )

    Table(
        "support_ticket_messages", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("ticket_id", Integer, ForeignKey("support_tickets.id"), nullable=False),
        Column("sender_type", String(20), nullable=False),
        Column("sender_id", Integer, nullable=True),
        Column("body", Text, nullable=False),
        Column("created_at", DateTime, nullable=False),
        Index("idx_stm_ticket", "ticket_id"),
    )

    Table(
        "support_faq_articles", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("category", String(50), nullable=True),
        Column("title", String(200), nullable=False),
        Column("body", Text, nullable=False),
        Column("helpful_count", Integer, default=0),
        Column("created_at", DateTime, nullable=False),
    )

    return meta


# ---------------------------------------------------------------------------
# Realistic sample data
# ---------------------------------------------------------------------------

_PRODUCT_NAMES = [
    "Wireless Bluetooth Headphones",
    "USB-C Charging Cable 6ft",
    "Stainless Steel Water Bottle",
    "Laptop Stand Adjustable",
    "Mechanical Keyboard RGB",
    "Portable Phone Charger 10000mAh",
    "Cotton Crew Socks 6-Pack",
    "LED Desk Lamp Dimmable",
    "Yoga Mat Non-Slip",
    "Cast Iron Skillet 12-inch",
]

_BRANDS = [
    "TechWave", "CableMax", "HydroFlask", "ErgoDesk", "KeyMaster",
    "PowerBank Pro", "ComfortFit", "BrightLight", "ZenFlow", "CookPro",
]

_CATEGORIES = [
    ("Electronics", None, 0, "Electronics"),
    ("Audio", 1, 1, "Electronics/Audio"),
    ("Cables", 1, 1, "Electronics/Cables"),
    ("Kitchen", None, 0, "Kitchen"),
    ("Sports", None, 0, "Sports"),
    ("Furniture", None, 0, "Furniture"),
    ("Clothing", None, 0, "Clothing"),
    ("Lighting", 1, 1, "Electronics/Lighting"),
]

_NAMES = [
    "John Doe", "Jane Smith", "Maria Garcia", "Robert Johnson",
    "Emily Chen", "Michael Brown", "Sarah Wilson", "David Lee",
    "Lisa Anderson", "James Taylor",
]

_EMAILS = [
    "john.doe.42@email.com", "jane.smith.99@email.com",
    "maria.garcia@mail.com", "robert.j.55@email.com",
    "emily.chen@inbox.com", "m.brown.77@email.com",
    "sarah.w@email.com", "david.lee.88@mail.com",
    "lisa.a@inbox.com", "james.t.33@email.com",
]

_STREETS = [
    "123 Oak Street", "456 Maple Avenue", "789 Pine Road",
    "321 Elm Boulevard", "654 Cedar Lane", "987 Birch Drive",
    "147 Walnut Court", "258 Cherry Way", "369 Spruce Place",
    "741 Ash Circle",
]

_CITIES = [
    ("Portland", "OR", "97201"), ("Seattle", "WA", "98101"),
    ("San Francisco", "CA", "94102"), ("Austin", "TX", "73301"),
    ("Denver", "CO", "80201"), ("Chicago", "IL", "60601"),
    ("Boston", "MA", "02101"), ("New York", "NY", "10001"),
    ("Miami", "FL", "33101"), ("Phoenix", "AZ", "85001"),
]

_ORDER_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled"]
_SHIPMENT_STATUSES = ["preparing", "shipped", "in_transit", "delivered"]
_TICKET_CATEGORIES = ["order_issue", "return", "billing", "account", "product_question"]
_CARRIERS = ["USPS", "UPS", "FedEx", "DHL"]


def _insert_amazon_sample_data(engine: object, metadata: MetaData) -> None:
    """Insert 5-10 rows per table with realistic Amazon-like values."""
    rng = _DetRng(42)
    ts_base = datetime(2025, 1, 15)

    with engine.connect() as conn:  # type: ignore[union-attr]
        # --- customers_accounts (10 rows) ---
        for i in range(1, 11):
            conn.execute(
                metadata.tables["customers_accounts"].insert().values(
                    email=_EMAILS[i - 1],
                    phone=f"+1-555-{rng.randint(1000, 9999)}",
                    name=_NAMES[i - 1],
                    password_hash=f"$2b$12$hash{i:04d}",
                    status=rng.choice(["active", "active", "active", "suspended"]),
                    created_at=ts_base + timedelta(days=i * 3),
                )
            )

        # --- customers_addresses (10 rows) ---
        for i in range(1, 11):
            city, state, zipcode = _CITIES[i - 1]
            conn.execute(
                metadata.tables["customers_addresses"].insert().values(
                    account_id=i,
                    street=_STREETS[i - 1],
                    city=city,
                    state=state,
                    zip=zipcode,
                    country="US",
                    is_default=(i <= 5),
                )
            )

        # --- customers_payment_methods (10 rows) ---
        for i in range(1, 11):
            conn.execute(
                metadata.tables["customers_payment_methods"].insert().values(
                    account_id=i,
                    type=rng.choice(["credit_card", "debit_card", "paypal"]),
                    card_last_four=f"{rng.randint(1000, 9999)}",
                    expiry=f"20{rng.randint(26, 30)}/{rng.randint(1, 12):02d}",
                    is_default=True,
                )
            )

        # --- customers_preferences (10 rows) ---
        for i in range(1, 11):
            conn.execute(
                metadata.tables["customers_preferences"].insert().values(
                    account_id=i,
                    language=rng.choice(["en", "es", "fr", "de"]),
                    currency=rng.choice(["USD", "EUR", "GBP"]),
                    notification_prefs='{"email":true,"sms":false}',
                )
            )

        # --- catalog_categories (8 rows) ---
        for i, (name, parent, level, path) in enumerate(_CATEGORIES, 1):
            conn.execute(
                metadata.tables["catalog_categories"].insert().values(
                    name=name,
                    parent_id=parent,
                    level=level,
                    path=path,
                )
            )

        # --- warehouse_warehouses (5 rows — needed before catalog_inventory) ---
        _warehouses = [
            ("PDX Fulfillment", "PDX1", "Portland", "OR"),
            ("SEA Fulfillment", "SEA1", "Seattle", "WA"),
            ("ORD Fulfillment", "ORD1", "Chicago", "IL"),
            ("JFK Fulfillment", "JFK1", "New York", "NY"),
            ("DFW Fulfillment", "DFW1", "Dallas", "TX"),
        ]
        for name, code, city, state in _warehouses:
            conn.execute(
                metadata.tables["warehouse_warehouses"].insert().values(
                    name=name, code=code, city=city, state=state, country="US",
                    capacity=rng.randint(5000, 50000),
                )
            )

        # --- catalog_products (10 rows) ---
        for i in range(1, 11):
            conn.execute(
                metadata.tables["catalog_products"].insert().values(
                    asin=f"B0{rng.randint(10000000, 99999999)}",
                    title=_PRODUCT_NAMES[i - 1],
                    description=f"High quality {_PRODUCT_NAMES[i - 1].lower()} for everyday use.",
                    brand=_BRANDS[i - 1],
                    category_id=rng.randint(1, 8),
                    price=round(rng.uniform(9.99, 199.99), 2),
                    weight=round(rng.uniform(0.1, 5.0), 2),
                )
            )

        # --- catalog_product_images (10 rows) ---
        for i in range(1, 11):
            conn.execute(
                metadata.tables["catalog_product_images"].insert().values(
                    product_id=i,
                    url=f"https://cdn.example.com/products/{i}/main.jpg",
                    position=0,
                    alt_text=f"Image of {_PRODUCT_NAMES[i - 1]}",
                )
            )

        # --- catalog_reviews (10 rows) ---
        for i in range(1, 11):
            conn.execute(
                metadata.tables["catalog_reviews"].insert().values(
                    product_id=rng.randint(1, 10),
                    account_id=rng.randint(1, 10),
                    rating=rng.randint(1, 5),
                    title=rng.choice(["Great product!", "Good value", "Not bad", "Amazing!", "Okay"]),
                    body=rng.choice([
                        "Works exactly as described. Very happy with purchase.",
                        "Decent quality for the price. Would recommend.",
                        "Arrived quickly. Good packaging.",
                        "Better than expected. Will buy again.",
                        "Does the job. Nothing special.",
                    ]),
                    verified=(i % 2 == 0),
                    created_at=ts_base + timedelta(days=i * 5),
                )
            )

        # --- catalog_inventory (10 rows) ---
        for i in range(1, 11):
            conn.execute(
                metadata.tables["catalog_inventory"].insert().values(
                    product_id=rng.randint(1, 10),
                    warehouse_id=rng.randint(1, 5),
                    quantity=rng.randint(0, 500),
                    reserved=rng.randint(0, 20),
                    reorder_point=rng.randint(5, 50),
                )
            )

        # --- orders_carts (8 rows) ---
        for i in range(1, 9):
            conn.execute(
                metadata.tables["orders_carts"].insert().values(
                    account_id=rng.randint(1, 10),
                    status=rng.choice(["active", "converted", "abandoned"]),
                    created_at=ts_base + timedelta(days=i * 2),
                    updated_at=ts_base + timedelta(days=i * 2, hours=3),
                )
            )

        # --- orders_cart_items (10 rows) ---
        for i in range(1, 11):
            conn.execute(
                metadata.tables["orders_cart_items"].insert().values(
                    cart_id=rng.randint(1, 8),
                    product_id=rng.randint(1, 10),
                    quantity=rng.randint(1, 3),
                    unit_price=round(rng.uniform(9.99, 199.99), 2),
                )
            )

        # --- orders_orders (10 rows) ---
        for i in range(1, 11):
            subtotal = round(rng.uniform(15.0, 500.0), 2)
            tax = round(subtotal * 0.08, 2)
            shipping = round(rng.uniform(0.0, 15.0), 2)
            conn.execute(
                metadata.tables["orders_orders"].insert().values(
                    account_id=rng.randint(1, 10),
                    shipping_address_id=rng.randint(1, 10),
                    payment_method_id=rng.randint(1, 10),
                    subtotal=subtotal,
                    tax=tax,
                    shipping_cost=shipping,
                    total=round(subtotal + tax + shipping, 2),
                    status=rng.choice(_ORDER_STATUSES),
                    placed_at=ts_base + timedelta(days=i * 4),
                )
            )

        # --- orders_order_items (10 rows) ---
        for i in range(1, 11):
            conn.execute(
                metadata.tables["orders_order_items"].insert().values(
                    order_id=rng.randint(1, 10),
                    product_id=rng.randint(1, 10),
                    quantity=rng.randint(1, 4),
                    unit_price=round(rng.uniform(9.99, 199.99), 2),
                    status=rng.choice(["pending", "shipped", "delivered", "returned"]),
                )
            )

        # --- orders_shipments (8 rows) ---
        for i in range(1, 9):
            shipped = ts_base + timedelta(days=i * 5)
            conn.execute(
                metadata.tables["orders_shipments"].insert().values(
                    order_id=rng.randint(1, 10),
                    carrier=rng.choice(_CARRIERS),
                    tracking_number=f"1Z{rng.randint(100000000, 999999999)}",
                    shipped_at=shipped,
                    delivered_at=shipped + timedelta(days=rng.randint(1, 7)) if i % 3 != 0 else None,
                    status=rng.choice(_SHIPMENT_STATUSES),
                )
            )

        # --- orders_returns (5 rows) ---
        for i in range(1, 6):
            conn.execute(
                metadata.tables["orders_returns"].insert().values(
                    order_item_id=rng.randint(1, 10),
                    reason=rng.choice(["defective", "wrong_item", "changed_mind", "not_as_described"]),
                    status=rng.choice(["requested", "approved", "completed", "denied"]),
                    refund_amount=round(rng.uniform(10.0, 150.0), 2),
                    created_at=ts_base + timedelta(days=i * 8),
                )
            )

        # --- billing_invoices (10 rows) ---
        for i in range(1, 11):
            amount = round(rng.uniform(15.0, 500.0), 2)
            tax = round(amount * 0.08, 2)
            conn.execute(
                metadata.tables["billing_invoices"].insert().values(
                    order_id=rng.randint(1, 10),
                    account_id=rng.randint(1, 10),
                    amount=amount,
                    tax=tax,
                    total=round(amount + tax, 2),
                    status=rng.choice(["draft", "sent", "paid", "overdue"]),
                    issued_at=ts_base + timedelta(days=i * 4),
                    due_at=ts_base + timedelta(days=i * 4 + 30),
                )
            )

        # --- billing_payments (8 rows) ---
        for i in range(1, 9):
            conn.execute(
                metadata.tables["billing_payments"].insert().values(
                    invoice_id=rng.randint(1, 10),
                    payment_method_id=rng.randint(1, 10),
                    amount=round(rng.uniform(15.0, 500.0), 2),
                    currency="USD",
                    processor_ref=f"ch_{rng.randint(10000000, 99999999)}",
                    status=rng.choice(["pending", "completed", "failed"]),
                    processed_at=ts_base + timedelta(days=i * 5),
                )
            )

        # --- billing_refunds (5 rows) ---
        for i in range(1, 6):
            conn.execute(
                metadata.tables["billing_refunds"].insert().values(
                    payment_id=rng.randint(1, 8),
                    return_id=rng.randint(1, 5),
                    amount=round(rng.uniform(10.0, 150.0), 2),
                    reason=rng.choice(["product_return", "overcharge", "cancelled"]),
                    status=rng.choice(["pending", "processed", "denied"]),
                    processed_at=ts_base + timedelta(days=i * 10),
                )
            )

        # --- billing_subscriptions (5 rows) ---
        for i in range(1, 6):
            started = ts_base + timedelta(days=i * 15)
            conn.execute(
                metadata.tables["billing_subscriptions"].insert().values(
                    account_id=rng.randint(1, 10),
                    plan=rng.choice(["Prime Monthly", "Prime Annual", "Music Unlimited", "Kindle Unlimited"]),
                    price=rng.choice([14.99, 139.0, 9.99, 11.99]),
                    billing_cycle=rng.choice(["monthly", "annual"]),
                    status=rng.choice(["active", "active", "cancelled"]),
                    started_at=started,
                    next_billing_at=started + timedelta(days=30),
                )
            )

        # --- billing_subscription_payments (5 rows) ---
        for i in range(1, 6):
            start = ts_base + timedelta(days=i * 30)
            conn.execute(
                metadata.tables["billing_subscription_payments"].insert().values(
                    subscription_id=rng.randint(1, 5),
                    amount=rng.choice([14.99, 139.0, 9.99, 11.99]),
                    period_start=start,
                    period_end=start + timedelta(days=30),
                    status="paid",
                )
            )

        # --- billing_gift_cards (5 rows) ---
        for i in range(1, 6):
            original = rng.choice([25.0, 50.0, 100.0, 200.0])
            conn.execute(
                metadata.tables["billing_gift_cards"].insert().values(
                    code=f"GIFT-{rng.randint(10000, 99999)}",
                    balance=round(rng.uniform(0.0, original), 2),
                    original_amount=original,
                    purchaser_id=rng.randint(1, 10),
                    recipient_email=f"recipient{i}@email.com",
                    status=rng.choice(["active", "active", "redeemed"]),
                )
            )

        # --- billing_promotions (5 rows) ---
        for i in range(1, 6):
            conn.execute(
                metadata.tables["billing_promotions"].insert().values(
                    code=rng.choice(["SAVE10", "WELCOME20", "SUMMER15", "FLASH25", "HOLIDAY30"])[: -1] + str(i),
                    type=rng.choice(["percentage", "fixed_amount"]),
                    value=rng.choice([10.0, 15.0, 20.0, 25.0, 5.0]),
                    min_order=rng.choice([25.0, 50.0, 0.0]),
                    max_uses=rng.randint(100, 10000),
                    used_count=rng.randint(0, 500),
                    valid_from=ts_base,
                    valid_until=ts_base + timedelta(days=180),
                )
            )

        # --- analytics_page_views (10 rows) ---
        for i in range(1, 11):
            conn.execute(
                metadata.tables["analytics_page_views"].insert().values(
                    account_id=rng.randint(1, 10) if i % 3 != 0 else None,
                    session_id=f"sess_{rng.randint(10000, 99999)}",
                    page_type=rng.choice(["product", "category", "home", "checkout", "search"]),
                    page_id=f"page_{rng.randint(1, 100)}",
                    referrer=rng.choice(["google.com", "facebook.com", "direct", None]),
                    user_agent="Mozilla/5.0",
                    ip_address=f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}",
                    created_at=ts_base + timedelta(hours=i * 3),
                )
            )

        # --- analytics_search_queries (8 rows) ---
        _search_terms = [
            "bluetooth headphones", "usb c cable", "water bottle",
            "laptop stand", "keyboard", "phone charger", "yoga mat", "skillet",
        ]
        for i in range(1, 9):
            conn.execute(
                metadata.tables["analytics_search_queries"].insert().values(
                    account_id=rng.randint(1, 10) if i % 4 != 0 else None,
                    query_text=_search_terms[i - 1],
                    results_count=rng.randint(5, 200),
                    clicked_product_id=rng.randint(1, 10) if i % 2 == 0 else None,
                    created_at=ts_base + timedelta(hours=i * 4),
                )
            )

        # --- analytics_click_events (10 rows) ---
        for i in range(1, 11):
            conn.execute(
                metadata.tables["analytics_click_events"].insert().values(
                    account_id=rng.randint(1, 10) if i % 3 != 0 else None,
                    session_id=f"sess_{rng.randint(10000, 99999)}",
                    event_type=rng.choice(["add_to_cart", "buy_now", "wishlist", "share"]),
                    element_id=f"btn_{rng.randint(1, 50)}",
                    page_url=f"https://shop.example.com/product/{rng.randint(1, 10)}",
                    created_at=ts_base + timedelta(hours=i * 2),
                )
            )

        # --- analytics_conversion_funnels (3 rows) ---
        for name in ["search_to_purchase", "cart_to_checkout", "visit_to_signup"]:
            conn.execute(
                metadata.tables["analytics_conversion_funnels"].insert().values(
                    name=name,
                    steps_json='["view","click","add_to_cart","checkout","purchase"]',
                    created_at=ts_base,
                )
            )

        # --- analytics_daily_metrics (10 rows) ---
        for i in range(1, 11):
            conn.execute(
                metadata.tables["analytics_daily_metrics"].insert().values(
                    date=ts_base + timedelta(days=i),
                    metric_name=rng.choice(["revenue", "orders", "visitors", "conversion_rate"]),
                    metric_value=round(rng.uniform(100.0, 50000.0), 2),
                    dimension=rng.choice(["category", "channel", "device", None]),
                    dimension_value=rng.choice(["electronics", "organic", "mobile", None]),
                )
            )

        # --- analytics_ab_tests (6 rows) ---
        _test_names = ["new_checkout_flow", "recommendation_widget", "search_ranking_v2"]
        for test_name in _test_names:
            for variant in ["control", "treatment"]:
                conn.execute(
                    metadata.tables["analytics_ab_tests"].insert().values(
                        name=test_name,
                        variant=variant,
                        metric_name="conversion_rate",
                        metric_value=round(rng.uniform(0.02, 0.15), 4),
                        sample_size=rng.randint(1000, 50000),
                        significance=round(rng.uniform(0.01, 0.99), 3),
                    )
                )

        # --- warehouse_picking_lists (8 rows) ---
        for i in range(1, 9):
            conn.execute(
                metadata.tables["warehouse_picking_lists"].insert().values(
                    order_id=rng.randint(1, 10),
                    warehouse_id=rng.randint(1, 5),
                    status=rng.choice(["pending", "picking", "packed", "shipped"]),
                    assigned_to=rng.choice(["worker_a", "worker_b", "worker_c", None]),
                    created_at=ts_base + timedelta(days=i * 3),
                )
            )

        # --- warehouse_shipping_rates (8 rows) ---
        for carrier in _CARRIERS:
            for level in ["standard", "express"]:
                conn.execute(
                    metadata.tables["warehouse_shipping_rates"].insert().values(
                        carrier=carrier,
                        service_level=level,
                        weight_min=0.0,
                        weight_max=70.0 if level == "standard" else 30.0,
                        zone="domestic",
                        rate=round(rng.uniform(5.0, 35.0), 2),
                    )
                )

        # --- support_tickets (8 rows) ---
        for i in range(1, 9):
            conn.execute(
                metadata.tables["support_tickets"].insert().values(
                    account_id=rng.randint(1, 10),
                    order_id=rng.randint(1, 10) if i % 3 != 0 else None,
                    category=rng.choice(_TICKET_CATEGORIES),
                    priority=rng.choice(["low", "medium", "high", "urgent"]),
                    status=rng.choice(["open", "in_progress", "resolved", "closed"]),
                    subject=rng.choice([
                        "Where is my order?",
                        "Request refund for damaged item",
                        "Cannot apply promo code",
                        "Wrong item received",
                        "Subscription billing question",
                        "Account access issue",
                        "Product quality complaint",
                        "Delivery delay inquiry",
                    ]),
                    created_at=ts_base + timedelta(days=i * 3),
                    resolved_at=ts_base + timedelta(days=i * 3 + 2) if i % 2 == 0 else None,
                )
            )

        # --- support_ticket_messages (10 rows) ---
        for i in range(1, 11):
            conn.execute(
                metadata.tables["support_ticket_messages"].insert().values(
                    ticket_id=rng.randint(1, 8),
                    sender_type=rng.choice(["customer", "agent"]),
                    sender_id=rng.randint(1, 10),
                    body=rng.choice([
                        "I need help with my recent order.",
                        "Thank you for contacting us. Let me look into this.",
                        "The item arrived damaged. Please advise.",
                        "We apologize for the inconvenience. Processing your refund.",
                        "When will my replacement arrive?",
                    ]),
                    created_at=ts_base + timedelta(days=i * 2, hours=i),
                )
            )

        # --- support_faq_articles (5 rows) ---
        _faqs = [
            ("return", "How to Return an Item", "You can initiate a return within 30 days of delivery."),
            ("billing", "Understanding Your Invoice", "Invoices are generated after order confirmation."),
            ("shipping", "Shipping and Delivery Times", "Standard shipping takes 3-5 business days."),
            ("account", "Managing Your Account Settings", "Go to Account Settings to update your profile."),
            ("return", "Refund Policy", "Refunds are processed within 5-7 business days after return receipt."),
        ]
        for cat, title, body in _faqs:
            conn.execute(
                metadata.tables["support_faq_articles"].insert().values(
                    category=cat,
                    title=title,
                    body=body,
                    helpful_count=rng.randint(5, 500),
                    created_at=ts_base,
                )
            )

        conn.commit()


# ---------------------------------------------------------------------------
# Pytest fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def amazon_db_engine():
    """Amazon-like e-commerce database with ~40 tables across 7 schemas."""
    engine = create_engine("sqlite://", echo=False)

    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    metadata = _build_amazon_metadata()
    metadata.create_all(engine)
    _insert_amazon_sample_data(engine, metadata)
    return engine
