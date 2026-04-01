# ruff: noqa: S311
"""Shared test fixtures — SQLite in-memory database with realistic schema."""

from __future__ import annotations

import pytest
from sqlalchemy import (
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
    Boolean,
    CheckConstraint,
    create_engine,
    event,
)
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import DeclarativeBase, Session


class Base(DeclarativeBase):
    pass


# --- Auth schema ---

class AuthUsers(Base):
    __tablename__ = "auth_users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), nullable=False, unique=True)
    name = Column(String(100), nullable=False)
    phone = Column(String(20), nullable=True)
    password_hash = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=True)


class AuthSessions(Base):
    __tablename__ = "auth_sessions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("auth_users.id"), nullable=False)
    token = Column(String(255), nullable=False)
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False)
    expires_at = Column(DateTime, nullable=False)

    __table_args__ = (
        Index("idx_sessions_user_id", "user_id"),
        Index("idx_sessions_token", "token", unique=True),
    )


class AuthRoles(Base):
    __tablename__ = "auth_roles"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False, unique=True)
    description = Column(Text, nullable=True)


class AuthUserRoles(Base):
    __tablename__ = "auth_user_roles"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("auth_users.id"), nullable=False)
    role_id = Column(Integer, ForeignKey("auth_roles.id"), nullable=False)

    __table_args__ = (
        Index("idx_user_roles_user", "user_id"),
        Index("idx_user_roles_role", "role_id"),
    )


# --- Billing schema ---

class BillingProducts(Base):
    __tablename__ = "billing_products"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    price = Column(Float, nullable=False)
    category = Column(String(50), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, nullable=False)


class BillingDiscounts(Base):
    __tablename__ = "billing_discounts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(50), nullable=False, unique=True)
    percentage = Column(Float, nullable=False)
    valid_from = Column(DateTime, nullable=False)
    valid_until = Column(DateTime, nullable=True)
    max_uses = Column(Integer, nullable=True)


class BillingOrders(Base):
    __tablename__ = "billing_orders"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("auth_users.id"), nullable=False)
    total = Column(Float, nullable=False)
    status = Column(String(20), nullable=False, default="pending")
    discount_id = Column(Integer, ForeignKey("billing_discounts.id"), nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("idx_orders_user", "user_id"),
        Index("idx_orders_status", "status"),
        Index("idx_orders_created", "created_at"),
        CheckConstraint("total >= 0", name="ck_orders_total_positive"),
    )


class BillingOrderItems(Base):
    __tablename__ = "billing_order_items"
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey("billing_orders.id"), nullable=False)
    product_id = Column(Integer, ForeignKey("billing_products.id"), nullable=False)
    quantity = Column(Integer, nullable=False, default=1)
    unit_price = Column(Float, nullable=False)

    __table_args__ = (
        Index("idx_order_items_order", "order_id"),
        Index("idx_order_items_product", "product_id"),
    )


class BillingInvoices(Base):
    __tablename__ = "billing_invoices"
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey("billing_orders.id"), nullable=False)
    contact_email = Column(String(255), nullable=False)
    amount = Column(Float, nullable=False)
    status = Column(String(20), nullable=False, default="draft")
    issued_at = Column(DateTime, nullable=True)
    paid_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("idx_invoices_order", "order_id"),
    )


class BillingPayments(Base):
    __tablename__ = "billing_payments"
    id = Column(Integer, primary_key=True, autoincrement=True)
    invoice_id = Column(Integer, ForeignKey("billing_invoices.id"), nullable=False)
    amount = Column(Float, nullable=False)
    method = Column(String(20), nullable=False)
    card_last_four = Column(String(4), nullable=True)
    processed_at = Column(DateTime, nullable=False)

    __table_args__ = (
        Index("idx_payments_invoice", "invoice_id"),
    )


# --- Analytics schema ---

class AnalyticsEvents(Base):
    __tablename__ = "analytics_events"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("auth_users.id"), nullable=True)
    event_type = Column(String(50), nullable=False)
    page = Column(String(255), nullable=True)
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False)

    __table_args__ = (
        Index("idx_events_user", "user_id"),
        Index("idx_events_type", "event_type"),
        Index("idx_events_created", "created_at"),
    )


class AnalyticsDailyRevenue(Base):
    __tablename__ = "analytics_daily_revenue"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, nullable=False, unique=True)
    total_revenue = Column(Float, nullable=False)
    order_count = Column(Integer, nullable=False)
    avg_order_value = Column(Float, nullable=True)


class AnalyticsFunnels(Base):
    __tablename__ = "analytics_funnels"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    steps = Column(Text, nullable=False)
    conversion_rate = Column(Float, nullable=True)
    created_at = Column(DateTime, nullable=False)


# --- Fixtures ---

@pytest.fixture
def db_engine():
    """Create an in-memory SQLite database with all tables and sample data."""
    engine = create_engine("sqlite://", echo=False)

    # Enable foreign keys for SQLite
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    Base.metadata.create_all(engine)
    _insert_sample_data(engine)
    return engine


class _DetRng:
    """Deterministic PRNG for test data (not used for security)."""

    def __init__(self, seed: int = 42) -> None:
        self._state = seed

    def _next(self) -> int:
        # Linear congruential generator (Numerical Recipes constants)
        self._state = (self._state * 1664525 + 1013904223) & 0xFFFFFFFF
        return self._state

    def randint(self, lo: int, hi: int) -> int:
        return lo + self._next() % (hi - lo + 1)

    def uniform(self, lo: float, hi: float) -> float:
        return lo + (self._next() / 0xFFFFFFFF) * (hi - lo)

    def choice(self, seq: list | tuple):  # noqa: ANN001
        return seq[self._next() % len(seq)]


def _insert_sample_data(engine):
    """Insert realistic sample data."""
    from datetime import datetime, timedelta

    rng = _DetRng(42)

    with Session(engine) as session:
        # Users
        users = []
        for i in range(1, 21):
            u = AuthUsers(
                email=f"user{i}@example.com",
                name=f"User {i}",
                phone=f"+1-555-{i:04d}" if i % 3 == 0 else None,
                password_hash=f"hash_{i}",
                is_active=i % 5 != 0,
                created_at=datetime(2025, 1, 1) + timedelta(days=i),
                updated_at=datetime(2025, 6, 1) + timedelta(days=i) if i % 2 == 0 else None,
            )
            session.add(u)
            users.append(u)
        session.flush()

        # Roles
        for role_name in ["admin", "editor", "viewer", "billing_admin", "support"]:
            session.add(AuthRoles(name=role_name, description=f"{role_name} role"))
        session.flush()

        # User roles
        for i in range(1, 11):
            session.add(AuthUserRoles(user_id=i, role_id=(i % 5) + 1))
        session.flush()

        # Sessions
        for i in range(1, 31):
            session.add(AuthSessions(
                user_id=(i % 20) + 1,
                token=f"tok_{i:06d}",
                ip_address=f"192.168.1.{i % 255}",
                user_agent=f"Mozilla/5.0 (Agent {i})",
                created_at=datetime(2025, 6, 1) + timedelta(hours=i),
                expires_at=datetime(2025, 6, 2) + timedelta(hours=i),
            ))
        session.flush()

        # Products
        products = []
        for i in range(1, 11):
            p = BillingProducts(
                name=f"Product {i}",
                description=f"Description for product {i}",
                price=round(rng.uniform(9.99, 299.99), 2),
                category=rng.choice(["electronics", "books", "clothing", "food"]),
                is_active=True,
                created_at=datetime(2025, 1, 1),
            )
            session.add(p)
            products.append(p)
        session.flush()

        # Discounts
        for i in range(1, 4):
            session.add(BillingDiscounts(
                code=f"SAVE{i}0",
                percentage=i * 10.0,
                valid_from=datetime(2025, 1, 1),
                valid_until=datetime(2025, 12, 31),
                max_uses=100,
            ))
        session.flush()

        # Orders
        for i in range(1, 26):
            session.add(BillingOrders(
                user_id=(i % 20) + 1,
                total=round(rng.uniform(10.0, 500.0), 2),
                status=rng.choice(["pending", "confirmed", "shipped", "delivered"]),
                discount_id=(i % 3) + 1 if i % 5 == 0 else None,
                created_at=datetime(2025, 3, 1) + timedelta(days=i),
                updated_at=datetime(2025, 3, 2) + timedelta(days=i) if i % 2 == 0 else None,
            ))
        session.flush()

        # Order items
        for i in range(1, 51):
            session.add(BillingOrderItems(
                order_id=(i % 25) + 1,
                product_id=(i % 10) + 1,
                quantity=rng.randint(1, 5),
                unit_price=round(rng.uniform(9.99, 99.99), 2),
            ))
        session.flush()

        # Invoices
        for i in range(1, 21):
            session.add(BillingInvoices(
                order_id=(i % 25) + 1,
                contact_email=f"billing{i}@example.com",
                amount=round(rng.uniform(10.0, 500.0), 2),
                status=rng.choice(["draft", "sent", "paid"]),
                issued_at=datetime(2025, 4, 1) + timedelta(days=i),
                paid_at=datetime(2025, 4, 10) + timedelta(days=i) if i % 3 == 0 else None,
            ))
        session.flush()

        # Payments
        for i in range(1, 16):
            session.add(BillingPayments(
                invoice_id=(i % 20) + 1,
                amount=round(rng.uniform(10.0, 500.0), 2),
                method=rng.choice(["credit_card", "debit_card", "bank_transfer"]),
                card_last_four=f"{rng.randint(1000, 9999)}" if i % 2 == 0 else None,
                processed_at=datetime(2025, 4, 15) + timedelta(days=i),
            ))
        session.flush()

        # Events
        for i in range(1, 51):
            session.add(AnalyticsEvents(
                user_id=(i % 20) + 1 if i % 4 != 0 else None,
                event_type=rng.choice(["page_view", "click", "purchase", "signup"]),
                page=rng.choice(["/home", "/products", "/checkout", "/profile"]),
                ip_address=f"10.0.{i % 255}.{(i * 7) % 255}",
                user_agent=f"Mozilla/5.0 (Event Agent {i})",
                created_at=datetime(2025, 6, 1) + timedelta(minutes=i * 30),
            ))
        session.flush()

        # Daily revenue
        for i in range(30):
            session.add(AnalyticsDailyRevenue(
                date=datetime(2025, 6, 1) + timedelta(days=i),
                total_revenue=round(rng.uniform(500.0, 5000.0), 2),
                order_count=rng.randint(5, 50),
                avg_order_value=round(rng.uniform(50.0, 200.0), 2),
            ))
        session.flush()

        # Funnels
        for name in ["signup", "purchase", "onboarding"]:
            session.add(AnalyticsFunnels(
                name=name,
                steps="step1,step2,step3",
                conversion_rate=round(rng.uniform(0.1, 0.9), 2),
                created_at=datetime(2025, 1, 1),
            ))

        session.commit()


@pytest.fixture
def db_url(db_engine):
    """Return the engine directly (SQLite in-memory can't reconnect via URL)."""
    return db_engine


# ---------------------------------------------------------------------------
# Scaled fixture — 50 tables across 5 schema prefixes (MetaData/Table API)
# ---------------------------------------------------------------------------


def _build_scaled_metadata() -> MetaData:
    """Define 50 tables using the Table/Column API for programmatic generation.

    Schema prefixes:
      auth_     (8 tables)
      billing_  (10 tables)
      analytics_(8 tables)
      inventory_(12 tables)
      support_  (12 tables)
    """
    meta = MetaData()

    # -- auth_ (8 tables) --------------------------------------------------
    Table(
        "auth_users_v2", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("email", String(255), nullable=False, unique=True),
        Column("name", String(100), nullable=False),
        Column("phone", String(20), nullable=True),
        Column("password_hash", String(255), nullable=False),
        Column("is_active", Boolean, default=True),
        Column("avatar_url", String(500), nullable=True),
        Column("created_at", DateTime, nullable=False),
        Column("updated_at", DateTime, nullable=True),
    )

    Table(
        "auth_sessions_v2", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("user_id", Integer, ForeignKey("auth_users_v2.id"), nullable=False),
        Column("token", String(255), nullable=False),
        Column("ip_address", String(45), nullable=True),
        Column("user_agent", Text, nullable=True),
        Column("created_at", DateTime, nullable=False),
        Column("expires_at", DateTime, nullable=False),
        Index("idx_sv2_sessions_user", "user_id"),
        Index("idx_sv2_sessions_token", "token", unique=True),
    )

    Table(
        "auth_roles_v2", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(50), nullable=False, unique=True),
        Column("description", Text, nullable=True),
        Column("is_system", Boolean, default=False),
    )

    Table(
        "auth_permissions", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(100), nullable=False, unique=True),
        Column("resource", String(100), nullable=False),
        Column("action", String(50), nullable=False),
        Column("description", Text, nullable=True),
    )

    Table(
        "auth_user_roles_v2", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("user_id", Integer, ForeignKey("auth_users_v2.id"), nullable=False),
        Column("role_id", Integer, ForeignKey("auth_roles_v2.id"), nullable=False),
        Index("idx_sv2_ur_user", "user_id"),
        Index("idx_sv2_ur_role", "role_id"),
    )

    Table(
        "auth_tokens", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("user_id", Integer, ForeignKey("auth_users_v2.id"), nullable=False),
        Column("token_type", String(20), nullable=False),
        Column("token_hash", String(255), nullable=False),
        Column("expires_at", DateTime, nullable=False),
        Column("created_at", DateTime, nullable=False),
        Index("idx_sv2_tokens_user", "user_id"),
    )

    Table(
        "auth_devices", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("user_id", Integer, ForeignKey("auth_users_v2.id"), nullable=False),
        Column("device_name", String(100), nullable=True),
        Column("device_type", String(50), nullable=True),
        Column("ip_address", String(45), nullable=True),
        Column("last_seen_at", DateTime, nullable=True),
        Column("created_at", DateTime, nullable=False),
        Index("idx_sv2_devices_user", "user_id"),
    )

    Table(
        "auth_audit_log", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("user_id", Integer, ForeignKey("auth_users_v2.id"), nullable=True),
        Column("action", String(50), nullable=False),
        Column("resource_type", String(50), nullable=True),
        Column("resource_id", Integer, nullable=True),
        Column("ip_address", String(45), nullable=True),
        Column("details", Text, nullable=True),
        Column("created_at", DateTime, nullable=False),
        Index("idx_sv2_audit_user", "user_id"),
        Index("idx_sv2_audit_action", "action"),
        Index("idx_sv2_audit_created", "created_at"),
    )

    # -- billing_ (10 tables) ----------------------------------------------
    Table(
        "billing_categories", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(100), nullable=False),
        Column("slug", String(100), nullable=False, unique=True),
        Column("parent_id", Integer, ForeignKey("billing_categories.id"), nullable=True),
        Column("description", Text, nullable=True),
    )

    Table(
        "billing_products_v2", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(200), nullable=False),
        Column("sku", String(50), nullable=True, unique=True),
        Column("description", Text, nullable=True),
        Column("price", Float, nullable=False),
        Column("category_id", Integer, ForeignKey("billing_categories.id"), nullable=True),
        Column("is_active", Boolean, default=True),
        Column("weight", Float, nullable=True),
        Column("created_at", DateTime, nullable=False),
        Index("idx_sv2_products_category", "category_id"),
    )

    Table(
        "billing_discounts_v2", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("code", String(50), nullable=False, unique=True),
        Column("percentage", Float, nullable=False),
        Column("valid_from", DateTime, nullable=False),
        Column("valid_until", DateTime, nullable=True),
        Column("max_uses", Integer, nullable=True),
        Column("times_used", Integer, default=0),
    )

    Table(
        "billing_orders_v2", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("user_id", Integer, ForeignKey("auth_users_v2.id"), nullable=False),
        Column("total", Float, nullable=False),
        Column("currency", String(3), default="USD"),
        Column("status", String(20), nullable=False, default="pending"),
        Column("discount_id", Integer, ForeignKey("billing_discounts_v2.id"), nullable=True),
        Column("shipping_address", Text, nullable=True),
        Column("created_at", DateTime, nullable=False),
        Column("updated_at", DateTime, nullable=True),
        Index("idx_sv2_orders_user", "user_id"),
        Index("idx_sv2_orders_status", "status"),
        Index("idx_sv2_orders_created", "created_at"),
        CheckConstraint("total >= 0", name="ck_sv2_orders_total_positive"),
    )

    Table(
        "billing_order_items_v2", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("order_id", Integer, ForeignKey("billing_orders_v2.id"), nullable=False),
        Column("product_id", Integer, ForeignKey("billing_products_v2.id"), nullable=False),
        Column("quantity", Integer, nullable=False, default=1),
        Column("unit_price", Float, nullable=False),
        Column("subtotal", Float, nullable=False),
        Index("idx_sv2_oi_order", "order_id"),
        Index("idx_sv2_oi_product", "product_id"),
    )

    Table(
        "billing_invoices_v2", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("order_id", Integer, ForeignKey("billing_orders_v2.id"), nullable=False),
        Column("contact_email", String(255), nullable=False),
        Column("amount", Float, nullable=False),
        Column("tax_amount", Float, default=0),
        Column("status", String(20), nullable=False, default="draft"),
        Column("issued_at", DateTime, nullable=True),
        Column("due_at", DateTime, nullable=True),
        Column("paid_at", DateTime, nullable=True),
        Index("idx_sv2_inv_order", "order_id"),
    )

    Table(
        "billing_payments_v2", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("invoice_id", Integer, ForeignKey("billing_invoices_v2.id"), nullable=False),
        Column("amount", Float, nullable=False),
        Column("method", String(20), nullable=False),
        Column("card_number", String(4), nullable=True),
        Column("transaction_id", String(100), nullable=True),
        Column("processed_at", DateTime, nullable=False),
        Index("idx_sv2_pay_invoice", "invoice_id"),
    )

    Table(
        "billing_subscriptions", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("user_id", Integer, ForeignKey("auth_users_v2.id"), nullable=False),
        Column("plan_name", String(50), nullable=False),
        Column("price_monthly", Float, nullable=False),
        Column("status", String(20), nullable=False, default="active"),
        Column("trial_ends_at", DateTime, nullable=True),
        Column("started_at", DateTime, nullable=False),
        Column("cancelled_at", DateTime, nullable=True),
        Index("idx_sv2_sub_user", "user_id"),
    )

    Table(
        "billing_refunds", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("payment_id", Integer, ForeignKey("billing_payments_v2.id"), nullable=False),
        Column("amount", Float, nullable=False),
        Column("reason", Text, nullable=True),
        Column("status", String(20), nullable=False, default="pending"),
        Column("processed_at", DateTime, nullable=True),
        Index("idx_sv2_ref_payment", "payment_id"),
    )

    Table(
        "billing_tax_rates", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("country", String(2), nullable=False),
        Column("region", String(100), nullable=True),
        Column("rate", Float, nullable=False),
        Column("name", String(100), nullable=False),
        Column("is_active", Boolean, default=True),
    )

    # -- analytics_ (8 tables) ---------------------------------------------
    Table(
        "analytics_events_v2", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("user_id", Integer, ForeignKey("auth_users_v2.id"), nullable=True),
        Column("event_type", String(50), nullable=False),
        Column("page", String(255), nullable=True),
        Column("ip_address", String(45), nullable=True),
        Column("user_agent", Text, nullable=True),
        Column("session_id", String(100), nullable=True),
        Column("created_at", DateTime, nullable=False),
        Index("idx_sv2_evt_user", "user_id"),
        Index("idx_sv2_evt_type", "event_type"),
        Index("idx_sv2_evt_created", "created_at"),
    )

    Table(
        "analytics_page_views", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("user_id", Integer, ForeignKey("auth_users_v2.id"), nullable=True),
        Column("page_url", String(500), nullable=False),
        Column("referrer", String(500), nullable=True),
        Column("duration_ms", Integer, nullable=True),
        Column("created_at", DateTime, nullable=False),
        Index("idx_sv2_pv_user", "user_id"),
    )

    Table(
        "analytics_sessions", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("user_id", Integer, ForeignKey("auth_users_v2.id"), nullable=True),
        Column("session_token", String(100), nullable=False),
        Column("started_at", DateTime, nullable=False),
        Column("ended_at", DateTime, nullable=True),
        Column("page_count", Integer, default=0),
        Index("idx_sv2_asess_user", "user_id"),
    )

    Table(
        "analytics_funnels_v2", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(100), nullable=False),
        Column("steps", Text, nullable=False),
        Column("conversion_rate", Float, nullable=True),
        Column("created_at", DateTime, nullable=False),
    )

    Table(
        "analytics_cohorts", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(100), nullable=False),
        Column("criteria", Text, nullable=True),
        Column("user_count", Integer, default=0),
        Column("created_at", DateTime, nullable=False),
    )

    Table(
        "analytics_metrics", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(100), nullable=False),
        Column("metric_type", String(50), nullable=False),
        Column("value", Float, nullable=False),
        Column("recorded_date", DateTime, nullable=False),
        Index("idx_sv2_met_date", "recorded_date"),
    )

    Table(
        "analytics_dashboards", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(100), nullable=False),
        Column("owner_id", Integer, ForeignKey("auth_users_v2.id"), nullable=False),
        Column("layout", Text, nullable=True),
        Column("is_public", Boolean, default=False),
        Column("created_at", DateTime, nullable=False),
        Index("idx_sv2_dash_owner", "owner_id"),
    )

    Table(
        "analytics_reports", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("dashboard_id", Integer, ForeignKey("analytics_dashboards.id"), nullable=True),
        Column("name", String(100), nullable=False),
        Column("query_sql", Text, nullable=True),
        Column("schedule", String(50), nullable=True),
        Column("last_run_at", DateTime, nullable=True),
        Column("created_at", DateTime, nullable=False),
    )

    # -- inventory_ (12 tables) --------------------------------------------
    Table(
        "inventory_warehouses", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(100), nullable=False),
        Column("address", Text, nullable=True),
        Column("city", String(100), nullable=True),
        Column("country", String(2), nullable=True),
        Column("is_active", Boolean, default=True),
        Column("capacity", Integer, nullable=True),
    )

    Table(
        "inventory_locations", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("warehouse_id", Integer, ForeignKey("inventory_warehouses.id"), nullable=False),
        Column("aisle", String(10), nullable=False),
        Column("shelf", String(10), nullable=False),
        Column("bin", String(10), nullable=True),
        Index("idx_sv2_loc_wh", "warehouse_id"),
    )

    Table(
        "inventory_stock", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("product_id", Integer, ForeignKey("billing_products_v2.id"), nullable=False),
        Column("location_id", Integer, ForeignKey("inventory_locations.id"), nullable=False),
        Column("quantity", Integer, nullable=False, default=0),
        Column("reserved", Integer, nullable=False, default=0),
        Column("updated_at", DateTime, nullable=True),
        Index("idx_sv2_stock_prod", "product_id"),
        Index("idx_sv2_stock_loc", "location_id"),
    )

    Table(
        "inventory_suppliers", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(200), nullable=False),
        Column("contact_email", String(255), nullable=True),
        Column("phone", String(20), nullable=True),
        Column("country", String(2), nullable=True),
        Column("is_active", Boolean, default=True),
    )

    Table(
        "inventory_purchase_orders", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("supplier_id", Integer, ForeignKey("inventory_suppliers.id"), nullable=False),
        Column("status", String(20), nullable=False, default="draft"),
        Column("total", Float, nullable=False),
        Column("ordered_at", DateTime, nullable=True),
        Column("received_at", DateTime, nullable=True),
        Column("created_at", DateTime, nullable=False),
        Index("idx_sv2_po_supplier", "supplier_id"),
    )

    Table(
        "inventory_purchase_items", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("purchase_order_id", Integer, ForeignKey("inventory_purchase_orders.id"), nullable=False),
        Column("product_id", Integer, ForeignKey("billing_products_v2.id"), nullable=False),
        Column("quantity", Integer, nullable=False),
        Column("unit_cost", Float, nullable=False),
        Index("idx_sv2_pi_po", "purchase_order_id"),
    )

    Table(
        "inventory_transfers", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("from_location_id", Integer, ForeignKey("inventory_locations.id"), nullable=False),
        Column("to_location_id", Integer, ForeignKey("inventory_locations.id"), nullable=False),
        Column("product_id", Integer, ForeignKey("billing_products_v2.id"), nullable=False),
        Column("quantity", Integer, nullable=False),
        Column("status", String(20), nullable=False, default="pending"),
        Column("created_at", DateTime, nullable=False),
    )

    Table(
        "inventory_shipments", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("order_id", Integer, ForeignKey("billing_orders_v2.id"), nullable=False),
        Column("warehouse_id", Integer, ForeignKey("inventory_warehouses.id"), nullable=False),
        Column("tracking_number", String(100), nullable=True),
        Column("carrier", String(50), nullable=True),
        Column("status", String(20), nullable=False, default="preparing"),
        Column("shipped_at", DateTime, nullable=True),
        Column("delivered_at", DateTime, nullable=True),
        Index("idx_sv2_ship_order", "order_id"),
    )

    Table(
        "inventory_shipment_items", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("shipment_id", Integer, ForeignKey("inventory_shipments.id"), nullable=False),
        Column("product_id", Integer, ForeignKey("billing_products_v2.id"), nullable=False),
        Column("quantity", Integer, nullable=False),
        Index("idx_sv2_si_ship", "shipment_id"),
    )

    Table(
        "inventory_returns", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("order_id", Integer, ForeignKey("billing_orders_v2.id"), nullable=False),
        Column("user_id", Integer, ForeignKey("auth_users_v2.id"), nullable=False),
        Column("reason", Text, nullable=True),
        Column("status", String(20), nullable=False, default="requested"),
        Column("refund_amount", Float, nullable=True),
        Column("created_at", DateTime, nullable=False),
        Index("idx_sv2_ret_order", "order_id"),
    )

    Table(
        "inventory_adjustments", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("product_id", Integer, ForeignKey("billing_products_v2.id"), nullable=False),
        Column("location_id", Integer, ForeignKey("inventory_locations.id"), nullable=False),
        Column("quantity_change", Integer, nullable=False),
        Column("reason", Text, nullable=True),
        Column("adjusted_by", Integer, ForeignKey("auth_users_v2.id"), nullable=True),
        Column("created_at", DateTime, nullable=False),
    )

    Table(
        "inventory_categories", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(100), nullable=False),
        Column("parent_id", Integer, ForeignKey("inventory_categories.id"), nullable=True),
        Column("description", Text, nullable=True),
    )

    # -- support_ (12 tables) ----------------------------------------------
    Table(
        "support_teams", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(100), nullable=False),
        Column("description", Text, nullable=True),
        Column("is_active", Boolean, default=True),
    )

    Table(
        "support_agents", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("user_id", Integer, ForeignKey("auth_users_v2.id"), nullable=False),
        Column("team_id", Integer, ForeignKey("support_teams.id"), nullable=True),
        Column("display_name", String(100), nullable=False),
        Column("email", String(255), nullable=False),
        Column("is_available", Boolean, default=True),
        Index("idx_sv2_agent_user", "user_id"),
        Index("idx_sv2_agent_team", "team_id"),
    )

    Table(
        "support_sla_policies", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(100), nullable=False),
        Column("first_response_hours", Integer, nullable=False),
        Column("resolution_hours", Integer, nullable=False),
        Column("priority", String(20), nullable=False),
    )

    Table(
        "support_tickets", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("user_id", Integer, ForeignKey("auth_users_v2.id"), nullable=False),
        Column("agent_id", Integer, ForeignKey("support_agents.id"), nullable=True),
        Column("sla_id", Integer, ForeignKey("support_sla_policies.id"), nullable=True),
        Column("subject", String(255), nullable=False),
        Column("description", Text, nullable=True),
        Column("status", String(20), nullable=False, default="open"),
        Column("priority", String(20), nullable=False, default="medium"),
        Column("channel", String(20), nullable=True),
        Column("created_at", DateTime, nullable=False),
        Column("updated_at", DateTime, nullable=True),
        Column("resolved_at", DateTime, nullable=True),
        Index("idx_sv2_tkt_user", "user_id"),
        Index("idx_sv2_tkt_agent", "agent_id"),
        Index("idx_sv2_tkt_status", "status"),
        Index("idx_sv2_tkt_created", "created_at"),
    )

    Table(
        "support_ticket_comments", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("ticket_id", Integer, ForeignKey("support_tickets.id"), nullable=False),
        Column("author_id", Integer, ForeignKey("auth_users_v2.id"), nullable=False),
        Column("body", Text, nullable=False),
        Column("is_internal", Boolean, default=False),
        Column("created_at", DateTime, nullable=False),
        Index("idx_sv2_tc_ticket", "ticket_id"),
    )

    Table(
        "support_knowledge_base", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("title", String(200), nullable=False),
        Column("slug", String(200), nullable=False, unique=True),
        Column("category", String(100), nullable=True),
        Column("is_published", Boolean, default=False),
        Column("created_at", DateTime, nullable=False),
    )

    Table(
        "support_articles", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("knowledge_base_id", Integer, ForeignKey("support_knowledge_base.id"), nullable=False),
        Column("title", String(200), nullable=False),
        Column("body", Text, nullable=False),
        Column("author_id", Integer, ForeignKey("auth_users_v2.id"), nullable=False),
        Column("view_count", Integer, default=0),
        Column("helpful_count", Integer, default=0),
        Column("created_at", DateTime, nullable=False),
        Column("updated_at", DateTime, nullable=True),
        Index("idx_sv2_art_kb", "knowledge_base_id"),
    )

    Table(
        "support_tags", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(50), nullable=False, unique=True),
        Column("color", String(7), nullable=True),
    )

    Table(
        "support_article_tags", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("article_id", Integer, ForeignKey("support_articles.id"), nullable=False),
        Column("tag_id", Integer, ForeignKey("support_tags.id"), nullable=False),
        Index("idx_sv2_at_article", "article_id"),
        Index("idx_sv2_at_tag", "tag_id"),
    )

    Table(
        "support_customer_feedback", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("ticket_id", Integer, ForeignKey("support_tickets.id"), nullable=False),
        Column("user_id", Integer, ForeignKey("auth_users_v2.id"), nullable=False),
        Column("rating", Integer, nullable=False),
        Column("comment", Text, nullable=True),
        Column("created_at", DateTime, nullable=False),
        Index("idx_sv2_fb_ticket", "ticket_id"),
    )

    Table(
        "support_escalations", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("ticket_id", Integer, ForeignKey("support_tickets.id"), nullable=False),
        Column("from_agent_id", Integer, ForeignKey("support_agents.id"), nullable=True),
        Column("to_agent_id", Integer, ForeignKey("support_agents.id"), nullable=True),
        Column("reason", Text, nullable=True),
        Column("created_at", DateTime, nullable=False),
        Index("idx_sv2_esc_ticket", "ticket_id"),
    )

    Table(
        "support_macros", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(100), nullable=False),
        Column("content", Text, nullable=False),
        Column("created_by", Integer, ForeignKey("auth_users_v2.id"), nullable=True),
        Column("usage_count", Integer, default=0),
        Column("created_at", DateTime, nullable=False),
    )

    return meta


def _insert_scaled_sample_data(engine, metadata: MetaData):
    """Insert 5-20 rows per table using deterministic PRNG (not for security)."""
    from datetime import datetime, timedelta

    rng = _DetRng(42)
    tables = metadata.sorted_tables  # respects FK order
    ts_base = datetime(2025, 1, 1)

    with engine.begin() as conn:
        def _val(col, i):  # noqa: ANN001
            cname = col.name
            ctype = str(col.type).upper()

            if col.primary_key and col.autoincrement:
                return None
            if col.foreign_keys:
                return max(1, (i % 10) + 1)
            if "BOOL" in ctype:
                return i % 3 != 0
            if "DATE" in ctype or "TIME" in ctype:
                return ts_base + timedelta(days=i, hours=i % 24)
            if "FLOAT" in ctype or "REAL" in ctype:
                return round(rng.uniform(1.0, 999.99), 2)
            if "INT" in ctype:
                return rng.randint(0, 1000)
            if "email" in cname:
                return f"user{i}@example.com"
            if "phone" in cname:
                return f"+1-555-{i:04d}"
            if "ip_address" in cname or cname == "ip":
                return f"192.168.{i % 255}.{(i * 7) % 255}"
            if "card_number" in cname:
                return f"{rng.randint(1000, 9999)}"
            if "token" in cname or "hash" in cname:
                return f"tok_{i:06d}"
            if "url" in cname:
                return f"https://example.com/page/{i}"
            if "slug" in cname:
                return f"slug-{i}"
            if "VARCHAR" in ctype or "TEXT" in ctype or "CHAR" in ctype:
                return f"{cname}_{i}"
            return f"val_{i}"

        for tbl in tables:
            row_count = rng.randint(5, 20)
            for i in range(1, row_count + 1):
                row = {}
                for col in tbl.columns:
                    if col.primary_key and col.autoincrement:
                        continue
                    val = _val(col, i)
                    if val is not None:
                        row[col.name] = val
                try:
                    conn.execute(tbl.insert().values(**row))
                except (IntegrityError, OperationalError):
                    pass  # Skip FK/unique constraint violations in test data


@pytest.fixture
def scaled_db_engine():
    """Create a 50-table database for realistic benchmarking."""
    engine = create_engine("sqlite://", echo=False)

    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    metadata = _build_scaled_metadata()
    metadata.create_all(engine)
    _insert_scaled_sample_data(engine, metadata)
    return engine
