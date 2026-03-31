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
    String,
    Text,
    Boolean,
    CheckConstraint,
    create_engine,
    event,
    text,
)
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


def _insert_sample_data(engine):
    """Insert realistic sample data."""
    from datetime import datetime, timedelta
    import random

    random.seed(42)  # Deterministic data

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
                price=round(random.uniform(9.99, 299.99), 2),
                category=random.choice(["electronics", "books", "clothing", "food"]),
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
                total=round(random.uniform(10.0, 500.0), 2),
                status=random.choice(["pending", "confirmed", "shipped", "delivered"]),
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
                quantity=random.randint(1, 5),
                unit_price=round(random.uniform(9.99, 99.99), 2),
            ))
        session.flush()

        # Invoices
        for i in range(1, 21):
            session.add(BillingInvoices(
                order_id=(i % 25) + 1,
                contact_email=f"billing{i}@example.com",
                amount=round(random.uniform(10.0, 500.0), 2),
                status=random.choice(["draft", "sent", "paid"]),
                issued_at=datetime(2025, 4, 1) + timedelta(days=i),
                paid_at=datetime(2025, 4, 10) + timedelta(days=i) if i % 3 == 0 else None,
            ))
        session.flush()

        # Payments
        for i in range(1, 16):
            session.add(BillingPayments(
                invoice_id=(i % 20) + 1,
                amount=round(random.uniform(10.0, 500.0), 2),
                method=random.choice(["credit_card", "debit_card", "bank_transfer"]),
                card_last_four=f"{random.randint(1000, 9999)}" if i % 2 == 0 else None,
                processed_at=datetime(2025, 4, 15) + timedelta(days=i),
            ))
        session.flush()

        # Events
        for i in range(1, 51):
            session.add(AnalyticsEvents(
                user_id=(i % 20) + 1 if i % 4 != 0 else None,
                event_type=random.choice(["page_view", "click", "purchase", "signup"]),
                page=random.choice(["/home", "/products", "/checkout", "/profile"]),
                ip_address=f"10.0.{i % 255}.{(i * 7) % 255}",
                user_agent=f"Mozilla/5.0 (Event Agent {i})",
                created_at=datetime(2025, 6, 1) + timedelta(minutes=i * 30),
            ))
        session.flush()

        # Daily revenue
        for i in range(30):
            session.add(AnalyticsDailyRevenue(
                date=datetime(2025, 6, 1) + timedelta(days=i),
                total_revenue=round(random.uniform(500.0, 5000.0), 2),
                order_count=random.randint(5, 50),
                avg_order_value=round(random.uniform(50.0, 200.0), 2),
            ))
        session.flush()

        # Funnels
        for name in ["signup", "purchase", "onboarding"]:
            session.add(AnalyticsFunnels(
                name=name,
                steps=f"step1,step2,step3",
                conversion_rate=round(random.uniform(0.1, 0.9), 2),
                created_at=datetime(2025, 1, 1),
            ))

        session.commit()


@pytest.fixture
def db_url(db_engine):
    """Return the engine directly (SQLite in-memory can't reconnect via URL)."""
    return db_engine
