"""Correctness + token benchmark — does dbook help agents write better SQL?"""

# ruff: noqa: S101

from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass

import pytest  # type: ignore[import-untyped]
from sqlalchemy import (  # type: ignore[import-untyped]
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    event,
    text,
)

from dbook.catalog import SQLAlchemyCatalog
from dbook.compiler import compile_book
from tests.benchmark_helpers import AgentSimulator, count_tokens


# --- 5-table fixture (inline, simple e-commerce) ---

def create_5_table_db():
    """Create a minimal 5-table e-commerce DB."""
    engine = create_engine("sqlite://")

    @event.listens_for(engine, "connect")
    def pragma(c, r):
        cursor = c.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    metadata = MetaData()

    users = Table("users", metadata,
        Column("id", Integer, primary_key=True),
        Column("email", String(255), nullable=False, unique=True),
        Column("name", String(100), nullable=False),
        Column("plan", String(20), nullable=False),  # free, pro, enterprise
        Column("is_active", Boolean, default=True),
        Column("created_at", DateTime, nullable=False),
    )

    products = Table("products", metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(200), nullable=False),
        Column("price", Float, nullable=False),
        Column("category", String(50), nullable=False),  # electronics, books, clothing
        Column("in_stock", Boolean, default=True),
    )

    orders = Table("orders", metadata,
        Column("id", Integer, primary_key=True),
        Column("user_id", Integer, ForeignKey("users.id"), nullable=False),
        Column("total", Float, nullable=False),
        Column("status", String(20), nullable=False),  # pending, confirmed, shipped, delivered, cancelled
        Column("created_at", DateTime, nullable=False),
        Index("idx_orders_user", "user_id"),
        Index("idx_orders_status", "status"),
    )

    order_items = Table("order_items", metadata,
        Column("id", Integer, primary_key=True),
        Column("order_id", Integer, ForeignKey("orders.id"), nullable=False),
        Column("product_id", Integer, ForeignKey("products.id"), nullable=False),
        Column("quantity", Integer, nullable=False),
        Column("unit_price", Float, nullable=False),
    )

    payments = Table("payments", metadata,
        Column("id", Integer, primary_key=True),
        Column("order_id", Integer, ForeignKey("orders.id"), nullable=False),
        Column("amount", Float, nullable=False),
        Column("method", String(20), nullable=False),  # credit_card, debit_card, paypal, bank_transfer
        Column("status", String(20), nullable=False),  # pending, completed, failed, refunded
        Column("processed_at", DateTime, nullable=False),
    )

    metadata.create_all(engine)

    # Insert sample data
    from datetime import datetime
    with engine.connect() as conn:
        conn.execute(users.insert(), [
            {"id": 1, "email": "alice@example.com", "name": "Alice Smith", "plan": "pro", "is_active": True, "created_at": datetime(2025, 1, 1)},
            {"id": 2, "email": "bob@example.com", "name": "Bob Jones", "plan": "free", "is_active": True, "created_at": datetime(2025, 2, 1)},
            {"id": 3, "email": "carol@example.com", "name": "Carol White", "plan": "enterprise", "is_active": False, "created_at": datetime(2025, 3, 1)},
        ])
        conn.execute(products.insert(), [
            {"id": 1, "name": "Wireless Headphones", "price": 79.99, "category": "electronics", "in_stock": True},
            {"id": 2, "name": "Python Cookbook", "price": 45.00, "category": "books", "in_stock": True},
            {"id": 3, "name": "Running Shoes", "price": 120.00, "category": "clothing", "in_stock": False},
        ])
        conn.execute(orders.insert(), [
            {"id": 1, "user_id": 1, "total": 124.99, "status": "delivered", "created_at": datetime(2025, 6, 1)},
            {"id": 2, "user_id": 1, "total": 45.00, "status": "shipped", "created_at": datetime(2025, 6, 15)},
            {"id": 3, "user_id": 2, "total": 79.99, "status": "pending", "created_at": datetime(2025, 7, 1)},
        ])
        conn.execute(order_items.insert(), [
            {"id": 1, "order_id": 1, "product_id": 1, "quantity": 1, "unit_price": 79.99},
            {"id": 2, "order_id": 1, "product_id": 2, "quantity": 1, "unit_price": 45.00},
            {"id": 3, "order_id": 2, "product_id": 2, "quantity": 1, "unit_price": 45.00},
            {"id": 4, "order_id": 3, "product_id": 1, "quantity": 1, "unit_price": 79.99},
        ])
        conn.execute(payments.insert(), [
            {"id": 1, "order_id": 1, "amount": 124.99, "method": "credit_card", "status": "completed", "processed_at": datetime(2025, 6, 1)},
            {"id": 2, "order_id": 2, "amount": 45.00, "method": "paypal", "status": "completed", "processed_at": datetime(2025, 6, 15)},
            {"id": 3, "order_id": 3, "amount": 79.99, "method": "bank_transfer", "status": "pending", "processed_at": datetime(2025, 7, 1)},
        ])
        conn.commit()

    return engine


@pytest.fixture
def small_db_engine():
    return create_5_table_db()


# --- Correctness tasks ---
# Each task defines:
# - question: what the agent needs to answer
# - key_facts: facts that MUST be present in metadata for correct SQL
# - For each key_fact, we check if it appears in the agent's read content

@dataclass
class CorrectnessTask:
    id: str
    question: str
    key_facts: list[str]  # strings that must appear in agent's read content
    tables_needed: list[str]  # tables agent should find

CORRECTNESS_TASKS = [
    CorrectnessTask(
        id="CQ1",
        question="Count delivered orders over $100",
        key_facts=["delivered", "total", "status"],  # Agent needs to know status values and total column
        tables_needed=["orders"],
    ),
    CorrectnessTask(
        id="CQ2",
        question="Get email addresses of customers on the pro plan",
        key_facts=["email", "plan", "pro"],  # Agent needs to know plan values
        tables_needed=["users"],
    ),
    CorrectnessTask(
        id="CQ3",
        question="Find total revenue from credit card payments",
        key_facts=["amount", "method", "credit_card", "completed"],  # Need payment method values AND status values
        tables_needed=["payments"],
    ),
    CorrectnessTask(
        id="CQ4",
        question="List products in the electronics category that are in stock",
        key_facts=["category", "electronics", "in_stock"],
        tables_needed=["products"],
    ),
    CorrectnessTask(
        id="CQ5",
        question="Get order details with customer name and email",
        key_facts=["user_id", "users", "name", "email", "JOIN"],  # Need to know the FK join
        tables_needed=["orders", "users"],
    ),
    CorrectnessTask(
        id="CQ6",
        question="Calculate average order value by payment method",
        key_facts=["total", "method", "order_id", "JOIN"],  # Need join path orders->payments
        tables_needed=["orders", "payments"],
    ),
    CorrectnessTask(
        id="CQ7",
        question="Find which products have never been ordered",
        key_facts=["product_id", "order_items", "LEFT JOIN"],  # Need to know the relationship
        tables_needed=["products", "order_items"],
    ),
    CorrectnessTask(
        id="CQ8",
        question="Get refunded payments with order status",
        key_facts=["refunded", "status", "order_id", "JOIN"],  # Need payment status values + join
        tables_needed=["payments", "orders"],
    ),
]


def _get_ddl(engine) -> str:
    """Get raw DDL for all tables."""
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL"
        ))
        return "\n\n".join(row[0] for row in result)


def _check_correctness(content: str, task: CorrectnessTask) -> tuple[int, int, list[str]]:
    """Check how many key facts are present in the content.
    Returns (found, total, missing_facts)."""
    content_lower = content.lower()
    found = 0
    missing = []
    for fact in task.key_facts:
        if fact.lower() in content_lower:
            found += 1
        else:
            missing.append(fact)
    return found, len(task.key_facts), missing


def _run_no_dbook(engine, task: CorrectnessTask) -> tuple[int, int, int, list[str]]:
    """Agent reads all DDL. Returns (tokens, facts_found, facts_total, missing)."""
    ddl = _get_ddl(engine)
    tokens = count_tokens(ddl)
    found, total, missing = _check_correctness(ddl, task)
    return tokens, found, total, missing


def _run_dbook_agent(dbook_path: Path, task: CorrectnessTask) -> tuple[int, int, int, list[str]]:
    """Agent navigates dbook. Returns (tokens, facts_found, facts_total, missing)."""
    agent = AgentSimulator(dbook_path)

    # Read NAVIGATION.md
    nav = agent.read_file("NAVIGATION.md")
    all_content = nav

    # Find relevant tables from navigation
    for table_name in task.tables_needed:
        schemas_dir = dbook_path / "schemas"
        for schema_dir in schemas_dir.iterdir():
            if schema_dir.is_dir():
                table_file = schema_dir / f"{table_name}.md"
                if table_file.exists():
                    content = agent.read_file(str(table_file.relative_to(dbook_path)))
                    all_content += "\n" + content

    found, total, missing = _check_correctness(all_content, task)
    return agent.tokens_consumed, found, total, missing


class TestCorrectnessSmallDB:
    """Correctness benchmark on 5-table DB."""

    def test_full_correctness_comparison(self, small_db_engine, tmp_path):
        """Compare no-dbook vs dbook on correctness AND tokens for 5-table DB."""
        # Compile dbook
        catalog = SQLAlchemyCatalog(small_db_engine)
        book = catalog.introspect_all()
        compile_book(book, tmp_path)

        ddl_tokens = count_tokens(_get_ddl(small_db_engine))

        separator = "=" * 90
        print(f"\n{separator}")  # noqa: T201
        print("  CORRECTNESS BENCHMARK: 5-Table DB")  # noqa: T201
        print(f"  DDL baseline: {ddl_tokens} tokens")  # noqa: T201
        print(separator)  # noqa: T201
        print(f"\n  {'Task':<6} {'Question':<45} {'No-dbook':>10} {'dbook':>10} {'Save':>7} {'DDL Facts':>10} {'dbook Facts':>12}")  # noqa: T201
        print(f"  {'-' * 85}")  # noqa: T201

        total_no_dbook_facts = 0
        total_dbook_facts = 0
        total_facts = 0

        for task in CORRECTNESS_TASKS:
            nd_tok, nd_found, nd_total, _nd_missing = _run_no_dbook(small_db_engine, task)
            db_tok, db_found, db_total, db_missing = _run_dbook_agent(tmp_path, task)

            save = f"{(1 - db_tok / nd_tok) * 100:.0f}%" if nd_tok > 0 else "N/A"
            q_short = task.question[:43] + ".." if len(task.question) > 45 else task.question

            nd_score = f"{nd_found}/{nd_total}"
            db_score = f"{db_found}/{db_total}"

            # Mark improvements
            marker = ""
            if db_found > nd_found:
                marker = " <- dbook wins"
            elif db_found < nd_found:
                marker = " <- DDL wins"

            print(f"  {task.id:<6} {q_short:<45} {nd_tok:>10} {db_tok:>10} {save:>7} {nd_score:>10} {db_score:>12}{marker}")  # noqa: T201

            if db_missing and db_found < db_total:
                print(f"         Missing in dbook: {', '.join(db_missing)}")  # noqa: T201

            total_no_dbook_facts += nd_found
            total_dbook_facts += db_found
            total_facts += nd_total

        print(f"  {'-' * 85}")  # noqa: T201
        nd_pct = total_no_dbook_facts / total_facts * 100 if total_facts else 0
        db_pct = total_dbook_facts / total_facts * 100 if total_facts else 0
        print(f"  {'TOTAL':<6} {'':45} {'':>10} {'':>10} {'':>7} {nd_pct:>9.0f}% {db_pct:>11.0f}%")  # noqa: T201
        print(f"\n  Key fact coverage: DDL={total_no_dbook_facts}/{total_facts} ({nd_pct:.0f}%)  dbook={total_dbook_facts}/{total_facts} ({db_pct:.0f}%)")  # noqa: T201
        if db_pct > nd_pct:
            print(f"  dbook improvement: +{db_pct - nd_pct:.0f}% more key facts available")  # noqa: T201
        print()  # noqa: T201

        # dbook should have at least as many facts as DDL
        assert total_dbook_facts >= total_no_dbook_facts, (
            f"dbook ({total_dbook_facts}) should have at least as many key facts as DDL ({total_no_dbook_facts})"
        )


class TestCorrectnessAllScales:
    """Correctness benchmark across 5, 13, and 50 tables."""

    def test_correctness_across_scales(self, small_db_engine, db_engine, scaled_db_engine, tmp_path):
        """Compare correctness at 3 scales."""
        engines = [
            ("5 tables", small_db_engine),
            ("13 tables", db_engine),
            ("50 tables", scaled_db_engine),
        ]

        separator = "=" * 70
        print(f"\n{separator}")  # noqa: T201
        print("  CORRECTNESS ACROSS SCALES")  # noqa: T201
        print(separator)  # noqa: T201

        for label, engine in engines:
            catalog = SQLAlchemyCatalog(engine)
            book = catalog.introspect_all()
            out_dir = tmp_path / label.replace(" ", "_")
            compile_book(book, out_dir)

            ddl = _get_ddl(engine)
            _ddl_tokens = count_tokens(ddl)

            # Only run tasks whose tables exist in this DB
            all_tables: set[str] = set()
            for schema in book.schemas.values():
                all_tables.update(schema.tables.keys())

            applicable_tasks = [
                t for t in CORRECTNESS_TASKS
                if all(tn in all_tables for tn in t.tables_needed)
            ]

            if not applicable_tasks:
                print(f"\n  {label}: No applicable tasks (tables don't match)")  # noqa: T201
                continue

            nd_total_facts = 0
            db_total_facts = 0
            total_facts = 0
            nd_total_tok = 0
            db_total_tok = 0

            for task in applicable_tasks:
                nd_tok, nd_found, nd_total, _ = _run_no_dbook(engine, task)
                db_tok, db_found, db_total, _ = _run_dbook_agent(out_dir, task)
                nd_total_facts += nd_found
                db_total_facts += db_found
                total_facts += nd_total
                nd_total_tok += nd_tok
                db_total_tok += db_tok

            n = len(applicable_tasks)
            nd_avg_tok = nd_total_tok / n
            db_avg_tok = db_total_tok / n
            tok_save = (1 - db_avg_tok / nd_avg_tok) * 100
            nd_pct = nd_total_facts / total_facts * 100
            db_pct = db_total_facts / total_facts * 100

            print(f"\n  {label} ({n} tasks):")  # noqa: T201
            print(f"    Tokens:      DDL={nd_avg_tok:.0f}  dbook={db_avg_tok:.0f}  savings={tok_save:.0f}%")  # noqa: T201
            print(f"    Correctness: DDL={nd_total_facts}/{total_facts} ({nd_pct:.0f}%)  dbook={db_total_facts}/{total_facts} ({db_pct:.0f}%)")  # noqa: T201
            if db_pct > nd_pct:
                print(f"    dbook advantage: +{db_pct - nd_pct:.0f}% more key facts")  # noqa: T201

        print()  # noqa: T201
