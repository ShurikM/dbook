"""Amazon DB correctness benchmark — do agents get enough facts for correct SQL?"""

# ruff: noqa: S101

from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass

from sqlalchemy import text  # type: ignore[import-untyped]

from dbook.catalog import SQLAlchemyCatalog
from dbook.compiler import compile_book
from dbook.llm.provider import MockProvider
from dbook.llm.enricher import enrich_book
from tests.benchmark_helpers import AgentSimulator, count_tokens
from tests.amazon_fixture import amazon_db_engine  # noqa: F401


@dataclass
class CorrectnessTask:
    id: str
    agent: str
    question: str
    tables_needed: list[str]
    key_facts: list[str]  # Must appear in agent's read content for correct SQL


AMAZON_TASKS = [
    # BILLING AGENT
    CorrectnessTask(
        id="B1", agent="billing",
        question="Process a refund for a returned item",
        tables_needed=["billing_refunds", "billing_payments", "orders_returns"],
        key_facts=["refund", "amount", "payment_id", "return_id", "status"],
    ),
    CorrectnessTask(
        id="B2", agent="billing",
        question="Generate monthly invoice summary for a customer",
        tables_needed=["billing_invoices", "customers_accounts"],
        key_facts=["invoice", "amount", "total", "account_id", "issued_at", "status"],
    ),
    CorrectnessTask(
        id="B3", agent="billing",
        question="Check subscription renewal status and upcoming charges",
        tables_needed=["billing_subscriptions", "billing_subscription_payments"],
        key_facts=["subscription", "next_billing_at", "price", "status", "billing_cycle"],
    ),
    CorrectnessTask(
        id="B4", agent="billing",
        question="Apply a promotional discount code to a pending order",
        tables_needed=["billing_promotions", "orders_orders"],
        key_facts=["promotion", "code", "value", "min_order", "valid_from", "valid_until"],
    ),
    CorrectnessTask(
        id="B5", agent="billing",
        question="Redeem a gift card balance for an order payment",
        tables_needed=["billing_gift_cards"],
        key_facts=["gift_card", "balance", "code", "status"],
    ),
    # SALES AGENT
    CorrectnessTask(
        id="S1", agent="sales",
        question="Find top-selling products with their reviews",
        tables_needed=["catalog_products", "catalog_reviews"],
        key_facts=["product", "rating", "review", "title", "product_id"],
    ),
    CorrectnessTask(
        id="S2", agent="sales",
        question="Check real-time inventory across warehouses",
        tables_needed=["catalog_inventory", "warehouse_warehouses"],
        key_facts=["inventory", "quantity", "warehouse_id", "product_id", "reserved"],
    ),
    CorrectnessTask(
        id="S3", agent="sales",
        question="Get customer's cart contents and prices",
        tables_needed=["orders_carts", "orders_cart_items"],
        key_facts=["cart", "product_id", "quantity", "unit_price", "account_id"],
    ),
    CorrectnessTask(
        id="S4", agent="sales",
        question="Track an order's shipment status and delivery",
        tables_needed=["orders_shipments", "orders_orders"],
        key_facts=["shipment", "tracking_number", "status", "carrier", "delivered_at"],
    ),
    CorrectnessTask(
        id="S5", agent="sales",
        question="Find what customers who bought X also bought",
        tables_needed=["orders_order_items", "orders_orders"],
        key_facts=["order_item", "product_id", "order_id", "account_id"],
    ),
    # SUPPORT AGENT
    CorrectnessTask(
        id="C1", agent="support",
        question="Look up customer's recent orders and open support tickets",
        tables_needed=["support_tickets", "orders_orders", "customers_accounts"],
        key_facts=["ticket", "order_id", "account_id", "status", "subject"],
    ),
    CorrectnessTask(
        id="C2", agent="support",
        question="Find FAQ articles related to returns",
        tables_needed=["support_faq_articles"],
        key_facts=["faq", "article", "category", "title", "body"],
    ),
    CorrectnessTask(
        id="C3", agent="support",
        question="Verify customer identity: check email, phone, recent orders",
        tables_needed=["customers_accounts", "orders_orders"],
        key_facts=["email", "phone", "name", "account_id", "status"],
    ),
    # ANALYTICS AGENT
    CorrectnessTask(
        id="A1", agent="analytics",
        question="Analyze search-to-purchase conversion rates",
        tables_needed=["analytics_search_queries", "analytics_conversion_funnels"],
        key_facts=["search", "query_text", "results_count", "clicked_product_id", "conversion"],
    ),
    CorrectnessTask(
        id="A2", agent="analytics",
        question="Get A/B test results for the new checkout flow",
        tables_needed=["analytics_ab_tests"],
        key_facts=["ab_test", "variant", "metric_value", "significance", "sample_size"],
    ),
]


def _get_ddl(engine) -> str:
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL"
        ))
        return "\n\n".join(row[0] for row in result)


def _check_facts(content: str, task: CorrectnessTask) -> tuple[int, int, list[str]]:
    content_lower = content.lower()
    found = 0
    missing = []
    for fact in task.key_facts:
        if fact.lower() in content_lower:
            found += 1
        else:
            missing.append(fact)
    return found, len(task.key_facts), missing


def _run_no_dbook(engine, task: CorrectnessTask) -> tuple[int, int, int]:
    ddl = _get_ddl(engine)
    tokens = count_tokens(ddl)
    found, total, _ = _check_facts(ddl, task)
    return tokens, found, total


def _run_dbook_agent(dbook_path: Path, task: CorrectnessTask) -> tuple[int, int, int, list[str]]:
    agent = AgentSimulator(dbook_path)
    nav = agent.read_file("NAVIGATION.md")
    all_content = nav

    # Read the tables this task needs
    for table_name in task.tables_needed:
        schemas_dir = dbook_path / "schemas"
        for schema_dir in schemas_dir.iterdir():
            if schema_dir.is_dir():
                table_file = schema_dir / f"{table_name}.md"
                if table_file.exists():
                    content = agent.read_file(str(table_file.relative_to(dbook_path)))
                    all_content += "\n" + content

    found, total, missing = _check_facts(all_content, task)
    return agent.tokens_consumed, found, total, missing


class TestAmazonCorrectness:
    """Amazon DB: correctness comparison -- DDL vs Base dbook vs LLM dbook."""

    def test_amazon_correctness_full(self, amazon_db_engine, tmp_path):  # noqa: F811
        """Run all 15 tasks measuring key fact coverage."""
        # Get DDL baseline
        ddl = _get_ddl(amazon_db_engine)
        ddl_tokens = count_tokens(ddl)

        # Compile base
        catalog = SQLAlchemyCatalog(amazon_db_engine)
        base_book = catalog.introspect_all()
        base_dir = tmp_path / "base"
        compile_book(base_book, base_dir)

        # Compile LLM
        llm_book = catalog.introspect_all()
        llm_book.mode = "llm"
        enrich_book(llm_book, MockProvider())
        llm_dir = tmp_path / "llm"
        compile_book(llm_book, llm_dir)

        # Run all tasks
        print(f"\n{'=' * 110}")  # noqa: T201
        print("  AMAZON E-COMMERCE DB -- CORRECTNESS BENCHMARK")  # noqa: T201
        print("  ~34 tables, 15 business tasks, 4 agent types")  # noqa: T201
        print(f"  DDL baseline: {ddl_tokens} tokens")  # noqa: T201
        print(f"{'=' * 110}")  # noqa: T201
        print(f"\n  {'Task':<5} {'Agent':<10} {'Question':<42} {'DDL tok':>8} {'Base tok':>9} {'LLM tok':>9} {'DDL Facts':>10} {'Base Facts':>11} {'LLM Facts':>10}")  # noqa: T201
        print(f"  {'-' * 105}")  # noqa: T201

        ddl_total_facts = 0
        base_total_facts = 0
        llm_total_facts = 0
        total_facts = 0
        ddl_total_tok = 0
        base_total_tok = 0
        llm_total_tok = 0

        by_agent = {}

        for task in AMAZON_TASKS:
            nd_tok, nd_found, nd_total = _run_no_dbook(amazon_db_engine, task)
            base_tok, base_found, base_total_t, base_missing = _run_dbook_agent(base_dir, task)
            llm_tok, llm_found, llm_total_t, llm_missing = _run_dbook_agent(llm_dir, task)

            q_short = task.question[:40] + ".." if len(task.question) > 42 else task.question

            marker = ""
            if base_found > nd_found:
                marker = " +"
            if llm_found > base_found:
                marker += " LLM+"

            print(f"  {task.id:<5} {task.agent:<10} {q_short:<42} {nd_tok:>8} {base_tok:>9} {llm_tok:>9} {nd_found:>4}/{nd_total:<5} {base_found:>4}/{base_total_t:<6} {llm_found:>4}/{llm_total_t:<5}{marker}")  # noqa: T201

            if base_missing:
                print(f"         Base missing: {', '.join(base_missing)}")  # noqa: T201

            ddl_total_facts += nd_found
            base_total_facts += base_found
            llm_total_facts += llm_found
            total_facts += nd_total
            ddl_total_tok += nd_tok
            base_total_tok += base_tok
            llm_total_tok += llm_tok

            # Track by agent type
            if task.agent not in by_agent:
                by_agent[task.agent] = {"ddl": 0, "base": 0, "llm": 0, "total": 0, "count": 0}
            by_agent[task.agent]["ddl"] += nd_found
            by_agent[task.agent]["base"] += base_found
            by_agent[task.agent]["llm"] += llm_found
            by_agent[task.agent]["total"] += nd_total
            by_agent[task.agent]["count"] += 1

        n = len(AMAZON_TASKS)
        print(f"  {'-' * 105}")  # noqa: T201

        ddl_pct = ddl_total_facts / total_facts * 100
        base_pct = base_total_facts / total_facts * 100
        llm_pct = llm_total_facts / total_facts * 100

        ddl_avg = ddl_total_tok / n
        base_avg = base_total_tok / n
        llm_avg = llm_total_tok / n
        base_save = (1 - base_avg / ddl_avg) * 100
        llm_save = (1 - llm_avg / ddl_avg) * 100

        print(f"  {'TOTAL':<5} {'':10} {'':42} {ddl_avg:>8.0f} {base_avg:>9.0f} {llm_avg:>9.0f} {ddl_pct:>9.0f}% {base_pct:>10.0f}% {llm_pct:>9.0f}%")  # noqa: T201

        print("\n  SUMMARY:")  # noqa: T201
        print(f"    Key fact coverage:  DDL={ddl_total_facts}/{total_facts} ({ddl_pct:.0f}%)  Base={base_total_facts}/{total_facts} ({base_pct:.0f}%)  LLM={llm_total_facts}/{total_facts} ({llm_pct:.0f}%)")  # noqa: T201
        print(f"    Avg tokens/task:    DDL={ddl_avg:.0f}  Base={base_avg:.0f} ({base_save:+.0f}%)  LLM={llm_avg:.0f} ({llm_save:+.0f}%)")  # noqa: T201

        if base_pct > ddl_pct:
            print(f"    dbook Base advantage: +{base_pct - ddl_pct:.0f}% more key facts")  # noqa: T201
        if llm_pct > base_pct:
            print(f"    dbook LLM advantage:  +{llm_pct - base_pct:.0f}% more key facts vs base")  # noqa: T201

        # By agent breakdown
        print("\n  BY AGENT TYPE:")  # noqa: T201
        print(f"    {'Agent':<12} {'Tasks':>6} {'DDL':>12} {'Base dbook':>12} {'LLM dbook':>12}")  # noqa: T201
        for agent_name, stats in sorted(by_agent.items()):
            d_pct = stats['ddl'] / stats['total'] * 100
            b_pct = stats['base'] / stats['total'] * 100
            l_pct = stats['llm'] / stats['total'] * 100
            print(f"    {agent_name:<12} {stats['count']:>6} {d_pct:>11.0f}% {b_pct:>11.0f}% {l_pct:>11.0f}%")  # noqa: T201

        print()  # noqa: T201

        # Assertions
        assert base_total_facts >= ddl_total_facts, f"Base dbook ({base_total_facts}) should have >= DDL facts ({ddl_total_facts})"
        assert llm_total_facts >= base_total_facts, f"LLM ({llm_total_facts}) should have >= Base facts ({base_total_facts})"
