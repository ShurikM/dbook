"""Amazon DB correctness benchmark — do agents get enough facts for correct SQL?

Tests for ENUM VALUES, not just column names. These are the facts that DDL
DOESN'T have but dbook DOES (via enum value documentation).
"""

# ruff: noqa: S101

from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass, field

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
    structural_facts: list[str]  # Column/table names — DDL has these too
    value_facts: list[str] = field(default_factory=list)  # Enum values — only dbook has these


AMAZON_TASKS = [
    # BILLING AGENT
    CorrectnessTask(
        id="B1", agent="billing",
        question="Process a refund for a returned item",
        tables_needed=["billing_refunds", "billing_payments", "orders_returns"],
        structural_facts=["refund", "amount", "payment_id", "return_id", "status"],
        value_facts=["processed", "denied", "pending"],  # refund statuses
    ),
    CorrectnessTask(
        id="B2", agent="billing",
        question="Generate monthly invoice summary for a customer",
        tables_needed=["billing_invoices", "customers_accounts"],
        structural_facts=["invoice", "amount", "total", "account_id", "issued_at", "status"],
        value_facts=["sent", "draft", "paid", "overdue"],  # invoice statuses
    ),
    CorrectnessTask(
        id="B3", agent="billing",
        question="Check subscription renewal status and upcoming charges",
        tables_needed=["billing_subscriptions", "billing_subscription_payments"],
        structural_facts=["subscription", "next_billing_at", "price", "status", "billing_cycle"],
        value_facts=["monthly", "annual", "Prime Monthly", "Prime Annual"],  # billing cycles + plan names
    ),
    CorrectnessTask(
        id="B4", agent="billing",
        question="Apply a promotional discount code to a pending order",
        tables_needed=["billing_promotions", "orders_orders"],
        structural_facts=["promotion", "code", "value", "min_order", "valid_from", "valid_until"],
        value_facts=["percentage", "fixed_amount"],  # promotion types
    ),
    CorrectnessTask(
        id="B5", agent="billing",
        question="Redeem a gift card balance for an order payment",
        tables_needed=["billing_gift_cards"],
        structural_facts=["gift_card", "balance", "code", "status"],
        value_facts=["redeemed"],  # gift card status values (besides 'active')
    ),
    # SALES AGENT
    CorrectnessTask(
        id="S1", agent="sales",
        question="Find top-selling products with their reviews",
        tables_needed=["catalog_products", "catalog_reviews"],
        structural_facts=["product", "rating", "review", "title", "product_id"],
        value_facts=[],  # no enum values on these tables that matter
    ),
    CorrectnessTask(
        id="S2", agent="sales",
        question="Check real-time inventory across warehouses",
        tables_needed=["catalog_inventory", "warehouse_warehouses"],
        structural_facts=["inventory", "quantity", "warehouse_id", "product_id", "reserved"],
        value_facts=[],  # no enum values
    ),
    CorrectnessTask(
        id="S3", agent="sales",
        question="Get customer's cart contents and prices",
        tables_needed=["orders_carts", "orders_cart_items"],
        structural_facts=["cart", "product_id", "quantity", "unit_price", "account_id"],
        value_facts=["abandoned", "converted"],  # cart status values
    ),
    CorrectnessTask(
        id="S4", agent="sales",
        question="Track an order's shipment status and delivery",
        tables_needed=["orders_shipments", "orders_orders"],
        structural_facts=["shipment", "tracking_number", "status", "carrier", "delivered_at"],
        value_facts=["in_transit", "preparing", "shipped", "delivered"],  # shipment statuses
    ),
    CorrectnessTask(
        id="S5", agent="sales",
        question="Find what customers who bought X also bought",
        tables_needed=["orders_order_items", "orders_orders"],
        structural_facts=["order_item", "product_id", "order_id", "account_id"],
        value_facts=["returned"],  # order item status to exclude returns
    ),
    # SUPPORT AGENT
    CorrectnessTask(
        id="C1", agent="support",
        question="Look up customer's recent orders and open support tickets",
        tables_needed=["support_tickets", "orders_orders", "customers_accounts"],
        structural_facts=["ticket", "order_id", "account_id", "status", "subject"],
        value_facts=["in_progress", "resolved", "closed", "urgent"],  # ticket statuses + priority
    ),
    CorrectnessTask(
        id="C2", agent="support",
        question="Find FAQ articles related to returns",
        tables_needed=["support_faq_articles"],
        structural_facts=["faq", "article", "category", "title", "body"],
        value_facts=["return", "shipping", "billing"],  # FAQ category values
    ),
    CorrectnessTask(
        id="C3", agent="support",
        question="Verify customer identity and check payment methods",
        tables_needed=["customers_accounts", "customers_payment_methods"],
        structural_facts=["email", "phone", "name", "account_id", "type"],
        value_facts=["credit_card", "debit_card", "paypal"],  # payment method types
    ),
    # ANALYTICS AGENT
    CorrectnessTask(
        id="A1", agent="analytics",
        question="Analyze search-to-purchase conversion rates",
        tables_needed=["analytics_search_queries", "analytics_conversion_funnels"],
        structural_facts=["search", "query_text", "results_count", "clicked_product_id", "conversion"],
        value_facts=[],  # no enum values on these tables
    ),
    CorrectnessTask(
        id="A2", agent="analytics",
        question="Get A/B test results for the new checkout flow",
        tables_needed=["analytics_ab_tests"],
        structural_facts=["ab_test", "variant", "metric_value", "significance", "sample_size"],
        value_facts=["control", "treatment"],  # A/B test variant values
    ),
]


def _get_ddl(engine) -> str:
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL"
        ))
        return "\n\n".join(row[0] for row in result)


def _check_facts(content: str, facts: list[str]) -> tuple[int, int, list[str]]:
    """Check which facts appear in content. Returns (found, total, missing)."""
    content_lower = content.lower()
    found = 0
    missing = []
    for fact in facts:
        if fact.lower() in content_lower:
            found += 1
        else:
            missing.append(fact)
    return found, len(facts), missing


def _run_no_dbook(engine, task: CorrectnessTask) -> dict:
    """Run DDL-only baseline. Returns struct/value fact coverage."""
    ddl = _get_ddl(engine)
    tokens = count_tokens(ddl)
    s_found, s_total, s_missing = _check_facts(ddl, task.structural_facts)
    v_found, v_total, v_missing = _check_facts(ddl, task.value_facts)
    return {
        "tokens": tokens,
        "struct_found": s_found, "struct_total": s_total, "struct_missing": s_missing,
        "value_found": v_found, "value_total": v_total, "value_missing": v_missing,
    }


def _run_dbook_agent(dbook_path: Path, task: CorrectnessTask) -> dict:
    """Run dbook agent. Returns struct/value fact coverage."""
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

    s_found, s_total, s_missing = _check_facts(all_content, task.structural_facts)
    v_found, v_total, v_missing = _check_facts(all_content, task.value_facts)
    return {
        "tokens": agent.tokens_consumed,
        "struct_found": s_found, "struct_total": s_total, "struct_missing": s_missing,
        "value_found": v_found, "value_total": v_total, "value_missing": v_missing,
    }


def _pct(n: int, d: int) -> str:
    """Format as percentage string, handle zero denominator."""
    return f"{n / d * 100:.0f}%" if d else "n/a"


class TestAmazonCorrectness:
    """Amazon DB: correctness comparison -- DDL vs Base dbook vs LLM dbook."""

    def test_amazon_correctness_full(self, amazon_db_engine, tmp_path):  # noqa: F811
        """Run all 15 tasks measuring structural AND value fact coverage."""
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
        w = 140
        print(f"\n{'=' * w}")  # noqa: T201
        print("  AMAZON E-COMMERCE DB -- CORRECTNESS BENCHMARK (struct + value facts)")  # noqa: T201
        print("  ~34 tables, 15 business tasks, 4 agent types")  # noqa: T201
        print(f"  DDL baseline: {ddl_tokens} tokens")  # noqa: T201
        print(f"{'=' * w}")  # noqa: T201
        print(f"\n  {'Task':<5} {'Agent':<10} {'Question':<40} {'DDL struct':>11} {'DDL value':>10} {'Base struct':>12} {'Base value':>11} {'LLM struct':>11} {'LLM value':>10}")  # noqa: T201
        print(f"  {'-' * (w - 5)}")  # noqa: T201

        # Accumulators
        totals = {src: {"sf": 0, "st": 0, "vf": 0, "vt": 0, "tok": 0}
                  for src in ("ddl", "base", "llm")}
        by_agent: dict[str, dict] = {}

        for task in AMAZON_TASKS:
            ddl_r = _run_no_dbook(amazon_db_engine, task)
            base_r = _run_dbook_agent(base_dir, task)
            llm_r = _run_dbook_agent(llm_dir, task)

            q_short = task.question[:38] + ".." if len(task.question) > 40 else task.question

            def _fmt(r: dict) -> tuple[str, str]:
                return (
                    f"{r['struct_found']}/{r['struct_total']}",
                    f"{r['value_found']}/{r['value_total']}" if r['value_total'] else "-",
                )

            ds, dv = _fmt(ddl_r)
            bs, bv = _fmt(base_r)
            ls, lv = _fmt(llm_r)

            # Marker for dbook advantage
            marker = ""
            if base_r["value_found"] > ddl_r["value_found"]:
                marker = "  << dbook wins on values"

            print(f"  {task.id:<5} {task.agent:<10} {q_short:<40} {ds:>11} {dv:>10} {bs:>12} {bv:>11} {ls:>11} {lv:>10}{marker}")  # noqa: T201

            if base_r["value_missing"]:
                print(f"         Base value missing: {', '.join(base_r['value_missing'])}")  # noqa: T201
            if ddl_r["struct_missing"]:
                print(f"         DDL struct missing: {', '.join(ddl_r['struct_missing'])}")  # noqa: T201

            # Accumulate
            for src, r in [("ddl", ddl_r), ("base", base_r), ("llm", llm_r)]:
                totals[src]["sf"] += r["struct_found"]
                totals[src]["st"] += r["struct_total"]
                totals[src]["vf"] += r["value_found"]
                totals[src]["vt"] += r["value_total"]
                totals[src]["tok"] += r["tokens"]

            # By agent type
            if task.agent not in by_agent:
                by_agent[task.agent] = {
                    src: {"sf": 0, "st": 0, "vf": 0, "vt": 0}
                    for src in ("ddl", "base", "llm")
                }
                by_agent[task.agent]["count"] = 0
            for src, r in [("ddl", ddl_r), ("base", base_r), ("llm", llm_r)]:
                by_agent[task.agent][src]["sf"] += r["struct_found"]
                by_agent[task.agent][src]["st"] += r["struct_total"]
                by_agent[task.agent][src]["vf"] += r["value_found"]
                by_agent[task.agent][src]["vt"] += r["value_total"]
            by_agent[task.agent]["count"] += 1

        n = len(AMAZON_TASKS)
        print(f"  {'-' * (w - 5)}")  # noqa: T201

        # Summary
        ddl_s_pct = totals["ddl"]["sf"] / totals["ddl"]["st"] * 100
        base_s_pct = totals["base"]["sf"] / totals["base"]["st"] * 100
        llm_s_pct = totals["llm"]["sf"] / totals["llm"]["st"] * 100

        ddl_v_pct = totals["ddl"]["vf"] / totals["ddl"]["vt"] * 100 if totals["ddl"]["vt"] else 0
        base_v_pct = totals["base"]["vf"] / totals["base"]["vt"] * 100 if totals["base"]["vt"] else 0
        llm_v_pct = totals["llm"]["vf"] / totals["llm"]["vt"] * 100 if totals["llm"]["vt"] else 0

        ddl_all = totals["ddl"]["sf"] + totals["ddl"]["vf"]
        base_all = totals["base"]["sf"] + totals["base"]["vf"]
        llm_all = totals["llm"]["sf"] + totals["llm"]["vf"]
        total_all = totals["ddl"]["st"] + totals["ddl"]["vt"]

        ddl_all_pct = ddl_all / total_all * 100 if total_all else 0
        base_all_pct = base_all / total_all * 100 if total_all else 0
        llm_all_pct = llm_all / total_all * 100 if total_all else 0

        ddl_avg_tok = totals["ddl"]["tok"] / n
        base_avg_tok = totals["base"]["tok"] / n
        llm_avg_tok = totals["llm"]["tok"] / n

        print("\n  SUMMARY:")  # noqa: T201
        print(f"    Structural facts:  DDL={totals['ddl']['sf']}/{totals['ddl']['st']} ({ddl_s_pct:.0f}%)  dbook={totals['base']['sf']}/{totals['base']['st']} ({base_s_pct:.0f}%)  LLM={totals['llm']['sf']}/{totals['llm']['st']} ({llm_s_pct:.0f}%)")  # noqa: T201
        print(f"    Value-level facts: DDL={totals['ddl']['vf']}/{totals['ddl']['vt']} ({ddl_v_pct:.0f}%)  dbook={totals['base']['vf']}/{totals['base']['vt']} ({base_v_pct:.0f}%)  LLM={totals['llm']['vf']}/{totals['llm']['vt']} ({llm_v_pct:.0f}%)")  # noqa: T201
        print(f"    Overall:           DDL={ddl_all}/{total_all} ({ddl_all_pct:.0f}%)  dbook={base_all}/{total_all} ({base_all_pct:.0f}%)  LLM={llm_all}/{total_all} ({llm_all_pct:.0f}%)")  # noqa: T201

        if base_all_pct > ddl_all_pct:
            print(f"    dbook advantage: +{base_all_pct - ddl_all_pct:.0f}% overall (value facts are the difference)")  # noqa: T201

        print(f"\n    Avg tokens/task:   DDL={ddl_avg_tok:.0f}  dbook={base_avg_tok:.0f}  LLM={llm_avg_tok:.0f}")  # noqa: T201

        # By agent breakdown
        print("\n  BY AGENT TYPE:")  # noqa: T201
        print(f"    {'Agent':<12} {'Tasks':>5}  {'DDL struct':>11} {'DDL value':>10}  {'dbook struct':>13} {'dbook value':>12}  {'LLM struct':>11} {'LLM value':>10}")  # noqa: T201
        for agent_name, stats in sorted(by_agent.items()):
            if agent_name == "count":
                continue
            def _a_pct(s: dict, k: str) -> str:  # noqa: E306
                return _pct(s[k[:1] + "f"], s[k[:1] + "t"])
            print(  # noqa: T201
                f"    {agent_name:<12} {stats['count']:>5}"
                f"  {_pct(stats['ddl']['sf'], stats['ddl']['st']):>11} {_pct(stats['ddl']['vf'], stats['ddl']['vt']):>10}"
                f"  {_pct(stats['base']['sf'], stats['base']['st']):>13} {_pct(stats['base']['vf'], stats['base']['vt']):>12}"
                f"  {_pct(stats['llm']['sf'], stats['llm']['st']):>11} {_pct(stats['llm']['vf'], stats['llm']['vt']):>10}"
            )

        print()  # noqa: T201

        # Assertions
        # Structural: dbook should have at least what DDL has
        assert totals["base"]["sf"] >= totals["ddl"]["sf"], (
            f"Base dbook struct ({totals['base']['sf']}) should have >= DDL struct ({totals['ddl']['sf']})"
        )
        # Value: dbook should beat DDL on value facts (the whole point!)
        assert totals["base"]["vf"] > totals["ddl"]["vf"], (
            f"Base dbook values ({totals['base']['vf']}) MUST beat DDL values ({totals['ddl']['vf']}) "
            f"— this is the core dbook advantage"
        )
        # LLM should be >= base
        assert totals["llm"]["sf"] + totals["llm"]["vf"] >= totals["base"]["sf"] + totals["base"]["vf"], (
            f"LLM ({totals['llm']['sf'] + totals['llm']['vf']}) should have >= Base facts "
            f"({totals['base']['sf'] + totals['base']['vf']})"
        )

    def test_amazon_correctness_scaled(self, scaled_db_engine, tmp_path):  # noqa: F811
        """Run applicable tasks on the 50-table scaled DB."""
        # Compile dbook for scaled DB
        catalog = SQLAlchemyCatalog(scaled_db_engine)
        book = catalog.introspect_all()
        base_dir = tmp_path / "scaled_base"
        compile_book(book, base_dir)

        ddl = _get_ddl(scaled_db_engine)
        ddl_tokens = count_tokens(ddl)

        # Get available tables
        schemas_dir = base_dir / "schemas"
        available_tables: set[str] = set()
        for schema_dir in schemas_dir.iterdir():
            if schema_dir.is_dir():
                for f in schema_dir.iterdir():
                    if f.suffix == ".md":
                        available_tables.add(f.stem)

        # Find tasks where ALL needed tables exist in scaled DB
        applicable: list[CorrectnessTask] = []
        for task in AMAZON_TASKS:
            if all(t in available_tables for t in task.tables_needed):
                applicable.append(task)

        print(f"\n  SCALED DB (50 tables) -- {len(applicable)}/{len(AMAZON_TASKS)} tasks applicable")  # noqa: T201
        print(f"  DDL baseline: {ddl_tokens} tokens")  # noqa: T201

        if not applicable:
            print("  No applicable tasks for scaled DB (different table names)")  # noqa: T201
            return

        # Run applicable tasks
        struct_found_ddl = 0
        struct_found_dbook = 0
        value_found_ddl = 0
        value_found_dbook = 0
        struct_total = 0
        value_total = 0

        for task in applicable:
            ddl_r = _run_no_dbook(scaled_db_engine, task)
            base_r = _run_dbook_agent(base_dir, task)

            struct_found_ddl += ddl_r["struct_found"]
            struct_found_dbook += base_r["struct_found"]
            value_found_ddl += ddl_r["value_found"]
            value_found_dbook += base_r["value_found"]
            struct_total += ddl_r["struct_total"]
            value_total += ddl_r["value_total"]

            print(  # noqa: T201
                f"    {task.id}: struct DDL={ddl_r['struct_found']}/{ddl_r['struct_total']} "
                f"dbook={base_r['struct_found']}/{base_r['struct_total']}  "
                f"value DDL={ddl_r['value_found']}/{ddl_r['value_total']} "
                f"dbook={base_r['value_found']}/{base_r['value_total']}"
            )

        if struct_total + value_total > 0:
            print(f"\n  Scaled totals: struct DDL={_pct(struct_found_ddl, struct_total)} dbook={_pct(struct_found_dbook, struct_total)}")  # noqa: T201
            print(f"                 value  DDL={_pct(value_found_ddl, value_total)} dbook={_pct(value_found_dbook, value_total)}")  # noqa: T201
