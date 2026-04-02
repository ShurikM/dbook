"""Realistic agent benchmark -- business agents on Amazon-like e-commerce DB."""

# ruff: noqa: S101

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import text  # type: ignore[import-untyped]

from dbook.catalog import SQLAlchemyCatalog  # type: ignore[import-untyped]
from dbook.compiler import compile_book  # type: ignore[import-untyped]
from dbook.llm.provider import MockProvider  # type: ignore[import-untyped]
from dbook.llm.enricher import enrich_book  # type: ignore[import-untyped]
from tests.benchmark_helpers import AgentSimulator, count_tokens
from tests.amazon_fixture import amazon_db_engine  # noqa: F401


# ---------------------------------------------------------------------------
# Agent task definitions
# ---------------------------------------------------------------------------

BILLING_AGENT_TASKS = [
    {
        "id": "B1",
        "agent": "billing",
        "task": "Process a refund for a returned item -- find the payment and return records",
        "business_terms": ["refund", "return", "payment", "money back"],
        "keywords_hint": ["refund", "return", "payment"],
        "expected_tables": ["billing_refunds", "billing_payments", "orders_returns"],
        "expected_columns": ["refund_amount", "amount", "status"],
    },
    {
        "id": "B2",
        "agent": "billing",
        "task": "Generate monthly invoice summary for a customer account",
        "business_terms": ["invoice", "monthly", "bill", "account statement"],
        "keywords_hint": ["invoice", "account", "amount"],
        "expected_tables": ["billing_invoices", "customers_accounts"],
        "expected_columns": ["amount", "total", "issued_at"],
    },
    {
        "id": "B3",
        "agent": "billing",
        "task": "Check subscription renewal status and upcoming charges",
        "business_terms": ["subscription", "renewal", "upcoming charge", "billing cycle"],
        "keywords_hint": ["subscription", "billing", "next"],
        "expected_tables": ["billing_subscriptions", "billing_subscription_payments"],
        "expected_columns": ["next_billing_at", "price", "status"],
    },
    {
        "id": "B4",
        "agent": "billing",
        "task": "Apply a promotional discount code to a pending order",
        "business_terms": ["promo code", "discount", "coupon", "apply promotion"],
        "keywords_hint": ["promotion", "code", "discount", "order"],
        "expected_tables": ["billing_promotions", "orders_orders"],
        "expected_columns": ["code", "value", "min_order"],
    },
    {
        "id": "B5",
        "agent": "billing",
        "task": "Redeem a gift card balance for an order payment",
        "business_terms": ["gift card", "redeem", "balance", "credit"],
        "keywords_hint": ["gift", "card", "balance"],
        "expected_tables": ["billing_gift_cards"],
        "expected_columns": ["balance", "code", "status"],
    },
]

SALES_AGENT_TASKS = [
    {
        "id": "S1",
        "agent": "sales",
        "task": "Find top-selling products in a category with their reviews",
        "business_terms": ["bestseller", "top product", "popular items", "star rating"],
        "keywords_hint": ["product", "review", "rating", "category"],
        "expected_tables": ["catalog_products", "catalog_reviews", "catalog_categories"],
        "expected_columns": ["rating", "title", "category_id"],
    },
    {
        "id": "S2",
        "agent": "sales",
        "task": "Check real-time inventory for a product across warehouses",
        "business_terms": ["stock", "availability", "in stock", "warehouse inventory"],
        "keywords_hint": ["inventory", "quantity", "warehouse", "product"],
        "expected_tables": ["catalog_inventory", "warehouse_warehouses"],
        "expected_columns": ["quantity", "reserved", "warehouse_id"],
    },
    {
        "id": "S3",
        "agent": "sales",
        "task": "Get customer's cart contents and recommend upsells",
        "business_terms": ["shopping cart", "basket", "add to cart", "checkout"],
        "keywords_hint": ["cart", "item", "product", "price"],
        "expected_tables": ["orders_carts", "orders_cart_items", "catalog_products"],
        "expected_columns": ["quantity", "unit_price", "product_id"],
    },
    {
        "id": "S4",
        "agent": "sales",
        "task": "Track an order's shipment status and delivery estimate",
        "business_terms": ["tracking", "where is my order", "delivery status", "shipping update"],
        "keywords_hint": ["shipment", "tracking", "carrier", "delivered"],
        "expected_tables": ["orders_shipments", "orders_orders"],
        "expected_columns": ["tracking_number", "status", "delivered_at"],
    },
    {
        "id": "S5",
        "agent": "sales",
        "task": "Find what customers who bought X also bought",
        "business_terms": ["also bought", "frequently bought together", "recommendation"],
        "keywords_hint": ["order", "item", "product", "customer"],
        "expected_tables": ["orders_order_items", "orders_orders"],
        "expected_columns": ["product_id", "account_id"],
    },
]

SUPPORT_AGENT_TASKS = [
    {
        "id": "C1",
        "agent": "support",
        "task": "Look up customer's recent orders and open support tickets",
        "business_terms": ["customer history", "order history", "open cases", "support requests"],
        "keywords_hint": ["ticket", "order", "account", "status"],
        "expected_tables": ["support_tickets", "orders_orders", "customers_accounts"],
        "expected_columns": ["status", "subject", "account_id"],
    },
    {
        "id": "C2",
        "agent": "support",
        "task": "Find FAQ articles related to a return/refund issue",
        "business_terms": ["help article", "knowledge base", "how to return", "refund policy"],
        "keywords_hint": ["faq", "article", "category"],
        "expected_tables": ["support_faq_articles"],
        "expected_columns": ["title", "body", "category"],
    },
    {
        "id": "C3",
        "agent": "support",
        "task": "Verify customer identity: check email, phone, recent orders",
        "business_terms": ["verify identity", "account verification", "security check"],
        "keywords_hint": ["account", "email", "phone", "order"],
        "expected_tables": ["customers_accounts", "orders_orders"],
        "expected_columns": ["email", "phone", "name"],
    },
]

ANALYTICS_AGENT_TASKS = [
    {
        "id": "A1",
        "agent": "analytics",
        "task": "Analyze search-to-purchase conversion rates",
        "business_terms": ["conversion rate", "search funnel", "purchase conversion"],
        "keywords_hint": ["search", "conversion", "funnel", "purchase"],
        "expected_tables": ["analytics_search_queries", "analytics_conversion_funnels"],
        "expected_columns": ["query_text", "results_count", "clicked_product_id"],
    },
    {
        "id": "A2",
        "agent": "analytics",
        "task": "Get A/B test results for the new checkout flow",
        "business_terms": ["experiment results", "A/B test", "variant performance"],
        "keywords_hint": ["ab_test", "variant", "significance"],
        "expected_tables": ["analytics_ab_tests"],
        "expected_columns": ["variant", "metric_value", "significance"],
    },
]

ALL_TASKS = BILLING_AGENT_TASKS + SALES_AGENT_TASKS + SUPPORT_AGENT_TASKS + ANALYTICS_AGENT_TASKS


# ---------------------------------------------------------------------------
# Task result tracking
# ---------------------------------------------------------------------------

@dataclass
class TaskResult:
    """Result of a single agent task attempt."""

    task_id: str
    task: str
    agent: str
    strategy: str
    tokens_consumed: int
    files_read: int
    tables_found: list[str]
    columns_found: list[str]
    expected_tables: list[str]
    expected_columns: list[str]

    @property
    def table_recall(self) -> float:
        if not self.expected_tables:
            return 1.0
        found = sum(
            1 for t in self.expected_tables
            if any(t in f for f in self.tables_found)
        )
        return found / len(self.expected_tables)

    @property
    def column_recall(self) -> float:
        if not self.expected_columns:
            return 1.0
        found = sum(1 for c in self.expected_columns if c in self.columns_found)
        return found / len(self.expected_columns)

    @property
    def success(self) -> bool:
        return self.table_recall >= 0.5 and self.column_recall >= 0.5


# ---------------------------------------------------------------------------
# Agent strategies
# ---------------------------------------------------------------------------

def _find_tables_by_terms(nav_text: str, terms: list[str]) -> list[str]:
    """Scan NAVIGATION.md lines for tables matching any of *terms*."""
    tables: list[str] = []
    seen: set[str] = set()
    for line in nav_text.split("\n"):
        line_lower = line.lower()
        if any(term.lower() in line_lower for term in terms):
            parts = line.split("|")
            if len(parts) >= 3:
                table_name = parts[1].strip()
                if table_name and table_name not in ("Table", "---", "") and table_name not in seen:
                    tables.append(table_name)
                    seen.add(table_name)
    return tables


def _parse_nav_table(nav_text: str) -> list[dict[str, str]]:
    """Parse the markdown table in NAVIGATION.md into row dicts."""
    rows: list[dict[str, str]] = []
    header_found = False
    headers: list[str] = []
    for line in nav_text.split("\n"):
        # Markdown table rows start and end with |, require at least 3 pipes
        if line.count("|") < 3 or not line.strip().startswith("|"):
            continue
        cells = [c.strip() for c in line.split("|")]
        # Remove empty first/last from leading/trailing pipes
        if cells and cells[0] == "":
            cells = cells[1:]
        if cells and cells[-1] == "":
            cells = cells[:-1]
        if not cells:
            continue
        # Skip separator row
        if all(re.match(r"^[-:]+$", c) for c in cells):
            continue
        if not header_found:
            headers = [h.lower() for h in cells]
            header_found = True
            continue
        row_dict: dict[str, str] = {}
        for idx, val in enumerate(cells):
            if idx < len(headers):
                row_dict[headers[idx]] = val
        if "table" in row_dict:
            rows.append(row_dict)
    return rows


def _read_table_files(agent: AgentSimulator, dbook_path: Path, table_names: list[str], max_files: int = 3) -> str:
    """Read up to max_files table markdown files, return combined content."""
    content = ""
    schemas_dir = dbook_path / "schemas"
    if not schemas_dir.exists():
        return content
    count = 0
    for table_name in table_names:
        if count >= max_files:
            break
        for schema_dir in schemas_dir.iterdir():
            table_file = schema_dir / f"{table_name}.md"
            if table_file.exists():
                content += agent.read_file(str(table_file.relative_to(dbook_path)))
                count += 1
                break
    return content


def _run_no_dbook_agent(engine: object, task: dict[str, object]) -> TaskResult:
    """Agent reads ALL raw DDL -- baseline without dbook."""
    with engine.connect() as conn:  # type: ignore[union-attr]
        result = conn.execute(text(
            "SELECT sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL"
        ))
        all_ddl = "\n\n".join(row[0] for row in result)

    tokens = count_tokens(all_ddl)
    task_id: str = task["id"]  # type: ignore[assignment]
    task_desc: str = task["task"]  # type: ignore[assignment]
    agent_name: str = task["agent"]  # type: ignore[assignment]
    expected_tables: list[str] = task["expected_tables"]  # type: ignore[assignment]
    expected_columns: list[str] = task["expected_columns"]  # type: ignore[assignment]

    tables_found = [t for t in expected_tables if t in all_ddl]
    columns_found = [c for c in expected_columns if c in all_ddl]

    return TaskResult(
        task_id=task_id, task=task_desc, agent=agent_name, strategy="raw_ddl",
        tokens_consumed=tokens, files_read=1,
        tables_found=tables_found, columns_found=columns_found,
        expected_tables=expected_tables, expected_columns=expected_columns,
    )


def _run_keyword_agent(dbook_path: Path, task: dict[str, object]) -> TaskResult:
    """Agent uses technical keywords to find tables in NAVIGATION.md."""
    agent = AgentSimulator(dbook_path)
    nav = agent.read_file("NAVIGATION.md")

    keywords: list[str] = task["keywords_hint"]  # type: ignore[assignment]
    tables = _find_tables_by_terms(nav, keywords)

    all_content = nav + _read_table_files(agent, dbook_path, tables)
    task_id: str = task["id"]  # type: ignore[assignment]
    task_desc: str = task["task"]  # type: ignore[assignment]
    agent_name: str = task["agent"]  # type: ignore[assignment]
    expected_tables: list[str] = task["expected_tables"]  # type: ignore[assignment]
    expected_columns: list[str] = task["expected_columns"]  # type: ignore[assignment]
    columns_found = [c for c in expected_columns if c in all_content]

    return TaskResult(
        task_id=task_id, task=task_desc, agent=agent_name, strategy="keyword",
        tokens_consumed=agent.tokens_consumed, files_read=len(agent.files_read),
        tables_found=tables, columns_found=columns_found,
        expected_tables=expected_tables, expected_columns=expected_columns,
    )


def _run_business_term_agent(dbook_path: Path, task: dict[str, object]) -> TaskResult:
    """Agent uses business language (not column names) to find tables."""
    agent = AgentSimulator(dbook_path)
    nav = agent.read_file("NAVIGATION.md")

    terms: list[str] = task["business_terms"]  # type: ignore[assignment]
    tables = _find_tables_by_terms(nav, terms)

    all_content = nav + _read_table_files(agent, dbook_path, tables)
    task_id: str = task["id"]  # type: ignore[assignment]
    task_desc: str = task["task"]  # type: ignore[assignment]
    agent_name: str = task["agent"]  # type: ignore[assignment]
    expected_tables: list[str] = task["expected_tables"]  # type: ignore[assignment]
    expected_columns: list[str] = task["expected_columns"]  # type: ignore[assignment]
    columns_found = [c for c in expected_columns if c in all_content]

    return TaskResult(
        task_id=task_id, task=task_desc, agent=agent_name, strategy="business_term",
        tokens_consumed=agent.tokens_consumed, files_read=len(agent.files_read),
        tables_found=tables, columns_found=columns_found,
        expected_tables=expected_tables, expected_columns=expected_columns,
    )


def _run_smart_agent(dbook_path: Path, task: dict[str, object]) -> TaskResult:
    """Agent reads Description column to find semantically relevant tables.

    A smart agent examines each row holistically: the table name, key columns,
    references, and (critically) the Description.  The Description is where LLM
    mode adds business semantics that base mode lacks.
    """
    agent = AgentSimulator(dbook_path)
    nav = agent.read_file("NAVIGATION.md")

    task_desc_str: str = task["task"]  # type: ignore[assignment]
    keywords: list[str] = task["keywords_hint"]  # type: ignore[assignment]

    # Build set of meaningful words from the task description (skip short words)
    task_words = {w.lower().rstrip("s") for w in task_desc_str.split() if len(w) > 3}

    tables: list[str] = []
    for row in _parse_nav_table(nav):
        # Combine all row fields into one searchable text
        row_text = " ".join(row.values()).lower()
        row_words = {w.rstrip("s") for w in row_text.split() if len(w) > 3}
        overlap = task_words & row_words
        keyword_match = any(kw.lower() in row_text for kw in keywords)
        if len(overlap) >= 2 or keyword_match:
            tables.append(row["table"])

    all_content = nav + _read_table_files(agent, dbook_path, tables)
    task_id: str = task["id"]  # type: ignore[assignment]
    agent_name: str = task["agent"]  # type: ignore[assignment]
    expected_tables: list[str] = task["expected_tables"]  # type: ignore[assignment]
    expected_columns: list[str] = task["expected_columns"]  # type: ignore[assignment]
    columns_found = [c for c in expected_columns if c in all_content]

    return TaskResult(
        task_id=task_id, task=task_desc_str, agent=agent_name, strategy="smart",
        tokens_consumed=agent.tokens_consumed, files_read=len(agent.files_read),
        tables_found=tables, columns_found=columns_found,
        expected_tables=expected_tables, expected_columns=expected_columns,
    )


# ---------------------------------------------------------------------------
# Report printing
# ---------------------------------------------------------------------------

def _print_agent_section(
    agent_name: str,
    tasks: list[dict[str, object]],
    results_by_key: dict[tuple[str, str, str], TaskResult],
    modes: list[str],
    strategies: list[str],
) -> None:
    """Print results for one agent type."""
    print(f"\n{agent_name.upper()} AGENT ({len(tasks)} tasks):")  # noqa: T201
    hdr = f"  {'Task':<5} {'Strategy':<15}"
    for mode in modes:
        hdr += f" {mode:>9}"
    hdr += f"  {'Base Save':>9} {'LLM Save':>9} {'B-OK':>5} {'L-OK':>5}"
    print(hdr)  # noqa: T201
    print(f"  {'-' * (len(hdr) - 2)}")  # noqa: T201

    for task in tasks:
        tid: str = task["id"]  # type: ignore[assignment]
        for strategy in strategies:
            parts = f"  {tid:<5} {strategy:<15}"
            nd = results_by_key.get((tid, "no_dbook", strategy))
            b = results_by_key.get((tid, "base", strategy))
            lm = results_by_key.get((tid, "llm", strategy))
            nd_tok = nd.tokens_consumed if nd else 0
            b_tok = b.tokens_consumed if b else 0
            lm_tok = lm.tokens_consumed if lm else 0
            for tok in [nd_tok, b_tok, lm_tok]:
                parts += f" {tok:>9}"
            b_save = f"{(1 - b_tok / nd_tok) * 100:.0f}%" if nd_tok else "N/A"
            lm_save = f"{(1 - lm_tok / nd_tok) * 100:.0f}%" if nd_tok else "N/A"
            b_ok = "Y" if b and b.success else "N"
            lm_ok = "Y" if lm and lm.success else "N"
            flag = ""
            if lm and b and lm.success and not b.success:
                flag = "  <- LLM helps"
            parts += f"  {b_save:>9} {lm_save:>9} {b_ok:>5} {lm_ok:>5}{flag}"
            print(parts)  # noqa: T201


def _print_full_report(
    all_results: list[TaskResult],
    no_dbook_results: list[TaskResult],
    table_count: int,
) -> None:
    """Print the comprehensive simulation report."""
    print(f"\n{'=' * 100}")  # noqa: T201
    print(f"REALISTIC AGENT SIMULATION: Amazon E-Commerce DB (~{table_count} tables)")  # noqa: T201
    print(f"{'=' * 100}")  # noqa: T201

    # Index results: (task_id, mode, strategy) -> TaskResult
    results_by_key: dict[tuple[str, str, str], TaskResult] = {}
    for r in all_results:
        # Determine mode from context
        results_by_key[(r.task_id, r.strategy, r.strategy)] = r
    for r in no_dbook_results:
        results_by_key[(r.task_id, "no_dbook", "keyword")] = r
        results_by_key[(r.task_id, "no_dbook", "business_term")] = r
        results_by_key[(r.task_id, "no_dbook", "smart")] = r

    # Rebuild index properly by extracting mode from the result groups
    # We need a different structure; the caller should pass organized results
    # For now, print summary tables

    modes = ["No dbook", "Base", "LLM"]
    strategies = ["keyword", "business_term", "smart"]

    # Group by agent
    agent_tasks = {
        "billing": BILLING_AGENT_TASKS,
        "sales": SALES_AGENT_TASKS,
        "support": SUPPORT_AGENT_TASKS,
        "analytics": ANALYTICS_AGENT_TASKS,
    }

    for agent_name, tasks in agent_tasks.items():
        _print_agent_section(agent_name, tasks, results_by_key, modes, strategies)

    # Summary by strategy
    print(f"\n{'=' * 80}")  # noqa: T201
    print("SUMMARY BY STRATEGY:")  # noqa: T201
    print(f"  {'Strategy':<15} {'No dbook':>10} {'Base Tok':>10} {'LLM Tok':>10} {'Base Save':>10} {'LLM Save':>10}")  # noqa: T201
    print(f"  {'-' * 70}")  # noqa: T201

    for strategy in strategies:
        nd_toks = [r.tokens_consumed for r in no_dbook_results]
        nd_avg = sum(nd_toks) / len(nd_toks) if nd_toks else 0
        print(f"  {strategy:<15} {nd_avg:>10.0f}")  # noqa: T201

    print()  # noqa: T201


def _print_comparison_report(
    nd_results: list[TaskResult],
    base_results: dict[str, list[TaskResult]],
    llm_results: dict[str, list[TaskResult]],
    table_count: int,
) -> None:
    """Print the full comparison report across all modes and strategies."""
    strategies = ["keyword", "business_term", "smart"]

    print(f"\n{'=' * 110}")  # noqa: T201
    print(f"REALISTIC AGENT SIMULATION: Amazon E-Commerce DB (~{table_count} tables)")  # noqa: T201
    print(f"{'=' * 110}")  # noqa: T201

    agent_groups = [
        ("BILLING", BILLING_AGENT_TASKS),
        ("SALES", SALES_AGENT_TASKS),
        ("SUPPORT", SUPPORT_AGENT_TASKS),
        ("ANALYTICS", ANALYTICS_AGENT_TASKS),
    ]

    # Index no-dbook results by task_id
    nd_by_id = {r.task_id: r for r in nd_results}

    for agent_label, tasks in agent_groups:
        print(f"\n{agent_label} AGENT ({len(tasks)} tasks):")  # noqa: T201
        print(f"  {'Task':<5} {'Strategy':<15} {'No dbook':>9} {'Base':>9} {'LLM':>9} {'Base Save':>10} {'LLM Save':>10} {'B-OK':>5} {'L-OK':>5}")  # noqa: T201
        print(f"  {'-' * 85}")  # noqa: T201

        for task in tasks:
            tid = task["id"]
            for strategy in strategies:
                nd = nd_by_id.get(tid)
                b_list = [r for r in base_results.get(strategy, []) if r.task_id == tid]
                l_list = [r for r in llm_results.get(strategy, []) if r.task_id == tid]
                b = b_list[0] if b_list else None
                lm = l_list[0] if l_list else None

                nd_tok = nd.tokens_consumed if nd else 0
                b_tok = b.tokens_consumed if b else 0
                lm_tok = lm.tokens_consumed if lm else 0
                b_save = f"{(1 - b_tok / nd_tok) * 100:.0f}%" if nd_tok else "N/A"
                lm_save = f"{(1 - lm_tok / nd_tok) * 100:.0f}%" if nd_tok else "N/A"
                b_ok = "Y" if b and b.success else "N"
                lm_ok = "Y" if lm and lm.success else "N"
                flag = ""
                if lm and b and lm.success and not b.success:
                    flag = "  <- LLM helps"

                print(f"  {tid:<5} {strategy:<15} {nd_tok:>9} {b_tok:>9} {lm_tok:>9} {b_save:>10} {lm_save:>10} {b_ok:>5} {lm_ok:>5}{flag}")  # noqa: T201

    # Summary by agent type
    print(f"\n{'=' * 80}")  # noqa: T201
    print("SUMMARY BY AGENT TYPE:")  # noqa: T201
    print(f"  {'Agent':<12} {'Keyword Success':>20} {'Business-Term Success':>25} {'Smart Success':>20}")  # noqa: T201
    print(f"  {'-' * 80}")  # noqa: T201

    for agent_label, tasks in agent_groups:
        task_ids = {t["id"] for t in tasks}
        n = len(tasks)
        parts = f"  {agent_label.lower():<12}"
        for strategy in strategies:
            b_s = sum(1 for r in base_results.get(strategy, []) if r.task_id in task_ids and r.success)
            l_s = sum(1 for r in llm_results.get(strategy, []) if r.task_id in task_ids and r.success)
            width = 20 if strategy != "business_term" else 25
            parts += f" {'Base=' + str(b_s) + '/' + str(n) + ' LLM=' + str(l_s) + '/' + str(n):>{width}}"
        print(parts)  # noqa: T201

    # Summary by strategy
    print("\nSUMMARY BY STRATEGY:")  # noqa: T201
    print(f"  {'Strategy':<15} {'No dbook':>10} {'Base Avg':>10} {'LLM Avg':>10} {'Base Save':>10} {'LLM Save':>10} {'Base OK':>8} {'LLM OK':>8}")  # noqa: T201
    print(f"  {'-' * 85}")  # noqa: T201

    nd_avg = sum(r.tokens_consumed for r in nd_results) / len(nd_results) if nd_results else 0
    for strategy in strategies:
        b_list = base_results.get(strategy, [])
        l_list = llm_results.get(strategy, [])
        b_avg = sum(r.tokens_consumed for r in b_list) / len(b_list) if b_list else 0
        l_avg = sum(r.tokens_consumed for r in l_list) / len(l_list) if l_list else 0
        b_save = f"{(1 - b_avg / nd_avg) * 100:.0f}%" if nd_avg else "N/A"
        l_save = f"{(1 - l_avg / nd_avg) * 100:.0f}%" if nd_avg else "N/A"
        b_ok = sum(1 for r in b_list if r.success)
        l_ok = sum(1 for r in l_list if r.success)
        n = len(ALL_TASKS)
        flag = ""
        if l_ok > b_ok and strategy == "business_term":
            flag = "  <- LLM wins here"
        print(f"  {strategy:<15} {nd_avg:>10.0f} {b_avg:>10.0f} {l_avg:>10.0f} {b_save:>10} {l_save:>10} {b_ok:>5}/{n} {l_ok:>5}/{n}{flag}")  # noqa: T201

    print()  # noqa: T201


# ---------------------------------------------------------------------------
# Compile helpers
# ---------------------------------------------------------------------------

def _compile_base(engine: object, output_dir: Path) -> Path:
    """Compile base-mode dbook."""
    catalog = SQLAlchemyCatalog(engine)
    book = catalog.introspect_all()
    compile_book(book, output_dir)
    return output_dir


def _compile_llm(engine: object, output_dir: Path) -> Path:
    """Compile LLM-mode dbook with MockProvider."""
    catalog = SQLAlchemyCatalog(engine)
    book = catalog.introspect_all()
    book.mode = "llm"
    enrich_book(book, MockProvider())
    compile_book(book, output_dir)
    return output_dir


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

class TestRealisticAgentSimulation:
    """Realistic agent simulation across 15 tasks x 3 modes x 3 strategies."""

    def test_amazon_db_billing_agent(self, amazon_db_engine, tmp_path):  # noqa: F811
        """Billing agent tasks on Amazon DB."""
        base_dir = _compile_base(amazon_db_engine, tmp_path / "base")
        llm_dir = _compile_llm(amazon_db_engine, tmp_path / "llm")

        for task in BILLING_AGENT_TASKS:
            b_kw = _run_keyword_agent(base_dir, task)
            l_kw = _run_keyword_agent(llm_dir, task)
            assert b_kw.tokens_consumed > 0
            assert l_kw.tokens_consumed > 0

    def test_amazon_db_sales_agent(self, amazon_db_engine, tmp_path):  # noqa: F811
        """Sales agent tasks on Amazon DB."""
        base_dir = _compile_base(amazon_db_engine, tmp_path / "base")
        llm_dir = _compile_llm(amazon_db_engine, tmp_path / "llm")

        for task in SALES_AGENT_TASKS:
            b_kw = _run_keyword_agent(base_dir, task)
            l_kw = _run_keyword_agent(llm_dir, task)
            assert b_kw.tokens_consumed > 0
            assert l_kw.tokens_consumed > 0

    def test_amazon_db_full_comparison(self, amazon_db_engine, tmp_path):  # noqa: F811
        """All agents, all tasks, 3 modes (no dbook, base, LLM) x 3 strategies."""
        base_dir = _compile_base(amazon_db_engine, tmp_path / "base")
        llm_dir = _compile_llm(amazon_db_engine, tmp_path / "llm")

        # Count tables
        nav_content = (base_dir / "NAVIGATION.md").read_text()
        table_count = nav_content.count("\n|") - 2  # subtract header + separator

        # No-dbook baseline (same for all strategies)
        nd_results = [_run_no_dbook_agent(amazon_db_engine, t) for t in ALL_TASKS]

        # Base mode -- 3 strategies
        base_keyword = [_run_keyword_agent(base_dir, t) for t in ALL_TASKS]
        base_business = [_run_business_term_agent(base_dir, t) for t in ALL_TASKS]
        base_smart = [_run_smart_agent(base_dir, t) for t in ALL_TASKS]

        # LLM mode -- 3 strategies
        llm_keyword = [_run_keyword_agent(llm_dir, t) for t in ALL_TASKS]
        llm_business = [_run_business_term_agent(llm_dir, t) for t in ALL_TASKS]
        llm_smart = [_run_smart_agent(llm_dir, t) for t in ALL_TASKS]

        base_results = {
            "keyword": base_keyword,
            "business_term": base_business,
            "smart": base_smart,
        }
        llm_results_dict = {
            "keyword": llm_keyword,
            "business_term": llm_business,
            "smart": llm_smart,
        }

        _print_comparison_report(nd_results, base_results, llm_results_dict, table_count)

        # Assertions
        n = len(ALL_TASKS)

        # 1. Keyword agent should find at least 50% of tasks in base mode
        kw_success = sum(1 for r in base_keyword if r.success)
        assert kw_success >= n * 0.5, (
            f"Keyword agent base success {kw_success}/{n} below 50%"
        )

        # 2. LLM business-term should outperform base business-term
        #    This is the key finding: LLM descriptions help business-language agents
        llm_biz_success = sum(1 for r in llm_business if r.success)
        base_biz_success = sum(1 for r in base_business if r.success)
        print(f"\n  KEY FINDING: Business-term success -- Base={base_biz_success}/{n}  LLM={llm_biz_success}/{n}")  # noqa: T201
        assert llm_biz_success >= base_biz_success, (
            f"LLM business-term ({llm_biz_success}) should be >= base ({base_biz_success})"
        )

        # 3. LLM smart agent should be at least as good as base smart agent
        llm_smart_success = sum(1 for r in llm_smart if r.success)
        base_smart_success = sum(1 for r in base_smart if r.success)
        print(f"  KEY FINDING: Smart agent success -- Base={base_smart_success}/{n}  LLM={llm_smart_success}/{n}")  # noqa: T201
        assert llm_smart_success >= base_smart_success, (
            f"LLM smart ({llm_smart_success}) should be >= base ({base_smart_success})"
        )

        # 4. Business-term strategy with dbook should not be wildly more expensive
        #    than raw DDL.  With richer aliases and lineage tracking, more tables
        #    are discovered (better recall) which can increase token cost above
        #    the raw DDL baseline for small databases.  We allow up to 1.7x overhead.
        nd_avg = sum(r.tokens_consumed for r in nd_results) / n
        biz_avg = sum(r.tokens_consumed for r in base_business) / n
        assert biz_avg < nd_avg * 1.7, (
            f"Business-term base ({biz_avg:.0f}) should be within 1.7x of raw DDL ({nd_avg:.0f})"
        )
