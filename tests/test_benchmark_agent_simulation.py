"""Realistic agent simulation benchmark — agents performing actual data tasks."""

# ruff: noqa: S101

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import text  # type: ignore[import-untyped]

from dbook.catalog import SQLAlchemyCatalog
from dbook.compiler import compile_book
from dbook.llm.provider import MockProvider
from dbook.llm.enricher import enrich_book
from tests.benchmark_helpers import AgentSimulator, count_tokens


# --- Agent Task Definitions ---

AGENT_TASKS = [
    {
        "id": "T1",
        "task": "Get all active users with their email addresses",
        "keywords": ["user", "email", "active"],
        "expected_tables": ["auth_users"],
        "expected_columns": ["email", "is_active"],
        "difficulty": "easy",
    },
    {
        "id": "T2",
        "task": "Find total revenue per customer",
        "keywords": ["order", "total", "user", "revenue"],
        "expected_tables": ["billing_orders", "auth_users"],
        "expected_columns": ["total", "user_id"],
        "difficulty": "medium",
    },
    {
        "id": "T3",
        "task": "List all products that have never been ordered",
        "keywords": ["product", "order", "item"],
        "expected_tables": ["billing_products", "billing_order_items"],
        "expected_columns": ["product_id"],
        "difficulty": "medium",
    },
    {
        "id": "T4",
        "task": "Track user login sessions with IP addresses",
        "keywords": ["session", "login", "ip", "user"],
        "expected_tables": ["auth_sessions"],
        "expected_columns": ["ip_address", "user_id"],
        "difficulty": "easy",
    },
    {
        "id": "T5",
        "task": "Calculate average order value by product category",
        "keywords": ["order", "product", "category", "price", "average"],
        "expected_tables": ["billing_orders", "billing_order_items", "billing_products"],
        "expected_columns": ["category", "unit_price"],
        "difficulty": "hard",
    },
    {
        "id": "T6",
        "task": "Find unpaid invoices with customer contact info",
        "keywords": ["invoice", "paid", "email", "contact", "status"],
        "expected_tables": ["billing_invoices"],
        "expected_columns": ["contact_email", "status"],
        "difficulty": "easy",
    },
    {
        "id": "T7",
        "task": "Get user roles and permissions",
        "keywords": ["role", "user", "permission"],
        "expected_tables": ["auth_roles", "auth_user_roles"],
        "expected_columns": ["role_id", "user_id"],
        "difficulty": "easy",
    },
    {
        "id": "T8",
        "task": "Analyze page view patterns by event type",
        "keywords": ["event", "page", "view", "analytics"],
        "expected_tables": ["analytics_events"],
        "expected_columns": ["event_type", "page"],
        "difficulty": "easy",
    },
    {
        "id": "T9",
        "task": "Find daily revenue trends over the last month",
        "keywords": ["revenue", "daily", "date", "trend"],
        "expected_tables": ["analytics_daily_revenue"],
        "expected_columns": ["date", "total_revenue"],
        "difficulty": "easy",
    },
    {
        "id": "T10",
        "task": "Get payment history with card details for a specific invoice",
        "keywords": ["payment", "card", "invoice"],
        "expected_tables": ["billing_payments", "billing_invoices"],
        "expected_columns": ["card_last_four", "invoice_id"],
        "difficulty": "medium",
    },
    {
        "id": "T11",
        "task": "Build a customer 360 view: orders, payments, sessions",
        "keywords": ["user", "order", "payment", "session"],
        "expected_tables": ["auth_users", "billing_orders", "billing_payments", "auth_sessions"],
        "expected_columns": ["user_id"],
        "difficulty": "hard",
    },
    {
        "id": "T12",
        "task": "Find which discount codes have been used most",
        "keywords": ["discount", "code", "order"],
        "expected_tables": ["billing_discounts", "billing_orders"],
        "expected_columns": ["discount_id", "code"],
        "difficulty": "medium",
    },
    {
        "id": "T13",
        "task": "Identify PII columns that need GDPR compliance review",
        "keywords": ["email", "phone", "name", "ip", "card"],
        "expected_tables": ["auth_users", "billing_payments", "billing_invoices"],
        "expected_columns": ["email", "phone", "card_last_four", "contact_email"],
        "difficulty": "medium",
    },
    {
        "id": "T14",
        "task": "Find conversion funnel drop-off rates",
        "keywords": ["funnel", "conversion"],
        "expected_tables": ["analytics_funnels"],
        "expected_columns": ["conversion_rate", "steps"],
        "difficulty": "easy",
    },
    {
        "id": "T15",
        "task": "Get order items with product names and quantities",
        "keywords": ["order", "item", "product", "name", "quantity"],
        "expected_tables": ["billing_order_items", "billing_products"],
        "expected_columns": ["quantity", "product_id"],
        "difficulty": "medium",
    },
]


@dataclass
class TaskResult:
    task_id: str
    task: str
    difficulty: str
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
        found = sum(1 for t in self.expected_tables if any(t in f for f in self.tables_found))
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


def _run_no_dbook_agent(engine, task: dict) -> TaskResult:
    """Agent reads ALL raw DDL to answer the task."""
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL"
        ))
        all_ddl = "\n\n".join(row[0] for row in result)

    tokens = count_tokens(all_ddl)

    # The agent "reads" all DDL — check if expected tables/columns appear
    tables_found = [t for t in task["expected_tables"] if t in all_ddl]
    columns_found = [c for c in task["expected_columns"] if c in all_ddl]

    return TaskResult(
        task_id=task["id"], task=task["task"], difficulty=task["difficulty"],
        tokens_consumed=tokens, files_read=1,
        tables_found=tables_found, columns_found=columns_found,
        expected_tables=task["expected_tables"], expected_columns=task["expected_columns"],
    )


def _run_dbook_agent(dbook_path: Path, task: dict) -> TaskResult:
    """Agent navigates dbook output to answer the task."""
    agent = AgentSimulator(dbook_path)

    # Step 1: Read NAVIGATION.md
    nav = agent.read_file("NAVIGATION.md")

    # Step 2: Find relevant tables from navigation using keywords
    keywords = task["keywords"]
    relevant_tables = []
    for line in nav.split("\n"):
        line_lower = line.lower()
        if any(kw in line_lower for kw in keywords):
            parts = line.split("|")
            if len(parts) >= 3:
                table_name = parts[1].strip()
                if table_name and table_name not in ("Table", "---", ""):
                    relevant_tables.append(table_name)

    # Deduplicate, keep order
    seen = set()
    unique_tables = []
    for t in relevant_tables:
        if t not in seen:
            seen.add(t)
            unique_tables.append(t)

    # Step 3: Read up to 3 most relevant table files
    all_content = nav
    tables_found = list(unique_tables)
    columns_found = []

    for table_name in unique_tables[:3]:
        # Find the file
        schemas_dir = dbook_path / "schemas"
        for schema_dir in schemas_dir.iterdir():
            table_file = schema_dir / f"{table_name}.md"
            if table_file.exists():
                content = agent.read_file(str(table_file.relative_to(dbook_path)))
                all_content += content
                break

    # Check which expected columns appear in all read content
    columns_found = [c for c in task["expected_columns"] if c in all_content]

    return TaskResult(
        task_id=task["id"], task=task["task"], difficulty=task["difficulty"],
        tokens_consumed=agent.tokens_consumed, files_read=len(agent.files_read),
        tables_found=tables_found, columns_found=columns_found,
        expected_tables=task["expected_tables"], expected_columns=task["expected_columns"],
    )


class TestAgentSimulation:
    """Realistic agent simulation across 15 tasks x 3 modes."""

    def test_full_simulation_13_tables(self, db_engine, tmp_path):
        """Run all 15 tasks against 13-table DB in all 3 modes."""
        # Compile both modes
        catalog = SQLAlchemyCatalog(db_engine)

        base_dir = tmp_path / "base"
        book_base = catalog.introspect_all()
        compile_book(book_base, base_dir)

        llm_dir = tmp_path / "llm"
        book_llm = catalog.introspect_all()
        book_llm.mode = "llm"
        enrich_book(book_llm, MockProvider())
        compile_book(book_llm, llm_dir)

        # Run all tasks
        no_dbook_results = [_run_no_dbook_agent(db_engine, t) for t in AGENT_TASKS]
        base_results = [_run_dbook_agent(base_dir, t) for t in AGENT_TASKS]
        llm_results = [_run_dbook_agent(llm_dir, t) for t in AGENT_TASKS]

        _print_simulation_report("SMALL DB (13 tables)", no_dbook_results, base_results, llm_results)

        # Assertions
        base_success = sum(1 for r in base_results if r.success)
        assert base_success >= 10, f"Base mode only succeeded on {base_success}/15 tasks"

    def test_full_simulation_50_tables(self, scaled_db_engine, tmp_path):
        """Run all 15 tasks against 50-table DB in all 3 modes."""
        catalog = SQLAlchemyCatalog(scaled_db_engine)

        base_dir = tmp_path / "base"
        book_base = catalog.introspect_all()
        compile_book(book_base, base_dir)

        llm_dir = tmp_path / "llm"
        book_llm = catalog.introspect_all()
        book_llm.mode = "llm"
        enrich_book(book_llm, MockProvider())
        compile_book(book_llm, llm_dir)

        no_dbook_results = [_run_no_dbook_agent(scaled_db_engine, t) for t in AGENT_TASKS]
        base_results = [_run_dbook_agent(base_dir, t) for t in AGENT_TASKS]
        llm_results = [_run_dbook_agent(llm_dir, t) for t in AGENT_TASKS]

        _print_simulation_report("LARGE DB (50 tables)", no_dbook_results, base_results, llm_results)

        # At 50 tables, LLM dbook may use more tokens than raw DDL because
        # enrichments (enum values, example queries, semantic FK descriptions)
        # and lineage tracking add correctness value. The overhead should stay
        # within 50%.
        llm_avg_tok = sum(r.tokens_consumed for r in llm_results) / len(llm_results)
        no_dbook_avg_tok = sum(r.tokens_consumed for r in no_dbook_results) / len(no_dbook_results)
        overhead = (llm_avg_tok - no_dbook_avg_tok) / no_dbook_avg_tok if no_dbook_avg_tok else 0
        assert overhead < 0.50, f"LLM dbook overhead ({overhead:.0%}) should be < 50% vs no dbook ({llm_avg_tok:.0f} vs {no_dbook_avg_tok:.0f})"


def _print_simulation_report(title, no_dbook, base, llm):
    """Print detailed simulation report."""
    n = len(no_dbook)

    print(f"\n{'=' * 100}")  # noqa: T201
    print(f"  AGENT SIMULATION: {title}")  # noqa: T201
    print(f"  {n} realistic tasks x 3 modes")  # noqa: T201
    print(f"{'=' * 100}")  # noqa: T201

    print(f"\n  {'Task':<5} {'Description':<45} {'Diff':<6} {'No dbook':>9} {'Base':>9} {'LLM':>9} {'Base Save':>10} {'B-OK':>5} {'L-OK':>5}")  # noqa: T201
    print(f"  {'-' * 95}")  # noqa: T201

    for nd, b, lm in zip(no_dbook, base, llm):
        desc = nd.task[:43] + ".." if len(nd.task) > 45 else nd.task
        b_save = f"{(1 - b.tokens_consumed / nd.tokens_consumed) * 100:.0f}%" if nd.tokens_consumed else "N/A"
        print(f"  {nd.task_id:<5} {desc:<45} {nd.difficulty:<6} {nd.tokens_consumed:>9} {b.tokens_consumed:>9} {lm.tokens_consumed:>9} {b_save:>10} {'Y' if b.success else 'N':>5} {'Y' if lm.success else 'N':>5}")  # noqa: T201

    print(f"  {'-' * 95}")  # noqa: T201

    nd_avg = sum(r.tokens_consumed for r in no_dbook) / n
    b_avg = sum(r.tokens_consumed for r in base) / n
    l_avg = sum(r.tokens_consumed for r in llm) / n
    b_success = sum(1 for r in base if r.success)
    l_success = sum(1 for r in llm if r.success)
    nd_success = sum(1 for r in no_dbook if r.success)
    b_save_avg = f"{(1 - b_avg / nd_avg) * 100:.0f}%" if nd_avg else "N/A"

    print(f"  {'AVG':<5} {'':45} {'':6} {nd_avg:>9.0f} {b_avg:>9.0f} {l_avg:>9.0f} {b_save_avg:>10} {b_success:>4}/{n} {l_success:>4}/{n}")  # noqa: T201

    print("\n  DISCOVERY QUALITY:")  # noqa: T201
    b_table_recall = sum(r.table_recall for r in base) / n
    l_table_recall = sum(r.table_recall for r in llm) / n
    b_col_recall = sum(r.column_recall for r in base) / n
    l_col_recall = sum(r.column_recall for r in llm) / n
    print(f"    Table recall:  Base={b_table_recall:.0%}  LLM={l_table_recall:.0%}")  # noqa: T201
    print(f"    Column recall: Base={b_col_recall:.0%}  LLM={l_col_recall:.0%}")  # noqa: T201
    print(f"    Task success:  No dbook={nd_success}/{n}  Base={b_success}/{n}  LLM={l_success}/{n}")  # noqa: T201

    # By difficulty
    for diff in ("easy", "medium", "hard"):
        b_diff = [r for r in base if r.difficulty == diff]
        l_diff = [r for r in llm if r.difficulty == diff]
        nd_diff = [r for r in no_dbook if r.difficulty == diff]
        if b_diff:
            b_s = sum(1 for r in b_diff if r.success)
            l_s = sum(1 for r in l_diff if r.success)
            nd_s = sum(1 for r in nd_diff if r.success)
            print(f"    {diff.upper():>8}: No dbook={nd_s}/{len(nd_diff)}  Base={b_s}/{len(b_diff)}  LLM={l_s}/{len(l_diff)}")  # noqa: T201
    print()  # noqa: T201
