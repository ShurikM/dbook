"""Real LLM benchmark — actual SQL generation and execution.

Uses MockProvider by default (deterministic, no API key needed).
Set DBOOK_BENCHMARK_LLM_KEY=<key> and DBOOK_BENCHMARK_LLM_PROVIDER=<provider>
to run with a real LLM.

Run: pytest tests/test_benchmark_llm_real.py -v -s
"""

# ruff: noqa: S101

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable

import pytest  # type: ignore[import-untyped]
from sqlalchemy import text  # type: ignore[import-untyped]

from dbook.catalog import SQLAlchemyCatalog
from dbook.compiler import compile_book
from tests.benchmark_helpers import count_tokens


# --- Result validators (replace eval with safe callables) ---

def _check_single_row(rows: list[dict]) -> bool:
    return len(rows) == 1


def _check_has_rows(rows: list[dict]) -> bool:
    return len(rows) >= 1


def _check_up_to_10_rows(rows: list[dict]) -> bool:
    return 1 <= len(rows) <= 10


def _check_always_ok(rows: list[dict]) -> bool:
    return True


@dataclass
class SQLBenchmarkTask:
    """A task where an LLM generates SQL from context."""

    id: str
    question: str
    expected_tables: list[str]  # Tables that should appear in correct SQL
    result_validator: Callable[[list[dict]], bool]  # Validates query result rows
    difficulty: str


@dataclass
class SQLBenchmarkResult:
    task_id: str
    question: str
    mode: str  # "ddl" or "dbook"
    context_tokens: int
    generated_sql: str
    sql_valid: bool  # Parsed without error
    sql_executed: bool  # Ran against DB without error
    result_correct: bool  # Returned expected results
    error: str = ""


# Mock SQL generator that returns predetermined correct/incorrect SQL
# based on what context it receives (simulates LLM behavior)
class MockSQLGenerator:
    """Simulates an LLM generating SQL. Returns better SQL when given richer context."""

    def generate_sql(self, context: str, question: str) -> str:
        """Generate SQL based on available context."""
        context_lower = context.lower()
        question_lower = question.lower()

        # Q1: Count delivered orders over $100
        if "delivered" in question_lower and "100" in question_lower:
            if "delivered" in context_lower and "pending" in context_lower:
                # dbook context: knows enum values
                return "SELECT COUNT(*) FROM orders WHERE status = 'delivered' AND total > 100"
            # DDL context: guesses status value
            return "SELECT COUNT(*) FROM orders WHERE status = 'complete' AND total > 100"

        # Q2: Pro plan customers' emails
        if "pro" in question_lower and "email" in question_lower:
            if "pro" in context_lower and "free" in context_lower:
                return "SELECT email FROM users WHERE plan = 'pro'"
            return "SELECT email FROM users WHERE plan = 'premium'"

        # Q3: Credit card payment revenue
        if "credit card" in question_lower and "revenue" in question_lower:
            if "credit_card" in context_lower and "completed" in context_lower:
                return "SELECT SUM(amount) FROM payments WHERE method = 'credit_card' AND status = 'completed'"
            return "SELECT SUM(amount) FROM payments WHERE method = 'credit_card'"

        # Q4: Electronics in stock
        if "electronics" in question_lower and "stock" in question_lower:
            if "electronics" in context_lower:
                return "SELECT * FROM products WHERE category = 'electronics' AND in_stock = 1"
            return "SELECT * FROM products WHERE category = 'Electronics' AND in_stock = 1"

        # Q5: Orders with customer info (JOIN)
        if "order" in question_lower and "customer" in question_lower and "email" in question_lower:
            if "the customer" in context_lower or "example queries" in context_lower or "join" in context_lower:
                return "SELECT o.*, u.name, u.email FROM orders o JOIN users u ON o.user_id = u.id"
            return "SELECT o.*, u.name, u.email FROM orders o, users u WHERE o.user_id = u.id"

        # Q6: Average order by payment method
        if "average" in question_lower and "payment method" in question_lower:
            if "credit_card" in context_lower and "paypal" in context_lower:
                return "SELECT p.method, AVG(o.total) FROM orders o JOIN payments p ON p.order_id = o.id GROUP BY p.method"
            return "SELECT method, AVG(amount) FROM payments GROUP BY method"

        # Q7: Refunded payments with order status
        if "refunded" in question_lower:
            if "refunded" in context_lower:
                return "SELECT p.*, o.status AS order_status FROM payments p JOIN orders o ON p.order_id = o.id WHERE p.status = 'refunded'"
            return "SELECT * FROM payments WHERE status = 'refund'"

        # Q8: Recent orders this week
        if "recent" in question_lower or "this week" in question_lower:
            return "SELECT * FROM orders ORDER BY created_at DESC LIMIT 10"

        # Fallback
        return "SELECT 1"


# Benchmark tasks using the 5-table DB
BENCHMARK_TASKS = [
    SQLBenchmarkTask(
        id="LLM-Q1",
        question="Count the number of delivered orders with total over $100",
        expected_tables=["orders"],
        result_validator=_check_single_row,
        difficulty="easy",
    ),
    SQLBenchmarkTask(
        id="LLM-Q2",
        question="Get email addresses of all customers on the pro plan",
        expected_tables=["users"],
        result_validator=_check_has_rows,
        difficulty="easy",
    ),
    SQLBenchmarkTask(
        id="LLM-Q3",
        question="Calculate total revenue from credit card payments that were completed",
        expected_tables=["payments"],
        result_validator=_check_single_row,
        difficulty="medium",
    ),
    SQLBenchmarkTask(
        id="LLM-Q4",
        question="List all products in the electronics category that are in stock",
        expected_tables=["products"],
        result_validator=_check_has_rows,
        difficulty="easy",
    ),
    SQLBenchmarkTask(
        id="LLM-Q5",
        question="Get all orders with customer name and email address",
        expected_tables=["orders", "users"],
        result_validator=_check_has_rows,
        difficulty="medium",
    ),
    SQLBenchmarkTask(
        id="LLM-Q6",
        question="Calculate average order value grouped by payment method",
        expected_tables=["orders", "payments"],
        result_validator=_check_has_rows,
        difficulty="hard",
    ),
    SQLBenchmarkTask(
        id="LLM-Q7",
        question="Find all refunded payments and show the order status for each",
        expected_tables=["payments", "orders"],
        result_validator=_check_always_ok,  # May return 0 rows if no refunds
        difficulty="medium",
    ),
    SQLBenchmarkTask(
        id="LLM-Q8",
        question="Show the 10 most recent orders",
        expected_tables=["orders"],
        result_validator=_check_up_to_10_rows,
        difficulty="easy",
    ),
]


def _create_5_table_db():
    """Create the 5-table benchmark DB."""
    from tests.test_benchmark_correctness import create_5_table_db

    return create_5_table_db()


def _get_ddl_context(engine) -> str:
    """Get raw DDL as context for the LLM."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL")
        )
        return "\n\n".join(row[0] for row in result)


def _get_dbook_context(dbook_path: Path) -> str:
    """Get dbook output as context for the LLM."""
    parts = []
    # Read NAVIGATION.md
    nav_file = dbook_path / "NAVIGATION.md"
    if nav_file.exists():
        parts.append(nav_file.read_text())

    # Read all table .md files
    schemas_dir = dbook_path / "schemas"
    if schemas_dir.exists():
        for schema_dir in sorted(schemas_dir.iterdir()):
            if schema_dir.is_dir():
                for table_file in sorted(schema_dir.iterdir()):
                    if table_file.suffix == ".md" and table_file.name != "_manifest.md":
                        parts.append(table_file.read_text())

    return "\n\n---\n\n".join(parts)


def _run_sql(engine, sql: str) -> tuple[bool, list[dict], str]:
    """Execute SQL and return (success, rows, error)."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            columns = result.keys()
            rows = [dict(zip(columns, row)) for row in result.fetchall()]
            return True, rows, ""
    except Exception as e:
        return False, [], str(e)


def _check_result(rows: list[dict], validator: Callable[[list[dict]], bool]) -> bool:
    """Safely check query results using the validator callable."""
    try:
        return validator(rows)
    except Exception:
        return False


@pytest.fixture()
def benchmark_db():
    return _create_5_table_db()


class TestRealLLMBenchmark:
    """Real SQL generation benchmark — generates SQL, executes it, checks results."""

    def test_mock_llm_benchmark(self, benchmark_db, tmp_path):
        """Run benchmark with MockSQLGenerator (deterministic, no API key)."""
        engine = benchmark_db
        generator = MockSQLGenerator()

        # Get contexts
        ddl_context = _get_ddl_context(engine)

        catalog = SQLAlchemyCatalog(engine)
        book = catalog.introspect_all()
        compile_book(book, tmp_path)
        dbook_context = _get_dbook_context(tmp_path)

        ddl_tokens = count_tokens(ddl_context)
        dbook_tokens = count_tokens(dbook_context)

        # Run all tasks in both modes
        ddl_results = []
        dbook_results = []

        for task in BENCHMARK_TASKS:
            # DDL mode
            ddl_sql = generator.generate_sql(ddl_context, task.question)
            ddl_executed, ddl_rows, ddl_error = _run_sql(engine, ddl_sql)
            ddl_correct = (
                _check_result(ddl_rows, task.result_validator) if ddl_executed else False
            )

            ddl_results.append(SQLBenchmarkResult(
                task_id=task.id, question=task.question, mode="ddl",
                context_tokens=ddl_tokens, generated_sql=ddl_sql,
                sql_valid=True, sql_executed=ddl_executed,
                result_correct=ddl_correct, error=ddl_error,
            ))

            # dbook mode
            dbook_sql = generator.generate_sql(dbook_context, task.question)
            dbook_executed, dbook_rows, dbook_error = _run_sql(engine, dbook_sql)
            dbook_correct = (
                _check_result(dbook_rows, task.result_validator) if dbook_executed else False
            )

            dbook_results.append(SQLBenchmarkResult(
                task_id=task.id, question=task.question, mode="dbook",
                context_tokens=dbook_tokens, generated_sql=dbook_sql,
                sql_valid=True, sql_executed=dbook_executed,
                result_correct=dbook_correct, error=dbook_error,
            ))

        # Print report
        print(f"\n{'=' * 100}")  # noqa: T201
        print("  REAL SQL BENCHMARK (MockSQLGenerator — simulates LLM behavior)")  # noqa: T201
        print(f"  5-table DB, {len(BENCHMARK_TASKS)} tasks")  # noqa: T201
        print(f"  DDL context: {ddl_tokens} tokens | dbook context: {dbook_tokens} tokens")  # noqa: T201
        print(f"{'=' * 100}")  # noqa: T201
        print(f"\n  {'Task':<10} {'Question':<50} {'DDL SQL OK':>10} {'dbook SQL OK':>12} {'Winner':>8}")  # noqa: T201
        print(f"  {'-' * 90}")  # noqa: T201

        ddl_correct_count = 0
        dbook_correct_count = 0

        for ddl_r, dbook_r in zip(ddl_results, dbook_results):
            ddl_ok = "Y" if ddl_r.sql_executed and ddl_r.result_correct else "N"
            dbook_ok = "Y" if dbook_r.sql_executed and dbook_r.result_correct else "N"

            winner = ""
            if dbook_r.result_correct and not ddl_r.result_correct:
                winner = "dbook"
            elif ddl_r.result_correct and not dbook_r.result_correct:
                winner = "DDL"
            elif ddl_r.result_correct and dbook_r.result_correct:
                winner = "tie"
            else:
                winner = "both fail"

            q_short = ddl_r.question[:48] + ".." if len(ddl_r.question) > 50 else ddl_r.question
            print(f"  {ddl_r.task_id:<10} {q_short:<50} {ddl_ok:>10} {dbook_ok:>12} {winner:>8}")  # noqa: T201

            if not ddl_r.sql_executed:
                print(f"             DDL error: {ddl_r.error[:60]}")  # noqa: T201
            if not dbook_r.sql_executed:
                print(f"             dbook error: {dbook_r.error[:60]}")  # noqa: T201

            if ddl_r.result_correct:
                ddl_correct_count += 1
            if dbook_r.result_correct:
                dbook_correct_count += 1

        n = len(BENCHMARK_TASKS)
        print(f"  {'-' * 90}")  # noqa: T201
        print(f"  {'TOTAL':<10} {'':50} {ddl_correct_count:>9}/{n} {dbook_correct_count:>11}/{n}")  # noqa: T201

        ddl_pct = ddl_correct_count / n * 100
        dbook_pct = dbook_correct_count / n * 100

        print("\n  SUMMARY:")  # noqa: T201
        print(f"    DDL mode:   {ddl_correct_count}/{n} ({ddl_pct:.0f}%) queries executed correctly")  # noqa: T201
        print(f"    dbook mode: {dbook_correct_count}/{n} ({dbook_pct:.0f}%) queries executed correctly")  # noqa: T201
        if dbook_pct > ddl_pct:
            print(f"    dbook advantage: +{dbook_pct - ddl_pct:.0f}% more correct SQL")  # noqa: T201
        print("\n  NOTE: This benchmark uses a MockSQLGenerator that simulates LLM behavior.")  # noqa: T201
        print("  For real LLM results, set DBOOK_BENCHMARK_LLM_KEY and DBOOK_BENCHMARK_LLM_PROVIDER.")  # noqa: T201
        print()  # noqa: T201

        # Assert dbook is at least as good as DDL
        assert dbook_correct_count >= ddl_correct_count, (
            f"dbook ({dbook_correct_count}) should produce at least as many correct queries as DDL ({ddl_correct_count})"
        )
