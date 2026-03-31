# ruff: noqa: S101
"""Phase 2 Benchmark — token efficiency and agent accuracy."""

from __future__ import annotations

import json

import pytest  # type: ignore[import-untyped]

from dbook.catalog import SQLAlchemyCatalog  # type: ignore[import-untyped]
from dbook.compiler import compile_book  # type: ignore[import-untyped]
from tests.benchmark_helpers import (  # type: ignore[import-untyped]
    AgentSimulator,
    BenchmarkReport,
    BenchmarkResult,
    count_tokens,
)


@pytest.fixture
def compiled_book(db_engine, tmp_path):
    """Compile test DB into dbook output."""
    catalog = SQLAlchemyCatalog(db_engine)
    book = catalog.introspect_all()
    compile_book(book, tmp_path)
    return tmp_path


@pytest.fixture
def baseline_tokens(db_engine):
    """Measure baseline: tokens for raw DDL dump of all tables."""
    from sqlalchemy import text  # type: ignore[import-untyped]

    with db_engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT sql FROM sqlite_master "
                "WHERE type='table' AND sql IS NOT NULL"
            )
        )
        all_ddl = "\n\n".join(row[0] for row in result)
    return count_tokens(all_ddl)


class TestBenchmarkBase:
    """Agent simulation benchmark for base mode."""

    def test_q1_user_email(self, compiled_book):
        """Q1: Where is user email stored?"""
        agent = AgentSimulator(compiled_book)

        # Agent reads NAVIGATION.md first
        agent.read_file("NAVIGATION.md")

        # Agent checks concepts.json for "email"
        concepts_raw = agent.read_file("concepts.json")
        concepts = json.loads(concepts_raw)

        # "email" concept should point to the right table
        assert "email" in concepts
        email_tables = concepts["email"]["tables"]
        assert "columns" in concepts["email"]

        # Agent reads the table file
        found = False
        for table_path in email_tables:
            content = agent.read_file(table_path)
            if "email" in content.lower():
                found = True
                break

        assert found, "Agent could not find user email"
        assert agent.tokens_consumed > 0

    def test_q2_orders_customers_link(self, compiled_book):
        """Q2: How are orders linked to customers?"""
        agent = AgentSimulator(compiled_book)

        agent.read_file("NAVIGATION.md")

        # Find the orders table
        schema_dir = compiled_book / "schemas" / "default"
        orders_file = None
        for f in schema_dir.iterdir():
            if (
                "order" in f.name
                and f.name != "_manifest.md"
                and "item" not in f.name
            ):
                orders_file = f"schemas/default/{f.name}"
                break

        assert orders_file is not None
        content = agent.read_file(orders_file)

        # Should show FK to auth_users
        assert "auth_users" in content
        assert agent.tokens_consumed > 0

    def test_q4_analytics_structure(self, compiled_book):
        """Q4: Show me the analytics schema structure."""
        agent = AgentSimulator(compiled_book)

        agent.read_file("NAVIGATION.md")
        manifest = agent.read_file("schemas/default/_manifest.md")

        # Should list analytics tables
        assert "analytics" in manifest.lower()
        assert agent.tokens_consumed > 0

    def test_q7_users_references(self, compiled_book):
        """Q7: Which tables reference the users table?"""
        agent = AgentSimulator(compiled_book)

        users_content = agent.read_file("schemas/default/auth_users.md")

        assert "## Referenced By" in users_content
        # Should list tables that reference auth_users
        assert (
            "auth_sessions" in users_content
            or "billing_orders" in users_content
        )

    def test_q8_orders_indexes(self, compiled_book):
        """Q8: What indexes exist on the orders table?"""
        agent = AgentSimulator(compiled_book)

        content = agent.read_file("schemas/default/billing_orders.md")

        assert "## Indexes" in content
        assert agent.tokens_consumed > 0

    def test_q9_timestamp_columns(self, compiled_book):
        """Q9: Find all columns related to timestamps."""
        agent = AgentSimulator(compiled_book)

        concepts_raw = agent.read_file("concepts.json")
        concepts = json.loads(concepts_raw)

        # Should find "created"/"updated" terms (from created_at, updated_at)
        timestamp_terms = [
            t
            for t in concepts
            if t in ("created", "updated", "at", "date", "expires")
        ]
        assert len(timestamp_terms) > 0

    def test_q10_change_detection(self, compiled_book):
        """Q10: What changed since last compile?"""
        agent = AgentSimulator(compiled_book)

        checksums_raw = agent.read_file("checksums.json")
        checksums = json.loads(checksums_raw)

        assert len(checksums) == 13
        # All hashes should be non-empty
        assert all(h for h in checksums.values())
        assert agent.tokens_consumed > 0

    def test_token_savings_vs_baseline(self, compiled_book, baseline_tokens):
        """Verify token savings >= 50% vs raw DDL baseline."""
        agent = AgentSimulator(compiled_book)

        # Simulate typical query: read NAVIGATION + concepts + 1 table
        agent.read_file("NAVIGATION.md")
        agent.read_file("concepts.json")
        agent.read_file("schemas/default/auth_users.md")

        savings_pct = (
            (1 - agent.tokens_consumed / baseline_tokens) * 100
        )
        assert savings_pct >= 50, (
            f"Token savings only {savings_pct:.1f}% "
            f"(consumed {agent.tokens_consumed} "
            f"vs baseline {baseline_tokens})"
        )

    def test_benchmark_report(self, compiled_book, baseline_tokens):
        """Generate full benchmark report."""
        report = BenchmarkReport(
            phase="Phase 2 — Markdown Compiler",
            mode="base",
            baseline_tokens=baseline_tokens,
        )

        # Run Q1
        agent = AgentSimulator(compiled_book)
        agent.read_file("NAVIGATION.md")
        concepts_raw = agent.read_file("concepts.json")
        concepts = json.loads(concepts_raw)
        found_email = "email" in concepts
        if found_email:
            tables = concepts["email"]["tables"]
            if tables:
                agent.read_file(tables[0])
        report.results.append(
            BenchmarkResult(
                question_id="Q1",
                question="Where is user email stored?",
                expected_answer=["auth_users", "email"],
                files_read=list(agent.files_read),
                tokens_consumed=agent.tokens_consumed,
                answer_found=found_email,
            )
        )

        # Run Q7
        agent.reset()
        content = agent.read_file("schemas/default/auth_users.md")
        found_refs = "Referenced By" in content
        report.results.append(
            BenchmarkResult(
                question_id="Q7",
                question="Which tables reference the users table?",
                expected_answer=["auth_sessions", "billing_orders"],
                files_read=list(agent.files_read),
                tokens_consumed=agent.tokens_consumed,
                answer_found=found_refs,
            )
        )

        # Run Q10
        agent.reset()
        checksums_raw = agent.read_file("checksums.json")
        checksums = json.loads(checksums_raw)
        found_checksums = len(checksums) == 13
        report.results.append(
            BenchmarkResult(
                question_id="Q10",
                question="What changed since last compile?",
                expected_answer=["checksums"],
                files_read=list(agent.files_read),
                tokens_consumed=agent.tokens_consumed,
                answer_found=found_checksums,
            )
        )

        # Print report
        print("\n" + report.summary())  # noqa: T201

        # Assert phase gate
        assert report.accuracy >= 0.85, (
            f"Accuracy {report.accuracy:.0%} < 85%"
        )
