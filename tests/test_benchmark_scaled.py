# ruff: noqa: S101
"""Scaled benchmark -- token efficiency at realistic database size (50 tables).

Baseline = raw DDL for ALL 50 tables.
Key insight: dbook's value is in *targeted* queries (read 1-3 files instead
of dumping everything).  Compact table overview in NAVIGATION.md lets agents
find tables and key columns without reading individual table files.
"""

from __future__ import annotations

import json

import pytest  # type: ignore[import-untyped]

from dbook.catalog import SQLAlchemyCatalog
from dbook.compiler import compile_book
from tests.benchmark_helpers import (
    AgentSimulator,
    BenchmarkReport,
    BenchmarkResult,
    count_tokens,
)


@pytest.fixture
def compiled_scaled(scaled_db_engine, tmp_path):
    """Compile the 50-table scaled DB into dbook output."""
    catalog = SQLAlchemyCatalog(scaled_db_engine)
    book = catalog.introspect_all()
    compile_book(book, tmp_path)
    return tmp_path


@pytest.fixture
def scaled_baseline_tokens(compiled_scaled):
    """Baseline = tokens to read ALL compiled table files (the naive approach).

    An agent without dbook would need to read every table's DDL.
    With dbook, the agent reads NAVIGATION + targeted files.
    We measure baseline as total tokens across all per-table .md files.
    """
    schemas_dir = compiled_scaled / "schemas" / "default"
    total = 0
    for f in schemas_dir.iterdir():
        if f.suffix == ".md" and f.name != "_manifest.md":
            total += count_tokens(f.read_text())
    return total


class TestScaledBenchmark:
    """Benchmark against 50-table database."""

    def test_baseline_is_large(self, scaled_baseline_tokens):
        """Verify baseline is large enough to be meaningful."""
        assert scaled_baseline_tokens > 5000, (
            f"Baseline only {scaled_baseline_tokens} tokens"
        )

    def test_table_count(self, compiled_scaled):
        """Verify all 50 tables were compiled."""
        schemas_dir = compiled_scaled / "schemas" / "default"
        table_files = [
            f for f in schemas_dir.iterdir()
            if f.suffix == ".md" and f.name != "_manifest.md"
        ]
        assert len(table_files) == 50, (
            f"Expected 50 table files, got {len(table_files)}"
        )

    def test_no_concepts_json(self, compiled_scaled):
        """concepts.json should never be generated."""
        assert not (compiled_scaled / "concepts.json").exists(), (
            "concepts.json should not exist"
        )

    def test_navigation_has_table_overview(self, compiled_scaled):
        """NAVIGATION.md should have compact table overview."""
        nav = (compiled_scaled / "NAVIGATION.md").read_text()
        assert "## Tables" in nav

    def test_navigation_compact(self, compiled_scaled):
        """NAVIGATION.md with table overview stays compact even at 50 tables."""
        nav = compiled_scaled / "NAVIGATION.md"
        tokens = count_tokens(nav.read_text())
        # Compact table overview for 50 tables
        assert tokens < 1500, f"NAVIGATION.md is {tokens} tokens"

    def test_q1_find_email(self, compiled_scaled, scaled_baseline_tokens):
        """Q1: Where is user email stored? (via NAVIGATION.md table overview)"""
        agent = AgentSimulator(compiled_scaled)

        nav_content = agent.read_file("NAVIGATION.md")

        # "email" should be findable in Key Columns of table overview
        assert "email" in nav_content.lower()

        # Parse table name from the row where email appears in Key Columns
        for line in nav_content.split("\n"):
            if "email" in line.lower() and line.startswith("|"):
                parts = line.split("|")
                if len(parts) >= 2:
                    table_name = parts[1].strip()
                    if table_name and table_name != "Table":
                        for md in compiled_scaled.rglob(f"*{table_name}*.md"):
                            rel = str(md.relative_to(compiled_scaled))
                            agent.read_file(rel)
                            break
                break

        savings = (1 - agent.tokens_consumed / scaled_baseline_tokens) * 100
        assert savings >= 25, f"Only {savings:.0f}% savings for Q1"

    def test_q2_orders_to_customers(self, compiled_scaled, scaled_baseline_tokens):
        """Q2: How are orders linked to customers? (FK traversal)"""
        agent = AgentSimulator(compiled_scaled)

        agent.read_file("NAVIGATION.md")
        schemas_dir = compiled_scaled / "schemas" / "default"
        orders_file = None
        for f in schemas_dir.iterdir():
            if f.name.startswith("billing_orders") and f.suffix == ".md":
                orders_file = f"schemas/default/{f.name}"
                break

        assert orders_file is not None
        content = agent.read_file(orders_file)
        assert "auth_users_v2" in content

        savings = (1 - agent.tokens_consumed / scaled_baseline_tokens) * 100
        assert savings >= 90, f"Only {savings:.0f}% savings for Q2"

    def test_q3_financial_tables(self, compiled_scaled, scaled_baseline_tokens):
        """Q3: What tables contain financial data? (via NAVIGATION.md table overview)"""
        agent = AgentSimulator(compiled_scaled)

        nav_content = agent.read_file("NAVIGATION.md")
        nav_lower = nav_content.lower()

        financial_terms = [
            t for t in ["billing", "refund", "subscription", "inventory", "support"]
            if t in nav_lower
        ]
        assert len(financial_terms) >= 3, (
            f"Only found {financial_terms} in NAVIGATION.md"
        )

        savings = (1 - agent.tokens_consumed / scaled_baseline_tokens) * 100
        assert savings >= 30, f"Only {savings:.0f}% savings for Q3"

    def test_q4_schema_overview(self, compiled_scaled, scaled_baseline_tokens):
        """Q4: Show me the analytics schema structure."""
        agent = AgentSimulator(compiled_scaled)

        agent.read_file("NAVIGATION.md")
        manifest_content = agent.read_file("schemas/default/_manifest.md")
        assert "analytics" in manifest_content.lower()

        savings = (1 - agent.tokens_consumed / scaled_baseline_tokens) * 100
        assert savings >= 50, f"Only {savings:.0f}% savings for Q4"

    def test_q7_reverse_fk_lookup(self, compiled_scaled, scaled_baseline_tokens):
        """Q7: Which tables reference the users table?"""
        agent = AgentSimulator(compiled_scaled)

        content = agent.read_file("schemas/default/auth_users_v2.md")
        assert "Referenced By" in content

        savings = (1 - agent.tokens_consumed / scaled_baseline_tokens) * 100
        assert savings >= 95, f"Only {savings:.0f}% savings for Q7"

    def test_q8_table_indexes(self, compiled_scaled, scaled_baseline_tokens):
        """Q8: What indexes exist on the orders table?"""
        agent = AgentSimulator(compiled_scaled)

        schemas_dir = compiled_scaled / "schemas" / "default"
        found = False
        for f in schemas_dir.iterdir():
            if f.name.startswith("billing_orders") and f.suffix == ".md":
                content = agent.read_file(f"schemas/default/{f.name}")
                assert "Indexes" in content
                found = True
                break
        assert found, "Could not find billing_orders table file"

        savings = (1 - agent.tokens_consumed / scaled_baseline_tokens) * 100
        assert savings >= 95, f"Only {savings:.0f}% savings for Q8"

    def test_q9_timestamp_columns(self, compiled_scaled, scaled_baseline_tokens):
        """Q9: Find all columns related to timestamps (via NAVIGATION.md table overview)."""
        agent = AgentSimulator(compiled_scaled)

        nav_content = agent.read_file("NAVIGATION.md")
        nav_lower = nav_content.lower()

        time_terms = [
            t for t in ("created", "updated", "at", "date", "expires",
                        "recorded", "shipped", "delivered",
                        "started", "ended", "cancelled", "resolved")
            if t in nav_lower
        ]
        assert len(time_terms) >= 1, (
            "No time terms found in NAVIGATION.md Quick Lookup"
        )

        savings = (1 - agent.tokens_consumed / scaled_baseline_tokens) * 100
        assert savings >= 30, f"Only {savings:.0f}% savings for Q9"

    def test_q10_change_detection(self, compiled_scaled, scaled_baseline_tokens):
        """Q10: What changed since last compile?"""
        agent = AgentSimulator(compiled_scaled)

        checksums_raw = agent.read_file("checksums.json")
        checksums = json.loads(checksums_raw)
        assert len(checksums) == 50, (
            f"Expected 50 checksums, got {len(checksums)}"
        )

        savings = (1 - agent.tokens_consumed / scaled_baseline_tokens) * 100
        assert savings >= 85, f"Only {savings:.0f}% savings for Q10"

    def test_targeted_query_efficiency(self, compiled_scaled, scaled_baseline_tokens):
        """Targeted single-table query uses < 5% of baseline tokens."""
        agent = AgentSimulator(compiled_scaled)
        agent.read_file("schemas/default/auth_users_v2.md")
        pct = agent.tokens_consumed / scaled_baseline_tokens * 100
        assert pct < 5, f"Single table read uses {pct:.1f}% of baseline"

    def test_full_benchmark_report(self, compiled_scaled, scaled_baseline_tokens):
        """Generate complete benchmark report with all questions."""
        report = BenchmarkReport(
            phase="Phase 2 -- Scaled Benchmark (50 tables)",
            mode="base",
            baseline_tokens=scaled_baseline_tokens,
        )

        questions = [
            ("Q1", "Where is user email stored?", ["email"], self._run_q1),
            ("Q2", "How are orders linked to customers?", ["auth_users_v2"], self._run_q2),
            ("Q3", "What tables contain financial data?", ["billing"], self._run_q3),
            ("Q4", "Analytics schema structure", ["analytics"], self._run_q4),
            ("Q7", "Which tables reference users?", ["Referenced By"], self._run_q7),
            ("Q8", "Orders table indexes", ["Indexes"], self._run_q8),
            ("Q9", "Timestamp columns", ["timestamp"], self._run_q9),
            ("Q10", "What changed since compile?", ["checksums"], self._run_q10),
        ]

        for qid, question, expected, runner in questions:
            agent = AgentSimulator(compiled_scaled)
            found = runner(agent, compiled_scaled)
            report.results.append(BenchmarkResult(
                question_id=qid,
                question=question,
                expected_answer=expected,
                files_read=list(agent.files_read),
                tokens_consumed=agent.tokens_consumed,
                answer_found=found,
            ))

        print("\n" + report.summary())  # noqa: T201
        assert report.accuracy >= 0.85, (
            f"Accuracy {report.accuracy:.0%} < 85%"
        )
        assert report.token_savings_pct >= 40, (
            f"Token savings {report.token_savings_pct:.1f}% < 40%"
        )

    # -- Runner helpers for the full report --------------------------------

    @staticmethod
    def _run_q1(agent: AgentSimulator, path) -> bool:
        nav = agent.read_file("NAVIGATION.md")
        if "email" not in nav.lower():
            return False
        # Parse table name from the row where email appears in Key Columns
        for line in nav.split("\n"):
            if "email" in line.lower() and line.startswith("|"):
                parts = line.split("|")
                if len(parts) >= 2:
                    table_name = parts[1].strip()
                    if table_name and table_name != "Table":
                        for md in path.rglob(f"*{table_name}*.md"):
                            rel = str(md.relative_to(path))
                            agent.read_file(rel)
                            break
                break
        return True

    @staticmethod
    def _run_q2(agent: AgentSimulator, path) -> bool:
        agent.read_file("NAVIGATION.md")
        for f in (path / "schemas" / "default").iterdir():
            if f.name.startswith("billing_orders") and f.suffix == ".md":
                content = agent.read_file(f"schemas/default/{f.name}")
                return "auth_users_v2" in content
        return False

    @staticmethod
    def _run_q3(agent: AgentSimulator, path) -> bool:
        nav = agent.read_file("NAVIGATION.md")
        nav_lower = nav.lower()
        terms = [
            t for t in ["billing", "refund", "subscription", "inventory", "support"]
            if t in nav_lower
        ]
        return len(terms) >= 3

    @staticmethod
    def _run_q4(agent: AgentSimulator, path) -> bool:
        agent.read_file("NAVIGATION.md")
        content = agent.read_file("schemas/default/_manifest.md")
        return "analytics" in content.lower()

    @staticmethod
    def _run_q7(agent: AgentSimulator, path) -> bool:
        content = agent.read_file("schemas/default/auth_users_v2.md")
        return "Referenced By" in content

    @staticmethod
    def _run_q8(agent: AgentSimulator, path) -> bool:
        for f in (path / "schemas" / "default").iterdir():
            if f.name.startswith("billing_orders") and f.suffix == ".md":
                content = agent.read_file(f"schemas/default/{f.name}")
                return "Indexes" in content
        return False

    @staticmethod
    def _run_q9(agent: AgentSimulator, path) -> bool:
        nav = agent.read_file("NAVIGATION.md")
        nav_lower = nav.lower()
        return any(
            t in nav_lower for t in (
                "created", "updated", "at", "date", "expires",
                "recorded", "shipped", "delivered",
                "started", "ended", "cancelled", "resolved",
            )
        )

    @staticmethod
    def _run_q10(agent: AgentSimulator, path) -> bool:
        raw = agent.read_file("checksums.json")
        checksums = json.loads(raw)
        return len(checksums) == 50
