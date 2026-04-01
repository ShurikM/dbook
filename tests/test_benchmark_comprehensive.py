"""Comprehensive benchmark: all modes x all scales."""

# ruff: noqa: S101

from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import text  # type: ignore[import-untyped]

from dbook.catalog import SQLAlchemyCatalog
from dbook.compiler import compile_book
from dbook.llm.provider import MockProvider
from dbook.llm.enricher import enrich_book
from tests.benchmark_helpers import AgentSimulator, count_tokens


def _get_ddl_tokens(engine) -> int:
    """Get total tokens for raw DDL of all tables."""
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL"
        ))
        all_ddl = "\n\n".join(row[0] for row in result)
    return count_tokens(all_ddl)


def _compile_base(engine, output_dir: Path) -> Path:
    """Compile in base mode."""
    catalog = SQLAlchemyCatalog(engine)
    book = catalog.introspect_all()
    compile_book(book, output_dir)
    return output_dir


def _compile_llm(engine, output_dir: Path) -> Path:
    """Compile in LLM mode."""
    catalog = SQLAlchemyCatalog(engine)
    book = catalog.introspect_all()
    book.mode = "llm"
    provider = MockProvider()
    enrich_book(book, provider)
    compile_book(book, output_dir)
    return output_dir


def _find_term_in_dbook(agent: AgentSimulator, compiled_path: Path, term: str) -> bool:
    """Find a term in NAVIGATION.md table overview."""
    nav = agent.read_file("NAVIGATION.md")
    return term in nav.lower()


def _get_table_for_term(agent: AgentSimulator, compiled_path: Path, term: str) -> str | None:
    """Get the first table file path for a term from NAVIGATION.md table overview."""
    nav = agent.read_file("NAVIGATION.md")
    # Look for term in table overview rows (Key Columns or table name)
    for line in nav.split("\n"):
        if term in line.lower() and line.startswith("|"):
            # Extract table name from the first column
            parts = line.split("|")
            if len(parts) >= 2:
                table_name = parts[1].strip()
                if table_name and table_name != "Table":
                    # Find the actual file
                    for md in compiled_path.rglob(f"*{table_name}*.md"):
                        rel = str(md.relative_to(compiled_path))
                        return rel
    return None


class TestComprehensiveBenchmark:
    """Definitive benchmark: No dbook vs Base dbook vs LLM dbook at small and large scale."""

    def test_small_db_comparison(self, db_engine, tmp_path):
        """13-table DB: No dbook vs Base vs LLM."""
        ddl_tokens = _get_ddl_tokens(db_engine)
        base_dir = _compile_base(db_engine, tmp_path / "base")
        llm_dir = _compile_llm(db_engine, tmp_path / "llm")

        results = self._run_all_questions(ddl_tokens, base_dir, llm_dir)
        self._print_report("SMALL DB (13 tables)", ddl_tokens, results)

        # Assert base dbook saves tokens vs no dbook for targeted queries
        for qid, no_db, base_tok, llm_tok, base_ok, llm_ok in results:
            if qid in ("Q7", "Q10"):  # Single-file queries
                assert base_tok < no_db, (
                    f"{qid}: Base dbook ({base_tok}) should be cheaper than no dbook ({no_db})"
                )

    def test_large_db_comparison(self, scaled_db_engine, tmp_path):
        """50-table DB: No dbook vs Base vs LLM."""
        ddl_tokens = _get_ddl_tokens(scaled_db_engine)
        base_dir = _compile_base(scaled_db_engine, tmp_path / "base")
        llm_dir = _compile_llm(scaled_db_engine, tmp_path / "llm")

        results = self._run_all_questions(ddl_tokens, base_dir, llm_dir)
        self._print_report("LARGE DB (50 tables)", ddl_tokens, results)

        # For targeted queries (Q7, Q8, Q10), dbook is much cheaper
        targeted = [r for r in results if r[0] in ("Q7", "Q8", "Q10")]
        targeted_avg = sum(r[2] for r in targeted) / len(targeted)
        assert targeted_avg < ddl_tokens, (
            f"Targeted query avg ({targeted_avg:.0f}) should be cheaper than no dbook ({ddl_tokens})"
        )
        # All queries should find their answers
        assert all(r[4] for r in results), "All base queries should find answers"

    def _run_all_questions(self, ddl_tokens, base_dir, llm_dir):
        """Run 6 questions against base and LLM dirs.

        Returns list of (qid, no_dbook_tok, base_tok, llm_tok, base_ok, llm_ok).
        """
        questions = [
            ("Q1", "email", self._q_concept_then_table),
            ("Q3", "payment,invoice,billing,order,price", self._q_concept_count),
            ("Q7", None, self._q_references),
            ("Q8", None, self._q_indexes),
            ("Q9", "created,updated,date,time", self._q_concept_count),
            ("Q10", None, self._q_checksums),
        ]

        results = []
        for qid, arg, runner in questions:
            # Base
            agent_base = AgentSimulator(base_dir)
            base_ok = runner(agent_base, base_dir, arg)

            # LLM
            agent_llm = AgentSimulator(llm_dir)
            llm_ok = runner(agent_llm, llm_dir, arg)

            results.append((
                qid, ddl_tokens,
                agent_base.tokens_consumed, agent_llm.tokens_consumed,
                base_ok, llm_ok,
            ))

        return results

    def _q_concept_then_table(self, agent, path, term):
        """Read nav, find term, read its table."""
        agent.read_file("NAVIGATION.md")
        found = _find_term_in_dbook(agent, path, term)
        if found:
            table_path = _get_table_for_term(agent, path, term)
            if table_path:
                agent.read_file(table_path)
        return found

    def _q_concept_count(self, agent, path, terms_csv):
        """Check how many terms are findable in NAVIGATION.md."""
        nav_content = agent.read_file("NAVIGATION.md")
        terms = [t.strip() for t in terms_csv.split(",")]
        nav_lower = nav_content.lower()
        found = sum(1 for t in terms if t in nav_lower)
        return found >= 3

    def _q_references(self, agent, path, _arg):
        """Find tables referencing users."""
        # Find auth_users or users table
        schemas_dir = path / "schemas"
        for md in schemas_dir.rglob("*users*.md"):
            if md.name != "_manifest.md":
                rel = str(md.relative_to(path))
                content = agent.read_file(rel)
                return "Related Tables" in content
        return False

    def _q_indexes(self, agent, path, _arg):
        """Find indexes on orders table."""
        schemas_dir = path / "schemas"
        for md in schemas_dir.rglob("*orders*.md"):
            if md.name != "_manifest.md" and "item" not in md.name:
                rel = str(md.relative_to(path))
                content = agent.read_file(rel)
                return "Indexes" in content
        return False

    def _q_checksums(self, agent, path, _arg):
        """Read checksums."""
        raw = agent.read_file("checksums.json")
        checksums = json.loads(raw)
        return len(checksums) > 0

    def _print_report(self, title, ddl_tokens, results):
        """Print formatted comparison report."""
        print(f"\n{'=' * 80}")  # noqa: T201
        print(f"  {title}")  # noqa: T201
        print(f"{'=' * 80}")  # noqa: T201
        print(f"\n  No dbook baseline: {ddl_tokens} tokens (agent reads ALL raw DDL)")  # noqa: T201
        print(  # noqa: T201
            f"\n  {'Q':<5} {'No dbook':>10} {'Base':>10} {'Base Save':>10}"
            f" {'LLM':>10} {'LLM Save':>10} {'Base OK':>8} {'LLM OK':>8}"
        )
        print(f"  {'-' * 75}")  # noqa: T201

        base_total = 0
        llm_total = 0
        base_ok_count = 0
        llm_ok_count = 0

        for qid, no_db, base_tok, llm_tok, base_ok, llm_ok in results:
            b_save = f"{(1 - base_tok / no_db) * 100:.0f}%"
            l_save = f"{(1 - llm_tok / no_db) * 100:.0f}%"
            b_mark = "pass" if base_ok else "FAIL"
            l_mark = "pass" if llm_ok else "FAIL"
            print(  # noqa: T201
                f"  {qid:<5} {no_db:>10} {base_tok:>10} {b_save:>10}"
                f" {llm_tok:>10} {l_save:>10} {b_mark:>8} {l_mark:>8}"
            )
            base_total += base_tok
            llm_total += llm_tok
            if base_ok:
                base_ok_count += 1
            if llm_ok:
                llm_ok_count += 1

        n = len(results)
        base_avg = base_total / n
        llm_avg = llm_total / n
        b_save_avg = f"{(1 - base_avg / ddl_tokens) * 100:.0f}%"
        l_save_avg = f"{(1 - llm_avg / ddl_tokens) * 100:.0f}%"

        print(f"  {'-' * 75}")  # noqa: T201
        print(  # noqa: T201
            f"  {'AVG':<5} {ddl_tokens:>10} {base_avg:>10.0f} {b_save_avg:>10}"
            f" {llm_avg:>10.0f} {l_save_avg:>10}"
            f" {base_ok_count:>7}/{n} {llm_ok_count:>7}/{n}"
        )
        print()  # noqa: T201
