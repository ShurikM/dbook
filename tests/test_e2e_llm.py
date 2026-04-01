# ruff: noqa: S101
"""Phase 5 E2E tests — LLM enrichment with MockProvider."""

from __future__ import annotations

import json

from dbook.catalog import SQLAlchemyCatalog
from dbook.compiler import compile_book
from dbook.llm.provider import MockProvider
from dbook.llm.enricher import enrich_book


class TestLLMEnrichment:
    """Test LLM enrichment pipeline."""

    def test_enrich_generates_table_summaries(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "llm"
        provider = MockProvider()

        enrich_book(book, provider)

        users = book.schemas["default"].tables["auth_users"]
        assert users.summary, "Table should have an LLM-generated summary"
        assert len(users.summary) > 20, "Summary should be meaningful"
        assert "user" in users.summary.lower() or "account" in users.summary.lower()

    def test_enrich_generates_schema_narratives(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "llm"
        provider = MockProvider()

        enrich_book(book, provider)

        schema = book.schemas["default"]
        assert schema.narrative, "Schema should have a narrative"
        assert len(schema.narrative) > 20

    def test_enrich_generates_column_purposes(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "llm"
        provider = MockProvider()

        enrich_book(book, provider)

        users = book.schemas["default"].tables["auth_users"]
        assert users.column_purposes, "Should have column purposes"
        # MockProvider returns purposes for auth_users columns
        assert "email" in users.column_purposes or len(users.column_purposes) > 0

    def test_enrich_generates_concept_aliases(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "llm"
        provider = MockProvider()

        enrich_book(book, provider)

        # Check aliases were stored
        aliases = getattr(book, '_concept_aliases', None)
        assert aliases is not None, "Concept aliases should be generated"
        assert "user" in aliases
        assert len(aliases["user"]) >= 2

    def test_enrich_returns_stats(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "llm"
        provider = MockProvider()

        result = enrich_book(book, provider)

        assert result["tables_enriched"] == 13
        assert result["schemas_enriched"] >= 1
        assert result["aliases_added"] > 0
        assert result["total_llm_calls"] > 0

    def test_mock_provider_call_count(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "llm"
        provider = MockProvider()

        enrich_book(book, provider)

        # Expected: 13 table summaries + 13 column purposes + 1 schema narrative + 1 concept aliases = 28
        assert provider.call_count >= 20, f"Expected >= 20 LLM calls, got {provider.call_count}"

    def test_enrich_updates_mode(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "base"
        provider = MockProvider()

        enrich_book(book, provider)
        assert book.mode == "llm"

    def test_enrich_pii_mode_becomes_full(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "pii"
        provider = MockProvider()

        enrich_book(book, provider)
        assert book.mode == "full"


class TestLLMCompiledOutput:
    """Test LLM-enriched compiled output."""

    def test_compiled_output_has_semantic_summaries(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "llm"
        provider = MockProvider()
        enrich_book(book, provider)
        compile_book(book, tmp_path)

        users_file = (tmp_path / "schemas" / "default" / "auth_users.md").read_text()
        # Should have the LLM summary, not a mechanical one
        assert "authentication" in users_file.lower() or "account" in users_file.lower()

    def test_compiled_output_has_schema_narrative(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "llm"
        provider = MockProvider()
        enrich_book(book, provider)
        compile_book(book, tmp_path)

        manifest = (tmp_path / "schemas" / "default" / "_manifest.md").read_text()
        # Schema narrative should appear in manifest
        assert len(manifest) > 100

    def test_compiled_concepts_have_aliases(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "llm"
        provider = MockProvider()
        enrich_book(book, provider)
        compile_book(book, tmp_path)

        concepts = json.loads((tmp_path / "concepts.json").read_text())
        # Check that some concepts have aliases
        has_aliases = any(
            len(v.get("aliases", [])) > 0
            for v in concepts.values()
        )
        assert has_aliases, "Some concepts should have LLM-generated aliases"

    def test_column_purposes_in_table_file(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "llm"
        provider = MockProvider()
        enrich_book(book, provider)
        compile_book(book, tmp_path)

        users_file = (tmp_path / "schemas" / "default" / "auth_users.md").read_text()
        # Column purposes should appear in the Comment column of the table
        # MockProvider returns purposes like "User's email address, used for login and notifications"
        assert "login" in users_file.lower() or "notification" in users_file.lower() or "identifier" in users_file.lower()


class TestLLMBenchmark:
    """Benchmark LLM-enriched output vs base mode."""

    def test_q6_revenue_query_improved(self, db_engine, tmp_path):
        """Q6: Best way to query revenue — should improve with LLM summaries."""
        from tests.benchmark_helpers import AgentSimulator

        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "llm"
        provider = MockProvider()
        enrich_book(book, provider)
        compile_book(book, tmp_path)

        agent = AgentSimulator(tmp_path)

        # With LLM summaries, concepts should have aliases
        concepts_raw = agent.read_file("concepts.json")
        concepts = json.loads(concepts_raw)

        # "revenue" should be findable via aliases like "income", "sales", "earnings"
        revenue_found = "revenue" in concepts
        # Or via alias search
        if not revenue_found:
            for term, data in concepts.items():
                if "revenue" in data.get("aliases", []) or "income" in data.get("aliases", []):
                    revenue_found = True
                    break

        assert revenue_found, "Revenue concept should be findable in LLM-enriched concepts"

    def test_concept_aliases_improve_discovery(self, db_engine, tmp_path):
        """Concept aliases should make more terms discoverable."""
        catalog = SQLAlchemyCatalog(db_engine)

        # Base mode
        book_base = catalog.introspect_all()
        compile_book(book_base, tmp_path / "base")
        base_concepts = json.loads((tmp_path / "base" / "concepts.json").read_text())
        base_alias_count = sum(len(v.get("aliases", [])) for v in base_concepts.values())

        # LLM mode
        book_llm = catalog.introspect_all()
        book_llm.mode = "llm"
        provider = MockProvider()
        enrich_book(book_llm, provider)
        compile_book(book_llm, tmp_path / "llm")
        llm_concepts = json.loads((tmp_path / "llm" / "concepts.json").read_text())
        llm_alias_count = sum(len(v.get("aliases", [])) for v in llm_concepts.values())

        assert llm_alias_count > base_alias_count, (
            f"LLM mode should have more aliases: {llm_alias_count} vs {base_alias_count}"
        )

    def test_summaries_are_factually_correct(self, db_engine):
        """LLM summaries should not hallucinate columns or tables."""
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "llm"
        provider = MockProvider()
        enrich_book(book, provider)

        users = book.schemas["default"].tables["auth_users"]
        summary = users.summary.lower()

        # Should mention relevant concepts
        assert "user" in summary or "account" in summary or "auth" in summary

        # Should NOT mention tables that don't exist
        assert "nonexistent_table" not in summary

    def test_all_base_benchmarks_still_pass(self, db_engine, tmp_path):
        """All base-mode benchmark queries should still work with LLM enrichment."""
        from tests.benchmark_helpers import AgentSimulator

        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "llm"
        provider = MockProvider()
        enrich_book(book, provider)
        compile_book(book, tmp_path)

        # Q1: Find email
        agent = AgentSimulator(tmp_path)
        concepts_raw = agent.read_file("concepts.json")
        concepts = json.loads(concepts_raw)
        assert "email" in concepts

        # Q7: Referenced by
        agent.reset()
        users = agent.read_file("schemas/default/auth_users.md")
        assert "Referenced By" in users

        # Q10: Checksums
        agent.reset()
        checksums = json.loads(agent.read_file("checksums.json"))
        assert len(checksums) == 13

    def test_full_benchmark_report(self, db_engine, tmp_path):
        """Generate benchmark report for LLM mode."""
        from tests.benchmark_helpers import AgentSimulator, BenchmarkReport, BenchmarkResult, count_tokens

        catalog = SQLAlchemyCatalog(db_engine)

        # Baseline
        from sqlalchemy import text  # type: ignore[import-untyped]
        with db_engine.connect() as conn:
            result = conn.execute(text(
                "SELECT sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL"
            ))
            all_ddl = "\n\n".join(row[0] for row in result)
        baseline = count_tokens(all_ddl)

        # Compile with LLM
        book = catalog.introspect_all()
        book.mode = "llm"
        provider = MockProvider()
        enrich_book(book, provider)
        compile_book(book, tmp_path)

        report = BenchmarkReport(
            phase="Phase 5 — LLM Enrichment",
            mode="llm",
            baseline_tokens=baseline,
        )

        # Q1
        agent = AgentSimulator(tmp_path)
        agent.read_file("NAVIGATION.md")
        raw = agent.read_file("concepts.json")
        c = json.loads(raw)
        found = "email" in c
        if found and c["email"]["tables"]:
            agent.read_file(c["email"]["tables"][0])
        report.results.append(BenchmarkResult(
            question_id="Q1", question="Where is user email?",
            expected_answer=["email"], files_read=list(agent.files_read),
            tokens_consumed=agent.tokens_consumed, answer_found=found,
        ))

        # Q6 (revenue)
        agent.reset()
        agent.read_file("NAVIGATION.md")
        raw = agent.read_file("concepts.json")
        c = json.loads(raw)
        found_rev = "revenue" in c or any("revenue" in v.get("aliases", []) for v in c.values())
        report.results.append(BenchmarkResult(
            question_id="Q6", question="Best way to query revenue?",
            expected_answer=["revenue", "daily_revenue"], files_read=list(agent.files_read),
            tokens_consumed=agent.tokens_consumed, answer_found=found_rev,
        ))

        print("\n" + report.summary())  # noqa: T201
        assert report.accuracy >= 0.85


class TestBaseVsLLMComparison:
    """Side-by-side benchmark: base mode vs LLM-enriched mode on same questions."""

    def test_three_way_comparison(self, db_engine, tmp_path):
        """Compare: No dbook (raw DDL) vs Base dbook vs LLM dbook."""
        from tests.benchmark_helpers import AgentSimulator, count_tokens

        catalog = SQLAlchemyCatalog(db_engine)

        # --- NO DBOOK: Raw DDL baseline ---
        from sqlalchemy import text as sa_text  # type: ignore[import-untyped]
        with db_engine.connect() as conn:
            result = conn.execute(sa_text(
                "SELECT sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL"
            ))
            all_ddl = "\n\n".join(row[0] for row in result)
        no_dbook_tokens = count_tokens(all_ddl)

        # --- BASE DBOOK ---
        book_base = catalog.introspect_all()
        base_dir = tmp_path / "base"
        compile_book(book_base, base_dir)

        # --- LLM DBOOK ---
        book_llm = catalog.introspect_all()
        book_llm.mode = "llm"
        provider = MockProvider()
        enrich_book(book_llm, provider)
        llm_dir = tmp_path / "llm"
        compile_book(book_llm, llm_dir)

        # Define questions
        questions = [
            ("Q1", "Where is user email stored?", self._q1),
            ("Q3", "What tables contain financial data?", self._q3),
            ("Q6", "Best way to query revenue by date?", self._q6),
            ("Q7", "Which tables reference users?", self._q7),
            ("Q9", "Find timestamp columns", self._q9),
            ("Q10", "What changed since compile?", self._q10),
        ]

        # Run against both dbook modes
        base_results = []
        llm_results = []

        for qid, question, runner in questions:
            # Base dbook
            agent_base = AgentSimulator(base_dir)
            found_base = runner(agent_base, base_dir)
            base_results.append((qid, agent_base.tokens_consumed, len(agent_base.files_read), found_base))

            # LLM dbook
            agent_llm = AgentSimulator(llm_dir)
            found_llm = runner(agent_llm, llm_dir)
            llm_results.append((qid, agent_llm.tokens_consumed, len(agent_llm.files_read), found_llm))

        # Concept alias comparison
        base_concepts = json.loads((base_dir / "concepts.json").read_text())
        llm_concepts = json.loads((llm_dir / "concepts.json").read_text())
        base_aliases = sum(len(v.get("aliases", [])) for v in base_concepts.values())
        llm_aliases = sum(len(v.get("aliases", [])) for v in llm_concepts.values())

        # Print three-way comparison
        print("\n" + "=" * 72)  # noqa: T201
        print("THREE-WAY COMPARISON: No dbook vs Base dbook vs LLM dbook")  # noqa: T201
        print("=" * 72)  # noqa: T201
        print(f"\nNo dbook baseline: {no_dbook_tokens} tokens (agent reads ALL raw DDL)")  # noqa: T201
        print(f"\n{'Question':<8} {'No dbook':>10} {'Base':>10} {'LLM':>10} {'Base Save':>10} {'LLM Save':>10} {'Base OK':>8} {'LLM OK':>8}")  # noqa: T201
        print("-" * 72)  # noqa: T201

        base_total = 0
        llm_total = 0
        base_correct = 0
        llm_correct = 0

        for (qid, b_tok, b_files, b_ok), (_, l_tok, l_files, l_ok) in zip(base_results, llm_results):
            b_save = f"{(1 - b_tok / no_dbook_tokens) * 100:.0f}%" if no_dbook_tokens > 0 else "N/A"
            l_save = f"{(1 - l_tok / no_dbook_tokens) * 100:.0f}%" if no_dbook_tokens > 0 else "N/A"
            print(f"{qid:<8} {no_dbook_tokens:>10} {b_tok:>10} {l_tok:>10} {b_save:>10} {l_save:>10} {'✓' if b_ok else '✗':>8} {'✓' if l_ok else '✗':>8}")  # noqa: T201
            base_total += b_tok
            llm_total += l_tok
            if b_ok: base_correct += 1
            if l_ok: llm_correct += 1

        n = len(questions)
        base_avg = base_total / n
        llm_avg = llm_total / n
        base_save_avg = (1 - base_avg / no_dbook_tokens) * 100 if no_dbook_tokens > 0 else 0
        llm_save_avg = (1 - llm_avg / no_dbook_tokens) * 100 if no_dbook_tokens > 0 else 0

        print("-" * 72)  # noqa: T201
        print(f"{'AVG':<8} {no_dbook_tokens:>10} {base_avg:>10.0f} {llm_avg:>10.0f} {base_save_avg:>9.0f}% {llm_save_avg:>9.0f}% {base_correct:>7}/{n} {llm_correct:>7}/{n}")  # noqa: T201
        print(f"\nConcept aliases: base={base_aliases}, llm={llm_aliases} (+{llm_aliases - base_aliases})")  # noqa: T201
        print("\nVERDICT:")  # noqa: T201
        print(f"  No dbook:   {no_dbook_tokens} tok/question (agent reads everything)")  # noqa: T201
        print(f"  Base dbook: {base_avg:.0f} tok/question ({base_save_avg:.0f}% savings, {base_correct}/{n} accurate)")  # noqa: T201
        print(f"  LLM dbook:  {llm_avg:.0f} tok/question ({llm_save_avg:.0f}% savings, {llm_correct}/{n} accurate, +{llm_aliases} aliases)")  # noqa: T201

        # Assertions
        assert llm_correct >= base_correct, "LLM mode should be at least as accurate"
        assert llm_aliases > base_aliases, "LLM mode should have more concept aliases"

    def _q1(self, agent, path):
        agent.read_file("NAVIGATION.md")
        raw = agent.read_file("concepts.json")
        c = json.loads(raw)
        if "email" not in c:
            return False
        tables = c["email"]["tables"]
        if tables:
            agent.read_file(tables[0])
        return True

    def _q3(self, agent, path):
        agent.read_file("NAVIGATION.md")
        raw = agent.read_file("concepts.json")
        c = json.loads(raw)
        terms = [t for t in ["payment", "invoice", "billing", "order", "price"] if t in c]
        return len(terms) >= 3

    def _q6(self, agent, path):
        agent.read_file("NAVIGATION.md")
        raw = agent.read_file("concepts.json")
        c = json.loads(raw)
        # Check direct term or aliases
        if "revenue" in c:
            return True
        for term, data in c.items():
            if any(a in ["revenue", "income", "sales", "earnings"] for a in data.get("aliases", [])):
                return True
        return False

    def _q7(self, agent, path):
        content = agent.read_file("schemas/default/auth_users.md")
        return "Referenced By" in content

    def _q9(self, agent, path):
        raw = agent.read_file("concepts.json")
        c = json.loads(raw)
        return any(t in c for t in ["created", "updated", "date", "time"])

    def _q10(self, agent, path):
        raw = agent.read_file("checksums.json")
        checksums = json.loads(raw)
        return len(checksums) == 13
