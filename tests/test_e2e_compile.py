# ruff: noqa: S101
"""Phase 2 E2E tests — compile and validate output structure."""

from __future__ import annotations

import json

from dbook.catalog import SQLAlchemyCatalog
from dbook.compiler import compile_book


class TestCompileOutput:
    """Verify compiled output has correct structure and content."""

    def test_compile_creates_navigation(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        compile_book(book, tmp_path)

        nav = tmp_path / "NAVIGATION.md"
        assert nav.exists()
        content = nav.read_text()
        assert "# Database Book:" in content
        assert "## Schemas" in content
        assert "## How to Navigate" in content

    def test_compile_embeds_concepts_in_navigation(self, db_engine, tmp_path):
        """Small DB (<20 tables): concepts embedded in NAVIGATION.md, no concepts.json."""
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        compile_book(book, tmp_path)

        # concepts.json should NOT exist for a 13-table DB
        concepts_file = tmp_path / "concepts.json"
        assert not concepts_file.exists(), (
            "concepts.json should not be generated for small DBs (<20 tables)"
        )

        # Instead, concepts should be embedded in NAVIGATION.md
        nav_content = (tmp_path / "NAVIGATION.md").read_text()
        assert "## Quick Lookup" in nav_content
        # "email" concept should appear in the Quick Lookup table
        assert "email" in nav_content.lower()

    def test_compile_creates_checksums_json(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        compile_book(book, tmp_path)

        checksums_file = tmp_path / "checksums.json"
        assert checksums_file.exists()
        checksums = json.loads(checksums_file.read_text())
        assert len(checksums) == 13  # 13 tables

    def test_compile_creates_schema_dirs(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        compile_book(book, tmp_path)

        schemas_dir = tmp_path / "schemas"
        assert schemas_dir.exists()
        # SQLite puts everything in "default" schema
        default_dir = schemas_dir / "default"
        assert default_dir.exists()

    def test_compile_creates_manifest(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        compile_book(book, tmp_path)

        manifest = tmp_path / "schemas" / "default" / "_manifest.md"
        assert manifest.exists()
        content = manifest.read_text()
        assert "## Tables" in content
        assert "auth_users" in content

    def test_compile_creates_table_files(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        compile_book(book, tmp_path)

        schema_dir = tmp_path / "schemas" / "default"
        expected_tables = [
            "auth_users",
            "auth_sessions",
            "auth_roles",
            "auth_user_roles",
            "billing_products",
            "billing_discounts",
            "billing_orders",
            "billing_order_items",
            "billing_invoices",
            "billing_payments",
            "analytics_events",
            "analytics_daily_revenue",
            "analytics_funnels",
        ]
        for table in expected_tables:
            md = schema_dir / f"{table}.md"
            assert md.exists(), f"Missing {table}.md"

    def test_table_file_has_columns(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        compile_book(book, tmp_path)

        users_file = tmp_path / "schemas" / "default" / "auth_users.md"
        content = users_file.read_text()
        assert "## Columns" in content
        assert "email" in content
        upper = content.upper()
        assert "VARCHAR" in upper or "STRING" in upper or "TEXT" in upper

    def test_table_file_has_foreign_keys(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        compile_book(book, tmp_path)

        orders_file = tmp_path / "schemas" / "default" / "billing_orders.md"
        content = orders_file.read_text()
        assert "## Foreign Keys" in content
        assert "auth_users" in content

    def test_table_file_has_indexes(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        compile_book(book, tmp_path)

        orders_file = tmp_path / "schemas" / "default" / "billing_orders.md"
        content = orders_file.read_text()
        assert "## Indexes" in content

    def test_table_file_has_sample_data(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        compile_book(book, tmp_path)

        users_file = tmp_path / "schemas" / "default" / "auth_users.md"
        content = users_file.read_text()
        assert "## Sample Data" in content

    def test_table_file_has_referenced_by(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        compile_book(book, tmp_path)

        users_file = tmp_path / "schemas" / "default" / "auth_users.md"
        content = users_file.read_text()
        assert "## Referenced By" in content
        # auth_users is referenced by sessions, user_roles, orders, events
        assert "auth_sessions" in content or "auth_user_roles" in content

    def test_compile_returns_summary(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        result = compile_book(book, tmp_path)

        assert result["tables"] == 13
        assert result["schemas"] == 1  # "default" for SQLite
        # 2 root (NAVIGATION.md + checksums.json) + 1 manifest + 13 tables = 16
        # (no concepts.json for small DB)
        assert result["files_written"] >= 15

    def test_navigation_under_700_tokens(self, db_engine, tmp_path):
        """NAVIGATION.md with embedded Quick Lookup stays compact."""
        from tests.benchmark_helpers import count_tokens

        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        compile_book(book, tmp_path)

        nav = tmp_path / "NAVIGATION.md"
        tokens = count_tokens(nav.read_text())
        # With Quick Lookup embedded (top 20 terms), budget is higher but still compact
        assert tokens < 700, (
            f"NAVIGATION.md is {tokens} tokens, should be < 700"
        )
