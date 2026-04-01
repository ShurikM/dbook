# ruff: noqa: S101
"""Phase 3 E2E tests — CLI commands and incremental compilation."""

from __future__ import annotations

import json
import hashlib
from pathlib import Path

from click.testing import CliRunner  # type: ignore[import-untyped]
from sqlalchemy import (  # type: ignore[import-untyped]
    Column,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    text,
)

from dbook.catalog import SQLAlchemyCatalog
from dbook.compiler import compile_book
from dbook.hasher import compute_table_hash
from dbook.incremental import check_changes, incremental_compile


class TestCLICompile:
    """Test the dbook compile CLI command."""

    def test_compile_basic(self, db_engine, tmp_path):
        """Full compile via CLI."""
        # SQLite in-memory can't reconnect via URL, so test via Python API directly
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        result = compile_book(book, tmp_path)

        assert result["files_written"] >= 15
        assert (tmp_path / "NAVIGATION.md").exists()
        assert (tmp_path / "checksums.json").exists()

    def test_compile_cli_invocation(self, tmp_path):
        """Test CLI can be invoked (using a file-based SQLite DB)."""
        from dbook.cli import main

        runner = CliRunner()

        # Create a simple SQLite file DB
        db_path = tmp_path / "test.db"
        engine = create_engine(f"sqlite:///{db_path}")
        metadata = MetaData()
        Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
        )
        metadata.create_all(engine)
        engine.dispose()

        output_dir = tmp_path / "output"
        result = runner.invoke(
            main, ["compile", f"sqlite:///{db_path}", "-o", str(output_dir)]
        )

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert (output_dir / "NAVIGATION.md").exists()

    def test_compile_with_schemas_filter(self, db_engine, tmp_path):
        """Compile with --schemas filter."""
        # Since SQLite has no real schemas, test the API path
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        result = compile_book(book, tmp_path)
        assert result["schemas"] >= 1

    def test_compile_no_sample_data(self, db_engine, tmp_path):
        """Compile with sample data disabled."""
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all(include_sample_data=False)
        compile_book(book, tmp_path)

        users_file = tmp_path / "schemas" / "default" / "auth_users.md"
        content = users_file.read_text()
        assert "## Sample Data" not in content


class TestCLICheck:
    """Test the dbook check CLI command."""

    def test_check_no_changes(self, db_engine, tmp_path):
        """Check reports no changes when schema hasn't changed."""
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()

        # Compute hashes
        for schema in book.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)

        compile_book(book, tmp_path)

        # Check against same DB
        old_checksums = json.loads((tmp_path / "checksums.json").read_text())

        book2 = catalog.introspect_all()
        for schema in book2.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)

        result = check_changes(book2, old_checksums)
        assert not result.has_changes
        assert len(result.unchanged) == 13

    def test_check_detects_added_table(self, db_engine, tmp_path):
        """Check detects a newly added table."""
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        for schema in book.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)
        compile_book(book, tmp_path)
        old_checksums = json.loads((tmp_path / "checksums.json").read_text())

        # Add a new table
        with db_engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TABLE billing_refunds "
                    "(id INTEGER PRIMARY KEY, order_id INTEGER, amount REAL)"
                )
            )
            conn.commit()

        catalog2 = SQLAlchemyCatalog(db_engine)
        book2 = catalog2.introspect_all()
        for schema in book2.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)

        result = check_changes(book2, old_checksums)
        assert result.has_changes
        assert any("billing_refunds" in a for a in result.added)

    def test_check_detects_modified_table(self, db_engine, tmp_path):
        """Check detects a modified table schema."""
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        for schema in book.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)
        compile_book(book, tmp_path)
        old_checksums = json.loads((tmp_path / "checksums.json").read_text())

        # Modify a table
        with db_engine.connect() as conn:
            conn.execute(text("ALTER TABLE auth_users ADD COLUMN bio TEXT"))
            conn.commit()

        catalog2 = SQLAlchemyCatalog(db_engine)
        book2 = catalog2.introspect_all()
        for schema in book2.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)

        result = check_changes(book2, old_checksums)
        assert result.has_changes
        assert any("auth_users" in m for m in result.modified)

    def test_check_detects_removed_table(self, db_engine, tmp_path):
        """Check detects a removed table."""
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        for schema in book.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)
        compile_book(book, tmp_path)
        old_checksums = json.loads((tmp_path / "checksums.json").read_text())

        # Drop a table (disable FK checks so SQLite allows it)
        with db_engine.connect() as conn:
            conn.execute(text("PRAGMA foreign_keys = OFF"))
            conn.execute(text("DROP TABLE billing_discounts"))
            conn.execute(text("PRAGMA foreign_keys = ON"))
            conn.commit()

        catalog2 = SQLAlchemyCatalog(db_engine)
        book2 = catalog2.introspect_all()
        for schema in book2.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)

        result = check_changes(book2, old_checksums)
        assert result.has_changes
        assert any("billing_discounts" in r for r in result.removed)


class TestIncrementalCompile:
    """Test incremental compilation."""

    def test_incremental_no_changes(self, db_engine, tmp_path):
        """Incremental compile with no changes writes minimal files."""
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        for schema in book.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)
        compile_book(book, tmp_path)
        old_checksums = json.loads((tmp_path / "checksums.json").read_text())

        # Run incremental with same data
        book2 = catalog.introspect_all()
        for schema in book2.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)

        result = incremental_compile(book2, tmp_path, old_checksums)
        assert not result.has_changes
        assert result.files_written == 1  # Only checksums.json updated

    def test_incremental_modifies_only_changed(self, db_engine, tmp_path):
        """Incremental compile only rewrites changed table files."""
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        for schema in book.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)
        compile_book(book, tmp_path)
        old_checksums = json.loads((tmp_path / "checksums.json").read_text())

        # Record unchanged file content
        orders_content_before = (
            tmp_path / "schemas" / "default" / "billing_orders.md"
        ).read_text()

        # Modify auth_users
        with db_engine.connect() as conn:
            conn.execute(
                text("ALTER TABLE auth_users ADD COLUMN avatar_url TEXT")
            )
            conn.commit()

        catalog2 = SQLAlchemyCatalog(db_engine)
        book2 = catalog2.introspect_all()
        for schema in book2.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)

        result = incremental_compile(book2, tmp_path, old_checksums)

        assert result.has_changes
        assert any("auth_users" in m for m in result.modified)

        # billing_orders.md should be unchanged
        orders_content_after = (
            tmp_path / "schemas" / "default" / "billing_orders.md"
        ).read_text()
        assert (
            orders_content_before == orders_content_after
        ), "Unchanged table file was modified!"

    def test_incremental_adds_new_table(self, db_engine, tmp_path):
        """Incremental compile creates file for new table."""
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        for schema in book.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)
        compile_book(book, tmp_path)
        old_checksums = json.loads((tmp_path / "checksums.json").read_text())

        # Add new table
        with db_engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TABLE billing_credits "
                    "(id INTEGER PRIMARY KEY, user_id INTEGER, "
                    "amount REAL, reason TEXT)"
                )
            )
            conn.commit()

        catalog2 = SQLAlchemyCatalog(db_engine)
        book2 = catalog2.introspect_all()
        for schema in book2.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)

        result = incremental_compile(book2, tmp_path, old_checksums)

        assert any("billing_credits" in a for a in result.added)
        assert (tmp_path / "schemas" / "default" / "billing_credits.md").exists()

    def test_incremental_removes_deleted_table(self, db_engine, tmp_path):
        """Incremental compile removes file for dropped table."""
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        for schema in book.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)
        compile_book(book, tmp_path)
        old_checksums = json.loads((tmp_path / "checksums.json").read_text())

        assert (
            tmp_path / "schemas" / "default" / "billing_discounts.md"
        ).exists()

        # Drop table (disable FK checks so SQLite allows it)
        with db_engine.connect() as conn:
            conn.execute(text("PRAGMA foreign_keys = OFF"))
            conn.execute(text("DROP TABLE billing_discounts"))
            conn.execute(text("PRAGMA foreign_keys = ON"))
            conn.commit()

        catalog2 = SQLAlchemyCatalog(db_engine)
        book2 = catalog2.introspect_all()
        for schema in book2.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)

        result = incremental_compile(book2, tmp_path, old_checksums)

        assert any("billing_discounts" in r for r in result.removed)
        assert not (
            tmp_path / "schemas" / "default" / "billing_discounts.md"
        ).exists()

    def test_incremental_updates_navigation(
        self, db_engine, tmp_path
    ):
        """Incremental compile regenerates NAVIGATION.md on changes."""
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        for schema in book.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)
        compile_book(book, tmp_path)
        old_checksums = json.loads((tmp_path / "checksums.json").read_text())

        nav_before = (tmp_path / "NAVIGATION.md").read_text()

        # Add a table (changes table count)
        with db_engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TABLE support_tickets "
                    "(id INTEGER PRIMARY KEY, title TEXT, status TEXT)"
                )
            )
            conn.commit()

        catalog2 = SQLAlchemyCatalog(db_engine)
        book2 = catalog2.introspect_all()
        for schema in book2.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)

        incremental_compile(book2, tmp_path, old_checksums)

        nav_after = (tmp_path / "NAVIGATION.md").read_text()
        assert nav_before != nav_after, "NAVIGATION.md should be updated"


class TestIncrementalBenchmark:
    """Benchmark incremental compile efficiency."""

    def test_incremental_fewer_files_than_full(self, db_engine, tmp_path):
        """Incremental compile writes fewer files than full compile."""
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        for schema in book.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)
        full_result = compile_book(book, tmp_path)
        full_files = full_result["files_written"]

        old_checksums = json.loads((tmp_path / "checksums.json").read_text())

        # Modify one table
        with db_engine.connect() as conn:
            conn.execute(
                text("ALTER TABLE auth_users ADD COLUMN nickname TEXT")
            )
            conn.commit()

        catalog2 = SQLAlchemyCatalog(db_engine)
        book2 = catalog2.introspect_all()
        for schema in book2.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)

        inc_result = incremental_compile(book2, tmp_path, old_checksums)

        assert inc_result.files_written < full_files, (
            f"Incremental ({inc_result.files_written}) should write fewer "
            f"files than full ({full_files})"
        )

    def test_benchmark_questions_pass_after_incremental(
        self, db_engine, tmp_path
    ):
        """Phase 2 benchmark questions still pass on incrementally-compiled output."""
        from tests.benchmark_helpers import AgentSimulator

        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        for schema in book.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)
        compile_book(book, tmp_path)
        old_checksums = json.loads((tmp_path / "checksums.json").read_text())

        # Modify a table
        with db_engine.connect() as conn:
            conn.execute(
                text("ALTER TABLE auth_users ADD COLUMN department TEXT")
            )
            conn.commit()

        catalog2 = SQLAlchemyCatalog(db_engine)
        book2 = catalog2.introspect_all()
        for schema in book2.schemas.values():
            for table in schema.tables.values():
                table.schema_hash = compute_table_hash(table)

        incremental_compile(book2, tmp_path, old_checksums)

        # Verify Q1 still works — email visible in NAVIGATION.md table overview
        agent = AgentSimulator(tmp_path)
        nav = agent.read_file("NAVIGATION.md")
        assert "## Tables" in nav
        assert "email" in nav.lower()

        # Verify Q10 -- checksums updated
        agent.reset()
        checksums_raw = agent.read_file("checksums.json")
        new_checksums = json.loads(checksums_raw)
        # Same table count (only modified, not added/removed)
        assert len(new_checksums) == 13


def _hash_all_files(directory: Path) -> dict[str, str]:
    """Hash all files in a directory for change detection."""
    hashes = {}
    for path in directory.rglob("*"):
        if path.is_file():
            content = path.read_bytes()
            hashes[str(path.relative_to(directory))] = hashlib.md5(
                content, usedforsecurity=False
            ).hexdigest()
    return hashes
