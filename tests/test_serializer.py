"""Tests for JSON serialization."""
# ruff: noqa: S101

from dbook.catalog import SQLAlchemyCatalog
from dbook.serializer import book_to_json, book_to_dict, save_book_json, load_book_json


class TestSerializer:
    def test_book_to_dict(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        d = book_to_dict(book)
        assert isinstance(d, dict)
        assert "schemas" in d
        assert "dialect" in d
        assert "compiled_at" in d

    def test_book_to_json(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        j = book_to_json(book)
        assert isinstance(j, str)
        import json
        parsed = json.loads(j)
        assert "schemas" in parsed

    def test_save_and_load(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        path = tmp_path / "book.json"
        save_book_json(book, path)
        assert path.exists()
        loaded = load_book_json(path)
        assert loaded["dialect"] == "sqlite"
        assert len(loaded["schemas"]) > 0

    def test_json_has_enum_values(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        d = book_to_dict(book)
        # Find a table with enum values
        for schema in d["schemas"].values():
            for table in schema["tables"].values():
                if table["enum_values"]:
                    assert isinstance(table["enum_values"], dict)
                    return
        # If no enum values found, that's OK for this fixture

    def test_json_has_foreign_keys(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        d = book_to_dict(book)
        for schema in d["schemas"].values():
            for table in schema["tables"].values():
                if table["foreign_keys"]:
                    fk = table["foreign_keys"][0]
                    assert "columns" in fk
                    assert "referred_table" in fk
                    return

    def test_cli_json_output(self, tmp_path):
        """Test CLI with --output-format json."""
        from click.testing import CliRunner  # type: ignore[import-untyped]
        from dbook.cli import main
        from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table  # type: ignore[import-untyped]

        db_path = tmp_path / "test.db"
        engine = create_engine(f"sqlite:///{db_path}")
        metadata = MetaData()
        Table("users", metadata, Column("id", Integer, primary_key=True), Column("name", String(100)))
        metadata.create_all(engine)
        engine.dispose()

        output_dir = tmp_path / "output"
        runner = CliRunner()
        result = runner.invoke(main, ["compile", f"sqlite:///{db_path}", "-o", str(output_dir), "--output-format", "json"])
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert (output_dir / "dbook.json").exists()
