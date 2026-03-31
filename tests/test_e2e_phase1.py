"""Phase 1 E2E tests — introspection completeness and hash stability."""

from __future__ import annotations

from dbook.catalog import SQLAlchemyCatalog
from dbook.hasher import compute_table_hash


class TestIntrospectionCompleteness:
    """Verify the catalog introspects all tables with full metadata."""

    def test_discovers_all_tables(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        tables = catalog.list_tables()
        expected = {
            "auth_users", "auth_sessions", "auth_roles", "auth_user_roles",
            "billing_products", "billing_discounts", "billing_orders",
            "billing_order_items", "billing_invoices", "billing_payments",
            "analytics_events", "analytics_daily_revenue", "analytics_funnels",
        }
        assert set(tables) == expected, f"Missing tables: {expected - set(tables)}"

    def test_column_extraction(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        table = catalog.introspect_table("auth_users")
        col_names = [c.name for c in table.columns]
        assert "id" in col_names
        assert "email" in col_names
        assert "name" in col_names
        assert "phone" in col_names
        assert "password_hash" in col_names
        assert "is_active" in col_names
        assert "created_at" in col_names

    def test_primary_key_detection(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        table = catalog.introspect_table("auth_users")
        assert "id" in table.primary_key

    def test_foreign_key_detection(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        table = catalog.introspect_table("billing_orders")
        fk_targets = [fk.referred_table for fk in table.foreign_keys]
        assert "auth_users" in fk_targets
        assert "billing_discounts" in fk_targets

    def test_index_detection(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        table = catalog.introspect_table("billing_orders")
        idx_names = [idx.name for idx in table.indexes if idx.name]
        assert any("user" in name for name in idx_names), f"No user index found in {idx_names}"

    def test_row_count(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        table = catalog.introspect_table("auth_users")
        assert table.row_count is not None
        assert table.row_count == 20

    def test_sample_data(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        table = catalog.introspect_table("auth_users", sample_limit=5)
        assert len(table.sample_data) == 5
        assert "email" in table.sample_data[0]

    def test_introspect_all(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        # SQLite has no real schemas — everything is in "default"
        assert "default" in book.schemas or None in book.schemas
        total_tables = sum(len(s.tables) for s in book.schemas.values())
        assert total_tables == 13


class TestHashStability:
    """Verify schema hashes are deterministic and change-sensitive."""

    def test_hash_is_deterministic(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        table1 = catalog.introspect_table("auth_users")
        table2 = catalog.introspect_table("auth_users")
        assert compute_table_hash(table1) == compute_table_hash(table2)

    def test_hash_changes_on_schema_change(self, db_engine):
        from sqlalchemy import text
        catalog = SQLAlchemyCatalog(db_engine)

        table_before = catalog.introspect_table("auth_users")
        hash_before = compute_table_hash(table_before)

        # Add a column
        with db_engine.connect() as conn:
            conn.execute(text("ALTER TABLE auth_users ADD COLUMN bio TEXT"))
            conn.commit()

        # Re-introspect (need fresh inspector)
        catalog2 = SQLAlchemyCatalog(db_engine)
        table_after = catalog2.introspect_table("auth_users")
        hash_after = compute_table_hash(table_after)

        assert hash_before != hash_after, "Hash should change after ALTER TABLE"

    def test_hash_ignores_data_changes(self, db_engine):
        from sqlalchemy import text
        catalog = SQLAlchemyCatalog(db_engine)

        table_before = catalog.introspect_table("auth_users")
        hash_before = compute_table_hash(table_before)

        # Insert data (schema unchanged)
        with db_engine.connect() as conn:
            conn.execute(text(
                "INSERT INTO auth_users (email, name, password_hash, is_active, created_at) "
                "VALUES ('new@test.com', 'New User', 'hash', 1, '2025-01-01')"
            ))
            conn.commit()

        table_after = catalog.introspect_table("auth_users")
        hash_after = compute_table_hash(table_after)

        assert hash_before == hash_after, "Hash should NOT change on data-only changes"
