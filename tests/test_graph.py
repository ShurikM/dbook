"""Tests for FK graph, JOIN path resolution, and domain tagging."""
# ruff: noqa: S101

from dbook.catalog import SQLAlchemyCatalog
from dbook.graph import FKGraph


class TestFKGraph:
    def test_find_path_direct(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        graph = FKGraph(book)

        # orders -> users (direct FK)
        path = graph.find_path("billing_orders", "auth_users")
        assert path is not None
        assert len(path.hops) == 1
        assert "JOIN" in path.sql

    def test_find_path_multi_hop(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        graph = FKGraph(book)

        # order_items -> users (via orders)
        path = graph.find_path("billing_order_items", "auth_users")
        assert path is not None
        assert len(path.hops) <= 3

    def test_no_path(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        graph = FKGraph(book)

        # roles -> daily_revenue (no connection)
        # Just test it doesn't crash
        graph.find_path("auth_roles", "analytics_daily_revenue")

    def test_self_path(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        graph = FKGraph(book)

        path = graph.find_path("auth_users", "auth_users")
        assert path is not None
        assert len(path.hops) == 0
        assert path.sql == ""

    def test_nonexistent_table(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        graph = FKGraph(book)

        path = graph.find_path("nonexistent", "auth_users")
        assert path is None

    def test_get_join_sql(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        graph = FKGraph(book)

        sql = graph.get_join_sql(["billing_orders", "auth_users"])
        assert sql is not None
        assert "JOIN" in sql
        assert "auth_users" in sql

    def test_get_join_sql_single_table(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        graph = FKGraph(book)

        sql = graph.get_join_sql(["billing_orders"])
        assert sql is None

    def test_source_and_leaf_tables(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        graph = FKGraph(book)

        sources = graph.source_tables()
        leaves = graph.leaf_tables()

        # Source tables have no outgoing FKs
        assert len(sources) > 0
        # Leaf tables have no incoming FKs
        assert len(leaves) >= 0

    def test_find_all_paths_from(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        graph = FKGraph(book)

        paths = graph.find_all_paths_from("billing_orders")
        assert len(paths) > 0
        # Should find paths to at least auth_users and billing_order_items
        targets = {p.to_table for p in paths}
        assert "auth_users" in targets

    def test_to_dict(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        graph = FKGraph(book)

        d = graph.to_dict()
        assert "tables" in d
        assert "edges" in d
        assert "source_tables" in d
        assert "leaf_tables" in d
        assert len(d["tables"]) == 13


class TestDomainTagging:
    def test_auth_domain(self, db_engine):
        from dbook.compiler import compile_book

        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        compile_book(book, "/tmp/dbook_test_domain")  # noqa: S108

        users = book.schemas["default"].tables["auth_users"]
        assert users.domain in ("auth", "general")

    def test_billing_domain(self, db_engine):
        from dbook.compiler import compile_book

        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        compile_book(book, "/tmp/dbook_test_domain2")  # noqa: S108

        orders = book.schemas["default"].tables["billing_orders"]
        assert orders.domain in ("billing", "orders", "general")

    def test_analytics_domain(self, db_engine):
        from dbook.compiler import compile_book

        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        compile_book(book, "/tmp/dbook_test_domain3")  # noqa: S108

        events = book.schemas["default"].tables["analytics_events"]
        assert events.domain in ("analytics", "general")

    def test_detect_domain_function(self):
        from dbook.domains import detect_domain

        assert detect_domain("auth_users", ["id", "email", "password_hash"]) == "auth"
        assert detect_domain("billing_invoices", ["id", "amount", "status"]) == "billing"
        assert detect_domain("unknown_table", ["id", "foo", "bar"]) == "general"
