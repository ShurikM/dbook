# ruff: noqa: S101
"""Phase 4 E2E tests — PII detection and sample data redaction."""

from __future__ import annotations

from dbook.catalog import SQLAlchemyCatalog
from dbook.compiler import compile_book
from dbook.pii.scanner import scan_book
from dbook.pii.patterns import detect_pii_by_column_name


class TestColumnNameDetection:
    """Test PII detection from column names (no Presidio needed)."""

    def test_detects_email(self):
        pii_type, confidence, sensitivity = detect_pii_by_column_name("email")
        assert pii_type == "EMAIL"
        assert confidence >= 0.8
        assert sensitivity in ("high", "critical")

    def test_detects_contact_email(self):
        pii_type, _, _ = detect_pii_by_column_name("contact_email")
        assert pii_type == "EMAIL"

    def test_detects_phone(self):
        pii_type, _, _ = detect_pii_by_column_name("phone")
        assert pii_type == "PHONE"

    def test_detects_ip_address(self):
        pii_type, _, sensitivity = detect_pii_by_column_name("ip_address")
        assert pii_type == "IP_ADDRESS"
        assert sensitivity == "medium"

    def test_detects_card_last_four(self):
        pii_type, _, _ = detect_pii_by_column_name("card_last_four")
        assert pii_type == "CREDIT_CARD_PARTIAL"

    def test_detects_ssn(self):
        pii_type, _, sensitivity = detect_pii_by_column_name("ssn")
        assert pii_type == "SSN"
        assert sensitivity == "critical"

    def test_detects_name(self):
        pii_type, _, _ = detect_pii_by_column_name("name")
        assert pii_type == "PERSON"

    def test_no_false_positive_id(self):
        pii_type, _, _ = detect_pii_by_column_name("id")
        assert pii_type is None

    def test_no_false_positive_created_at(self):
        pii_type, _, _ = detect_pii_by_column_name("created_at")
        assert pii_type is None

    def test_no_false_positive_status(self):
        pii_type, _, _ = detect_pii_by_column_name("status")
        assert pii_type is None

    def test_no_false_positive_amount(self):
        pii_type, _, _ = detect_pii_by_column_name("amount")
        assert pii_type is None


class TestPIIScanning:
    """Test PII scanning on actual database metadata."""

    def test_scan_detects_pii_in_users_table(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        scan_book(book, use_presidio=False)  # Column name patterns only

        users = book.schemas["default"].tables["auth_users"]
        pii_cols = {col.name: col for col in users.columns if col.pii_type}

        assert "email" in pii_cols
        assert pii_cols["email"].pii_type == "EMAIL"
        assert "name" in pii_cols
        assert pii_cols["name"].pii_type == "PERSON"

    def test_scan_detects_pii_in_payments(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        scan_book(book, use_presidio=False)

        payments = book.schemas["default"].tables["billing_payments"]
        pii_cols = {col.name: col for col in payments.columns if col.pii_type}

        assert "card_last_four" in pii_cols

    def test_scan_detects_pii_in_sessions(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        scan_book(book, use_presidio=False)

        sessions = book.schemas["default"].tables["auth_sessions"]
        pii_cols = {col.name: col for col in sessions.columns if col.pii_type}

        assert "ip_address" in pii_cols

    def test_total_pii_columns_detected(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        scan_book(book, use_presidio=False)

        total_pii = sum(
            1 for s in book.schemas.values()
            for t in s.tables.values()
            for c in t.columns
            if c.pii_type
        )
        # Expected: email, name, phone (users) + ip_address, user_agent (sessions) +
        # contact_email (invoices) + card_last_four (payments) + ip_address, user_agent (events)
        assert total_pii >= 7, f"Only detected {total_pii} PII columns, expected >= 7"


class TestSampleDataRedaction:
    """Test that sample data is properly redacted."""

    def test_email_redacted_in_samples(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        scan_book(book, use_presidio=False)

        users = book.schemas["default"].tables["auth_users"]
        for row in users.sample_data:
            email_val = row.get("email", "")
            assert "[REDACTED:" in str(email_val), f"Email not redacted: {email_val}"

    def test_name_redacted_in_samples(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        scan_book(book, use_presidio=False)

        users = book.schemas["default"].tables["auth_users"]
        for row in users.sample_data:
            name_val = row.get("name", "")
            assert "[REDACTED:" in str(name_val), f"Name not redacted: {name_val}"

    def test_non_pii_columns_not_redacted(self, db_engine):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        scan_book(book, use_presidio=False)

        users = book.schemas["default"].tables["auth_users"]
        for row in users.sample_data:
            # id should NOT be redacted
            id_val = str(row.get("id", ""))
            assert "[REDACTED:" not in id_val, f"ID incorrectly redacted: {id_val}"
            # created_at should NOT be redacted
            created_val = str(row.get("created_at", ""))
            assert "[REDACTED:" not in created_val, f"created_at incorrectly redacted: {created_val}"

    def test_zero_pii_leakage_in_compiled_output(self, db_engine, tmp_path):
        """Verify no raw PII values appear in any compiled output file."""
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "pii"
        scan_book(book, use_presidio=False)
        compile_book(book, tmp_path)

        # Collect all known PII values from the original (pre-redaction) sample data
        # We can check that specific patterns don't appear
        for md_file in tmp_path.rglob("*.md"):
            content = md_file.read_text()
            # Sample emails follow pattern user{N}@example.com
            for i in range(1, 21):
                assert f"user{i}@example.com" not in content, (
                    f"PII leak in {md_file.name}: user{i}@example.com"
                )


class TestPIIInCompiledOutput:
    """Test PII markers appear correctly in compiled markdown."""

    def test_navigation_has_sensitivity_overview(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "pii"
        scan_book(book, use_presidio=False)
        compile_book(book, tmp_path)

        nav = (tmp_path / "NAVIGATION.md").read_text()
        assert "PII detected" in nav, "NAVIGATION.md should have PII summary line"

    def test_table_file_shows_pii_columns(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "pii"
        scan_book(book, use_presidio=False)
        compile_book(book, tmp_path)

        users_file = (tmp_path / "schemas" / "default" / "auth_users.md").read_text()
        assert "PII" in users_file, "Table file should show PII column"
        assert "EMAIL" in users_file, "Should show EMAIL PII type"

    def test_sample_data_shows_redacted_in_output(self, db_engine, tmp_path):
        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "pii"
        scan_book(book, use_presidio=False)
        compile_book(book, tmp_path)

        users_file = (tmp_path / "schemas" / "default" / "auth_users.md").read_text()
        assert "[REDACTED:" in users_file, "Sample data should show [REDACTED:TYPE]"

    def test_q5_pii_in_billing_now_answerable(self, db_engine, tmp_path):
        """Q5: Is there PII in the billing schema? Should be answerable with PII mode."""
        from tests.benchmark_helpers import AgentSimulator

        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        book.mode = "pii"
        scan_book(book, use_presidio=False)
        compile_book(book, tmp_path)

        agent = AgentSimulator(tmp_path)
        nav = agent.read_file("NAVIGATION.md")

        # Q5 should be answerable from the PII summary line in NAVIGATION.md
        # or from reading billing table files
        has_sensitivity_info = "PII detected" in nav

        if not has_sensitivity_info:
            # Read billing tables directly
            invoices = agent.read_file("schemas/default/billing_invoices.md")
            payments = agent.read_file("schemas/default/billing_payments.md")
            has_sensitivity_info = "EMAIL" in invoices or "CREDIT_CARD" in payments

        assert has_sensitivity_info, "Agent should be able to find PII info in billing schema"


class TestPIIBenchmark:
    """Benchmark PII detection rates."""

    def test_detection_recall(self, db_engine):
        """PII detection recall >= 90%."""
        known_pii_columns = {
            "auth_users": ["email", "name", "phone"],
            "auth_sessions": ["ip_address", "user_agent"],
            "billing_invoices": ["contact_email"],
            "billing_payments": ["card_last_four"],
            "analytics_events": ["ip_address", "user_agent"],
        }

        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        scan_book(book, use_presidio=False)

        total_expected = sum(len(cols) for cols in known_pii_columns.values())
        detected = 0

        for table_name, expected_cols in known_pii_columns.items():
            table = book.schemas["default"].tables[table_name]
            pii_col_names = {col.name for col in table.columns if col.pii_type}
            for expected in expected_cols:
                if expected in pii_col_names:
                    detected += 1

        recall = detected / total_expected
        assert recall >= 0.90, (
            f"PII detection recall {recall:.0%} < 90% ({detected}/{total_expected})"
        )

    def test_false_positive_rate(self, db_engine):
        """False positive rate < 10%."""
        known_non_pii = [
            "id", "created_at", "updated_at", "status", "total", "amount",
            "quantity", "price", "is_active", "description", "category",
            "event_type", "page", "order_count", "conversion_rate",
        ]

        catalog = SQLAlchemyCatalog(db_engine)
        book = catalog.introspect_all()
        scan_book(book, use_presidio=False)

        false_positives = 0
        checked = 0
        for s in book.schemas.values():
            for t in s.tables.values():
                for col in t.columns:
                    if col.name in known_non_pii:
                        checked += 1
                        if col.pii_type:
                            false_positives += 1

        if checked > 0:
            fp_rate = false_positives / checked
            assert fp_rate < 0.10, (
                f"False positive rate {fp_rate:.0%} >= 10% ({false_positives}/{checked})"
            )

    def test_token_overhead_under_15_percent(self, db_engine, tmp_path):
        """PII mode token overhead < 15% vs base mode."""
        from tests.benchmark_helpers import count_tokens

        catalog = SQLAlchemyCatalog(db_engine)

        # Base mode
        book_base = catalog.introspect_all()
        base_dir = tmp_path / "base"
        compile_book(book_base, base_dir)
        base_tokens = sum(
            count_tokens(f.read_text()) for f in base_dir.rglob("*.md")
        )

        # PII mode
        book_pii = catalog.introspect_all()
        book_pii.mode = "pii"
        scan_book(book_pii, use_presidio=False)
        pii_dir = tmp_path / "pii"
        compile_book(book_pii, pii_dir)
        pii_tokens = sum(
            count_tokens(f.read_text()) for f in pii_dir.rglob("*.md")
        )

        overhead = (pii_tokens - base_tokens) / base_tokens * 100 if base_tokens > 0 else 0
        assert overhead < 15, f"PII mode overhead {overhead:.1f}% >= 15%"
