# pyright: reportMissingImports=false
"""Database catalog abstraction with SQLAlchemy default implementation."""

from __future__ import annotations

import logging
from typing import Protocol, runtime_checkable

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

from dbook.models import (
    BookMeta,
    ColumnInfo,
    ForeignKeyInfo,
    IndexInfo,
    SchemaMeta,
    TableMeta,
)

logger = logging.getLogger(__name__)

_MASKED = "*" * 3


@runtime_checkable
class Catalog(Protocol):
    """Abstraction over database/catalog metadata sources."""

    def list_schemas(self) -> list[str | None]: ...
    def list_tables(self, schema: str | None = None) -> list[str]: ...
    def introspect_table(
        self,
        table_name: str,
        schema: str | None = None,
        include_sample_data: bool = True,
        sample_limit: int = 5,
        include_row_count: bool = True,
    ) -> TableMeta: ...


class SQLAlchemyCatalog:
    """Default Catalog implementation using SQLAlchemy Inspector."""

    def __init__(self, url_or_engine: str | Engine) -> None:
        if isinstance(url_or_engine, str):
            self.engine = create_engine(url_or_engine)
            self._url = url_or_engine
        else:
            self.engine = url_or_engine
            self._url = str(url_or_engine.url)
        self._inspector = inspect(self.engine)
        self.dialect = self.engine.dialect.name

    @property
    def sanitized_url(self) -> str:
        """URL with password masked."""
        parsed = make_url(self._url)
        if parsed.password:
            parsed = parsed.set(password=_MASKED)  # noqa: S106
        return str(parsed)

    def list_schemas(self) -> list[str | None]:
        """List available schemas."""
        try:
            schemas = self._inspector.get_schema_names()
            # Filter out internal schemas
            schemas = [s for s in schemas if s not in ("information_schema", "pg_catalog")]
            # SQLite reports "main" but has no real schema concept — normalize to None
            if self.dialect == "sqlite":
                return [None]
            return schemas
        except Exception:
            return [None]  # Fallback for dialects that don't support schema listing

    def list_tables(self, schema: str | None = None) -> list[str]:
        """List tables in a schema."""
        return self._inspector.get_table_names(schema=schema)

    def introspect_table(
        self,
        table_name: str,
        schema: str | None = None,
        include_sample_data: bool = True,
        sample_limit: int = 5,
        include_row_count: bool = True,
    ) -> TableMeta:
        """Introspect a single table and return its metadata."""
        # Columns
        raw_columns = self._inspector.get_columns(table_name, schema=schema)
        pk_constraint = self._inspector.get_pk_constraint(table_name, schema=schema)
        pk_cols = tuple(pk_constraint.get("constrained_columns", []))

        columns = []
        for col in raw_columns:
            columns.append(ColumnInfo(
                name=col["name"],
                type=str(col["type"]),
                nullable=col.get("nullable", True),
                default=str(col["default"]) if col.get("default") is not None else None,
                is_primary_key=col["name"] in pk_cols,
                comment=col.get("comment"),
            ))

        # Foreign keys
        raw_fks = self._inspector.get_foreign_keys(table_name, schema=schema)
        foreign_keys = []
        for fk in raw_fks:
            foreign_keys.append(ForeignKeyInfo(
                columns=tuple(fk["constrained_columns"]),
                referred_schema=fk.get("referred_schema"),
                referred_table=fk["referred_table"],
                referred_columns=tuple(fk["referred_columns"]),
                name=fk.get("name"),
            ))

        # Indexes
        raw_indexes = self._inspector.get_indexes(table_name, schema=schema)
        indexes = []
        for idx in raw_indexes:
            indexes.append(IndexInfo(
                name=idx.get("name"),
                columns=tuple(idx["column_names"]),
                unique=idx.get("unique", False),
            ))

        # Table comment
        try:
            comment = self._inspector.get_table_comment(table_name, schema=schema).get("text")
        except Exception:
            comment = None

        # Row count (dialect-specific)
        row_count = None
        if include_row_count:
            row_count = self._row_count(table_name, schema)

        # Sample data
        sample_data: list[dict[str, str | int | float | bool | None]] = []
        if include_sample_data:
            sample_data = self._sample_data(table_name, schema, sample_limit)

        # Enum values
        enum_vals = self._enum_values(table_name, schema, columns, row_count)

        return TableMeta(
            name=table_name,
            schema=schema,
            columns=columns,
            primary_key=pk_cols,
            foreign_keys=foreign_keys,
            indexes=indexes,
            row_count=row_count,
            comment=comment,
            sample_data=sample_data,
            enum_values=enum_vals,
        )

    def introspect_all(
        self,
        schemas: list[str | None] | None = None,
        include_sample_data: bool = True,
        sample_limit: int = 5,
        include_row_count: bool = True,
    ) -> BookMeta:
        """Introspect all tables across specified schemas."""
        resolved_schemas: list[str | None] = (
            self.list_schemas() if schemas is None else schemas
        )

        schema_metas: dict[str, SchemaMeta] = {}
        for schema in resolved_schemas:
            tables: dict[str, TableMeta] = {}
            for table_name in self.list_tables(schema=schema):
                try:
                    table_meta = self.introspect_table(
                        table_name,
                        schema=schema,
                        include_sample_data=include_sample_data,
                        sample_limit=sample_limit,
                        include_row_count=include_row_count,
                    )
                    tables[table_name] = table_meta
                except Exception as e:
                    logger.warning("Failed to introspect %s.%s: %s", schema, table_name, e)

            schema_metas[schema or "default"] = SchemaMeta(
                name=schema or "default",
                tables=tables,
            )

        return BookMeta(
            database_url=self.sanitized_url,
            dialect=self.dialect,
            schemas=schema_metas,
        )

    # --- Dialect-specific helpers ---

    @staticmethod
    def _qualified_name(table_name: str, schema: str | None) -> str:
        """Build a quoted qualified table name."""
        if schema:
            return f'"{schema}"."{table_name}"'
        return f'"{table_name}"'

    def _row_count(self, table_name: str, schema: str | None) -> int | None:
        """Get row count via COUNT(*)."""
        try:
            qualified = self._qualified_name(table_name, schema)
            query = "SELECT COUNT(*) FROM " + qualified  # noqa: S608
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                return result.scalar()
        except Exception as e:
            logger.warning("Failed to get row count for %s: %s", table_name, e)
            return None

    def _sample_data(
        self, table_name: str, schema: str | None, limit: int,
    ) -> list[dict[str, str | int | float | bool | None]]:
        """Get sample rows."""
        try:
            qualified = self._qualified_name(table_name, schema)
            query = "SELECT * FROM " + qualified + " LIMIT " + str(limit)  # noqa: S608
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                columns = result.keys()
                rows: list[dict[str, str | int | float | bool | None]] = []
                for row in result:
                    rows.append({
                        col: self._serialize_value(val)
                        for col, val in zip(columns, row)
                    })
                return rows
        except Exception as e:
            logger.warning("Failed to get sample data for %s: %s", table_name, e)
            return []

    def _table_size(self, table_name: str, schema: str | None) -> int | None:
        """Get table size in bytes. Dialect-specific."""
        try:
            if self.dialect == "postgresql":
                qualified = f"{schema}.{table_name}" if schema else table_name
                query = "SELECT pg_total_relation_size('" + qualified + "')"  # noqa: S608
                with self.engine.connect() as conn:
                    result = conn.execute(text(query))
                    return result.scalar()
        except Exception as e:
            logger.debug("Failed to get table size for %s: %s", table_name, e)
        return None

    def _enum_values(
        self,
        table_name: str,
        schema: str | None,
        columns: list[ColumnInfo],
        row_count: int | None,
    ) -> dict[str, list[str]]:
        """Detect enum-like columns and query their distinct values.

        Only queries columns that look like enums:
        - Named: status, type, category, priority, level, role, state, kind, tier, plan, method
        - Boolean columns
        - VARCHAR/TEXT with short max length patterns

        Only on tables with < 100K rows. Max 20 distinct values per column.
        """
        ENUM_PATTERNS = {  # noqa: N806
            "status", "type", "category", "priority", "level", "role",
            "state", "kind", "tier", "plan", "method", "currency", "country",
            "gender", "variant", "mode", "phase", "grade", "stage",
        }

        if row_count and row_count > 100000:
            return {}

        enum_values: dict[str, list[str]] = {}
        for col in columns:
            col_lower = col.name.lower()
            is_enum_like = (
                col_lower in ENUM_PATTERNS
                or any(col_lower.endswith(f"_{p}") for p in ENUM_PATTERNS)
                or "BOOLEAN" in col.type.upper()
                or "BOOL" in col.type.upper()
            )

            if not is_enum_like:
                continue

            try:
                qualified = self._qualified_name(table_name, schema)
                with self.engine.connect() as conn:
                    result = conn.execute(text(
                        f'SELECT DISTINCT "{col.name}" FROM {qualified}'  # noqa: S608
                        f' WHERE "{col.name}" IS NOT NULL LIMIT 20'
                    ))
                    values = sorted([str(row[0]) for row in result if row[0] is not None])
                    if values and len(values) <= 20:
                        enum_values[col.name] = values
            except Exception as e:
                logger.debug("Failed to get enum values for %s.%s: %s", table_name, col.name, e)

        return enum_values

    @staticmethod
    def _serialize_value(value: object) -> str | int | float | bool | None:
        """Convert DB value to JSON-safe type."""
        if value is None:
            return None
        if isinstance(value, (int, float, bool, str)):
            return value
        return str(value)
