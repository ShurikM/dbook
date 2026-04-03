"""Async introspection for large databases."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from dbook.catalog import SQLAlchemyCatalog
from dbook.models import BookMeta, SchemaMeta

logger = logging.getLogger(__name__)


class AsyncSQLAlchemyCatalog:
    """Async wrapper around SQLAlchemyCatalog for parallel table introspection."""

    def __init__(self, url_or_engine: Any, max_workers: int = 4):
        self._catalog = SQLAlchemyCatalog(url_or_engine)
        self._max_workers = max_workers

    async def introspect_all(
        self,
        schemas: list[str | None] | None = None,
        include_sample_data: bool = True,
        sample_limit: int = 5,
        include_row_count: bool = True,
    ) -> BookMeta:
        """Introspect all tables with parallel per-table introspection."""
        resolved: list[str | None] = (
            self._catalog.list_schemas() if schemas is None else schemas
        )

        book = BookMeta(
            database_url=self._catalog.sanitized_url,
            dialect=self._catalog.dialect,
        )

        for schema in resolved:
            table_names = self._catalog.list_tables(schema=schema)

            # Introspect tables in parallel using thread pool
            with ThreadPoolExecutor(
                max_workers=self._max_workers,
            ) as executor:
                futures = {
                    executor.submit(
                        self._catalog.introspect_table,
                        table_name,
                        schema=schema,
                        include_sample_data=include_sample_data,
                        sample_limit=sample_limit,
                        include_row_count=include_row_count,
                    ): table_name
                    for table_name in table_names
                }

                tables = {}
                for future in futures:
                    tbl_name = futures[future]
                    try:
                        table_meta = future.result()
                        tables[tbl_name] = table_meta
                    except Exception:
                        logger.warning(
                            "Failed to introspect %s.%s",
                            schema, tbl_name,
                            exc_info=True,
                        )

                key = schema or "default"
                book.schemas[key] = SchemaMeta(
                    name=key,
                    tables=tables,
                )

        return book
