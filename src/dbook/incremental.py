"""Incremental compilation — only recompile tables with changed schemas."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from dbook.hasher import compute_table_hash
from dbook.models import BookMeta
from dbook.generators.navigation import generate_navigation
from dbook.generators.manifest import generate_manifest
from dbook.generators.table import generate_table
from dbook.generators.checksums import generate_checksums

logger = logging.getLogger(__name__)


@dataclass
class IncrementalResult:
    """Result of an incremental compile or check."""
    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    modified: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    files_written: int = 0

    @property
    def has_changes(self) -> bool:
        return bool(self.added or self.removed or self.modified)


def check_changes(book: BookMeta, old_checksums: dict[str, str]) -> IncrementalResult:
    """Compare current schema hashes against stored checksums.

    Does NOT write any files — just reports what changed.
    """
    result = IncrementalResult()

    # Compute current hashes
    new_checksums: dict[str, str] = {}
    for schema_name, schema in book.schemas.items():
        for table_name, table in schema.tables.items():
            key = f"{schema_name}.{table_name}"
            if not table.schema_hash:
                table.schema_hash = compute_table_hash(table)
            new_checksums[key] = table.schema_hash

    # Compare
    old_keys = set(old_checksums.keys())
    new_keys = set(new_checksums.keys())

    result.added = sorted(new_keys - old_keys)
    result.removed = sorted(old_keys - new_keys)

    for key in sorted(old_keys & new_keys):
        if old_checksums[key] != new_checksums[key]:
            result.modified.append(key)
        else:
            result.unchanged.append(key)

    return result


def incremental_compile(
    book: BookMeta,
    output_dir: str | Path,
    old_checksums: dict[str, str],
) -> IncrementalResult:
    """Incrementally compile — only regenerate files for changed tables.

    Steps:
    1. Compute hashes for all current tables
    2. Compare against old checksums
    3. For MODIFIED tables: regenerate their .md file
    4. For ADDED tables: generate new .md file
    5. For REMOVED tables: delete their .md file
    6. If any changes: regenerate affected _manifest.md, NAVIGATION.md
    7. Always update checksums.json
    """
    output = Path(output_dir)

    # Compute hashes
    for schema in book.schemas.values():
        for table in schema.tables.values():
            if not table.schema_hash:
                table.schema_hash = compute_table_hash(table)

    # Detect changes
    result = check_changes(book, old_checksums)
    files_written = 0

    if not result.has_changes:
        logger.info("No schema changes detected")
        # Still update checksums (in case format changed)
        (output / "checksums.json").write_text(generate_checksums(book))
        result.files_written = 1
        return result

    # Track which schemas are affected
    affected_schemas: set[str] = set()

    # Handle MODIFIED tables
    for key in result.modified:
        schema_name, table_name = key.split(".", 1)
        affected_schemas.add(schema_name)
        table = book.schemas[schema_name].tables[table_name]

        # Ensure summary exists
        if not table.summary:
            table.summary = _mechanical_summary(table)

        table_content = generate_table(table, book)
        table_path = output / "schemas" / schema_name / f"{table_name}.md"
        table_path.write_text(table_content)
        files_written += 1
        logger.info(f"Updated: {key}")

    # Handle ADDED tables
    for key in result.added:
        schema_name, table_name = key.split(".", 1)
        affected_schemas.add(schema_name)
        table = book.schemas[schema_name].tables[table_name]

        if not table.summary:
            table.summary = _mechanical_summary(table)

        schema_dir = output / "schemas" / schema_name
        schema_dir.mkdir(parents=True, exist_ok=True)

        table_content = generate_table(table, book)
        (schema_dir / f"{table_name}.md").write_text(table_content)
        files_written += 1
        logger.info(f"Added: {key}")

    # Handle REMOVED tables
    for key in result.removed:
        schema_name, table_name = key.split(".", 1)
        affected_schemas.add(schema_name)

        table_path = output / "schemas" / schema_name / f"{table_name}.md"
        if table_path.exists():
            table_path.unlink()
            logger.info(f"Removed: {key}")

    # Regenerate manifests for affected schemas
    for schema_name in affected_schemas:
        if schema_name in book.schemas:
            manifest_content = generate_manifest(book.schemas[schema_name])
            manifest_path = output / "schemas" / schema_name / "_manifest.md"
            manifest_path.write_text(manifest_content)
            files_written += 1
            logger.info(f"Updated manifest: {schema_name}")

    # Regenerate NAVIGATION.md (always, since table counts/rows may have changed)
    nav_content = generate_navigation(book)
    (output / "NAVIGATION.md").write_text(nav_content)
    files_written += 1

    # Update checksums.json
    checksums_content = generate_checksums(book)
    (output / "checksums.json").write_text(checksums_content)
    files_written += 1

    result.files_written = files_written
    return result


def _mechanical_summary(table) -> str:
    """Generate a mechanical summary from structural data."""
    parts = [f"Table '{table.name}'"]
    if table.row_count is not None:
        parts.append(f"with {table.row_count:,} rows")
    parts.append(f"and {len(table.columns)} columns")
    if table.primary_key:
        parts.append(f"(PK: {', '.join(table.primary_key)})")
    fk_count = len(table.foreign_keys)
    idx_count = len(table.indexes)
    parts.append(f"with {fk_count} foreign keys and {idx_count} index(es)")
    return " ".join(parts) + "."
