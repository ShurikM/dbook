"""Compile BookMeta into a layered markdown directory structure."""

from __future__ import annotations

import logging
from pathlib import Path

from dbook.hasher import compute_table_hash
from dbook.models import BookMeta
from dbook.generators.navigation import generate_navigation
from dbook.generators.manifest import generate_manifest
from dbook.generators.table import generate_table
from dbook.generators.concepts import generate_concepts_json
from dbook.generators.checksums import generate_checksums

logger = logging.getLogger(__name__)


def compile_book(book: BookMeta, output_dir: str | Path) -> dict:
    """Compile a BookMeta into a layered markdown directory.

    Output structure:
        output_dir/
            NAVIGATION.md
            concepts.json
            checksums.json
            schemas/
                {schema_name}/
                    _manifest.md
                    {table_name}.md
                    ...

    Parameters
    ----------
    book : BookMeta
        The introspected database metadata.
    output_dir : str | Path
        Directory to write output files.

    Returns
    -------
    dict
        Summary: {"files_written": int, "schemas": int, "tables": int}
    """
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    files_written = 0
    total_tables = 0

    # Compute schema hashes for all tables
    for schema in book.schemas.values():
        for table in schema.tables.values():
            if not table.schema_hash:
                table.schema_hash = compute_table_hash(table)

    # Generate mechanical summaries for tables without summaries
    for schema in book.schemas.values():
        for table in schema.tables.values():
            if not table.summary:
                table.summary = _mechanical_summary(table)

    # NAVIGATION.md (L0)
    nav_content = generate_navigation(book)
    (output / "NAVIGATION.md").write_text(nav_content)
    files_written += 1
    logger.info("Written NAVIGATION.md")

    # concepts.json (Ls)
    concepts_content = generate_concepts_json(book)
    (output / "concepts.json").write_text(concepts_content)
    files_written += 1
    logger.info("Written concepts.json")

    # checksums.json
    checksums_content = generate_checksums(book)
    (output / "checksums.json").write_text(checksums_content)
    files_written += 1
    logger.info("Written checksums.json")

    # Per-schema output
    schemas_dir = output / "schemas"
    schemas_dir.mkdir(exist_ok=True)

    for schema_name, schema in sorted(book.schemas.items()):
        schema_dir = schemas_dir / schema_name
        schema_dir.mkdir(exist_ok=True)

        # _manifest.md (L1)
        manifest_content = generate_manifest(schema)
        (schema_dir / "_manifest.md").write_text(manifest_content)
        files_written += 1

        # Per-table .md (L2)
        for table_name, table in sorted(schema.tables.items()):
            table_content = generate_table(table, book)
            (schema_dir / f"{table_name}.md").write_text(table_content)
            files_written += 1
            total_tables += 1

        logger.info(f"Written schema '{schema_name}': 1 manifest + {len(schema.tables)} tables")

    return {
        "files_written": files_written,
        "schemas": len(book.schemas),
        "tables": total_tables,
    }


def _mechanical_summary(table) -> str:
    """Generate a mechanical summary from structural data."""
    parts = [f"Table '{table.name}'"]
    if table.row_count is not None:
        parts.append(f"with {table.row_count:,} rows")
    parts.append(f"and {len(table.columns)} columns")
    if table.primary_key:
        parts.append(f"(PK: {', '.join(table.primary_key)})")
    if table.foreign_keys:
        targets = [fk.referred_table for fk in table.foreign_keys]
        parts.append(f"references {', '.join(targets)}")
    if table.indexes:
        parts.append(f"with {len(table.indexes)} index(es)")
    return " ".join(parts) + "."
