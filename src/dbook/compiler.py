"""Compile BookMeta into a layered markdown directory structure."""

from __future__ import annotations

import logging
from pathlib import Path

from dbook.hasher import compute_table_hash
from dbook.models import BookMeta
from dbook.generators.navigation import generate_navigation
from dbook.generators.manifest import generate_manifest
from dbook.generators.table import generate_table
from dbook.generators.checksums import generate_checksums

logger = logging.getLogger(__name__)


def compile_book(book: BookMeta, output_dir: str | Path, metrics_file: str | Path | None = None) -> dict:
    """Compile a BookMeta into a layered markdown directory.

    Output structure:
        output_dir/
            NAVIGATION.md
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
    metrics_file : str | Path | None
        Optional path to a metrics.yaml with user-defined metric definitions.

    Returns
    -------
    dict
        Summary: {"files_written": int, "schemas": int, "tables": int}
    """
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    files_written = 0

    # Compute schema hashes for all tables
    for schema in book.schemas.values():
        for table in schema.tables.values():
            if not table.schema_hash:
                table.schema_hash = compute_table_hash(table)

    # PII scanning (if mode requires it)
    if book.mode in ("pii", "full"):
        from dbook.pii.scanner import scan_book  # type: ignore[import-not-found]
        scan_book(book)

    # LLM enrichment (if mode requires it)
    if book.mode in ("llm", "full"):
        try:
            from dbook.llm.enricher import enrich_book as llm_enrich
            llm_enrich(book, book._llm_provider)  # type: ignore[attr-defined]
        except (ImportError, AttributeError) as e:
            logger.warning(f"LLM enrichment skipped: {e}")

    # Generate mechanical summaries for tables without summaries
    for schema in book.schemas.values():
        for table in schema.tables.values():
            if not table.summary:
                table.summary = _mechanical_summary(table)

    # Load user-defined metrics
    from dbook.metrics import MetricDefinition, load_metrics

    user_metrics: list[MetricDefinition] = []
    if metrics_file:
        user_metrics = load_metrics(metrics_file)
        if user_metrics:
            logger.info(f"Loaded {len(user_metrics)} user-defined metrics from {metrics_file}")

    # NAVIGATION.md (L0) — compact table overview
    nav_content = generate_navigation(book, user_metrics=user_metrics)
    (output / "NAVIGATION.md").write_text(nav_content)
    files_written += 1
    logger.info("Written NAVIGATION.md")

    # checksums.json
    checksums_content = generate_checksums(book)
    (output / "checksums.json").write_text(checksums_content)
    files_written += 1
    logger.info("Written checksums.json")

    # Per-schema output
    schemas_dir = output / "schemas"
    schemas_dir.mkdir(exist_ok=True)

    tables_written = 0
    for schema_name, schema in sorted(book.schemas.items()):
        schema_dir = schemas_dir / schema_name
        schema_dir.mkdir(exist_ok=True)

        # _manifest.md (L1)
        manifest_content = generate_manifest(schema)
        (schema_dir / "_manifest.md").write_text(manifest_content)
        files_written += 1

        # Per-table .md (L2)
        for table_name, table in sorted(schema.tables.items()):
            table_content = generate_table(table, book, user_metrics=user_metrics)
            (schema_dir / f"{table_name}.md").write_text(table_content)
            files_written += 1
            tables_written += 1

        logger.info(f"Written schema '{schema_name}': 1 manifest + {len(schema.tables)} tables")

    return {
        "files_written": files_written,
        "schemas": len(book.schemas),
        "tables": tables_written,
    }


def _mechanical_summary(table) -> str:
    """Generate a mechanical summary from structural data."""
    parts = [f"Table '{table.name}'"]
    if table.row_count is not None:
        parts.append(f"with {table.row_count:,} rows")
    parts.append(f"and {len(table.columns)} columns")
    if table.primary_key:
        parts.append(f"(PK: {', '.join(table.primary_key)})")
    fk_count = len(table.foreign_keys) if table.foreign_keys else 0
    parts.append(f"with {fk_count} foreign keys")
    idx_count = len(table.indexes) if table.indexes else 0
    parts.append(f"and {idx_count} index(es).")
    return " ".join(parts)
