"""dbook CLI — compile database metadata for AI agent consumption."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import click  # type: ignore[import-untyped]

from dbook import __version__


@click.group()
@click.version_option(version=__version__, prog_name="dbook")
def main():
    """dbook — Database metadata compiler for AI agent consumption."""
    pass


@main.command()  # type: ignore[attr-defined]
@click.argument("database_url")
@click.option("--output", "-o", required=True, type=click.Path(), help="Output directory for compiled dbook")
@click.option("--schemas", "-s", default=None, help="Comma-separated list of schemas to include")
@click.option("--incremental", "-i", is_flag=True, help="Only recompile tables with changed schemas")
@click.option("--sample-rows", default=5, type=int, help="Number of sample rows per table (default: 5)")
@click.option("--no-sample-data", is_flag=True, help="Skip sample data collection")
@click.option("--no-row-count", is_flag=True, help="Skip row count queries")
@click.option("--pii", is_flag=True, help="Enable PII detection (requires dbook[pii])")
@click.option("--llm", is_flag=True, help="Enable LLM enrichment (requires dbook[llm])")
@click.option("--llm-provider", default=None, help="LLM provider: anthropic, openai, gemini")
@click.option("--llm-key", default=None, help="LLM API key")
def compile(database_url, output, schemas, incremental, sample_rows, no_sample_data, no_row_count, pii, llm, llm_provider, llm_key):
    """Compile database metadata into a dbook directory.

    DATABASE_URL is a SQLAlchemy connection string (e.g., postgresql://user:pass@host/db).
    DB type is auto-detected from the URL scheme.
    """
    from dbook.catalog import SQLAlchemyCatalog
    from dbook.compiler import compile_book

    output_path = Path(output)
    schema_list = [s.strip() for s in schemas.split(",")] if schemas else None

    click.echo(f"dbook v{__version__}")
    click.echo("Connecting to database...")

    try:
        catalog = SQLAlchemyCatalog(database_url)
    except Exception as e:
        click.echo(f"Error: Failed to connect: {e}", err=True)
        sys.exit(1)

    click.echo(f"Dialect: {catalog.dialect}")

    # Determine mode
    mode = "base"
    if pii and llm:
        mode = "full"
    elif pii:
        mode = "pii"
    elif llm:
        mode = "llm"

    click.echo(f"Mode: {mode}")
    click.echo("Introspecting schema...")

    start = time.monotonic()

    book = catalog.introspect_all(
        schemas=schema_list,
        include_sample_data=not no_sample_data,
        sample_limit=sample_rows,
        include_row_count=not no_row_count,
    )
    book.mode = mode

    elapsed_introspect = time.monotonic() - start
    total_tables = sum(len(s.tables) for s in book.schemas.values())
    click.echo(f"Found {total_tables} tables in {len(book.schemas)} schema(s) ({elapsed_introspect:.1f}s)")

    # PII scanning
    if pii:
        from dbook.pii.scanner import scan_book  # type: ignore[import-not-found]
        click.echo("Scanning for PII...")
        scan_book(book)
        pii_count = sum(
            1 for s in book.schemas.values()
            for t in s.tables.values()
            for c in t.columns
            if c.pii_type
        )
        click.echo(f"  {pii_count} PII columns detected, sample data redacted")

    if llm:
        if not llm_provider:
            click.echo("Error: --llm-provider required with --llm", err=True)
            sys.exit(1)
        if not llm_key and llm_provider != "mock":
            click.echo("Error: --llm-key required with --llm", err=True)
            sys.exit(1)

        from dbook.llm.provider import create_provider
        provider = create_provider(llm_provider, api_key=llm_key or "", model=None)
        book._llm_provider = provider  # type: ignore[attr-defined]

        from dbook.llm.enricher import enrich_book as llm_enrich
        click.echo("Enriching with LLM...")
        result = llm_enrich(book, provider)
        click.echo(f"  {result['tables_enriched']} tables enriched")
        click.echo(f"  {result['schemas_enriched']} schema narratives generated")
        click.echo(f"  {result['aliases_added']} concept aliases added")
        click.echo(f"  {result['total_llm_calls']} LLM calls made")

    # Compile
    start = time.monotonic()

    if incremental and output_path.exists():
        checksums_file = output_path / "checksums.json"
        if checksums_file.exists():
            from dbook.incremental import incremental_compile
            old_checksums = json.loads(checksums_file.read_text())
            click.echo("Running incremental compile...")
            result = incremental_compile(book, output_path, old_checksums)
            elapsed_compile = time.monotonic() - start
            click.echo(f"Incremental compile complete ({elapsed_compile:.1f}s):")
            click.echo(f"  Added: {len(result.added)}")
            click.echo(f"  Modified: {len(result.modified)}")
            click.echo(f"  Removed: {len(result.removed)}")
            click.echo(f"  Unchanged: {len(result.unchanged)}")
            click.echo(f"  Files written: {result.files_written}")
        else:
            click.echo("No existing checksums found. Running full compile...")
            result = compile_book(book, output_path)
            elapsed_compile = time.monotonic() - start
            click.echo(f"Full compile: {result['files_written']} files written ({elapsed_compile:.1f}s)")
    else:
        result = compile_book(book, output_path)
        elapsed_compile = time.monotonic() - start
        click.echo(f"Compiled {result['tables']} tables across {result['schemas']} schema(s)")
        click.echo(f"  Files written: {result['files_written']}")
        click.echo(f"  Output: {output_path}")
        click.echo(f"  Time: {elapsed_compile:.1f}s")

    click.echo("Done.")


@main.command()  # type: ignore[attr-defined]
@click.argument("book_dir", type=click.Path(exists=True))
@click.argument("database_url")
@click.option("--schemas", "-s", default=None, help="Comma-separated list of schemas to check")
def check(book_dir, database_url, schemas):
    """Check for schema changes between a compiled dbook and a live database.

    BOOK_DIR is the path to an existing compiled dbook directory.
    DATABASE_URL is the database to compare against.

    Exit codes: 0 = no changes, 1 = changes detected.
    """
    from dbook.catalog import SQLAlchemyCatalog
    from dbook.incremental import check_changes
    from dbook.hasher import compute_table_hash

    book_path = Path(book_dir)
    checksums_file = book_path / "checksums.json"

    if not checksums_file.exists():
        click.echo("Error: No checksums.json found. Run 'dbook compile' first.", err=True)
        sys.exit(2)

    schema_list = [s.strip() for s in schemas.split(",")] if schemas else None

    click.echo(f"dbook v{__version__}")
    click.echo("Checking for schema changes...")

    try:
        catalog = SQLAlchemyCatalog(database_url)
    except Exception as e:
        click.echo(f"Error: Failed to connect: {e}", err=True)
        sys.exit(1)

    book = catalog.introspect_all(
        schemas=schema_list,
        include_sample_data=False,
        include_row_count=False,
    )

    # Compute hashes for current state
    for schema in book.schemas.values():
        for table in schema.tables.values():
            table.schema_hash = compute_table_hash(table)

    old_checksums = json.loads(checksums_file.read_text())
    result = check_changes(book, old_checksums)

    if result.has_changes:
        click.echo("Changes detected:")
        if result.added:
            click.echo(f"  Added ({len(result.added)}): {', '.join(result.added)}")
        if result.modified:
            click.echo(f"  Modified ({len(result.modified)}): {', '.join(result.modified)}")
        if result.removed:
            click.echo(f"  Removed ({len(result.removed)}): {', '.join(result.removed)}")
        click.echo(f"  Unchanged: {len(result.unchanged)}")
        click.echo("\nRun 'dbook compile --incremental' to update.")
        sys.exit(1)
    else:
        click.echo(f"No changes detected. All {len(result.unchanged)} tables up to date.")
        sys.exit(0)
