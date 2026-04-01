"""LLM enrichment orchestrator."""

from __future__ import annotations

import json
import logging

from dbook.models import BookMeta
from dbook.llm.prompts import (
    table_summary_prompt,
    concept_aliases_prompt,
    schema_narrative_prompt,
    column_purposes_prompt,
)
from dbook.llm.provider import LLMProvider

logger = logging.getLogger(__name__)


def enrich_book(
    book: BookMeta,
    provider: LLMProvider,
    enrich_summaries: bool = True,
    enrich_columns: bool = True,
    enrich_narratives: bool = True,
    enrich_aliases: bool = True,
) -> dict:
    """Enrich a BookMeta with LLM-generated content.

    Parameters
    ----------
    book : BookMeta
        The book to enrich (modified in place).
    provider : LLMProvider
        The LLM provider to use for completions.
    enrich_summaries : bool
        Generate semantic table summaries.
    enrich_columns : bool
        Generate column purpose descriptions.
    enrich_narratives : bool
        Generate schema narratives.
    enrich_aliases : bool
        Generate concept index aliases.

    Returns
    -------
    dict
        Summary of enrichment: {tables_enriched, schemas_enriched, aliases_added, total_llm_calls}
    """
    total_calls = 0
    tables_enriched = 0
    schemas_enriched = 0

    # Step 1: Table summaries
    if enrich_summaries:
        for schema in book.schemas.values():
            for table in schema.tables.values():
                try:
                    prompt = table_summary_prompt(table)
                    summary = provider.complete(prompt, max_tokens=200)
                    table.summary = summary.strip()
                    total_calls += 1
                    tables_enriched += 1
                    logger.debug(f"Enriched summary: {table.name}")
                except Exception as e:
                    logger.warning(f"Failed to enrich summary for {table.name}: {e}")

    # Step 2: Column purposes
    if enrich_columns:
        for schema in book.schemas.values():
            for table in schema.tables.values():
                try:
                    prompt = column_purposes_prompt(table)
                    response = provider.complete(prompt, max_tokens=500)
                    purposes = _parse_json_response(response)
                    if purposes:
                        table.column_purposes = purposes
                    total_calls += 1
                    logger.debug(f"Enriched columns: {table.name}")
                except Exception as e:
                    logger.warning(f"Failed to enrich columns for {table.name}: {e}")

    # Step 3: Schema narratives
    if enrich_narratives:
        for schema in book.schemas.values():
            try:
                prompt = schema_narrative_prompt(schema)
                narrative = provider.complete(prompt, max_tokens=300)
                schema.narrative = narrative.strip()
                total_calls += 1
                schemas_enriched += 1
                logger.debug(f"Enriched narrative: {schema.name}")
            except Exception as e:
                logger.warning(f"Failed to enrich narrative for {schema.name}: {e}")

    # Step 4: Concept aliases (handled separately — needs concepts built first)
    # The aliases are stored for later injection into concepts.json by the compiler
    aliases_added = 0
    if enrich_aliases:
        try:
            # Collect all table names and existing concept terms
            all_tables = []
            for schema in book.schemas.values():
                for table_name in schema.tables:
                    all_tables.append(table_name)

            # Build a minimal concept dict for the prompt
            from dbook.generators.concepts import generate_concepts
            concepts = generate_concepts(book)

            prompt = concept_aliases_prompt(concepts, all_tables)
            response = provider.complete(prompt, max_tokens=1000)
            alias_map = _parse_json_response(response)

            if alias_map:
                # Store aliases on the book for the compiler to use
                # We attach it as a temporary attribute
                book._concept_aliases = alias_map  # type: ignore[attr-defined]
                aliases_added = sum(len(v) for v in alias_map.values() if isinstance(v, list))

            total_calls += 1
            logger.debug(f"Generated {aliases_added} concept aliases")
        except Exception as e:
            logger.warning(f"Failed to generate concept aliases: {e}")

    # Update mode
    if book.mode == "base":
        book.mode = "llm"
    elif book.mode == "pii":
        book.mode = "full"

    result = {
        "tables_enriched": tables_enriched,
        "schemas_enriched": schemas_enriched,
        "aliases_added": aliases_added,
        "total_llm_calls": total_calls,
    }
    logger.info(f"LLM enrichment complete: {result}")
    return result


def _parse_json_response(response: str) -> dict | None:
    """Parse a JSON response from LLM, handling common formatting issues."""
    response = response.strip()

    # Try to find JSON in the response
    # Sometimes LLMs wrap JSON in ```json ... ```
    if "```" in response:
        parts = response.split("```")
        for part in parts:
            part = part.strip()
            if part.startswith("json"):
                part = part[4:].strip()
            if part.startswith("{"):
                try:
                    return json.loads(part)
                except json.JSONDecodeError:
                    continue

    # Direct parse
    try:
        return json.loads(response)
    except json.JSONDecodeError:
        logger.warning(f"Failed to parse LLM JSON response: {response[:200]}")
        return None
