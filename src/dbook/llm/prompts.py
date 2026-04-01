"""Prompt templates for LLM enrichment."""

from __future__ import annotations

from dbook.models import SchemaMeta, TableMeta


def table_summary_prompt(table: TableMeta) -> str:
    """Generate prompt for table summary."""
    cols = ", ".join(f"{c.name} ({c.type})" for c in table.columns)
    fks = ", ".join(f"{','.join(fk.columns)} → {fk.referred_table}" for fk in table.foreign_keys)

    prompt = f"""Summarize this database table in 1-2 sentences. Focus on its business purpose, what data it stores, and how it relates to other tables.

Table: {table.name}
Columns: {cols}
Primary Key: {', '.join(table.primary_key)}
Foreign Keys: {fks if fks else 'none'}
Row Count: {table.row_count if table.row_count else 'unknown'}

Respond with ONLY the summary text, no prefixes or formatting."""
    return prompt


def concept_aliases_prompt(concepts: dict, all_tables: list[str]) -> str:
    """Generate prompt for concept index aliases."""
    concept_list = ", ".join(sorted(concepts.keys())[:50])  # Cap at 50 terms

    prompt = f"""Given these database concepts extracted from table and column names, provide 2-4 business-friendly aliases/synonyms for each term that an AI agent might search for.

Concepts: {concept_list}

Tables in this database: {', '.join(all_tables)}

Respond with a JSON object mapping each concept to an array of aliases. Example:
{{"user": ["customer", "client", "account holder"], "payment": ["transaction", "charge"]}}

Include ONLY concepts where you can provide meaningful aliases. Respond with ONLY the JSON object."""
    return prompt


def schema_narrative_prompt(schema: SchemaMeta) -> str:
    """Generate prompt for schema narrative."""
    tables_desc = []
    for name, table in sorted(schema.tables.items()):
        fks = [f"{','.join(fk.columns)}→{fk.referred_table}" for fk in table.foreign_keys]
        fk_str = f" (refs: {', '.join(fks)})" if fks else ""
        tables_desc.append(f"  - {name}: {len(table.columns)} cols, {table.row_count or '?'} rows{fk_str}")

    tables_text = "\n".join(tables_desc)

    prompt = f"""Describe this database schema's business purpose and data flow in 2-3 sentences. Explain how the tables relate to each other and what business process they support.

Schema: {schema.name}
Tables:
{tables_text}

Respond with ONLY the narrative text, no prefixes or formatting."""
    return prompt


def column_purposes_prompt(table: TableMeta) -> str:
    """Generate prompt for column purpose descriptions."""
    cols_info = []
    for col in table.columns:
        parts = [f"{col.name}: {col.type}"]
        if col.is_primary_key:
            parts.append("(PK)")
        if not col.nullable:
            parts.append("NOT NULL")
        if col.default:
            parts.append(f"DEFAULT {col.default}")
        cols_info.append(" ".join(parts))

    cols_text = "\n".join(f"  - {c}" for c in cols_info)
    fks = ", ".join(f"{','.join(fk.columns)} → {fk.referred_table}" for fk in table.foreign_keys)

    prompt = f"""Describe the purpose of each column in this database table. Give a brief (10-20 word) description of what each column stores and how it's used.

Table: {table.name}
Columns:
{cols_text}
Foreign Keys: {fks if fks else 'none'}

Respond with a JSON object mapping column_name to purpose string. Example:
{{"id": "Unique identifier, auto-incremented primary key", "email": "User email for login and notifications"}}

Respond with ONLY the JSON object."""
    return prompt
