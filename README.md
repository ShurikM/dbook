# dbook

A metadata compiler that introspects databases and generates structured, token-efficient documentation for AI agent consumption.

## The Problem

AI agents interacting with databases waste massive tokens consuming raw DDL/schema dumps. A 50-table database produces ~15,000 tokens of CREATE TABLE statements. Most of that is irrelevant to any given question.

## The Solution

dbook connects to your database, introspects the schema, and compiles a **layered, navigable directory of markdown files** — a "book" that agents can browse progressively:

```
dbook_output/
  NAVIGATION.md              # ~100 tokens: schema overview, quick reference
  concepts.json              # term → table/column mapping for search
  checksums.json             # schema hashes for change detection
  schemas/
    auth/
      _manifest.md           # schema-level overview
      users.md               # full table metadata
      sessions.md
    billing/
      _manifest.md
      orders.md
      payments.md
```

An agent answering "where is user email stored?" reads **~150 tokens** (NAVIGATION.md + concepts.json + users.md) instead of **~15,000 tokens** (full DDL dump). That's **99% token savings**.

## Features

- **Progressive disclosure** — L0 catalog → L1 schema manifest → L2 table details → Ls concept index
- **Database agnostic** — PostgreSQL, MySQL, SQLite, Snowflake, BigQuery via SQLAlchemy
- **Schema change detection** — SHA256 hash per table; incremental recompilation of only changed tables
- **PII detection** — Optional Microsoft Presidio integration marks sensitive columns and redacts sample data
- **LLM enrichment** — Optional LLM pass generates semantic summaries, concept aliases, cross-table narratives
- **Multiple interfaces** — Python library, CLI tool, MCP server, Claude Code skill

## Quick Start

```bash
pip install dbook

# Compile your database into a dbook
dbook compile "postgresql://user:pass@host/db" --output ./my_dbook

# Check for schema changes
dbook check ./my_dbook "postgresql://user:pass@host/db"

# Incremental recompile (only changed tables)
dbook compile "postgresql://user:pass@host/db" --output ./my_dbook --incremental
```

## Optional Features

```bash
# PII detection + sample data redaction
pip install dbook[pii]
dbook compile "postgresql://..." --output ./my_dbook --pii

# LLM-enriched semantic summaries
pip install dbook[llm]
dbook compile "postgresql://..." --output ./my_dbook --llm --llm-provider anthropic --llm-key sk-...
```

## Architecture

```
Catalog Protocol (abstraction layer)
    └── SQLAlchemyCatalog (default implementation)
            └── SQLAlchemy Inspector (80% uniform cross-dialect)
            └── Dialect-specific helpers (row counts, sizes, samples)

BookMeta (introspected data) → Compiler → Markdown directory output
```

The `Catalog` protocol allows future non-SQL sources (Unity Catalog, AWS Glue, Hive Metastore) without changing the compiler.

## How Agents Use It

1. **Read NAVIGATION.md** (~100 tok) — get schema overview
2. **Search concepts.json** (~50 tok) — find specific terms/columns
3. **Read _manifest.md** (~200 tok) — schema-level detail
4. **Read table.md** (~300 tok) — full table metadata

Total per question: **~150-350 tokens** vs **~15,000 tokens** baseline.

## Capability Matrix

| Feature | Base | + Presidio | + LLM | + Both |
|---------|------|-----------|-------|--------|
| Schema structure | Full | Full | Full | Full |
| Concept index | Mechanical | Mechanical | + Aliases/synonyms | + Aliases/synonyms |
| Table summaries | Mechanical | Mechanical | Semantic | Semantic |
| PII detection | - | Column names + sample data | - | Full |
| Sample data redaction | - | Auto-redact | - | Auto-redact |
| Cross-table narratives | - | - | Full | Full |

## License

MIT
