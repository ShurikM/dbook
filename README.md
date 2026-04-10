[![CI](https://github.com/ShurikM/dbook/actions/workflows/ci.yml/badge.svg)](https://github.com/ShurikM/dbook/actions)

# dbook — v0.4.0

A metadata compiler that turns database schemas into AI-optimized documentation.

> **dbook** connects to your database, introspects every table, and automatically generates structured metadata that AI agents can navigate -- enum values, data lineage, example queries, auto-detected metrics, and PII markers. One command, fully automated, no manual authoring. Agents with dbook score **4.7/5** on SQL tasks vs **3.2/5** with raw DDL, while reading **77% fewer tokens**.

<p align="center">
  <img src="docs/dbook-animation.gif" alt="dbook in action" width="800">
</p>

## What's New in 0.4.0

- **Clean plugin architecture** -- dbook core is fully self-contained, no external LLM dependency required
- **LLM providers as plugins** -- agentlib, OpenAI, Anthropic, Gemini supported via `LLMProvider` protocol
- **Own token counter** -- no longer depends on agentlib for token counting
- **Removed agentlib shim** -- cleaner dependency graph, faster imports

## The Problem

Your AI agents are **blind to your data**.

Raw DDL tells agents the structure -- but not the meaning:
- `status VARCHAR(20)` -- agents guess "active", "enabled", "1"... the real values are "pending", "shipped", "delivered"
- `user_id INTEGER REFERENCES users(id)` -- but what IS this relationship? The customer? The assignee? The creator?
- Your gold layer exists because consumers couldn't read silver -- but AI agents CAN, with the right metadata

**The result:**
- You maintain expensive gold layer ETL just for AI consumption
- Every agent re-discovers the schema independently (10 agents = 10x cost)
- Schema changes break agents silently -- no one knows until production fails
- Agents access PII columns unknowingly -- compliance risk with every query
- Agents guess enum values and write wrong SQL -- silent data quality issues

## What dbook Does

One command connects to your database, introspects every table, runs `SELECT DISTINCT` on enum columns, traces foreign key chains, detects PII patterns, and generates a complete metadata directory -- no configuration, no manual authoring:

<p align="center">
  <img src="docs/architecture.svg" alt="dbook Architecture" width="800">
</p>

```bash
pip install dbook
dbook compile "postgresql://user:pass@host/db" --output ./my_dbook
```

### What agents get:

**1. Enum value documentation** -- auto-detected via `SELECT DISTINCT`
```
status: pending, confirmed, shipped, delivered, cancelled
method: credit_card, debit_card, paypal, bank_transfer
```

**2. Semantic FK descriptions** -- agents understand relationships
```
-> users via user_id -- the customer who placed this order
<- order_items.order_id -- line items in this order
```

**3. Example queries** -- patterns agents can follow
```sql
- By status: SELECT * FROM orders WHERE status IN ('pending', 'confirmed')
- Revenue over time: SELECT DATE(created_at), SUM(total) FROM orders GROUP BY DATE(created_at)
```

**4. Auto-detected metrics** -- common aggregations ready to use
```
- Total Amount: SELECT SUM(total) FROM orders
- Count by Status: SELECT status, COUNT(*) FROM orders GROUP BY status
- Amount over time: SELECT DATE(created_at), SUM(total) FROM orders GROUP BY DATE(created_at)
```

**5. Data lineage** -- how tables connect in the data flow
```
Source tables: users, products (no dependencies)
Intermediate: orders -> depends on users | <- used by order_items, invoices
Leaf: payments -> depends on invoices
```

**6. PII detection** -- marks sensitive columns, redacts sample data
```
| email | VARCHAR(255) | EMAIL (0.90) | high |
| card_last_four | VARCHAR(4) | CREDIT_CARD_PARTIAL (0.70) | low |
```

**7. Query validation** -- SQLGlot-powered, catches errors before execution
```python
validator = QueryValidator(book)
result = validator.validate("SELECT * FROM orders WHERE status = 'completed'")
# Warning: 'completed' not in known values: pending, confirmed, shipped, delivered, cancelled
```

## What Makes dbook Different

dbook is not a documentation tool you maintain by hand. It is a compiler that connects to your live database, runs real queries, and generates everything automatically.

| | Raw DDL | Manual docs | dbook |
|---|---------|-------------|-------|
| **What agents read** | Full schema dump | Whatever you wrote | Only the tables they need |
| **Enum values** | Not available | You maintain them | Auto-discovered via `SELECT DISTINCT` |
| **Metrics** | Agent guesses | You define them | Auto-detected (SUM columns, COUNT-by-enum, time series) |
| **Data lineage** | Agent traces FKs manually | You diagram it | Auto-mapped from FK chains (root, intermediate, leaf) |
| **Example queries** | None | You write them | Auto-generated (FK joins, unique-key lookups) |
| **PII detection** | None | You flag columns | Auto-detected (email, SSN, phone patterns) |
| **Schema changes** | Re-dump everything | You update manually | Per-table checksums, incremental recompilation |
| **Setup effort** | Zero | Hours per schema | One command: `dbook compile` |
| **Token cost** | 100% (reads everything) | Varies | 23% (reads only what's needed) |

The token savings come from the architecture, not from compression. dbook organizes metadata into navigable layers so agents read 2-3 files per task instead of the entire schema. But the quality improvement comes from what those files contain -- actual enum values, real relationship semantics, working query patterns, and pre-computed metrics that raw DDL simply does not have.

## dbook vs. Semantic Layers

Semantic layers like [dbt's Semantic Layer](https://docs.getdbt.com/docs/build/about-metricflow) guarantee correctness for **modeled** metrics — the LLM picks the right metric/dimension, and a deterministic engine generates provably correct SQL. But they only cover what's been explicitly modeled, and require significant upfront investment.

dbook solves a different problem: giving agents enough context to work with **any** table, **any** column, **any** query — with zero setup. The two approaches are complementary:

| | Semantic Layer (dbt) | dbook |
|--|--|--|
| **Correctness** | Guaranteed (for modeled queries) | Agent-dependent |
| **Coverage** | Only modeled metrics | Any query the schema supports |
| **Setup cost** | High (human-curated ontology) | Zero (`dbook compile <url>`) |
| **Failure mode** | Explicit refusal | Best-effort with rich context |

**The ideal stack uses both:** semantic layer for high-stakes business metrics (board reports, KPIs, audited numbers), dbook for everything else (ad-hoc exploration, cross-schema discovery, novel questions nobody pre-modeled). dbook's roadmap includes importing dbt semantic definitions so agents get curated metrics with high confidence and fall back to schema-guided SQL for the rest.

## Key Benchmark Results

### Scorecard: dbook vs Raw DDL

Tested against a realistic e-commerce database modeled after Amazon: 7 schemas, 34 tables, covering users, orders, inventory, payments, and support. 15 real agent tasks across 3 agent personas (Billing, Care, Sales) -- each requiring the agent to find the right tables, write correct SQL, execute it, and return accurate results. Every scenario scored by a judge on 4 dimensions.

- **dbook Score: 4.7/5** vs Baseline (raw DDL) **3.2/5** -- improvement of **+1.5**
- **Token savings: 77%** (7,792 vs 33,656 tokens per scenario)

The improvement is not just efficiency -- it is correctness. dbook agents find the right tables, use valid enum values in WHERE clauses, join on correct foreign keys, and return accurate results. Baseline agents reading raw DDL frequently guess wrong enum values, miss relevant tables, and produce SQL that returns empty or incorrect results.

### Per-Dimension Scoring

The biggest gains are in SQL correctness and result accuracy -- exactly the dimensions where enum values, relationship metadata, and example queries make the difference.

| Dimension | dbook | Baseline (DDL) | Delta |
|-----------|-------|----------------|-------|
| Table Discovery | 4.7 | 4.3 | +0.4 |
| SQL Correctness | 4.7 | 3.0 | **+1.7** |
| Result Accuracy | 4.3 | 2.6 | **+1.7** |
| Response Quality | 4.7 | 2.7 | **+2.0** |

### Per-Agent Breakdown

| Agent Type | dbook | Baseline | Token Savings |
|------------|-------|----------|---------------|
| Billing | 4.8/5 | 3.0/5 | 77% |
| Care | 4.8/5 | 3.0/5 | 77% |
| Sales | 4.5/5 | 3.5/5 | 77% |

> Benchmarked against a 34-table e-commerce schema on PostgreSQL. All scores from automated test runs — see `benchmarks/` for scenarios, seed data, and reproducible results.

### Benchmark System Design

<p align="center">
  <img src="docs/benchmark-system.svg" alt="Benchmark System Design" width="800">
</p>

## Navigation Architecture

dbook uses an L0/L1/L2 layered navigation architecture (inspired by [agentlib](https://github.com/barkain/agentlib)) that organizes metadata into progressively detailed layers. This architecture is what delivers the 77% token savings -- agents read 2-3 files per task instead of the entire schema.

| Layer | File | What it contains |
|-------|------|-----------------|
| L0 -- Overview | NAVIGATION.md | Schema listing with row counts, table descriptions |
| L1 -- Section | _manifest.md | Per-schema details, cross-table relationships |
| L2 -- Detail | {table}.md | Columns, enum values, FKs, metrics, sample data, example queries |
| Lookup | concepts.json | Table/column term index with mechanical + LLM aliases |
| Protocol | SKILL.md | Agent navigation instructions |

dbook's core -- compilation, metadata generation, enum discovery, FK analysis, PII detection -- works without any LLM. LLM enrichment is optional and adds semantic summaries, concept aliases, and schema narratives on top of the mechanical output.

### LLM Provider Plugin Model

dbook defines an `LLMProvider` protocol that any LLM backend can implement:

- **`MockProvider`** -- built-in, deterministic responses for testing
- **`AgentlibProvider`** -- uses agentlib's multi-provider LLM layer (Anthropic, OpenAI, Gemini, xAI, DeepSeek)
- **Custom providers** -- implement the `LLMProvider` protocol with a single `complete()` method

To use LLM enrichment, install a provider backend. For example, with agentlib:

```bash
pip install agentlib
dbook compile "postgresql://..." --output ./my_dbook --llm --llm-provider anthropic --llm-key sk-...
```

## Architecture

<p align="center">
  <img src="docs/pipeline.svg" alt="dbook Compilation Pipeline" width="800">
</p>

### Catalog Protocol
Database-agnostic via `Catalog` protocol. Default `SQLAlchemyCatalog` supports any SQLAlchemy-compatible database. DB type auto-detected from URL.

### Supported Databases
PostgreSQL, MySQL, SQLite, Snowflake, BigQuery -- any database with a SQLAlchemy dialect.

## Usage

### Full compile
```bash
dbook compile "postgresql://user:pass@host/db" --output ./my_dbook
```

### With PII detection (marks sensitive columns, redacts sample data)
```bash
pip install "dbook[pii]"
dbook compile "postgresql://..." --output ./my_dbook --pii
```

### With LLM enrichment (semantic summaries, concept aliases)
```bash
pip install "dbook[llm]"
dbook compile "postgresql://..." --output ./my_dbook --llm --llm-provider anthropic --llm-key sk-...
```

### Check for schema changes
```bash
dbook check ./my_dbook "postgresql://user:pass@host/db"
```

### Incremental recompile (only changed tables)
```bash
dbook compile "postgresql://..." --output ./my_dbook --incremental
```

### Python API
```python
from dbook.catalog import SQLAlchemyCatalog
from dbook.compiler import compile_book
from dbook.validator import QueryValidator

# Compile
catalog = SQLAlchemyCatalog("postgresql://user:pass@host/db")
book = catalog.introspect_all()
compile_book(book, "./my_dbook")

# Validate agent SQL
validator = QueryValidator(book)
result = validator.validate("SELECT * FROM orders WHERE status = 'delivered'")
print(result.valid, result.errors, result.warnings)
```

## Optional Features

| Feature | Install | Flag | What it adds |
|---------|---------|------|-------------|
| PII detection | `pip install "dbook[pii]"` | `--pii` | Column sensitivity markers, sample data redaction |
| LLM enrichment | `pip install "dbook[llm]"` | `--llm` | Semantic summaries, concept aliases, schema narratives |
| Metrics | `pip install "dbook[metrics]"` | `--metrics` | User-defined canonical business metrics |

## The Silver Layer Insight

Traditional data pipelines create gold layers because consumers can't read raw data. With dbook, AI agents can understand silver directly -- reducing the need for gold views for discovery and ad-hoc queries.

<p align="center">
  <img src="docs/silver-layer.svg" alt="The Silver Layer Insight" width="800">
</p>

> **Note:** dbook reduces the need for gold views for discovery and ad-hoc queries.
> Gold layers still provide value for: enforced business rules, canonical metric
> definitions, data quality guarantees, and grain standardization. For critical metrics,
> define them in `metrics.yaml` or import them from your dbt semantic layer --
> dbook includes canonical definitions in its output so agents use the
> authoritative calculation, not their own interpretation.

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -q --tb=short
```

139 tests covering: introspection, compilation, CLI, PII detection, LLM enrichment, query validation, and realistic agent simulation benchmarks.

## License

Apache License 2.0
