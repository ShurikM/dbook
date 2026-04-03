# dbook

A database metadata compiler that makes AI agents understand your database — not just its structure, but its meaning.

> **dbook** compiles your database schema into AI-ready metadata — enum values, semantic relationships, example queries, auto-detected metrics, data lineage, and PII markers. In SQL execution benchmarks, agents with dbook produce 100% correct SQL vs 75% with raw DDL.

## The Problem

Your AI agents are **blind to your data**.

Raw DDL tells agents the structure — but not the meaning:
- `status VARCHAR(20)` — agents guess "active", "enabled", "1"... the real values are "pending", "shipped", "delivered"
- `user_id INTEGER REFERENCES users(id)` — but what IS this relationship? The customer? The assignee? The creator?
- Your gold layer exists because consumers couldn't read silver — but AI agents CAN, with the right metadata

**The result:**
- You maintain expensive gold layer ETL just for AI consumption
- Every agent re-discovers the schema independently (10 agents = 10x cost)
- Schema changes break agents silently — no one knows until production fails
- Agents access PII columns unknowingly — compliance risk with every query
- Agents guess enum values and write wrong SQL — silent data quality issues

## What dbook Does

Connects to any database, introspects the schema, and generates structured metadata that gives agents the context DDL lacks:

<p align="center">
  <img src="docs/architecture.svg" alt="dbook Architecture" width="800">
</p>

```bash
pip install dbook
dbook compile "postgresql://user:pass@host/db" --output ./my_dbook
```

### What agents get:

**1. Enum value documentation** — auto-detected via `SELECT DISTINCT`
```
status: pending, confirmed, shipped, delivered, cancelled
method: credit_card, debit_card, paypal, bank_transfer
```

**2. Semantic FK descriptions** — agents understand relationships
```
→ users via user_id — the customer who placed this order
← order_items.order_id — line items in this order
```

**3. Example queries** — patterns agents can follow
```sql
- By status: SELECT * FROM orders WHERE status IN ('pending', 'confirmed')
- Revenue over time: SELECT DATE(created_at), SUM(total) FROM orders GROUP BY DATE(created_at)
```

**4. Auto-detected metrics** — common aggregations ready to use
```
- Total Amount: SELECT SUM(total) FROM orders
- Count by Status: SELECT status, COUNT(*) FROM orders GROUP BY status
- Amount over time: SELECT DATE(created_at), SUM(total) FROM orders GROUP BY DATE(created_at)
```

**5. Data lineage** — how tables connect in the data flow
```
Source tables: users, products (no dependencies)
Intermediate: orders → depends on users | ← used by order_items, invoices
Leaf: payments → depends on invoices
```

**6. PII detection** — marks sensitive columns, redacts sample data
```
| email | VARCHAR(255) | EMAIL (0.90) | high |
| card_last_four | VARCHAR(4) | CREDIT_CARD_PARTIAL (0.70) | low |
```

**7. Query validation** — SQLGlot-powered, catches errors before execution
```python
validator = QueryValidator(book)
result = validator.validate("SELECT * FROM orders WHERE status = 'completed'")
# Warning: 'completed' not in known values: pending, confirmed, shipped, delivered, cancelled
```

## Key Benchmark Results

### SQL Execution Benchmark: DDL vs dbook

Tested on an Amazon-like e-commerce database (34 tables, 15 business tasks, 4 agent types):

| Fact Type | Raw DDL | Base dbook | LLM dbook |
|-----------|---------|-----------|-----------|
| Structural (column names) | 100% | 100% | 100% |
| Value-level (enum values) | 21% | 88% | 94% |
| **Overall key fact coverage** | **76%** | **96%** | **98%** |
| **SQL execution correctness** | **75%** | **100%** | **100%** |

**In the SQL execution benchmark, dbook achieves 100% correct SQL vs 75% with raw DDL** — the difference between agents that guess enum values and agents that know them.

### On a 5-table database:
- DDL key fact coverage: 69% -> dbook: 93% (+24% improvement)
- SQL execution benchmark: DDL produces 75% correct SQL -> dbook: 100% correct SQL

### Agent Discovery (business-term search):
- 15 real business tasks (billing, sales, support, analytics agents)
- All 3 modes achieve 15/15 success with mechanical aliases
- Business terms like "shopping cart", "refund", "A/B test" correctly map to tables

### Token Savings (at scale):
- 50 tables: ~50% fewer tokens per query vs reading all DDL
- Scales linearly — larger databases see larger savings

## Architecture

```
SQLAlchemy Inspector → BookMeta → Compiler → Output Directory
                                     ↓
                      NAVIGATION.md    (table overview + lineage)
                      schemas/
                        {schema}/
                          _manifest.md  (schema details + relationships)
                          {table}.md    (columns, values, FKs, metrics, examples)
```

### Catalog Protocol
Database-agnostic via `Catalog` protocol. Default `SQLAlchemyCatalog` supports any SQLAlchemy-compatible database. DB type auto-detected from URL.

### Supported Databases
PostgreSQL, MySQL, SQLite, Snowflake, BigQuery — any database with a SQLAlchemy dialect.

## Usage

### Full compile
```bash
dbook compile "postgresql://user:pass@host/db" --output ./my_dbook
```

### With PII detection (marks sensitive columns, redacts sample data)
```bash
pip install dbook[pii]
dbook compile "postgresql://..." --output ./my_dbook --pii
```

### With LLM enrichment (semantic summaries, concept aliases)
```bash
pip install dbook[llm]
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
| PII detection | `pip install dbook[pii]` | `--pii` | Column sensitivity markers, sample data redaction |
| LLM enrichment | `pip install dbook[llm]` | `--llm` | Semantic summaries, concept aliases, schema narratives |
| Metrics | `pip install dbook[metrics]` | `--metrics` | User-defined canonical business metrics |

## The Silver Layer Insight

Traditional data pipelines create gold layers because consumers can't read raw data. With dbook, AI agents can understand silver directly — reducing the need for gold views for discovery and ad-hoc queries.

<p align="center">
  <img src="docs/silver-layer.svg" alt="The Silver Layer Insight" width="800">
</p>

> **Note:** dbook reduces the need for gold views for discovery and ad-hoc queries.
> Gold layers still provide value for: enforced business rules, canonical metric
> definitions, data quality guarantees, and grain standardization. For critical metrics,
> define them in `metrics.yaml` — dbook includes them in its output so agents use the
> canonical definition, not their own interpretation.

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -q --tb=short
```

118 tests covering: introspection, compilation, CLI, PII detection, LLM enrichment, query validation, and realistic agent simulation benchmarks.

## License

Apache License 2.0
