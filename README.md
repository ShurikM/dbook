# dbook

A database metadata compiler that makes AI agents understand your database — not just its structure, but its meaning.

> **dbook** compiles your database schema into AI-ready metadata — enum values, semantic relationships, example queries, auto-detected metrics, data lineage, and PII markers. Agents with dbook write correct SQL 96% of the time vs 76% with raw DDL.

## The Problem

AI agents writing SQL against databases fail because raw DDL lacks context:
- `status VARCHAR(20)` — what values are valid? (pending? active? 0/1/2?)
- `total FLOAT` — gross or net? with tax? in dollars or cents?
- `user_id INTEGER REFERENCES users(id)` — but what IS the relationship semantically?

Our benchmarks show: **agents with raw DDL have only 76% of the facts needed for correct SQL. With dbook: 96%.**

## What dbook Does

Connects to any database, introspects the schema, and generates structured metadata that gives agents the context DDL lacks:

```mermaid
graph LR
    DB[(Any Database)] -->|SQLAlchemy| I[Introspector]
    I -->|Schema + Data| E{Enrichment}
    E -->|Column Patterns| PII[PII Scanner]
    E -->|SELECT DISTINCT| ENUM[Enum Detector]
    E -->|FK Analysis| LIN[Lineage Builder]
    E -->|Optional| LLM[LLM Enricher]

    PII --> C[Compiler]
    ENUM --> C
    LIN --> C
    LLM -.-> C

    C --> NAV[NAVIGATION.md<br/>Table overview + lineage]
    C --> TBL[Table .md files<br/>Columns, values, FKs,<br/>metrics, examples]
    C --> MAN[_manifest.md<br/>Schema relationships]

    NAV --> AGENT[AI Agent]
    TBL --> AGENT
    MAN --> AGENT

    AGENT -->|Writes SQL| V[Query Validator<br/>SQLGlot]
    V -->|Valid ✓| DB
    V -->|Errors/Warnings| AGENT

    style DB fill:#4a90d9,stroke:#2d6cb4,color:#fff
    style AGENT fill:#22c55e,stroke:#16a34a,color:#fff
    style V fill:#f59e0b,stroke:#d97706,color:#fff
    style NAV fill:#8b5cf6,stroke:#7c3aed,color:#fff
    style TBL fill:#8b5cf6,stroke:#7c3aed,color:#fff
    style MAN fill:#8b5cf6,stroke:#7c3aed,color:#fff
    style LLM fill:#64748b,stroke:#475569,color:#fff
```

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

### Agent Correctness: DDL vs dbook

```mermaid
graph TB
    subgraph DDL["Raw DDL (76% facts)"]
        D1[Table names ✓]
        D2[Column names ✓]
        D3[Column types ✓]
        D4[FK references ✓]
        D5["Enum values ✗"]
        D6["Semantic meaning ✗"]
        D7["Query patterns ✗"]
        D8["Data lineage ✗"]
        D9["PII markers ✗"]
        D10["Metrics ✗"]
    end

    subgraph DBOOK["dbook (96% facts)"]
        B1[Table names ✓]
        B2[Column names ✓]
        B3[Column types ✓]
        B4[FK references ✓]
        B5["Enum values ✓<br/>pending, shipped, delivered..."]
        B6["FK semantics ✓<br/>→ users (the customer)"]
        B7["Example queries ✓<br/>SELECT ... JOIN ... WHERE ..."]
        B8["Data lineage ✓<br/>source → intermediate → leaf"]
        B9["PII markers ✓<br/>EMAIL, PHONE, SSN"]
        B10["Auto metrics ✓<br/>SUM(total) GROUP BY status"]
    end

    style DDL fill:#1e293b,stroke:#334155,color:#94a3b8
    style DBOOK fill:#0f172a,stroke:#22c55e,color:#e2e8f0
    style D5 fill:#ef4444,stroke:#dc2626,color:#fff
    style D6 fill:#ef4444,stroke:#dc2626,color:#fff
    style D7 fill:#ef4444,stroke:#dc2626,color:#fff
    style D8 fill:#ef4444,stroke:#dc2626,color:#fff
    style D9 fill:#ef4444,stroke:#dc2626,color:#fff
    style D10 fill:#ef4444,stroke:#dc2626,color:#fff
    style B5 fill:#22c55e,stroke:#16a34a,color:#fff
    style B6 fill:#22c55e,stroke:#16a34a,color:#fff
    style B7 fill:#22c55e,stroke:#16a34a,color:#fff
    style B8 fill:#22c55e,stroke:#16a34a,color:#fff
    style B9 fill:#22c55e,stroke:#16a34a,color:#fff
    style B10 fill:#22c55e,stroke:#16a34a,color:#fff
```

Tested on an Amazon-like e-commerce database (34 tables, 15 business tasks, 4 agent types):

| Fact Type | Raw DDL | Base dbook | LLM dbook |
|-----------|---------|-----------|-----------|
| Structural (column names) | 100% | 100% | 100% |
| Value-level (enum values) | 21% | 88% | 94% |
| **Overall** | **76%** | **96%** | **98%** |

**dbook adds +20% correctness** — the difference between agents that guess enum values and agents that know them.

### On a 5-table database:
- DDL key facts: 69% → dbook: 93% (+24% improvement)
- dbook wins on 6 of 8 SQL tasks

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

## The Silver Layer Insight

Traditional data pipelines create gold layers because consumers can't read raw data. With dbook, AI agents can understand silver directly — the agent becomes the gold layer, building views on-demand for each question.

```mermaid
graph LR
    subgraph Traditional["Traditional Pipeline"]
        TB[Bronze<br/>200 tables] -->|ETL| TS[Silver<br/>20 tables] -->|ETL| TG[Gold<br/>5 tables] --> TC[Consumer]
    end

    subgraph WithDbook["With dbook"]
        WB[Bronze<br/>200 tables] -->|ETL| WS[Silver<br/>20 tables] -->|dbook compile| WD[dbook metadata] --> WA[AI Agent<br/>builds gold on-demand]
    end

    style TB fill:#cd7f32,stroke:#8b5a2b,color:#fff
    style TS fill:#c0c0c0,stroke:#808080,color:#000
    style TG fill:#ffd700,stroke:#b8860b,color:#000
    style TC fill:#64748b,stroke:#475569,color:#fff
    style WB fill:#cd7f32,stroke:#8b5a2b,color:#fff
    style WS fill:#c0c0c0,stroke:#808080,color:#000
    style WD fill:#8b5cf6,stroke:#7c3aed,color:#fff
    style WA fill:#22c55e,stroke:#16a34a,color:#fff
```

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -q --tb=short
```

118 tests covering: introspection, compilation, CLI, PII detection, LLM enrichment, query validation, and realistic agent simulation benchmarks.

## License

Apache License 2.0
