# dbook — Development Rules

## What This Project Is
A metadata compiler that introspects databases and generates a layered directory of markdown files for AI agent consumption. The output format IS the product — agents read these files to understand databases.

## Golden Rules

### 1. Never sacrifice output quality to pass a test
If a test has unrealistic targets, fix the test — do NOT strip content from the output to make the number look better. The output must be genuinely useful for an AI agent. A compact but useless output is worse than a verbose but informative one.

### 2. The output format is a UX decision
Every markdown file an agent reads is a user interface. Treat it with the same care as a frontend component:
- NAVIGATION.md MUST use markdown tables for schema listings (not bullet lists)
- Table .md files MUST show all columns with Type, Nullable, Default, PK, and Comment
- Sample data MUST show 3-5 rows with ALL columns (not truncated)
- Foreign keys MUST show full qualified references
- Referenced By sections MUST use schema-qualified names

### 3. Benchmark against realistic scale
- The primary test fixture (13 tables) tests correctness
- Token savings benchmarks MUST also run against a scaled fixture (50+ tables)
- All 10 benchmark questions must be tested, not a subset
- Per-question token breakdown must be reported
- Baseline = tokens for ALL raw DDL; per-question cost = tokens agent actually reads

### 4. Progressive disclosure is the value proposition
Token savings come from the ARCHITECTURE (read 2-3 files instead of all tables), not from compacting individual files. If NAVIGATION.md is 150 tokens and a table file is 200 tokens, that's fine — the savings come from NOT reading the other 49 table files.

### 5. Don't over-filter the concept index
The concept index should include ALL meaningful terms extracted from table and column names. Filtering out "common" terms like "id", "name", "type" removes the most-searched terms. Only filter truly structural noise: single characters, articles.

### 6. Test what matters
- E2E tests: verify output structure and content completeness
- Benchmark tests: measure token efficiency at realistic scale AND verify agent can find correct answers
- No unit tests in isolation — always test the full pipeline

## Output Format Specifications

### NAVIGATION.md (L0 — must be <300 tokens)
```
# Database Book: {dialect}
Compiled: {timestamp} | Mode: {mode}

## Schemas

| Schema | Tables | Total Rows | Description |
|--------|--------|-----------|-------------|
| auth | 4 | 60,205 | users, sessions, roles, user_roles |
| billing | 6 | 138,550 | orders, invoices, payments, products, ... |

## Quick Reference
- User data: schemas/auth/users.md
- Financial: schemas/billing/_manifest.md

## How to Navigate
1. Read this file for overview
2. Check concepts.json to find specific terms
3. Read schemas/{name}/_manifest.md for schema details
4. Read schemas/{name}/{table}.md for full table metadata
```

### Per-Table .md (L2 — no size limit, completeness matters)
Must include ALL of these sections when applicable:
- Header with mechanical summary
- Columns table (ALL columns, ALL attributes)
- Primary Key
- Foreign Keys with full references
- Indexes
- Sample Data (3-5 rows, all columns)
- Referenced By (schema-qualified)

### concepts.json (Ls)
- Include all terms from splitting table/column names on underscores and camelCase
- Only filter: single characters, pure numbers
- Each term maps to tables (file paths) and columns (qualified names)
- aliases field present but empty in base mode (populated by LLM in Phase 5)

## Architecture

```
SQLAlchemyCatalog → BookMeta → Compiler → Markdown Directory
                                  ↓
                    generators/navigation.py  → NAVIGATION.md
                    generators/manifest.py    → _manifest.md
                    generators/table.py       → {table}.md
                    generators/concepts.py    → concepts.json
                    generators/checksums.py   → checksums.json
```

## Commands
```bash
pip install -e ".[dev]"          # Install in dev mode
pytest tests/ -q --tb=short      # Run all tests
pytest tests/ -v -s              # Run with verbose output (see benchmark reports)
```

## Key Files
- src/dbook/models.py — Data models (ColumnInfo, TableMeta, BookMeta, etc.)
- src/dbook/catalog.py — Catalog protocol + SQLAlchemyCatalog
- src/dbook/hasher.py — SHA256 schema hashing
- src/dbook/compiler.py — Compile BookMeta → markdown directory
- src/dbook/generators/ — Individual markdown/json generators
- tests/conftest.py — SQLite test fixture (13 tables)
- tests/benchmark_helpers.py — AgentSimulator, token counting, BenchmarkReport
- PLAN.md — Full implementation plan (7 phases)
