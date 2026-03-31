# dbook — Detailed Implementation Plan

## Context

**Problem:** AI agents interacting with databases waste massive tokens consuming raw DDL/schema dumps. No existing tool provides a structured, progressive-disclosure metadata artifact optimized for agent consumption.

**Solution:** dbook — a metadata compiler that connects to any database, introspects the schema, and generates a layered, navigable directory of markdown files. Agents read a ~100-token navigation file first, drill into specific tables only when needed, and use a concept index to map business terms to schema objects.

**Key design decisions from discussion:**
- Output is a **directory of markdown files**, not a single JSON blob
- **No version history** — just current state + hash-based change detection
- **Three optional capability tiers:** Base, +Presidio (PII), +LLM (semantic enrichment)
- **No unit tests** — E2E tests with benchmarking measuring token efficiency and agent task completion
- Progressive disclosure: NAVIGATION.md (L0) → concepts.json (Ls) → schema/_manifest.md (L1) → schema/table.md (L2)

---

## Output Structure

```
dbook_output/
  NAVIGATION.md              # L0: catalog (~100-200 tok)
  concepts.json              # Ls: term → table/column mapping
  checksums.json             # table_name → schema_hash for change detection
  schemas/
    auth/
      _manifest.md           # L1: schema overview, table list, relationship map
      users.md               # L2: full table metadata
      sessions.md
    billing/
      _manifest.md
      orders.md
      invoices.md
      payments.md
    analytics/
      _manifest.md
      events.md
      daily_revenue.md
```

---

## Architecture: Catalog Protocol + SQLAlchemy

### Why SQLAlchemy

SQLAlchemy's Inspector API provides ~80% cross-dialect uniformity for schema introspection (tables, columns, PKs, FKs, indexes, constraints, comments). The remaining 20% (row counts, table sizes, partition info, sample data) needs dialect-specific SQL regardless of approach. Reimplementing the uniform 80% would be wasteful.

**DB type detection is automatic** — SQLAlchemy parses the connection URL scheme:
- `postgresql://...` → Postgres dialect
- `mysql://...` → MySQL dialect
- `sqlite:///...` → SQLite dialect
- `snowflake://...` → Snowflake dialect
- `bigquery://...` → BigQuery dialect

### Catalog Protocol

A thin `Catalog` protocol sits between the compiler and SQLAlchemy, allowing future non-SQL catalog sources (Unity Catalog, AWS Glue, Hive Metastore) without changing the compiler.

```python
class Catalog(Protocol):
    """Abstraction over database/catalog metadata sources."""
    def list_schemas(self) -> list[str]: ...
    def list_tables(self, schema: str) -> list[str]: ...
    def introspect_table(self, schema: str, table: str) -> TableMeta: ...

class SQLAlchemyCatalog(Catalog):
    """Default: covers any SQLAlchemy-supported database."""
    def __init__(self, url: str):
        self.engine = create_engine(url)
        self.inspector = inspect(self.engine)
        self.dialect = self.engine.dialect.name

    # Uniform via Inspector (80%):
    # - list_schemas, list_tables, get_columns, get_pk_constraint,
    #   get_foreign_keys, get_indexes, get_unique_constraints, get_table_comment

    # Dialect-specific helpers (20%):
    def _row_count(self, schema, table) -> int: ...
    def _table_size(self, schema, table) -> int | None: ...
    def _sample_data(self, schema, table, limit=5) -> list[dict]: ...
```

### SQLAlchemy Uniformity Matrix

| Feature | Uniform via Inspector | Dialect-Specific |
|---------|----------------------|-----------------|
| List tables/schemas | Yes | - |
| Columns (name, type, nullable, default) | Yes | type representation varies |
| Primary keys | Yes | - |
| Foreign keys | Yes | BigQuery/DynamoDB lack FKs |
| Indexes | Yes | index types vary (btree, gin, gist) |
| Unique constraints | Yes | - |
| Table/column comments | Mostly | SQLite lacks support |
| Row count | - | SELECT COUNT(*) (raw SQL) |
| Table size on disk | - | pg_total_relation_size / information_schema.TABLES / none |
| Partition info | - | Fully dialect-specific |
| Sample data | - | LIMIT vs TOP vs FETCH FIRST |

### Optional Dependencies by DB

| Database | Driver Package | Install |
|----------|---------------|---------|
| PostgreSQL | psycopg2-binary | `pip install dbook[postgres]` |
| MySQL | pymysql | `pip install dbook[mysql]` |
| SQLite | (stdlib) | included |
| Snowflake | snowflake-sqlalchemy | `pip install dbook[snowflake]` |
| BigQuery | sqlalchemy-bigquery | `pip install dbook[bigquery]` |

---

## Test Database (Shared Fixture)

All benchmarks run against a **standard test database** (SQLite in-memory) with realistic structure:

**3 schemas simulated via table prefixes** (SQLite limitation), **~15 tables:**

| Schema | Table | Rows | PII Columns | Purpose |
|--------|-------|------|-------------|---------|
| auth | users | 10,000 | email, phone, name | User accounts |
| auth | sessions | 50,000 | ip_address | Login sessions |
| auth | roles | 5 | - | RBAC roles |
| auth | user_roles | 200 | - | User-role mapping |
| billing | orders | 25,000 | - | Purchase orders |
| billing | order_items | 75,000 | - | Line items per order |
| billing | invoices | 20,000 | contact_email | Invoice records |
| billing | payments | 18,000 | card_last_four | Payment transactions |
| billing | products | 500 | - | Product catalog |
| billing | discounts | 50 | - | Discount codes |
| analytics | events | 100,000 | user_agent, ip_address | Tracking events |
| analytics | daily_revenue | 365 | - | Aggregated daily stats |
| analytics | funnels | 10 | - | Conversion funnels |

**Includes:** PKs, FKs between tables, composite indexes, check constraints, column comments, nullable columns, defaults, enum-like check constraints.

---

## Benchmark Framework

### Agent Simulation

A Python test harness that simulates an agent navigating dbook output:

```python
class AgentSimulator:
    """Simulates an agent consuming dbook output, tracking token usage."""

    def __init__(self, dbook_path: str):
        self.dbook_path = dbook_path
        self.tokens_consumed = 0
        self.files_read = []

    def read_file(self, relative_path: str) -> str:
        """Read a file, counting tokens consumed."""
        content = (Path(dbook_path) / relative_path).read_text()
        self.tokens_consumed += count_tokens(content)
        self.files_read.append(relative_path)
        return content

    def reset(self):
        self.tokens_consumed = 0
        self.files_read = []
```

### Benchmark Questions (10 standard queries)

| # | Question | Expected Answer | Difficulty |
|---|----------|----------------|------------|
| Q1 | "Where is user email stored?" | auth/users.md → email column | Easy |
| Q2 | "How are orders linked to customers?" | billing/orders.md → user_id FK → auth/users | Medium |
| Q3 | "What tables contain financial data?" | billing/payments, billing/invoices, billing/orders | Medium |
| Q4 | "Show me the analytics schema structure" | analytics/_manifest.md → 3 tables | Easy |
| Q5 | "Is there PII in the billing schema?" | invoices.contact_email, payments.card_last_four | Medium (requires Presidio) |
| Q6 | "What's the best way to query revenue by date?" | analytics/daily_revenue or JOIN orders+payments | Hard |
| Q7 | "Which tables reference the users table?" | sessions, user_roles, orders, invoices, events | Medium |
| Q8 | "What indexes exist on the orders table?" | list indexes from orders.md | Easy |
| Q9 | "Find all columns related to timestamps" | created_at, updated_at across multiple tables | Medium |
| Q10 | "What changed since last compile?" | detect schema modifications via checksums | Easy |

### Baseline Measurement

Before any dbook phase, establish baseline: **tokens consumed if agent reads raw DDL for all tables** (simulated via CREATE TABLE statements for entire test DB). This is the "no dbook" scenario.

### Quality Metrics (measured per benchmark question)

| Metric | How Measured | Target |
|--------|-------------|--------|
| **Token Efficiency** | tokens_consumed / baseline_tokens x 100 | <15% of baseline |
| **Accuracy** | correct_answer in agent_output (exact match on table/column names) | 100% Easy, >90% Medium, >80% Hard |
| **Navigation Depth** | len(files_read) to reach answer | avg <3 files per question |
| **First-File Hit Rate** | % of questions answerable from NAVIGATION.md + concepts.json alone | >40% |
| **PII Detection Rate** | detected_pii_columns / actual_pii_columns | >95% (Presidio phase) |
| **Redaction Completeness** | PII values in output files / PII values in source | 0% (no PII leaks) |

### Benchmark Report Format

Each phase produces a benchmark report:

```
BENCHMARK: Phase N — {Phase Name}
══════════════════════════════════
Test DB: 15 tables, 3 schemas, ~299K rows
Mode: {base|+presidio|+llm|+both}

BASELINE: {N} tokens (raw DDL dump)

RESULTS:
  Q1: ✓ 142 tok (1.8% baseline) — 2 files read
  Q2: ✓ 287 tok (3.6% baseline) — 3 files read
  ...
  Q10: ✗ FAILED — could not determine changes

SUMMARY:
  Accuracy: 9/10 (90%)
  Avg tokens/question: 203 tok (2.5% baseline)
  Avg files read: 2.4
  First-file hit rate: 50%
  Token efficiency vs baseline: 97.5% savings

PHASE GATE: {PASS|FAIL} (requires >85% accuracy, >90% token savings)
```

---

## Implementation Phases

---

### PHASE 1: Foundation + Introspection
**Goal:** Connect to DB, extract all metadata, produce Python data structures.

**Dependencies:** None (first phase)

#### Files to Create

| File | Purpose |
|------|---------|
| `pyproject.toml` | Package config: hatchling, sqlalchemy>=2.0, optional deps for DB drivers/pii/llm |
| `src/dbook/__init__.py` | Package root (minimal, exports added later) |
| `src/dbook/models.py` | Dataclasses: ColumnInfo, ForeignKeyInfo, IndexInfo, TableMeta, SchemaMeta, BookMeta |
| `src/dbook/catalog.py` | Catalog protocol + SQLAlchemyCatalog implementation — wraps sqlalchemy.inspect(), dialect-specific helpers for row counts/sizes/samples |
| `src/dbook/hasher.py` | compute_table_hash(TableMeta) -> str — SHA256 canonical hash |
| `tests/conftest.py` | SQLite test DB fixture with all 15 tables, FKs, sample data |
| `tests/benchmark_helpers.py` | AgentSimulator, count_tokens(), BenchmarkReport, baseline measurement |

#### Data Model Design

```python
@dataclass
class ColumnInfo:
    name: str
    type: str                    # e.g., "VARCHAR(255)"
    nullable: bool
    default: str | None
    is_primary_key: bool
    comment: str | None
    pii_type: str | None = None  # Filled by Phase 4 (Presidio)
    pii_confidence: float = 0.0
    sensitivity: str = "none"    # none | low | medium | high

@dataclass
class ForeignKeyInfo:
    columns: tuple[str, ...]
    referred_schema: str | None
    referred_table: str
    referred_columns: tuple[str, ...]
    name: str | None

@dataclass
class IndexInfo:
    name: str | None
    columns: tuple[str, ...]
    unique: bool

@dataclass
class TableMeta:
    name: str
    schema: str | None
    columns: list[ColumnInfo]
    primary_key: tuple[str, ...]
    foreign_keys: list[ForeignKeyInfo]
    indexes: list[IndexInfo]
    row_count: int | None
    comment: str | None
    sample_data: list[dict]       # First 5 rows
    schema_hash: str = ""         # Computed by hasher
    summary: str = ""             # Mechanical or LLM-generated
    column_purposes: dict[str, str] = field(default_factory=dict)

@dataclass
class SchemaMeta:
    name: str
    tables: dict[str, TableMeta]
    narrative: str = ""

@dataclass
class BookMeta:
    database_url: str            # Sanitized (no password)
    dialect: str
    schemas: dict[str, SchemaMeta]
    compiled_at: datetime
    compiler_version: str
    mode: str                    # "base" | "pii" | "llm" | "full"
```

#### Hasher Algorithm

```
canonical = {
    "columns": sorted([{name, type, nullable, default, is_pk} for col]),
    "primary_key": sorted(pk_columns),
    "foreign_keys": sorted([{columns, ref_table, ref_columns}]),
    "indexes": sorted([{columns, unique}]),
    "comment": table_comment
}
hash = sha256(json.dumps(canonical, sort_keys=True))
```

#### Benchmark 1: Introspection Completeness

**What we test:**
- Introspector connects to test DB and extracts all 15 tables
- All columns, FKs, indexes, PKs are captured
- Row counts are accurate
- Sample data is present
- Schema hashes are stable (same input → same hash)
- Hash changes when schema changes (ALTER TABLE → different hash)

**Metrics:**
- Tables discovered: 15/15
- Columns captured: all (exact count per table)
- FK relationships: all (exact count)
- Hash stability: 100% (run twice, same hashes)
- Hash sensitivity: 100% (modify schema, hash changes)

**No token benchmark yet** — no markdown output in this phase.

**Phase Gate:** All structural assertions pass. Hash stability + sensitivity = 100%.

---

### PHASE 2: Markdown Compiler
**Goal:** Transform BookMeta into the layered markdown file structure.

**Dependencies:** Phase 1 (needs BookMeta data structures and introspector)

#### Files to Create

| File | Purpose |
|------|---------|
| `src/dbook/compiler.py` | compile_book(BookMeta, output_dir) — main orchestrator |
| `src/dbook/generators/navigation.py` | Generates NAVIGATION.md from BookMeta |
| `src/dbook/generators/manifest.py` | Generates _manifest.md per schema |
| `src/dbook/generators/table.py` | Generates per-table .md files |
| `src/dbook/generators/concepts.py` | Builds concepts.json — mechanical term extraction |
| `src/dbook/generators/checksums.py` | Writes checksums.json |
| `tests/test_e2e_compile.py` | E2E: introspect test DB → compile → validate output structure |
| `tests/test_benchmark_base.py` | Benchmark: agent simulation against compiled output |

#### NAVIGATION.md Format (L0)

```markdown
# Database Book: {db_name}
Compiled: {timestamp} | Dialect: {dialect} | Mode: {mode}

## Schemas

| Schema | Tables | Total Rows | Description |
|--------|--------|-----------|-------------|
| auth | 4 | 60,205 | Authentication, users, sessions, RBAC |
| billing | 6 | 138,550 | Orders, payments, invoicing, products |
| analytics | 3 | 100,375 | Event tracking, revenue aggregation |

## Quick Reference
- User data: `auth/users.md`
- Financial tables: `billing/_manifest.md`
- Event tracking: `analytics/events.md`

## How to Navigate
1. Read this file for overview
2. Check `concepts.json` to find specific terms
3. Read `schemas/{name}/_manifest.md` for schema details
4. Read `schemas/{name}/{table}.md` for full table metadata
```

#### Per-Table .md Format (L2)

```markdown
# {schema}.{table_name}
{mechanical_summary}

## Columns

| Column | Type | Nullable | Default | PK | Comment |
|--------|------|----------|---------|-----|---------|
| id | INTEGER | no | autoincrement | yes | - |
| email | VARCHAR(255) | no | - | - | User email address |

## Primary Key
`id`

## Foreign Keys
| Column | References | On Delete |
|--------|-----------|-----------|
| user_id | auth.users.id | CASCADE |

## Indexes
| Name | Columns | Unique |
|------|---------|--------|
| idx_orders_user | user_id | no |
| idx_orders_date | created_at | no |

## Sample Data
| id | user_id | total | status | created_at |
|----|---------|-------|--------|-----------|
| 1 | 42 | 99.50 | shipped | 2026-01-15 |
| 2 | 17 | 245.00 | pending | 2026-01-16 |

## Referenced By
- billing.order_items.order_id
- billing.invoices.order_id
```

#### concepts.json Format (Ls)

```json
{
  "user": {
    "tables": ["auth/users.md"],
    "columns": ["auth.users.*", "billing.orders.user_id", "analytics.events.user_id"],
    "aliases": []
  },
  "email": {
    "tables": ["auth/users.md", "billing/invoices.md"],
    "columns": ["auth.users.email", "billing.invoices.contact_email"],
    "aliases": []
  },
  "payment": {
    "tables": ["billing/payments.md"],
    "columns": ["billing.payments.*"],
    "aliases": []
  },
  "revenue": {
    "tables": ["analytics/daily_revenue.md"],
    "columns": ["analytics.daily_revenue.*", "billing.orders.total"],
    "aliases": []
  }
}
```

Mechanical extraction: split column/table names on `_`, `-`, camelCase. Deduplicate. Map each term to all tables/columns containing it.

#### Benchmark 2: Token Efficiency + Agent Accuracy (Base Mode)

**Setup:** Compile test DB → run agent simulator against all 10 benchmark questions.

**Agent navigation protocol** (simulated):
1. Read NAVIGATION.md
2. Based on question, either: check concepts.json OR read a _manifest.md
3. Read specific table .md file(s) as needed
4. Extract answer

**Phase Gate:**
- Accuracy: >=8/10 (Q5 expected fail without Presidio, Q6 partial OK)
- Token savings vs baseline: >=90%
- Avg files read: <=3
- All output files valid markdown
- NAVIGATION.md < 300 tokens
- concepts.json complete (all table/column name terms mapped)

---

### PHASE 3: CLI + Incremental Compilation
**Goal:** dbook CLI commands + incremental recompile (only changed tables).

**Dependencies:** Phase 2 (needs compiler)

#### Files to Create

| File | Purpose |
|------|---------|
| `src/dbook/cli.py` | Click-based CLI: dbook compile, dbook check |
| `src/dbook/incremental.py` | Compare checksums.json vs live DB, recompile only changed tables |
| `tests/test_e2e_cli.py` | E2E: CLI invocation, incremental recompile after schema change |

#### CLI Commands

```bash
# Full compile
dbook compile "postgresql://user:pass@host/db" --output ./my_dbook --schemas auth,billing  # DB type auto-detected from URL

# Check for stale tables (no recompile)
dbook check ./my_dbook "postgresql://user:pass@host/db"

# Incremental recompile (only changed tables)
dbook compile "postgresql://user:pass@host/db" --output ./my_dbook --incremental

# With PII scanning (Phase 4)
dbook compile "postgresql://..." --output ./my_dbook --pii

# With LLM enrichment (Phase 5)
dbook compile "postgresql://..." --output ./my_dbook --llm --llm-provider anthropic --llm-key sk-...
```

#### Incremental Logic

```
1. Load checksums.json from existing output dir
2. Introspect live DB → compute new hashes
3. Compare:
   - hash_old[table] != hash_new[table] → MODIFIED (recompile table .md)
   - table in new but not old → ADDED (generate new table .md)
   - table in old but not new → REMOVED (delete table .md)
   - hash_old == hash_new → UNCHANGED (skip)
4. If any table changed → regenerate _manifest.md for affected schemas
5. If any table added/removed → regenerate NAVIGATION.md + concepts.json
6. Update checksums.json
7. Report: {added, removed, modified, unchanged}
```

#### Benchmark 3: Incremental Efficiency

**Test scenario:**
1. Full compile of test DB → record time + files written
2. ALTER TABLE: add column to auth.users, drop table billing.discounts, add table billing.refunds
3. Run dbook compile --incremental
4. Measure: files rewritten vs total files

**Metrics:**

| Metric | Target |
|--------|--------|
| Files rewritten on incremental | only changed tables + their schema manifests + NAVIGATION.md + concepts.json |
| Unchanged table files | byte-identical (verified via hash) |
| dbook check correctly reports | all added, removed, modified tables |
| Incremental compile time vs full | <30% of full compile time |

**Phase Gate:**
- Incremental correctly detects all 3 changes
- Unchanged files untouched
- CLI returns proper exit codes (0 = no changes, 1 = changes found for check)
- All Phase 2 benchmark questions still pass on incrementally-compiled output

---

### PHASE 4: PII Scanner (Presidio Integration)
**Goal:** Detect and mark sensitive data, redact PII from sample data in output.

**Dependencies:** Phase 2 (needs compiler and table .md generation)

#### Files to Create

| File | Purpose |
|------|---------|
| `src/dbook/pii/__init__.py` | PII module init, graceful import check for presidio |
| `src/dbook/pii/scanner.py` | PIIScanner — wraps Presidio Analyzer, scans columns + sample data |
| `src/dbook/pii/redactor.py` | redact_sample_data(rows, pii_results) — replaces PII with [REDACTED:{type}] |
| `src/dbook/pii/patterns.py` | Column name patterns (regex): ssn, credit_card, phone, dob, etc. |
| `tests/test_e2e_pii.py` | E2E: compile with --pii, verify detection + redaction |

#### PII Detection Strategy

**Layer 1 — Column Name Pattern Matching (no Presidio needed):**
```python
PII_COLUMN_PATTERNS = {
    r"(?i)(email|e_mail)": ("EMAIL", 0.9),
    r"(?i)(phone|mobile|cell)": ("PHONE", 0.9),
    r"(?i)(ssn|social_security)": ("SSN", 0.95),
    r"(?i)(credit_card|card_number|cc_num)": ("CREDIT_CARD", 0.95),
    r"(?i)(first_name|last_name|full_name)": ("PERSON", 0.85),
    r"(?i)(ip_address|ip_addr|remote_ip)": ("IP_ADDRESS", 0.8),
    r"(?i)(date_of_birth|dob|birth_date)": ("DATE_OF_BIRTH", 0.9),
    r"(?i)(address|street|zip|postal)": ("ADDRESS", 0.7),
}
```

**Layer 2 — Presidio Analyzer on Sample Data (requires presidio-analyzer):**
- Run each sample row's string values through Presidio
- Aggregate entity types per column across all sample rows
- Confidence = max confidence across samples

**Output additions:**
- PII and Sensitivity columns added to table .md column tables
- Sample data redacted: PII values replaced with [REDACTED:{type}]
- Sensitivity Overview section added to NAVIGATION.md

#### Benchmark 4: PII Detection + Token Impact

**Test data:** Insert realistic PII into test DB sample rows.

**Metrics:**

| Metric | Target |
|--------|--------|
| Column-name PII detection | >=95% recall |
| Sample-data PII detection (with Presidio) | >=90% recall |
| False positive rate | <10% |
| Sample data redaction completeness | 100% (zero PII in output files) |
| Q5 (PII in billing?) now answerable | Yes |
| Token overhead vs base mode | <15% increase |

**Phase Gate:**
- PII detection recall >=90%
- Zero raw PII values in any output file
- Q5 benchmark now passes
- All other benchmark questions still pass
- Token overhead <15% vs Phase 2 baseline

---

### PHASE 5: LLM Enrichment
**Goal:** Optional LLM pass generating semantic summaries, concept aliases, cross-table narratives.

**Dependencies:** Phase 2 (compiler), Phase 4 nice-to-have (PII context enriches LLM prompts)

#### Files to Create

| File | Purpose |
|------|---------|
| `src/dbook/llm/__init__.py` | LLM module init |
| `src/dbook/llm/provider.py` | Abstract LLM provider + Anthropic/OpenAI/Gemini implementations |
| `src/dbook/llm/enricher.py` | enrich_book(BookMeta, provider) — orchestrates all LLM calls |
| `src/dbook/llm/prompts.py` | Prompt templates for each enrichment type |
| `tests/test_e2e_llm.py` | E2E: compile with --llm, compare output quality |

#### LLM Enrichment Steps

**Step 1 — Table Summaries** (1 LLM call per table):
Input: table name, columns, FKs, sample data, row count
Output: 1-2 sentence semantic summary

**Step 2 — Concept Index Aliases** (1 LLM call for entire concept index):
Input: mechanical concept index + all table/column names
Output: aliases/synonyms per concept

**Step 3 — Schema Narratives** (1 LLM call per schema):
Input: all tables in schema with columns and FKs
Output: paragraph describing the schema's business purpose and data flow

**Step 4 — Column Purpose Descriptions** (batched, 1 call per table):
Input: table context + column names/types
Output: purpose string per column

**Total LLM calls:** ~15 (tables) + ~3 (schemas) + 1 (concepts) = ~19 calls

#### Provider Abstraction

```python
class LLMProvider(Protocol):
    async def complete(self, prompt: str, max_tokens: int = 500) -> str: ...

class AnthropicProvider(LLMProvider): ...  # claude-haiku-4-5
class OpenAIProvider(LLMProvider): ...     # gpt-4o-mini
class GeminiProvider(LLMProvider): ...     # gemini-2.0-flash
```

Default to cheapest models since metadata generation doesn't need frontier reasoning.

#### Benchmark 5: LLM Enrichment Quality + Token Savings

**Comparison:** base mode vs LLM-enriched mode on all 10 benchmark questions.

**Metrics:**

| Metric | Base Mode Target | LLM Mode Target |
|--------|-----------------|-----------------|
| Accuracy | >=8/10 | >=9/10 |
| Avg tokens per question | X | < X (better routing) |
| Concept index hit rate | Z% | Z + 20% |
| First-file hit rate | A% | A + 15% |
| Q6 accuracy | partial | full |

**Phase Gate:**
- All base-mode benchmarks still pass
- Q6 accuracy improves
- Concept index hit rate improves by >=15%
- LLM summaries contain zero factual errors
- Total LLM cost < $0.05 for test DB

---

### PHASE 6: MCP Server
**Goal:** Expose dbook as an MCP server for runtime agent access.

**Dependencies:** Phase 2 (needs compiled output)

#### Files to Create

| File | Purpose |
|------|---------|
| `src/dbook/mcp/__init__.py` | MCP module |
| `src/dbook/mcp/server.py` | FastMCP server with 4 tools |
| `tests/test_e2e_mcp.py` | E2E: start MCP server, call tools, verify responses |

#### MCP Tools

```python
@mcp.tool()
def browse_schemas() -> str:
    """Returns NAVIGATION.md content — schema overview."""

@mcp.tool()
def read_table(schema: str, table: str) -> str:
    """Returns full table .md content for a specific table."""

@mcp.tool()
def search_concepts(term: str) -> str:
    """Searches concepts.json for matching terms."""

@mcp.tool()
def check_freshness() -> str:
    """Compares checksums.json vs live DB. Reports stale tables."""
```

Token budget caps per tool: browse_schemas 500 tok, read_table 2000 tok, search_concepts 500 tok, check_freshness 200 tok.

#### Benchmark 6: MCP vs File-Based Access

Same 10 questions, agent uses MCP tools instead of reading files.

| Metric | Target |
|--------|--------|
| All 10 questions answerable | Yes |
| Token usage | <= file-based mode |
| Avg tool calls per question | <=3 |
| Response latency per tool | <100ms |

**Phase Gate:** All questions answerable, token-equivalent or better than file mode.

---

### PHASE 7: Claude Code Skill
**Goal:** SKILL.md teaching agents the navigation protocol for file-based dbook access.

**Dependencies:** Phase 2 (needs output format finalized)

#### Files to Create

| File | Purpose |
|------|---------|
| `skills/dbook-navigator/SKILL.md` | Navigation protocol instructions |

#### Benchmark 7: Skill-Guided vs Unguided

| Metric | Without Skill | With Skill Target |
|--------|--------------|-------------------|
| Avg tokens per question | X | < X |
| Navigation efficiency | A files | < A files |
| Accuracy | C% | >= C% |

**Phase Gate:** Skill-guided agent uses fewer tokens on average.

---

## Dependency Graph

```
PHASE 1: Foundation + Introspection
    │
    ▼
PHASE 2: Markdown Compiler ──────────────────┐
    │                                         │
    ├─────────────┬──────────────┐            │
    ▼             ▼              ▼            │
PHASE 3:     PHASE 4:       PHASE 6:         │
CLI +        PII Scanner    MCP Server        │
Incremental      │                            │
                 ▼                            │
             PHASE 5:                         │
             LLM Enrichment                   │
                                              ▼
                                         PHASE 7:
                                         Claude Code Skill
```

**Parallelizable after Phase 2:** Phases 3, 4, 6, 7 are independent. Phase 5 benefits from Phase 4 but doesn't require it.

---

## Execution Plan

| Wave | Phases | Parallel? | Agents |
|------|--------|-----------|--------|
| 0 | Phase 1: Foundation | single | 1 general-purpose |
| 1 | Phase 2: Compiler | single | 1 general-purpose |
| 2 | Phase 3: CLI, Phase 4: PII, Phase 6: MCP, Phase 7: Skill | parallel | 4 general-purpose |
| 3 | Phase 5: LLM Enrichment | single | 1 general-purpose |
| 4 | Final benchmark: all modes, all questions | single | 1 task-completion-verifier |

**Execution Mode:** team

---

## Final Acceptance Criteria

| Criterion | Measurement |
|-----------|-------------|
| Token savings vs raw DDL | >=90% across all 10 questions (base mode) |
| Agent accuracy | >=90% on 10 benchmark questions (base), >=95% (LLM mode) |
| PII detection recall | >=90% |
| Zero PII leakage | 0 raw PII values in any output file |
| Incremental compile correctness | detects all schema changes, recompiles only affected files |
| MCP tools functional | all 4 tools return correct data |
| CLI functional | dbook compile and dbook check work end-to-end |

## Risks

| Risk | Mitigation |
|------|-----------|
| SQLite lacks real schemas | Simulate via table name prefixes or use ATTACH DATABASE |
| Non-SQL catalogs (Unity, Glue) | Catalog protocol allows future REST-based adapters without changing compiler |
| Presidio install size (~200MB) | Optional dependency: pip install dbook[pii] |
| LLM costs during testing | Use mock/stub provider for CI; real provider for benchmark runs |
| Token counting accuracy | Use tiktoken with cl100k_base encoding for consistent measurement |
| Large databases (1000+ tables) | Test with scale fixture; ensure NAVIGATION.md stays <500 tokens via truncation |
