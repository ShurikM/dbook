# dbook Roadmap

> Database metadata compiler for AI agent consumption.
> CogniMesh handles governance. dbook handles understanding.

## References

| Document | Key Insight | Location |
|----------|-------------|----------|
| Meta Analytics Agent | Metadata-first approach, semantic layer, domain scoping | [Medium article](https://medium.com/@AnalyticsAtMeta/inside-metas-home-grown-ai-analytics-agent-4ea6779acfb3) |
| MLOps Memory Layer | Schema-as-memory, FK traversal graphs, multi-granularity metadata, embeddings | [MLOps Community article](https://mlops.community/engineering-the-memory-layer-for-an-ai-agent-to-navigate-large-scale-event-data/) |
| Meta deep analysis | dbook vs Meta's Ingredients comparison | `docs/research/meta_analytics_agent_analysis.md` (in CogniMesh repo) |
| MLOps deep analysis | dbook vs memory layer comparison | `docs/research/mlops_memory_layer_analysis.md` (in CogniMesh repo) |

## Current State (v0.1.0)

- SQLAlchemy-based introspection (Postgres, MySQL, SQLite, Snowflake, BigQuery)
- Rich metadata: columns, FKs, indexes, enums, sample data, row counts
- PII detection (Presidio)
- LLM enrichment (summaries, concept aliases, narratives)
- QueryValidator (SQLGlot)
- SHA256 change detection
- Progressive-disclosure markdown output (L0/L1/L2)
- Concept index (term -> table/column mapping)
- Integrated into CogniMesh via DbookBridge

---

## Phase 2: Schema-as-Memory (P1)

**Inspired by:** MLOps article -- "the schema IS the agent's long-term memory"

The MLOps article's central thesis is that the schema represents the most consequential decision in the entire pipeline. In the context of Agentic RAG, the schema acts as the agent's long-term memory structure. dbook already compiles rich metadata, but these enhancements transform it into a true memory artifact that agents load once and reason from throughout their session.

### 2.1 Pre-Compiled FK Traversal Paths

Compile FK relationships into navigable graph edges stored in BookMeta:

- **What:** For every table, compute all reachable tables via FK chains (up to depth 3) and store as an adjacency list: `{"customers": {"orders": {"path": "orders.customer_id -> customers.id", "join_clause": "JOIN orders ON orders.customer_id = customers.id"}, "returns": {"path": "returns.customer_id -> customers.id", "join_clause": "JOIN returns ON returns.customer_id = customers.id"}, "payments": {"path": "orders.customer_id -> customers.id -> payments.order_id -> orders.id", "join_clause": "JOIN orders ON ... JOIN payments ON ..."}}}`.
- **Bidirectional:** From `customers` you can reach `orders`. From `orders` you can reach `customers`. Both directions are pre-compiled.
- **Named paths:** Common multi-hop paths get human-readable names: "customer -> orders -> order_items" becomes the "customer purchase detail" path.
- **Markdown output:** Add a "Reachable Tables" section to each table .md file: "From this table, you can reach: orders (1 hop via customer_id), order_items (2 hops via customer_id -> order_id), payments (2 hops via customer_id -> order_id)."
- **Why:** The MLOps article pre-compiles Speaker -> Talk -> Topic traversal paths into graph edges, which "significantly reduces the risk of hallucinations and retrieval errors." Currently agents must reason about FK chains at query time. Pre-compiled paths let T2 composition (in CogniMesh) use ready-made JOIN clauses instead of discovering them.

### 2.2 Multi-Granularity Metadata

Extend metadata to three levels of zoom for different agent needs:

- **Column-level** (exists today): type, nullable, default, enum values, PII markers, sample values. No changes needed.
- **Table-level** (add): purpose summary (mechanical or LLM-generated), primary access patterns ("typically filtered by date + status, aggregated by customer"), typical query shapes ("SELECT with GROUP BY is the dominant pattern"), row count category (small: <1K, medium: 1K-100K, large: 100K-10M, huge: >10M). Derived from schema structure and optionally from CogniMesh audit logs (if available via integration).
- **Domain-level** (add): schema narrative (exists via LLM enrichment), cross-table relationship map ("the billing domain connects customers to their financial transactions via orders, payments, and invoices"), business domain classification (e.g., "financial", "authentication", "analytics"). The domain-level narrative provides context that individual table descriptions cannot.
- **Markdown output:** Table-level metadata goes into each table .md header. Domain-level metadata goes into `_manifest.md` for each schema.
- **Why:** The MLOps article maintains three descriptor sets (chunk-level, talk-level, speaker-level) to enable intent-specific search. Agents asking "what's in this schema?" need domain-level context. Agents composing SQL need column-level detail. Multi-granularity ensures the right level of detail is available without reading everything.

### 2.3 Type-Aware Query Hints

For each column, generate actionable hints that tell agents what operations are valid:

- **Enum columns:** "Filterable by: ['active', 'inactive', 'suspended']. Use equality or IN clause."
- **Date columns:** "Supports range queries (BETWEEN, >, <). Typical grain: daily. Indexed: yes."
- **FK columns:** "Joinable to {referred_table}.{referred_column}. Use INNER JOIN for required relationships, LEFT JOIN for optional."
- **Numeric columns:** "Aggregatable (SUM, AVG, MIN, MAX). Range: [0, 1,000,000]. Supports comparison operators."
- **Text columns:** "Searchable via LIKE or ILIKE. Average length: 45 chars. Not recommended for exact match unless indexed."
- **Boolean columns:** "Filterable. Distribution: 72% true, 28% false." (from sample data analysis)
- **Markdown output:** Add a "Query Hints" subsection to each column in the table .md file.
- **BookMeta:** Add `query_hints: dict[str, str]` to ColumnInfo.
- **Why:** The MLOps article emphasizes that "type strictness directly determines what natural language patterns the agent can handle." Storing `yt_views` as Integer (not String) enables `views > 500`. Query hints make this explicit: the agent does not need to infer what operations are valid from the type alone.

### 2.4 Serializable Memory Artifact

Export the complete BookMeta as a single portable file that agents can load once:

- **Format:** JSON (human-readable, widely supported) with optional MessagePack (compact, fast deserialization) variant.
- **Contents:** All metadata + FK traversal graph + concept index + query hints in one artifact. Everything an agent needs to understand the database without re-introspecting.
- **Versioning:** Include schema hash (aggregated SHA256 of all table hashes) as the artifact version. If the hash matches the loaded artifact, no re-compilation needed.
- **Cache invalidation:** Agents compare the artifact's schema hash against a lightweight `dbook check` call. If hashes differ, re-download the artifact. If identical, use the cached version.
- **Size budget:** For a 50-table database, the JSON artifact should be <500KB. For 500 tables, <5MB. Include compression (gzip) option.
- **CLI:** `dbook compile --output-format json` produces `dbook_artifact.json` alongside the markdown directory.
- **Why:** Agents should not re-introspect every session. Load the compiled artifact once, use it until the schema changes. This is the operational realization of "schema-as-memory" -- the memory is a file the agent loads at startup.

---

## Phase 3: Discovery and Search (P2)

**Inspired by:** MLOps article -- embedding-based discovery, constrained search; Meta -- team-curated semantic layer

### 3.1 Embedding-Based Semantic Search

Add optional vector embeddings for semantic table and column discovery:

- **What to embed:** Table descriptions (summary + column list), column purposes (from LLM enrichment), concept index entries.
- **Embedding model:** Configurable. Default: sentence-transformers `all-MiniLM-L6-v2` (local, no API key needed, 384 dimensions). Optional: OpenAI `text-embedding-3-small`, Google `text-embedding-004`.
- **Storage:** Embeddings stored alongside BookMeta in the serializable artifact (Phase 2.4). For JSON output, base64-encoded float arrays. For MessagePack, native binary.
- **Search API:** `BookMeta.search(query: str, top_k: int = 5) -> list[SearchResult]`. Returns ranked tables/columns with similarity scores.
- **Use case:** "Find tables related to customer churn" -> returns `customer_profiles` (0.89), `orders` (0.76), `support_tickets` (0.71). The concept index handles exact matches; embeddings handle semantic similarity ("churn" matches "risk_score", "days_since_last_order").
- **Optional dependency:** `pip install dbook[embeddings]`. Without it, search falls back to concept index keyword matching.
- **Why:** The MLOps article uses three embedding indexes for intent-specific search. dbook's concept index is keyword-based -- it misses synonyms and paraphrases. Embeddings close this gap.

### 3.2 Concept Index Enrichment

Allow manual concept overrides and business-level mappings on top of auto-generated concepts:

- **Manual overrides:** A `concepts_override.json` file that teams maintain alongside dbook output. Entries in the override file take precedence over auto-generated entries.
- **Business-level concepts:** Support multi-column concepts: `"revenue": {"tables": ["orders", "payments"], "columns": ["orders.total_amount", "order_items.subtotal", "payments.amount"], "description": "Net revenue after refunds", "aliases": ["sales", "income", "earnings"]}`.
- **Merge logic:** Auto-generated concepts provide the baseline. Manual overrides add, modify, or suppress entries. The merged result is what agents see.
- **Why:** Meta's Ingredients layer is hand-curated because structure alone cannot capture business semantics. "revenue" means "net revenue after refunds" -- no amount of schema introspection discovers this. The override mechanism lets teams encode institutional knowledge without modifying auto-generated output.

### 3.3 Schema Diff Reports

Compare two BookMeta snapshots and generate human-readable diff reports:

- **Input:** Two BookMeta objects (or two serialized artifacts from different points in time).
- **Output:** Structured diff: `{added_tables: [...], removed_tables: [...], modified_tables: [{table: "orders", changes: [{type: "column_added", column: "discount_code", details: "VARCHAR(50), nullable"}, {type: "fk_removed", details: "orders.promo_id -> promotions.id"}]}]}`.
- **Markdown report:** Human-readable diff for PR reviews: "## Schema Changes\n- **Added:** billing.refunds (5 columns)\n- **Modified:** billing.orders -- added column discount_code (VARCHAR(50))\n- **Removed FK:** orders.promo_id -> promotions.id".
- **CLI:** `dbook diff artifact_v1.json artifact_v2.json` produces the diff report.
- **Why:** SHA256 tells you IF something changed. Schema diff tells you WHAT changed. Essential for reviewing database migrations in pull requests and understanding the impact of schema evolution on downstream agents and UCs.

### 3.4 dbt Semantic Layer Integration

Import dbt's semantic definitions to combine curated business metrics with dbook's auto-generated metadata:

- **What:** When a dbt project exists alongside the database, dbook reads `semantic_models.yml` and `metrics.yml` to extract curated metric definitions, dimensions, and entity relationships.
- **High-confidence tier:** Metrics imported from dbt are marked as "curated" in the concept index and table files. Agents see a clear signal: "this metric has a canonical definition" vs. "this metric was auto-detected from column patterns."
- **Output:** Each table .md file gains a "Curated Metrics" section (from dbt) distinct from "Auto-Detected Metrics" (from dbook). The concept index maps business terms from dbt's semantic models to tables/columns with a `source: "dbt"` marker.
- **Fallback pattern:** For questions matching a dbt metric, agents use the canonical definition. For everything else, they fall back to dbook's schema-guided metadata. This implements the hybrid pattern recommended by dbt's own benchmarks.
- **CLI:** `dbook compile "postgresql://..." --dbt-project ./my_dbt_project` discovers and imports semantic definitions automatically.
- **Why:** dbt's 2026 benchmarks show semantic layers achieve 98-100% accuracy for in-scope queries vs. ~62-90% for text-to-SQL. Importing these definitions gives dbook users the best of both worlds — guaranteed accuracy for modeled metrics, universal coverage for everything else — without requiring agents to interact with two separate systems.

---

## Phase 4: MCP Server (P1)

### 4.1 Standalone dbook MCP Server

Expose dbook as an independent MCP server for agent access to schema intelligence:

- **5 tools:**
  - `dbook_introspect` -- Introspect a database and return BookMeta summary (schema list, table counts, high-level overview). For first-time exploration.
  - `dbook_validate_sql` -- Validate a SQL query against the compiled schema. Returns: valid/invalid, error details, suggested fixes. Uses SQLGlot + BookMeta.
  - `dbook_search` -- Search the concept index (and optionally embeddings) for tables/columns matching a term. Returns ranked results.
  - `dbook_table_detail` -- Get full metadata for a specific table: columns, FKs, indexes, query hints, sample data, FK traversal paths.
  - `dbook_check_drift` -- Compare current live schema against last compilation. Returns list of added/modified/removed tables.
- **Separation of concerns:** Agents use dbook MCP for understanding (explore schema, discover tables, validate SQL). Agents use CogniMesh MCP for governed access (execute queries, get audited results). The two servers complement each other.
- **Standalone value:** dbook MCP is useful even without CogniMesh. Any agent that needs to understand a database schema benefits from dbook, whether or not governed query execution is needed.
- **Why:** dbook currently requires CogniMesh to surface its intelligence to agents. An independent MCP server makes dbook useful as a standalone tool for any AI agent workflow.

### 4.2 Claude Desktop Integration

Zero-config integration with Claude Desktop and other MCP-compatible clients:

- **Config:** Add dbook MCP to `claude_desktop_config.json`:
  ```json
  {
    "mcpServers": {
      "dbook": {
        "command": "dbook",
        "args": ["mcp", "--db-url", "postgresql://user:pass@host/db"]
      }
    }
  }
  ```
- **Auto-discovery:** On first connection, dbook introspects the database and compiles BookMeta. Subsequent connections use cached artifact if schema hash matches.
- **Works with any database:** Point at any SQLAlchemy-compatible URL. PostgreSQL, MySQL, SQLite, Snowflake, BigQuery.
- **Why:** Claude Desktop is the most accessible MCP client. Making dbook a one-line config addition lowers the adoption barrier to near zero.

---

## Phase 5: Output Formats (P2)

### 5.1 Agent-Optimized JSON Output

Add a structured JSON output format alongside the existing progressive-disclosure markdown:

- **Current:** Markdown directory (NAVIGATION.md, _manifest.md, table.md files). Optimized for human reading and progressive disclosure.
- **Add:** Single JSON file containing all metadata in a structured format optimized for programmatic consumption. Agents that parse JSON are more reliable than agents that parse markdown tables.
- **Structure:** `{dialect, schemas: {name: {tables: {name: {columns: [...], fks: [...], indexes: [...], query_hints: {...}, fk_traversal: {...}}}}}, concept_index: {...}, schema_hashes: {...}}`.
- **Token efficiency:** Verify the claim that compiled metadata is ~90% smaller than raw DDL at scale (50+ tables). Measure: tokens(dbook JSON) vs tokens(raw CREATE TABLE statements).
- **CLI:** `dbook compile --format json` (default: markdown). `dbook compile --format both` for both outputs.
- **Why:** Markdown is great for progressive disclosure (agents read only what they need). JSON is great for programmatic access (agents load the full artifact once). Both formats serve different agent architectures.

### 5.2 Context Window Budgeting

Given a token budget, generate the optimal metadata subset:

- **API:** `BookMeta.within_budget(token_budget: int, focus: str | None = None) -> str`. Returns the best metadata that fits within the budget.
- **Prioritization:** Most-queried tables first (if audit data available), then largest tables, then tables matching the focus term. Within tables: primary key, FKs, and indexed columns first. Enum values and sample data last (they consume the most tokens but are least critical).
- **Progressive truncation:** At 4K tokens, include full metadata for 2-3 focus tables + summary for the rest. At 8K tokens, include full metadata for 5-7 tables. At 16K tokens, include everything.
- **Use case:** An agent with a 4K token context window asks about revenue. `BookMeta.within_budget(4000, focus="revenue")` returns full detail for `orders`, `payments`, `daily_revenue` and a one-line summary for everything else.
- **Why:** The MLOps article emphasizes "ensuring that the retrieved context is high-quality and token-efficient." Not all agents have large context windows. Budget-aware output ensures dbook works within any constraint.

---

## Phase 6: Scale and Quality (P3)

### 6.1 Async Introspection

Add async introspection for large schemas:

- **Current:** Synchronous SQLAlchemy Inspector. Tables are introspected sequentially. For a 50-table database this takes ~5 seconds. For 1000 tables, it would take ~100 seconds.
- **Add:** Async introspection using `asyncpg` (Postgres) or `aiomysql` (MySQL) with connection pooling. Introspect tables in parallel (configurable concurrency, default 10).
- **Expected improvement:** 1000-table introspection drops from ~100s to ~15s (limited by database connection pool, not client serialization).
- **Fallback:** Async is optional. If async driver is not installed, fall back to synchronous SQLAlchemy. `pip install dbook[async-postgres]`.
- **Why:** Synchronous introspection blocks startup at scale. The MLOps article separates ingestion from querying -- dbook should similarly ensure that compilation does not block serving.

### 6.2 Dialect-Specific Enrichment

Extract dialect-specific metadata that SQLAlchemy's generic Inspector misses:

- **Postgres:** `pg_stat_user_tables` for row count estimates without `COUNT(*)`, `pg_description` for column comments, `pg_total_relation_size` for table sizes, `pg_indexes` for index types (btree, gin, gist, brin).
- **MySQL:** `information_schema.STATISTICS` for index cardinality, `information_schema.TABLES` for row estimates and data length, table and column comments from `COMMENT` attribute.
- **Snowflake:** `SHOW COLUMNS` for extended column metadata, `DESCRIBE TABLE` for clustering keys, `INFORMATION_SCHEMA.TABLE_STORAGE_METRICS` for size.
- **BigQuery:** `INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` for nested/repeated column handling, `INFORMATION_SCHEMA.TABLE_OPTIONS` for table descriptions, partition and clustering column metadata.
- **Implementation:** Per-dialect enrichment classes that run after the generic SQLAlchemy introspection and augment BookMeta with dialect-specific details.
- **Why:** Generic SQLAlchemy Inspector captures ~80% of useful metadata. The remaining 20% (row estimates without COUNT(*), index types, partition info) requires dialect-specific queries. This 20% is often the most operationally useful metadata.

### 6.3 PyPI Release (P1)

Publish dbook as a standalone package on PyPI:

- **Package:** `dbook` on PyPI.
- **Extras:** `dbook[postgres]`, `dbook[mysql]`, `dbook[snowflake]`, `dbook[bigquery]`, `dbook[pii]`, `dbook[llm]`, `dbook[embeddings]`, `dbook[async-postgres]`.
- **Minimal install:** `pip install dbook` gets SQLite support + markdown output + concept index + query validator. Everything else is opt-in.
- **Versioned releases:** Semantic versioning, changelog, migration guide for breaking changes.
- **CLI entry point:** `dbook compile`, `dbook check`, `dbook diff`, `dbook mcp` available after install.
- **Why:** dbook's value proposition is frictionless schema understanding. `pip install dbook && dbook compile sqlite:///my.db` should work in 30 seconds.

---

## Priority Matrix

| Item | Impact | Effort | Priority |
|------|--------|--------|----------|
| 2.1 FK traversal paths | High | Low | P1 |
| 2.4 Serializable artifact | High | Low | P1 |
| 4.1 MCP server | High | Medium | P1 |
| 6.3 PyPI release | High | Low | P1 |
| 2.2 Multi-granularity metadata | High | Medium | P2 |
| 2.3 Type-aware query hints | Medium | Low | P2 |
| 3.1 Embedding search | Medium | Medium | P2 |
| 3.2 Concept enrichment | Medium | Low | P2 |
| 3.4 dbt semantic integration | High | Medium | P2 |
| 5.1 JSON output | Medium | Low | P2 |
| 3.3 Schema diff reports | Medium | Low | P2 |
| 4.2 Claude Desktop integration | Medium | Low | P2 |
| 5.2 Context budgeting | Medium | Medium | P3 |
| 6.1 Async introspection | Medium | High | P3 |
| 6.2 Dialect enrichment | Medium | Medium | P3 |
