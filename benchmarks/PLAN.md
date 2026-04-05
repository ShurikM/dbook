# Realistic dbook Benchmark Suite

## Context
dbook compiles database metadata into layered markdown files (NAVIGATION.md, _manifest.md, table.md) that AI agents read directly. Current benchmarks use in-memory SQLite with small fixtures. We need a realistic benchmark with Docker Postgres, realistic data volumes, and agent scenarios that simulate real business interactions — proving dbook's value vs raw DDL.

## Architecture

```
benchmarks/
├── Makefile                        # make benchmark = setup + seed + compile + run
├── docker-compose.yml              # Postgres 16 on port 5433
├── requirements.txt                # faker, pyyaml, psycopg2-binary
├── schema/
│   ├── 00_schemas.sql              # 7 CREATE SCHEMA statements
│   ├── 01_customers.sql            # 7 tables
│   ├── 02_catalog.sql              # 10 tables
│   ├── 03_orders.sql               # 10 tables
│   ├── 04_billing.sql              # 13 tables
│   ├── 05_analytics.sql            # 10 tables
│   ├── 06_warehouse.sql            # 10 tables
│   └── 07_support.sql              # 10 tables  (~70 tables total)
├── seed/
│   ├── seed_config.yaml            # Row counts per table (~1.2M total)
│   └── seed_data.py                # Faker-based data generation + golden records
├── scenarios/
│   ├── base.py                     # ScenarioSpec, ScenarioResult, AgentSimulatorV2
│   ├── billing.py                  # 5 billing agent scenarios (B1-B5)
│   ├── care.py                     # 5 care agent scenarios (C1-C5)
│   └── sales.py                    # 5 sales agent scenarios (S1-S5)
├── agents/
│   ├── base_agent.py               # BaseAgent: navigate dbook -> generate SQL -> execute -> respond
│   ├── billing_agent.py            # Financial term navigation priority
│   ├── care_agent.py               # Support/order term navigation priority
│   └── sales_agent.py              # Product/inventory term navigation priority
├── judge/
│   ├── mock_judge.py               # Deterministic fact-checking (default, no API key)
│   └── llm_judge.py                # Real LLM judge (optional, BENCHMARK_LLM_KEY)
├── runner.py                       # Orchestrates 15 scenarios x 2 modes
└── report.py                       # Console table + JSON + HTML report
results/
└── benchmark_report.html           # Auto-generated visual report (Chart.js)
```

## Database: 7 Postgres Schemas, ~70 Tables, ~1.2M Rows

| Schema | Tables | Key Tables | Rows |
|--------|--------|-----------|------|
| customers | 7 | accounts (10K), addresses, payment_methods, wishlists, login_history (50K) | ~108K |
| catalog | 10 | products (5K), reviews (25K), inventory (15K), categories, price_history, brands | ~80K |
| orders | 10 | orders (50K), order_items (150K), shipments (45K), returns, carts, status_history (100K) | ~393K |
| billing | 13 | invoices (50K), payments (55K), refunds, subscriptions, gift_cards, promotions, ledger_entries (80K), payment_disputes | ~212K |
| analytics | 10 | page_views (200K), search_queries (30K), sessions (40K), click_events, recommendations | ~444K |
| warehouse | 10 | warehouses (8), locations (500), stock (10K), suppliers, purchase_orders, picking_lists (45K) | ~65K |
| support | 10 | tickets (5K), ticket_messages (15K), agents, teams, escalations, knowledge_base, sla_policies | ~29K |

Laptop-friendly: ~1.2M rows total, ~1GB disk, 256MB shared_buffers.

## 15 Agent Scenarios

### Billing Agent (B1-B5)
- **B1**: "Customer #4521 disputing $89.99 charge from March 15" → payments + invoices + refunds + disputes
- **B2**: "Billing summary for account #1200: 3 months invoices, outstanding balance" → invoices + payments
- **B3**: "Cancel Premium Annual subscription, prorated refund?" → subscriptions + sub_payments + promotions
- **B4**: "Apply promo SPRING25 to order #78234, check validity" → promotions + orders
- **B5**: "Reconcile gift card GC-4455: balance, all transactions" → gift_cards + gift_card_transactions

### Care Agent (C1-C5)
- **C1**: "Order #34567 hasn't arrived, check tracking" → orders + shipments + status_history
- **C2**: "Return item from order #45678, check eligibility" → orders + order_items + shipments + returns
- **C3**: "Escalate ticket #8901, customer had 3 tickets in 2 weeks" → tickets + messages + escalations
- **C4**: "Verify identity: alice@example.com" → accounts + orders + payment_methods + login_history
- **C5**: "Why was I charged twice?" → payments + invoices + orders

### Sales Agent (S1-S5)
- **S1**: "Top-rated electronics products in stock" → products + reviews + categories + inventory
- **S2**: "Check ASIN B00X4WHP5E availability across warehouses" → products + inventory + warehouses
- **S3**: "Cart #9876 total with tax, check stock" → carts + cart_items + products + inventory + tax_rates
- **S4**: "Products frequently bought with product #234" → order_items + orders + products
- **S5**: "Wireless headphones under $100 with reviews" → products + reviews + categories

Each scenario has: expected_tables, expected_columns, expected_facts, golden_sql, difficulty level.

## How It Works

### Two modes compared per scenario:
1. **No dbook (baseline)**: Agent gets `pg_dump --schema-only` (~20K tokens for 70 tables)
2. **With dbook**: Agent reads NAVIGATION.md → finds relevant tables → reads 2-4 table.md files (~1-2K tokens)

### Agent navigation (dbook mode):
1. Read NAVIGATION.md — scan table overview for domain-relevant terms
2. Identify 2-4 relevant tables from Description/Key Columns
3. Read those table.md files — get columns, FKs, enum values, sample data, example queries
4. Construct SQL from metadata
5. Execute against Postgres
6. Format response

### Mock agent (default, no API key):
- Uses keyword matching to find tables in NAVIGATION.md
- Uses golden_sql from scenario spec (tests table discovery + token efficiency, not LLM quality)

### Real LLM agent (optional, BENCHMARK_LLM_KEY):
- Sends context + question to Claude/GPT
- Gets real SQL generation + natural language response

## Judge Scoring (4 dimensions, 1-5 each)

| Dimension | Score 5 | Score 3 | Score 1 |
|-----------|---------|---------|---------|
| Table Discovery | 100% expected tables found | 50% found | <25% found |
| SQL Correctness | Executes, returns results | Executes, empty results | Fails to execute |
| Result Accuracy | All expected facts present | 60% facts | <40% facts |
| Response Quality | All key entities mentioned | 60% mentioned | <40% mentioned |

Mock judge: deterministic keyword/fact matching. LLM judge (optional): Claude scores with temperature=0, median of 3 calls.

## Execution Flow: `make benchmark`

```
make setup    → docker compose up -d, create venv, pip install -e ../[postgres] faker pyyaml
make seed     → psql runs schema/*.sql, python seed_data.py generates 1.2M rows + golden records
make compile  → dbook compile postgresql://bench:bench@localhost:5433/benchdb -o output/dbook
              → pg_dump --schema-only > output/baseline.sql
make run      → python runner.py (15 scenarios x 2 modes, mock judge, console + JSON report)
              → python report.py (generates results/benchmark_report.html, auto-opens in browser)
make clean    → docker compose down -v, rm -rf output/ results/ .venv/
```

Total time: ~2.5 minutes (Docker 10s + Schema 5s + Seed 60s + Compile 45s + Run 30s).

## Golden Records (seeded for deterministic scenarios)

Specific records inserted by seed_data.py that scenarios reference:
- Account #4521 (alice@example.com) with orders, payments, subscription
- Account #1200 with 3 months invoice history
- Subscription id=42 (Premium Annual) for account #4521
- Promo code SPRING25 (25% off, min $50, active)
- Gift card GC-4455 ($100 original, multiple transactions)
- Order #34567 with shipment tracking
- Order #45678 with returnable item
- Ticket #8901 (customer with 3 tickets in 2 weeks)
- Product ASIN B00X4WHP5E across multiple warehouses
- Cart #9876 with 3 items
- Product #234 with co-occurrence purchase data

## Key Design Decisions

1. **File-based, no MCP** — agents read dbook markdown directly (matches dbook's design philosophy)
2. **Port 5433** — avoids conflicts with local Postgres
3. **Mock by default** — no API keys needed; measures table discovery + token efficiency
4. **Real LLM optional** — set BENCHMARK_LLM_KEY for actual SQL generation evaluation
5. **Idempotent** — `make benchmark` can re-run without manual cleanup (DROP SCHEMA CASCADE + recreate)
6. **dbook installed from parent** — `pip install -e ../[postgres]` ensures latest source is tested

## Files to Reuse from Existing Codebase
- `tests/amazon_fixture.py` — schema patterns to expand (40 → 70 tables)
- `tests/benchmark_helpers.py` — AgentSimulator, BenchmarkReport, count_tokens
- `tests/test_benchmark_realistic.py` — agent strategy patterns (keyword, business-term, smart)
- `src/dbook/validator.py` — SQL validation against schema
- `src/dbook/llm/provider.py` — LLMProvider protocol + MockProvider pattern

## Visual Report (HTML)

After `make run` completes, `benchmarks/report.py` generates `results/benchmark_report.html` and auto-opens it in the default browser (`webbrowser.open()`). The report is a self-contained HTML file using Chart.js (loaded from CDN), requiring no build step.

### Visualizations

1. **Summary Scorecard** (top of page) — Large-format numbers showing:
   - Overall dbook score (average across all 4 dimensions, all 15 scenarios)
   - Overall baseline score (same calculation)
   - Improvement delta (dbook - baseline, with +/- sign and color)
   - Token savings percentage ((baseline_tokens - dbook_tokens) / baseline_tokens * 100)

2. **Radar/Spider Chart** — Single radar chart with two overlaid polygons (dbook in teal, baseline in gray) comparing average scores across the 4 dimensions: Table Discovery, SQL Correctness, Result Accuracy, Response Quality. Scale 0-5, gridlines at each integer.

3. **Token Efficiency Bar Chart** — Grouped bar chart with 3 groups (Billing, Care, Sales), each group showing two bars: avg tokens/scenario for dbook vs baseline. Y-axis = token count. Highlights the progressive-disclosure savings.

4. **Per-Scenario Heatmap** — Table with 15 rows (B1-B5, C1-C5, S1-S5) and 8 value columns (4 dimensions x 2 modes). Each cell color-coded: green (#22c55e) for scores 4-5, yellow (#eab308) for score 3, red (#ef4444) for scores 1-2. Scenario name and difficulty shown in label columns.

5. **Agent-Type Breakdown** — 3 mini cards (Billing, Care, Sales) each showing:
   - Aggregate dbook score (avg of 4 dimensions across 5 scenarios)
   - Aggregate baseline score
   - Delta
   - Avg token usage for each mode

### Implementation Details

- **Generator**: `benchmarks/report.py` reads `results/benchmark_results.json`, builds the HTML string with embedded JSON data in a `<script>` tag, writes to `results/benchmark_report.html`
- **Theme**: Dark background (#1a1a2e primary, #16213e cards), light text (#e0e0e0), teal accents (#0d7377) matching dbook branding
- **Layout**: CSS Grid, responsive (2-column on desktop, single column on mobile)
- **Charts**: Chart.js 4.x from `https://cdn.jsdelivr.net/npm/chart.js`, configured with `Chart.register(...)` on page load
- **Heatmap**: Pure HTML table with inline background-color styles (no canvas needed)
- **Print-friendly**: `@media print` rules — white background, black text, charts sized to fit pages
- **No dependencies**: Single HTML file, works offline once loaded (Chart.js cached by browser)

### Template Structure

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>dbook Benchmark Report</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>/* dark theme, grid layout, print styles */</style>
</head>
<body>
  <header><!-- Summary Scorecard: 4 big-number cards --></header>
  <main>
    <section id="radar"><!-- Radar chart canvas --></section>
    <section id="tokens"><!-- Token bar chart canvas --></section>
    <section id="heatmap"><!-- Per-scenario heatmap table --></section>
    <section id="agents"><!-- 3 agent-type mini cards --></section>
  </main>
  <script>
    const DATA = /* embedded JSON from benchmark_results.json */;
    // Render all charts on DOMContentLoaded
  </script>
</body>
</html>
```

## Verification
1. `make benchmark` completes in <5 minutes on laptop
2. All 15 scenarios execute without SQL errors in dbook mode
3. dbook mode scores higher than baseline on all 4 dimensions (aggregate)
4. dbook mode uses >80% fewer tokens than baseline (aggregate)
5. Report JSON is well-formed and contains all 30 results (15 x 2 modes)

## Implementation Order
1. Docker + Makefile + schema SQL files
2. seed_data.py with golden records + Faker bulk generation
3. ScenarioSpec + all 15 scenarios with golden SQL
4. BaseAgent + 3 specialized agents
5. MockJudge
6. Runner + Report
7. End-to-end test: `make benchmark`
8. (Optional) LLM judge + real LLM agent mode
