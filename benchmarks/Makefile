.PHONY: benchmark setup seed dump reseed compile run run-llm clean cleanall help

DATABASE_URL ?= postgresql://bench:bench@localhost:5433/benchdb
DBOOK_OUTPUT ?= output/dbook
BASELINE_DDL ?= output/baseline.sql
RESULTS_DIR  ?= results
VENV         ?= .venv
PYTHON       ?= $(VENV)/bin/python
PIP          ?= $(VENV)/bin/pip
DBOOK        ?= $(VENV)/bin/dbook
DATA_DUMP    ?= data/benchdb.sql.gz

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

benchmark: setup seed compile run ## Full benchmark pipeline

setup: ## Start Postgres, create venv, install dbook
	docker compose up -d --wait
	@echo "Waiting for Postgres..."
	@until docker compose exec -T postgres pg_isready -U bench -d benchdb > /dev/null 2>&1; do sleep 1; done
	@echo "Postgres ready."
	@test -d $(VENV) || python3 -m venv $(VENV)
	$(PIP) install -q -e "../[postgres]"
	$(PIP) install -q -r requirements.txt

seed: ## Create schemas and seed data (uses cached dump if available)
	@if [ -f $(DATA_DUMP) ]; then \
		echo "Restoring from cached dump..."; \
		gunzip -c $(DATA_DUMP) | docker compose exec -T postgres psql -U bench -d benchdb -q; \
		echo "Restored."; \
	else \
		echo "Creating schemas..."; \
		for f in schema/0*.sql; do \
			docker compose exec -T postgres psql -U bench -d benchdb -f /dev/stdin < "$$f"; \
		done; \
		echo "Seeding data..."; \
		$(PYTHON) seed/seed_data.py --db-url "$(DATABASE_URL)"; \
		echo "Caching dump..."; \
		mkdir -p data; \
		docker compose exec -T postgres pg_dump -U bench benchdb | gzip > $(DATA_DUMP); \
		echo "Cached to $(DATA_DUMP)"; \
	fi

dump: ## Create/update cached data dump
	@mkdir -p data
	docker compose exec -T postgres pg_dump -U bench benchdb | gzip > $(DATA_DUMP)
	@echo "Dump saved to $(DATA_DUMP)"

reseed: ## Force re-seed (delete cache and regenerate)
	@rm -f $(DATA_DUMP)
	@$(MAKE) seed

compile: ## Run dbook compile + generate baseline DDL
	@mkdir -p output results
	@echo "Compiling dbook..."
	$(DBOOK) compile "$(DATABASE_URL)" -o $(DBOOK_OUTPUT) \
		--schemas customers,catalog,orders,billing,analytics,warehouse,support \
		--sample-rows 5
	@echo "Generating baseline DDL..."
	docker compose exec -T postgres pg_dump -U bench --schema-only benchdb > $(BASELINE_DDL)

run: ## Execute benchmark scenarios and generate report
	@set -a && [ -f .env ] && . .env; set +a; \
	$(PYTHON) runner.py \
		--db-url "$(DATABASE_URL)" \
		--dbook-path $(DBOOK_OUTPUT) \
		--baseline-ddl $(BASELINE_DDL) \
		--output $(RESULTS_DIR)
	$(PYTHON) report.py \
		--input $(RESULTS_DIR)/benchmark_results.json \
		--output $(RESULTS_DIR)/benchmark_report.html

run-llm: ## Execute benchmark with real LLM (requires GOOGLE_API_KEY in .env)
	BENCHMARK_LLM_MODE=real GOOGLE_API_KEY=$$(grep GOOGLE_API_KEY .env | cut -d= -f2) \
	$(PYTHON) runner.py \
		--db-url "$(DATABASE_URL)" \
		--dbook-path $(DBOOK_OUTPUT) \
		--baseline-ddl $(BASELINE_DDL) \
		--output $(RESULTS_DIR)
	$(PYTHON) report.py \
		--input $(RESULTS_DIR)/benchmark_results.json \
		--output $(RESULTS_DIR)/benchmark_report.html

clean: ## Stop Postgres, remove output (keeps data cache)
	docker compose down -v
	rm -rf output/ results/ $(VENV)/

cleanall: clean ## Clean everything including data cache
	rm -rf data/
