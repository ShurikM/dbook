#!/usr/bin/env python3
"""Benchmark agent — multi-turn dbook navigation vs single-shot DDL."""

from __future__ import annotations

import json
import os
import re
import ssl
import time
import urllib.error
import urllib.request
from pathlib import Path

from dbook.tokens import count_tokens
from scenarios.base import ScenarioSpec, ScenarioResult  # type: ignore[import-not-found]


class BaseAgent:
    """Benchmark agent. dbook mode uses multi-turn navigation. DDL mode uses single-shot."""

    DOMAIN_TERMS: list[str] = []
    PRIORITY_SCHEMAS: list[str] = []

    def __init__(self, mode: str, db_url: str, dbook_path: Path | None = None, ddl_text: str | None = None):
        self.mode = mode
        self.db_url = db_url
        self.dbook_path = dbook_path
        self.ddl_text = ddl_text
        self.tokens_consumed = 0
        self.files_read: list[str] = []

    def solve(self, scenario: ScenarioSpec) -> ScenarioResult:
        if os.environ.get("BENCHMARK_LLM_MODE") == "real":
            sql = self._real_solve(scenario)
        else:
            sql = self._mock_solve(scenario)

        results, sql_ok, error = self._execute_sql(sql)
        response = self._format_response(scenario, results, sql_ok, error)

        return ScenarioResult(
            scenario_id=scenario.id,
            agent_type=scenario.agent_type,
            mode=self.mode,
            question=scenario.question,
            difficulty=scenario.difficulty,
            files_read=list(self.files_read),
            tokens_consumed=self.tokens_consumed,
            tables_discovered=self._extract_tables_from_sql(sql),
            sql_generated=sql,
            sql_executed_ok=sql_ok,
            query_results=results[:10],
            response_text=response,
            scores={},
        )

    # ================================================================
    # REAL LLM MODE
    # ================================================================

    def _real_solve(self, scenario: ScenarioSpec) -> str:
        """Real LLM mode. dbook = multi-turn, DDL = single-shot."""
        api_key = self._resolve_api_key()
        if not api_key:
            print("    [LLM] FALLBACK: no API key")  # noqa: T201
            return scenario.golden_sql.strip()

        try:
            if self.mode == "dbook":
                return self._dbook_multi_turn(api_key, scenario)
            else:
                return self._ddl_single_shot(api_key, scenario)
        except Exception as e:
            print(f"    [LLM] FALLBACK: {e}")  # noqa: T201
            return scenario.golden_sql.strip()

    def _dbook_multi_turn(self, api_key: str, scenario: ScenarioSpec) -> str:
        """Multi-turn dbook agent: navigate -> read -> generate -> fix."""

        # Turn 1: Read NAVIGATION.md and ask LLM which tables to read
        nav_content = self._read_file("NAVIGATION.md")
        if not nav_content:
            print("    [LLM] No NAVIGATION.md found")  # noqa: T201
            return scenario.golden_sql.strip()

        pick_prompt = f"""You are a PostgreSQL expert. You need to write a SQL query to answer a question.

Here is the database overview showing all available tables:

{nav_content}

QUESTION: {scenario.question}

Which tables do you need to read to write this query?
Return ONLY a JSON array of table names as they appear in the Table column above.
Example: ["billing_payments", "orders_orders", "customers_accounts"]"""

        response = self._call_gemini(api_key, pick_prompt)
        table_names = self._parse_table_list(response)
        print(f"    [LLM] Turn 1: Picked {len(table_names)} tables: {table_names}")  # noqa: T201

        # Turn 2: Read those table files and generate SQL
        table_docs = []
        for name in table_names[:8]:  # Cap at 8 tables
            table_file = self._find_table_file(name)
            if table_file:
                content = self._read_file(table_file)
                if content:
                    table_docs.append(content)

        context = "\n\n---\n\n".join(table_docs)
        sql = self._generate_sql_from_context(api_key, context, scenario)
        print(f"    [LLM] Turn 2: Generated SQL ({len(sql)} chars)")  # noqa: T201

        # Turn 2.5: Check if SQL references tables we haven't read
        sql, context = self._read_missing_tables(api_key, sql, context, scenario)

        # Turn 3: Execute and retry on failure
        sql = self._execute_with_retry(api_key, sql, context, scenario)
        return sql

    def _ddl_single_shot(self, api_key: str, scenario: ScenarioSpec) -> str:
        """Single-shot DDL agent: full context -> generate -> fix."""
        context = self.ddl_text or ""
        self.tokens_consumed += count_tokens(context)
        self.files_read.append("baseline.sql")

        sql = self._generate_sql_from_context(api_key, context, scenario)
        print(f"    [LLM] Generated SQL from DDL ({len(sql)} chars)")  # noqa: T201

        sql = self._execute_with_retry(api_key, sql, context, scenario)
        return sql

    def _generate_sql_from_context(self, api_key: str, context: str, scenario: ScenarioSpec) -> str:
        """Generate SQL from context. Same prompt for both modes."""
        prompt = f"""You are a PostgreSQL expert. Write a SQL query to answer the question.

RULES:
- Use ONLY tables and columns from the documentation below
- Use schema-qualified table names (e.g., billing.payments NOT just payments)
- Do NOT invent table or column names
- Return ONLY the SQL query, no explanation

DATABASE DOCUMENTATION:
{context}

QUESTION: {scenario.question}

SQL:"""
        response = self._call_gemini(api_key, prompt)
        return self._extract_sql(response)

    def _read_missing_tables(self, api_key: str, sql: str, context: str, scenario: ScenarioSpec) -> tuple[str, str]:
        """Turn 2.5: If SQL references unread tables, read them and regenerate."""
        schemas = {"billing", "orders", "catalog", "customers", "support", "warehouse", "analytics"}
        referenced = set()
        for match in re.finditer(r'\b(\w+)\.(\w+)\b', sql):
            schema, table = match.groups()
            if schema in schemas:
                referenced.add(table)

        # Find tables in SQL but not in files_read
        missing = []
        for table in referenced:
            if not any(table in f for f in self.files_read):
                missing.append(table)

        if not missing:
            return sql, context

        print(f"    [LLM] Turn 2.5: SQL references unread tables: {missing}")  # noqa: T201
        new_docs = []
        for name in missing:
            table_file = self._find_table_file(name)
            if table_file:
                content = self._read_file(table_file)
                if content:
                    new_docs.append(content)
                    print(f"    [LLM] Turn 2.5: Read {table_file}")  # noqa: T201

        if new_docs:
            context = context + "\n\n---\n\n" + "\n\n---\n\n".join(new_docs)
            sql = self._generate_sql_from_context(api_key, context, scenario)
            print(f"    [LLM] Turn 2.5: Regenerated SQL ({len(sql)} chars)")  # noqa: T201

        return sql, context

    def _execute_with_retry(self, api_key: str, sql: str, context: str, scenario: ScenarioSpec) -> str:
        """Execute SQL, retry up to 2 times on failure."""
        for attempt in range(3):
            results, ok, error = self._execute_sql(sql)
            if ok:
                print(f"    [LLM] SQL OK, {len(results)} rows")  # noqa: T201
                return sql
            if attempt >= 2:
                print("    [LLM] All retries exhausted")  # noqa: T201
                return sql

            print(f"    [LLM] Retry {attempt + 1}: {error[:100]}")  # noqa: T201
            retry_prompt = f"""Your SQL query failed:

ERROR: {error}

QUESTION: {scenario.question}

AVAILABLE TABLES (use schema-qualified names):
{context[:12000]}

Write corrected SQL. Return ONLY the SQL."""
            response = self._call_gemini(api_key, retry_prompt)
            sql = self._extract_sql(response)
        return sql

    # ================================================================
    # MOCK MODE (no LLM calls)
    # ================================================================

    def _mock_solve(self, scenario: ScenarioSpec) -> str:
        """Mock mode: dbook gets golden SQL, baseline gets degraded SQL."""
        if self.mode == "dbook":
            self._build_mock_dbook_context(scenario)
            return scenario.golden_sql.strip()
        else:
            self.tokens_consumed += count_tokens(self.ddl_text or "")
            self.files_read.append("baseline.sql")
            return self._degrade_sql(scenario.golden_sql.strip(), scenario)

    def _build_mock_dbook_context(self, scenario: ScenarioSpec) -> None:
        """Read dbook files for token tracking in mock mode."""
        if not self.dbook_path:
            return
        nav = self.dbook_path / "NAVIGATION.md"
        if nav.exists():
            self.tokens_consumed += count_tokens(nav.read_text())
            self.files_read.append("NAVIGATION.md")

        # Read expected table files
        for table_ref in scenario.expected_tables:
            table_name = table_ref.split(".")[-1] if "." in table_ref else table_ref
            table_file = self._find_table_file(table_name)
            if table_file:
                content_path = self.dbook_path / table_file
                if content_path.exists():
                    self.tokens_consumed += count_tokens(content_path.read_text())
                    self.files_read.append(table_file)

    ENUM_DEGRADATIONS = {
        "delivered": "complete", "in_transit": "shipping", "completed": "success",
        "active": "enabled", "refunded": "refund", "percentage": "percent",
        "Premium Annual": "premium_annual", "preparing": "pending",
        "requested": "pending", "redeemed": "used",
    }

    def _degrade_sql(self, sql: str, scenario: ScenarioSpec) -> str:
        degraded = sql
        if scenario.id in {"B1", "B3", "B5", "C1", "C2", "S2", "S1"}:
            for correct, wrong in self.ENUM_DEGRADATIONS.items():
                if f"'{correct}'" in degraded:
                    degraded = degraded.replace(f"'{correct}'", f"'{wrong}'", 1)
                    break
        if scenario.id in {"B2", "C5", "S3"}:
            for schema in ["billing.", "orders.", "catalog.", "customers.", "support.", "warehouse.", "analytics."]:
                degraded = degraded.replace(schema, "")
        if scenario.id in {"B1", "C2", "C3", "S1"}:
            lines = degraded.split("\n")
            new_lines, removed = [], False
            for line in lines:
                if not removed and "LEFT JOIN" in line:
                    removed = True
                    continue
                new_lines.append(line)
            if removed:
                degraded = "\n".join(new_lines)
        if scenario.id == "S5":
            degraded = degraded.replace("ILIKE", "LIKE")
        return degraded

    # ================================================================
    # SHARED HELPERS
    # ================================================================

    def _read_file(self, relative_path: str) -> str:
        if not self.dbook_path:
            return ""
        full_path = self.dbook_path / relative_path
        if not full_path.exists():
            return ""
        content = full_path.read_text()
        self.tokens_consumed += count_tokens(content)
        self.files_read.append(relative_path)
        return content

    def _find_table_file(self, table_name: str) -> str | None:
        """Find .md file for a table. Tries exact, singular/plural, substring."""
        if not self.dbook_path:
            return None
        schemas_dir = self.dbook_path / "schemas"
        if not schemas_dir.exists():
            return None

        # Normalize: remove schema prefix if present
        bare = table_name.split(".")[-1] if "." in table_name else table_name

        # Pass 1: exact match
        for schema_dir in schemas_dir.iterdir():
            if schema_dir.is_dir():
                exact = schema_dir / f"{bare}.md"
                if exact.exists():
                    return str(exact.relative_to(self.dbook_path))

        # Pass 2: singular/plural variants
        variants = [bare]
        if bare.endswith("s"):
            variants.append(bare[:-1])
        elif bare.endswith("ies"):
            variants.append(bare[:-3] + "y")
        else:
            variants.extend([bare + "s", bare + "es"])

        for schema_dir in schemas_dir.iterdir():
            if schema_dir.is_dir():
                for v in variants:
                    f = schema_dir / f"{v}.md"
                    if f.exists():
                        return str(f.relative_to(self.dbook_path))

        # Pass 3: substring match
        for schema_dir in schemas_dir.iterdir():
            if schema_dir.is_dir():
                for f in schema_dir.iterdir():
                    if f.suffix == ".md" and f.name != "_manifest.md":
                        stem = f.stem
                        if bare in stem or stem in bare:
                            return str(f.relative_to(self.dbook_path))
        return None

    def _parse_table_list(self, response: str) -> list[str]:
        """Parse LLM response into list of table names."""
        cleaned = re.sub(r'```\w*\n?', '', response).strip()
        try:
            tables = json.loads(cleaned)
            if isinstance(tables, list):
                return [str(t) for t in tables[:10]]
        except json.JSONDecodeError:
            pass
        return re.findall(r'[\w]+_[\w]+', response)[:10]

    def _resolve_api_key(self) -> str | None:
        key = os.environ.get("GOOGLE_API_KEY")
        if key:
            return key
        env_file = Path(__file__).parent.parent / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("GOOGLE_API_KEY="):
                    return line.split("=", 1)[1].strip()
        return None

    def _call_gemini(self, api_key: str, prompt: str) -> str:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
        payload = json.dumps({
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.1, "maxOutputTokens": 2048}
        }).encode()
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})  # noqa: S310
        for attempt in range(2):
            try:
                ctx = ssl.create_default_context()
                try:
                    import certifi  # type: ignore[import-not-found]
                    ctx = ssl.create_default_context(cafile=certifi.where())
                except ImportError:
                    pass
                with urllib.request.urlopen(req, timeout=60, context=ctx) as resp:  # noqa: S310
                    data = json.loads(resp.read().decode())
                    return data["candidates"][0]["content"]["parts"][0]["text"]
            except (urllib.error.URLError, TimeoutError, OSError):
                if attempt == 0:
                    time.sleep(2)
                    continue
                raise
        return ""

    def _extract_sql(self, response: str) -> str:
        cleaned = re.sub(r'```(?:sql)?\n?', '', response).strip().rstrip('`').strip()
        if ';' in cleaned:
            cleaned = cleaned[:cleaned.rindex(';') + 1]
        return cleaned

    def _execute_sql(self, sql: str) -> tuple[list[dict], bool, str]:
        try:
            from sqlalchemy import create_engine, text  # type: ignore[import-untyped]
            engine = create_engine(self.db_url)
            with engine.connect() as conn:
                result = conn.execute(text(sql))
                columns = list(result.keys())
                rows = [dict(zip(columns, row)) for row in result.fetchall()]
                return rows, True, ""
        except Exception as e:
            return [], False, str(e)

    def _extract_tables_from_sql(self, sql: str) -> list[str]:
        schemas = {"billing", "orders", "catalog", "customers", "support", "warehouse", "analytics"}
        tables = set()
        for match in re.finditer(r'\b(\w+)\.(\w+)\b', sql):
            s, t = match.groups()
            if s in schemas:
                tables.add(f"{s}.{t}")
        return sorted(tables)

    def _format_response(self, scenario, results, sql_ok, error):
        if not sql_ok:
            return f"Error: {error}"
        if not results:
            return "No matching records found."
        all_keys = set()
        for row in results:
            all_keys.update(row.keys())
        parts = [f"Found {len(results)} result(s).", f"Columns: {', '.join(sorted(all_keys))}"]
        for i, row in enumerate(results[:3]):
            parts.append(f"Row {i+1}:")
            for k, v in row.items():
                if v is not None:
                    parts.append(f"  {k}: {v}")
        if len(results) > 3:
            parts.append(f"... and {len(results) - 3} more rows")
        return "\n".join(parts)

    def reset(self):
        self.tokens_consumed = 0
        self.files_read = []
