"""Mock judge — deterministic scoring based on fact matching."""

from __future__ import annotations

import math
from scenarios.base import ScenarioSpec, ScenarioResult  # type: ignore[import-not-found]


class MockJudge:
    """Scores scenario results using deterministic keyword/fact matching."""

    def score(self, spec: ScenarioSpec, result: ScenarioResult) -> dict[str, float]:
        """Score a result on 4 dimensions (1-5 each)."""
        return {
            "table_discovery": self._score_table_discovery(spec, result),
            "sql_correctness": self._score_sql_correctness(spec, result),
            "result_accuracy": self._score_result_accuracy(spec, result),
            "response_quality": self._score_response_quality(spec, result),
        }

    def _score_table_discovery(self, spec: ScenarioSpec, result: ScenarioResult) -> float:
        """How many expected tables did the agent find?"""
        if not spec.expected_tables:
            return 5.0
        sql_lower = result.sql_generated.lower() if result.sql_generated else ""
        found = sum(
            1 for t in spec.expected_tables
            if any(t.split(".")[-1] in f for f in result.files_read)
            or any(t.split(".")[-1] in td for td in result.tables_discovered)
            or t.split(".")[-1] in sql_lower
        )
        recall = found / len(spec.expected_tables)
        return max(1.0, math.ceil(recall * 5))

    def _score_sql_correctness(self, spec: ScenarioSpec, result: ScenarioResult) -> float:
        """Did the SQL execute and return results?"""
        if not result.sql_executed_ok:
            return 1.0
        # Check if expected tables are referenced in the SQL
        sql_lower = result.sql_generated.lower()
        tables_in_sql = sum(
            1 for t in spec.expected_tables
            if t.split(".")[-1] in sql_lower
        )
        has_results = len(result.query_results) > 0
        if has_results:
            # Executed with results
            if tables_in_sql == len(spec.expected_tables):
                return 5.0
            elif tables_in_sql >= len(spec.expected_tables) * 0.5:
                return 4.0
            return 3.0
        else:
            # Executed but 0 rows — still check table coverage
            if tables_in_sql >= len(spec.expected_tables) * 0.5:
                return 4.0
            return 3.0

    def _score_result_accuracy(self, spec: ScenarioSpec, result: ScenarioResult) -> float:
        """Do results contain expected facts?"""
        if not result.sql_executed_ok:
            return 1.0
        # Build searchable text from query_results AND response_text
        searchable_parts = []
        for row in result.query_results:
            for k, v in row.items():
                searchable_parts.append(str(k).lower())
                if v is not None:
                    searchable_parts.append(str(v).lower())
        if result.response_text:
            searchable_parts.append(result.response_text.lower())
        results_text = " ".join(searchable_parts)
        if not results_text.strip():
            return 1.0

        found = 0
        for fact in spec.expected_facts:
            # A fact matches if at least half its significant words (>3 chars) appear
            fact_words = [w for w in fact.lower().split() if len(w) > 3]
            if not fact_words:
                found += 1
                continue
            matching = sum(1 for word in fact_words if word in results_text)
            if matching >= max(1, len(fact_words) / 2):
                found += 1

        if not spec.expected_facts:
            return 5.0
        ratio = found / len(spec.expected_facts)
        return max(1.0, math.ceil(ratio * 5))

    def _score_response_quality(self, spec: ScenarioSpec, result: ScenarioResult) -> float:
        """Does the response mention key entities?"""
        if not result.sql_executed_ok:
            return 1.0
        if not result.response_text:
            return 1.0

        response_lower = result.response_text.lower()

        # Check expected_columns against query_results column KEYS (if available) and response_text
        column_keys = set()
        for row in result.query_results:
            column_keys.update(k.lower() for k in row.keys())

        entities_found = 0
        total_entities = 0
        for col in spec.expected_columns:
            total_entities += 1
            if col.lower() in column_keys or col.lower() in response_lower:
                entities_found += 1

        if total_entities == 0:
            return 5.0
        ratio = entities_found / total_entities
        return max(1.0, math.ceil(ratio * 5))
