"""Base classes for benchmark scenarios."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ScenarioSpec:
    """Defines a single benchmark scenario."""
    id: str                          # "B1", "C3", "S2"
    agent_type: str                  # "billing", "care", "sales"
    question: str                    # Natural language question
    expected_tables: list[str]       # e.g. ["billing.payments", "orders.orders"]
    expected_columns: list[str]      # e.g. ["amount", "status", "processed_at"]
    expected_facts: list[str]        # Key facts that should appear in results
    golden_sql: str                  # Reference SQL that answers the question
    difficulty: str                  # "easy", "medium", "hard"
    setup_sql: str | None = None     # Optional scenario-specific setup


@dataclass
class ScenarioResult:
    """Result of running a single scenario."""
    scenario_id: str
    agent_type: str
    mode: str                        # "dbook" or "no_dbook"
    question: str
    difficulty: str
    files_read: list[str] = field(default_factory=list)
    tokens_consumed: int = 0
    tables_discovered: list[str] = field(default_factory=list)
    sql_generated: str = ""
    sql_executed_ok: bool = False
    query_results: list[dict] = field(default_factory=list)
    response_text: str = ""
    scores: dict[str, float] = field(default_factory=dict)
    # Scores keys: table_discovery, sql_correctness, result_accuracy, response_quality
