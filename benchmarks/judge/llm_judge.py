"""LLM judge — real LLM evaluation (optional, requires BENCHMARK_LLM_KEY)."""

from __future__ import annotations

import os

from scenarios.base import ScenarioSpec, ScenarioResult


class LLMJudge:
    """Scores scenario results using a real LLM (Claude/GPT)."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.environ.get("BENCHMARK_LLM_KEY")
        if not self.api_key:
            raise ValueError("BENCHMARK_LLM_KEY environment variable required for LLM judge")

    def score(self, spec: ScenarioSpec, result: ScenarioResult) -> dict[str, float]:
        """Score using LLM. Falls back to mock scoring if LLM unavailable."""
        # TODO: Implement real LLM judging
        # For now, delegate to mock judge
        from judge.mock_judge import MockJudge
        return MockJudge().score(spec, result)
