"""Benchmark helpers for measuring token efficiency and agent accuracy."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from dbook.tokens import count_tokens


@dataclass
class BenchmarkResult:
    """Result of a single benchmark question."""
    question_id: str
    question: str
    expected_answer: list[str]  # table/column names that should be found
    files_read: list[str] = field(default_factory=list)
    tokens_consumed: int = 0
    answer_found: bool = False
    notes: str = ""


@dataclass
class BenchmarkReport:
    """Aggregated benchmark results for a phase."""
    phase: str
    mode: str  # "base", "pii", "llm", "full"
    baseline_tokens: int = 0  # tokens for raw DDL dump
    results: list[BenchmarkResult] = field(default_factory=list)

    @property
    def accuracy(self) -> float:
        if not self.results:
            return 0.0
        return sum(1 for r in self.results if r.answer_found) / len(self.results)

    @property
    def avg_tokens(self) -> float:
        if not self.results:
            return 0.0
        return sum(r.tokens_consumed for r in self.results) / len(self.results)

    @property
    def avg_files_read(self) -> float:
        if not self.results:
            return 0.0
        return sum(len(r.files_read) for r in self.results) / len(self.results)

    @property
    def token_savings_pct(self) -> float:
        if self.baseline_tokens == 0:
            return 0.0
        return (1 - self.avg_tokens / self.baseline_tokens) * 100

    def summary(self) -> str:
        lines = [
            f"BENCHMARK: {self.phase}",
            f"{'=' * 40}",
            f"Mode: {self.mode}",
            f"Baseline: {self.baseline_tokens} tokens (raw DDL)",
            "",
            "RESULTS:",
        ]
        for r in self.results:
            status = "✓" if r.answer_found else "✗"
            pct = (r.tokens_consumed / self.baseline_tokens * 100) if self.baseline_tokens else 0
            lines.append(
                f"  {r.question_id}: {status} {r.tokens_consumed} tok "
                f"({pct:.1f}% baseline) — {len(r.files_read)} files read"
            )
        lines.extend([
            "",
            "SUMMARY:",
            f"  Accuracy: {sum(1 for r in self.results if r.answer_found)}/{len(self.results)} ({self.accuracy:.0%})",
            f"  Avg tokens/question: {self.avg_tokens:.0f} tok ({100 - self.token_savings_pct:.1f}% baseline)",
            f"  Avg files read: {self.avg_files_read:.1f}",
            f"  Token savings: {self.token_savings_pct:.1f}%",
        ])
        gate = "PASS" if self.accuracy >= 0.85 and self.token_savings_pct >= 90 else "FAIL"
        lines.append(f"\n  PHASE GATE: {gate}")
        return "\n".join(lines)


class AgentSimulator:
    """Simulates an agent navigating dbook output, tracking token usage."""

    def __init__(self, dbook_path: str | Path):
        self.dbook_path = Path(dbook_path)
        self.tokens_consumed = 0
        self.files_read: list[str] = []

    def read_file(self, relative_path: str) -> str:
        """Read a file from the dbook output, tracking tokens."""
        full_path = self.dbook_path / relative_path
        if not full_path.exists():
            return ""
        content = full_path.read_text()
        self.tokens_consumed += count_tokens(content)
        self.files_read.append(relative_path)
        return content

    def reset(self):
        """Reset counters for a new question."""
        self.tokens_consumed = 0
        self.files_read = []
