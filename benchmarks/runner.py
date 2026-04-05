#!/usr/bin/env python3
"""Benchmark runner — executes all scenarios in dbook and no-dbook modes."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add benchmarks dir to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from scenarios import ALL_SCENARIOS
from agents import AGENT_MAP
from judge import MockJudge


def run_benchmark(args: argparse.Namespace) -> list[dict]:
    """Run all scenarios and return results."""
    # Load baseline DDL
    ddl_text = Path(args.baseline_ddl).read_text() if Path(args.baseline_ddl).exists() else ""
    dbook_path = Path(args.dbook_path) if args.dbook_path else None

    # Filter scenarios if specified
    if args.scenarios == "all":
        scenarios = ALL_SCENARIOS
    else:
        ids = {s.strip() for s in args.scenarios.split(",")}
        scenarios = [s for s in ALL_SCENARIOS if s.id in ids]

    # Initialize judge
    if args.mode == "llm":
        from judge.llm_judge import LLMJudge
        judge = LLMJudge()
    else:
        judge = MockJudge()

    results = []
    total = len(scenarios) * 2  # 2 modes per scenario
    current = 0

    print(f"\n{'=' * 80}")
    print(f"  dbook Benchmark — {len(scenarios)} scenarios x 2 modes")
    print(f"  Database: {args.db_url}")
    print(f"  dbook output: {args.dbook_path}")
    print(f"  Baseline DDL: {args.baseline_ddl}")
    print(f"{'=' * 80}\n")

    for scenario in scenarios:
        agent_cls = AGENT_MAP[scenario.agent_type]

        for mode in ["dbook", "no_dbook"]:
            current += 1
            start = time.time()

            agent = agent_cls(
                mode=mode,
                db_url=args.db_url,
                dbook_path=dbook_path if mode == "dbook" else None,
                ddl_text=ddl_text if mode == "no_dbook" else None,
            )

            result = agent.solve(scenario)
            result.scores = judge.score(scenario, result)

            elapsed = time.time() - start
            status = "OK" if result.sql_executed_ok else "FAIL"
            avg_score = sum(result.scores.values()) / len(result.scores) if result.scores else 0

            print(
                f"  [{current:2d}/{total}] {scenario.id} {mode:<10} "
                f"{status:<4} {result.tokens_consumed:>6} tok  "
                f"score={avg_score:.1f}  ({elapsed:.1f}s)"
            )

            results.append(_result_to_dict(result))
            agent.reset()

    return results


def _result_to_dict(result) -> dict:
    """Convert ScenarioResult to JSON-serializable dict."""
    return {
        "scenario_id": result.scenario_id,
        "agent_type": result.agent_type,
        "mode": result.mode,
        "question": result.question,
        "difficulty": result.difficulty,
        "files_read": result.files_read,
        "tokens_consumed": result.tokens_consumed,
        "tables_discovered": result.tables_discovered,
        "sql_generated": result.sql_generated,
        "sql_executed_ok": result.sql_executed_ok,
        "query_results_count": len(result.query_results),
        "response_text": result.response_text[:500],  # Truncate for JSON
        "scores": result.scores,
    }


def print_summary(results: list[dict]):
    """Print console summary table."""
    sep_wide = "=" * 100
    sep_narrow = "-" * 95
    print(f"\n{sep_wide}")
    print("  RESULTS SUMMARY")
    print(sep_wide)

    # Header
    print(f"\n  {'Scenario':<10} {'Agent':<10} {'Diff':<8} "
          f"{'--- dbook ---':^30} {'--- baseline ---':^30}")
    print(f"  {'':10} {'':10} {'':8} "
          f"{'Tok':>7} {'TableDisc':>9} {'SQLCorr':>7} {'ResAcc':>6} {'RespQ':>5} "
          f"{'Tok':>7} {'TableDisc':>9} {'SQLCorr':>7} {'ResAcc':>6} {'RespQ':>5}")
    print(f"  {sep_narrow}")

    # Group by scenario
    by_scenario: dict[str, dict] = {}
    for r in results:
        sid = r["scenario_id"]
        if sid not in by_scenario:
            by_scenario[sid] = {}
        by_scenario[sid][r["mode"]] = r

    score_keys = [
        "table_discovery",
        "sql_correctness",
        "result_accuracy",
        "response_quality",
    ]
    dbook_totals = {"tokens": 0, "table_discovery": 0, "sql_correctness": 0,
                    "result_accuracy": 0, "response_quality": 0, "count": 0}
    baseline_totals = {"tokens": 0, "table_discovery": 0, "sql_correctness": 0,
                       "result_accuracy": 0, "response_quality": 0, "count": 0}

    for sid, modes in sorted(by_scenario.items()):
        db = modes.get("dbook", {})
        nd = modes.get("no_dbook", {})

        db_scores = db.get("scores", {})
        nd_scores = nd.get("scores", {})

        print(
            f"  {sid:<10} {db.get('agent_type', ''):10} {db.get('difficulty', ''):8} "
            f"{db.get('tokens_consumed', 0):>7} "
            f"{db_scores.get('table_discovery', 0):>5.1f} "
            f"{db_scores.get('sql_correctness', 0):>5.1f} "
            f"{db_scores.get('result_accuracy', 0):>5.1f} "
            f"{db_scores.get('response_quality', 0):>5.1f} "
            f"{nd.get('tokens_consumed', 0):>7} "
            f"{nd_scores.get('table_discovery', 0):>5.1f} "
            f"{nd_scores.get('sql_correctness', 0):>5.1f} "
            f"{nd_scores.get('result_accuracy', 0):>5.1f} "
            f"{nd_scores.get('response_quality', 0):>5.1f}"
        )

        for key in score_keys:
            dbook_totals[key] += db_scores.get(key, 0)
            baseline_totals[key] += nd_scores.get(key, 0)
        dbook_totals["tokens"] += db.get("tokens_consumed", 0)
        baseline_totals["tokens"] += nd.get("tokens_consumed", 0)
        dbook_totals["count"] += 1
        baseline_totals["count"] += 1

    # Aggregates
    n = dbook_totals["count"] or 1
    print(f"  {sep_narrow}")
    print(
        f"  {'AVERAGE':<10} {'':10} {'':8} "
        f"{dbook_totals['tokens'] // n:>7} "
        f"{dbook_totals['table_discovery'] / n:>5.1f} "
        f"{dbook_totals['sql_correctness'] / n:>5.1f} "
        f"{dbook_totals['result_accuracy'] / n:>5.1f} "
        f"{dbook_totals['response_quality'] / n:>5.1f} "
        f"{baseline_totals['tokens'] // n:>7} "
        f"{baseline_totals['table_discovery'] / n:>5.1f} "
        f"{baseline_totals['sql_correctness'] / n:>5.1f} "
        f"{baseline_totals['result_accuracy'] / n:>5.1f} "
        f"{baseline_totals['response_quality'] / n:>5.1f}"
    )

    # Token savings
    if baseline_totals["tokens"] > 0:
        savings = (1 - dbook_totals["tokens"] / baseline_totals["tokens"]) * 100
        dbook_avg = dbook_totals["tokens"] // n
        baseline_avg = baseline_totals["tokens"] // n
        print(
            f"\n  Token savings: {savings:.1f}% "
            f"(dbook avg: {dbook_avg} tok vs baseline: {baseline_avg} tok)"
        )

    # Overall scores
    db_avg = sum(dbook_totals[k] for k in score_keys) / (4 * n)
    nd_avg = sum(baseline_totals[k] for k in score_keys) / (4 * n)
    print(
        f"  Overall score: dbook={db_avg:.2f}/5.0  "
        f"baseline={nd_avg:.2f}/5.0  delta=+{db_avg - nd_avg:.2f}"
    )
    print()


def main():
    """Parse CLI args and run the benchmark."""
    parser = argparse.ArgumentParser(description="dbook Benchmark Runner")
    parser.add_argument(
        "--db-url",
        default="postgresql://bench:bench@localhost:5433/benchdb",
    )
    parser.add_argument("--dbook-path", default="output/dbook")
    parser.add_argument("--baseline-ddl", default="output/baseline.sql")
    parser.add_argument("--output", default="results")
    parser.add_argument("--mode", choices=["mock", "llm"], default="mock")
    parser.add_argument(
        "--scenarios",
        default="all",
        help="Scenario IDs (comma-separated) or 'all'",
    )
    args = parser.parse_args()

    # Ensure output dir exists
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Run benchmark
    results = run_benchmark(args)

    # Save JSON results
    output_file = output_dir / "benchmark_results.json"
    report_data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "config": {
            "db_url": args.db_url,
            "dbook_path": args.dbook_path,
            "mode": args.mode,
            "scenario_count": len(results) // 2,
        },
        "results": results,
    }
    output_file.write_text(json.dumps(report_data, indent=2, default=str))

    # Print console summary
    print_summary(results)
    print(f"  Results saved to: {output_file!s}")


if __name__ == "__main__":
    main()
