#!/usr/bin/env python3
"""Generate visual HTML benchmark report from JSON results."""

from __future__ import annotations

import argparse
import json
import logging
import webbrowser
from pathlib import Path

logger = logging.getLogger(__name__)


def generate_report(data: dict, output_path: Path):
    """Generate self-contained HTML report with Chart.js visualizations."""
    results = data["results"]
    timestamp = data.get("timestamp", "")

    # Compute aggregates
    dbook_results = [r for r in results if r["mode"] == "dbook"]
    baseline_results = [r for r in results if r["mode"] == "no_dbook"]

    n = len(dbook_results) or 1

    # Overall averages
    dims = ["table_discovery", "sql_correctness", "result_accuracy", "response_quality"]
    dbook_avgs = {d: sum(r["scores"].get(d, 0) for r in dbook_results) / n for d in dims}
    baseline_avgs = {d: sum(r["scores"].get(d, 0) for r in baseline_results) / n for d in dims}

    dbook_overall = sum(dbook_avgs.values()) / 4
    baseline_overall = sum(baseline_avgs.values()) / 4
    delta = dbook_overall - baseline_overall

    dbook_avg_tok = sum(r["tokens_consumed"] for r in dbook_results) / n
    baseline_avg_tok = sum(r["tokens_consumed"] for r in baseline_results) / n
    token_savings = (1 - dbook_avg_tok / baseline_avg_tok) * 100 if baseline_avg_tok > 0 else 0

    # Per agent type
    agent_types = ["billing", "care", "sales"]
    agent_data = {}
    for at in agent_types:
        db = [r for r in dbook_results if r["agent_type"] == at]
        nd = [r for r in baseline_results if r["agent_type"] == at]
        an = len(db) or 1
        agent_data[at] = {
            "dbook_avg": sum(sum(r["scores"].get(d, 0) for d in dims) / 4 for r in db) / an,
            "baseline_avg": sum(sum(r["scores"].get(d, 0) for d in dims) / 4 for r in nd) / an,
            "dbook_tokens": sum(r["tokens_consumed"] for r in db) / an,
            "baseline_tokens": sum(r["tokens_consumed"] for r in nd) / an,
        }

    # Build per-scenario data for heatmap
    scenario_data = []
    by_scenario = {}
    for r in results:
        sid = r["scenario_id"]
        if sid not in by_scenario:
            by_scenario[sid] = {}
        by_scenario[sid][r["mode"]] = r

    for sid in sorted(by_scenario.keys()):
        modes = by_scenario[sid]
        db = modes.get("dbook", {})
        nd = modes.get("no_dbook", {})
        scenario_data.append({
            "id": sid,
            "agent": db.get("agent_type", ""),
            "difficulty": db.get("difficulty", ""),
            "dbook": db.get("scores", {}),
            "baseline": nd.get("scores", {}),
            "dbook_tokens": db.get("tokens_consumed", 0),
            "baseline_tokens": nd.get("tokens_consumed", 0),
        })

    # Embed data as JSON
    chart_data = json.dumps({
        "dbook_avgs": dbook_avgs,
        "baseline_avgs": baseline_avgs,
        "dbook_overall": round(dbook_overall, 2),
        "baseline_overall": round(baseline_overall, 2),
        "delta": round(delta, 2),
        "token_savings": round(token_savings, 1),
        "dbook_avg_tokens": round(dbook_avg_tok),
        "baseline_avg_tokens": round(baseline_avg_tok),
        "agent_data": agent_data,
        "scenario_data": scenario_data,
        "dims": dims,
        "timestamp": timestamp,
    }, indent=2)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>dbook Benchmark Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: 'SF Mono', 'Monaco', 'Cascadia Code', 'Consolas', monospace; background: #1a1a2e; color: #e0e0e0; padding: 20px; }}
.container {{ max-width: 1200px; margin: 0 auto; }}
h1 {{ color: #0d7377; font-size: 1.8rem; margin-bottom: 5px; }}
.subtitle {{ color: #888; font-size: 0.85rem; margin-bottom: 25px; }}

/* Scorecard */
.scorecard {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin-bottom: 30px; }}
.score-card {{ background: #16213e; border-radius: 12px; padding: 20px; text-align: center; }}
.score-card .label {{ color: #888; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }}
.score-card .value {{ font-size: 2.2rem; font-weight: bold; }}
.score-card .value.teal {{ color: #0d7377; }}
.score-card .value.gray {{ color: #888; }}
.score-card .value.green {{ color: #22c55e; }}
.score-card .value.blue {{ color: #3b82f6; }}
.score-card .detail {{ color: #666; font-size: 0.7rem; margin-top: 4px; }}

/* Charts grid */
.charts {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 30px; }}
.chart-card {{ background: #16213e; border-radius: 12px; padding: 20px; }}
.chart-card h2 {{ color: #0d7377; font-size: 1rem; margin-bottom: 15px; }}
.chart-container {{ position: relative; height: 400px; }}

/* Heatmap */
.heatmap-card {{ background: #16213e; border-radius: 12px; padding: 20px; margin-bottom: 30px; overflow-x: auto; }}
.heatmap-card h2 {{ color: #0d7377; font-size: 1rem; margin-bottom: 15px; }}
table {{ width: 100%; border-collapse: collapse; font-size: 0.8rem; }}
th {{ color: #888; text-transform: uppercase; font-size: 0.65rem; letter-spacing: 1px; padding: 8px 6px; text-align: center; border-bottom: 2px solid #2a2a4e; }}
td {{ padding: 8px 6px; text-align: center; border-bottom: 1px solid #2a2a4e; }}
td.label {{ text-align: left; color: #ccc; font-weight: bold; }}
td.diff {{ color: #888; font-size: 0.75rem; }}
.cell {{ border-radius: 4px; padding: 4px 8px; font-weight: bold; }}

/* Agent cards */
.agent-cards {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin-bottom: 30px; }}
.agent-card {{ background: #16213e; border-radius: 12px; padding: 20px; }}
.agent-card h3 {{ color: #0d7377; font-size: 0.9rem; margin-bottom: 12px; text-transform: capitalize; }}
.agent-stat {{ display: flex; justify-content: space-between; margin-bottom: 6px; font-size: 0.8rem; }}
.agent-stat .label {{ color: #888; }}

/* Print */
@media print {{
  body {{ background: white; color: black; }}
  .score-card, .chart-card, .heatmap-card, .agent-card {{ background: #f5f5f5; border: 1px solid #ddd; }}
  h1, h2, h3, .score-card .value.teal {{ color: #0d7377; }}
}}

/* Mobile */
@media (max-width: 768px) {{
  .scorecard {{ grid-template-columns: repeat(2, 1fr); }}
  .charts {{ grid-template-columns: 1fr; }}
  .agent-cards {{ grid-template-columns: 1fr; }}
}}
</style>
</head>
<body>
<div class="container">
  <h1>dbook Benchmark Report</h1>
  <p class="subtitle" id="subtitle"></p>

  <!-- Scorecard -->
  <div class="scorecard">
    <div class="score-card">
      <div class="label">dbook Score</div>
      <div class="value teal" id="dbook-score"></div>
      <div class="detail">avg across 4 dimensions</div>
    </div>
    <div class="score-card">
      <div class="label">Baseline Score</div>
      <div class="value gray" id="baseline-score"></div>
      <div class="detail">raw DDL context</div>
    </div>
    <div class="score-card">
      <div class="label">Improvement</div>
      <div class="value green" id="delta-score"></div>
      <div class="detail">dbook advantage</div>
    </div>
    <div class="score-card">
      <div class="label">Token Savings</div>
      <div class="value blue" id="token-savings"></div>
      <div class="detail" id="token-detail"></div>
    </div>
  </div>

  <!-- Charts -->
  <div class="charts">
    <div class="chart-card">
      <h2>Scoring Dimensions</h2>
      <div class="chart-container">
        <canvas id="radarChart"></canvas>
      </div>
    </div>
    <div class="chart-card">
      <h2>Token Efficiency by Agent</h2>
      <div class="chart-container">
        <canvas id="tokenChart"></canvas>
      </div>
    </div>
  </div>

  <!-- Heatmap -->
  <div class="heatmap-card">
    <h2>Per-Scenario Breakdown</h2>
    <table id="heatmapTable"></table>
  </div>

  <!-- Agent cards -->
  <div class="agent-cards" id="agentCards"></div>
</div>

<script>
const DATA = {chart_data};

// Populate scorecard
document.getElementById('subtitle').textContent =
  DATA.scenario_data.length + ' scenarios | ' + DATA.timestamp.split('T')[0];
document.getElementById('dbook-score').textContent = DATA.dbook_overall.toFixed(1) + '/5';
document.getElementById('baseline-score').textContent = DATA.baseline_overall.toFixed(1) + '/5';
document.getElementById('delta-score').textContent = '+' + DATA.delta.toFixed(1);
document.getElementById('token-savings').textContent = DATA.token_savings.toFixed(0) + '%';
document.getElementById('token-detail').textContent =
  DATA.dbook_avg_tokens + ' vs ' + DATA.baseline_avg_tokens + ' tok/scenario';

// Radar chart
const radarCtx = document.getElementById('radarChart').getContext('2d');
const dimLabels = DATA.dims.map(d => d.split('_').map(w => w[0].toUpperCase() + w.slice(1)).join(' '));
new Chart(radarCtx, {{
  type: 'radar',
  data: {{
    labels: dimLabels,
    datasets: [
      {{
        label: 'dbook',
        data: DATA.dims.map(d => DATA.dbook_avgs[d]),
        borderColor: '#0d7377',
        backgroundColor: 'rgba(13, 115, 119, 0.2)',
        borderWidth: 2,
        pointRadius: 4,
      }},
      {{
        label: 'Baseline (DDL)',
        data: DATA.dims.map(d => DATA.baseline_avgs[d]),
        borderColor: '#666',
        backgroundColor: 'rgba(102, 102, 102, 0.1)',
        borderWidth: 2,
        pointRadius: 4,
      }}
    ]
  }},
  options: {{
    responsive: true,
    maintainAspectRatio: false,
    scales: {{
      r: {{
        min: 0, max: 5,
        ticks: {{ stepSize: 1, color: '#888', backdropColor: 'transparent' }},
        grid: {{ color: '#2a2a4e' }},
        pointLabels: {{ color: '#ccc', font: {{ size: 11 }} }},
      }}
    }},
    plugins: {{ legend: {{ labels: {{ color: '#ccc' }} }} }}
  }}
}});

// Token bar chart
const tokenCtx = document.getElementById('tokenChart').getContext('2d');
const agents = Object.keys(DATA.agent_data);
new Chart(tokenCtx, {{
  type: 'bar',
  data: {{
    labels: agents.map(a => a[0].toUpperCase() + a.slice(1)),
    datasets: [
      {{
        label: 'dbook',
        data: agents.map(a => Math.round(DATA.agent_data[a].dbook_tokens)),
        backgroundColor: '#0d7377',
        borderRadius: 4,
      }},
      {{
        label: 'Baseline (DDL)',
        data: agents.map(a => Math.round(DATA.agent_data[a].baseline_tokens)),
        backgroundColor: '#6b7280',
        borderRadius: 4,
      }}
    ]
  }},
  options: {{
    responsive: true,
    maintainAspectRatio: false,
    scales: {{
      y: {{ ticks: {{ color: '#888' }}, grid: {{ color: '#2a2a4e' }},
           title: {{ display: true, text: 'Tokens / Scenario', color: '#888' }} }},
      x: {{ ticks: {{ color: '#888' }}, grid: {{ display: false }} }}
    }},
    plugins: {{ legend: {{ labels: {{ color: '#ccc' }} }} }}
  }}
}});

// Heatmap table
function cellColor(score) {{
  if (score >= 4) return '#22c55e';
  if (score >= 3) return '#eab308';
  return '#ef4444';
}}

function cellHtml(score) {{
  const bg = cellColor(score);
  return '<span class="cell" style="background:' + bg + '22;color:' + bg + '">' + score.toFixed(1) + '</span>';
}}

let tableHtml = '<thead><tr><th>ID</th><th>Agent</th><th>Diff</th>';
const dimLabelMap = {{
    'table_discovery': 'Table Discovery',
    'sql_correctness': 'SQL Correct',
    'result_accuracy': 'Result Accuracy',
    'response_quality': 'Response Quality'
}};
DATA.dims.forEach(d => {{
  const label = dimLabelMap[d] || d;
  tableHtml += '<th>dbook ' + label + '</th><th>Base ' + label + '</th>';
}});
tableHtml += '<th>dbook Tok</th><th>Base Tok</th></tr></thead><tbody>';

DATA.scenario_data.forEach(s => {{
  tableHtml += '<tr><td class="label">' + s.id + '</td>';
  tableHtml += '<td class="diff">' + s.agent + '</td>';
  tableHtml += '<td class="diff">' + s.difficulty + '</td>';
  DATA.dims.forEach(d => {{
    tableHtml += '<td>' + cellHtml(s.dbook[d] || 0) + '</td>';
    tableHtml += '<td>' + cellHtml(s.baseline[d] || 0) + '</td>';
  }});
  tableHtml += '<td>' + s.dbook_tokens.toLocaleString() + '</td>';
  tableHtml += '<td>' + s.baseline_tokens.toLocaleString() + '</td>';
  tableHtml += '</tr>';
}});
tableHtml += '</tbody>';
document.getElementById('heatmapTable').innerHTML = tableHtml;

// Agent cards
let cardsHtml = '';
agents.forEach(a => {{
  const d = DATA.agent_data[a];
  const delta = (d.dbook_avg - d.baseline_avg).toFixed(1);
  const tokSave = ((1 - d.dbook_tokens / d.baseline_tokens) * 100).toFixed(0);
  cardsHtml += '<div class="agent-card"><h3>' + a + ' Agent</h3>';
  cardsHtml += '<div class="agent-stat"><span class="label">dbook score</span><span style="color:#0d7377">' + d.dbook_avg.toFixed(1) + '/5</span></div>';
  cardsHtml += '<div class="agent-stat"><span class="label">Baseline score</span><span style="color:#888">' + d.baseline_avg.toFixed(1) + '/5</span></div>';
  cardsHtml += '<div class="agent-stat"><span class="label">Delta</span><span style="color:#22c55e">+' + delta + '</span></div>';
  cardsHtml += '<div class="agent-stat"><span class="label">Token savings</span><span style="color:#3b82f6">' + tokSave + '%</span></div>';
  cardsHtml += '</div>';
}});
document.getElementById('agentCards').innerHTML = cardsHtml;
</script>
</body>
</html>"""

    output_path.write_text(html)
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Generate dbook benchmark HTML report")
    parser.add_argument("--input", default="results/benchmark_results.json")
    parser.add_argument("--output", default="results/benchmark_report.html")
    parser.add_argument("--no-open", action="store_true", help="Don't open in browser")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        logger.error("Error: %s not found. Run 'make run' first.", input_path)
        return

    data = json.loads(input_path.read_text())
    generate_report(data, output_path)
    logger.info("Report generated: %s", output_path)

    if not args.no_open:
        webbrowser.open(str(output_path.resolve()))


if __name__ == "__main__":
    main()
