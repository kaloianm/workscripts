#!/usr/bin/env python3
"""
Analyze Locust stats_history.csv for one or two MongoDB experiment runs.

Reads phase boundaries from <ExperimentName>/experiment_metadata.json.
Supports single-run summary and two-run comparison.

Usage:
    # Single run summary
    python3 analyze_locust_run.py DeleteMany1TB-String [--output-html report.html]

    # Two-run comparison
    python3 analyze_locust_run.py DeleteMany1TB-String --compare FastBulkDelete1TB-String [--output-html cmp.html]

    # Override CSV path (e.g. for a live download)
    python3 analyze_locust_run.py DeleteMany1TB-String --csv path/to/live.csv [--output-html report.html]

    # Override phase boundaries directly (for runs without metadata)
    python3 analyze_locust_run.py OldRun --locust-start-unix 1234567890 --delete-start-unix 1234571490
"""

import argparse
import csv
import json
import os
import statistics
import sys
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _load_metadata(experiment_dir, locust_start_unix=None, delete_start_unix=None):
    """Load phase boundary timestamps from metadata file or CLI overrides."""
    meta_path = os.path.join(experiment_dir, 'experiment_metadata.json')
    meta = {}
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            meta = json.load(f)

    ls = locust_start_unix or meta.get('locust_start_unix')
    ds = delete_start_unix or meta.get('delete_start_unix')

    if not ls or not ds:
        raise ValueError(
            f'Phase boundaries not found for {experiment_dir}.\n'
            f'Either create {meta_path} or pass --locust-start-unix and --delete-start-unix.')

    return float(ls), float(ds), meta


def _find_csv(experiment_dir, csv_override=None):
    """Return path to stats_history CSV, checking gathered logs and root of experiment dir."""
    if csv_override:
        return csv_override
    candidates = [
        os.path.join(experiment_dir, 'logs', 'locust', 'locust_results_stats_history.csv'),
        os.path.join(experiment_dir, 'locust_results_stats_history_live.csv'),
        os.path.join(experiment_dir, 'locust_results_stats_history.csv'),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    raise FileNotFoundError(f'No locust_results_stats_history.csv found for {experiment_dir}.\n'
                            f'Run gather-logs first, or pass --csv <path>.')


def _find_stats_csv(experiment_dir):
    """Return path to per-transaction stats.csv (optional)."""
    candidates = [
        os.path.join(experiment_dir, 'logs', 'locust', 'locust_results_stats.csv'),
        os.path.join(experiment_dir, 'locust_results_stats.csv'),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return None


def parse_history(history_csv, locust_start, delete_start):
    """
    Parse locust_results_stats_history.csv and split into three phases:
      - warmup: rows before (delete_start - 5 minutes)
      - baseline: last 5 minutes before delete_start  (primary comparison window)
      - during: rows from delete_start onward

    Returns a dict with per-phase stats and raw time-series data for charting.
    """
    baseline_window_start = delete_start - 5 * 60

    warmup_p50, warmup_p99 = [], []
    baseline_rps, baseline_p50, baseline_p99, baseline_p999, baseline_users = [], [], [], [], []
    during_rps, during_p50, during_p99, during_p999, during_users = [], [], [], [], []

    # Raw time-series for Chart.js (elapsed hours from delete_start, Aggregated rows only)
    timeseries = []  # [{elapsed_h, p50, p75, p90, p99, p999}]

    with open(history_csv) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('Name') != 'Aggregated':
                continue
            try:
                ts = float(row['Timestamp'])
                rps = float(row['Requests/s'])
                users = int(row['User Count'])
                p50_raw = row.get('50%', '')
                p99_raw = row.get('99%', '')
                p999_raw = row.get('99.9%', '')
                if not p50_raw or p50_raw in ('N/A', '0') or rps == 0:
                    continue
                p50 = float(p50_raw)
                p99 = float(p99_raw) if p99_raw and p99_raw not in ('N/A', '0') else p50
                p999 = float(p999_raw) if p999_raw and p999_raw not in ('N/A', '0') else p99
            except (ValueError, KeyError):
                continue

            if ts < locust_start:
                continue

            elapsed_h = (ts - delete_start) / 3600

            if ts < baseline_window_start:
                warmup_p50.append(p50)
                warmup_p99.append(p99)
            elif ts < delete_start:
                baseline_rps.append(rps)
                baseline_p50.append(p50)
                baseline_p99.append(p99)
                baseline_p999.append(p999)
                baseline_users.append(users)
            else:
                during_rps.append(rps)
                during_p50.append(p50)
                during_p99.append(p99)
                during_p999.append(p999)
                during_users.append(users)

            # Collect for chart: from PRE_HOURS before delete through POST_HOURS after
            PRE_HOURS = 0.5
            POST_HOURS = 24.0
            if -PRE_HOURS <= elapsed_h <= POST_HOURS:
                p75_raw = row.get('75%', '')
                p90_raw = row.get('90%', '')
                p75 = float(p75_raw) if p75_raw and p75_raw not in ('N/A', '0') else p50
                p90 = float(p90_raw) if p90_raw and p90_raw not in ('N/A', '0') else p50
                timeseries.append({
                    'elapsed_h': round(elapsed_h, 4),
                    'p50': p50,
                    'p75': p75,
                    'p90': p90,
                    'p99': p99,
                    'p999': p999
                })

    def med(lst):
        return round(statistics.median(lst), 1) if lst else float('nan')

    def mn(lst):
        return round(statistics.mean(lst), 1) if lst else float('nan')

    return {
        'warmup': {
            'p50_median': med(warmup_p50),
            'p99_median': med(warmup_p99),
            'samples': len(warmup_p50),
        },
        'baseline': {
            'rps_mean': mn(baseline_rps),
            'p50_median': med(baseline_p50),
            'p99_median': med(baseline_p99),
            'p999_median': med(baseline_p999),
            'users_mean': mn(baseline_users),
            'samples': len(baseline_rps),
        },
        'during': {
            'rps_mean': mn(during_rps),
            'p50_median': med(during_p50),
            'p99_median': med(during_p99),
            'p999_median': med(during_p999),
            'users_mean': mn(during_users),
            'samples': len(during_rps),
        },
        'timeseries': timeseries,
    }


def parse_stats(stats_csv):
    """Parse locust_results_stats.csv into a dict keyed by transaction name."""
    if not stats_csv:
        return {}
    rows = {}
    with open(stats_csv) as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows[row['Name']] = row
    return rows


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def fmt(val, decimals=1):
    if val != val:  # nan
        return 'N/A'
    return f'{val:.{decimals}f}'


def pct_change(before, after):
    if before == 0 or before != before:
        return float('nan')
    return (after - before) / before * 100


def pct_str(before, after):
    v = pct_change(before, after)
    if v != v:
        return 'N/A'
    sign = '+' if v >= 0 else ''
    return f'{sign}{v:.1f}%'


def ts_to_utc(unix_ts):
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')


# ---------------------------------------------------------------------------
# Terminal output
# ---------------------------------------------------------------------------


def print_single_run(name, data, meta, stats_rows):
    delete_start = meta.get('delete_start_unix', 0)
    locust_start = meta.get('locust_start_unix', 0)
    action = meta.get('action', 'unknown')
    warmup_min = (delete_start - locust_start) / 60 if delete_start and locust_start else 0

    print(f'\n{"="*80}')
    print(f'EXPERIMENT: {name}')
    print(f'{"="*80}')
    print(f'  Action:       {action}')
    if locust_start:
        print(f'  Locust start: {ts_to_utc(locust_start)}')
    if delete_start:
        print(f'  Delete start: {ts_to_utc(delete_start)}  (warm-up: {warmup_min:.0f} min)')

    w = data['warmup']
    b = data['baseline']
    d = data['during']

    print(f'\n{"─"*80}')
    print(f'LATENCY SUMMARY (cumulative Locust percentiles — see caveat below)')
    print(f'{"─"*80}')
    print(f'  {"Phase":<28} {"Samples":>8} {"P50 ms":>8} {"P99 ms":>8} {"P99.9 ms":>10}')
    print(f'  {"─"*60}')
    print(
        f'  {"Warm-up (entire period)":<28} {w["samples"]:>8} {fmt(w["p50_median"]):>8} {fmt(w["p99_median"]):>8}'
    )
    print(
        f'  {"Baseline (last 5 min)":<28} {b["samples"]:>8} {fmt(b["p50_median"]):>8} {fmt(b["p99_median"]):>8} {fmt(b["p999_median"]):>10}'
    )
    print(
        f'  {"During delete":<28} {d["samples"]:>8} {fmt(d["p50_median"]):>8} {fmt(d["p99_median"]):>8} {fmt(d["p999_median"]):>10}'
    )
    print(f'  {"─"*60}')
    print(
        f'  {"Δ change (baseline→during)":<28} {"":>8} {pct_str(b["p50_median"], d["p50_median"]):>8} {pct_str(b["p99_median"], d["p99_median"]):>8}'
    )
    print(f'\n  Throughput: baseline {fmt(b["rps_mean"])} RPS → during {fmt(d["rps_mean"])} RPS'
          f' ({pct_str(b["rps_mean"], d["rps_mean"])})')

    if stats_rows:
        _print_per_txn({name: stats_rows})

    print(f'\n  ⚠  P99 caveat: Locust reports cumulative percentiles since launch.')
    print(f'     P99 may appear to improve during the delete — this is a statistical')
    print(f'     artifact (growing denominator), not a real improvement. Trust P50.')


def print_comparison(name1, data1, meta1, name2, data2, meta2):
    b1, d1 = data1['baseline'], data1['during']
    b2, d2 = data2['baseline'], data2['during']

    print(f'\n{"="*90}')
    print(f'COMPARISON: {name1}  vs  {name2}')
    print(f'{"="*90}')
    print(
        f'  {"":40} {"Baseline P50":>13} {"During P50":>12} {"Δ P50":>8} {"Baseline P99":>13} {"During P99":>12} {"Δ P99":>8}'
    )
    print(f'  {"─"*100}')

    def row(label, b, d):
        return (f'  {label:<40} {fmt(b["p50_median"]):>13} {fmt(d["p50_median"]):>12}'
                f' {pct_str(b["p50_median"], d["p50_median"]):>8}'
                f' {fmt(b["p99_median"]):>13} {fmt(d["p99_median"]):>12}'
                f' {pct_str(b["p99_median"], d["p99_median"]):>8}')

    print(row(name1[:40], b1, d1))
    print(row(name2[:40], b2, d2))

    print(f'\n  Throughput: {name1[:30]}: {fmt(b1["rps_mean"])} → {fmt(d1["rps_mean"])} RPS'
          f' ({pct_str(b1["rps_mean"], d1["rps_mean"])})')
    print(f'             {name2[:30]}: {fmt(b2["rps_mean"])} → {fmt(d2["rps_mean"])} RPS'
          f' ({pct_str(b2["rps_mean"], d2["rps_mean"])})')

    print(f'\n  ⚠  P99 caveat: cumulative percentiles — trust P50 for degradation signal.')


def _print_per_txn(stats_by_name):
    TXN_TYPES = [
        'select_shard_key', 'update_by_shard_key', 'select_shard_key_by_secondary_index',
        'insert_new_shard_key', 'Aggregated'
    ]

    for run_name, stats in stats_by_name.items():
        print(f'\n  Per-transaction breakdown — {run_name}')
        print(
            f'  {"Transaction":<42} {"Requests":>10} {"RPS":>7} {"P50ms":>7} {"P99ms":>7} {"Failures":>9}'
        )
        print(f'  {"─"*85}')
        for txn in TXN_TYPES:
            row = stats.get(txn)
            if not row:
                continue
            label = txn if txn != 'Aggregated' else '--- TOTAL ---'
            req = int(row.get('Request Count', 0))
            rps = float(row.get('Requests/s', 0))
            p50 = row.get('50%', 'N/A')
            p99 = row.get('99%', 'N/A')
            fails = int(row.get('Failure Count', 0))
            print(f'  {label:<42} {req:>10,} {rps:>7.1f} {p50:>7} {p99:>7} {fails:>9,}')


# ---------------------------------------------------------------------------
# HTML output
# ---------------------------------------------------------------------------

_HTML_TEMPLATE = '''\
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>{title}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<style>
  body {{ font-family: sans-serif; margin: 2em; background: #1a1a1a; color: #e0e0e0; }}
  h1, h2, h3 {{ color: #ccc; }}
  table {{ border-collapse: collapse; margin: 1em 0; width: 100%; }}
  th, td {{ border: 1px solid #444; padding: 6px 12px; text-align: right; }}
  th {{ background: #2a2a2a; color: #aaa; }}
  tr:nth-child(even) {{ background: #222; }}
  td:first-child, th:first-child {{ text-align: left; }}
  .delta-up {{ color: #f55; }}
  .delta-down {{ color: #5f5; }}
  .caveat {{ background: #2a1a00; border-left: 3px solid #f90; padding: 0.8em 1em; margin: 1em 0; }}
  .chart-wrap {{ width: 100%; max-width: 1200px; margin: 1.5em 0; }}
  canvas {{ background: #111; border-radius: 4px; }}
</style>
</head>
<body>
<h1>{title}</h1>
<p>{subtitle}</p>

<div class="caveat">
  <strong>⚠ P99 caveat:</strong> Locust reports <em>cumulative</em> percentiles since launch,
  not rolling. P99 may appear to improve during the delete — this is a statistical artifact
  of the growing denominator. <strong>P50 is the most reliable degradation signal.</strong>
</div>

{phase_tables_html}

<h2>Latency Time Series</h2>
<p>Shaded region = pre-delete baseline. Vertical line at t=0 = delete start.</p>
{charts_html}

<script>
{chart_scripts}
</script>
</body>
</html>
'''

_CHART_COLORS = {
    'P50': '#1f77b4',
    'P75': '#4c9de0',
    'P90': '#7fbfff',
    'P99': '#e05c1f',
    'P99.9': '#c0392b',
}


def _phase_table_html(name, data, meta):
    b, d, w = data['baseline'], data['during'], data['warmup']
    action = meta.get('action', 'unknown')
    locust_start = meta.get('locust_start_unix', 0)
    delete_start = meta.get('delete_start_unix', 0)
    warmup_min = (delete_start - locust_start) / 60 if delete_start and locust_start else 0

    def delta_cell(before, after):
        v = pct_change(before, after)
        if v != v:
            return '<td>N/A</td>'
        cls = 'delta-up' if v > 0 else 'delta-down'
        sign = '+' if v >= 0 else ''
        return f'<td class="{cls}">{sign}{v:.1f}%</td>'

    html = f'<h2>{name}</h2>\n'
    html += f'<p>Action: <code>{action}</code>'
    if locust_start:
        html += f' &nbsp;|&nbsp; Locust start: {ts_to_utc(locust_start)}'
    if delete_start:
        html += f' &nbsp;|&nbsp; Delete start: {ts_to_utc(delete_start)} (warm-up: {warmup_min:.0f} min)'
    html += '</p>\n'

    html += '''<table>
<thead><tr>
  <th>Phase</th><th>Samples</th><th>P50 ms</th><th>P99 ms</th><th>P99.9 ms</th><th>RPS</th>
</tr></thead><tbody>
'''
    html += (
        f'<tr><td>Warm-up (full period)</td><td>{w["samples"]}</td>'
        f'<td>{fmt(w["p50_median"])}</td><td>{fmt(w["p99_median"])}</td><td>—</td><td>—</td></tr>\n'
    )
    html += (f'<tr><td>Baseline (last 5 min before delete)</td><td>{b["samples"]}</td>'
             f'<td>{fmt(b["p50_median"])}</td><td>{fmt(b["p99_median"])}</td>'
             f'<td>{fmt(b["p999_median"])}</td><td>{fmt(b["rps_mean"])}</td></tr>\n')
    html += (f'<tr><td>During delete</td><td>{d["samples"]}</td>'
             f'<td>{fmt(d["p50_median"])}</td><td>{fmt(d["p99_median"])}</td>'
             f'<td>{fmt(d["p999_median"])}</td><td>{fmt(d["rps_mean"])}</td></tr>\n')
    html += f'<tr><td><strong>Δ baseline → during</strong></td><td></td>'
    html += delta_cell(b['p50_median'], d['p50_median'])
    html += delta_cell(b['p99_median'], d['p99_median'])
    html += delta_cell(b['p999_median'], d['p999_median'])
    html += delta_cell(b['rps_mean'], d['rps_mean'])
    html += '</tr>\n</tbody></table>\n'
    return html


def _comparison_table_html(name1, data1, name2, data2):
    b1, d1 = data1['baseline'], data1['during']
    b2, d2 = data2['baseline'], data2['during']

    def delta_cell(before, after):
        v = pct_change(before, after)
        if v != v:
            return '<td>N/A</td>'
        cls = 'delta-up' if v > 0 else 'delta-down'
        sign = '+' if v >= 0 else ''
        return f'<td class="{cls}">{sign}{v:.1f}%</td>'

    html = '<h2>Side-by-Side Comparison</h2>\n'
    html += '''<table>
<thead><tr>
  <th>Run</th>
  <th>Baseline P50</th><th>During P50</th><th>Δ P50</th>
  <th>Baseline P99</th><th>During P99</th><th>Δ P99</th>
  <th>Baseline RPS</th><th>During RPS</th><th>Δ RPS</th>
</tr></thead><tbody>
'''
    for name, b, d in [(name1, b1, d1), (name2, b2, d2)]:
        html += f'<tr><td>{name}</td>'
        html += f'<td>{fmt(b["p50_median"])}</td><td>{fmt(d["p50_median"])}</td>'
        html += delta_cell(b['p50_median'], d['p50_median'])
        html += f'<td>{fmt(b["p99_median"])}</td><td>{fmt(d["p99_median"])}</td>'
        html += delta_cell(b['p99_median'], d['p99_median'])
        html += f'<td>{fmt(b["rps_mean"])}</td><td>{fmt(d["rps_mean"])}</td>'
        html += delta_cell(b['rps_mean'], d['rps_mean'])
        html += '</tr>\n'
    html += '</tbody></table>\n'
    return html


def _chart_html_and_script(canvas_id, label, timeseries):
    """Return (div_html, js_string) for one Chart.js time-series chart."""
    if not timeseries:
        return f'<p>No time-series data for {label}.</p>', ''

    labels = [d['elapsed_h'] for d in timeseries]
    series = {
        'P50': [d['p50'] for d in timeseries],
        'P75': [d['p75'] for d in timeseries],
        'P90': [d['p90'] for d in timeseries],
        'P99': [d['p99'] for d in timeseries],
        'P99.9': [d['p999'] for d in timeseries],
    }

    datasets_js = []
    for pct_label, values in series.items():
        color = _CHART_COLORS[pct_label]
        datasets_js.append(
            f'{{label:"{pct_label}",data:{json.dumps(values)},borderColor:"{color}",'
            f'backgroundColor:"{color}22",borderWidth:1.5,pointRadius:0,tension:0.1}}')

    pre_hours = 0.5
    div_html = (f'<div class="chart-wrap"><canvas id="{canvas_id}" height="120"></canvas></div>\n')

    js = f'''
(function() {{
  const labels = {json.dumps(labels)};
  const ctx = document.getElementById("{canvas_id}").getContext("2d");

  // Shade the pre-delete baseline region
  const baselinePlugin = {{
    id: "baseline",
    beforeDraw(chart) {{
      const {{ctx: c, chartArea, scales}} = chart;
      if (!chartArea) return;
      const x0 = scales.x.getPixelForValue(-{pre_hours});
      const x1 = scales.x.getPixelForValue(0);
      c.save();
      c.fillStyle = "rgba(255,249,196,0.15)";
      c.fillRect(x0, chartArea.top, x1 - x0, chartArea.height);
      c.restore();
    }}
  }};

  new Chart(ctx, {{
    type: "line",
    plugins: [baselinePlugin],
    data: {{labels, datasets: [{",".join(datasets_js)}]}},
    options: {{
      animation: false,
      plugins: {{
        title: {{display: true, text: "{label}", color: "#ccc", font: {{size: 14}}}},
        legend: {{labels: {{color: "#aaa"}}}},
        tooltip: {{mode: "index", intersect: false}}
      }},
      scales: {{
        x: {{
          type: "linear",
          title: {{display: true, text: "Hours since delete start (negative = pre-delete baseline)", color: "#888"}},
          ticks: {{color: "#888"}},
          grid: {{color: "#333"}},
          min: -{pre_hours},
          max: Math.max(...labels)
        }},
        y: {{
          title: {{display: true, text: "Latency (ms) — cumulative Locust percentiles", color: "#888"}},
          ticks: {{color: "#888"}},
          grid: {{color: "#333"}},
          min: 0
        }}
      }}
    }}
  }});
}})();
'''
    return div_html, js


def write_html(output_path, title, subtitle, runs_data, comparison=False):
    """
    Write a self-contained HTML analysis report.

    runs_data: list of (name, data, meta) tuples — one or two entries
    """
    phase_tables = []
    charts_html_parts = []
    chart_scripts = []

    for i, (name, data, meta) in enumerate(runs_data):
        phase_tables.append(_phase_table_html(name, data, meta))
        canvas_id = f'chart_{i}'
        div_html, js = _chart_html_and_script(canvas_id, name, data['timeseries'])
        charts_html_parts.append(div_html)
        chart_scripts.append(js)

    if comparison and len(runs_data) == 2:
        n1, d1, _ = runs_data[0]
        n2, d2, _ = runs_data[1]
        phase_tables.insert(0, _comparison_table_html(n1, d1, n2, d2))

    html = _HTML_TEMPLATE.format(
        title=title,
        subtitle=subtitle,
        phase_tables_html='\n'.join(phase_tables),
        charts_html=''.join(charts_html_parts),
        chart_scripts='\n'.join(chart_scripts),
    )

    with open(output_path, 'w') as f:
        f.write(html)

    print(f'HTML report written to: {output_path}')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('experiment', help='Experiment directory name (e.g. DeleteMany1TB-String)')
    ap.add_argument('--compare', metavar='EXPERIMENT2', help='Second experiment to compare against')
    ap.add_argument('--csv', metavar='PATH',
                    help='Override path to locust_results_stats_history.csv for the first run')
    ap.add_argument('--csv2', metavar='PATH',
                    help='Override path to locust_results_stats_history.csv for the second run')
    ap.add_argument('--locust-start-unix', type=float,
                    help='Override locust start Unix timestamp for the first run')
    ap.add_argument('--delete-start-unix', type=float,
                    help='Override delete start Unix timestamp for the first run')
    ap.add_argument('--output-html', metavar='FILE', help='Write HTML analysis report to this file')
    args = ap.parse_args()

    # --- First run ---
    locust_start1, delete_start1, meta1 = _load_metadata(args.experiment, args.locust_start_unix,
                                                         args.delete_start_unix)
    csv1 = _find_csv(args.experiment, args.csv)
    print(f'Analyzing {args.experiment} from {csv1} ...', file=sys.stderr)
    data1 = parse_history(csv1, locust_start1, delete_start1)
    stats1 = parse_stats(_find_stats_csv(args.experiment))

    if not args.compare:
        print_single_run(args.experiment, data1, meta1, stats1)
        if args.output_html:
            title = f'Locust Analysis — {args.experiment}'
            action = meta1.get('action', '')
            subtitle = f'Action: {action}'
            write_html(args.output_html, title, subtitle, [(args.experiment, data1, meta1)])
        return

    # --- Second run (comparison) ---
    locust_start2, delete_start2, meta2 = _load_metadata(args.compare)
    csv2 = _find_csv(args.compare, args.csv2)
    print(f'Analyzing {args.compare} from {csv2} ...', file=sys.stderr)
    data2 = parse_history(csv2, locust_start2, delete_start2)
    stats2 = parse_stats(_find_stats_csv(args.compare))

    print_comparison(args.experiment, data1, meta1, args.compare, data2, meta2)
    print()
    print_single_run(args.experiment, data1, meta1, stats1)
    print()
    print_single_run(args.compare, data2, meta2, stats2)

    if args.output_html:
        title = f'Locust Comparison — {args.experiment} vs {args.compare}'
        subtitle = (f'{args.experiment}: {meta1.get("action","?")} &nbsp;|&nbsp; '
                    f'{args.compare}: {meta2.get("action","?")}')
        write_html(args.output_html, title, subtitle, [(args.experiment, data1, meta1),
                                                       (args.compare, data2, meta2)],
                   comparison=True)


if __name__ == '__main__':
    main()
