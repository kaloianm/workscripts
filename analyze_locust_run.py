#!/usr/bin/env python3
"""
Analyze Locust stats_history.csv for one or more MongoDB experiment runs.

Reads phase boundaries from <ExperimentName>/experiment_metadata.json.
Supports single-run summary and multi-run visual comparison.

Usage:
    # Single run summary
    python3 analyze_locust_run.py DeleteMany1TB-String [--output-html report.html]

    # Multi-run comparison plots (any combination of flags)
    python3 analyze_locust_run.py A B C --compare-locust-latencies --compare-ftdc

    # Override CSV / phase boundaries (single-run mode only)
    python3 analyze_locust_run.py OldRun --csv path/to/live.csv
    python3 analyze_locust_run.py OldRun --locust-start-unix 1234567890 --delete-start-unix 1234571490
"""

import argparse
import csv
import glob
import json
import os
import statistics
import sys
import tarfile
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


# ---------------------------------------------------------------------------
# Delete-log timing
# ---------------------------------------------------------------------------

_TS_FMT = '%Y-%m-%d %H:%M:%S,%f'


def _parse_delete_log(experiment_dir):
    """Return (delete_start_unix, delete_end_unix) parsed from the operation log.

    Reads locust_results_fast_bulk_delete.log or locust_results_delete_many.log.
    Lines look like:
        2026-05-07 16:29:56,450 INFO START command=...
        2026-05-11 11:53:26,168 INFO END
    Returns (None, None) if no log found or no timestamps parsed.
    """
    for log_name in ('locust_results_fast_bulk_delete.log', 'locust_results_delete_many.log'):
        path = os.path.join(experiment_dir, 'logs', 'locust', log_name)
        if not os.path.exists(path):
            continue
        start_unix = end_unix = None
        with open(path) as f:
            for line in f:
                ts_str = line[:23]
                try:
                    dt = datetime.strptime(ts_str, _TS_FMT).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
                if ' INFO START' in line and start_unix is None:
                    start_unix = dt.timestamp()
                elif ' INFO END' in line:
                    end_unix = dt.timestamp()
        if start_unix is not None:
            return start_unix, end_unix
    return None, None


# ---------------------------------------------------------------------------
# FTDC helpers
# ---------------------------------------------------------------------------

_FTDC_TOOL_PATH = os.path.expanduser('~/workspace/llm-ftdc-analysis')

# Substrings matched (case-sensitive) against full dotted metric key.
# Deliberately narrow to avoid accumulating hundreds of MB of metric data.
# Server-level WiredTiger cache metrics only (exact key suffixes under serverStatus.wiredTiger.cache.*)
_FTDC_METRIC_SUBSTRINGS = [
    'serverStatus.wiredTiger.cache.tracked dirty bytes in the cache',
    'serverStatus.wiredTiger.cache.bytes currently in the cache',
    'serverStatus.wiredTiger.cache.leaf pages read into cache',
    'serverStatus.wiredTiger.cache.tracked dirty internal page bytes in the cache',
    'serverStatus.wiredTiger.cache.application thread time evicting (usecs)',
    'serverStatus.wiredTiger.cache.eviction server unable to reach eviction goal',
    'serverStatus.wiredTiger.cache.eviction server slept, because we did not make progress with eviction',
    'serverStatus.wiredTiger.thread-yield.application thread time waiting for cache (usecs)',
]

_FTDC_FRIENDLY = {
    'serverStatus.wiredTiger.cache.tracked dirty bytes in the cache': 'Dirty bytes in cache',
    'serverStatus.wiredTiger.cache.bytes currently in the cache': 'Cache bytes used',
    'serverStatus.wiredTiger.cache.leaf pages read into cache': 'Leaf pages read from disk/s',
    'serverStatus.wiredTiger.cache.tracked dirty internal page bytes in the cache': 'Dirty internal page bytes',
    'serverStatus.wiredTiger.cache.application thread time evicting (usecs)': 'App-thread eviction time (us/s)',
    'serverStatus.wiredTiger.cache.eviction server unable to reach eviction goal': 'Eviction goal misses/s',
    'serverStatus.wiredTiger.cache.eviction server slept, because we did not make progress with eviction': 'Eviction server sleeps/s',
    'serverStatus.wiredTiger.thread-yield.application thread time waiting for cache (usecs)': 'App-thread cache wait (us/s)',
}

# These are cumulative counters — convert to per-second rates.
_FTDC_COUNTER_SUBSTRINGS = [
    'serverStatus.wiredTiger.cache.leaf pages read into cache',
    'serverStatus.wiredTiger.cache.application thread time evicting (usecs)',
    'serverStatus.wiredTiger.cache.eviction server unable to reach eviction goal',
    'serverStatus.wiredTiger.cache.eviction server slept, because we did not make progress with eviction',
    'serverStatus.wiredTiger.thread-yield.application thread time waiting for cache (usecs)',
]


def _metric_label(key):
    return _FTDC_FRIENDLY.get(key, key.split('.')[-1])


def _is_counter_metric(key):
    return key in _FTDC_COUNTER_SUBSTRINGS


def _find_ftdc_data(experiment_dir):
    """Return path to diagnostic.data directory, extracting tar.gz if needed."""
    # Already-extracted directory (FastBulkDelete ships this way after gather-logs)
    for candidate in glob.glob(os.path.join(experiment_dir, 'logs', 'mongod-rs-*')):
        if os.path.isdir(candidate):
            diag = os.path.join(candidate, 'diagnostic.data')
            if os.path.isdir(diag):
                return diag

    # Compressed archive — extract once
    for archive in glob.glob(os.path.join(experiment_dir, 'logs', 'mongod-rs-*.tar.gz')):
        extract_dir = archive[:-len('.tar.gz')]
        diag = os.path.join(extract_dir, 'diagnostic.data')
        if not os.path.isdir(diag):
            print(f'  Extracting {os.path.basename(archive)} ...', file=sys.stderr)
            os.makedirs(extract_dir, exist_ok=True)
            with tarfile.open(archive) as tf:
                tf.extractall(path=os.path.dirname(extract_dir))
        if os.path.isdir(diag):
            return diag
    return None


def _filter_ftdc_to_window(ftdc_path, t0_unix, max_hours):
    """Return a temp directory containing copies of only FTDC files within the time window.

    FTDC file names encode their start time (metrics.YYYY-MM-DDTHH-MM-SSZ-NNNNN).
    We keep any file whose name-encoded timestamp is <= t0_unix + max_hours + 1h.
    The caller is responsible for cleaning up the returned tempdir.
    """
    import re
    import shutil
    import tempfile

    end_unix = t0_unix + (max_hours + 1) * 3600  # +1h margin for the last partial file

    files = sorted(f for f in os.listdir(ftdc_path) if f.startswith('metrics.'))
    in_window = []
    for fn in files:
        m = re.match(r'metrics\.(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z)', fn)
        if not m:
            continue
        try:
            dt = datetime.strptime(m.group(1), '%Y-%m-%dT%H-%M-%SZ').replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if dt.timestamp() <= end_unix:
            in_window.append(fn)

    # Copy to a local temp dir on the same filesystem as /tmp so the reader can find them.
    # Symlinks are ignored by read_ftdc_parallel; hardlinks fail cross-device.
    tmpdir = tempfile.mkdtemp(prefix='ftdc_window_')
    for fn in in_window:
        shutil.copy2(os.path.join(ftdc_path, fn), os.path.join(tmpdir, fn))
    return tmpdir, len(in_window), len(files)


def _collect_ftdc_timeseries(ftdc_path, t0_unix, max_hours=24, max_workers=1):
    """Return {metric_key: {'ts': [unix_s,...], 'val': [v,...]}} for key cache metrics.

    Only keeps metrics matching _FTDC_METRIC_SUBSTRINGS, reading at most
    max_hours of data starting from t0_unix.  Symlinks only the relevant
    FTDC files into a temp dir before reading to avoid OOM on long archives.
    Uses max_workers=1 (single worker) to minimise peak memory.
    """
    import shutil

    if _FTDC_TOOL_PATH not in sys.path:
        sys.path.insert(0, _FTDC_TOOL_PATH)
    try:
        import readers_pi as readers
    except ImportError as exc:
        raise ImportError(
            f'readers_pi not found.  Set up llm-ftdc-analysis at {_FTDC_TOOL_PATH}.\n'
            f'  git clone https://github.com/10gen/employees /tmp/emp-sparse\n'
            f'  cd /tmp/emp-sparse && git sparse-checkout set home/luke.pearson/llm-ftdc-analysis\n'
            f'  cp -r home/luke.pearson/llm-ftdc-analysis {_FTDC_TOOL_PATH}') from exc

    end_unix = t0_unix + max_hours * 3600
    tmpdir, n_files, total_files = _filter_ftdc_to_window(ftdc_path, t0_unix, max_hours)
    print(f'    using {n_files}/{total_files} FTDC files within {max_hours}h window',
          file=sys.stderr)

    result = {}
    try:
        for chunk in readers.read_ftdc_parallel(tmpdir, max_workers=max_workers):
            if chunk is None:
                continue

            ts_vals = None
            matched = {}
            for raw_key, values in chunk.items():
                k = '.'.join(str(x) for x in raw_key)
                if k == 'serverStatus.start':
                    ts_vals = [v / 1000.0 for v in values]  # ms → s
                elif any(sub in k for sub in _FTDC_METRIC_SUBSTRINGS):
                    matched[k] = list(values)

            if ts_vals is None or not matched:
                continue

            # Skip chunks entirely outside the window
            if ts_vals[-1] < t0_unix or ts_vals[0] > end_unix:
                continue

            for k, vals in matched.items():
                if k not in result:
                    result[k] = {'ts': [], 'val': []}
                for i, ts in enumerate(ts_vals):
                    if t0_unix <= ts <= end_unix and i < len(vals):
                        result[k]['ts'].append(ts)
                        result[k]['val'].append(float(vals[i]))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    return result


def _rates_from_timeseries(ts_list, val_list):
    """Convert a cumulative counter time-series to per-second rates."""
    rates_ts, rates_val = [], []
    for i in range(1, len(ts_list)):
        dt = ts_list[i] - ts_list[i - 1]
        if dt <= 0:
            continue
        delta = val_list[i] - val_list[i - 1]
        rates_ts.append(ts_list[i])
        rates_val.append(max(0.0, delta / dt))
    return rates_ts, rates_val


def _select_top_ftdc_metrics(all_data, n=3):
    """Return the n metric keys with the highest mean ratio across experiments."""
    common = set.intersection(*[set(d.keys()) for d in all_data.values()]) if all_data else set()
    scores = {}
    for metric in common:
        means = []
        for exp_data in all_data.values():
            ts = exp_data[metric]['ts']
            vals = exp_data[metric]['val']
            if _is_counter_metric(metric):
                _, rv = _rates_from_timeseries(ts, vals)
                sample = rv
            else:
                sample = vals
            clean = [v for v in sample if v is not None and v > 0]
            if clean:
                means.append(statistics.mean(clean))
        if len(means) < 2 or min(means) == 0:
            continue
        scores[metric] = max(means) / min(means)
    return sorted(scores, key=scores.get, reverse=True)[:n]


# ---------------------------------------------------------------------------
# Comparison plots
# ---------------------------------------------------------------------------

_PLOT_COLORS = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red', 'tab:purple']
_BUCKET_H = 0.25  # 15-minute buckets


def _plot_locust_latencies(experiments, output_path='comparison_locust.png'):
    """Generate a per-15-min max latency comparison PNG.

    experiments: list of (name, meta) tuples.
    """
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker

    metrics = [('50%', 'P50'), ('90%', 'P90'), ('99%', 'P99')]
    fig, axes = plt.subplots(3, 1, figsize=(16, 12), sharex=True)
    fig.suptitle(
        'OLTP Latency — max reported Locust value per 15 min, log scale\n'
        '(time-normalised to experiment start; red dashed = delete fires at t=1h)',
        fontsize=11,
        y=0.995)

    max_h = 0
    for ax, (col, pname) in zip(axes, metrics):
        for (name, meta), color in zip(experiments, _PLOT_COLORS):
            t0 = meta['locust_start_unix']
            _, delete_end = _parse_delete_log(name)

            buckets = {}
            csv_path = _find_csv(name)
            with open(csv_path) as f:
                for row in csv.DictReader(f):
                    if row['Name'] != 'Aggregated':
                        continue
                    val = row.get(col, '')
                    if val in ('N/A', '', '0'):
                        continue
                    try:
                        v = float(val)
                    except ValueError:
                        continue
                    if v <= 0:
                        continue
                    h = (float(row['Timestamp']) - t0) / 3600.0
                    if h < 0:
                        continue
                    b = int(h / _BUCKET_H)
                    buckets[b] = max(buckets.get(b, 0), v)

            xs = sorted(buckets)
            xs_h = [b * _BUCKET_H + _BUCKET_H / 2 for b in xs]
            max_h = max(max_h, max(xs_h, default=0))

            if delete_end is not None:
                completion_h = (delete_end - t0) / 3600.0
                xd = [x for x in xs_h if x <= completion_h]
                xa = [x for x in xs_h if x > completion_h]
                yd = [buckets[b] for b, x in zip(xs, xs_h) if x <= completion_h]
                ya = [buckets[b] for b, x in zip(xs, xs_h) if x > completion_h]
                ax.plot(xd, yd, label=name, color=color, linewidth=1.5, alpha=0.85)
                if xa:
                    ax.plot(xa, ya, color=color, linewidth=1.0, alpha=0.35, linestyle=':',
                            label='_nolegend_')
                ax.axvline(x=completion_h, color=color, linestyle='-', linewidth=1.1,
                           alpha=0.6, label='_nolegend_')
            else:
                ys = [buckets[b] for b in xs]
                ax.plot(xs_h, ys, label=name, color=color, linewidth=1.5, alpha=0.85)

        ax.axvline(x=1.0, color='red', linestyle='--', linewidth=1.2, alpha=0.8,
                   label='delete starts (t=1h)' if ax is axes[0] else '_nolegend_')
        ax.set_ylabel(f'max {pname} / 15 min (ms)', fontsize=9)
        ax.set_yscale('log')
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f'{v:.0f}'))
        ax.grid(axis='both', alpha=0.25, which='both')
        if ax is axes[0]:
            ax.legend(loc='upper right', fontsize=8.5)

    axes[-1].set_xlabel('Hours since experiment start', fontsize=10)
    axes[-1].set_xlim(left=0, right=max_h)
    axes[-1].xaxis.set_major_locator(ticker.MultipleLocator(12))
    axes[-1].xaxis.set_minor_locator(ticker.MultipleLocator(3))

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f'Saved {output_path}')


def _plot_ftdc_comparison(experiments, output_path='comparison_ftdc.png', ftdc_hours=24):
    """Generate a per-second FTDC metric comparison PNG.

    experiments: list of (name, meta) tuples.
    """
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker

    # Collect timeseries for each experiment
    print('Collecting FTDC timeseries (this may take a minute) ...', file=sys.stderr)
    all_data = {}
    for name, meta in experiments:
        t0 = meta['locust_start_unix']
        ftdc_path = _find_ftdc_data(name)
        if ftdc_path is None:
            print(f'  WARNING: no FTDC data found for {name}, skipping.', file=sys.stderr)
            continue
        print(f'  Reading {name} from {ftdc_path} ...', file=sys.stderr)
        all_data[name] = _collect_ftdc_timeseries(ftdc_path, t0, max_hours=ftdc_hours)

    if not all_data:
        print('No FTDC data collected — skipping comparison_ftdc.png.', file=sys.stderr)
        return

    top_metrics = _select_top_ftdc_metrics(all_data, n=3)
    if not top_metrics:
        print('No common FTDC metrics found across experiments.', file=sys.stderr)
        return

    n_plots = len(top_metrics)
    fig, axes = plt.subplots(n_plots, 1, figsize=(16, 4 * n_plots), sharex=True)
    if n_plots == 1:
        axes = [axes]
    fig.suptitle(
        f'WiredTiger cache metrics — per-second, first {ftdc_hours}h, normalised to experiment start\n'
        '(red dashed = delete fires at t=1h; solid vertical = delete complete)',
        fontsize=11,
        y=0.995)

    max_h = 0
    for ax, metric in zip(axes, top_metrics):
        is_counter = _is_counter_metric(metric)
        label = _metric_label(metric)

        for (name, meta), color in zip(experiments, _PLOT_COLORS):
            if name not in all_data or metric not in all_data[name]:
                continue
            t0 = meta['locust_start_unix']
            ts_raw = all_data[name][metric]['ts']
            val_raw = all_data[name][metric]['val']

            if is_counter:
                ts_list, val_list = _rates_from_timeseries(ts_raw, val_raw)
            else:
                ts_list, val_list = ts_raw, val_raw

            if not ts_list:
                continue

            xs_h = [(ts - t0) / 3600.0 for ts in ts_list]
            max_h = max(max_h, max(xs_h))

            _, delete_end = _parse_delete_log(name)
            if delete_end is not None:
                completion_h = (delete_end - t0) / 3600.0
                xd = [x for x, v in zip(xs_h, val_list) if x <= completion_h]
                xa = [x for x, v in zip(xs_h, val_list) if x > completion_h]
                yd = [v for x, v in zip(xs_h, val_list) if x <= completion_h]
                ya = [v for x, v in zip(xs_h, val_list) if x > completion_h]
                ax.plot(xd, yd, label=name, color=color, linewidth=1.2, alpha=0.85)
                if xa:
                    ax.plot(xa, ya, color=color, linewidth=0.8, alpha=0.35, linestyle=':',
                            label='_nolegend_')
                ax.axvline(x=completion_h, color=color, linestyle='-', linewidth=1.1,
                           alpha=0.6, label='_nolegend_')
            else:
                ax.plot(xs_h, val_list, label=name, color=color, linewidth=1.2, alpha=0.85)

        ax.axvline(x=1.0, color='red', linestyle='--', linewidth=1.2, alpha=0.8,
                   label='delete starts (t=1h)' if ax is axes[0] else '_nolegend_')
        ax.set_ylabel(label, fontsize=9)
        ax.set_yscale('log')
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f'{v:.0f}'))
        ax.grid(axis='both', alpha=0.25, which='both')
        if ax is axes[0]:
            ax.legend(loc='upper right', fontsize=8.5)

    axes[-1].set_xlabel('Hours since experiment start', fontsize=10)
    axes[-1].set_xlim(left=0, right=max_h)
    axes[-1].xaxis.set_major_locator(ticker.MultipleLocator(6))
    axes[-1].xaxis.set_minor_locator(ticker.MultipleLocator(3))

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f'Saved {output_path}')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('experiments', nargs='+',
                    help='One or more experiment directory names')
    ap.add_argument('--compare-locust-latencies', action='store_true',
                    help='Generate comparison_locust.png: per-15-min max P50/P90/P99 over time')
    ap.add_argument('--compare-ftdc', action='store_true',
                    help='Generate comparison_ftdc.png: top-3 WiredTiger cache metrics over time')
    ap.add_argument('--ftdc-hours', type=float, default=24,
                    help='Hours of FTDC data to collect per experiment (default: 24)')
    ap.add_argument('--csv', metavar='PATH',
                    help='Override CSV path for the first experiment (single-run mode)')
    ap.add_argument('--locust-start-unix', type=float,
                    help='Override locust start timestamp (single-run mode)')
    ap.add_argument('--delete-start-unix', type=float,
                    help='Override delete start timestamp (single-run mode)')
    ap.add_argument('--output-html', metavar='FILE',
                    help='Write HTML report (single-run mode only)')
    args = ap.parse_args()

    # --- Comparison modes (N experiments) ---
    if args.compare_locust_latencies or args.compare_ftdc:
        exp_list = []
        for name in args.experiments:
            _, _, meta = _load_metadata(name)
            exp_list.append((name, meta))

        if args.compare_locust_latencies:
            _plot_locust_latencies(exp_list)
        if args.compare_ftdc:
            _plot_ftdc_comparison(exp_list, ftdc_hours=args.ftdc_hours)
        return

    # --- Single-run summary mode ---
    name = args.experiments[0]
    if len(args.experiments) > 1:
        print(f'Note: multiple experiments given but no --compare-* flag; '
              f'analysing only {name}.', file=sys.stderr)

    locust_start, delete_start, meta = _load_metadata(name, args.locust_start_unix,
                                                      args.delete_start_unix)
    csv_path = _find_csv(name, args.csv)
    print(f'Analyzing {name} from {csv_path} ...', file=sys.stderr)
    data = parse_history(csv_path, locust_start, delete_start)
    stats = parse_stats(_find_stats_csv(name))

    print_single_run(name, data, meta, stats)

    if args.output_html:
        write_html(args.output_html, f'Locust Analysis — {name}', f'Action: {meta.get("action","")}',
                   [(name, data, meta)])


if __name__ == '__main__':
    main()
