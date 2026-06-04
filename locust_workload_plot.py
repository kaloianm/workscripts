#!/usr/bin/env python3
#
# Plots P50/P90/P99 latency from one or more Locust experiment directories,
# overlaying all experiments on a shared elapsed-time x-axis.
#
# Usage:
#   locust_workload_plot.py compare <ExperimentDir> [<ExperimentDir> ...] [options]
#   locust_workload_plot.py histogram <ExperimentDir> [<ExperimentDir> ...] [options]
#
# Common options (both modes):
#   --request-name NAME    Name column value to filter on (default: Aggregated)
#   --format pdf|png|jpg   Output image format (default: pdf)
#
# compare options:
#   --bucket-minutes N     Bucket width in minutes, max aggregation (default: 15)
#   --ftdc SUBSTRINGS      Comma-separated substrings to match FTDC metric names.
#                          Requires $FTDC_TOOL to point to the llm-ftdc-analysis directory.
#                          Counter metrics (opcounters etc.) are automatically converted
#                          to per-second rates. Each matched metric gets its own chart page.
#
# Output:
#   Written to the current working directory:
#     locust_latency.csv           — bucketed source data for all time-series charts
#     locust_latency.pdf           — all charts as separate pages (PDF only)
#     locust_latency_p50.<fmt>     — one file per percentile (non-PDF formats)
#
#     locust_latency_histogram.csv — pre-computed histogram bins for histogram charts
#     locust_latency_histogram.pdf — histogram charts (--histogram mode)
#
# Examples:
#   python3 locust_workload_plot.py compare Exp1 Exp2 --bucket-minutes 15 --format pdf --ftdc "wiredTiger"
#   python3 locust_workload_plot.py histogram Exp1 Exp2 --format pdf

import argparse
import glob
import os
import re
import sys
from datetime import datetime, timezone

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.ticker import FixedLocator


def load_experiment(exp_dir, request_name):
    csv_path = os.path.join(exp_dir, 'logs', 'locust', 'locust_results_stats_history.csv')
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(f'CSV not found: {csv_path}')

    df = pd.read_csv(csv_path)

    df = df[df['Name'] == request_name].copy()
    if df.empty:
        raise ValueError(f'No rows with Name={request_name!r} in {csv_path}')

    for col in ['50%', '90%', '99%']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=['50%', '90%', '99%', 'Timestamp'])
    df = df[df['50%'] > 0]  # skip pre-ramp rows where latency is 0

    t0 = df['Timestamp'].min()
    df['elapsed_min'] = (df['Timestamp'] - t0) / 60.0

    return df, t0


def bucket_data(df, bucket_minutes):
    df = df.copy()
    df['bucket'] = (df['elapsed_min'] // bucket_minutes) * bucket_minutes
    return df.groupby('bucket')[['50%', '90%', '99%']].max().reset_index()


def _import_ftdc_tools():
    ftdc_tool = os.environ.get('FTDC_TOOL')
    if not ftdc_tool:
        raise EnvironmentError(
            '$FTDC_TOOL is not set — point it to the llm-ftdc-analysis directory')
    if not os.path.isdir(ftdc_tool):
        raise EnvironmentError(f'$FTDC_TOOL={ftdc_tool!r} is not a directory')
    if ftdc_tool not in sys.path:
        sys.path.insert(0, ftdc_tool)
    import ftdc_compare_fast as ftdc_mod
    return ftdc_mod


def _find_ftdc_path(exp_dir):
    matches = glob.glob(os.path.join(exp_dir, 'logs', 'mongod-*', 'diagnostic.data'))
    if not matches:
        raise FileNotFoundError(f'No diagnostic.data found under {exp_dir}/logs/mongod-*/')
    return matches[0]


def load_ftdc_data(exp_dir, t0_unix, substrings, ftdc_mod, max_workers=4):
    """Load FTDC metrics whose dotted key contains any of the substrings.

    Counter metrics are automatically converted to per-second rates.

    Returns:
        dict: {metric_key_str: {'ts': elapsed_min_array, 'values': values_array,
                                'is_rate': bool}}
    """
    ftdc_path = _find_ftdc_path(exp_dir)
    print(f'  Reading FTDC from {ftdc_path} ...')
    metrics_dict, _, _ = ftdc_mod.collect_metrics(ftdc_path, max_workers=max_workers)

    # Locate the timestamp series (Unix ms)
    ts_key = next((k for k in metrics_dict if k.endswith('.start') or k == 'start'), None)
    if ts_key is None:
        raise ValueError(f'No timestamp (.start) key found in FTDC data at {ftdc_path}')
    ts_ms = np.asarray(metrics_dict[ts_key], dtype=np.float64)
    elapsed_min = (ts_ms / 1000.0 - t0_unix) / 60.0

    result = {}
    lc_substrings = [s.lower() for s in substrings]
    for key, values in metrics_dict.items():
        if key == ts_key:
            continue
        if not any(s in key.lower() for s in lc_substrings):
            continue

        arr = np.asarray(values, dtype=np.float64)
        is_rate = ftdc_mod.is_likely_counter(key, arr)
        if is_rate:
            arr = ftdc_mod.compute_rates(arr)
            ts = elapsed_min[1:]  # rate aligns to the later of the two samples
        else:
            ts = elapsed_min

        # Trim to matching length and drop samples before T0
        n = min(len(ts), len(arr))
        ts, arr = ts[:n], arr[:n]
        mask = ts >= 0
        result[key] = {'ts': ts[mask], 'values': arr[mask], 'is_rate': is_rate}

    return result


_PHASES_LOG_RE = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+) \w+ (START|END) (.+)')
_LEGACY_LOG_RE = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+) \w+ (START|END)')
_LEGACY_LOG_FILES = {
    'locust_results_delete_many.log': 'delete_many',
    'locust_results_fast_bulk_delete.log': 'fastBulkDelete',
}


def parse_phases_log(exp_dir):
    """Return list of (phase_name, start_unix, end_unix_or_None) sorted by start time.
    Reads locust_results_phases.log if present; falls back to legacy per-operation logs."""
    phases_path = os.path.join(exp_dir, 'logs', 'locust', 'locust_results_phases.log')
    if os.path.isfile(phases_path) and os.path.getsize(phases_path) > 0:
        return _parse_new_phases_log(phases_path)
    return _parse_legacy_phases_log(exp_dir)


def _parse_new_phases_log(path):
    open_starts = {}
    result = []
    with open(path) as f:
        for line in f:
            m = _PHASES_LOG_RE.match(line)
            if not m:
                continue
            ts = datetime.strptime(m.group(1),
                                   '%Y-%m-%d %H:%M:%S,%f').replace(tzinfo=timezone.utc).timestamp()
            event, name = m.group(2), m.group(3).strip()
            if event == 'START':
                open_starts[name] = ts
            elif event == 'END' and name in open_starts:
                result.append((name, open_starts.pop(name), ts))
    for name, start_ts in open_starts.items():
        result.append((name, start_ts, None))
    result.sort(key=lambda x: x[1])
    return result


def _parse_legacy_phases_log(exp_dir):
    for filename, phase_name in _LEGACY_LOG_FILES.items():
        path = os.path.join(exp_dir, 'logs', 'locust', filename)
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            continue
        start_ts = end_ts = None
        with open(path) as f:
            for line in f:
                m = _LEGACY_LOG_RE.match(line)
                if not m:
                    continue
                ts = datetime.strptime(
                    m.group(1), '%Y-%m-%d %H:%M:%S,%f').replace(tzinfo=timezone.utc).timestamp()
                if m.group(2) == 'START':
                    start_ts = ts
                elif m.group(2) == 'END':
                    end_ts = ts
        if start_ts is not None:
            return [(phase_name, start_ts, end_ts)]
    return []


def bucket_ftdc_metric(ts, values, bucket_minutes):
    if len(ts) == 0:
        return np.array([]), np.array([])
    df = pd.DataFrame({'bucket': (ts // bucket_minutes) * bucket_minutes, 'value': values})
    grouped = df.groupby('bucket')['value'].max().reset_index()
    return grouped['bucket'].to_numpy(), grouped['value'].to_numpy()


def main():
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('experiments', nargs='+', metavar='ExperimentDir',
                        help='One or more experiment directory paths')
    common.add_argument('--request-name', default='Aggregated',
                        help='Value of the Name column to plot (default: Aggregated)')
    common.add_argument('--format', choices=['pdf', 'png', 'jpg'], default='pdf', dest='fmt',
                        help='Output format (default: pdf)')

    parser = argparse.ArgumentParser(
        description='Plot Locust P50/P90/P99 latency across experiments')
    subparsers = parser.add_subparsers(dest='mode', required=True)

    compare_p = subparsers.add_parser('compare', parents=[common],
                                      help='Plot P50/P90/P99 latency time series')
    compare_p.add_argument('--bucket-minutes', type=int, default=15,
                           help='Bucket width in minutes, max aggregation (default: 15)')
    compare_p.add_argument(
        '--ftdc', default=None, metavar='SUBSTRINGS',
        help='Comma-separated substrings to match FTDC metric names '
        '(requires $FTDC_TOOL). Each matched metric gets its own chart.')
    compare_p.add_argument('--ftdc-workers', type=int, default=4, metavar='N',
                           help='Parallel workers for FTDC decoding (default: 4)')

    hist_p = subparsers.add_parser('histogram', parents=[common],
                                   help='Plot latency distributions grouped by phase')
    hist_p.add_argument('--warmup', metavar='DIR', default=None,
                        help='Experiment dir whose data is overlaid on all subplots as "baseline"')

    args = parser.parse_args()
    is_histogram = args.mode == 'histogram'

    ftdc_substrings = []
    ftdc_mod = None
    if not is_histogram:
        ftdc_substrings = [s.strip() for s in args.ftdc.split(',')] if args.ftdc else []
        ftdc_mod = _import_ftdc_tools() if ftdc_substrings else None

    warmup_dir = getattr(args, 'warmup', None)
    warmup_dir_norm = os.path.normpath(warmup_dir) if warmup_dir else None

    exp_dirs = list(args.experiments)
    if warmup_dir_norm and not any(os.path.normpath(d) == warmup_dir_norm for d in exp_dirs):
        exp_dirs.insert(0, warmup_dir)

    experiments = {}
    experiments_raw = {}
    experiment_t0s = {}
    for exp_dir in exp_dirs:
        name = os.path.basename(exp_dir.rstrip('/\\'))
        print(f'Loading {name} ...')
        df, t0 = load_experiment(exp_dir, args.request_name)
        experiments_raw[name] = df
        if not is_histogram:
            experiments[name] = bucket_data(df, args.bucket_minutes)
        experiment_t0s[name] = t0

    warmup_raw = None
    if warmup_dir:
        wname = os.path.basename(warmup_dir.rstrip('/\\'))
        print(f'Loading warmup baseline from {wname} ...')
        warmup_raw, warmup_t0 = load_experiment(warmup_dir, args.request_name)
        warmup_phases = parse_phases_log(warmup_dir)
        target_phase = next((p for p in warmup_phases if p[0] == 'WarmUp'), None)
        if target_phase is None:
            target_phase = next((p for p in warmup_phases if p[0] == 'Normal run'), None)
        if target_phase is not None:
            pname_w, s_w, e_w = target_phase
            s_min = (s_w - warmup_t0) / 60.0
            e_min = (e_w - warmup_t0) / 60.0 if e_w is not None else None
            mask = warmup_raw['elapsed_min'] >= s_min
            if e_min is not None:
                mask &= warmup_raw['elapsed_min'] <= e_min
            warmup_raw = warmup_raw[mask]
            if warmup_raw.empty:
                print(f'  Warning: warmup phase "{pname_w}" yielded no data rows', file=sys.stderr)

    # Load FTDC data per experiment: {exp_name: {metric_key: {...}}}
    ftdc_by_exp = {}
    if ftdc_substrings:
        for exp_dir in args.experiments:
            name = os.path.basename(exp_dir.rstrip('/\\'))
            print(f'Loading FTDC for {name} ...')
            try:
                ftdc_by_exp[name] = load_ftdc_data(exp_dir, experiment_t0s[name], ftdc_substrings,
                                                   ftdc_mod, args.ftdc_workers)
            except FileNotFoundError as e:
                print(f'  Warning: {e}', file=sys.stderr)
                ftdc_by_exp[name] = {}

    # Parse phase start/end times from operation logs
    experiment_phases = {}
    for exp_dir in args.experiments:
        name = os.path.basename(exp_dir.rstrip('/\\'))
        t0 = experiment_t0s[name]
        experiment_phases[name] = [(pname, (s - t0) / 3600.0,
                                    (e - t0) / 3600.0 if e is not None else None)
                                   for pname, s, e in parse_phases_log(exp_dir)]

    # Collect all FTDC metric keys seen across any experiment
    all_ftdc_keys = sorted({k for exp_data in ftdc_by_exp.values() for k in exp_data})
    if ftdc_substrings and not all_ftdc_keys:
        print('Warning: --ftdc matched no metrics in any experiment', file=sys.stderr)

    metrics = [('50%', 'P50'), ('90%', 'P90'), ('99%', 'P99')]
    colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

    def _x_axis(ax, max_elapsed_h):
        bucket_h = args.bucket_minutes / 60
        major_h = max(bucket_h, round(max_elapsed_h / 10 / bucket_h + 0.5) * bucket_h)
        ax.set_xlim(left=0, right=max_elapsed_h)
        ax.xaxis.set_major_locator(plt.MultipleLocator(major_h))
        ax.xaxis.set_minor_locator(plt.MultipleLocator(bucket_h))
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:g}h'))
        ax.set_xlabel('Elapsed time (hours)', fontsize=12)

    def _build_timeseries_dataframe():
        series = {}
        for col, label in metrics:
            for exp_name, data in experiments.items():
                series[f'{label} / {exp_name}'] = data.set_index('bucket')[col]
        for metric_key in all_ftdc_keys:
            is_rate = any(ftdc_by_exp[n][metric_key]['is_rate'] for n in ftdc_by_exp
                          if metric_key in ftdc_by_exp[n])
            col_label = f'{metric_key} (rate/s)' if is_rate else metric_key
            for exp_name in experiments:
                entry = ftdc_by_exp.get(exp_name, {}).get(metric_key)
                if entry is None:
                    continue
                bx, by = bucket_ftdc_metric(entry['ts'], entry['values'], args.bucket_minutes)
                if len(bx) == 0:
                    continue
                series[f'{col_label} / {exp_name}'] = pd.Series(by, index=bx)
        df = pd.DataFrame(series)
        df.index = df.index / 60
        df.index.name = 'elapsed_hours'

        # Phase columns: string phase name at each time point, '' when outside all phases
        phase_cols = {}
        for exp_name in experiments:
            phases = experiment_phases.get(exp_name, [])

            def _phase_name(h, phases=phases):
                for pname, s, e in phases:
                    if h >= s and (e is None or h <= e):
                        return pname
                return ''

            phase_cols[f'Phase / {exp_name}'] = pd.Series([_phase_name(h) for h in df.index],
                                                          index=df.index, dtype='str')
        df = pd.concat([pd.DataFrame(phase_cols, index=df.index), df], axis=1)
        return df

    def _build_histogram_dataframe(warmup_raw=None):
        """Compute histogram bins for each (percentile, phase, experiment) and return as
        long-format DataFrame with columns [percentile, phase, experiment, bin_left, bin_right,
        count].  phase='all' means no phase info (all data combined).
        """
        percentiles = [('50%', 'P50'), ('99%', 'P99')]

        groups = []
        for exp_name, df_raw in experiments_raw.items():
            phases = experiment_phases.get(exp_name, [])
            if not phases:
                groups.append((df_raw, exp_name, None))
            else:
                phase_subsets: dict[str, list] = {}
                for pname, s, e in phases:
                    if pname == 'WarmUp':
                        continue  # never gets its own subplot
                    if warmup_raw is not None and pname == 'Normal run':
                        continue  # baseline overlay already represents the Normal run data
                    s_min = s * 60
                    e_min = e * 60 if e is not None else None
                    mask = df_raw['elapsed_min'] >= s_min
                    if e_min is not None:
                        mask &= df_raw['elapsed_min'] <= e_min
                    subset = df_raw[mask]
                    if not subset.empty:
                        phase_subsets.setdefault(pname, []).append(subset)
                _delete_phase_suffix = {'RecordStore': '-recordStore', 'Indexes': '-indexes'}
                for pname, subsets in phase_subsets.items():
                    combined = pd.concat(subsets) if len(subsets) > 1 else subsets[0]
                    plot_phase = 'Delete' if pname in _delete_phase_suffix else pname
                    plot_exp = exp_name + _delete_phase_suffix.get(pname, '')
                    groups.append((combined, plot_exp, plot_phase))

        x_threshold = 500
        n_lin, n_log = 50, 30

        rows = []
        for col, pct_label in percentiles:
            for subset, exp_name, phase in groups:
                vals = subset[col].dropna()
                if vals.empty:
                    continue
                lo, hi = max(0.0, float(vals.min())), float(vals.max())
                if lo >= hi:
                    continue
                edges_lin = np.linspace(lo, min(x_threshold, hi), n_lin + 1)
                if hi > x_threshold:
                    edges_log = np.logspace(np.log10(x_threshold), np.log10(hi), n_log + 1)[1:]
                    bin_edges = np.concatenate([edges_lin, edges_log])
                else:
                    bin_edges = edges_lin
                counts, bin_edges = np.histogram(vals, bins=bin_edges)
                n_total = len(vals)
                phase_val = 'all' if phase is None else phase
                for left, right, count in zip(bin_edges[:-1], bin_edges[1:], counts):
                    rows.append({
                        'percentile': pct_label,
                        'phase': phase_val,
                        'experiment': exp_name,
                        'bin_left': left,
                        'bin_right': right,
                        'count': count / n_total,
                    })

        # Inject baseline (warmup) bins into every phase subplot
        if warmup_raw is not None:
            all_phases = sorted(set(g[2] if g[2] is not None else 'all' for g in groups))
            for col, pct_label in percentiles:
                vals = warmup_raw[col].dropna()
                if vals.empty:
                    continue
                lo, hi = max(0.0, float(vals.min())), float(vals.max())
                if lo >= hi:
                    continue
                edges_lin = np.linspace(lo, min(x_threshold, hi), n_lin + 1)
                if hi > x_threshold:
                    edges_log = np.logspace(np.log10(x_threshold), np.log10(hi), n_log + 1)[1:]
                    bin_edges = np.concatenate([edges_lin, edges_log])
                else:
                    bin_edges = edges_lin
                counts, bin_edges = np.histogram(vals, bins=bin_edges)
                n_total = len(vals)
                for phase_val in all_phases:
                    for left, right, count in zip(bin_edges[:-1], bin_edges[1:], counts):
                        rows.append({
                            'percentile': pct_label,
                            'phase': phase_val,
                            'experiment': 'baseline',
                            'bin_left': left,
                            'bin_right': right,
                            'count': count / n_total,
                        })

        return pd.DataFrame(rows,
                            columns=['percentile', 'phase', 'experiment', 'bin_left', 'bin_right',
                                     'count'])

    def _make_latency_figure(label, df, y_max=None):
        exp_names = [c.split(' / ', 1)[1] for c in df.columns if c.startswith(f'{label} /')]

        fig, ax = plt.subplots(figsize=(16, 6))
        for i, exp_name in enumerate(exp_names):
            series = df[f'{label} / {exp_name}'].dropna()
            ax.plot(series.index, series.values, label=exp_name, color=colors[i % len(colors)],
                    linewidth=1.5, marker='o', markersize=3)
        ax.set_ylabel(f'{label} Latency (ms)', fontsize=12)
        ax.set_title(
            f'{label} Request Latency — {args.request_name}\n'
            f'({args.bucket_minutes}-min buckets, max within bucket)', fontsize=13)
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)

        linear_threshold = 250
        ax.set_yscale('symlog', linthresh=linear_threshold, linscale=1)
        ax.set_ylim(bottom=0, top=y_max)
        ax.axhline(linear_threshold, color='gray', linestyle=':', linewidth=0.8, alpha=0.6)

        fig.canvas.draw()
        yticks = sorted(set(t for t in ax.get_yticks() if t >= 0) | {float(linear_threshold)})
        ax.yaxis.set_major_locator(FixedLocator(yticks))
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x):,}'))

        max_elapsed_h = df.index.max()
        end_marker_y = np.linspace(0.04, 0.96, 14)
        for i, exp_name in enumerate(exp_names):
            phases = experiment_phases.get(exp_name, [])
            color = colors[i % len(colors)]
            for _, start_h, end_h in phases:
                ax.axvline(start_h, color=color, linestyle='--', linewidth=1.2, alpha=0.8)
                if end_h is not None:
                    ax.plot([end_h] * len(end_marker_y), end_marker_y, color=color,
                            linestyle='None', marker='+', markersize=5,
                            markeredgewidth=1.2, alpha=0.9,
                            transform=ax.get_xaxis_transform())

        _x_axis(ax, max_elapsed_h)
        fig.tight_layout()
        return fig

    def _make_ftdc_figure(metric_prefix, df):
        is_rate = metric_prefix.endswith(' (rate/s)')
        exp_names = [c.split(' / ', 1)[1] for c in df.columns if c.startswith(f'{metric_prefix} /')]

        fig, ax = plt.subplots(figsize=(16, 6))
        for i, exp_name in enumerate(exp_names):
            series = df[f'{metric_prefix} / {exp_name}'].dropna()
            ax.plot(series.index, series.values, label=exp_name, color=colors[i % len(colors)],
                    linewidth=1.5, marker='o', markersize=3)

        display_key = metric_prefix[:-len(' (rate/s)')] if is_rate else metric_prefix
        ylabel = f'{display_key}\n(per-second rate)' if is_rate else metric_prefix
        ax.set_ylabel(ylabel, fontsize=10)
        ax.set_title(
            f'FTDC: {display_key}\n'
            f'({args.bucket_minutes}-min buckets, max within bucket)', fontsize=13)
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.set_ylim(bottom=0)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:,.0f}'))

        _x_axis(ax, df.index.max())
        fig.tight_layout()
        return fig

    def _make_histogram_figure(df):
        pct_order = ['P50', 'P99']
        phase_order = ['Delete', 'PostRun']
        phases = sorted(df['phase'].unique(),
                        key=lambda p: (phase_order.index(p) if p in phase_order else len(phase_order), p))
        all_exp_names = [n for n in dict.fromkeys(df['experiment']) if n != 'baseline']
        exp_colors = {name: colors[i % len(colors)] for i, name in enumerate(all_exp_names)}

        subplot_order = [(pct, phase) for pct in pct_order for phase in phases]
        n_subplots = len(subplot_order)
        fig, axes = plt.subplots(n_subplots, 1, figsize=(16, 5 * n_subplots))
        axes = np.array(axes).reshape(n_subplots)

        x_threshold = 500

        for ax, (pct, phase) in zip(axes, subplot_order):
            subset = df[(df['percentile'] == pct) & (df['phase'] == phase)]
            baseline_grp = subset[subset['experiment'] == 'baseline']
            main_grp = subset[subset['experiment'] != 'baseline']
            for exp_name, grp in main_grp.groupby('experiment', sort=False):
                edges = np.append(grp['bin_left'].values, grp['bin_right'].values[-1])
                ax.stairs(grp['count'].values, edges,
                          color=exp_colors.get(exp_name, 'gray'),
                          linewidth=1.2, label=exp_name)
            if not baseline_grp.empty:
                edges = np.append(baseline_grp['bin_left'].values,
                                  baseline_grp['bin_right'].values[-1])
                ax.stairs(baseline_grp['count'].values, edges,
                          color='gray', linewidth=1.5, linestyle='--', alpha=0.7,
                          label='baseline')
            phase_label = 'All data' if phase == 'all' else phase
            ax.set_title(f'{pct} — {phase_label}', fontsize=12)
            ax.set_xlabel('Latency (ms)', fontsize=11)
            ax.set_ylabel('Fraction of samples', fontsize=11)
            ax.grid(True, alpha=0.3)
            ax.set_xscale('symlog', linthresh=x_threshold, linscale=1)
            ax.set_xlim(left=0)
            ax.axvline(x_threshold, color='gray', linestyle=':', linewidth=0.8, alpha=0.6)
            ax.set_yscale('log')
            ax.set_ylim(bottom=1e-6)
            ax.legend(fontsize=8)

        fig.canvas.draw()
        global_xmax = max(ax.get_xlim()[1] for ax in axes)
        global_ymax = max(ax.get_ylim()[1] for ax in axes)
        for ax in axes:
            ax.set_xlim(right=global_xmax)
            ax.set_ylim(top=global_ymax)

        fig.canvas.draw()
        for ax in axes:
            xticks = sorted(set(t for t in ax.get_xticks() if t >= 0) | {float(x_threshold)})
            ax.xaxis.set_major_locator(FixedLocator(xticks))
            ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x):,}'))
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'{y:.4g}'))

        fig.tight_layout()
        return fig

    def _print_phase_summary():
        rows = []
        for exp_name, df_raw in experiments_raw.items():
            phases = experiment_phases.get(exp_name, [])
            if not phases:
                rows.append((exp_name, 'all', df_raw))
            else:
                phase_subsets: dict[str, list] = {}
                for pname, s, e in phases:
                    if pname == 'WarmUp':
                        continue
                    if warmup_raw is not None and pname == 'Normal run':
                        continue
                    s_min = s * 60
                    e_min = e * 60 if e is not None else None
                    mask = df_raw['elapsed_min'] >= s_min
                    if e_min is not None:
                        mask &= df_raw['elapsed_min'] <= e_min
                    subset = df_raw[mask]
                    if not subset.empty:
                        phase_subsets.setdefault(pname, []).append(subset)
                _delete_phase_suffix = {'RecordStore': '-recordStore', 'Indexes': '-indexes'}
                for pname, subsets in phase_subsets.items():
                    combined = pd.concat(subsets) if len(subsets) > 1 else subsets[0]
                    plot_phase = 'Delete' if pname in _delete_phase_suffix else pname
                    plot_exp = exp_name + _delete_phase_suffix.get(pname, '')
                    rows.append((plot_exp, plot_phase, combined))
        if warmup_raw is not None:
            rows.append(('baseline', 'WarmUp/Normal', warmup_raw))
        if not rows:
            return
        col_w = max(max(len(r[0]) for r in rows), 3)
        phase_w = max(max(len(r[1]) for r in rows), 5)
        print()
        print(f"{'Run':<{col_w}}  {'Phase':<{phase_w}}  {'P50 (ms)':>10}  {'P99 (ms)':>10}")
        print('-' * (col_w + phase_w + 28))
        for exp_name, phase, subset in rows:
            p50 = subset['50%'].median()
            p99 = subset['99%'].median()
            print(f"{exp_name:<{col_w}}  {phase:<{phase_w}}  {p50:>10.0f}  {p99:>10.0f}")
        print()

    # Step 1: Build CSV (source of truth — exact values that will be plotted)
    if is_histogram:
        df_csv = _build_histogram_dataframe(warmup_raw=warmup_raw)
        csv_path = 'locust_latency_histogram.csv'
        df_csv.to_csv(csv_path, index=False)
        print(f'Saved: {csv_path} ({len(df_csv)} rows)')
        _print_phase_summary()
    else:
        df_csv = _build_timeseries_dataframe()
        csv_path = 'locust_latency.csv'
        df_csv.to_csv(csv_path)
        print(f'Saved: {csv_path} ({len(df_csv)} rows, {len(df_csv.columns)} data columns)')

    # Step 2: Load from CSV and plot
    if is_histogram:
        df_plot = pd.read_csv(csv_path)
        fig = _make_histogram_figure(df_plot)
        if args.fmt == 'pdf':
            out_path = 'locust_latency_histogram.pdf'
            with PdfPages(out_path) as pdf:
                pdf.savefig(fig, dpi=150)
        else:
            out_path = f'locust_latency_histogram.{args.fmt}'
            fig.savefig(out_path, dpi=150)
        plt.close(fig)
        print(f'Saved: {out_path}')
        return

    df_plot = pd.read_csv(csv_path, index_col='elapsed_hours')

    # Identify FTDC metric prefixes from CSV column names (preserving order)
    skip_prefixes = ('Phase /', 'P50 /', 'P90 /', 'P99 /')
    ftdc_prefixes = []
    seen_prefixes = set()
    for col in df_plot.columns:
        if any(col.startswith(p) for p in skip_prefixes):
            continue
        parts = col.rsplit(' / ', 1)
        if len(parts) == 2 and parts[0] not in seen_prefixes:
            ftdc_prefixes.append(parts[0])
            seen_prefixes.add(parts[0])

    lat_cols = [c for c in df_plot.columns if any(c.startswith(f'{lbl} /') for _, lbl in metrics)]
    global_lat_ymax = float(df_plot[lat_cols].max().max()) if lat_cols else None

    if args.fmt == 'pdf':
        out_path = 'locust_latency.pdf'
        with PdfPages(out_path) as pdf:
            for _, label in metrics:
                fig = _make_latency_figure(label, df_plot, y_max=global_lat_ymax)
                pdf.savefig(fig, dpi=150)
                plt.close(fig)
            for metric_prefix in ftdc_prefixes:
                fig = _make_ftdc_figure(metric_prefix, df_plot)
                pdf.savefig(fig, dpi=150)
                plt.close(fig)
        print(f'Saved: {out_path}')
    else:
        for _, label in metrics:
            fig = _make_latency_figure(label, df_plot, y_max=global_lat_ymax)
            out_path = f'locust_latency_{label.lower()}.{args.fmt}'
            fig.savefig(out_path, dpi=150)
            plt.close(fig)
            print(f'Saved: {out_path}')
        for i, metric_prefix in enumerate(ftdc_prefixes):
            fig = _make_ftdc_figure(metric_prefix, df_plot)
            safe_name = metric_prefix.replace(' ', '_').replace('/', '_')[-60:]
            out_path = f'locust_ftdc_{i:02d}_{safe_name}.{args.fmt}'
            fig.savefig(out_path, dpi=150)
            plt.close(fig)
            print(f'Saved: {out_path}')


if __name__ == '__main__':
    main()
