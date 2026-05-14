#!/usr/bin/env python3
#
# Plots P50/P90/P99 latency from one or more Locust experiment directories,
# overlaying all experiments on a shared elapsed-time x-axis.
#
# Usage:
#   locust_workload_plot.py <ExperimentDir> [<ExperimentDir> ...] [options]
#
# Options:
#   --request-name NAME    Name column value to filter on (default: Aggregated)
#   --bucket-minutes N     Bucket width in minutes, max aggregation (default: 15)
#   --format pdf|png|jpg   Output image format (default: pdf)
#   --ftdc SUBSTRINGS      Comma-separated substrings to match FTDC metric names.
#                          Requires $FTDC_TOOL to point to the llm-ftdc-analysis directory.
#                          Counter metrics (opcounters etc.) are automatically converted
#                          to per-second rates. Each matched metric gets its own chart page.
#
# Output:
#   Written to the current working directory:
#     locust_latency.pdf       — all charts as separate pages (PDF only)
#     locust_latency_p50.<fmt> — one file per percentile (non-PDF formats)
#
# Example:
#   python3 locust_workload_plot.py ExperimentName --bucket-minutes 15 --format pdf --ftdc "wiredTiger"

import argparse
import glob
import os
import re
import sys
from datetime import datetime, timezone

import matplotlib.pyplot as plt
from scipy.stats import gaussian_kde
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


_LOG_TS_RE = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+) \w+ (START|END)')
_LOG_FILES = ['locust_results_delete_many.log', 'locust_results_fast_bulk_delete.log']


def parse_delete_log(exp_dir):
    """Return (start_unix, end_unix) from the non-empty delete log, or None if not found."""
    for name in _LOG_FILES:
        path = os.path.join(exp_dir, 'logs', 'locust', name)
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            continue
        start_ts = end_ts = None
        with open(path) as f:
            for line in f:
                m = _LOG_TS_RE.match(line)
                if not m:
                    continue
                ts = datetime.strptime(
                    m.group(1), '%Y-%m-%d %H:%M:%S,%f').replace(tzinfo=timezone.utc).timestamp()
                if m.group(2) == 'START':
                    start_ts = ts
                elif m.group(2) == 'END':
                    end_ts = ts
        if start_ts is not None:
            return start_ts, end_ts
    return None


def bucket_ftdc_metric(ts, values, bucket_minutes):
    if len(ts) == 0:
        return np.array([]), np.array([])
    df = pd.DataFrame({'bucket': (ts // bucket_minutes) * bucket_minutes, 'value': values})
    grouped = df.groupby('bucket')['value'].max().reset_index()
    return grouped['bucket'].to_numpy(), grouped['value'].to_numpy()


def main():
    parser = argparse.ArgumentParser(
        description='Plot Locust P50/P90/P99 latency across experiments')
    parser.add_argument('experiments', nargs='+', metavar='ExperimentDir',
                        help='One or more experiment directory paths')
    parser.add_argument('--request-name', default='Aggregated',
                        help='Value of the Name column to plot (default: Aggregated)')
    parser.add_argument('--bucket-minutes', type=int, default=15,
                        help='Bucket width in minutes, max aggregation (default: 15)')
    parser.add_argument('--format', choices=['pdf', 'png', 'jpg', 'csv'], default='pdf', dest='fmt',
                        help='Output format (default: pdf)')
    parser.add_argument(
        '--ftdc', default=None, metavar='SUBSTRINGS',
        help='Comma-separated substrings to match FTDC metric names '
        '(requires $FTDC_TOOL). Each matched metric gets its own chart.')
    parser.add_argument('--ftdc-workers', type=int, default=4, metavar='N',
                        help='Parallel workers for FTDC decoding (default: 4)')
    parser.add_argument('--histogram', action='store_true',
                        help='Plot latency distributions grouped by phase instead of time series')
    args = parser.parse_args()

    if args.histogram and args.ftdc:
        parser.error('--histogram and --ftdc are mutually exclusive')

    ftdc_substrings = [s.strip() for s in args.ftdc.split(',')] if args.ftdc else []
    ftdc_mod = _import_ftdc_tools() if ftdc_substrings and not args.histogram else None

    experiments = {}
    experiments_raw = {}
    experiment_t0s = {}
    for exp_dir in args.experiments:
        name = os.path.basename(exp_dir.rstrip('/\\'))
        print(f'Loading {name} ...')
        df, t0 = load_experiment(exp_dir, args.request_name)
        experiments_raw[name] = df
        experiments[name] = bucket_data(df, args.bucket_minutes)
        experiment_t0s[name] = t0

    # Load FTDC data per experiment: {exp_name: {metric_key: {...}}}
    ftdc_by_exp = {}
    if ftdc_substrings and not args.histogram:
        for exp_dir in args.experiments:
            name = os.path.basename(exp_dir.rstrip('/\\'))
            print(f'Loading FTDC for {name} ...')
            try:
                ftdc_by_exp[name] = load_ftdc_data(exp_dir, experiment_t0s[name], ftdc_substrings,
                                                   ftdc_mod, args.ftdc_workers)
            except FileNotFoundError as e:
                print(f'  Warning: {e}', file=sys.stderr)
                ftdc_by_exp[name] = {}

    # Parse delete start/end times from operation logs
    experiment_delete_times = {}
    for exp_dir in args.experiments:
        name = os.path.basename(exp_dir.rstrip('/\\'))
        result = parse_delete_log(exp_dir)
        if result:
            start_unix, end_unix = result
            t0 = experiment_t0s[name]
            start_h = (start_unix - t0) / 3600.0
            end_h = (end_unix - t0) / 3600.0 if end_unix is not None else None
            experiment_delete_times[name] = (start_h, end_h)
        else:
            experiment_delete_times[name] = None

    # Collect all FTDC metric keys seen across any experiment
    all_ftdc_keys = sorted({k for exp_data in ftdc_by_exp.values() for k in exp_data})
    if ftdc_substrings and not all_ftdc_keys:
        print('Warning: --ftdc matched no metrics in any experiment', file=sys.stderr)

    metrics = [('50%', 'P50'), ('90%', 'P90'), ('99%', 'P99')]
    colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

    def _x_axis(ax):
        max_elapsed_h = max(d['bucket'].max() for d in experiments.values()) / 60
        bucket_h = args.bucket_minutes / 60
        major_h = max(bucket_h, round(max_elapsed_h / 10 / bucket_h + 0.5) * bucket_h)
        ax.set_xlim(left=0)
        ax.xaxis.set_major_locator(plt.MultipleLocator(major_h))
        ax.xaxis.set_minor_locator(plt.MultipleLocator(bucket_h))
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:g}h'))
        ax.set_xlabel('Elapsed time (hours)', fontsize=12)

    def _make_latency_figure(col, label):
        fig, ax = plt.subplots(figsize=(16, 6))
        for i, (exp_name, data) in enumerate(experiments.items()):
            ax.plot(data['bucket'] / 60, data[col], label=exp_name, color=colors[i % len(colors)],
                    linewidth=1.5, marker='o', markersize=3)
        ax.set_ylabel(f'{label} Latency (ms)', fontsize=12)
        ax.set_title(
            f'{label} Request Latency — {args.request_name}\n'
            f'({args.bucket_minutes}-min buckets, max within bucket)', fontsize=13)
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)

        linear_threshold = 250
        ax.set_yscale('symlog', linthresh=linear_threshold, linscale=1)
        ax.set_ylim(bottom=0)
        ax.axhline(linear_threshold, color='gray', linestyle=':', linewidth=0.8, alpha=0.6)

        fig.canvas.draw()
        yticks = sorted(set(t for t in ax.get_yticks() if t >= 0) | {float(linear_threshold)})
        ax.yaxis.set_major_locator(FixedLocator(yticks))
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x):,}'))

        max_elapsed_h = max(d['bucket'].max() for d in experiments.values()) / 60
        # Shift each experiment's lines by ~3 px so overlapping lines remain visible
        px_in_data_h = max_elapsed_h / (16 * 150)  # 1 px in data units (16" fig, 150 dpi)
        for i, exp_name in enumerate(experiments):
            times = experiment_delete_times.get(exp_name)
            if not times:
                continue
            color = colors[i % len(colors)]
            shift = i * 3 * px_in_data_h
            start_h, end_h = times
            ax.axvline(start_h + shift, color=color, linestyle='--', linewidth=1.2, alpha=0.8)
            if end_h is not None:
                ax.axvline(end_h + shift, color=color, linestyle=':', linewidth=1.2, alpha=0.8)

        _x_axis(ax)
        fig.tight_layout()
        return fig

    def _make_ftdc_figure(metric_key):
        is_rate = any(ftdc_by_exp[n][metric_key]['is_rate'] for n in ftdc_by_exp
                      if metric_key in ftdc_by_exp[n])
        fig, ax = plt.subplots(figsize=(16, 6))
        for i, exp_name in enumerate(experiments):
            exp_data = ftdc_by_exp.get(exp_name, {})
            if metric_key not in exp_data:
                continue
            entry = exp_data[metric_key]
            bx, by = bucket_ftdc_metric(entry['ts'], entry['values'], args.bucket_minutes)
            if len(bx) == 0:
                continue
            ax.plot(bx / 60, by, label=exp_name, color=colors[i % len(colors)], linewidth=1.5,
                    marker='o', markersize=3)

        ylabel = f'{metric_key}\n(per-second rate)' if is_rate else metric_key
        ax.set_ylabel(ylabel, fontsize=10)
        ax.set_title(
            f'FTDC: {metric_key}\n'
            f'({args.bucket_minutes}-min buckets, max within bucket)', fontsize=13)
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.set_ylim(bottom=0)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:,.0f}'))

        _x_axis(ax)
        fig.tight_layout()
        return fig

    def _make_histogram_figure():
        percentiles = [('50%', 'P50'), ('90%', 'P90'), ('99%', 'P99')]

        # Build (subset_df, exp_name, phase) groups, skipping empty subsets
        groups = []
        for exp_name, df_raw in experiments_raw.items():
            times = experiment_delete_times.get(exp_name)
            if times is None:
                groups.append((df_raw, exp_name, None))
            else:
                start_min = times[0] * 60
                end_min = times[1] * 60 if times[1] is not None else None
                m0 = df_raw['elapsed_min'] < start_min
                m1 = (~m0) if end_min is None else ((df_raw['elapsed_min'] >= start_min) &
                                                    (df_raw['elapsed_min'] <= end_min))
                candidates = [(df_raw[m0], 0), (df_raw[m1], 1)]
                if end_min is not None:
                    candidates.append((df_raw[df_raw['elapsed_min'] > end_min], 2))
                for subset, phase in candidates:
                    if not subset.empty:
                        groups.append((subset, exp_name, phase))

        x_threshold = 500
        shared_x = {}
        for col, _ in percentiles:
            all_vals = pd.concat([g[0][col].dropna() for g in groups])
            if all_vals.empty:
                shared_x[col] = np.linspace(0, 1, 500)
                continue
            lo, hi = max(0.0, float(all_vals.min())), float(all_vals.max())
            x_lin = np.linspace(lo, min(x_threshold, hi), 300)
            x_log = np.logspace(np.log10(x_threshold), np.log10(hi),
                                200) if hi > x_threshold else np.array([])
            shared_x[col] = np.unique(np.concatenate([x_lin, x_log]))

        # One color per experiment, consistent across all subplots
        exp_colors = {name: colors[i % len(colors)] for i, name in enumerate(experiments_raw)}

        all_phases = sorted({phase
                             for _, _, phase in groups}, key=lambda p: 999 if p is None else p)
        phase_label = lambda p: 'All data' if p is None else f'Phase {p}'

        # One subplot per row: P50/Phase0, P50/Phase1, ..., P90/Phase0, ...
        subplot_order = [(col, pct_label, phase) for col, pct_label in percentiles
                         for phase in all_phases]
        n_subplots = len(subplot_order)
        fig, axes = plt.subplots(n_subplots, 1, figsize=(16, 5 * n_subplots))
        axes = np.array(axes).reshape(n_subplots)

        y_threshold = 500
        for ax, (col, pct_label, phase) in zip(axes, subplot_order):
            for subset, exp_name, p in groups:
                if p != phase:
                    continue
                vals = subset[col].dropna()
                if vals.nunique() < 2:
                    continue
                kde = gaussian_kde(vals)
                ax.plot(shared_x[col],
                        kde(shared_x[col]) * len(vals), color=exp_colors[exp_name], linewidth=1.0,
                        label=exp_name)
            ax.set_title(f'{pct_label} — {phase_label(phase)}', fontsize=12)
            ax.set_xlabel('Latency (ms)', fontsize=11)
            ax.set_ylabel('Samples per ms', fontsize=11)
            ax.grid(True, alpha=0.3)
            ax.set_xscale('symlog', linthresh=x_threshold, linscale=1)
            ax.set_xlim(left=0)
            ax.axvline(x_threshold, color='gray', linestyle=':', linewidth=0.8, alpha=0.6)
            ax.set_yscale('symlog', linthresh=y_threshold, linscale=1)
            ax.set_ylim(bottom=0)
            ax.axhline(y_threshold, color='gray', linestyle=':', linewidth=0.8, alpha=0.6)
            ax.legend(fontsize=8)

        fig.canvas.draw()
        for ax in axes:
            xticks = sorted(set(t for t in ax.get_xticks() if t >= 0) | {float(x_threshold)})
            ax.xaxis.set_major_locator(FixedLocator(xticks))
            ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x):,}'))
            yticks = sorted(set(t for t in ax.get_yticks() if t >= 0) | {float(y_threshold)})
            ax.yaxis.set_major_locator(FixedLocator(yticks))
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'{int(y):,}'))

        fig.suptitle(f'Latency Histograms by Phase — {args.request_name}', fontsize=13)
        fig.tight_layout()
        return fig

    def _build_csv_dataframe():
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

        # Phase columns: 0 = before delete, 1 = during delete, 2 = after delete
        phase_cols = {}
        for exp_name in experiments:
            times = experiment_delete_times.get(exp_name)
            if times is None:
                phase_cols[f'Phase / {exp_name}'] = pd.Series(np.nan, index=df.index)
            else:
                start_h, end_h = times

                def _phase(h, s=start_h, e=end_h):
                    if h < s:
                        return 0
                    if e is not None and h > e:
                        return 2
                    return 1

                phase_cols[f'Phase / {exp_name}'] = pd.Series([_phase(h) for h in df.index],
                                                              index=df.index, dtype='Int8')
        df = pd.concat([pd.DataFrame(phase_cols, index=df.index), df], axis=1)
        return df

    if args.fmt == 'csv':
        df = _build_csv_dataframe()
        out_path = 'locust_latency.csv'
        df.to_csv(out_path)
        print(f'Saved: {out_path} ({len(df)} rows, {len(df.columns)} data columns)')
        return

    if args.histogram:
        fig = _make_histogram_figure()
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

    if args.fmt == 'pdf':
        out_path = 'locust_latency.pdf'
        with PdfPages(out_path) as pdf:
            for col, label in metrics:
                fig = _make_latency_figure(col, label)
                pdf.savefig(fig, dpi=150)
                plt.close(fig)
            for metric_key in all_ftdc_keys:
                fig = _make_ftdc_figure(metric_key)
                pdf.savefig(fig, dpi=150)
                plt.close(fig)
        print(f'Saved: {out_path}')
    else:
        for col, label in metrics:
            fig = _make_latency_figure(col, label)
            out_path = f'locust_latency_{label.lower()}.{args.fmt}'
            fig.savefig(out_path, dpi=150)
            plt.close(fig)
            print(f'Saved: {out_path}')
        for i, metric_key in enumerate(all_ftdc_keys):
            fig = _make_ftdc_figure(metric_key)
            safe_name = metric_key.replace(' ', '_').replace('/', '_')[-60:]
            out_path = f'locust_ftdc_{i:02d}_{safe_name}.{args.fmt}'
            fig.savefig(out_path, dpi=150)
            plt.close(fig)
            print(f'Saved: {out_path}')


if __name__ == '__main__':
    main()
