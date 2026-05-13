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
#
# Output:
#   Written to the current working directory:
#     locust_latency.pdf       — all three percentiles as separate pages (PDF only)
#     locust_latency_p50.<fmt> — one file per percentile (non-PDF formats)
#
# Example:
#   python3 locust_workload_plot.py \
#       DeleteMany1TB-String-1000-Users \
#       FastBulkDelete1TB-String-1000-Users \
#       --bucket-minutes 15 --format pdf

import argparse
import os
import sys

import matplotlib.pyplot as plt
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

    return df


def bucket_data(df, bucket_minutes):
    df = df.copy()
    df['bucket'] = (df['elapsed_min'] // bucket_minutes) * bucket_minutes
    return df.groupby('bucket')[['50%', '90%', '99%']].max().reset_index()


def main():
    parser = argparse.ArgumentParser(
        description='Plot Locust P50/P90/P99 latency across experiments')
    parser.add_argument('experiments', nargs='+', metavar='ExperimentDir',
                        help='One or more experiment directory paths')
    parser.add_argument('--request-name', default='Aggregated',
                        help='Value of the Name column to plot (default: Aggregated)')
    parser.add_argument('--bucket-minutes', type=int, default=15,
                        help='Bucket width in minutes, max aggregation (default: 15)')
    parser.add_argument('--format', choices=['pdf', 'png', 'jpg'], default='pdf', dest='fmt',
                        help='Output image format (default: pdf)')
    args = parser.parse_args()

    experiments = {}
    for exp_dir in args.experiments:
        name = os.path.basename(exp_dir.rstrip('/\\'))
        print(f'Loading {name} ...')
        df = load_experiment(exp_dir, args.request_name)
        experiments[name] = bucket_data(df, args.bucket_minutes)

    metrics = [('50%', 'P50'), ('90%', 'P90'), ('99%', 'P99')]
    colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

    def _make_figure(col, label):
        fig, ax = plt.subplots(figsize=(16, 6))
        for i, (exp_name, data) in enumerate(experiments.items()):
            ax.plot(data['bucket'] / 60, data[col], label=exp_name, color=colors[i % len(colors)],
                    linewidth=1.5, marker='o', markersize=3)
        ax.set_xlabel('Elapsed time (hours)', fontsize=12)
        ax.set_ylabel(f'{label} Latency (ms)', fontsize=12)
        ax.set_title(
            f'{label} Request Latency — {args.request_name}\n'
            f'({args.bucket_minutes}-min buckets, max within bucket)', fontsize=13)
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.set_xlim(left=0)

        # Linear 0–250 ms, logarithmic above; linscale=1 gives the linear region
        # the same visual height as one log decade.
        linear_threshold = 250
        ax.set_yscale('symlog', linthresh=linear_threshold, linscale=1)
        ax.set_ylim(bottom=0)
        ax.axhline(linear_threshold, color='gray', linestyle=':', linewidth=0.8, alpha=0.6)

        # Ensure linear_threshold appears as a labeled tick regardless of auto placement
        fig.canvas.draw()
        yticks = sorted(set(t for t in ax.get_yticks() if t >= 0) | {float(linear_threshold)})
        ax.yaxis.set_major_locator(FixedLocator(yticks))
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x):,}'))

        max_elapsed_h = max(d['bucket'].max() for d in experiments.values()) / 60
        bucket_h = args.bucket_minutes / 60
        major_interval_h = max(bucket_h, round(max_elapsed_h / 10 / bucket_h + 0.5) * bucket_h)
        ax.xaxis.set_major_locator(plt.MultipleLocator(major_interval_h))
        ax.xaxis.set_minor_locator(plt.MultipleLocator(bucket_h))
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:g}h'))
        fig.tight_layout()
        return fig

    if args.fmt == 'pdf':
        out_path = 'locust_latency.pdf'
        with PdfPages(out_path) as pdf:
            for col, label in metrics:
                fig = _make_figure(col, label)
                pdf.savefig(fig, dpi=150)
                plt.close(fig)
        print(f'Saved: {out_path}')
    else:
        for col, label in metrics:
            fig = _make_figure(col, label)
            out_path = f'locust_latency_{label.lower()}.{args.fmt}'
            fig.savefig(out_path, dpi=150)
            plt.close(fig)
            print(f'Saved: {out_path}')


if __name__ == '__main__':
    main()
