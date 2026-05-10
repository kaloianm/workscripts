#!/usr/bin/env python3
#
help_string = '''
Reads MongoDB log lines from stdin and plots the value of a given numeric key
against the log timestamp, with a linear trend line overlaid.

Example:
    grep 'FastBulkDelete' mongod.log | ./plot_log_trend.py rate
'''

import argparse
import json
import re
import sys
from datetime import datetime

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np

TIMESTAMP_RE = re.compile(r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?[+-]\d{2}:?\d{2})')
JSON_RE = re.compile(r'\{.*\}\s*$')


def parse_line(line, keyword):
    ts_match = TIMESTAMP_RE.match(line)
    if not ts_match:
        return None
    json_match = JSON_RE.search(line)
    if not json_match:
        return None
    try:
        payload = json.loads(json_match.group(0))
    except json.JSONDecodeError:
        return None
    if keyword not in payload:
        return None
    value = payload[keyword]
    if not isinstance(value, (int, float)):
        return None
    return datetime.fromisoformat(ts_match.group(1)), float(value)


def main():
    parser = argparse.ArgumentParser(description=help_string,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('keyword', help='JSON key whose numeric value will be plotted')
    args = parser.parse_args()

    points = [p for p in (parse_line(l, args.keyword) for l in sys.stdin) if p is not None]
    if not points:
        sys.exit(f"No log lines with a numeric '{args.keyword}' key were found on stdin")

    times, values = zip(*points)
    times_num = mdates.date2num(times)
    values = np.array(values)

    plt.figure(figsize=(12, 6))
    plt.plot(values, marker='.', linestyle='-', color='tab:blue', label=args.keyword)

    # Linear trend line (dotted)
    if len(values) > 5:
        x = np.arange(len(values))
        z = np.polyfit(x, values, 1)
        p = np.poly1d(z)
        plt.plot(x, p(x), 'r--', linewidth=1.5, label=f'Trend (slope: {z[0]:.1f})')

    plt.title(f'{args.keyword} over time ({len(points)} samples)')
    plt.ylabel(args.keyword)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()

    plt.savefig(args.keyword)


if __name__ == '__main__':
    main()
