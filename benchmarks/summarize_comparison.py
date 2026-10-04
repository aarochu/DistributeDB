"""Summarize benchmarks/compare_revisions.sh output as a Markdown table.

Usage: python3 benchmarks/summarize_comparison.py BASE.csv HEAD.csv [BASE_LABEL HEAD_LABEL]

Each cell is the median over trials for one GET/SET mix. "Change" is head
relative to base; a negative change in a latency column is an improvement.
"""

import csv
import statistics
import sys

METRICS = [
    ("ops_per_second", "ops/s", 1.0),
    ("p50_latency_ns", "client p50 µs", 1000.0),
    ("p99_latency_ns", "client p99 µs", 1000.0),
    ("server_read_p99_us", "server read p99 µs", 1.0),
    ("server_read_lock_wait_p99_us", "read-lock wait p99 µs", 1.0),
    ("server_write_lock_hold_p99_us", "write-lock hold p99 µs", 1.0),
    ("replica_catch_up_ms", "replica catch-up ms", 1.0),
    ("server_peak_rss_kib", "server peak RSS MiB", 1024.0),
    ("data_bytes", "data on disk MiB", 1024.0 * 1024.0),
]


def load(path):
    with open(path, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    failed = [row for row in rows if row["failed_ops"] != "0" or row["skipped_ops"] != "0"]
    if failed:
        sys.exit(f"{path}: {len(failed)} runs had failed or skipped operations")
    by_mix = {}
    for row in rows:
        by_mix.setdefault(row["read_ratio"], []).append(row)
    return rows, by_mix


def median(rows, column, scale):
    values = [float(row[column]) / scale for row in rows if row.get(column)]
    return statistics.median(values) if values else None


def cell(value):
    return "n/a" if value is None else f"{value:,.0f}"


def main():
    base_rows, base = load(sys.argv[1])
    head_rows, head = load(sys.argv[2])
    if len(sys.argv) >= 5:
        base_label, head_label = sys.argv[3], sys.argv[4]
    else:
        base_label = "base `" + base_rows[0]["revision"].strip('"')[:7] + "`"
        head_label = "head `" + head_rows[0]["revision"].strip('"')[:7] + "`"
    print(f"{base_label} vs {head_label}, median of "
          f"{len(base_rows) // max(len(base), 1)} trials per mix.\n")
    print(f"| GET/SET | Metric | {base_label} | {head_label} | Change |")
    print("|---|---|---:|---:|---:|")
    for mix in sorted(base, key=float, reverse=True):
        reads = round(float(mix) * 100)
        for column, label, scale in METRICS:
            before = median(base[mix], column, scale)
            after = median(head.get(mix, []), column, scale)
            if before is None and after is None:
                continue
            change = "n/a"
            if before and after is not None:
                change = f"{(after - before) / before * 100:+.0f}%"
            print(f"| {reads}/{100 - reads} | {label} | {cell(before)} | {cell(after)} | {change} |")


if __name__ == "__main__":
    main()
