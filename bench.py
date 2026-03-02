#!/usr/bin/env python3
"""
Honest benchmark: Eastat (Ea kernels) vs pandas vs polars.

In-process timing with phase breakdowns showing where each tool spends time.
All tools compute equivalent statistics: count, mean, std, min, max, 25%, 50%, 75%.

Usage:
    python bench.py [test_file.csv]
    python bench.py --precision test_file.csv  # detailed precision comparison
"""

import argparse
import math
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent


# ---------------------------------------------------------------------------
# Eastat benchmark
# ---------------------------------------------------------------------------

def bench_eastat(filepath, force_no_quotes=False, force_quoted=False):
    """Benchmark eastat in-process with phase breakdown."""
    from eastat import process

    # Warmup
    process(filepath, force_no_quotes=force_no_quotes, force_quoted=force_quoted)

    # Timed run
    results, headers, n_rows, col_count, timings = process(
        filepath, force_no_quotes=force_no_quotes, force_quoted=force_quoted
    )

    return timings, results


# ---------------------------------------------------------------------------
# Pandas benchmark
# ---------------------------------------------------------------------------

def bench_pandas(filepath):
    """Benchmark pandas with phase breakdown. Returns (timings, describe_df) or None."""
    try:
        import pandas as pd
    except ImportError:
        print("  pandas not installed, skipping")
        return None

    # Warmup
    _ = pd.read_csv(filepath).describe()

    # Phase 1: read_csv
    t0 = time.perf_counter()
    df = pd.read_csv(filepath)
    t_read = time.perf_counter() - t0

    # Phase 2: .describe() full (count, mean, std, min, 25%, 50%, 75%, max)
    t0 = time.perf_counter()
    desc = df.describe()
    t_describe = time.perf_counter() - t0

    timings = {
        'read_csv': t_read,
        'describe': t_describe,
        'total': t_read + t_describe,
    }
    return timings, desc


# ---------------------------------------------------------------------------
# Polars benchmark
# ---------------------------------------------------------------------------

def bench_polars(filepath):
    """Benchmark polars with phase breakdown. Returns (timings, describe_df) or None."""
    try:
        import polars as pl
    except ImportError:
        print("  polars not installed, skipping")
        return None

    # Warmup
    _ = pl.read_csv(filepath).describe()

    # Phase 1: read_csv
    t0 = time.perf_counter()
    df = pl.read_csv(filepath)
    t_read = time.perf_counter() - t0

    # Phase 2: .describe()
    t0 = time.perf_counter()
    _ = df.describe()
    t_describe = time.perf_counter() - t0

    timings = {
        'read_csv': t_read,
        'describe': t_describe,
        'total': t_read + t_describe,
    }
    return timings, None


# ---------------------------------------------------------------------------
# Precision comparison (f32 vs f64)
# ---------------------------------------------------------------------------

def compare_precision(ea_results, pd_desc):
    """Compare eastat f32 results against pandas f64 results."""
    print("\n=== Precision Comparison (eastat f32 vs pandas f64) ===\n")

    stat_map = {
        'mean': 'mean', 'stddev': 'std',
        'min': 'min', 'max': 'max',
        'p25': '25%', 'p50': '50%', 'p75': '75%',
    }

    max_rel_err = 0.0
    max_rel_col = ''
    max_rel_stat = ''

    pd_cols = list(pd_desc.columns)

    for r in ea_results:
        if r['type'] not in ('integer', 'float'):
            continue

        col_name = r['name']
        if col_name not in pd_cols:
            continue

        print(f"  Column: {col_name}")
        print(f"    {'Stat':<8} {'eastat (f32)':>16} {'pandas (f64)':>16} {'rel err':>12}")
        print(f"    {'---'*8} {'---'*5} {'---'*5} {'---'*4}")

        for ea_key, pd_key in stat_map.items():
            ea_val = r.get(ea_key)
            if ea_val is None:
                continue

            try:
                pd_val = float(pd_desc.loc[pd_key, col_name])
            except (KeyError, ValueError):
                continue

            if pd_val == 0:
                rel_err = abs(ea_val - pd_val)
            else:
                rel_err = abs(ea_val - pd_val) / abs(pd_val)

            marker = ''
            if rel_err > 1e-3:
                marker = ' <-- drift'
            elif rel_err > 1e-5:
                marker = ' *'

            if rel_err > max_rel_err:
                max_rel_err = rel_err
                max_rel_col = col_name
                max_rel_stat = ea_key

            print(f"    {ea_key:<8} {ea_val:>16.6g} {pd_val:>16.6g} {rel_err:>12.2e}{marker}")

        print()

    print(f"  Max relative error: {max_rel_err:.2e} ({max_rel_stat} on \"{max_rel_col}\")")
    if max_rel_err > 1e-3:
        print(f"  WARNING: >0.1% divergence detected. f32 precision may be insufficient")
        print(f"  for this data distribution (large values or high variance).")
    elif max_rel_err > 1e-5:
        print(f"  Note: minor f32 rounding visible but within typical tolerance.")
    else:
        print(f"  f32 and f64 results agree to ~6 significant figures.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def find_test_file():
    """Find a test CSV file."""
    candidates = [
        SCRIPT_DIR / "test_1000000.csv",
        SCRIPT_DIR / "test_100000.csv",
        SCRIPT_DIR / "test_10k.csv",
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


def run_single_benchmark(filepath, show_precision=False):
    """Run benchmark on a single file."""
    file_size = filepath.stat().st_size
    size_mb = file_size / (1024**2)
    print(f"\nFile: {filepath.name} ({size_mb:.1f} MB)")
    print("=" * 72)

    # --- Eastat (fast scan) ---
    print("\n--- eastat (SIMD fast scan, no quotes) ---")
    ea_fast_timings, ea_results = bench_eastat(filepath, force_no_quotes=True)
    print(f"  scan:   {ea_fast_timings.get('scan', 0)*1000:7.1f} ms  (mode: {ea_fast_timings.get('scan_mode', '?')})")
    print(f"  layout: {ea_fast_timings.get('layout', 0)*1000:7.1f} ms")
    print(f"  stats:  {ea_fast_timings.get('stats', 0)*1000:7.1f} ms")
    ea_total = ea_fast_timings.get('total', 0)
    print(f"  total:  {ea_total*1000:7.1f} ms")

    throughput_mbs = size_mb / ea_total if ea_total > 0 else 0
    print(f"  throughput: {throughput_mbs:7.0f} MB/s")

    # --- Eastat (quoted scan) ---
    print("\n--- eastat (SIMD quoted scan) ---")
    ea_quoted_timings, _ = bench_eastat(filepath, force_quoted=True)
    print(f"  scan:   {ea_quoted_timings.get('scan', 0)*1000:7.1f} ms  (mode: {ea_quoted_timings.get('scan_mode', '?')})")
    print(f"  layout: {ea_quoted_timings.get('layout', 0)*1000:7.1f} ms")
    print(f"  stats:  {ea_quoted_timings.get('stats', 0)*1000:7.1f} ms")
    print(f"  total:  {ea_quoted_timings.get('total', 0)*1000:7.1f} ms")

    # --- Pandas ---
    pd_result = bench_pandas(filepath)
    pd_timings = None
    pd_desc = None

    if pd_result:
        pd_timings, pd_desc = pd_result
        print(f"\n--- pandas ---")
        print(f"  read_csv:  {pd_timings['read_csv']*1000:7.1f} ms")
        print(f"  .describe: {pd_timings['describe']*1000:7.1f} ms")
        print(f"  total:     {pd_timings['total']*1000:7.1f} ms")

    # --- Polars ---
    pl_result = bench_polars(filepath)
    pl_timings = None

    if pl_result:
        pl_timings, _ = pl_result
        print(f"\n--- polars ---")
        print(f"  read_csv:  {pl_timings['read_csv']*1000:7.1f} ms")
        print(f"  .describe: {pl_timings['describe']*1000:7.1f} ms")
        print(f"  total:     {pl_timings['total']*1000:7.1f} ms")

    # --- Comparison ---
    print("\n" + "-" * 72)

    if ea_total > 0:
        print(f"\nEquivalent work (count/mean/std/min/25%/50%/75%/max):")
        if pd_timings:
            ratio = pd_timings['total'] / ea_total
            print(f"  eastat vs pandas:  {ratio:.1f}x {'faster' if ratio > 1 else 'slower'}")
        if pl_timings:
            ratio = pl_timings['total'] / ea_total
            print(f"  eastat vs polars:  {ratio:.1f}x {'faster' if ratio > 1 else 'slower'}")

    scan_fast = ea_fast_timings.get('scan', 0)
    scan_quoted = ea_quoted_timings.get('scan', 0)
    if scan_fast > 0 and scan_quoted > 0:
        print(f"\nScan comparison:")
        print(f"  fast scan:   {scan_fast*1000:7.1f} ms")
        print(f"  quoted scan: {scan_quoted*1000:7.1f} ms")
        print(f"  ratio:       {scan_quoted/scan_fast:.2f}x slower (quote tracking overhead)")

    print(f"\nNotes: eastat uses f32 SIMD reductions; pandas/polars use f64.")

    # --- Precision comparison ---
    if show_precision and pd_desc is not None:
        compare_precision(ea_results, pd_desc)


def main():
    parser = argparse.ArgumentParser(
        description='Benchmark eastat vs pandas vs polars',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('file', nargs='?', help='CSV file to benchmark')
    parser.add_argument('--precision', action='store_true',
                        help='Show detailed f32 vs f64 precision comparison')

    args = parser.parse_args()

    if args.file:
        filepath = Path(args.file)
    else:
        filepath = find_test_file()
        if filepath is None:
            print("No test file found. Run generate_test.py first.")
            print("  python generate_test.py --rows=1000000")
            sys.exit(1)

    run_single_benchmark(filepath, show_precision=args.precision)


if __name__ == '__main__':
    main()
