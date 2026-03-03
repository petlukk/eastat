#!/usr/bin/env python3
"""Benchmark: per-phase timing breakdown for eastat vs pandas/polars."""
import csv
import os
import random
import sys
import tempfile
import time

import numpy as np


def generate_csv(path, n_rows=1_000_000, n_cols=5):
    """Generate test CSV with mix of plain and scientific notation."""
    random.seed(42)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([f"col_{i}" for i in range(n_cols)])
        for _ in range(n_rows):
            row = []
            for _ in range(n_cols):
                style = random.randint(0, 3)
                if style == 0:
                    row.append(f"{random.uniform(-1000, 1000):.4f}")
                elif style == 1:
                    row.append(str(random.randint(-10000, 10000)))
                elif style == 2:
                    row.append(f"{random.uniform(-1, 1):.6e}")
                else:
                    row.append(f"{random.uniform(-1e5, 1e5):.3E}")
            w.writerow(row)
    return os.path.getsize(path)


def bench_eastat(path):
    """Run eastat with per-phase timing breakdown."""
    from eastat import process
    from eastat import _csv_stats as csv_stats

    # Warm up shared library loading
    process(path)

    # Timed run
    results, headers, n_rows, col_count, timings = process(path)

    # Second pass: measure reduction vs percentile time separately
    import mmap as mmap_mod
    fd = os.open(str(path), os.O_RDONLY)
    try:
        mm = mmap_mod.mmap(fd, 0, access=mmap_mod.ACCESS_READ)
    finally:
        os.close(fd)
    text = np.frombuffer(mm, dtype=np.uint8)

    from eastat import _csv_scan as csv_scan
    from eastat import _csv_layout as csv_layout
    from eastat import _csv_parse as csv_parse

    n = len(text)
    delim_byte = ord(",")
    counts = np.zeros(3, dtype=np.int32)
    delim_pos = np.empty(n // 4 + 256, dtype=np.int32)
    lf_pos = np.empty(n // 20 + 256, dtype=np.int32)
    sample = bytes(text[:min(4096, n)])
    use_quoted = b'"' in sample
    if use_quoted:
        csv_scan.scan_positions_quoted(text, delim_byte, delim_pos, lf_pos, counts)
    else:
        csv_scan.scan_positions_fast(text, delim_byte, delim_pos, lf_pos, counts)
    n_delims, n_lfs = int(counts[0]), int(counts[1])
    header_dc = int(counts[2])
    delim_pos = delim_pos[:n_delims]
    lf_pos = lf_pos[:n_lfs]

    col_count_b = header_dc + 1
    first_nl = int(lf_pos[0]) if n_lfs > 0 else n
    header_end = int(lf_pos[0]) if n_lfs > 0 else -1

    row_starts = np.empty(n_lfs + 2, dtype=np.int32)
    row_ends = np.empty(n_lfs + 2, dtype=np.int32)
    n_rows_out = np.zeros(1, dtype=np.int32)
    csv_layout.build_row_arrays(lf_pos, n_lfs, header_end, n, row_starts, row_ends, n_rows_out)
    data_rows = int(n_rows_out[0])
    row_starts = row_starts[:data_rows]
    row_ends = row_ends[:data_rows]

    data_delim_pos = delim_pos[header_dc:]
    n_data_delims = len(data_delim_pos)
    delims_per_row = np.zeros(data_rows, dtype=np.int32)
    row_delim_offset = np.zeros(data_rows, dtype=np.int32)
    if n_data_delims > 0:
        csv_layout.build_row_delim_index(
            data_delim_pos, n_data_delims, row_ends, data_rows,
            delims_per_row, row_delim_offset,
        )

    fs_buf = np.empty(data_rows, dtype=np.int32)
    fe_buf = np.empty(data_rows, dtype=np.int32)
    val_buf = np.empty(data_rows, dtype=np.float32)
    cnt_buf = np.zeros(1, dtype=np.int32)

    t_parse_total = 0.0
    t_reduction_total = 0.0
    t_percentile_total = 0.0
    for ci in range(col_count_b):
        csv_layout.compute_field_bounds(
            ci, col_count_b, data_rows,
            row_starts, row_ends, data_delim_pos,
            row_delim_offset, delims_per_row, fs_buf, fe_buf,
        )
        cnt_buf[0] = 0
        t = time.perf_counter()
        csv_parse.batch_atof(text, fs_buf, fe_buf, val_buf, cnt_buf)
        t_parse_total += time.perf_counter() - t
        count = int(cnt_buf[0])

        if count >= data_rows * 0.5 and count >= 16:
            values = val_buf[:count]
            out_sum = np.zeros(1, dtype=np.float32)
            out_min = np.zeros(1, dtype=np.float32)
            out_max = np.zeros(1, dtype=np.float32)
            out_sumsq = np.zeros(1, dtype=np.float32)
            t = time.perf_counter()
            csv_stats.f32_column_stats(values, out_sum, out_min, out_max, out_sumsq)
            t_reduction_total += time.perf_counter() - t

            t = time.perf_counter()
            np.percentile(values, [25, 50, 75])
            t_percentile_total += time.perf_counter() - t

    try:
        mm.close()
    except BufferError:
        pass

    return {
        'n_rows': n_rows,
        'col_count': col_count,
        'mmap': timings['mmap'],
        'scan': timings['scan'],
        'layout': timings['layout'],
        'parse': t_parse_total,
        'stats_reduction': t_reduction_total,
        'stats_percentiles': t_percentile_total,
        'total': timings['total'],
    }


def bench_pandas(path):
    import pandas as pd
    t = time.perf_counter()
    df = pd.read_csv(path)
    df.describe()
    return time.perf_counter() - t


def bench_polars(path):
    import polars as pl
    t = time.perf_counter()
    df = pl.read_csv(path)
    df.describe()
    return time.perf_counter() - t


def main():
    n_rows = 1_000_000
    n_cols = 5
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        csv_path = f.name
    try:
        file_size = generate_csv(csv_path, n_rows, n_cols)
        size_mb = file_size / (1024 * 1024)
        print(f"File: {csv_path} ({n_rows:,} rows x {n_cols} cols, {size_mb:.1f} MB)")
        print()

        ea = bench_eastat(csv_path)
        print(f"eastat breakdown ({ea['col_count']} numeric columns):")
        print(f"  mmap           :  {ea['mmap']*1000:7.1f} ms")
        print(f"  scan (Ea SIMD) :  {ea['scan']*1000:7.1f} ms  - structural scan")
        print(f"  layout (Ea)    :  {ea['layout']*1000:7.1f} ms  - row/column indexing")
        print(f"  parse (Ea)     :  {ea['parse']*1000:7.1f} ms  - batch_atof with sci notation")
        print(f"  stats (Ea f32) :  {ea['stats_reduction']*1000:7.1f} ms  - SIMD sum/min/max/sumsq")
        print(f"  percentiles    :  {ea['stats_percentiles']*1000:7.1f} ms  - np.percentile (f64)")
        print(f"  total          :  {ea['total']*1000:7.1f} ms")
        print()

        try:
            t_pandas = bench_pandas(csv_path)
            print(f"pandas (read_csv + describe): {t_pandas*1000:7.1f} ms")
        except ImportError:
            t_pandas = None
            print("pandas: not installed")

        try:
            t_polars = bench_polars(csv_path)
            print(f"polars (read_csv + describe): {t_polars*1000:7.1f} ms")
        except ImportError:
            t_polars = None
            print("polars: not installed")

        print()
        if t_pandas is not None:
            print(f"Speedup: eastat / pandas = {t_pandas / ea['total']:.1f}x")
        if t_polars is not None:
            print(f"Speedup: eastat / polars = {t_polars / ea['total']:.1f}x")
    finally:
        os.unlink(csv_path)


if __name__ == "__main__":
    main()
