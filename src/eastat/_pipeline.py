"""CSV analysis pipeline — scan, layout, parse, stats."""
import math
import mmap
import os
import sys
import time
from pathlib import Path

import numpy as np

from . import _csv_scan as csv_scan
from . import _csv_layout as csv_layout
from . import _csv_parse as csv_parse
from . import _csv_stats as csv_stats


def process(filepath, delimiter=',', has_header=True, selected_columns=None,
            force_quoted=False, force_no_quotes=False):
    """Full CSV analysis pipeline. Returns (results, headers, n_rows, col_count, timings)."""
    filepath = Path(filepath)
    timings = {}
    t_total = time.perf_counter()

    # 1. Memory-map the file
    t0 = time.perf_counter()
    file_size = filepath.stat().st_size
    if file_size == 0:
        print("Error: empty file", file=sys.stderr)
        sys.exit(1)

    fd = os.open(str(filepath), os.O_RDONLY)
    try:
        mm = mmap.mmap(fd, 0, access=mmap.ACCESS_READ)
    finally:
        os.close(fd)

    text = np.frombuffer(mm, dtype=np.uint8)
    n = len(text)
    timings['mmap'] = time.perf_counter() - t0

    # 2. Structural scan
    t0 = time.perf_counter()
    delim_byte = ord(delimiter)
    counts = np.zeros(3, dtype=np.int32)

    if force_quoted:
        use_quoted = True
    elif force_no_quotes:
        use_quoted = False
    else:
        sample = bytes(text[:min(4096, n)])
        use_quoted = b'"' in sample

    if n < 128 * 1024 * 1024:
        delim_pos = np.empty(n // 4 + 256, dtype=np.int32)
        lf_pos = np.empty(n // 20 + 256, dtype=np.int32)
        if use_quoted:
            csv_scan.scan_positions_quoted(text, delim_byte, delim_pos, lf_pos, counts)
        else:
            csv_scan.scan_positions_fast(text, delim_byte, delim_pos, lf_pos, counts)
        n_delims = int(counts[0])
        n_lfs = int(counts[1])
        delim_pos = delim_pos[:n_delims]
        lf_pos = lf_pos[:n_lfs]
    else:
        csv_scan.count_positions_quoted(text, delim_byte, counts)
        n_delims = int(counts[0])
        n_lfs = int(counts[1])
        delim_pos = np.empty(n_delims, dtype=np.int32)
        lf_pos = np.empty(n_lfs, dtype=np.int32)
        counts[0] = 0
        counts[1] = 0
        counts[2] = 0
        if use_quoted:
            csv_scan.scan_positions_quoted(text, delim_byte, delim_pos, lf_pos, counts)
        else:
            csv_scan.scan_positions_fast(text, delim_byte, delim_pos, lf_pos, counts)

    header_dc = int(counts[2])
    timings['scan'] = time.perf_counter() - t0
    timings['scan_mode'] = 'quoted' if use_quoted else 'fast'

    # 3. Build layout
    t0 = time.perf_counter()
    col_count = header_dc + 1

    if has_header:
        first_nl = int(lf_pos[0]) if n_lfs > 0 else n
        hdr = bytes(text[:first_nl])
        if hdr.startswith(b'\xef\xbb\xbf'):
            hdr = hdr[3:]
        if hdr.endswith(b'\r'):
            hdr = hdr[:-1]
        headers = hdr.decode('utf-8', errors='replace').split(delimiter)
        header_end = int(lf_pos[0]) if n_lfs > 0 else -1
    else:
        headers = [f"col_{i}" for i in range(col_count)]
        header_end = -1

    row_starts = np.empty(n_lfs + 2, dtype=np.int32)
    row_ends = np.empty(n_lfs + 2, dtype=np.int32)
    n_rows_out = np.zeros(1, dtype=np.int32)

    csv_layout.build_row_arrays(lf_pos, n_lfs, header_end, n, row_starts, row_ends, n_rows_out)

    data_rows = int(n_rows_out[0])
    row_starts = row_starts[:data_rows]
    row_ends = row_ends[:data_rows]

    if data_rows == 0:
        try:
            mm.close()
        except BufferError:
            pass
        return [], headers, 0, col_count, timings

    data_delim_pos = delim_pos[header_dc:] if has_header else delim_pos
    n_data_delims = len(data_delim_pos)

    delims_per_row = np.zeros(data_rows, dtype=np.int32)
    row_delim_offset = np.zeros(data_rows, dtype=np.int32)

    if n_data_delims > 0:
        csv_layout.build_row_delim_index(
            data_delim_pos, n_data_delims, row_ends, data_rows,
            delims_per_row, row_delim_offset,
        )

    timings['layout'] = time.perf_counter() - t0

    # 4. Per-column statistics
    t0 = time.perf_counter()
    cols = selected_columns if selected_columns else list(range(col_count))
    results = []

    fs_buf = np.empty(data_rows, dtype=np.int32)
    fe_buf = np.empty(data_rows, dtype=np.int32)
    val_buf = np.empty(data_rows, dtype=np.float32)
    cnt_buf = np.zeros(1, dtype=np.int32)

    for ci in cols:
        if ci >= col_count:
            continue
        col_name = headers[ci] if ci < len(headers) else f"col_{ci}"

        csv_layout.compute_field_bounds(
            ci, col_count, data_rows,
            row_starts, row_ends, data_delim_pos,
            row_delim_offset, delims_per_row, fs_buf, fe_buf,
        )

        cnt_buf[0] = 0
        csv_parse.batch_atof(text, fs_buf, fe_buf, val_buf, cnt_buf)
        count = int(cnt_buf[0])

        if count >= data_rows * 0.5:
            stats = _numeric_stats(val_buf, count, data_rows)
            mn, mx = stats['min'], stats['max']
            if mn == int(mn) and mx == int(mx) and abs(mx) < 1e7:
                col_type = 'integer'
            else:
                col_type = 'float'
            stats['type'] = col_type
        else:
            stats = _string_stats(fs_buf, fe_buf, data_rows)
            stats['type'] = 'string'

        stats['name'] = col_name
        stats['col_index'] = ci
        results.append(stats)

    timings['stats'] = time.perf_counter() - t0
    timings['total'] = time.perf_counter() - t_total

    del text
    try:
        mm.close()
    except BufferError:
        pass
    return results, headers, data_rows, col_count, timings


def _numeric_stats(val_buf, count, total_rows):
    values = val_buf[:count]
    nulls = total_rows - count

    if count == 0:
        return {'rows': total_rows, 'nulls': nulls, 'count': 0,
                'min': None, 'max': None, 'mean': None, 'stddev': None,
                'sum': None, 'p25': None, 'p50': None, 'p75': None}

    if count >= 16:
        out_sum = np.zeros(1, dtype=np.float32)
        out_min = np.zeros(1, dtype=np.float32)
        out_max = np.zeros(1, dtype=np.float32)
        out_sumsq = np.zeros(1, dtype=np.float32)
        csv_stats.f32_column_stats(values, out_sum, out_min, out_max, out_sumsq)
        total_sum = float(out_sum[0])
        total_min = float(out_min[0])
        total_max = float(out_max[0])
        total_sumsq = float(out_sumsq[0])

        out_p25 = np.zeros(1, dtype=np.float32)
        out_p50 = np.zeros(1, dtype=np.float32)
        out_p75 = np.zeros(1, dtype=np.float32)
        csv_stats.f32_percentiles(values, total_min, total_max, out_p25, out_p50, out_p75)
        p25 = float(out_p25[0])
        p50 = float(out_p50[0])
        p75 = float(out_p75[0])
    else:
        arr = values.astype(np.float64)
        total_sum = float(np.sum(arr))
        total_min = float(np.min(arr))
        total_max = float(np.max(arr))
        total_sumsq = float(np.sum(arr ** 2))
        sorted_arr = np.sort(arr)
        p25 = float(np.percentile(sorted_arr, 25))
        p50 = float(np.percentile(sorted_arr, 50))
        p75 = float(np.percentile(sorted_arr, 75))

    mean = total_sum / count
    variance = max(0.0, total_sumsq / count - mean * mean)
    stddev = math.sqrt(variance)

    return {
        'rows': total_rows, 'nulls': nulls, 'count': count,
        'min': total_min, 'max': total_max,
        'mean': mean, 'stddev': stddev, 'sum': total_sum,
        'p25': p25, 'p50': p50, 'p75': p75,
    }


def _string_stats(fs_buf, fe_buf, total_rows):
    out_min = np.zeros(1, dtype=np.int32)
    out_max = np.zeros(1, dtype=np.int32)
    out_total = np.zeros(1, dtype=np.int32)
    out_null = np.zeros(1, dtype=np.int32)
    csv_parse.field_length_stats(fs_buf, fe_buf, out_min, out_max, out_total, out_null)
    null_count = int(out_null[0])
    valid = total_rows - null_count
    total_len = int(out_total[0])
    return {
        'rows': total_rows, 'nulls': null_count, 'count': valid,
        'min_length': int(out_min[0]),
        'max_length': int(out_max[0]),
        'mean_length': total_len / valid if valid > 0 else 0,
    }
