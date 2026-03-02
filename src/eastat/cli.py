"""CLI entry point for eastat."""
import argparse
import json
import sys
from pathlib import Path

from ._pipeline import process


def format_number(n):
    if n is None:
        return "N/A"
    if isinstance(n, float):
        if n == int(n) and abs(n) < 1e15:
            return f"{int(n):,}"
        if abs(n) >= 1000:
            return f"{n:,.2f}"
        if abs(n) < 0.01:
            return f"{n:.6g}"
        return f"{n:.4g}"
    return f"{n:,}"


def print_table(results, headers, row_count, col_count, timings, file_size):
    for r in results:
        col_idx = r['col_index']
        col_name = r['name']
        col_type = r['type'].capitalize()

        print(f"\n  {col_idx + 1}. \"{col_name}\" ({col_type})")
        print(f"        Rows: {format_number(r['rows']):>14}    Nulls: {format_number(r['nulls'])}")

        if r['type'] in ('integer', 'float'):
            if r.get('min') is not None:
                print(f"        Min: {format_number(r['min']):>15}    Max: {format_number(r['max'])}")
                print(f"        Mean: {format_number(r['mean']):>14}    StdDev: {format_number(r['stddev'])}")
                print(f"        25%: {format_number(r.get('p25')):>15}    50%: {format_number(r.get('p50'))}")
                print(f"        75%: {format_number(r.get('p75')):>15}")
        else:
            print(f"        Min Length: {format_number(r.get('min_length', 0)):>10}    Max Length: {format_number(r.get('max_length', 0))}")

    elapsed = timings.get('total', 0)
    throughput = file_size / elapsed / (1024**3) if elapsed > 0 else 0
    scan_mode = timings.get('scan_mode', '?')
    print(f"\nScanned {format_number(row_count)} rows x {col_count} columns in {elapsed:.2f}s ({throughput:.2f} GB/s)")
    print(f"  Scan mode: {scan_mode}")

    print(f"\n  Timings:")
    for phase in ['mmap', 'scan', 'layout', 'stats']:
        t = timings.get(phase, 0)
        if t > 0.0001:
            print(f"    {phase:>12}: {t*1000:8.1f} ms")


def print_json(results, headers, row_count, col_count, timings, file_size):
    elapsed = timings.get('total', 0)
    output = {
        'file_size': file_size,
        'row_count': row_count,
        'col_count': col_count,
        'elapsed_seconds': round(elapsed, 4),
        'throughput_gbps': round(file_size / elapsed / (1024**3), 2) if elapsed > 0 else 0,
        'scan_mode': timings.get('scan_mode', '?'),
        'columns': []
    }
    for r in results:
        col = {
            'index': r['col_index'],
            'name': r['name'],
            'type': r['type'],
            'rows': r['rows'],
            'nulls': r['nulls'],
        }
        if r['type'] in ('integer', 'float'):
            col.update({
                'min': r.get('min'), 'max': r.get('max'),
                'mean': r.get('mean'), 'stddev': r.get('stddev'),
                'p25': r.get('p25'), 'p50': r.get('p50'), 'p75': r.get('p75'),
            })
        else:
            col.update({
                'min_length': r.get('min_length', 0),
                'max_length': r.get('max_length', 0),
            })
        output['columns'].append(col)

    print(json.dumps(output, indent=2))


def main():
    parser = argparse.ArgumentParser(
        prog='eastat',
        description='CSV column statistics powered by Ea SIMD kernels',
    )
    parser.add_argument('file', help='CSV file to analyze')
    parser.add_argument('--delimiter', '-d', default=',', help='Field delimiter (default: comma)')
    parser.add_argument('--json', '-j', action='store_true', dest='json_output', help='Output as JSON')
    parser.add_argument('--columns', '-c', default=None, help='Comma-separated column indices (0-based)')
    parser.add_argument('--no-header', action='store_true', help='File has no header row')
    parser.add_argument('--no-quotes', action='store_true', help='Force fast scan (skip quote detection)')
    parser.add_argument('--quoted', action='store_true', help='Force quote-aware scan')
    args = parser.parse_args()

    delimiter = args.delimiter
    if delimiter == '\\t':
        delimiter = '\t'

    filepath = Path(args.file)
    if not filepath.exists():
        print(f"Error: file not found: {filepath}", file=sys.stderr)
        sys.exit(1)

    file_size = filepath.stat().st_size
    selected_columns = None
    if args.columns:
        selected_columns = [int(c.strip()) for c in args.columns.split(',')]

    results, headers, row_count, col_count, timings = process(
        filepath=filepath, delimiter=delimiter,
        has_header=not args.no_header, selected_columns=selected_columns,
        force_quoted=args.quoted, force_no_quotes=args.no_quotes,
    )

    if args.json_output:
        print_json(results, headers, row_count, col_count, timings, file_size)
    else:
        print_table(results, headers, row_count, col_count, timings, file_size)
