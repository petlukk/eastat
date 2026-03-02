# eastat

CSV column statistics powered by [Ea](https://github.com/petlukk/eacompute) SIMD kernels.

Computes count, mean, stddev, min, max, p25, p50, p75 for every numeric column. String columns get length statistics.

## How it works

Four Ea kernels form a zero-copy pipeline over a memory-mapped file:

| Kernel | What it does |
|--------|-------------|
| `csv_scan` | AVX2 structural scanner — finds all delimiter and newline positions using `u8x32` comparison + `movemask` + scalar bitscan for position extraction. Two modes: fast (no quotes) and quote-aware (SIMD + scalar XOR-carry). Includes `count_positions_quoted` for two-pass large-file strategy. |
| `csv_layout` | Builds row boundary arrays and per-row delimiter index via merge-scan. O(n_delims + n_rows). |
| `csv_parse` | Batch ASCII-to-float parser with whitespace/quote trimming. Field length stats for string columns. |
| `csv_stats` | `f32x8` dual-accumulator FMA reduction for sum, min, max, sum-of-squares in one pass. SIMD binary-search percentiles (p25/p50/p75) via 3 simultaneous searches with `select` + `reduce_add`. |

Auto-generated Python bindings (via `ea bind --python`) — zero manual FFI.

## Quick start

```bash
# Build kernels (requires eacompute)
./build.sh

# Generate test data
python3 generate_test.py --rows=1000000

# Run
python3 eastat.py data.csv
python3 eastat.py --json data.csv
python3 eastat.py -d '\t' data.tsv
python3 eastat.py -c 0,2,4 data.csv
python3 eastat.py --no-quotes data.csv   # force fast scan (skip quote detection)
python3 eastat.py --quoted data.csv      # force quote-aware scan
```

## Benchmark

```
python3 bench.py test_1000000.csv
```

Compares eastat (fast scan + quoted scan) vs pandas vs polars with phase breakdowns.

## Scan modes

eastat auto-detects whether the CSV contains quoted fields by sampling the first 4 KB:

- **Fast scan** — no quote handling. SIMD bitmask extraction via `movemask`. Best throughput.
- **Quoted scan** — tracks quote state via scalar XOR-carry over `movemask` bitmasks. Masks out delimiters/newlines inside quoted fields before position extraction.

For large files (>128 MB), a two-pass strategy avoids over-allocation: `count_positions_quoted` counts positions first, then exact-sized buffers are allocated for the SIMD scan pass.

Override with `--no-quotes` or `--quoted`.

## Stress testing

```bash
python3 generate_test.py --stress --rows=100000
python3 eastat.py stress_100000.csv --quoted
```

Generates adversarial CSVs with BOM, CRLF, quoted fields containing commas and embedded quotes.

## Requirements

- [eacompute](https://github.com/petlukk/eacompute) (the Ea compiler)
- Python 3.8+
- NumPy
