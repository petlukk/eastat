# eastat

CSV column statistics powered by [Ea](https://github.com/petlukk/eacompute) SIMD kernels.

Computes count, mean, stddev, min, max, p25, p50, p75 for every numeric column. String columns get length statistics.

## Install

```bash
pip install eastat
```

Pre-built wheels include compiled SIMD kernels for Linux x86_64 and aarch64. No compiler needed.

## Usage

```bash
eastat data.csv
eastat --json data.csv
eastat -d '\t' data.tsv
eastat -c 0,2,4 data.csv
eastat --no-quotes data.csv   # force fast scan (skip quote detection)
eastat --quoted data.csv      # force quote-aware scan
```

Or from Python:

```python
from eastat import process

results, headers, n_rows, col_count, timings = process("data.csv")
```

## How it works

Four Ea kernels form a zero-copy pipeline over a memory-mapped file:

| Kernel | What it does |
|--------|-------------|
| `csv_scan` | AVX2 structural scanner — finds delimiter and newline positions using `u8x32` comparison + `movemask`. Two modes: fast (no quotes) and quote-aware. Includes `count_positions_quoted` for two-pass large-file strategy. |
| `csv_layout` | Builds row boundary arrays and per-row delimiter index via merge-scan. O(n_delims + n_rows). |
| `csv_parse` | Batch ASCII-to-float parser with whitespace/quote trimming. Field length stats for string columns. |
| `csv_stats` | `f32x8` dual-accumulator FMA reduction for sum, min, max, sum-of-squares in one pass. SIMD binary-search percentiles (p25/p50/p75). |

## Scan modes

eastat auto-detects whether the CSV contains quoted fields by sampling the first 4 KB:

- **Fast scan** — no quote handling. SIMD chunk-skip via `movemask`. Best throughput.
- **Quoted scan** — tracks quote state to ignore delimiters/newlines inside quoted fields.

For large files (>128 MB), a two-pass strategy avoids over-allocation: `count_positions_quoted` counts positions first, then exact-sized buffers are allocated for the SIMD scan pass.

Override with `--no-quotes` or `--quoted`.

## Building from source

Only needed if there's no pre-built wheel for your platform.

```bash
# Install the Ea compiler
# See https://github.com/petlukk/eacompute/releases

# Compile kernels
EA_BIN=./ea ./build_kernels.sh

# Install
pip install -e .
```

## Requirements

- Python 3.9+
- NumPy
