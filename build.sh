#!/bin/bash
# Build Eastat: compile Ea kernels + generate Python bindings via ea bind.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
EA_ROOT="${EA_ROOT:-$SCRIPT_DIR/../eacompute}"
KERNEL_DIR="$SCRIPT_DIR/kernels"

if [ ! -d "$EA_ROOT" ]; then
    echo "ERROR: Ea compiler not found at $EA_ROOT"
    echo "Set EA_ROOT to the eacompute directory."
    exit 1
fi

echo "Building Ea compiler..."
(cd "$EA_ROOT" && cargo build --features=llvm --release --quiet)
EA="$EA_ROOT/target/release/ea"

for kernel in csv_scan csv_layout csv_parse csv_stats; do
    echo "Compiling ${kernel}.ea -> lib${kernel}.so"
    (cd "$SCRIPT_DIR" && "$EA" "$KERNEL_DIR/${kernel}.ea" --lib -o "lib${kernel}.so")

    echo "Generating ${kernel}.py (ea bind --python)"
    (cd "$SCRIPT_DIR" && "$EA" bind "$KERNEL_DIR/${kernel}.ea" --python)
done

# Clean up intermediate object files
rm -f "$SCRIPT_DIR"/*.o

echo ""
echo "Done. Build artifacts:"
ls -lh "$SCRIPT_DIR"/lib*.so "$SCRIPT_DIR"/*.py 2>/dev/null || true
echo ""
echo "Run: python eastat.py <file.csv>"
