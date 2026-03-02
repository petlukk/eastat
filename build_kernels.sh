#!/usr/bin/env bash
set -euo pipefail

# Resolve EA binary path (allows EA_BIN env var)
_ea_input="${EA_BIN:-ea}"
if [[ "$_ea_input" == */* ]]; then
    EA="$(cd "$(dirname "$_ea_input")" && pwd)/$(basename "$_ea_input")"
else
    EA="$_ea_input"
fi
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LIB_DIR="$SCRIPT_DIR/src/eastat/lib"
KERNEL_DIR="$SCRIPT_DIR/kernels"

# Detect platform (.so vs .dll, with/without lib prefix)
case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*|Windows_NT)
        EXT=".dll"
        PREFIX=""
        ;;
    *)
        EXT=".so"
        PREFIX="lib"
        ;;
esac

mkdir -p "$LIB_DIR"

for kernel in csv_scan csv_layout csv_parse csv_stats; do
    OUTNAME="${PREFIX}${kernel}${EXT}"
    echo "Compiling ${kernel}.ea -> ${OUTNAME}"
    (cd "$LIB_DIR" && "$EA" "$KERNEL_DIR/${kernel}.ea" --lib -o "$OUTNAME")
done

rm -f "$LIB_DIR"/*.o
echo "Done. Libraries in $LIB_DIR:"
ls -la "$LIB_DIR"/${PREFIX}*${EXT}
