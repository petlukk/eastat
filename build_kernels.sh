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

# Detect architecture for kernel selection
ARCH="$(uname -m)"

mkdir -p "$LIB_DIR"

for kernel in csv_scan csv_layout csv_parse csv_stats; do
    # Use ARM-specific kernels on aarch64 (NEON: 128-bit, no movemask)
    SRC="${kernel}.ea"
    if [[ "$ARCH" == "aarch64" || "$ARCH" == "arm64" ]]; then
        if [[ -f "$KERNEL_DIR/${kernel}_arm.ea" ]]; then
            SRC="${kernel}_arm.ea"
        fi
    fi

    OUTNAME="${PREFIX}${kernel}${EXT}"
    echo "Compiling ${SRC} -> ${OUTNAME}"
    (cd "$LIB_DIR" && "$EA" "$KERNEL_DIR/${SRC}" --lib -o "$OUTNAME")
done

rm -f "$LIB_DIR"/*.o
echo "Done. Libraries in $LIB_DIR:"
ls -la "$LIB_DIR"/${PREFIX}*${EXT}
