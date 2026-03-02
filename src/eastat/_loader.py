"""Platform detection and shared library loading."""
import ctypes
import platform
import sys
from pathlib import Path

_LIB_DIR = Path(__file__).parent / "lib"


def _lib_extension() -> str:
    if sys.platform == "win32":
        return ".dll"
    return ".so"


def load_library(name: str) -> ctypes.CDLL:
    """Load a shared library by name (e.g. 'csv_scan' -> libcsv_scan.so)."""
    ext = _lib_extension()
    prefix = "" if sys.platform == "win32" else "lib"
    lib_path = _LIB_DIR / f"{prefix}{name}{ext}"

    if not lib_path.exists():
        raise OSError(
            f"eastat kernel library not found: {lib_path}\n"
            "This usually means the package was not built for your platform.\n"
            "Please install a pre-built wheel: pip install eastat"
        )

    return ctypes.CDLL(str(lib_path))
