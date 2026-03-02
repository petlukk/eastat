"""Low-level ctypes bindings for the csv_scan kernel."""
import ctypes as _ct

import numpy as _np

from ._loader import load_library

_lib = load_library("csv_scan")

_lib.scan_positions_fast.argtypes = [_ct.POINTER(_ct.c_uint8), _ct.c_int32, _ct.c_uint8, _ct.POINTER(_ct.c_int32), _ct.POINTER(_ct.c_int32), _ct.POINTER(_ct.c_int32)]
_lib.scan_positions_fast.restype = None

_lib.scan_positions_quoted.argtypes = [_ct.POINTER(_ct.c_uint8), _ct.c_int32, _ct.c_uint8, _ct.POINTER(_ct.c_int32), _ct.POINTER(_ct.c_int32), _ct.POINTER(_ct.c_int32)]
_lib.scan_positions_quoted.restype = None

_lib.count_positions_quoted.argtypes = [_ct.POINTER(_ct.c_uint8), _ct.c_int32, _ct.c_uint8, _ct.POINTER(_ct.c_int32)]
_lib.count_positions_quoted.restype = None


def scan_positions_fast(text, delim, out_delim_pos, out_lf_pos, out_counts):
    _lib.scan_positions_fast(
        text.ctypes.data_as(_ct.POINTER(_ct.c_uint8)),
        _ct.c_int32(text.size),
        _ct.c_uint8(int(delim)),
        out_delim_pos.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        out_lf_pos.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        out_counts.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
    )


def scan_positions_quoted(text, delim, out_delim_pos, out_lf_pos, out_counts):
    _lib.scan_positions_quoted(
        text.ctypes.data_as(_ct.POINTER(_ct.c_uint8)),
        _ct.c_int32(text.size),
        _ct.c_uint8(int(delim)),
        out_delim_pos.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        out_lf_pos.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        out_counts.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
    )


def count_positions_quoted(text, delim, out_counts):
    _lib.count_positions_quoted(
        text.ctypes.data_as(_ct.POINTER(_ct.c_uint8)),
        _ct.c_int32(text.size),
        _ct.c_uint8(int(delim)),
        out_counts.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
    )
