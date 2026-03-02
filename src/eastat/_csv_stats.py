"""Low-level ctypes bindings for the csv_stats kernel."""
import ctypes as _ct

from ._loader import load_library

_lib = load_library("csv_stats")

_lib.f32_column_stats.argtypes = [_ct.POINTER(_ct.c_float), _ct.c_int32, _ct.POINTER(_ct.c_float), _ct.POINTER(_ct.c_float), _ct.POINTER(_ct.c_float), _ct.POINTER(_ct.c_float)]
_lib.f32_column_stats.restype = None

_lib.f32_percentiles.argtypes = [_ct.POINTER(_ct.c_float), _ct.c_int32, _ct.c_float, _ct.c_float, _ct.POINTER(_ct.c_float), _ct.POINTER(_ct.c_float), _ct.POINTER(_ct.c_float)]
_lib.f32_percentiles.restype = None


def f32_column_stats(data, out_sum, out_min, out_max, out_sumsq):
    _lib.f32_column_stats(
        data.ctypes.data_as(_ct.POINTER(_ct.c_float)),
        _ct.c_int32(data.size),
        out_sum.ctypes.data_as(_ct.POINTER(_ct.c_float)),
        out_min.ctypes.data_as(_ct.POINTER(_ct.c_float)),
        out_max.ctypes.data_as(_ct.POINTER(_ct.c_float)),
        out_sumsq.ctypes.data_as(_ct.POINTER(_ct.c_float)),
    )


def f32_percentiles(data, min_val, max_val, out_p25, out_p50, out_p75):
    _lib.f32_percentiles(
        data.ctypes.data_as(_ct.POINTER(_ct.c_float)),
        _ct.c_int32(data.size),
        _ct.c_float(float(min_val)),
        _ct.c_float(float(max_val)),
        out_p25.ctypes.data_as(_ct.POINTER(_ct.c_float)),
        out_p50.ctypes.data_as(_ct.POINTER(_ct.c_float)),
        out_p75.ctypes.data_as(_ct.POINTER(_ct.c_float)),
    )
