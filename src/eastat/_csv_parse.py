"""Low-level ctypes bindings for the csv_parse kernel."""
import ctypes as _ct

from ._loader import load_library

_lib = load_library("csv_parse")

_lib.batch_atof.argtypes = [_ct.POINTER(_ct.c_uint8), _ct.POINTER(_ct.c_int32), _ct.POINTER(_ct.c_int32), _ct.c_int32, _ct.POINTER(_ct.c_float), _ct.POINTER(_ct.c_int32)]
_lib.batch_atof.restype = None

_lib.field_length_stats.argtypes = [_ct.POINTER(_ct.c_int32), _ct.POINTER(_ct.c_int32), _ct.c_int32, _ct.POINTER(_ct.c_int32), _ct.POINTER(_ct.c_int32), _ct.POINTER(_ct.c_int32), _ct.POINTER(_ct.c_int32)]
_lib.field_length_stats.restype = None


def batch_atof(data, starts, ends, out, out_count):
    _lib.batch_atof(
        data.ctypes.data_as(_ct.POINTER(_ct.c_uint8)),
        starts.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        ends.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        _ct.c_int32(ends.size),
        out.ctypes.data_as(_ct.POINTER(_ct.c_float)),
        out_count.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
    )


def field_length_stats(starts, ends, out_min_len, out_max_len, out_total_len, out_null_count):
    _lib.field_length_stats(
        starts.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        ends.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        _ct.c_int32(ends.size),
        out_min_len.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        out_max_len.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        out_total_len.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        out_null_count.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
    )
