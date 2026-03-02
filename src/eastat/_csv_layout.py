"""Low-level ctypes bindings for the csv_layout kernel."""
import ctypes as _ct

from ._loader import load_library

_lib = load_library("csv_layout")

_lib.build_row_arrays.argtypes = [_ct.POINTER(_ct.c_int32), _ct.c_int32, _ct.c_int32, _ct.c_int32, _ct.POINTER(_ct.c_int32), _ct.POINTER(_ct.c_int32), _ct.POINTER(_ct.c_int32)]
_lib.build_row_arrays.restype = None

_lib.build_row_delim_index.argtypes = [_ct.POINTER(_ct.c_int32), _ct.c_int32, _ct.POINTER(_ct.c_int32), _ct.c_int32, _ct.POINTER(_ct.c_int32), _ct.POINTER(_ct.c_int32)]
_lib.build_row_delim_index.restype = None

_lib.compute_field_bounds.argtypes = [_ct.c_int32, _ct.c_int32, _ct.c_int32, _ct.POINTER(_ct.c_int32), _ct.POINTER(_ct.c_int32), _ct.POINTER(_ct.c_int32), _ct.POINTER(_ct.c_int32), _ct.POINTER(_ct.c_int32), _ct.POINTER(_ct.c_int32), _ct.POINTER(_ct.c_int32)]
_lib.compute_field_bounds.restype = None


def build_row_arrays(lf_pos, n_lfs, header_end, text_len, out_row_starts, out_row_ends, out_n_rows):
    _lib.build_row_arrays(
        lf_pos.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        _ct.c_int32(int(n_lfs)),
        _ct.c_int32(int(header_end)),
        _ct.c_int32(int(text_len)),
        out_row_starts.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        out_row_ends.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        out_n_rows.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
    )


def build_row_delim_index(delim_pos, n_delims, row_ends, n_rows, out_delims_per_row, out_row_delim_offset):
    _lib.build_row_delim_index(
        delim_pos.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        _ct.c_int32(int(n_delims)),
        row_ends.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        _ct.c_int32(int(n_rows)),
        out_delims_per_row.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        out_row_delim_offset.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
    )


def compute_field_bounds(col_idx, col_count, n_rows, row_starts, row_ends, delim_pos, row_delim_offset, delims_per_row, out_field_starts, out_field_ends):
    _lib.compute_field_bounds(
        _ct.c_int32(int(col_idx)),
        _ct.c_int32(int(col_count)),
        _ct.c_int32(int(n_rows)),
        row_starts.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        row_ends.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        delim_pos.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        row_delim_offset.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        delims_per_row.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        out_field_starts.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
        out_field_ends.ctypes.data_as(_ct.POINTER(_ct.c_int32)),
    )
