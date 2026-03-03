"""End-to-end tests for eastat."""
import csv
import os
import tempfile

import numpy as np
import pytest

from eastat import process


def _write_csv(rows, path):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        for row in rows:
            w.writerow(row)


@pytest.fixture
def simple_csv(tmp_path):
    p = tmp_path / "test.csv"
    _write_csv([
        ["id", "value", "name"],
        [1, 10.0, "alice"],
        [2, 20.0, "bob"],
        [3, 30.0, "charlie"],
        [4, 40.0, "diana"],
        [5, 50.0, "eve"],
    ], p)
    return p


@pytest.fixture
def quoted_csv(tmp_path):
    p = tmp_path / "quoted.csv"
    _write_csv([
        ["id", "desc", "value"],
        [1, 'Widget, standard', 10.0],
        [2, 'Gadget "Pro"', 20.0],
        [3, "Plain item", 30.0],
    ], p)
    return p


def test_basic_stats(simple_csv):
    results, headers, n_rows, col_count, timings = process(simple_csv)

    assert headers == ["id", "value", "name"]
    assert n_rows == 5
    assert col_count == 3

    id_col = results[0]
    assert id_col['name'] == 'id'
    assert id_col['type'] == 'integer'
    assert id_col['count'] == 5
    assert id_col['min'] == 1.0
    assert id_col['max'] == 5.0

    val_col = results[1]
    assert val_col['name'] == 'value'
    assert val_col['count'] == 5
    assert val_col['min'] == 10.0
    assert val_col['max'] == 50.0

    name_col = results[2]
    assert name_col['name'] == 'name'
    assert name_col['type'] == 'string'


def test_quoted_scan(quoted_csv):
    results, headers, n_rows, col_count, timings = process(
        quoted_csv, force_quoted=True,
    )
    assert n_rows == 3
    assert col_count == 3
    assert timings['scan_mode'] == 'quoted'

    val_col = results[2]
    assert val_col['count'] == 3
    assert val_col['min'] == 10.0
    assert val_col['max'] == 30.0


def test_fast_scan_mode(simple_csv):
    _, _, _, _, timings = process(simple_csv, force_no_quotes=True)
    assert timings['scan_mode'] == 'fast'


def test_column_selection(simple_csv):
    results, _, _, _, _ = process(simple_csv, selected_columns=[1])
    assert len(results) == 1
    assert results[0]['name'] == 'value'


def test_timings_present(simple_csv):
    _, _, _, _, timings = process(simple_csv)
    for key in ['mmap', 'scan', 'layout', 'stats', 'total']:
        assert key in timings
        assert timings[key] >= 0


def test_scientific_notation(tmp_path):
    """Verify batch_atof handles scientific notation like pandas/polars."""
    p = tmp_path / "sci.csv"
    _write_csv([
        ["val"],
        ["1.5e-3"],
        ["-2.0E+5"],
        ["3e10"],
        ["4.2E2"],
        ["7.0e0"],
    ], p)
    results, headers, n_rows, col_count, timings = process(p)
    assert n_rows == 5
    col = results[0]
    assert col['count'] == 5
    # f32 precision: check approximate ranges
    assert abs(col['min'] - (-200000.0)) < 1.0
    assert abs(col['max'] - 3e10) / 3e10 < 0.01


def test_percentiles_large(tmp_path):
    """100-row CSV exercising the count >= 16 branch with np.percentile."""
    p = tmp_path / "large.csv"
    rows = [["value"]]
    for i in range(1, 101):
        rows.append([float(i)])
    _write_csv(rows, p)
    results, _, n_rows, _, _ = process(p)
    assert n_rows == 100
    col = results[0]
    assert col['count'] == 100
    assert col['min'] == 1.0
    assert col['max'] == 100.0
    # np.percentile on 1..100 with linear interpolation
    assert abs(col['p25'] - 25.75) < 0.5
    assert abs(col['p50'] - 50.5) < 0.5
    assert abs(col['p75'] - 75.25) < 0.5
