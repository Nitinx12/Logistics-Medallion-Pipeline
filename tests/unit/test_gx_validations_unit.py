"""tests/unit/test_gx_validations_unit.py

Pure unit tests for gx/run_validations lightweight evaluator,
separate from integration gx_tests.
"""

import importlib.util
from pathlib import Path

import pandas as pd

SPEC = importlib.util.spec_from_file_location(
    "gx_run_validations_unit", Path(__file__).resolve().parents[2] / "gx" / "run_validations.py"
)
_mod = importlib.util.module_from_spec(SPEC)  # type: ignore
assert SPEC and SPEC.loader
SPEC.loader.exec_module(_mod)  # type: ignore

validate_dataframe = _mod.validate_dataframe  # type: ignore
_eval = _mod._eval_expectation  # type: ignore


def test_eval_not_null():
    df = pd.DataFrame({"a": [1, None, 3]})
    exp = {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "a"},
        "meta": {},
    }
    r = _eval(exp, df) if False else _mod._eval_expectation(df, exp)
    assert r["success"] is False
    assert r["observed"] == 1


def test_eval_unique():
    df = pd.DataFrame({"a": [1, 1, 2]})
    exp = {
        "expectation_type": "expect_column_values_to_be_unique",
        "kwargs": {"column": "a"},
        "meta": {},
    }
    r = _mod._eval_expectation(df, exp)
    assert r["success"] is False
    assert r["observed"] == 1


def test_eval_regex():
    df = pd.DataFrame({"a": ["ABC-123", "bad value!", "XYZ-1"]})
    exp = {
        "expectation_type": "expect_column_values_to_match_regex",
        "kwargs": {"column": "a", "regex": "^[A-Z0-9-]+$"},
        "meta": {},
    }
    r = _mod._eval_expectation(df, exp)
    assert r["success"] is False
    assert r["observed"] == 1


def test_eval_in_set():
    df = pd.DataFrame({"a": ["Contract", "Spot", "Bad"]})
    exp = {
        "expectation_type": "expect_column_values_to_be_in_set",
        "kwargs": {"column": "a", "value_set": ["Contract", "Spot"]},
        "meta": {},
    }
    r = _mod._eval_expectation(df, exp)
    assert r["success"] is False


def test_eval_between():
    df = pd.DataFrame({"a": [1, 50, 200]})
    exp = {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "a", "min_value": 0, "max_value": 100},
        "meta": {},
    }
    r = _mod._eval_expectation(df, exp)
    assert r["success"] is False
    assert r["observed"] == 1


def test_eval_table_row_count():
    df = pd.DataFrame({"a": [1]})
    exp = {
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {"min_value": 2},
        "meta": {},
    }
    r = _mod._eval_expectation(df, exp)
    assert r["success"] is False


def test_validate_dataframe_warn_severity():
    df = pd.DataFrame({"truck_id": ["A", None]})
    suite = {
        "expectation_suite_name": "silver.fuel_purchases",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "truck_id"},
                "meta": {"severity": "warn"},
            }
        ],
    }
    res = validate_dataframe(df, suite)
    assert res["success"] is False
    assert res["results"][0]["meta"]["severity"] == "warn"
