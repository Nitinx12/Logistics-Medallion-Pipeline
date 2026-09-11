"""tests/test_gx_validations.py - ensure GX demo suites pass"""

import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "gx_run_validations", Path(__file__).resolve().parents[2] / "gx" / "run_validations.py"
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
assert _spec and _spec.loader
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

demo_dataframe = _mod.demo_dataframe  # type: ignore[attr-defined]
load_suite = _mod.load_suite  # type: ignore[attr-defined]
validate_dataframe = _mod.validate_dataframe  # type: ignore[attr-defined]
list_suites = _mod.list_suites  # type: ignore[attr-defined]


def test_all_suites_exist():
    suites = list_suites()
    assert "silver.customers" in suites
    assert "silver.fuel_purchases" in suites
    assert "gold.fact_loads" in suites
    assert len(suites) >= 8


def test_demo_suites_pass_without_bad_data():
    for suite_name in list_suites():
        suite = load_suite(suite_name)
        df = demo_dataframe(suite_name, bad=False)
        res = validate_dataframe(df, suite)
        # all clean demos must pass, warns are not triggered on clean data
        assert res["success"] is True, f"{suite_name} failed on clean data: {res}"


def test_bad_demo_triggers_expected_warns():
    # trailers dupe is warn, should be detectable but not error
    suite = load_suite("silver.trailers")
    df = demo_dataframe("silver.trailers", bad=True)
    res = validate_dataframe(df, suite)
    assert res["success"] is False
    warn = [r for r in res["results"] if not r["success"]]
    assert any(r["meta"].get("severity") == "warn" for r in warn)

    # fuel_purchases null truck_id is warn
    suite = load_suite("silver.fuel_purchases")
    df = demo_dataframe("silver.fuel_purchases", bad=True)
    res = validate_dataframe(df, suite)
    assert res["success"] is False
    warn = [r for r in res["results"] if not r["success"]]
    assert any(r["kwargs"].get("column") == "truck_id" for r in warn)
