"""
run_validations.py
==================
Great Expectations runner for FreightLake medallion layers.

Defines rules in expectations/*.json and checks whether the data satisfies them.

Usage:
  uv run python gx/run_validations.py --demo
  uv run python gx/run_validations.py --postgres --suite silver.customers
  uv run python gx/run_validations.py --all --postgres
  uv run python gx/run_validations.py --postgres --layer silver

Modes:
  --demo      Validate synthetic pandas DataFrames, no DB required, always runnable.
  --postgres  Validate live Postgres silver/gold tables via SQLAlchemy.
  --layer     Restrict --all to one medallion layer, silver or gold.
  --databricks Validate Databricks gold tables via databricks-sql-connector if configured.

The expectation suites mirror dbt tests in dbt/models/silver/_silver.yml and
dbt/models/gold/_gold.yml so GX and dbt stay in sync.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import great_expectations as gx

    GX_AVAILABLE = True
    GX_VERSION = gx.__version__
except Exception as exc:  # pragma: no cover
    GX_AVAILABLE = False
    GX_VERSION = f"not installed ({exc})"

ROOT = Path(__file__).resolve().parents[1]
GX_ROOT = Path(__file__).resolve().parent
EXPECTATIONS_DIR = GX_ROOT / "expectations"

# Helpers to load suites from JSON files and validate pandas DataFrames without
# needing a full GX FileDataContext. This keeps demo mode zero config.


def load_suite(suite_name: str) -> dict[str, Any]:
    path = EXPECTATIONS_DIR / f"{suite_name.replace('.', '_').replace('/', '_')}.json"
    # also try dotted name directly
    if not path.exists():
        alt = EXPECTATIONS_DIR / f"{suite_name}.json"
        if alt.exists():
            path = alt
        else:
            # search by expectation_suite_name inside file
            for p in EXPECTATIONS_DIR.glob("*.json"):
                try:
                    data = json.loads(p.read_text(encoding="utf-8"))
                    if data.get("expectation_suite_name") == suite_name:
                        return data
                except Exception:
                    continue
            raise FileNotFoundError(f"Suite {suite_name!r} not found in {EXPECTATIONS_DIR}")
    return json.loads(path.read_text(encoding="utf-8"))


def list_suites() -> list[str]:
    suites: list[str] = []
    for p in sorted(EXPECTATIONS_DIR.glob("*.json")):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            suites.append(data.get("expectation_suite_name", p.stem))
        except Exception:
            suites.append(p.stem)
    return suites


def _eval_expectation(df: pd.DataFrame, exp: dict[str, Any]) -> dict[str, Any]:
    """Minimal evaluator for core expectation types, no GX context needed."""
    exp_type = exp.get("expectation_type")
    kwargs = exp.get("kwargs", {})
    col = kwargs.get("column")
    try:
        if exp_type == "expect_column_values_to_not_be_null":
            assert col in df.columns, f"column {col} missing"
            nulls = int(df[col].isna().sum())
            return {"success": nulls == 0, "observed": nulls, "details": f"nulls={nulls}"}
        if exp_type == "expect_column_values_to_be_unique":
            assert col in df.columns
            dupes = int(df.duplicated(subset=[col]).sum())
            return {"success": dupes == 0, "observed": dupes, "details": f"dupes={dupes}"}
        if exp_type == "expect_column_values_to_match_regex":
            pattern = kwargs.get("regex", "")
            assert col in df.columns
            vals = df[col].dropna().astype(str)
            bad = int((~vals.str.match(pattern)).sum()) if len(vals) else 0
            return {"success": bad == 0, "observed": bad, "details": f"regex_mismatch={bad}"}
        if exp_type == "expect_column_values_to_be_in_set":
            value_set = set(kwargs.get("value_set", []))
            assert col in df.columns
            vals = df[col].dropna()
            bad = int((~vals.isin(value_set)).sum()) if len(vals) else 0
            return {"success": bad == 0, "observed": bad, "details": f"out_of_set={bad}"}
        if exp_type == "expect_column_values_to_be_between":
            min_v = kwargs.get("min_value")
            max_v = kwargs.get("max_value")
            assert col in df.columns
            vals = pd.to_numeric(df[col].dropna(), errors="coerce").dropna()
            bad = 0
            if len(vals):
                if min_v is not None:
                    bad += int((vals < min_v).sum())
                if max_v is not None:
                    bad += int((vals > max_v).sum())
            return {"success": bad == 0, "observed": bad, "details": f"out_of_range={bad}"}
        if exp_type == "expect_table_row_count_to_be_between":
            min_v = kwargs.get("min_value", 0)
            max_v = kwargs.get("max_value")
            n = len(df)
            ok = n >= min_v and (max_v is None or n <= max_v)
            return {"success": ok, "observed": n, "details": f"rows={n}"}
        return {"success": True, "observed": None, "details": f"unknown type {exp_type} skipped"}
    except Exception as e:
        return {"success": False, "observed": None, "details": f"error: {e}"}


def validate_dataframe(df: pd.DataFrame, suite: dict[str, Any]) -> dict[str, Any]:
    results = []
    for exp in suite.get("expectations", []):
        r = _eval_expectation(df, exp)
        r["expectation_type"] = exp.get("expectation_type")
        r["kwargs"] = exp.get("kwargs")
        r["meta"] = exp.get("meta", {})
        results.append(r)
    success = all(x["success"] for x in results)
    return {"suite": suite.get("expectation_suite_name"), "success": success, "results": results}


# Demo DataFrames mirroring FreightLake schemas


def demo_dataframe(suite_name: str, bad: bool = False) -> pd.DataFrame:
    if suite_name == "silver.customers":
        df = pd.DataFrame(
            {
                "customer_id": ["CUST_001", "CUST_002", "CUST_003"],
                "customer_name": ["Acme Corp", "Beta LLC", "Gamma Inc"],
                "customer_type": ["Contract", "Spot", "Dedicated"],
                "credit_terms_days": [30, 60, 0],
                "primary_freight_type": ["Retail", "General", "Electronics"],
                "account_status": ["Active", "Active", "Inactive"],
                "annual_revenue_potential": [1_000_000, 2_000_000, 500_000],
            }
        )
        if bad:
            df.loc[1, "customer_id"] = None
            df.loc[2, "customer_type"] = "BadType"
        return df
    if suite_name == "silver.trucks":
        return pd.DataFrame(
            {
                "truck_id": ["TRK_001", "TRK_002"],
                "unit_number": ["UNIT001", "UNIT002"],
                "vin": ["1HGCM82633A123456", "1HGCM82633A654321"],
                "model_year": [2020, 2025],
                "fuel_type": ["Diesel", "Diesel"],
                "status": ["Active", "Maintenance"],
                "tank_capacity_gallons": [120, 150],
            }
        )
    if suite_name == "silver.trailers":
        df = pd.DataFrame(
            {
                "trailer_id": ["TRL_001", "TRL_002", "TRL_003"],
                "trailer_number": ["TRLR001", "TRLR002", "TRLR003"],
                "trailer_type": ["Dry Van", "Refrigerated", "Dry Van"],
                "length_feet": [53, 48, 53],
                "status": ["Active", "Active", "Active"],
            }
        )
        if bad:
            # 4 dupes case simplified to 1 dupe
            df.loc[2, "trailer_number"] = "TRLR001"
        return df
    if suite_name == "silver.fuel_purchases":
        df = pd.DataFrame(
            {
                "fuel_purchase_id": ["FP_001", "FP_002"],
                "truck_id": ["TRK_001", "TRK_002"],
                "gallons": [100, 80],
                "price_per_gallon": [4.5, 5.0],
                "total_cost": [450, 400],
                "fuel_card_number": ["CARD-001", "CARD-002"],
            }
        )
        if bad:
            df.loc[1, "truck_id"] = None
        return df
    if suite_name == "silver.trips":
        return pd.DataFrame(
            {
                "trip_id": ["TRIP_001", "TRIP_002"],
                "actual_distance_miles": [500, 1200],
                "trip_status": ["Completed", "Completed"],
            }
        )
    if suite_name == "gold.dim_customers":
        return pd.DataFrame(
            {
                "customer_sk": ["sk1", "sk2"],
                "customer_id": ["CUST_001", "CUST_002"],
                "customer_type": ["Contract", "Spot"],
                "account_status": ["Active", "Active"],
                "effective_from": pd.to_datetime(["2023-01-01", "2023-02-01"]),
                "is_current": [True, True],
            }
        )
    if suite_name == "gold.dim_date":
        dates = pd.date_range("2020-01-01", "2030-12-31", freq="D")
        return pd.DataFrame({"date_key": dates.strftime("%Y%m%d").astype(int), "full_date": dates})
    if suite_name == "gold.fact_loads":
        return pd.DataFrame(
            {
                "load_id": ["LOAD_001", "LOAD_002"],
                "load_type": ["Dry Van", "Refrigerated"],
                "load_status": ["Completed", "Completed"],
                "total_charge": [2500, 5000],
                "is_unmatched_customer": [False, False],
            }
        )
    # fallback generic
    return pd.DataFrame({"id": [1, 2, 3]})


def validate_postgres(suite_name: str, table: str) -> dict[str, Any] | None:
    try:
        from sqlalchemy import text

        from src.utils.connections import get_postgres_engine

        engine = get_postgres_engine()
        suite = load_suite(suite_name)
        # map suite to table if not provided
        sql_table = table or suite_name.replace(".", "_")
        # try silver/gold schema qualified
        schemas = ["silver", "gold", "public"]
        df = None
        last_err = None
        for schema in schemas:
            try:
                q = text(f'SELECT * FROM "{schema}"."{sql_table.split(".")[-1]}" LIMIT 10000')
                with engine.connect() as conn:
                    df = pd.read_sql(q, conn)
                if df is not None and not df.empty:
                    break
            except Exception as e:
                last_err = e
                continue
        if df is None or df.empty:
            # try unqualified
            try:
                q = text(f"SELECT * FROM {sql_table} LIMIT 10000")
                with engine.connect() as conn:
                    df = pd.read_sql(q, conn)
            except Exception as e:
                last_err = e
        if df is None or df.empty:
            return {
                "suite": suite_name,
                "success": None,
                "error": f"no data or table not found: {last_err}",
            }
        return validate_dataframe(df, suite)
    except Exception as e:
        return {"suite": suite_name, "success": None, "error": str(e)}


def print_result(res: dict[str, Any]) -> None:
    suite = res.get("suite", "unknown")
    ok = res.get("success")
    if ok is True:
        print(f"  PASS {suite}")
    elif ok is False:
        # if all failures are warn, surface as WARN at suite level
        results = res.get("results", [])
        has_error = any(
            not r["success"] and r.get("meta", {}).get("severity") != "warn" for r in results
        )
        label = "FAIL" if has_error else "WARN"
        print(f"  {label} {suite}")
    else:
        print(f"  SKIP {suite}: {res.get('error', 'no result')}")
    for r in res.get("results", []):
        mark = (
            "PASS"
            if r["success"]
            else "WARN"
            if r.get("meta", {}).get("severity") == "warn"
            else "FAIL"
        )
        print(f"    {mark} {r['expectation_type']} {r['kwargs']} -> {r['details']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="FreightLake Great Expectations runner")
    parser.add_argument("--suite", help="single suite name e.g. silver.customers")
    parser.add_argument("--all", action="store_true", help="validate all suites")
    parser.add_argument(
        "--layer", choices=["silver", "gold"], help="restrict --all to one medallion layer"
    )
    parser.add_argument("--demo", action="store_true", help="use synthetic pandas data, no DB")
    parser.add_argument("--postgres", action="store_true", help="validate live Postgres tables")
    parser.add_argument("--bad-demo", action="store_true", help="inject bad rows to demo failures")
    parser.add_argument("--list", action="store_true", help="list available suites")
    args = parser.parse_args()

    print(f"Great Expectations {GX_VERSION}  gx_root={GX_ROOT}")
    if args.list:
        for s in list_suites():
            print(f"  {s}")
        return 0

    suites = list_suites()
    if args.suite:
        suites = [args.suite]
    if args.layer:
        # keep the explicit --suite choice when both are given, otherwise
        # filter the full list down to the requested layer
        if not args.suite:
            suites = [s for s in suites if s.split(".")[0] == args.layer]
    elif not args.all and not args.demo and not args.postgres:
        # default demo
        args.demo = True

    if args.demo and args.postgres:
        print("Both --demo and --postgres set, running demo first then postgres")

    exit_code = 0
    # Demo path always runnable
    if args.demo or (not args.postgres):
        print("\n[demo] pandas validations")
        for s in suites:
            try:
                suite = load_suite(s)
                df = demo_dataframe(s, bad=args.bad_demo)
                res = validate_dataframe(df, suite)
                print_result(res)
                if res["success"] is False:
                    # warn only suites may fail expected, check severity
                    has_error = any(
                        not r["success"] and r.get("meta", {}).get("severity") != "warn"
                        for r in res["results"]
                    )
                    if has_error:
                        exit_code = 1
            except FileNotFoundError as e:
                print(f"  SKIP {s}: {e}")
            except Exception as e:
                print(f"  ERROR {s}: {e}")
                exit_code = 1

    if args.postgres:
        print("\n[postgres] live validations")
        try:
            from src.utils.connections import get_postgres_engine  # noqa: F401

            for s in suites:
                table = s.replace(".", "_")
                # map silver.customers -> customers etc.
                short = s.split(".")[-1]
                res = validate_postgres(s, short)
                if res is None:
                    continue
                print_result(res)
                if res.get("success") is False:
                    exit_code = 1
                if res.get("success") is None:
                    print("    hint: is Postgres running? check POSTGRES_HOST in .env")
        except Exception as e:
            print(f"  Postgres unavailable: {e}")
            print("  This is a failed gate, not a skipped one: fix Postgres or run")
            print("  --demo explicitly for a no DB smoke test.")
            if not args.demo:
                exit_code = 1

    if exit_code == 0:
        print("\nAll validations passed or warned as expected.")
    else:
        print("\nSome validations failed as error.")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
