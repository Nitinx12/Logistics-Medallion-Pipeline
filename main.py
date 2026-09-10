"""FreightLake single entry point — runs the whole pipeline end to end.

Reuses existing scripts/modules rather than reimplementing their logic.
Usage:
  python main.py [--skip-docker] [--dry-run] [--target {dev,local}]
  uv run python main.py

Steps in dependency order:
  1. setup checks -> 2. docker up (postgres, mongo) -> 3. seed ->
  4. bronze (postgres + mongo) -> 5. dbt build (silver + gold) -> 6. publish

Target selection:
  --target dev    Force the Databricks dbt target (requires credentials).
  --target local  Force the DuckDB local target (reads delta/bronze/*.parquet).
  (default)       Auto-detect: use 'local' when DATABRICKS_HOST is absent or
                  matches the placeholder from .env.example, else 'dev'.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import subprocess
import sys
import time
from datetime import UTC, datetime

_DATABRICKS_PLACEHOLDER = "https://<your-workspace>.cloud.databricks.com"


def find_project_root(start: pathlib.Path | None = None) -> pathlib.Path:
    cur = (start or pathlib.Path(__file__).resolve()).parent
    for _ in range(10):
        if (cur / "pyproject.toml").exists() and (cur / ".env.example").exists():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return pathlib.Path(__file__).resolve().parent


def _resolve_dbt_target(requested: str | None) -> str:
    """Return the dbt target to use.

    Priority:
    1. Explicit --target flag from the caller.
    2. DBT_TARGET env var.
    3. Auto-detect: 'local' when DATABRICKS_HOST is missing or placeholder.
    """
    if requested:
        return requested
    env_target = os.getenv("DBT_TARGET")
    if env_target:
        return env_target
    host = os.getenv("DATABRICKS_HOST", "")
    if not host or host == _DATABRICKS_PLACEHOLDER:
        return "local"
    return "dev"


ROOT = find_project_root()

# STEPS is built dynamically in main() once the dbt target is known.


def run_step(name: str, cmd: list[str], dry_run: bool = False) -> tuple[str, float, int]:
    start = time.time()
    ts = datetime.now(UTC).isoformat()
    print(f"\n[{ts}] >>> {name}: {' '.join(cmd)}")
    if dry_run:
        print(f"[{ts}] --- dry-run, skipping execution")
        return ("skipped (dry-run)", 0.0, 0)
    try:
        result = subprocess.run(cmd, cwd=ROOT, capture_output=False, text=True, check=False)
        elapsed = time.time() - start
        status = "ok" if result.returncode == 0 else f"failed ({result.returncode})"
        ts2 = datetime.now(UTC).isoformat()
        print(f"[{ts2}] <<< {name}: {status} in {elapsed:.1f}s")
        return (status, elapsed, result.returncode)
    except FileNotFoundError as e:
        elapsed = time.time() - start
        print(f"[{datetime.now(UTC).isoformat()}] <<< {name}: failed FileNotFound {e}")
        return (f"failed FileNotFound {e}", elapsed, 127)
    except Exception as e:  # noqa: BLE001
        elapsed = time.time() - start
        print(f"[{datetime.now(UTC).isoformat()}] <<< {name}: failed {e}")
        return (f"failed {e}", elapsed, 1)


def _warn_banner(message: str) -> None:
    border = "=" * 70
    print(f"\n{border}")
    print(f"  WARNING: {message}")
    print(f"{border}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="FreightLake one command pipeline")
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        help="skip docker up when Postgres/Mongo already running",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print planned steps without executing",
    )
    parser.add_argument(
        "--target",
        choices=["dev", "local"],
        default=None,
        help="dbt target to use: 'dev' (Databricks) or 'local' (DuckDB). "
        "Default: auto-detect from DATABRICKS_HOST.",
    )
    args = parser.parse_args()

    dbt_target = _resolve_dbt_target(args.target)

    STEPS = [
        ("setup", "Setup checks (uv, python, docker)", ["uv", "run", "python", "--version"]),
        (
            "docker",
            "Docker up (postgres, mongo)",
            # --wait blocks until each service's healthcheck reports healthy
            # (pg_isready / mongosh ping) before this step returns. Without it,
            # `up -d` returns as soon as the containers start, which races
            # seed.py against Postgres init/crash-recovery on a fresh volume.
            # Requires Docker Compose v2.17+.
            [
                "docker",
                "compose",
                "--env-file",
                ".env",
                "-f",
                "docker/compose.yml",
                "up",
                "-d",
                "--wait",
                "postgres",
                "mongo",
            ],
        ),
        ("seed", "Seed Postgres + Mongo", ["uv", "run", "python", "scripts/seed.py"]),
        (
            "bronze_pg",
            "Bronze Postgres OLTP",
            ["uv", "run", "python", "-m", "spark_jobs.bronze.extract_postgres_oltp"],
        ),
        (
            "bronze_mongo",
            "Bronze Mongo tracking",
            ["uv", "run", "python", "-m", "spark_jobs.bronze.extract_mongo_tracking"],
        ),
        (
            "dbt",
            f"dbt build silver + gold (target={dbt_target})",
            [
                "uv",
                "run",
                "dbt",
                "build",
                "--project-dir",
                "dbt",
                "--profiles-dir",
                "dbt",
                "--target",
                dbt_target,
            ],
        ),
        (
            "publish",
            "Publish gold to mart",
            ["uv", "run", "python", "-m", "spark_jobs.publish.publish_gold_to_postgres"],
        ),
    ]

    print(
        f"FreightLake pipeline start {datetime.now(UTC).isoformat()} "
        f"from {ROOT}  [dbt-target={dbt_target}]"
    )
    if dbt_target == "local":
        print(
            "  INFO: using local DuckDB dbt target — bronze data read from "
            "delta/bronze/*.parquet (no Databricks connection required)."
        )

    if args.dry_run:
        print("Dry run — planned steps:")
        for name, desc, cmd in STEPS:
            if args.skip_docker and name == "docker":
                print(f"  - {name}: {desc} [skip-docker, skipped]")
                continue
            print(f"  - {name}: {desc} -> {' '.join(cmd)}")
        return

    results: list[tuple[str, str, float]] = []
    used_fallback = False

    for name, desc, cmd in STEPS:
        if args.skip_docker and name == "docker":
            print(
                f"\n[{datetime.now(UTC).isoformat()}] >>> {name}: {desc} "
                "[skip-docker, skipped]"
            )
            results.append((name, "skipped", 0.0))
            continue

        # Special handling for dbt: if the warehouse is unavailable, fall back
        # to the local silver/gold scripts.  The fallback is a DEGRADED run —
        # it is logged loudly and the final exit code is 2 (not 0) so CI can
        # distinguish a true pass from a degraded one.
        if name == "dbt":
            status, elapsed, code = run_step(name, cmd, dry_run=False)
            if code != 0:
                _warn_banner(
                    f"dbt build failed (exit {code}). "
                    "Falling back to local silver/gold via scripts/run_local_pipeline.py. "
                    "This run is DEGRADED — fix dbt before merging."
                )
                try:
                    import scripts.run_local_pipeline as lp

                    t0 = time.time()
                    print(
                        f"[{datetime.now(UTC).isoformat()}] "
                        ">>> dbt-fallback: local silver/gold"
                    )
                    lp.silver()
                    lp.gold()
                    elapsed2 = time.time() - t0
                    print(
                        f"[{datetime.now(UTC).isoformat()}] "
                        f"<<< dbt-fallback: complete in {elapsed2:.1f}s"
                    )
                    results.append((name, "WARN: dbt failed (local fallback)", elapsed + elapsed2))
                    used_fallback = True
                    continue
                except Exception as e:  # noqa: BLE001
                    print(
                        f"[{datetime.now(UTC).isoformat()}] "
                        f"dbt-fallback also failed: {e}"
                    )
                    results.append((name, f"failed {e}", elapsed))
                    print(f"\nStep {name} failed and fallback also failed: {e}")
                    sys.exit(1)
            else:
                results.append((name, status, elapsed))
                continue

        status, elapsed, code = run_step(name, cmd, dry_run=False)
        if code != 0:
            print(f"\nStep {name} failed with code {code}, aborting pipeline")
            print("\nSummary:")
            for n, s, d in results:
                print(f"  {n:15} {s:35} {d:5.1f}s")
            print(f"  {name:15} failed{'':28} {elapsed:5.1f}s")
            sys.exit(code)
        results.append((name, status, elapsed))

    # Summary
    print("\n" + "=" * 70)
    if used_fallback:
        print("Pipeline complete (DEGRADED — dbt fallback was used)")
    else:
        print(f"Pipeline complete {datetime.now(UTC).isoformat()}")
    print("-" * 70)
    print(f"{'step':15} {'status':35} {'duration'}")
    print("-" * 70)
    for n, s, d in results:
        print(f"{n:15} {s:35} {d:5.1f}s")
    print("-" * 70)
    print("Verify: psql freightlake_mart -c 'SELECT tablename FROM pg_tables WHERE schemaname=''mart'''")
    print("Delta: delta/bronze, delta/silver, delta/gold")
    print("=" * 70)

    if used_fallback:
        _warn_banner(
            "Pipeline exiting with code 2 (degraded). "
            "dbt build failed and the local fallback was used instead. "
            "Run 'make dbt-run' to see the full dbt error and fix it."
        )
        sys.exit(2)


if __name__ == "__main__":
    main()