"""FreightLake single entry point — runs the whole pipeline end to end.

Reuses existing scripts/modules rather than reimplementing their logic.
Usage:
  python main.py [--skip-docker] [--dry-run]
  uv run python main.py

Steps in dependency order:
  1. setup checks -> 2. docker up (postgres, mongo) -> 3. seed ->
  4. bronze (postgres + mongo) -> 5. dbt build (silver + gold) -> 6. publish
"""

from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys
import time
from datetime import datetime, timezone


def find_project_root(start: pathlib.Path | None = None) -> pathlib.Path:
    cur = (start or pathlib.Path(__file__).resolve()).parent
    for _ in range(10):
        if (cur / "pyproject.toml").exists() and (cur / ".env.example").exists():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return pathlib.Path(__file__).resolve().parent


ROOT = find_project_root()
STEPS = [
    ("setup", "Setup checks (uv, python, docker)", ["uv", "run", "python", "--version"]),
    ("docker", "Docker up (postgres, mongo)", ["docker", "compose", "--env-file", ".env", "-f", "docker/compose.yml", "up", "-d", "postgres", "mongo"]),
    ("seed", "Seed Postgres + Mongo", ["uv", "run", "python", "scripts/seed.py"]),
    ("bronze_pg", "Bronze Postgres OLTP", ["uv", "run", "python", "-m", "spark_jobs.bronze.extract_postgres_oltp"]),
    ("bronze_mongo", "Bronze Mongo tracking", ["uv", "run", "python", "-m", "spark_jobs.bronze.extract_mongo_tracking"]),
    ("dbt", "dbt build silver + gold", ["uv", "run", "dbt", "build", "--project-dir", "dbt", "--profiles-dir", "dbt", "--target", "dev"]),
    ("publish", "Publish gold to mart", ["uv", "run", "python", "-m", "spark_jobs.publish.publish_gold_to_postgres"]),
]


def run_step(name: str, cmd: list[str], dry_run: bool = False) -> tuple[str, float, int]:
    start = time.time()
    ts = datetime.now(timezone.utc).isoformat()
    print(f"\n[{ts}] >>> {name}: {' '.join(cmd)}")
    if dry_run:
        print(f"[{ts}] --- dry-run, skipping execution")
        return ("skipped (dry-run)", 0.0, 0)
    try:
        # Use uv run where needed, but allow fallback for docker
        result = subprocess.run(cmd, cwd=ROOT, capture_output=False, text=True)
        elapsed = time.time() - start
        status = "ok" if result.returncode == 0 else f"failed ({result.returncode})"
        ts2 = datetime.now(timezone.utc).isoformat()
        print(f"[{ts2}] <<< {name}: {status} in {elapsed:.1f}s")
        return (status, elapsed, result.returncode)
    except FileNotFoundError as e:
        elapsed = time.time() - start
        print(f"[{datetime.now(timezone.utc).isoformat()}] <<< {name}: failed FileNotFound {e}")
        return (f"failed FileNotFound {e}", elapsed, 127)
    except Exception as e:  # noqa: BLE001
        elapsed = time.time() - start
        print(f"[{datetime.now(timezone.utc).isoformat()}] <<< {name}: failed {e}")
        return (f"failed {e}", elapsed, 1)


def main() -> None:
    parser = argparse.ArgumentParser(description="FreightLake one command pipeline")
    parser.add_argument("--skip-docker", action="store_true", help="skip docker up when Postgres/Mongo already running")
    parser.add_argument("--dry-run", action="store_true", help="print planned steps without executing")
    args = parser.parse_args()

    print(f"FreightLake pipeline start {datetime.now(timezone.utc).isoformat()} from {ROOT}")
    if args.dry_run:
        print("Dry run — planned steps:")
        for name, desc, cmd in STEPS:
            if args.skip_docker and name == "docker":
                print(f"  - {name}: {desc} [skip-docker, skipped]")
                continue
            print(f"  - {name}: {desc} -> {' '.join(cmd)}")
        return

    results = []
    for name, desc, cmd in STEPS:
        if args.skip_docker and name == "docker":
            print(f"\n[{datetime.now(timezone.utc).isoformat()}] >>> {name}: {desc} [skip-docker, skipped]")
            results.append((name, "skipped", 0.0))
            continue

        # Special handling for dbt: if warehouse not available, fallback to local silver/gold via run_local_pipeline
        if name == "dbt":
            status, elapsed, code = run_step(name, cmd, dry_run=False)
            if code != 0:
                print(f"[{datetime.now(timezone.utc).isoformat()}] dbt build failed (likely sql scope), falling back to local silver/gold via scripts/run_local_pipeline.py")
                # Run local silver/gold/publish fallback via import
                try:
                    import scripts.run_local_pipeline as lp

                    t0 = time.time()
                    print(f"[{datetime.now(timezone.utc).isoformat()}] >>> dbt-fallback: local silver/gold")
                    lp.silver()
                    lp.gold()
                    elapsed2 = time.time() - t0
                    print(f"[{datetime.now(timezone.utc).isoformat()}] <<< dbt-fallback: ok in {elapsed2:.1f}s")
                    results.append((name, "ok (fallback local)", elapsed + elapsed2))
                    continue
                except Exception as e:  # noqa: BLE001
                    print(f"[{datetime.now(timezone.utc).isoformat()}] dbt-fallback failed: {e}")
                    results.append((name, f"failed {e}", elapsed))
                    print(f"\nStep {name} failed: {e}")
                    sys.exit(1)
            else:
                results.append((name, status, elapsed))
                continue

        status, elapsed, code = run_step(name, cmd, dry_run=False)
        if code != 0:
            print(f"\nStep {name} failed with code {code}, aborting pipeline")
            # Print summary so far
            print("\nSummary:")
            for n, s, d in results:
                print(f"  {n:15} {s:20} {d:5.1f}s")
            print(f"  {name:15} failed{'':13} {elapsed:5.1f}s")
            sys.exit(code)
        results.append((name, status, elapsed))

    # Success summary
    print("\n" + "=" * 60)
    print(f"Pipeline complete {datetime.now(timezone.utc).isoformat()}")
    print("-" * 60)
    print(f"{'step':15} {'status':20} {'duration'}")
    print("-" * 60)
    for n, s, d in results:
        print(f"{n:15} {s:20} {d:5.1f}s")
    print("-" * 60)
    print("Verify: psql freightlake_mart -c 'SELECT tablename FROM pg_tables WHERE schemaname=''mart'''")
    print("Delta: delta/bronze, delta/silver, delta/gold")
    print("=" * 60)


if __name__ == "__main__":
    main()
