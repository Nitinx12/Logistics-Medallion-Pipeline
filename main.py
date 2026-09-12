"""FreightLake local pipeline runner.

Single entry point for a full local run: start the Docker stack, run both
bronze extraction jobs, build and test silver then gold with dbt, and
publish the gold star schema to the Postgres mart. Each stage runs exactly
the commands the Airflow DAGs run, so a local run exercises what
production executes.

Usage:
    uv run python main.py                  # full pipeline
    uv run python main.py --skip-docker    # services already running
    uv run python main.py --dry-run        # print the plan, execute nothing
    uv run python main.py --stage gold     # one stage only
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
LOG_FILE = REPO_ROOT / "logs" / f"local_runner_{datetime.now():%Y-%m-%d}.log"

STAGES = ("docker", "bronze", "silver", "gold", "publish")


def log(message: str) -> None:
    """Print a line and append it to the dated local_runner log."""
    line = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {message}"
    print(line, flush=True)
    LOG_FILE.parent.mkdir(exist_ok=True)
    with LOG_FILE.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def stage_commands() -> dict[str, list[tuple[list[str], Path]]]:
    """The command list per stage, mirroring the Airflow DAG task commands."""
    dbt_dir = REPO_ROOT / "dbt"
    return {
        "docker": [
            (
                ["docker", "compose", "--project-directory", "docker", "up", "-d", "--wait"],
                REPO_ROOT,
            ),
        ],
        "bronze": [
            (
                [
                    "uv",
                    "run",
                    "python",
                    "-m",
                    "src.jobs.pg_extract_incremental",
                    "--target-schema",
                    "bronze",
                ],
                REPO_ROOT,
            ),
            (
                [
                    "uv",
                    "run",
                    "python",
                    "-m",
                    "src.jobs.mongo_extract_incremental",
                    "--target-schema",
                    "bronze",
                ],
                REPO_ROOT,
            ),
        ],
        "silver": [
            (
                ["uv", "run", "dbt", "build", "--select", "tag:silver", "--profiles-dir", "."],
                dbt_dir,
            ),
        ],
        "gold": [
            (["uv", "run", "dbt", "build", "--select", "tag:gold", "--profiles-dir", "."], dbt_dir),
        ],
        "publish": [
            (["uv", "run", "python", "-m", "src.jobs.publish_gold_to_postgres"], REPO_ROOT),
        ],
    }


def run_command(command: list[str], cwd: Path, dry_run: bool) -> bool:
    """Run one command, streaming output; return True on success."""
    log(f"[local_runner] $ {' '.join(command)}  (in {cwd.name})")
    if dry_run:
        return True
    try:
        completed = subprocess.run(command, cwd=cwd, check=False)
    except FileNotFoundError:
        log(f"[local_runner] ERROR command not found: {command[0]}")
        return False
    return completed.returncode == 0


def main() -> int:
    """Parse flags and run the selected pipeline stages."""
    parser = argparse.ArgumentParser(description="FreightLake local pipeline runner.")
    parser.add_argument(
        "--stage",
        choices=STAGES,
        default=None,
        help="Run a single stage instead of the full pipeline.",
    )
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        help="Do not start the Docker stack (services already running).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the commands without executing anything.",
    )
    args = parser.parse_args()

    commands = stage_commands()
    stages = [args.stage] if args.stage else list(STAGES)
    if args.skip_docker and "docker" in stages:
        stages.remove("docker")

    log(f"[local_runner] start stages={','.join(stages)} dry_run={args.dry_run}")
    for stage in stages:
        for command, cwd in commands[stage]:
            if not run_command(command, cwd, args.dry_run):
                log(f"[local_runner] stage {stage} failed, stopping")
                return 1

    log("[local_runner] all stages passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
