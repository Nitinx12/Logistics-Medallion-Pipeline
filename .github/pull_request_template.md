<!--
Thank you for contributing to FreightLake.
Fill every section. Delete the guidance in italics before requesting review.
See docs/GIT_WORKFLOW.md for the full workflow and AGENTS.md for agent rules.
-->

## Summary

_One or two sentences on what this change does and why it is needed._

Closes # <!-- issue number if any -->

## Type of change

- [ ] `feat` new capability
- [ ] `fix` bug repair
- [ ] `chore` tooling or config
- [ ] `docs` docs only
- [ ] `refactor` no capability or repair
- [ ] `test` tests only
- [ ] `perf` performance

## Scope

_Area touched: bronze, silver, gold, mart, airflow, docker, dbt, gx, tests, deps, docs, scripts_

Scope: <!-- e.g. silver, gold, airflow -->

## Changes

* <!-- bullet per meaningful change -->
* <!-- keep focused on one topic per PR -->

## How to test

```bash
uv run ruff check .
uv run mypy .
uv run pytest
uv run python gx/run_validations.py --demo
# for dbt changes:
cd dbt && uv run dbt build --profiles-dir .
```

- [ ] Added or updated tests
- [ ] Updated dbt tests for any new or changed model
- [ ] Ran `dbt docs generate` if dbt models changed

## Checklist

- [ ] Branch is named `feature/...`, `fix/...`, `chore/...`, `docs/...` or `hotfix/...`
- [ ] Commits follow `type(scope): description` conventional commits
- [ ] Branch is current with `origin/main` (`git fetch && git rebase origin/main` or merge if shared)
- [ ] `uv sync` passes and `pyproject.toml` / `uv.lock` are in sync if deps changed
- [ ] New Bash script has matching PowerShell script under `scripts/powershell/` with same summary output
- [ ] Docs updated in same PR (`docs/` or `ARCHITECTURE.md` or `docs/DATA_DICTIONARY.md`)
- [ ] No secret, token, or populated `.env` in diff (checked `git diff --staged`)
- [ ] CI is green or failures are explained below

## Screenshots or logs

_If relevant, paste CI log excerpt or local run output._

## Breaking change

- [ ] Yes, flagged below and in `docs/CHANGELOG.md`
- [ ] No

_Details if yes:_
