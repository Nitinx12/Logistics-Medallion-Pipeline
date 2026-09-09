# Contributing to FreightLake

Read this before touching any file. These rules come directly from `AGENTS.md`.

---

## Environment Rules

- Use `uv` **only**. Never run `pip install` directly.
- Never edit `pyproject.toml` dependency lists by hand without running the matching `uv add` or `uv remove` afterward.
- Python version is pinned in `.python-version` and `pyproject.toml` (`>=3.13`). Do not assume the system interpreter.

**Adding a dependency:**
```bash
uv add <package>         # runtime dependency
uv add --dev <package>   # dev/lint/test dependency
```

---

## Branching Strategy

Never commit directly to `main`. Create a branch:

```bash
git checkout -b feature/<short-description>   # new feature
git checkout -b fix/<short-description>        # bug fix
git checkout -b chore/<short-description>      # maintenance
```

---

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
type(scope): short description

Examples:
feat(bronze): add watermark upsert for mongo tracking events
fix(silver): trim whitespace in stg_customers customer_id
chore(ci): add sqlfluff to python-ci workflow
docs(readme): update quick start with --skip-docker flag
```

Keep commits small and focused. Do not bundle an unrelated formatting pass into a feature commit.

---

## Paired Scripts Rule

Every Bash script under `scripts/bash/` must have a matching PowerShell script under `scripts/powershell/` with equivalent behavior. Do not add one without the other.

Current pairs:
- `scripts/bash/setup.sh` ↔ `scripts/powershell/setup.ps1`
- `scripts/bash/seed_data.sh` ↔ `scripts/powershell/seed_data.ps1`
- `scripts/bash/run_pipeline.sh` ↔ `scripts/powershell/run_pipeline.ps1`

---

## Code Style

**Python** (`spark_jobs/`, `scripts/`, `tests/`):
- `ruff` for linting and formatting
- `mypy` for type checking
- All public functions must have type hints
- Use `spark_jobs/utils/logger.py` (`get_logger(__name__, "stage")`) for logging — never `print()` in production code paths

**SQL:**
- `sqlfluff` with dialect `postgres` for OLTP and mart SQL
- `sqlfluff` with dialect `databricks` for dbt models and Databricks SQL
- Use `::VARCHAR` style casts, not `CAST(x AS VARCHAR)`

**Documentation:**
- No hyphens inside prose sentences. Hyphens are fine in code, file names, CLI flags, and identifiers.
- All architecture diagrams are Mermaid, checked into `.md` files.
- `README.md` stays short. Detail belongs in `docs/`.

---

## dbt Model Requirements

Any new or changed dbt model needs:
1. `not_null` and `unique` tests on its primary key in `schema.yml`
2. `relationships` tests on any foreign key column
3. Appears correctly in `dbt docs generate` output (run `make dbt-docs` to verify)

Never lower a test severity from `error` to `warn` to make a build pass.

---

## Definition of Done

A PR is ready to merge when **all** of these are true:

- [ ] `make lint` passes locally
- [ ] `make test` passes locally
- [ ] New dbt models have PK and FK tests in `schema.yml`
- [ ] New Bash scripts have PowerShell counterparts
- [ ] Affected documentation updated in the same PR
- [ ] No secret, token, or credential in the diff
- [ ] All four GitHub Actions workflows pass on the PR

---

## Git Hygiene

```bash
# Before pushing
git pull --rebase          # prefer rebase over merge for local branches
make ci                    # full lint + test locally

# Never do this on shared branches
git push --force           # use --force-with-lease on your own unshared branches only
```

Delete branches after merge:
```bash
git branch -d feature/<name>              # local
git push origin --delete feature/<name>   # remote
```
