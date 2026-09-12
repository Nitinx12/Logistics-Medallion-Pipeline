# Contributing

How to work in this repository. The full agent facing rules live in
`AGENTS.md`; this page is the human friendly version of the same contract.
The full git workflow with diagrams and command reference lives in
`docs/GIT_WORKFLOW.md`.

## Branching and commits

- Never commit directly to `main`. Work on `feature/<short description>`,
  `fix/<short description>`, or `chore/<short description>`. See
  `docs/GIT_WORKFLOW.md` section 2 for the complete branching model and
  lifecycle diagram.
- Conventional commits: `type(scope): description`, for example
  `feat(bronze): add watermark upsert for mongo tracking events`. See
  `docs/GIT_WORKFLOW.md` section 3 for types and scope.
- Keep commits small and focused on one logical change; do not bundle an
  unrelated formatting pass into a feature commit.
- Prefer `git pull --rebase` over a plain `git pull`.
- Never force push a shared branch; on your own unshared branch use
  `--force-with-lease` only.
- Install the pre commit hook once per clone: `scripts/bash/setup_hooks.sh`
  or `scripts/powershell/setup_hooks.ps1`. It runs `ruff check` and a secret
  scan on every commit.

## Environment rules

- `uv` only. Never call `pip install` directly.
- Never edit `pyproject.toml` by hand without running the matching
  `uv add` / `uv remove` so the lockfile stays correct.
- Python is pinned to 3.13 through `pyproject.toml`; do not assume the
  system interpreter.

## Paired scripts rule

Every Bash script under `scripts/bash/` must have a matching PowerShell
script under `scripts/powershell/` with equivalent behavior and the same
summary output. A change to one without the other is a defect. (The
PowerShell side is currently the open gap tracked in
`docs/PROJECT_PLAN.md` phase 13; new scripts must not widen it.)

## Documentation rules

- No hyphens in prose anywhere in documentation. Hyphens are fine inside
  code, file names, CLI flags, and identifiers, never inside a sentence.
  Rewrite the sentence instead.
- `README.md` stays short; full detail belongs in `ARCHITECTURE.md` and
  the files under `docs/`.
- Every architecture diagram is Mermaid, checked into the relevant `.md`
  file, using the layer colors already established (bronze, silver, gold,
  source blues and greens, mart purple, Airflow blue).
- After any change to a dbt model, run `dbt docs generate` before
  committing, and update `docs/DATA_DICTIONARY.md` in the same pull
  request when columns or tests change.

## Style

- Python: `ruff` for linting and formatting, type hints on public
  functions, run through `uv run`.
- SQL: `sqlfluff`, dialect `postgres` for `sql/`, dialect `databricks` or
  `sparksql` for `dbt/models/`; prefer `::VARCHAR` style casts.
- Logging through `src/utils/logger.py` conventions: stage scoped dated
  files under `logs/`, no debug prints in production code paths.

## Testing expectations

- New or changed dbt model: not null and unique tests on its primary key,
  plus a relationships test on every foreign key.
- New PySpark logic: a pytest covering at minimum the watermark filtering
  and the merge or write condition.
- Never lower a data quality severity from `error` to `warn` to make a
  build pass. Fix the data, or record a known issue in
  `docs/DATA_QUALITY.md`.

## Pull requests

Every change goes through a pull request against `main` using the template
at `.github/pull_request_template.md`. The template checklist mirrors the
definition of done. CI at `.github/workflows/ci.yml` must be green before
merge. See `docs/GIT_WORKFLOW.md` section 5 for the full pull request
process and merge strategies.

## Definition of done

A change is complete when:

1. The lint suite and `uv run pytest` pass locally
   (`scripts/bash/run_all_tests.sh` covers the full gate, same as CI).
2. Any new dbt model has tests and appears in `dbt docs generate` output.
3. Any new Bash script has its PowerShell counterpart.
4. Documentation affected by the change is updated in the same pull
   request, including the data dictionary for schema changes.
5. No secret, token, or credential appears anywhere in the diff.
6. The pull request template is complete and CI is green.
