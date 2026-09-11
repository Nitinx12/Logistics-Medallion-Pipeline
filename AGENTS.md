# AGENTS.md — FreightLake

Instructions for any AI coding agent (Claude Code, Codex CLI, or similar)
working in this repository. Read this before touching any file. See
`PROJECT_PLAN.md` for the full architecture and roadmap.

---

## 1. Project snapshot

FreightLake is a logistics ETL platform: Postgres and MongoDB as simulated
source systems, a bronze/silver/gold medallion built on Databricks with
PySpark and dbt, orchestrated by Airflow, and published to a Postgres serving
mart. Everything is containerized and automated through a Makefile plus
paired Bash and PowerShell scripts.

---

## 2. Environment rules

- `uv` only. Never call `pip install` directly, never edit `pyproject.toml`
  by hand without running the matching `uv add` / `uv remove` command
  afterward to keep the lock file correct.
- One virtual environment per project, managed entirely through `uv sync`.
- Pin the Python version in `pyproject.toml`; do not assume the system
  interpreter.
- Any new Python dependency must be added through `uv add <package>`, never
  appended manually to a requirements file.

---

## 3. Git hygiene

- Never commit directly to `main`. Work on a branch named `feature/<short
  description>`, `fix/<short description>`, or `chore/<short description>`.
- Commit messages follow conventional commits: `type(scope): description`,
  for example `feat(bronze): add watermark upsert for mongo tracking events`.
- Keep commits small and focused on one logical change. Do not bundle an
  unrelated formatting pass into a feature commit.
- Never force push a shared branch.
- Never commit `.env`, credentials, Databricks tokens, or any file matching
  the patterns in `.gitignore`. If a secret is accidentally staged, unstage
  it and confirm `.gitignore` covers it before committing anything else.

### 3.1 Common commands

Run these from the project root.

| Action | Command |
|---|---|
| Check status | `git status` |
| Stage a file | `git add <path>` |
| Stage everything tracked and changed | `git add -u` |
| Stage absolutely everything, including new files | `git add .` |
| Commit staged changes | `git commit -m "type(scope): description"` |
| Amend the last commit, only before it is pushed | `git commit --amend` |
| Create and switch to a new branch | `git checkout -b feature/<short description>` |
| Switch to an existing branch | `git checkout <branch>` |
| Push a new branch upstream for the first time | `git push -u origin feature/<short description>` |
| Push subsequent commits | `git push` |
| Pull the latest changes, rebasing your local commits on top | `git pull --rebase` |
| Fetch remote changes without merging | `git fetch origin` |
| Merge main into your feature branch to stay current | `git merge origin/main` |
| Rebase your branch onto main instead of merging | `git rebase origin/main` |
| Merge your branch into main after review, typically done via a pull request instead of locally | `git merge --no-ff feature/<short description>` |
| Clean up commit history before opening a pull request | `git rebase -i origin/main` |
| View commit history | `git log --oneline --graph --decorate` |
| See unstaged changes | `git diff` |
| See staged changes | `git diff --staged` |
| Stash work in progress | `git stash` |
| Restore the most recent stash | `git stash pop` |
| Undo a staged file, keep the edit | `git restore --staged <path>` |
| Discard local changes to a file | `git restore <path>` |
| Tag a release | `git tag -a v0.1.0 -m "message"` |
| Push tags | `git push --tags` |

Notes:

- Prefer `git pull --rebase` over a plain `git pull` so your own branch does
  not fill up with unnecessary merge commits.
- Rebase only a branch nobody else has pulled. Once a branch is shared,
  merge instead of rebasing it, otherwise you rewrite history out from under
  a collaborator.
- Never run `git push --force` on `main` or any shared branch. On your own
  unshared branch, use `git push --force-with-lease`, never a plain
  `--force`, and only after confirming you meant to rewrite that history.
- Delete a branch locally with `git branch -d <branch>` and remotely with
  `git push origin --delete <branch>` once it is merged, so stale branches do
  not pile up.

---

## 4. Documentation rules

- No hyphens in prose anywhere in documentation. Hyphens are fine inside code,
  file names, CLI flags, and identifiers (`docker-compose`, `feature/x`), but
  never inside a sentence describing something. Rewrite the sentence instead
  of reaching for a hyphenated compound adjective.
- `README.md` stays short. Full detail belongs in `ARCHITECTURE.md`,
  `PROJECT_PLAN.md`, or `docs/`.
- Every architecture diagram is Mermaid, checked into the relevant `.md`
  file, not an external image unless there is a specific reason a diagram
  cannot be expressed in Mermaid.
- After any change to a dbt model, run `dbt docs generate` again before
  committing, do not let the lineage graph and docs site drift from the
  actual models.

---

## 5. Code style

- SQL: lint with `sqlfluff`, dialect `postgres` for OLTP and mart SQL,
  dialect `databricks` (or `sparksql`) for dbt models. Prefer the
  `::VARCHAR` style cast over `CAST(x AS VARCHAR)` to match the existing
  convention.
- Python: `ruff` for linting and formatting, `mypy` for type checking, both
  run through `uv run`. Every module gets type hints on public functions.
- Bash and PowerShell: every Bash script under `scripts/bash/` must have a
  matching PowerShell script under `scripts/powershell/` with equivalent
  behavior. Do not add one without the other. Shared logging output should
  look and behave the same on both.
- Logging: centralized, stage scoped log files, matching the existing
  `logger.py` convention from prior projects. Do not print debug output
  directly to stdout in production code paths; log it instead.

---

## 6. Testing and quality gates

- Any new or changed dbt model needs at least a not null and unique test on
  its primary key, plus a relationships test wherever a foreign key exists.
- New PySpark logic needs a pytest covering at minimum the watermark
  filtering logic and the upsert merge condition.
- Run `make lint` and `make test` locally before opening a pull request; both
  also run in CI and a failing check blocks merge.
- Do not lower a data quality test's severity from `error` to `warn` to make
  a build pass. Fix the underlying data or model instead, or raise it as a
  known issue in `docs/data_dictionary.md` if it reflects a genuine upstream
  gap.

---

## 7. Secrets and safety

- All credentials come from `.env`, loaded through environment variables.
  `.env.example` documents every required variable with a placeholder value,
  never a real one.
- `dbt/profiles.yml` reads Databricks credentials from environment variables
  only. Never hardcode a host, token, or HTTP path in that file.
- Do not add a new external service call, API key, or telemetry hook without
  flagging it clearly in the pull request description.

---

## 8. Docker and Airflow notes

- Airflow 3.x requires the `dag-processor` service in `docker/compose.yml`;
  do not remove it when refactoring the compose file.
- Check for port collisions with other local projects before changing any
  published port, particularly 8080, which has collided with other local
  Airflow or Docker Compose based projects before.
- The `docker/` folder stays a sibling of `airflow/`, `dbt/`, and
  `spark_jobs/` at the project root; the compose build context is `..`
  because `compose.yml` lives inside `docker/`.

---

## 9. Definition of done

A change is complete when:

1. `make lint` and `make test` pass locally.
2. Any new dbt model has tests and appears in `dbt docs generate` output.
3. Any new Bash script has its PowerShell counterpart.
4. Documentation affected by the change is updated in the same pull request,
   not deferred to a follow up.
5. No secret, token, or credential appears anywhere in the diff.

---

## 10. Never do this

- Never bypass `uv` for dependency management.
- Never commit directly to `main`.
- Never commit a real secret or a populated `.env` file.
- Never add a Bash script without its PowerShell equivalent, or the reverse.
- Never silence a failing test instead of fixing the root cause.
- Never hand write something `dbt docs generate` should have produced.