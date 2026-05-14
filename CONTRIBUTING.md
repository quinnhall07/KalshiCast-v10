# Contributing to KalshiCast

Thanks for your interest in contributing! KalshiCast is a quantitative weather-contract
pricing and Kalshi betting pipeline. Because this project handles real financial data and
can execute live trades, code quality, reproducibility, and security are top priorities.

## Development setup

Requires Python 3.11+.

```bash
git clone https://github.com/quinnhall07/Kalshicast-v10.git
cd Kalshicast-v10
python -m venv .venv
source .venv/bin/activate           # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pre-commit install
```

Copy `.env.example` to `.env` and fill in your Oracle DB credentials and Kalshi API keys.
**Never commit `.env` or any wallet files.**

## Running tests

```bash
pytest                              # all tests
pytest -m "not integration"         # skip DB/network tests
pytest kalshicast/tests/test_pricing.py -v
pytest --cov=kalshicast --cov-report=term-missing
```

Integration tests are marked with `@pytest.mark.integration` and require live DB
or Kalshi API access. Slow tests (>1s) are marked `@pytest.mark.slow`.

## Branch naming

Use short, descriptive prefixes:

- `feat/<topic>` — new feature
- `fix/<topic>` — bug fix
- `chore/<topic>` — tooling, refactor, dependency bumps
- `docs/<topic>` — documentation only
- `test/<topic>` — test-only changes

Example: `fix/market-open-bin-edges`, `feat/mixture-of-normals`.

## Commit messages

We use [Conventional Commits](https://www.conventionalcommits.org/). The history
already follows this pattern:

```
feat(pricing): §6.5 mixture-of-normals, renormalize instead of skip
fix(pipeline): audit market_open — 6 silent bugs
chore: Restore dataset and stations configuration
```

Common types: `feat`, `fix`, `chore`, `docs`, `test`, `refactor`, `perf`, `build`, `ci`.
Scope (in parentheses) is optional but encouraged — typical scopes match top-level
packages: `pricing`, `pipeline`, `ml_v1`, `db`, `execution`, `collection`.

Keep the subject line under 72 characters. Reference issues in the body: `Fixes #42`.

## Code style

- **Formatter & linter:** [ruff](https://docs.astral.sh/ruff/) — both `ruff format` and
  `ruff check`. Configuration lives in `pyproject.toml`. Line length is 120.
- **Type hints** are encouraged on new public functions but not enforced.
- **Pre-commit:** runs ruff plus basic hygiene checks (trailing whitespace,
  yaml/toml validity, large-file guard) on every commit. CI re-runs the same
  hooks; passing locally avoids round-trips.

## Filing issues

Open issues at https://github.com/quinnhall07/Kalshicast-v10/issues. For bug reports
please include:

1. KalshiCast version (`python -c "import kalshicast; print(kalshicast.__version__)"`)
2. Python version and OS
3. Command you ran and full traceback
4. Whether you're in `--live` mode or paper-trading
5. Minimal reproduction if possible

**Do not file public issues for security vulnerabilities.** Use GitHub's private
vulnerability reporting — see [SECURITY.md](SECURITY.md).

## Pull requests

1. Fork and create a topic branch from `main`.
2. Add tests for any new behavior. Bug fixes should include a regression test.
3. Run `pre-commit run --all-files` and `pytest` locally.
4. Update `CHANGELOG.md` under `[Unreleased]` with a one-line summary.
5. Open a PR with a clear description of the change and any trading/financial impact.

PRs that change pricing logic, market-execution logic, or the ML model pipeline will
get extra scrutiny — please include backtest deltas or comparison metrics where
applicable.

## Conduct

Be respectful and constructive in issues, PRs, and reviews. Disagreements
about technical direction are welcome; personal attacks are not.
