# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Repo hygiene scaffolding: README, CONTRIBUTING, SECURITY, LICENSE (MIT),
  CHANGELOG, .editorconfig, .pre-commit-config, .mailmap.
- GitHub plumbing: PR template, issue templates, dependabot, CODEOWNERS,
  least-privilege `permissions:` blocks on all workflows.
- CI: `.github/workflows/test.yml` runs pytest and ruff on every PR.
- `kalshicast/logging_config.py`: centralized `setup_logging()` (used by
  pipeline orchestrators and the `__main__` CLI dispatcher).
- `kalshicast/tests/conftest.py`: shared fixtures (mock Kalshi API, sample
  stations/observations/shadow book/orderbook, logging silencer).
- `docs/QUICKSTART.md` and `docs/CLI.md`; the design spec was moved from
  the repo root to `docs/ARCHITECTURE.md`.
- Module + function docstrings on `pipeline/morning.py`, `pricing/shadow_book.py`,
  `ml_v1/train.py`.
- `[tool.ruff]` and `[tool.pytest.ini_options]` config in `pyproject.toml`.

### Changed
- Deduplicated weather-collector helpers (`_ensure_time_hour_z`, `_to_float`,
  `_get_with_retries`, `_is_retryable_exc`, `_parse_timeout_from_env`) into
  `kalshicast/collection/collectors/base.py`. No behavior change.
- Batched DB commits in the morning pipeline (was one commit per task).
- Cached `compute_lead_hours()` per group in `market_open` (was recomputed
  per candidate).
- Removed unused `DEBUG_DUMP` / `DEBUG_SOURCE` / `DEBUG_STATION` env knobs
  (documented but not consumed by any code path) in favour of
  `KALSHICAST_LOG_LEVEL` honored by `setup_logging()`.
- Replaced silent `except Exception: pass` blocks with logged exceptions in
  `pipeline/{morning,night,market_open}`, `db/schema`, `execution/orders`,
  `collection/kalshi_markets`.
- `__main__.py`: replaced 4× duplicated `logging.basicConfig` calls with the
  shared `setup_logging()` helper.
- Removed dead/unused imports across `pipeline/`, `processing/`, `execution/`,
  `evaluation/`, `collection/`, `pricing/`, `db/` and the test suite.

### Security
- Added explicit `permissions: contents: read` to every workflow.
- `.mailmap` aliases personal emails in git history to GitHub no-reply
  addresses (non-destructive; users must edit before public release).

## [10.0.0] - 2026-05-13

### Added
- Stateless live prediction inference engine.
- Mathematical matrices and optimal blend configuration shipped with the
  package for upstream CI inference tracking.
- Custom Bracket Loss blending with n=6 walk-forward CV and anti-collapse L2
  regularization for the ML pipeline.

### Changed
- Mixture-of-normals pricing (§6.5): renormalize across bins instead of
  skipping out-of-range mass.
- Widened Kalshi bin boundaries by 0.5°F to close dead zones at half-integer
  cutoffs.
- Bimodal sigma capped; phantom skewness edges skipped; market-hours gate
  added to market-open execution.
- Per-station Kalshi series config; pipeline diagnostics added.

### Fixed
- `pipeline/market_open` audit closed 6 silent bugs.
- Half-integer Kalshi bin handling.
- Oracle `DATE` / `TIMESTAMP` bind conversions.
- ML pipeline hardening across the board.
