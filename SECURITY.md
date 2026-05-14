# Security Policy

## Reporting a vulnerability

If you discover a security vulnerability in KalshiCast, please report it
**privately** — do not open a public GitHub issue.

Use GitHub's built-in private vulnerability reporting:
<https://github.com/quinnhall07/Kalshicast-v10/security/advisories/new>

Please include:

1. A description of the vulnerability and the affected component
   (collection / pricing / execution / DB / CI workflow / dependency).
2. Steps to reproduce or a minimal proof of concept.
3. The version / commit you tested against.
4. Any suggested mitigation.

We will acknowledge your report within **5 business days** and aim to provide
a remediation plan within **30 days** for confirmed issues. Critical issues
affecting live execution or credentials handling are prioritized.

## Scope

In scope:

- Code in this repository (`kalshicast/`, `scripts/`, `.github/workflows/`).
- Default Oracle / Kalshi credential handling and config loading.
- Pre-commit, CI, and release tooling shipped from this repo.

Out of scope:

- Vulnerabilities in upstream dependencies — please report those to the
  respective project. Where possible we will pin or patch around them.
- Issues in third-party APIs (Kalshi, Oracle, NWS, weather providers).
- Operational issues with your own deployment that do not stem from the code.

## Supported versions

Only the `main` branch and the most recent tagged release receive security
fixes. Older versions are best-effort.

## Acknowledgments

Reporters who follow responsible disclosure will be credited in `CHANGELOG.md`
under the corresponding release, unless they request otherwise.
