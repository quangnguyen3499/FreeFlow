# Security Policy

## Reporting a vulnerability

Do not open a public issue for a suspected security vulnerability.

Please report vulnerabilities privately through GitHub's vulnerability
reporting feature for this repository. Include a description, affected
component, reproduction steps, and any known impact.

We will acknowledge a valid report, assess it, and coordinate a fix and
disclosure timeline.

## Development security

- Never commit real credentials, API keys, or private partner data.
- Use `.env` for local secrets only; keep `.env.example` secret-free.
- Replace example passwords and credentials before deploying outside a local
  development environment.
