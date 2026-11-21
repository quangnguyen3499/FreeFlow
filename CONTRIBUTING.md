# Contributing

Contributions are welcome. Please keep changes focused and include tests for
new behavior or bug fixes.

## Development setup

Prerequisites: Docker, Docker Compose v2, and Python 3.10+.

```bash
make sync
make up
make test
```

Run `make help` to see the available commands.

## Before submitting

1. Create a focused branch.
2. Add or update tests alongside implementation changes.
3. Run `make test` and `make lint`.
4. Keep generated directories such as `enabled_dags/` and `staging/` out of
   commits.
5. Open a pull request describing the problem, implementation, and validation.

## Coding conventions

- Keep DAG modules declarative and place task logic in task/helper modules.
- Use parameterized SQL and avoid hard-coded environment-specific values.
- Prefer small, pure functions for transforms and validators.
- Follow the formatting and lint settings in `pyproject.toml`.
- Do not commit credentials, database exports, or local runtime data.
