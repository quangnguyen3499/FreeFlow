<div align="center">

# FreeFlow

**A modular Apache Airflow template for resilient, testable finance pipelines**

[![Python](https://img.shields.io/badge/Python-3.10%20%7C%203.11-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Airflow-2.9.2-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-v2-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

[Quick start](#quick-start) · [Architecture](#architecture) · [Development](#development) · [Contributing](CONTRIBUTING.md)

</div>

## Overview

FreeFlow is a production-oriented Airflow starter that demonstrates how to
build observable data pipelines with isolated domain logic, manifest-based DAG
deployment, and an offline-friendly test harness.

The included partner billing example extracts records, validates and
quarantines bad rows, applies decimal-safe transformations, verifies invariants,
and publishes artifacts to SeaweedFS or another S3-compatible store.

## Features

- **Declarative trigger DAGs** separate scheduling and configuration from core
  processing behavior.
- **Manifest-based synchronization** copies only an active DAG and its explicit
  dependencies into `enabled_dags/`.
- **Three-stage reliability gates** validate records, transform them precisely,
  and re-read outputs before delivery.
- **Circuit breaker protection** pauses a pipeline and alerts operators when
  invariants fail.
- **Hermetic tests** cover DAG structure, tasks, SQL, hooks, transformations,
  and validators without requiring the full runtime stack.
- **Docker-first development** provides one image for local Airflow and tests.

## Quick start

### Prerequisites

- Docker with Docker Compose v2
- Python 3.10+ for host-side tools

### Start the stack

```bash
# Copy one active DAG and its manifest dependencies
make sync

# Build and start Airflow, PostgreSQL, mock API, and SeaweedFS
make up

# Initialize Airflow and local connections
make init
```

Open [http://localhost:8080](http://localhost:8080) and sign in with
`admin` / `admin`.

For automatic synchronization as DAG files change:

```bash
make watch
```

### Run tests

```bash
make test
```

Run source checks locally:

```bash
make lint
make format-check
```

## Services

| Service | URL | Default credentials |
|---|---|---|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| Mock partner API | http://localhost:8081 | Mock service |
| SeaweedFS S3 API | http://localhost:8333 | `seaweedadmin` / `seaweedadminpassword` |
| SeaweedFS Master UI | http://localhost:9333 | No authentication |
| SeaweedFS Filer UI | http://localhost:8888 | No authentication |
| PostgreSQL | `localhost:5432` | `airflow` / `airflow` |

Example credentials are for local development only. Replace them before
deploying outside this workspace.

## Architecture

```text
trigger DAG ── configuration ──> processing DAG
                                      │
                                      ├─ validation and quarantine
                                      ├─ decimal-safe transformation
                                      ├─ invariant verification
                                      └─ delivery
                                             │
                                             ├─> SeaweedFS / S3
                                             └─> database or partner API
```

```text
dags/           Airflow DAG packages and shared utilities
plugins/        Hooks, operators, sensors, and Airflow compatibility helpers
tools/          Sync, watcher, environment seeding, and mock services
tests/          Unit and integration tests
docker/         Container environment and local service configuration
enabled_dags/   Generated, active DAG deployment (ignored by Git)
staging/        Generated runtime artifacts (ignored by Git)
```

### Add a DAG

1. Create a package under `dags/<domain>/<use-case>/`.
2. Add a `manifest` file listing its package and shared dependencies.
3. Keep DAG files declarative; place execution logic in `tasks.py` or helpers.
4. Add matching tests under `tests/`.
5. Sync the package:

```bash
make sync DAG=dags/<domain>/<use-case>
```

## Common commands

```bash
make help
make watch
make sync
make up
make down
make init
make test
make lint
make format-check
make logs
make ps
make shell
make clean
```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting a pull
request. For security issues, follow [SECURITY.md](SECURITY.md).

## License

FreeFlow is available under the [MIT License](LICENSE).
