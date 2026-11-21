# Airflow Production ETL Playbook

A reference architecture, design patterns, and engineering standards for building scalable, maintainable, and resilient Apache Airflow ETL pipelines.

---

## 1. Core Principles

- **One Image Everywhere:** Bake DAGs, plugins, and dependencies into a single Docker image. Local development, CI/CD, and production run identical images.
- **Run Everything in Containers:** Build, test, run, and debug inside containerized environments.
- **Absolute Imports from Repo Root:** Use `dags.<path>.<module>` and `plugins.<path>.<module>`. Never mix relative or partial import paths.
- **Thin DAGs, Thick Helpers:** `dag.py` only defines task wiring and DAG metadata. All business logic, queries, and transformations belong in `tasks.py`, `utils.py`, and hooks.
- **Decouple "When to Run" from "What to Run":** Processing DAGs set `schedule=None` and accept execution parameters via `conf`. Schedules are managed by a separate trigger layer.
- **Explicit Failures with Traceability:** Never fail silently. Attach identifiers to every error, isolate record failures, and broadcast structured alerts.
- **Financial Precision:** Always use `Decimal` with `ROUND_HALF_UP` for currency, tax, and fee calculations. Never use binary `float` or `round()`.

---

## 2. Repository Structure

```
repo/
├── dags/
│   ├── common_utils/          # Shared repo-wide utilities (airflow, date, format, notify, file)
│   ├── <domain>/
│   │   ├── shared/            # Domain-specific helpers (transforms, constants, validators)
│   │   ├── <subdomain>/
│   │   │   └── <dag_name>/    # One directory per DAG package
│   │   └── operations/        # Health checks, maintenance, and config-driven triggers
├── plugins/
│   ├── hooks/                 # Service connectors (BaseHook + pure Python clients)
│   ├── operators/             # Reusable custom operators (circuit breaker, storage, short-circuit)
│   └── sensors/               # Custom polling sensors
├── tests/                     # Path-for-path mirror of dags/ and plugins/
│   ├── __init__.py            # DagTestBase harness for execution without live DB
│   └── sample_files/          # Sanitized mock test fixtures
├── docker/
│   ├── envs/                  # Non-sensitive connections & variables for local dev
│   └── artifacts/             # requirements.txt, pytest.ini
└── tools/                     # Developer tooling (sync.py, watcher.py, mock_server.py)
```

---

## 3. DAG Package Anatomy

Each DAG package in `dags/<domain>/<subdomain>/<dag_name>/` contains:

```
├── dag.py        # DAG declaration & task wiring
├── tasks.py      # Factory functions returning operators with nested callables
├── utils.py      # Hook factories with @lru_cache() + pure business helpers
├── sql.py        # SQL queries with CTEs, bind parameters, and named constants
├── README.md     # Rendered as dag.doc_md on Airflow Web UI
└── manifest      # Dependency list for local sync and watcher
```

### 3.1 `dag.py`
Keep wiring concise:
```python
import os
from datetime import datetime
from airflow import DAG
import dags.<domain>.<subdomain>.<dag_name>.tasks as tasks

dag_id = "<domain>_<action>_<object>"

dag = DAG(
    dag_id=dag_id,
    default_args={"depends_on_past": False, "owner": "<domain>"},
    schedule=None,  # Driven by operations triggers or manual API conf
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
)

globals()[dag_id] = dag

with dag:
    setup = tasks.setup_task(dag_id)
    work = tasks.process_task()
    setup >> work

readme_path = os.path.join(os.path.dirname(__file__), "README.md")
if os.path.exists(readme_path):
    dag.doc_md = open(readme_path).read()
```

### 3.2 `tasks.py` — The `setup` Task Pattern
The first task in every DAG must be **`setup`**:
- Consolidates all execution inputs (`conf`, variables, connection IDs, temp paths, notification channels) into a single dictionary (`setup_value`).
- Passes `setup_value` via XCom.
- Downstream tasks read exclusively from `setup_value`.
- **Per-record isolation:** In batch processing, wrap record processing in `try/except`, collect failures in `RunSummary`, and raise an aggregated exception at the end.

### 3.3 `utils.py` — Cached Hook Factories
```python
@lru_cache()
def get_db_hook(conn_id="main_db"):
    return DatabaseHook(conn_id)

@lru_cache()
def get_service_client(conn_id="service_gateway"):
    return ServiceHook(conn_id).client
```
- Wrap hook instantiation in `@lru_cache()` to reuse connections per task execution.
- Expose all connection ID parameters with standard defaults.

---

## 4. Multi-Gate Reliability Pattern

To prevent corrupted or incomplete data from reaching downstream partners or systems:

```
Select Records -> Gate 1 (Validation) -> Gate 2 (Transform) -> Gate 3 (Invariant Verification) -> Deliver -> Finalize
```

1. **Gate 1 (Record Validation):** Validate schema, formats, and business rules per record. Isolate bad records into `RunSummary`; proceed with valid records.
2. **Gate 2 (Transformation):** Perform exact financial arithmetic with `Decimal(ROUND_HALF_UP)`. Generate staging artifacts.
3. **Gate 3 (Invariant Verification & Circuit Breaker):** Re-read generated files from disk and verify aggregate invariants (item sum = declared batch total, count = declared record count). If breached:
   - Trip **Circuit Breaker** (`disable_dag(dag_id)`).
   - Dispatch `CRITICAL` alerts to team channels.
   - Fail task immediately without retries (`AirflowFailException`).
4. **Deliver & Finalize:** Transmit verified batch and update status in database.

---

## 5. Connectors (Hook + Client Separation)

- **Hook:** Subclasses `BaseHook`, reads Airflow Connection (host, login, password, extra), and instantiates client.
- **Client:** Pure Python class handling HTTP verbs, timeouts, and retries (`tenacity`). Usable and testable outside of Airflow.
- **Service Gateway:** Route internal microservices through a single gateway connection using custom headers (`X-Target-Service`).

---

## 6. Testing Strategy

- `tests/` mirrors `dags/` path-for-path.
- Use `DagTestBase` for mock context, XCom tracking, and assertion helpers (`assert_execute_task_successfully`).
- Mock at the getter layer (`@patch("...tasks.utils.get_db_hook")`), never mock low-level HTTP or database drivers.
- Fixtures in `tests/sample_files/` must contain only sanitized mock data.

---

## 7. Local Dev & Manifest Sync

- Each DAG directory provides a `manifest` listing itself and its dependencies.
- Tools `./sync` and `./watcher` read manifests to populate `enabled_dags/`.
- Airflow containers mount `./enabled_dags/dags` to keep startup fast and eliminate noise.
