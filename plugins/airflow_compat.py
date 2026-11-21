"""Airflow compatibility module.

Ensures the project can run both in full Apache Airflow container environments
and in standalone local development/testing environments without external dependencies.
"""

from __future__ import annotations

import sys
from typing import Any, Callable, Dict, List, Optional
from datetime import datetime


def bootstrap_airflow_compat() -> None:
    """Bootstrap mock Airflow modules in sys.modules if real Airflow is not installed."""
    if "airflow" in sys.modules and getattr(sys.modules["airflow"], "__file__", None):
        return

    try:
        import airflow  # type: ignore # noqa: F401
        return
    except ImportError:
        pass

    import types

    # Root airflow module
    af = types.ModuleType("airflow")

    class AirflowException(Exception):
        """Base Airflow exception."""
        pass

    class AirflowFailException(AirflowException):
        """Exception that fails task without retry."""
        pass

    class AirflowSkipException(AirflowException):
        """Exception that skips task execution."""
        pass

    exceptions_mod = types.ModuleType("airflow.exceptions")
    exceptions_mod.AirflowException = AirflowException
    exceptions_mod.AirflowFailException = AirflowFailException
    exceptions_mod.AirflowSkipException = AirflowSkipException

    class BaseOperator:
        """Lightweight BaseOperator shim."""
        def __init__(
            self,
            task_id: str,
            default_args: Optional[Dict[str, Any]] = None,
            retries: int = 0,
            retry_delay: Any = None,
            on_failure_callback: Any = None,
            **kwargs: Any,
        ):
            self.task_id = task_id
            self.default_args = default_args or {}
            self.retries = retries
            self.upstream_list: List[BaseOperator] = []
            self.downstream_list: List[BaseOperator] = []
            self.dag: Optional[Any] = None

        def __rshift__(self, other: Any) -> Any:
            """Support self >> other."""
            if isinstance(other, BaseOperator):
                if other not in self.downstream_list:
                    self.downstream_list.append(other)
                if self not in other.upstream_list:
                    other.upstream_list.append(self)
                return other
            elif isinstance(other, list):
                for item in other:
                    self >> item
                return other
            return other

        def __lshift__(self, other: Any) -> Any:
            """Support self << other."""
            if isinstance(other, BaseOperator):
                other >> self
                return other
            elif isinstance(other, list):
                for item in other:
                    item >> self
                return other
            return other

        def execute(self, context: Dict[str, Any]) -> Any:
            raise NotImplementedError()

    class PythonOperator(BaseOperator):
        """Lightweight PythonOperator shim."""
        def __init__(self, *, task_id: str, python_callable: Callable[..., Any], op_kwargs: Optional[Dict[str, Any]] = None, **kwargs: Any):
            super().__init__(task_id=task_id, **kwargs)
            self.python_callable = python_callable
            self.op_kwargs = op_kwargs or {}

        def execute(self, context: Dict[str, Any]) -> Any:
            merged = {**self.op_kwargs, **context}
            return self.python_callable(**merged)

    class BranchPythonOperator(PythonOperator):
        """Lightweight BranchPythonOperator shim."""
        pass

    class ShortCircuitOperator(PythonOperator):
        """Lightweight ShortCircuitOperator shim."""
        pass

    class TriggerDagRunOperator(BaseOperator):
        """Lightweight TriggerDagRunOperator shim."""
        def __init__(self, *, task_id: str, trigger_dag_id: str, conf: Optional[Dict[str, Any]] = None, **kwargs: Any):
            super().__init__(task_id=task_id, **kwargs)
            self.trigger_dag_id = trigger_dag_id
            self.conf = conf or {}

    class DAG:
        """Lightweight DAG shim."""
        _active_dag_stack: List[DAG] = []

        def __init__(
            self,
            dag_id: str,
            default_args: Optional[Dict[str, Any]] = None,
            schedule_interval: Any = None,
            schedule: Any = None,
            start_date: Optional[datetime] = None,
            catchup: bool = False,
            max_active_runs: int = 1,
            doc_md: Optional[str] = None,
            tags: Optional[List[str]] = None,
            is_paused_upon_creation: bool = False,
            **kwargs: Any,
        ):
            self.dag_id = dag_id
            self.default_args = default_args or {}
            self.schedule_interval = schedule if schedule is not None else schedule_interval
            self.start_date = start_date
            self.catchup = catchup
            self.max_active_runs = max_active_runs
            self.doc_md = doc_md
            self.tags = tags or []
            self.tasks: Dict[str, BaseOperator] = {}
            self.is_paused = is_paused_upon_creation

        def add_task(self, task: BaseOperator) -> None:
            self.tasks[task.task_id] = task
            task.dag = self

        def get_task(self, task_id: str) -> BaseOperator:
            return self.tasks[task_id]

        def __enter__(self) -> DAG:
            DAG._active_dag_stack.append(self)
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            DAG._active_dag_stack.pop()

    # Hook base
    class Connection:
        """Lightweight Airflow Connection representation."""
        def __init__(
            self,
            conn_id: str,
            conn_type: str = "generic",
            host: Optional[str] = None,
            login: Optional[str] = None,
            password: Optional[str] = None,
            schema: Optional[str] = None,
            port: Optional[int] = None,
            extra: Optional[Dict[str, Any]] = None,
        ):
            self.conn_id = conn_id
            self.conn_type = conn_type
            self.host = host
            self.login = login
            self.password = password
            self.schema = schema
            self.port = port
            self.extra_dejson = extra or {}

    class BaseHook:
        """Lightweight BaseHook shim."""
        _connections_store: Dict[str, Connection] = {}

        def __init__(self, conn_id: Optional[str] = None):
            self.conn_id = conn_id

        @classmethod
        def get_connection(cls, conn_id: str) -> Connection:
            if conn_id in cls._connections_store:
                return cls._connections_store[conn_id]
            # Fallback mock connection
            return Connection(conn_id=conn_id, host="localhost")

        @classmethod
        def set_connection(cls, conn: Connection) -> None:
            cls._connections_store[conn.conn_id] = conn

    # Variable model
    class Variable:
        """Lightweight Variable shim."""
        _store: Dict[str, str] = {}

        @classmethod
        def get(cls, key: str, default_var: Any = None, deserialize_json: bool = False) -> Any:
            import json
            if key not in cls._store:
                if default_var is not None:
                    return default_var
                raise KeyError(f"Variable {key} does not exist")
            val = cls._store[key]
            if deserialize_json:
                return json.loads(val)
            return val

        @classmethod
        def set(cls, key: str, value: Any, serialize_json: bool = False) -> None:
            import json
            if serialize_json or not isinstance(value, str):
                cls._store[key] = json.dumps(value)
            else:
                cls._store[key] = str(value)

    # Attach to mock modules
    af.DAG = DAG
    af.AirflowException = AirflowException
    af.AirflowFailException = AirflowFailException
    af.AirflowSkipException = AirflowSkipException

    operators_mod = types.ModuleType("airflow.operators")
    python_ops_mod = types.ModuleType("airflow.operators.python")
    trigger_ops_mod = types.ModuleType("airflow.operators.trigger_dagrun")
    hooks_mod = types.ModuleType("airflow.hooks")
    base_hook_mod = types.ModuleType("airflow.hooks.base")
    models_mod = types.ModuleType("airflow.models")

    operators_mod.python = python_ops_mod
    operators_mod.trigger_dagrun = trigger_ops_mod

    python_ops_mod.PythonOperator = PythonOperator
    python_ops_mod.BranchPythonOperator = BranchPythonOperator
    python_ops_mod.ShortCircuitOperator = ShortCircuitOperator

    trigger_ops_mod.TriggerDagRunOperator = TriggerDagRunOperator

    hooks_mod.base = base_hook_mod
    base_hook_mod.BaseHook = BaseHook

    models_mod.Variable = Variable
    models_mod.Connection = Connection
    models_mod.DAG = DAG
    models_mod.BaseOperator = BaseOperator

    # Automatically register tasks created in DAG context
    original_base_op_init = BaseOperator.__init__

    def hooked_init(self: BaseOperator, task_id: str, *args: Any, **kwargs: Any) -> None:
        original_base_op_init(self, task_id, *args, **kwargs)
        if DAG._active_dag_stack:
            DAG._active_dag_stack[-1].add_task(self)

    BaseOperator.__init__ = hooked_init  # type: ignore

    # Register in sys.modules
    sys.modules["airflow"] = af
    sys.modules["airflow.exceptions"] = exceptions_mod
    sys.modules["airflow.operators"] = operators_mod
    sys.modules["airflow.operators.python"] = python_ops_mod
    sys.modules["airflow.operators.trigger_dagrun"] = trigger_ops_mod
    sys.modules["airflow.hooks"] = hooks_mod
    sys.modules["airflow.hooks.base"] = base_hook_mod
    sys.modules["airflow.models"] = models_mod


# Bootstrap on module import
bootstrap_airflow_compat()
