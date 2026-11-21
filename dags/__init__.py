"""Airflow DAGs root module."""

try:
    import airflow  # noqa: F401
except ImportError:
    import plugins.airflow_compat  # noqa: F401
