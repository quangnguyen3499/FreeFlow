"""Airflow plugins root module."""

# Ensure Airflow compatibility shim is loaded if Airflow is not installed in the current environment
try:
    import airflow  # noqa: F401
except ImportError:
    import plugins.airflow_compat  # noqa: F401
