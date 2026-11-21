"""Database Hook wrapper with fetch_all, transaction handling, and execution logging."""

from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional, Sequence, Union
from airflow.hooks.base import BaseHook  # type: ignore

logger = logging.getLogger(__name__)


class DatabaseHook(BaseHook):
    """Wrapper around Airflow DB hooks (Postgres / MySQL / SQLite) providing uniform fetch_all."""

    conn_name_attr = "conn_id"
    default_conn_name = "main_db"
    conn_type = "database"
    hook_name = "Unified Database Hook"

    def __init__(self, conn_id: str = "main_db"):
        super().__init__()
        self.conn_id = conn_id
        self._connection = None

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.get_connection(self.conn_id)
        return self._connection

    def get_conn(self) -> Any:
        """Obtain raw DB-API connection object."""
        conn_info = self.connection
        # If postgres / mysql provider is available in container
        if conn_info.conn_type == "postgres":
            try:
                from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore
                return PostgresHook(postgres_conn_id=self.conn_id).get_conn()
            except ImportError:
                pass

        # Local fallback using SQLite (stored in staging or file or in-memory)
        db_path = getattr(conn_info, "schema", None) or getattr(conn_info, "host", None) or ":memory:"
        if not db_path.endswith(".db") and db_path != ":memory:":
            db_path = f"{db_path}.db"
        return sqlite3.connect(db_path)

    def fetch_all(self, sql: str, parameters: Optional[Union[Dict[str, Any], Sequence[Any]]] = None) -> List[Dict[str, Any]]:
        """Execute a SELECT query and return list of dictionaries."""
        logger.info("Executing query on conn '%s':\n%s", self.conn_id, sql.strip())
        conn = self.get_conn()
        try:
            cursor = conn.cursor()
            if parameters:
                cursor.execute(sql, parameters)
            else:
                cursor.execute(sql)

            columns = [col[0] for col in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            conn.close()

    def execute(self, sql: str, parameters: Optional[Union[Dict[str, Any], Sequence[Any]]] = None) -> int:
        """Execute an INSERT, UPDATE, or DELETE query and commit transaction."""
        logger.info("Executing write statement on conn '%s':\n%s", self.conn_id, sql.strip())
        conn = self.get_conn()
        try:
            cursor = conn.cursor()
            if parameters:
                cursor.execute(sql, parameters)
            else:
                cursor.execute(sql)
            rowcount = cursor.rowcount
            conn.commit()
            return rowcount
        finally:
            conn.close()
