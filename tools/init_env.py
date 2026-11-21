#!/usr/bin/env python3
"""Environment initialization script.

Loads connections and variables from docker/envs/ into Airflow or local state,
and seeds the local database schema and sample transaction data for testing.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("init_env")


def load_json_file(path: str) -> dict:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def init_airflow_models():
    """Load connections and variables into Airflow models."""
    conns = load_json_file("docker/envs/connections.json")
    vars_data = load_json_file("docker/envs/variables.json")

    # 1. Attempt to load into real Airflow DB if Airflow is installed and DB is migrated
    db_success = False
    try:
        from airflow.models import Connection, Variable  # type: ignore
        from airflow.utils.session import provide_session  # type: ignore

        # Test if Variable table is queryable
        @provide_session
        def _check_tables(session=None):
            session.execute("SELECT 1 FROM variable LIMIT 1")
        _check_tables()

        # Load Variables
        for k, v in vars_data.items():
            val = json.dumps(v) if isinstance(v, (dict, list)) else str(v)
            Variable.set(k, val)
            logger.info("Loaded Airflow Variable: %s", k)

        # Load Connections
        @provide_session
        def _save_conns(session=None):
            for conn_id, c in conns.items():
                existing = session.query(Connection).filter(Connection.conn_id == conn_id).first()
                if not existing:
                    new_c = Connection(
                        conn_id=conn_id,
                        conn_type=c.get("conn_type", "generic"),
                        host=c.get("host"),
                        schema=c.get("schema"),
                        login=c.get("login"),
                        password=c.get("password"),
                        port=c.get("port"),
                        extra=json.dumps(c.get("extra", {})),
                    )
                    session.add(new_c)
                    logger.info("Loaded Airflow Connection: %s", conn_id)
            session.commit()

        _save_conns()
        db_success = True
        logger.info("Successfully loaded variables and connections into Airflow DB.")
    except Exception as exc:
        logger.warning("Could not persist models into active Airflow DB (%s). Using fallback cache.", exc)

    # 2. If DB was not accessible, store in runtime cache / compat hook
    if not db_success:
        try:
            from plugins.airflow_compat import bootstrap_airflow_compat
            bootstrap_airflow_compat()
            from airflow.hooks.base import BaseHook  # type: ignore
            from airflow.models import Connection, Variable  # type: ignore
            for k, v in vars_data.items():
                try:
                    Variable.set(k, v)
                except Exception:
                    pass
            for conn_id, c in conns.items():
                conn_obj = Connection(
                    conn_id=conn_id,
                    conn_type=c.get("conn_type", "generic"),
                    host=c.get("host"),
                    schema=c.get("schema"),
                    login=c.get("login"),
                    password=c.get("password"),
                    extra=c.get("extra", {}),
                )
                BaseHook.set_connection(conn_obj)
        except Exception as fallback_exc:
            logger.warning("Fallback cache initialization warning: %s", fallback_exc)


def seed_local_database(db_path: str = "staging/main_dev.db"):
    """Create local SQLite table and seed initial dummy records for end-to-end testing."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS partner_transactions (
        id VARCHAR(64) PRIMARY KEY,
        partner_code VARCHAR(32) NOT NULL,
        transaction_date VARCHAR(10) NOT NULL,
        amount DECIMAL(15, 2) NOT NULL,
        currency VARCHAR(3) NOT NULL,
        customer_ref VARCHAR(64),
        status VARCHAR(20) NOT NULL,
        batch_id VARCHAR(64),
        updated_at VARCHAR(30)
    );
    """)

    # Seed sample records if table is empty
    cur.execute("SELECT COUNT(*) FROM partner_transactions;")
    if cur.fetchone()[0] == 0:
        sample_rows = [
            ("TXN_001", "ALPHA_LOGISTICS", "2026-11-10", "1500.00", "USD", "CUST_A", "PENDING", None, None),
            ("TXN_002", "ALPHA_LOGISTICS", "2026-11-12", "2750.00", "USD", "CUST_B", "PENDING", None, None),
            ("TXN_003", "ALPHA_LOGISTICS", "2026-11-14", "5000.00", "USD", "CUST_C", "PENDING", None, None),
            ("TXN_004", "ALPHA_LOGISTICS", "2026-11-15", "1200.00", "USD", "CUST_D", "PENDING", None, None),
            ("TXN_005", "BETA_PAYMENTS", "2026-11-15", "3000.00", "USD", "CUST_E", "PENDING", None, None),
        ]
        cur.executemany(
            """INSERT INTO partner_transactions 
               (id, partner_code, transaction_date, amount, currency, customer_ref, status, batch_id, updated_at) 
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);""",
            sample_rows,
        )
        conn.commit()
        logger.info("Seeded %d sample records into %s.", len(sample_rows), db_path)
    else:
        logger.info("Table partner_transactions already contains data.")

    conn.close()


def ensure_enabled_dags_structure():
    """Ensure enabled_dags directory structure exists for container mounts."""
    os.makedirs("enabled_dags/dags", exist_ok=True)
    os.makedirs("enabled_dags/plugins", exist_ok=True)
    init_path = "enabled_dags/dags/__init__.py"
    if not os.path.exists(init_path):
        with open(init_path, "w", encoding="utf-8") as f:
            f.write('"""Enabled DAGs package."""\n')


def main():
    logger.info("Initializing environment configurations and database...")
    ensure_enabled_dags_structure()
    init_airflow_models()
    seed_local_database()
    logger.info("Environment initialization completed.")


if __name__ == "__main__":
    main()
