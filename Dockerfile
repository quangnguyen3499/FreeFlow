# =====================================================================
# Airflow Production & Local Development Image
# Principle: "One image everywhere" (Local, Test, Staging, Production)
# =====================================================================

FROM apache/airflow:2.9.2-python3.11

USER root
# Install system packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        git \
        build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements & install dependencies
COPY docker/artifacts/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Bake application code into container image
COPY --chown=airflow:root dags /opt/airflow/dags
COPY --chown=airflow:root plugins /opt/airflow/plugins
COPY --chown=airflow:root tests /opt/airflow/tests
COPY --chown=airflow:root docker /opt/airflow/docker
COPY --chown=airflow:root tools /opt/airflow/tools
COPY --chown=airflow:root docker/artifacts/pytest.ini /opt/airflow/pytest.ini

RUN chmod +x /opt/airflow/tools/sync.py \
             /opt/airflow/tools/watcher.py \
             /opt/airflow/tools/init_env.py \
             /opt/airflow/tools/mock_server.py \
    && ln -sf /opt/airflow/tools/sync.py /opt/airflow/sync \
    && ln -sf /opt/airflow/tools/watcher.py /opt/airflow/watcher

ENV PYTHONPATH="/opt/airflow:/opt/airflow/enabled_dags"
ENV AIRFLOW_HOME="/opt/airflow"
