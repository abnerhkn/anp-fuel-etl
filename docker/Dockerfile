FROM apache/airflow:2.8.1

USER root

RUN mkdir -p /opt/airflow/logs /opt/airflow/plugins /opt/airflow/dags /opt/airflow/data \
    && chown -R 50000:0 /opt/airflow \
    && chmod -R 775 /opt/airflow
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /opt/airflow/requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

USER 50000

