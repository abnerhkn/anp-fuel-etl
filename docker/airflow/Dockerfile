FROM apache/airflow:2.8.1

USER root

RUN mkdir -p /opt/airflow/logs /opt/airflow/plugins /opt/airflow/dags /opt/airflow/data \
    && chown -R 50000:0 /opt/airflow \
    && chmod -R 775 /opt/airflow

USER 50000

