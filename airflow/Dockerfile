FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl libpq-dev gcc docker-cli && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "apache-airflow==2.9.0" \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-postgres \
    requests

WORKDIR /opt/airflow

ENV AIRFLOW_HOME=/opt/airflow

EXPOSE 8080

ENTRYPOINT ["airflow"]
