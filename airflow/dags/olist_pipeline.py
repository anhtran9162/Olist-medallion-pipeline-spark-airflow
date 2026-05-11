"""
Olist Medallion Pipeline DAG.

Orchestrates the full Bronze → Silver → Gold data pipeline:
1. init_api — health check on FastAPI
2. bronze_batch_ingest — fetch 90% data from API → HDFS Bronze
3. kafka_producer — stream 10% orders to Kafka
4. bronze_stream_archive — archive Kafka events to HDFS Bronze (continuous)
5. silver_clean — Bronze → Silver (cleanse, NLP, impute)
6. gold_load — Silver → PostgreSQL Gold (star schema)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import requests


default_args = {
    "owner": "olist",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

SPARK_MASTER = "spark://spark-master:7077"
SPARK_JOBS_DIR = "/opt/spark/jobs"


def check_api_health(**context):
    """Health check on the FastAPI service."""
    resp = requests.get("http://api:8000/api/v1/health", timeout=10)
    resp.raise_for_status()
    print(f"API health: {resp.json()}")


with DAG(
    dag_id="olist_medallion_pipeline",
    default_args=default_args,
    description="Olist Medallion Architecture: Bronze → Silver → Gold",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "medallion"],
) as dag:

    init_api = PythonOperator(
        task_id="init_api",
        python_callable=check_api_health,
    )

    bronze_batch_ingest = SparkSubmitOperator(
        task_id="bronze_batch_ingest",
        application=f"{SPARK_JOBS_DIR}/bronze_ingest.py",
        master=SPARK_MASTER,
        conn_id="spark_default",
        conf={
            "spark.hadoop.fs.defaultFS": "hdfs://namenode:9000",
            "spark.hadoop.dfs.client.use.datanode.hostname": "true",
        },
        env_vars={
            "API_BASE": "http://api:8000/api/v1",
        },
    )

    kafka_producer = BashOperator(
        task_id="kafka_producer",
        bash_command=(
            "pip install confluent-kafka requests 2>/dev/null && "
            "python /opt/airflow/dags/scripts/order_producer.py"
        ),
        env={
            "API_BASE": "http://api:8000/api/v1",
            "KAFKA_BOOTSTRAP": "kafka:29092",
        },
    )

    bronze_stream_archive = SparkSubmitOperator(
        task_id="bronze_stream_archive",
        application=f"{SPARK_JOBS_DIR}/bronze_stream_archive.py",
        master=SPARK_MASTER,
        conn_id="spark_default",
        conf={
            "spark.hadoop.fs.defaultFS": "hdfs://namenode:9000",
            "spark.hadoop.dfs.client.use.datanode.hostname": "true",
            "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        },
        env_vars={
            "KAFKA_BOOTSTRAP": "kafka:29092",
        },
    )

    silver_clean = SparkSubmitOperator(
        task_id="silver_clean",
        application=f"{SPARK_JOBS_DIR}/silver_clean.py",
        master=SPARK_MASTER,
        conn_id="spark_default",
        conf={
            "spark.hadoop.fs.defaultFS": "hdfs://namenode:9000",
            "spark.hadoop.dfs.client.use.datanode.hostname": "true",
        },
    )

    gold_load = SparkSubmitOperator(
        task_id="gold_load",
        application=f"{SPARK_JOBS_DIR}/gold_load.py",
        master=SPARK_MASTER,
        conn_id="spark_default",
        conf={
            "spark.hadoop.fs.defaultFS": "hdfs://namenode:9000",
            "spark.hadoop.dfs.client.use.datanode.hostname": "true",
        },
        env_vars={
            "PG_URL": "jdbc:postgresql://postgres:5432/olist_dw",
            "PG_USER": "olist",
            "PG_PASSWORD": "olist123",
        },
    )

    # DAG dependencies
    init_api >> bronze_batch_ingest >> silver_clean >> gold_load
    init_api >> kafka_producer >> bronze_stream_archive >> silver_clean
