"""
Olist Medallion Pipeline DAG.

Orchestrates the full Bronze -> Silver -> Gold data pipeline:
1. init_api — health check on FastAPI
2. bronze_batch_ingest — fetch 90% data from API -> HDFS Bronze
3. kafka_producer — stream 10% orders to Kafka
4. bronze_stream_archive — archive Kafka events to HDFS Bronze (batch mode)
5. silver_clean — Bronze -> Silver (cleanse, NLP, impute)
6. gold_load — Silver -> PostgreSQL Gold (star schema)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests


default_args = {
    "owner": "olist",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

SPARK_SUBMIT = "docker exec olist-spark-master /opt/spark/bin/spark-submit"
SPARK_MASTER = "spark://spark-master:7077"
SPARK_JOBS_DIR = "/opt/spark/jobs"
HDFS_CONF = "--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 --conf spark.hadoop.dfs.client.use.datanode.hostname=true"
DELTA_CONF = "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"


def check_api_health(**context):
    """Health check on the FastAPI service."""
    resp = requests.get("http://api:8000/api/v1/health", timeout=10)
    resp.raise_for_status()
    print(f"API health: {resp.json()}")


with DAG(
    dag_id="olist_medallion_pipeline",
    default_args=default_args,
    description="Olist Medallion Architecture: Bronze -> Silver -> Gold",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "medallion"],
) as dag:

    init_api = PythonOperator(
        task_id="init_api",
        python_callable=check_api_health,
    )

    bronze_batch_ingest = BashOperator(
        task_id="bronze_batch_ingest",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"--master {SPARK_MASTER} {HDFS_CONF} {DELTA_CONF} "
            f"--conf spark.executorEnv.API_BASE=http://api:8000/api/v1 "
            f"{SPARK_JOBS_DIR}/bronze_ingest.py"
        ),
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

    bronze_stream_archive = BashOperator(
        task_id="bronze_stream_archive",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"--master {SPARK_MASTER} {HDFS_CONF} {DELTA_CONF} "
            f"--conf spark.executorEnv.KAFKA_BOOTSTRAP=kafka:29092 "
            f"--conf spark.streaming.stopGracefullyOnShutdown=true "
            f"{SPARK_JOBS_DIR}/bronze_stream_archive.py & "
            "STREAM_PID=$! && sleep 120 && "
            "APP_ID=$(curl -s http://spark-master:8080/ | grep -B5 'Bronze-Stream-Archive' | grep -oP 'app-\\d+-\\d+' | head -1); "
            "if [ -n \"$APP_ID\" ]; then curl -s -X POST 'http://spark-master:8080/app/kill/' -d \"id=$APP_ID&terminate=true\"; fi; "
            "kill $STREAM_PID 2>/dev/null && wait $STREAM_PID 2>/dev/null; "
            "exit 0"
        ),
    )

    silver_clean = BashOperator(
        task_id="silver_clean",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"--master {SPARK_MASTER} {HDFS_CONF} {DELTA_CONF} "
            f"{SPARK_JOBS_DIR}/silver_clean.py"
        ),
    )

    gold_load = BashOperator(
        task_id="gold_load",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"--master {SPARK_MASTER} {HDFS_CONF} {DELTA_CONF} "
            f"--conf spark.jars=/opt/spark/jars/postgresql-42.7.3.jar "
            f"{SPARK_JOBS_DIR}/gold_load.py"
        ),
    )

    # DAG dependencies
    init_api >> bronze_batch_ingest >> silver_clean >> gold_load
    init_api >> kafka_producer >> bronze_stream_archive >> silver_clean
