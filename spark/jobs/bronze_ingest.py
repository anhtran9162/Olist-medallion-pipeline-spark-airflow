"""
Bronze Batch Ingest: Fetch data from the FastAPI and write to HDFS Bronze layer.

- All reference tables: 100% loaded (customers, products, sellers, etc.)
- Orders table: first 90% loaded (chronological split; last 10% goes via Kafka)
- Uses Delta Lake format with explicit schema enforcement
"""

import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from schemas import BRONZE_SCHEMAS
from data_quality import run_bronze_checks

API_BASE = os.environ.get("API_BASE", "http://api:8000/api/v1")
HDFS_BASE = "hdfs://namenode:9000/data/bronze"

# Tables that are reference data — load 100%
REFERENCE_TABLES = [
    "olist_customers_dataset",
    "olist_geolocation_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "product_category_name_translation",
]

# The orders table gets a 90/10 split
ORDERS_TABLE = "olist_orders_dataset"


def fetch_table(table_name: str, page_size: int = 5000):
    """Fetch all rows from a table via paginated API calls."""
    all_rows = []
    page = 1
    while True:
        resp = requests.get(f"{API_BASE}/{table_name}", params={"page": page, "size": page_size})
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("data", [])
        if not rows:
            break
        all_rows.extend(rows)
        if len(all_rows) >= data["total_rows"]:
            break
        page += 1
    return all_rows


def write_delta(df, hdfs_path):
    """Write DataFrame as Delta Lake format with schema enforcement."""
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(hdfs_path)


def main():
    spark = SparkSession.builder \
        .appName("Bronze-Batch-Ingest") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # --- Load reference tables (100%) ---
    for table in REFERENCE_TABLES:
        print(f"Fetching {table}...")
        rows = fetch_table(table)
        if not rows:
            print(f"  SKIP {table} — no data returned")
            continue

        schema = BRONZE_SCHEMAS[table]
        df = spark.createDataFrame(rows, schema=schema)
        df = df.withColumn("ingestion_timestamp", current_timestamp())
        hdfs_path = f"{HDFS_BASE}/{table}"
        write_delta(df, hdfs_path)
        print(f"  {table}: {df.count()} rows → {hdfs_path}")

    # --- Load orders table (first 90% only) ---
    print(f"Fetching {ORDERS_TABLE}...")
    all_orders = fetch_table(ORDERS_TABLE)
    all_orders.sort(key=lambda x: x.get("order_purchase_timestamp", ""))

    split_idx = int(len(all_orders) * 0.9)
    batch_orders = all_orders[:split_idx]

    print(f"  Total orders: {len(all_orders)}, Batch (90%): {len(batch_orders)}, Stream (10%): {len(all_orders) - split_idx}")

    schema = BRONZE_SCHEMAS[ORDERS_TABLE]
    df_orders = spark.createDataFrame(batch_orders, schema=schema)
    df_orders = df_orders.withColumn("ingestion_timestamp", current_timestamp())
    hdfs_path = f"{HDFS_BASE}/{ORDERS_TABLE}"
    write_delta(df_orders, hdfs_path)
    print(f"  {ORDERS_TABLE}: {df_orders.count()} rows → {hdfs_path}")

    # Run Bronze data quality checks
    run_bronze_checks(spark)

    spark.stop()
    print("Bronze batch ingest complete.")


if __name__ == "__main__":
    main()
