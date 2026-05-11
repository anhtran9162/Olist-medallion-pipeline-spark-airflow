"""
Bronze Stream Archive: Continuously read Kafka events and archive to HDFS.

Ensures immutable storage of all streaming data in the Bronze layer before
Kafka's retention policy purges them. Uses Spark Structured Streaming with
checkpointing for exactly-once semantics.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
HDFS_BASE = "hdfs://namenode:9000/data/bronze/stream_archive"

TOPICS = [
    "ecommerce.orders.live",
    "ecommerce.logistics.updates",
]


def archive_topic(spark: SparkSession, topic: str):
    """Read a Kafka topic as a stream and write to HDFS as Parquet."""
    checkpoint_path = f"{HDFS_BASE}/_checkpoints/{topic}"
    output_path = f"{HDFS_BASE}/{topic}"

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Select and transform columns: preserve raw bytes as strings + metadata
    archive_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("kafka_value"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingestion_timestamp"),
    )

    query = archive_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="30 seconds") \
        .start()

    print(f"Started archiving topic '{topic}' → {output_path}")
    return query


def main():
    spark = SparkSession.builder \
        .appName("Bronze-Stream-Archive") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    queries = []
    for topic in TOPICS:
        q = archive_topic(spark, topic)
        queries.append(q)

    # Wait for any of the streaming queries to terminate
    for q in queries:
        q.awaitTermination()

    spark.stop()


if __name__ == "__main__":
    main()
