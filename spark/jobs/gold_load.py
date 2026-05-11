"""
Gold Load: Read Silver Parquet from HDFS, build star schema, write to PostgreSQL.

Star Schema:
- dim_customers, dim_sellers, dim_products, dim_order_status, dim_date
- fct_order_items (fact table at order-item granularity)
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

HDFS_SILVER = "hdfs://namenode:9000/data/silver"

PG_URL = os.environ.get("PG_URL", "jdbc:postgresql://postgres:5432/olist_dw")
PG_USER = os.environ.get("PG_USER", "olist")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "olist123")
PG_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}

DEFAULT_REVIEW_SCORE = 3

# Brazilian national holidays (fixed dates)
BRAZILIAN_HOLIDAYS = {
    "01-01", "04-21", "05-01", "09-07", "10-12", "11-02", "11-15", "12-25",
}


def read_silver(spark, table_name):
    """Read a Silver Parquet table from HDFS."""
    return spark.read.parquet(f"{HDFS_SILVER}/{table_name}")


def write_to_postgres(df, table_name):
    """Write a DataFrame to PostgreSQL, overwriting existing data."""
    df.write.jdbc(url=PG_URL, table=table_name, mode="overwrite", properties=PG_PROPERTIES)
    print(f"  Written {df.count()} rows to PostgreSQL/{table_name}")


def build_dim_customers(spark):
    """Build dim_customers with surrogate key."""
    customers = read_silver(spark, "olist_customers_dataset")
    dim = customers.select(
        "customer_id", "customer_unique_id",
        "customer_zip_code_prefix", "customer_city", "customer_state",
    ).dropDuplicates(["customer_id"]).orderBy("customer_id")

    w = Window.orderBy("customer_id")
    dim = dim.withColumn("customer_key", F.row_number().over(w))
    dim = dim.select("customer_key", "customer_id", "customer_unique_id",
                     "customer_zip_code_prefix", "customer_city", "customer_state")
    return dim


def build_dim_sellers(spark):
    """Build dim_sellers with surrogate key."""
    sellers = read_silver(spark, "olist_sellers_dataset")
    dim = sellers.select(
        "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state",
    ).dropDuplicates(["seller_id"]).orderBy("seller_id")

    w = Window.orderBy("seller_id")
    dim = dim.withColumn("seller_key", F.row_number().over(w))
    dim = dim.select("seller_key", "seller_id", "seller_zip_code_prefix",
                     "seller_city", "seller_state")
    return dim


def build_dim_products(spark):
    """Build dim_products with surrogate key, English category, and volume."""
    products = read_silver(spark, "olist_products_dataset")
    translation = read_silver(spark, "product_category_name_translation")

    # Join Portuguese → English category
    dim = products.join(translation, on="product_category_name", how="left")
    dim = dim.withColumn(
        "product_category_english",
        F.coalesce(F.col("product_category_name_english"), F.col("product_category_name")),
    )

    dim = dim.select(
        "product_id", "product_category_english",
        "product_weight_g", "product_length_cm", "product_volume_cm3",
    ).dropDuplicates(["product_id"]).orderBy("product_id")

    w = Window.orderBy("product_id")
    dim = dim.withColumn("product_key", F.row_number().over(w))
    dim = dim.select("product_key", "product_id", "product_category_english",
                     "product_weight_g", "product_length_cm", "product_volume_cm3")
    return dim


def build_dim_order_status(spark):
    """Build dim_order_status with surrogate key."""
    orders = read_silver(spark, "olist_orders_dataset")
    dim = orders.select(F.col("order_status").alias("status_name")) \
        .distinct() \
        .orderBy("status_name")

    w = Window.orderBy("status_name")
    dim = dim.withColumn("status_key", F.row_number().over(w))
    dim = dim.select("status_key", "status_name")
    return dim


def build_dim_date(spark):
    """Build dim_date from order purchase timestamps."""
    orders = read_silver(spark, "olist_orders_dataset")

    distinct_dates = (
        orders.select(F.to_date("order_purchase_timestamp").alias("full_date"))
        .filter(F.col("full_date").isNotNull())
        .distinct()
        .orderBy("full_date")
    )

    dim = distinct_dates.withColumn(
        "date_key", F.date_format("full_date", "yyyyMMdd").cast("int"),
    ).withColumn(
        "day_of_week", F.date_format("full_date", "EEEE"),
    ).withColumn(
        "month_name", F.date_format("full_date", "MMMM"),
    ).withColumn(
        "quarter", F.quarter("full_date"),
    ).withColumn(
        "year", F.year("full_date"),
    ).withColumn(
        "is_holiday_brazil",
        F.when(
            F.date_format("full_date", "MM-dd").isin(*list(BRAZILIAN_HOLIDAYS)),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    dim = dim.select("date_key", "full_date", "day_of_week", "month_name",
                     "quarter", "year", "is_holiday_brazil")
    return dim


def build_fct_order_items(spark, dim_customers, dim_products, dim_sellers,
                          dim_order_status, dim_date):
    """Build fct_order_items fact table at order-item granularity."""
    orders = read_silver(spark, "olist_orders_dataset")
    items = read_silver(spark, "olist_order_items_dataset")
    reviews = read_silver(spark, "olist_order_reviews_dataset")

    # Dedup reviews: keep latest review per order
    reviews_dedup = (
        reviews.withColumn(
            "rn",
            F.row_number().over(Window.partitionBy("order_id").orderBy(F.desc("review_answer_timestamp"))),
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Start from items, join orders and reviews
    fct = items.join(orders, on="order_id", how="left")
    fct = fct.join(
        reviews_dedup.select("order_id", "review_score"),
        on="order_id",
        how="left",
    )

    # Impute null review scores
    fct = fct.fillna({"review_score": DEFAULT_REVIEW_SCORE})

    # Map foreign keys via dimension tables
    fct = fct.join(
        dim_customers.select("customer_id", "customer_key"),
        on="customer_id", how="left",
    )
    fct = fct.join(
        dim_products.select("product_id", "product_key"),
        on="product_id", how="left",
    )
    fct = fct.join(
        dim_sellers.select("seller_id", "seller_key"),
        on="seller_id", how="left",
    )
    fct = fct.join(
        dim_order_status,
        fct["order_status"] == dim_order_status["status_name"],
        how="left",
    ).drop(dim_order_status["status_name"])

    # order_date_key from purchase timestamp
    fct = fct.withColumn(
        "order_date_key",
        F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int"),
    )

    # Compute logistics metrics
    fct = fct.withColumn(
        "processing_time_hours",
        F.round(
            (F.unix_timestamp("order_delivered_carrier_date") -
             F.unix_timestamp("order_approved_at")) / 3600, 2,
        ),
    )
    fct = fct.withColumn(
        "shipping_time_days",
        F.round(
            (F.unix_timestamp("order_delivered_customer_date") -
             F.unix_timestamp("order_delivered_carrier_date")) / 86400, 0,
        ).cast("int"),
    )

    # Select final columns
    result = fct.select(
        "order_id",
        "customer_key", "product_key", "seller_key",
        "order_date_key", "status_key",
        F.col("price").alias("item_price"),
        "freight_value",
        "review_score",
        "processing_time_hours",
        "shipping_time_days",
    ).dropDuplicates()

    # Add surrogate key
    w = Window.orderBy("order_id", "customer_key", "product_key", "seller_key")
    result = result.withColumn("order_item_key", F.row_number().over(w))
    result = result.select(
        "order_item_key", "order_id",
        "customer_key", "product_key", "seller_key",
        "order_date_key", "status_key",
        "item_price", "freight_value",
        "review_score", "processing_time_hours", "shipping_time_days",
    )

    return result


def main():
    spark = SparkSession.builder \
        .appName("Gold-Load") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # Build dimension tables
    print("Building dimension tables...")
    dim_customers = build_dim_customers(spark)
    dim_sellers = build_dim_sellers(spark)
    dim_products = build_dim_products(spark)
    dim_order_status = build_dim_order_status(spark)
    dim_date = build_dim_date(spark)

    # Build fact table
    print("Building fact table...")
    fct_order_items = build_fct_order_items(
        spark, dim_customers, dim_products, dim_sellers,
        dim_order_status, dim_date,
    )

    # Write to PostgreSQL
    print("Writing to PostgreSQL...")
    write_to_postgres(dim_customers, "dim_customers")
    write_to_postgres(dim_sellers, "dim_sellers")
    write_to_postgres(dim_products, "dim_products")
    write_to_postgres(dim_order_status, "dim_order_status")
    write_to_postgres(dim_date, "dim_date")
    write_to_postgres(fct_order_items, "fct_order_items")

    spark.stop()
    print("Gold load complete.")


if __name__ == "__main__":
    main()
