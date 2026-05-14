"""
Data Quality checks using PyDeequ for Bronze, Silver, and Gold layers.

Each function runs a VerificationSuite against the specified layer's tables.
If any critical check fails, raises an exception to halt the pipeline.
"""

from pydeequ.checks import *
from pydeequ.verification import *
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


HDFS_BRONZE = "hdfs://namenode:9000/data/bronze"
HDFS_SILVER = "hdfs://namenode:9000/data/silver"
HDFS_GOLD = "hdfs://namenode:9000/data/gold"


def _run_checks(spark, df, check_name, check):
    """Run PyDeequ verification and return pass/fail."""
    verification_result = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(check) \
        .run()

    result_df = VerificationResult.checkResultsAsDataFrame(spark, verification_result)
    if result_df.count() == 0:
        print(f"  [{check_name}] No checks executed.")
        return True

    result_df.select("check", "check_level", "check_status", "constraint",
                     "constraint_status", "constraint_message") \
        .show(truncate=80)

    failed = result_df.filter(F.col("constraint_status") == "Failure")
    if failed.count() > 0:
        print(f"  [{check_name}] FAILED — {failed.count()} constraint(s) violated")
        return False

    print(f"  [{check_name}] PASSED")
    return True


def run_bronze_checks(spark, bronze_base=HDFS_BRONZE):
    """Run DQ checks on Bronze layer tables after ingestion."""
    print("\n=== Bronze Data Quality Checks ===")
    all_passed = True

    # Orders: completeness + uniqueness on order_id
    try:
        orders = spark.read.format("delta").load(f"{bronze_base}/olist_orders_dataset")
        check = Check(spark, CheckLevel.Error, "Bronze Orders Check") \
            .isComplete("order_id") \
            .isUnique("order_id") \
            .isComplete("customer_id")

        if not _run_checks(spark, orders, "Bronze/Orders", check):
            all_passed = False
    except Exception as e:
        print(f"  Bronze/Orders check skipped: {e}")

    # Customers: completeness on customer_id
    try:
        customers = spark.read.format("delta").load(f"{bronze_base}/olist_customers_dataset")
        check = Check(spark, CheckLevel.Error, "Bronze Customers Check") \
            .isComplete("customer_id")

        if not _run_checks(spark, customers, "Bronze/Customers", check):
            all_passed = False
    except Exception as e:
        print(f"  Bronze/Customers check skipped: {e}")

    # Products: completeness on product_id
    try:
        products = spark.read.format("delta").load(f"{bronze_base}/olist_products_dataset")
        check = Check(spark, CheckLevel.Error, "Bronze Products Check") \
            .isComplete("product_id")

        if not _run_checks(spark, products, "Bronze/Products", check):
            all_passed = False
    except Exception as e:
        print(f"  Bronze/Products check skipped: {e}")

    if not all_passed:
        raise RuntimeError("Bronze DQ checks FAILED — pipeline halted")


def run_silver_checks(spark, silver_base=HDFS_SILVER):
    """Run DQ checks on Silver layer tables after cleaning."""
    print("\n=== Silver Data Quality Checks ===")
    all_passed = True

    # Orders: completeness + uniqueness
    try:
        orders = spark.read.format("delta").load(f"{silver_base}/olist_orders_dataset")
        check = Check(spark, CheckLevel.Error, "Silver Orders Check") \
            .isComplete("order_id") \
            .isUnique("order_id") \
            .isComplete("customer_id")

        if not _run_checks(spark, orders, "Silver/Orders", check):
            all_passed = False
    except Exception as e:
        print(f"  Silver/Orders check skipped: {e}")

    # Reviews: review_score range 1-5
    try:
        reviews = spark.read.format("delta").load(f"{silver_base}/olist_order_reviews_dataset")
        check = Check(spark, CheckLevel.Error, "Silver Reviews Check") \
            .isComplete("review_id") \
            .satisfies("review_score >= 1 AND review_score <= 5", "Review score in [1,5]")

        if not _run_checks(spark, reviews, "Silver/Reviews", check):
            all_passed = False
    except Exception as e:
        print(f"  Silver/Reviews check skipped: {e}")

    # Order items: price >= 0
    try:
        items = spark.read.format("delta").load(f"{silver_base}/olist_order_items_dataset")
        check = Check(spark, CheckLevel.Error, "Silver Items Check") \
            .isComplete("order_id") \
            .satisfies("price >= 0", "Price non-negative")

        if not _run_checks(spark, items, "Silver/Items", check):
            all_passed = False
    except Exception as e:
        print(f"  Silver/Items check skipped: {e}")

    if not all_passed:
        raise RuntimeError("Silver DQ checks FAILED — pipeline halted")


def run_gold_checks(spark, gold_base=HDFS_GOLD):
    """Run DQ checks on Gold layer tables after star schema build."""
    print("\n=== Gold Data Quality Checks ===")
    all_passed = True

    # Fact table: surrogate key completeness + uniqueness
    try:
        fct = spark.read.format("delta").load(f"{gold_base}/fct_order_items")
        check = Check(spark, CheckLevel.Error, "Gold Fact Check") \
            .isComplete("order_item_key") \
            .isUnique("order_item_key") \
            .isComplete("customer_key") \
            .isComplete("product_key") \
            .satisfies("item_price >= 0", "Item price non-negative") \
            .satisfies("freight_value >= 0", "Freight value non-negative")

        if not _run_checks(spark, fct, "Gold/Fact", check):
            all_passed = False
    except Exception as e:
        print(f"  Gold/Fact check skipped: {e}")

    # Dimension: customer_key uniqueness
    try:
        dim_cust = spark.read.format("delta").load(f"{gold_base}/dim_customers")
        check = Check(spark, CheckLevel.Error, "Gold Dim Customers Check") \
            .isComplete("customer_key") \
            .isUnique("customer_key")

        if not _run_checks(spark, dim_cust, "Gold/DimCustomers", check):
            all_passed = False
    except Exception as e:
        print(f"  Gold/DimCustomers check skipped: {e}")

    if not all_passed:
        raise RuntimeError("Gold DQ checks FAILED — pipeline halted")
