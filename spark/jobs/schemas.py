"""
Explicit schema definitions for all Bronze layer tables.

All columns are StringType in Bronze (raw layer preserves source types).
Silver layer handles type casting to proper types.
"""

from pyspark.sql.types import StructType, StructField, StringType

BRONZE_SCHEMAS = {
    "olist_orders_dataset": StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_status", StringType()),
        StructField("order_purchase_timestamp", StringType()),
        StructField("order_approved_at", StringType()),
        StructField("order_delivered_carrier_date", StringType()),
        StructField("order_delivered_customer_date", StringType()),
        StructField("order_estimated_delivery_date", StringType()),
    ]),
    "olist_order_items_dataset": StructType([
        StructField("order_id", StringType()),
        StructField("order_item_id", StringType()),
        StructField("product_id", StringType()),
        StructField("seller_id", StringType()),
        StructField("shipping_limit_date", StringType()),
        StructField("price", StringType()),
        StructField("freight_value", StringType()),
    ]),
    "olist_order_payments_dataset": StructType([
        StructField("order_id", StringType()),
        StructField("payment_sequential", StringType()),
        StructField("payment_type", StringType()),
        StructField("payment_installments", StringType()),
        StructField("payment_value", StringType()),
    ]),
    "olist_order_reviews_dataset": StructType([
        StructField("review_id", StringType()),
        StructField("order_id", StringType()),
        StructField("review_score", StringType()),
        StructField("review_comment_title", StringType()),
        StructField("review_comment_message", StringType()),
        StructField("review_creation_date", StringType()),
        StructField("review_answer_timestamp", StringType()),
    ]),
    "olist_customers_dataset": StructType([
        StructField("customer_id", StringType()),
        StructField("customer_unique_id", StringType()),
        StructField("customer_zip_code_prefix", StringType()),
        StructField("customer_city", StringType()),
        StructField("customer_state", StringType()),
    ]),
    "olist_products_dataset": StructType([
        StructField("product_id", StringType()),
        StructField("product_category_name", StringType()),
        StructField("product_name_lenght", StringType()),
        StructField("product_description_lenght", StringType()),
        StructField("product_photos_qty", StringType()),
        StructField("product_weight_g", StringType()),
        StructField("product_length_cm", StringType()),
        StructField("product_height_cm", StringType()),
        StructField("product_width_cm", StringType()),
    ]),
    "olist_sellers_dataset": StructType([
        StructField("seller_id", StringType()),
        StructField("seller_zip_code_prefix", StringType()),
        StructField("seller_city", StringType()),
        StructField("seller_state", StringType()),
    ]),
    "olist_geolocation_dataset": StructType([
        StructField("geolocation_zip_code_prefix", StringType()),
        StructField("geolocation_lat", StringType()),
        StructField("geolocation_lng", StringType()),
        StructField("geolocation_city", StringType()),
        StructField("geolocation_state", StringType()),
    ]),
    "product_category_name_translation": StructType([
        StructField("product_category_name", StringType()),
        StructField("product_category_name_english", StringType()),
    ]),
}
