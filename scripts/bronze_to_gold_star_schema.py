"""
Bronze → Gold ETL: Olist Star Schema (Pandas Prototype)

Reads raw CSVs from the Bronze layer, builds dimension tables with surrogate
keys, constructs the fact table at order-item granularity with foreign key
mappings and pre-calculated logistics metrics, then exports all 6 tables as CSVs.
"""

import pandas as pd
import numpy as np
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "brazilian-ecommerce"
OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "gold"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_REVIEW_SCORE = 3  # neutral imputation for missing review scores

# ---------------------------------------------------------------------------
# 1. Load Bronze (raw) CSVs
# ---------------------------------------------------------------------------
orders = pd.read_csv(RAW_DIR / "olist_orders_dataset.csv")
items = pd.read_csv(RAW_DIR / "olist_order_items_dataset.csv")
products = pd.read_csv(RAW_DIR / "olist_products_dataset.csv")
customers = pd.read_csv(RAW_DIR / "olist_customers_dataset.csv")
sellers = pd.read_csv(RAW_DIR / "olist_sellers_dataset.csv")
reviews = pd.read_csv(RAW_DIR / "olist_order_reviews_dataset.csv")
cat_translation = pd.read_csv(
    RAW_DIR / "product_category_name_translation.csv",
    encoding="utf-8-sig",  # file has a BOM
)

# ---------------------------------------------------------------------------
# 2. Cast timestamp columns (orders table)
# ---------------------------------------------------------------------------
ts_cols = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
for col in ts_cols:
    orders[col] = pd.to_datetime(orders[col], errors="coerce")

# ---------------------------------------------------------------------------
# 3. Build Dimension Tables
# ---------------------------------------------------------------------------

# --- dim_customers ---
# One row per customer_id (natural key). Surrogate key is a dense integer.
dim_customers = (
    customers[["customer_id", "customer_unique_id",
               "customer_zip_code_prefix", "customer_city", "customer_state"]]
    .drop_duplicates(subset=["customer_id"])
    .sort_values("customer_id")
    .reset_index(drop=True)
)
dim_customers.insert(0, "customer_key", range(1, len(dim_customers) + 1))

# --- dim_sellers ---
dim_sellers = (
    sellers[["seller_id", "seller_zip_code_prefix",
             "seller_city", "seller_state"]]
    .drop_duplicates(subset=["seller_id"])
    .sort_values("seller_id")
    .reset_index(drop=True)
)
dim_sellers.insert(0, "seller_key", range(1, len(dim_sellers) + 1))

# --- dim_products ---
# Join product category Portuguese name → English via translation table.
# Categories not in the translation table keep their original Portuguese name.
dim_products = products[[
    "product_id", "product_category_name",
    "product_weight_g", "product_length_cm",
    "product_height_cm", "product_width_cm",
]].copy()

dim_products = dim_products.merge(
    cat_translation,
    on="product_category_name",
    how="left",
)
# Fill untranslated categories with the original Portuguese name
dim_products["product_category_english"] = dim_products[
    "product_category_name_english"
].fillna(dim_products["product_category_name"])

# Compute volume: length × width × height (cm³)
dim_products["product_volume_cm3"] = (
    dim_products["product_length_cm"]
    * dim_products["product_width_cm"]
    * dim_products["product_height_cm"]
)

dim_products = (
    dim_products[[
        "product_id", "product_category_english",
        "product_weight_g", "product_length_cm", "product_volume_cm3",
    ]]
    .drop_duplicates(subset=["product_id"])
    .sort_values("product_id")
    .reset_index(drop=True)
)
dim_products.insert(0, "product_key", range(1, len(dim_products) + 1))

# --- dim_order_status ---
# Enumerate all distinct order statuses from the orders table.
dim_order_status = (
    orders[["order_status"]]
    .drop_duplicates()
    .sort_values("order_status")
    .reset_index(drop=True)
)
dim_order_status.insert(0, "status_key", range(1, len(dim_order_status) + 1))
dim_order_status.rename(columns={"order_status": "status_name"}, inplace=True)

# --- dim_date ---
# Extract every distinct calendar date that appears across key order timestamps.
# date_key is an integer in YYYYMMDD format.
date_sources = pd.concat([
    orders["order_purchase_timestamp"],
    orders["order_estimated_delivery_date"],
    orders["order_delivered_customer_date"],
]).dropna().dt.normalize()

distinct_dates = (
    date_sources
    .drop_duplicates()
    .sort_values()
    .reset_index(drop=True)
)

dim_date = pd.DataFrame({"full_date": distinct_dates})
dim_date["date_key"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
dim_date["day_of_week"] = dim_date["full_date"].dt.day_name()
dim_date["month_name"] = dim_date["full_date"].dt.month_name()
dim_date["quarter"] = dim_date["full_date"].dt.quarter
dim_date["year"] = dim_date["full_date"].dt.year
# Reorder so date_key comes first
dim_date = dim_date[["date_key", "full_date", "day_of_week",
                      "month_name", "quarter", "year"]]

# ---------------------------------------------------------------------------
# 4. Build Fact Table — fct_order_items
# ---------------------------------------------------------------------------
# Start from order_items (one row per item) and join in everything else.

# Merge items ← orders (to get customer_id, status, timestamps)
fct = items.merge(orders, on="order_id", how="left")

# Merge fct ← reviews (one review per order; some orders have none)
# A single order can have multiple reviews — keep the latest review per order.
reviews_dedup = (
    reviews.sort_values("review_answer_timestamp")
    .drop_duplicates(subset=["order_id"], keep="last")
)
fct = fct.merge(
    reviews_dedup[["order_id", "review_score"]],
    on="order_id",
    how="left",
)

# Impute missing review scores with a neutral default
fct["review_score"] = fct["review_score"].fillna(DEFAULT_REVIEW_SCORE).astype(int)

# --- Map foreign keys via the dimension tables ---
# Each merge pulls in the surrogate key, then we drop the natural key.

fct = fct.merge(dim_customers[["customer_id", "customer_key"]],
                on="customer_id", how="left")
fct = fct.merge(dim_products[["product_id", "product_key"]],
                on="product_id", how="left")
fct = fct.merge(dim_sellers[["seller_id", "seller_key"]],
                on="seller_id", how="left")
fct = fct.merge(dim_order_status, left_on="order_status",
                right_on="status_name", how="left")

# order_date_key: derive from order_purchase_timestamp → date_key
fct["order_date_key"] = (
    fct["order_purchase_timestamp"]
    .dt.strftime("%Y%m%d")
    .astype("Int64")      # nullable Int64 to handle NaT gracefully
)

# estimated_delivery_date_key: derive from order_estimated_delivery_date → date_key
fct["estimated_delivery_date_key"] = (
    fct["order_estimated_delivery_date"]
    .dt.strftime("%Y%m%d")
    .astype("Int64")
)

# delivered_date_key: derive from order_delivered_customer_date → date_key
fct["delivered_date_key"] = (
    fct["order_delivered_customer_date"]
    .dt.strftime("%Y%m%d")
    .astype("Int64")
)

# --- Logistics metrics ---
# processing_time_hours: approved → handed to carrier (hours)
fct["processing_time_hours"] = (
    (fct["order_delivered_carrier_date"] - fct["order_approved_at"])
    .dt.total_seconds() / 3600
)
# Round to 2 decimal places; NaN stays NaN where timestamps are missing
fct["processing_time_hours"] = fct["processing_time_hours"].round(2)

# shipping_time_days: carrier pickup → customer delivery (days)
fct["shipping_time_days"] = (
    (fct["order_delivered_customer_date"] - fct["order_delivered_carrier_date"])
    .dt.total_seconds() / 86400
)
fct["shipping_time_days"] = fct["shipping_time_days"].round(2)

# --- Select & order final fact columns ---
fct_order_items = fct[[
    "order_id",
    "customer_key", "product_key", "seller_key",
    "order_date_key", "estimated_delivery_date_key", "delivered_date_key",
    "status_key",
    "price", "freight_value",
    "review_score",
    "processing_time_hours", "shipping_time_days",
]].copy()

# Rename price → item_price for business clarity
fct_order_items.rename(columns={"price": "item_price"}, inplace=True)

# Drop exact duplicates that may arise from many-to-many joins
fct_order_items = fct_order_items.drop_duplicates().reset_index(drop=True)

# Generate surrogate key for the fact table
fct_order_items.insert(0, "order_item_key", range(1, len(fct_order_items) + 1))

# ---------------------------------------------------------------------------
# 5. Export Gold Layer CSVs
# ---------------------------------------------------------------------------
dim_customers.to_csv(OUT_DIR / "dim_customers.csv", index=False)
dim_sellers.to_csv(OUT_DIR / "dim_sellers.csv", index=False)
dim_products.to_csv(OUT_DIR / "dim_products.csv", index=False)
dim_order_status.to_csv(OUT_DIR / "dim_order_status.csv", index=False)
dim_date.to_csv(OUT_DIR / "dim_date.csv", index=False)
fct_order_items.to_csv(OUT_DIR / "fct_order_items.csv", index=False)

# ---------------------------------------------------------------------------
# Quick sanity summary
# ---------------------------------------------------------------------------
print("=== Gold Layer Export Complete ===")
print(f"  dim_customers:      {len(dim_customers):>7,} rows")
print(f"  dim_sellers:        {len(dim_sellers):>7,} rows")
print(f"  dim_products:       {len(dim_products):>7,} rows")
print(f"  dim_order_status:   {len(dim_order_status):>7,} rows")
print(f"  dim_date:           {len(dim_date):>7,} rows")
print(f"  fct_order_items:    {len(fct_order_items):>7,} rows")
print(f"\nOutput directory: {OUT_DIR}")
