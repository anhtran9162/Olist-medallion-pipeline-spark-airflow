"""Load Olist CSVs into SQLite for the simulated internal API."""

import pandas as pd
from pathlib import Path
from database import DB_PATH, get_connection

DATA_DIR = Path("/app/csv_data")

TABLES = [
    "olist_customers_dataset",
    "olist_geolocation_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_orders_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "product_category_name_translation",
]

INDEXES = {
    "olist_orders_dataset": ["order_id", "customer_id"],
    "olist_order_items_dataset": ["order_id", "product_id", "seller_id"],
    "olist_order_payments_dataset": ["order_id"],
    "olist_order_reviews_dataset": ["order_id"],
    "olist_customers_dataset": ["customer_id", "customer_unique_id"],
    "olist_products_dataset": ["product_id"],
    "olist_sellers_dataset": ["seller_id"],
    "olist_geolocation_dataset": ["geolocation_zip_code_prefix"],
}


def load_all():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection()

    for table in TABLES:
        csv_file = DATA_DIR / f"{table}.csv"
        if not csv_file.exists():
            print(f"  SKIP {table} — CSV not found")
            continue

        encoding = "utf-8-sig" if table == "product_category_name_translation" else "utf-8"
        df = pd.read_csv(csv_file, encoding=encoding)
        df.to_sql(table, conn, if_exists="replace", index=False)

        for col in INDEXES.get(table, []):
            idx_name = f"idx_{table}_{col}"
            conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({col})")

        print(f"  {table}: {len(df):,} rows loaded")

    conn.close()
    print(f"\nSQLite database created at {DB_PATH}")


if __name__ == "__main__":
    load_all()
