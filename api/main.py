"""FastAPI service simulating Olist's internal API backed by SQLite."""

from fastapi import FastAPI, HTTPException, Query
from typing import Optional
from database import get_connection

app = FastAPI(title="Olist Internal API", version="1.0.0")

VALID_TABLES = {
    "olist_customers_dataset",
    "olist_geolocation_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_orders_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "product_category_name_translation",
}

PK_COLUMNS = {
    "olist_orders_dataset": "order_id",
    "olist_order_items_dataset": "order_id",
    "olist_order_payments_dataset": "order_id",
    "olist_order_reviews_dataset": "order_id",
    "olist_customers_dataset": "customer_id",
    "olist_products_dataset": "product_id",
    "olist_sellers_dataset": "seller_id",
    "olist_geolocation_dataset": "geolocation_zip_code_prefix",
    "product_category_name_translation": "product_category_name",
}


@app.get("/api/v1/health")
def health_check():
    conn = get_connection()
    try:
        conn.execute("SELECT 1")
        return {"status": "healthy"}
    finally:
        conn.close()


@app.get("/api/v1/tables")
def list_tables():
    conn = get_connection()
    try:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        tables = []
        for (name,) in cursor.fetchall():
            count = conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
            tables.append({"table_name": name, "row_count": count})
        return {"tables": tables}
    finally:
        conn.close()


@app.get("/api/v1/{table_name}")
def get_table_data(
    table_name: str,
    page: int = Query(1, ge=1),
    size: int = Query(1000, ge=1, le=10000),
):
    if table_name not in VALID_TABLES:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    conn = get_connection()
    try:
        offset = (page - 1) * size
        total = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]

        rows = conn.execute(
            f'SELECT * FROM "{table_name}" LIMIT ? OFFSET ?',
            (size, offset),
        ).fetchall()

        columns = [desc[0] for desc in conn.execute(f'SELECT * FROM "{table_name}" LIMIT 0').description]
        data = [dict(zip(columns, row)) for row in rows]

        return {
            "table": table_name,
            "page": page,
            "size": size,
            "total_rows": total,
            "data": data,
        }
    finally:
        conn.close()


@app.get("/api/v1/{table_name}/{pk_value}")
def get_row_by_pk(table_name: str, pk_value: str):
    if table_name not in VALID_TABLES:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    pk_col = PK_COLUMNS.get(table_name)
    if not pk_col:
        raise HTTPException(status_code=400, detail=f"No primary key defined for '{table_name}'")

    conn = get_connection()
    try:
        row = conn.execute(
            f'SELECT * FROM "{table_name}" WHERE "{pk_col}" = ?',
            (pk_value,),
        ).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"Row with {pk_col}='{pk_value}' not found")

        columns = [desc[0] for desc in conn.execute(f'SELECT * FROM "{table_name}" LIMIT 0').description]
        return dict(zip(columns, row))
    finally:
        conn.close()


@app.get("/api/v1/orders/date-range")
def get_orders_by_date_range(
    start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    page: int = Query(1, ge=1),
    size: int = Query(1000, ge=1, le=10000),
):
    conn = get_connection()
    try:
        query = 'SELECT * FROM olist_orders_dataset WHERE 1=1'
        params = []

        if start:
            query += " AND order_purchase_timestamp >= ?"
            params.append(start)
        if end:
            query += " AND order_purchase_timestamp <= ?"
            params.append(end)

        total = conn.execute(f"SELECT COUNT(*) FROM ({query})", params).fetchone()[0]

        offset = (page - 1) * size
        query += " ORDER BY order_purchase_timestamp LIMIT ? OFFSET ?"
        params.extend([size, offset])

        rows = conn.execute(query, params).fetchall()
        columns = [desc[0] for desc in conn.execute("SELECT * FROM olist_orders_dataset LIMIT 0").description]
        data = [dict(zip(columns, row)) for row in rows]

        return {
            "table": "olist_orders_dataset",
            "date_range": {"start": start, "end": end},
            "page": page,
            "size": size,
            "total_rows": total,
            "data": data,
        }
    finally:
        conn.close()
