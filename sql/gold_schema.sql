-- Dimension Tables (Star Schema for Olist Data Warehouse)

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state VARCHAR(2)
);

CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_key SERIAL PRIMARY KEY,
    seller_id VARCHAR(50) UNIQUE NOT NULL,
    seller_zip_code_prefix INT,
    seller_city VARCHAR(100),
    seller_state VARCHAR(2)
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    product_category_english VARCHAR(100),
    product_weight_g DOUBLE PRECISION,
    product_length_cm DOUBLE PRECISION,
    product_volume_cm3 DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS dim_order_status (
    status_key SERIAL PRIMARY KEY,
    status_name VARCHAR(20) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week VARCHAR(10),
    month_name VARCHAR(10),
    quarter INT,
    year INT,
    is_holiday_brazil BOOLEAN DEFAULT FALSE
);

-- Fact Table

CREATE TABLE IF NOT EXISTS fct_order_items (
    order_item_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    customer_key INT REFERENCES dim_customers(customer_key),
    product_key INT REFERENCES dim_products(product_key),
    seller_key INT REFERENCES dim_sellers(seller_key),
    order_date_key INT REFERENCES dim_date(date_key),
    status_key INT REFERENCES dim_order_status(status_key),
    item_price NUMERIC(10,2),
    freight_value NUMERIC(10,2),
    review_score INT,
    processing_time_hours NUMERIC(10,2),
    shipping_time_days INT
);
