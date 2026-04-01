-- ============================================================
-- Superstore Analytics: PostgreSQL Schema
-- ============================================================

-- Drop tables if re-running
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS staging CASCADE;


-- Step 1: Create a staging table to hold the raw CSV
CREATE TABLE staging (
    row_id        INT,
    order_id      VARCHAR(25),
    order_date    VARCHAR(15), 
    ship_date     VARCHAR(15),
    ship_mode     VARCHAR(50),
    customer_id   VARCHAR(20),
    customer_name VARCHAR(100),
    segment       VARCHAR(50),
    country       VARCHAR(50),
    city          VARCHAR(100),
    state         VARCHAR(100),
    postal_code   VARCHAR(20),
    region        VARCHAR(50),
    product_id    VARCHAR(25),
    category      VARCHAR(50),
    sub_category  VARCHAR(50),
    product_name  VARCHAR(255),
    sales         NUMERIC(10,4),
    quantity      INT,
    discount      NUMERIC(5,4),
    profit        NUMERIC(10,4)
);

-- Import the Superstore.csv into staging table using PostgreSQL GUI then run below queries

-- ---- CUSTOMERS dimension table ----
CREATE TABLE customers (
    customer_id   VARCHAR(20)  PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    segment       VARCHAR(50)  NOT NULL,
    country       VARCHAR(50),
    city          VARCHAR(100),
    state         VARCHAR(100),
    postal_code   VARCHAR(20),
    region        VARCHAR(50)
);

-- ---- ORDERS fact table ----
CREATE TABLE orders (
    row_id        SERIAL       PRIMARY KEY,
    order_id      VARCHAR(25)  NOT NULL,
    order_date    DATE         NOT NULL,
    ship_date     DATE         NOT NULL,
    ship_mode     VARCHAR(50),
    customer_id   VARCHAR(20)  NOT NULL REFERENCES customers(customer_id),
    product_id    VARCHAR(25)  NOT NULL,
    category      VARCHAR(50),
    sub_category  VARCHAR(50),
    product_name  VARCHAR(255),
    sales         NUMERIC(10,4),
    quantity      INT,
    discount      NUMERIC(5,4),
    profit        NUMERIC(10,4)
);

-- Indexes for query performance
CREATE INDEX idx_orders_customer   ON orders(customer_id);
CREATE INDEX idx_orders_date       ON orders(order_date);
CREATE INDEX idx_orders_category   ON orders(category);

-- Step 2: Insert unique customers
INSERT INTO customers (customer_id, customer_name, segment,
                       country, city, state, postal_code, region)
SELECT DISTINCT
    customer_id, customer_name, segment,
    country, city, state, postal_code::VARCHAR, region
FROM staging
ON CONFLICT (customer_id) DO NOTHING;

-- Step 3: Insert orders (parse MM-DD-YY date format)
INSERT INTO orders (order_id, order_date, ship_date, ship_mode,
                    customer_id, product_id, category, sub_category,
                    product_name, sales, quantity, discount, profit)
SELECT
    order_id,
    TO_DATE(order_date, 'MM-DD-YY'),
    TO_DATE(ship_date,  'MM-DD-YY'),
    ship_mode,
    customer_id,
    product_id,
    category,
    sub_category,
    product_name,
    sales,
    quantity,
    discount,
    profit
FROM staging;

-- Testing
SELECT * FROM orders;
SELECT * FROM customers;

-- ============================================================
-- Superstore Analytics: KPI Queries
-- ============================================================

-- ── 1. OVERALL BUSINESS SUMMARY ────────────────────────────
SELECT
    COUNT(DISTINCT order_id)                                AS total_orders,
    COUNT(DISTINCT customer_id)                             AS total_customers,
    ROUND(SUM(sales),  2)                          			AS total_revenue,
    ROUND(SUM(profit), 2)                          			AS total_profit,
    ROUND(AVG(profit / NULLIF(sales, 0) * 100), 2) AS avg_profit_margin_pct
FROM orders;


-- ── 2. REVENUE & PROFIT BY YEAR ────────────────────────────
SELECT
    EXTRACT(YEAR FROM order_date)    AS year,
    ROUND(SUM(sales),  2)   AS revenue,
    ROUND(SUM(profit), 2)   AS profit,
    ROUND(SUM(quantity), 0) AS units_sold,
    COUNT(DISTINCT order_id)         AS orders
FROM orders
GROUP BY EXTRACT(YEAR FROM order_date)
ORDER BY EXTRACT(YEAR FROM order_date);


-- ── 3. REVENUE BY CATEGORY & SUB-CATEGORY ──────────────────
SELECT
    category,
    sub_category,
    ROUND(SUM(sales),  2)                          AS revenue,
    ROUND(SUM(profit), 2)                          AS profit,
    ROUND(SUM(profit) / NULLIF(SUM(sales), 0) * 100, 2)    AS margin_pct
FROM orders
GROUP BY category, sub_category
ORDER BY revenue DESC;


-- ── 4. TOP 10 CUSTOMERS BY REVENUE ─────────────────────────
SELECT
    c.customer_id,
    c.customer_name,
    c.segment,
    c.region,
    ROUND(SUM(o.sales),  2) AS total_revenue,
    ROUND(SUM(o.profit), 2) AS total_profit,
    COUNT(DISTINCT o.order_id)       AS order_count
FROM orders o
JOIN customers c 
ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name, c.segment, c.region
ORDER BY total_revenue DESC
LIMIT 10;


-- ── 5. MONTHLY SALES TREND ─────────────────────────────────
SELECT
    DATE_TRUNC('month', order_date)  AS month,
    ROUND(SUM(sales),  2)   AS monthly_revenue,
    ROUND(SUM(profit), 2)   AS monthly_profit,
    COUNT(DISTINCT order_id)         AS orders
FROM orders
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY DATE_TRUNC('month', order_date);


-- ── 6. SALES BY REGION & SEGMENT ───────────────────────────
SELECT
    c.region,
    c.segment,
    ROUND(SUM(o.sales),  2) AS revenue,
    ROUND(SUM(o.profit), 2) AS profit
FROM orders o
JOIN customers c 
ON o.customer_id = c.customer_id
GROUP BY c.region, c.segment
ORDER BY revenue DESC;


-- ── 7. DISCOUNT IMPACT ANALYSIS ────────────────────────────
SELECT
    CASE
        WHEN discount = 0      THEN 'No Discount'
        WHEN discount <= 0.10  THEN '1-10%'
        WHEN discount <= 0.20  THEN '11-20%'
        WHEN discount <= 0.30  THEN '21-30%'
        ELSE '31%+'
    END                                                     AS discount_band,
    COUNT(*)                                                AS transactions,
    ROUND(AVG(sales),  2)                          AS avg_order_value,
    ROUND(AVG(profit), 2)                          AS avg_profit,
    ROUND(SUM(profit) / NULLIF(SUM(sales), 0) * 100, 2)    AS margin_pct
FROM orders
GROUP BY discount_band
ORDER BY transactions DESC;


-- ── 8. SHIP MODE PERFORMANCE ───────────────────────────────
SELECT
    ship_mode,
    COUNT(DISTINCT order_id)                        AS orders,
    ROUND(AVG(ship_date - order_date), 1)  AS avg_ship_days,
    ROUND(SUM(sales), 2)                   AS revenue
FROM orders
GROUP BY ship_mode
ORDER BY orders DESC;


-- ── 9. PRODUCT PROFITABILITY (LOSS LEADERS) ───────────────
SELECT
    product_id,
    product_name,
    category,
    sub_category,
    ROUND(SUM(sales),  2)                       AS total_sales,
    ROUND(SUM(profit), 2)                       AS total_profit,
    ROUND(SUM(profit) / NULLIF(SUM(sales), 0) * 100, 2) AS margin_pct
FROM orders
GROUP BY product_id, product_name, category, sub_category
HAVING SUM(profit) < 0
ORDER BY total_profit ASC
LIMIT 20;
