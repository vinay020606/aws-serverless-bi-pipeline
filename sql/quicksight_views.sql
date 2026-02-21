-- ============================================================
--  quicksight_views.sql
--  Business intelligence SQL queries / views for QuickSight.
--  Run these in the Athena Query Editor.
--  QuickSight connects to Athena as its data source and
--  reads the saved query results from S3 (< 6 MB limit applies
--  to SPICE import mode; Direct Query has no such limit).
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- VIEW 1: Total Revenue by Category
--   Powers → QuickSight Bar/Pie chart: Revenue per Category
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW bi_pipeline_db.vw_revenue_by_category AS
SELECT
    category,
    COUNT(order_id)                          AS total_orders,
    SUM(total_price)                         AS total_revenue,
    ROUND(AVG(total_price), 2)               AS avg_order_value
FROM bi_pipeline_db.orders
WHERE status != 'Returned'
GROUP BY category
ORDER BY total_revenue DESC;


-- ────────────────────────────────────────────────────────────
-- VIEW 2: Monthly Revenue Trend
--   Powers → QuickSight Line chart: Revenue over time
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW bi_pipeline_db.vw_monthly_revenue AS
SELECT
    SUBSTR(order_date, 1, 7)                 AS month,          -- 'YYYY-MM'
    COUNT(order_id)                          AS total_orders,
    SUM(total_price)                         AS monthly_revenue,
    SUM(quantity)                            AS units_sold
FROM bi_pipeline_db.orders
WHERE status != 'Returned'
GROUP BY SUBSTR(order_date, 1, 7)
ORDER BY month ASC;


-- ────────────────────────────────────────────────────────────
-- VIEW 3: Revenue by Region
--   Powers → QuickSight Map / Geo chart: Regional breakdown
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW bi_pipeline_db.vw_revenue_by_region AS
SELECT
    region,
    COUNT(DISTINCT customer_id)              AS unique_customers,
    COUNT(order_id)                          AS total_orders,
    SUM(total_price)                         AS total_revenue
FROM bi_pipeline_db.orders
GROUP BY region
ORDER BY total_revenue DESC;


-- ────────────────────────────────────────────────────────────
-- VIEW 4: Order Status Distribution
--   Powers → QuickSight Donut chart: Status breakdown
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW bi_pipeline_db.vw_order_status AS
SELECT
    status,
    COUNT(order_id)                          AS order_count,
    ROUND(
        COUNT(order_id) * 100.0 / SUM(COUNT(order_id)) OVER(), 2
    )                                        AS percentage
FROM bi_pipeline_db.orders
GROUP BY status
ORDER BY order_count DESC;


-- ────────────────────────────────────────────────────────────
-- VIEW 5: Top 10 Products by Revenue
--   Powers → QuickSight Horizontal bar chart: Top products
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW bi_pipeline_db.vw_top_products AS
SELECT
    product_id,
    product_name,
    category,
    SUM(quantity)                            AS units_sold,
    SUM(total_price)                         AS total_revenue
FROM bi_pipeline_db.orders
WHERE status != 'Returned'
GROUP BY product_id, product_name, category
ORDER BY total_revenue DESC
LIMIT 10;


-- ────────────────────────────────────────────────────────────
-- VIEW 6: Customer Lifetime Value (CLV)
--   Powers → QuickSight KPI / Scatter chart: CLV analysis
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW bi_pipeline_db.vw_customer_clv AS
SELECT
    customer_id,
    customer_name,
    COUNT(order_id)                          AS total_orders,
    SUM(total_price)                         AS lifetime_value,
    MIN(order_date)                          AS first_order_date,
    MAX(order_date)                          AS last_order_date
FROM bi_pipeline_db.orders
WHERE status != 'Returned'
GROUP BY customer_id, customer_name
ORDER BY lifetime_value DESC;


-- ────────────────────────────────────────────────────────────
-- QUICKSIGHT SETUP INSTRUCTIONS:
-- 1. Open QuickSight → Manage Datasets → New Dataset
-- 2. Choose Athena as data source
-- 3. Select workgroup: primary (or your custom workgroup)
-- 4. Select database: bi_pipeline_db
-- 5. Choose any of the views above (vw_revenue_by_category etc.)
-- 6. Choose SPICE (< 6 MB) or Direct Query mode
-- 7. Build visuals using the available fields
-- ────────────────────────────────────────────────────────────
