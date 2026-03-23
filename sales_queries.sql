-- =============================================================================
-- Sales Dashboard & SQL Analytics
-- Database: SQLite  |  sales.db
-- =============================================================================


-- =============================================================================
-- SECTION 1: SCHEMA REFERENCE
-- =============================================================================

-- regions     (region_id, region_name)
-- customers   (customer_id, customer_name, email, region_id, segment)
-- products    (product_id, product_name, category, sub_category, unit_price)
-- orders      (order_id, customer_id, order_date, ship_date, status)
-- order_items (item_id, order_id, product_id, quantity, discount, revenue)


-- =============================================================================
-- SECTION 2: KPI QUERIES
-- =============================================================================

-- ----------------------------------------------------------------------------
-- Q1: Monthly Revenue Trend
--     Aggregates total revenue, order count, and average order value per month
-- ----------------------------------------------------------------------------
SELECT
    strftime('%Y-%m', o.order_date)          AS month,
    ROUND(SUM(oi.revenue), 2)                AS total_revenue,
    COUNT(DISTINCT o.order_id)               AS total_orders,
    ROUND(AVG(oi.revenue), 2)                AS avg_order_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY month
ORDER BY month;


-- ----------------------------------------------------------------------------
-- Q2: Top 10 Products by Revenue
--     Ranks products on total revenue with units sold and avg discount
-- ----------------------------------------------------------------------------
SELECT
    p.product_name,
    p.category,
    p.sub_category,
    SUM(oi.quantity)                         AS units_sold,
    ROUND(SUM(oi.revenue), 2)                AS total_revenue,
    ROUND(AVG(oi.discount) * 100, 1)         AS avg_discount_pct
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY p.product_id
ORDER BY total_revenue DESC
LIMIT 10;


-- ----------------------------------------------------------------------------
-- Q3: Regional Sales Performance
--     Uses a window function to compute each region's % share of total revenue
-- ----------------------------------------------------------------------------
SELECT
    r.region_name,
    COUNT(DISTINCT o.order_id)               AS total_orders,
    COUNT(DISTINCT o.customer_id)            AS unique_customers,
    ROUND(SUM(oi.revenue), 2)                AS total_revenue,
    ROUND(
        SUM(oi.revenue) * 1.0 /
        SUM(SUM(oi.revenue)) OVER () * 100, 1
    )                                        AS revenue_share_pct
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN customers   c  ON o.customer_id = c.customer_id
JOIN regions     r  ON c.region_id   = r.region_id
GROUP BY r.region_id
ORDER BY total_revenue DESC;


-- ----------------------------------------------------------------------------
-- Q4: Category Revenue Breakdown (CTE + Window Function)
--     Computes total revenue and percentage share per product category
-- ----------------------------------------------------------------------------
WITH category_totals AS (
    SELECT
        p.category,
        ROUND(SUM(oi.revenue), 2)            AS total_revenue,
        SUM(oi.quantity)                     AS units_sold
    FROM order_items oi
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY p.category
)
SELECT
    category,
    total_revenue,
    units_sold,
    ROUND(
        total_revenue * 100.0 /
        SUM(total_revenue) OVER (), 1
    )                                        AS revenue_share_pct
FROM category_totals
ORDER BY total_revenue DESC;


-- ----------------------------------------------------------------------------
-- Q5: Customer Segment KPIs
--     Revenue, order count, and average order value by business segment
-- ----------------------------------------------------------------------------
SELECT
    c.segment,
    COUNT(DISTINCT o.order_id)               AS total_orders,
    COUNT(DISTINCT o.customer_id)            AS unique_customers,
    ROUND(SUM(oi.revenue), 2)                AS total_revenue,
    ROUND(SUM(oi.revenue) /
          COUNT(DISTINCT o.order_id), 2)     AS avg_order_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN customers   c  ON o.customer_id = c.customer_id
GROUP BY c.segment
ORDER BY total_revenue DESC;


-- =============================================================================
-- SECTION 3: ADVANCED SQL — WINDOW FUNCTIONS
-- =============================================================================

-- ----------------------------------------------------------------------------
-- Q6: Month-over-Month Revenue Growth
--     Uses LAG() to compare each month's revenue with the previous month
-- ----------------------------------------------------------------------------
WITH monthly AS (
    SELECT
        strftime('%Y-%m', o.order_date)      AS month,
        ROUND(SUM(oi.revenue), 2)            AS revenue
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY month
)
SELECT
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month)       AS prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY month))
        / LAG(revenue) OVER (ORDER BY month) * 100, 1
    )                                        AS mom_growth_pct
FROM monthly
ORDER BY month;


-- ----------------------------------------------------------------------------
-- Q7: Running Total Revenue
--     Uses SUM() window function with ROWS UNBOUNDED PRECEDING
-- ----------------------------------------------------------------------------
WITH monthly AS (
    SELECT
        strftime('%Y-%m', o.order_date)      AS month,
        ROUND(SUM(oi.revenue), 2)            AS monthly_revenue
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY month
)
SELECT
    month,
    monthly_revenue,
    ROUND(
        SUM(monthly_revenue) OVER (
            ORDER BY month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2
    )                                        AS running_total
FROM monthly
ORDER BY month;


-- ----------------------------------------------------------------------------
-- Q8: Revenue Rank per Region (RANK + PARTITION BY)
--     Ranks products within each region by revenue
-- ----------------------------------------------------------------------------
SELECT
    r.region_name,
    p.product_name,
    p.category,
    ROUND(SUM(oi.revenue), 2)                AS total_revenue,
    RANK() OVER (
        PARTITION BY r.region_id
        ORDER BY SUM(oi.revenue) DESC
    )                                        AS region_rank
FROM order_items oi
JOIN orders   o ON oi.order_id   = o.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN customers c ON o.customer_id = c.customer_id
JOIN regions   r ON c.region_id   = r.region_id
GROUP BY r.region_id, p.product_id
ORDER BY r.region_name, region_rank;


-- =============================================================================
-- SECTION 4: BUSINESS INSIGHT QUERIES
-- =============================================================================

-- ----------------------------------------------------------------------------
-- Q9: Top 10 Customers by Revenue (CTE + RANK)
-- ----------------------------------------------------------------------------
WITH customer_revenue AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.segment,
        r.region_name,
        ROUND(SUM(oi.revenue), 2)            AS total_revenue,
        COUNT(DISTINCT o.order_id)           AS total_orders
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN customers   c  ON o.customer_id = c.customer_id
    JOIN regions     r  ON c.region_id   = r.region_id
    GROUP BY c.customer_id
),
ranked AS (
    SELECT *,
        RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
    FROM customer_revenue
)
SELECT * FROM ranked WHERE revenue_rank <= 10;


-- ----------------------------------------------------------------------------
-- Q10: Discount Impact Analysis
--       Compares revenue per unit at different discount bands
-- ----------------------------------------------------------------------------
SELECT
    CASE
        WHEN oi.discount = 0         THEN 'No Discount'
        WHEN oi.discount <= 0.10     THEN '1–10%'
        WHEN oi.discount <= 0.20     THEN '11–20%'
        ELSE '20%+'
    END                                      AS discount_band,
    COUNT(*)                                 AS line_items,
    ROUND(SUM(oi.revenue), 2)                AS total_revenue,
    ROUND(AVG(oi.revenue / oi.quantity), 2)  AS avg_revenue_per_unit
FROM order_items oi
GROUP BY discount_band
ORDER BY total_revenue DESC;


-- ----------------------------------------------------------------------------
-- Q11: Quarterly Revenue Summary
-- ----------------------------------------------------------------------------
SELECT
    strftime('%Y', o.order_date)             AS year,
    CASE strftime('%m', o.order_date)
        WHEN '01' THEN 'Q1' WHEN '02' THEN 'Q1' WHEN '03' THEN 'Q1'
        WHEN '04' THEN 'Q2' WHEN '05' THEN 'Q2' WHEN '06' THEN 'Q2'
        WHEN '07' THEN 'Q3' WHEN '08' THEN 'Q3' WHEN '09' THEN 'Q3'
        ELSE 'Q4'
    END                                      AS quarter,
    ROUND(SUM(oi.revenue), 2)                AS quarterly_revenue,
    COUNT(DISTINCT o.order_id)               AS total_orders
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY year, quarter
ORDER BY year, quarter;


-- ----------------------------------------------------------------------------
-- Q12: Average Days to Ship by Region
-- ----------------------------------------------------------------------------
SELECT
    r.region_name,
    ROUND(AVG(
        julianday(o.ship_date) - julianday(o.order_date)
    ), 2)                                    AS avg_days_to_ship,
    COUNT(DISTINCT o.order_id)               AS total_orders
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN regions   r ON c.region_id   = r.region_id
GROUP BY r.region_id
ORDER BY avg_days_to_ship;
