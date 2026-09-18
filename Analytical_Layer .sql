 """Q1: Seller x Product Category Late Delivery Rates (delivered_at > est_delivery_date)"""
CREATE OR REPLACE VIEW gold_vw_seller_late_deliveries AS
SELECT 
    s.seller_id,
    p.english_category,
    COUNT(f.order_id) AS total_orders,
    SUM(CASE WHEN f.delivered_at > f.est_delivery_date THEN 1 ELSE 0 END) AS late_orders,
    ROUND(SUM(CASE WHEN f.delivered_at > f.est_delivery_date THEN 1.0 ELSE 0.0 END) / COUNT(f.order_id) * 100, 2) AS late_delivery_rate_pct
FROM gold_fact_order_item f
JOIN gold_dim_seller s ON f.seller_sk = s.seller_sk
JOIN gold_dim_product p ON f.product_sk = p.product_sk
WHERE f.delivered_at IS NOT NULL AND f.est_delivery_date IS NOT NULL
GROUP BY s.seller_id, p.english_category
HAVING total_orders > 5;


-- Q2: Average Purchase-to-Delivery Lag by State
CREATE OR REPLACE VIEW gold_vw_delivery_lag_by_state AS
SELECT 
    c.state,
    ROUND(AVG(datediff(f.delivered_at, f.purchased_at)), 1) AS avg_delivery_days,
    COUNT(f.order_id) AS delivered_order_count
FROM gold_fact_order_item f
JOIN gold_dim_customer c ON f.customer_sk = c.customer_sk
WHERE f.delivered_at IS NOT NULL AND f.purchased_at IS NOT NULL
GROUP BY c.state;


-- Q3: Product Categories with Sharpest MoM Rise in Negative Reviews (score <= 2)
CREATE OR REPLACE VIEW gold_vw_negative_reviews_mom AS
WITH monthly_negatives AS (
    SELECT 
        p.english_category,
        date_format(d.date, 'yyyy-MM') as year_month,
        COUNT(f.order_id) as negative_review_count
    FROM gold_fact_order_item f
    JOIN gold_dim_product p ON f.product_sk = p.product_sk
    JOIN gold_dim_date d ON f.date_sk = d.date_sk
    WHERE f.review_score <= 2 AND f.review_score > 0
    GROUP BY p.english_category, date_format(d.date, 'yyyy-MM')
)
SELECT 
    english_category,
    year_month,
    negative_review_count,
    negative_review_count - LAG(negative_review_count, 1) OVER (PARTITION BY english_category ORDER BY year_month) AS mom_increase
FROM monthly_negatives;


-- Q4 (Custom): Orders with Anomalous Total Value vs Category P95 Baseline
CREATE OR REPLACE VIEW gold_vw_anomalous_order_values AS
WITH category_p95 AS (
    SELECT 
        p.english_category,
        percentile_approx(f.total_item_value, 0.95) as p95_value
    FROM gold_fact_order_item f
    JOIN gold_dim_product p ON f.product_sk = p.product_sk
    GROUP BY p.english_category
)
SELECT 
    f.order_id,
    p.english_category,
    f.total_item_value,
    cp.p95_value
FROM gold_fact_order_item f
JOIN gold_dim_product p ON f.product_sk = p.product_sk
JOIN category_p95 cp ON p.english_category = cp.english_category
WHERE f.total_item_value > cp.p95_value;
