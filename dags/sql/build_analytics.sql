-- Reconstruction complète des tables analytics à partir du DWH.
-- Full rebuild dans une transaction : idempotent par construction et < 2 s
-- pour quelques centaines de milliers de commandes. Les dashboards ne voient
-- jamais d'état intermédiaire (TRUNCATE + INSERT dans le même COMMIT).
BEGIN;

TRUNCATE analytics.daily_summary, analytics.seller_daily, analytics.category_daily;

INSERT INTO analytics.seller_daily (dt, seller_id, orders, revenue, commission)
SELECT dt, seller_id, COUNT(*), SUM(total_amount), SUM(commission_amount)
FROM dwh.fact_orders
WHERE is_valid
GROUP BY dt, seller_id;

INSERT INTO analytics.category_daily (dt, category, orders, revenue, commission)
SELECT f.dt, p.category, COUNT(*), SUM(f.total_amount), SUM(f.commission_amount)
FROM dwh.fact_orders f
JOIN dwh.dim_product p USING (product_id)
WHERE f.is_valid
GROUP BY f.dt, p.category;

INSERT INTO analytics.daily_summary (dt, total_orders, valid_orders, cancelled_orders, total_revenue,
                                     total_commission, avg_basket, active_sellers, top_seller_id,
                                     top_seller_revenue, updated_at)
WITH day_totals AS (
    SELECT dt,
           COUNT(*)                                   AS total_orders,
           COUNT(*) FILTER (WHERE is_valid)           AS valid_orders,
           COUNT(*) FILTER (WHERE NOT is_valid)       AS cancelled_orders,
           COALESCE(SUM(total_amount) FILTER (WHERE is_valid), 0) AS total_revenue,
           SUM(commission_amount)                     AS total_commission,
           ROUND(AVG(total_amount) FILTER (WHERE is_valid), 2)    AS avg_basket,
           COUNT(DISTINCT seller_id) FILTER (WHERE is_valid)      AS active_sellers
    FROM dwh.fact_orders
    GROUP BY dt
),
top_seller AS (
    SELECT DISTINCT ON (dt) dt, seller_id, revenue
    FROM analytics.seller_daily
    ORDER BY dt, revenue DESC
)
SELECT d.dt, d.total_orders, d.valid_orders, d.cancelled_orders, d.total_revenue,
       d.total_commission, d.avg_basket, d.active_sellers, t.seller_id, t.revenue, now()
FROM day_totals d
LEFT JOIN top_seller t USING (dt);

COMMIT;
