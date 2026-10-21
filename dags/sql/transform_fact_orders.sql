-- Transformation staging.orders → dwh.fact_orders pour la partition {{ ds }}
-- Pattern d'idempotence obligatoire : DELETE + INSERT par partition, dans une
-- seule transaction. Rejouer {{ ds }} N fois donne toujours le même résultat.
BEGIN;

-- 1. Membres inférés (Kimball « late arriving dimensions ») : un vendeur, un
--    produit ou une date vus dans une commande avant le refresh des dimensions
--    sont créés provisoirement pour respecter les FOREIGN KEY. Le DAG
--    marketplace_dims_refresh_daily écrase ensuite ces lignes.
INSERT INTO dwh.dim_date (dt, year, quarter, month, month_name, iso_week, day, day_of_week, day_name, is_weekend)
SELECT d, EXTRACT(YEAR FROM d), EXTRACT(QUARTER FROM d), EXTRACT(MONTH FROM d), to_char(d, 'TMmonth'),
       EXTRACT(WEEK FROM d), EXTRACT(DAY FROM d), EXTRACT(ISODOW FROM d), to_char(d, 'TMday'),
       EXTRACT(ISODOW FROM d) IN (6, 7)
FROM (SELECT DATE '{{ ds }}' AS d) AS x
ON CONFLICT (dt) DO NOTHING;

INSERT INTO dwh.dim_seller (seller_id, seller_name, is_inferred)
SELECT DISTINCT seller_id, 'Vendeur #' || seller_id || ' (inféré)', TRUE
FROM staging.orders
WHERE dt = '{{ ds }}'
ON CONFLICT (seller_id) DO NOTHING;

INSERT INTO dwh.dim_product (product_id, product_name, category, seller_id, is_inferred)
SELECT DISTINCT ON (product_id) product_id, 'Produit #' || product_id || ' (inféré)', 'Non catégorisé', seller_id, TRUE
FROM staging.orders
WHERE dt = '{{ ds }}'
ON CONFLICT (product_id) DO NOTHING;

-- 2. Idempotence : on remplace entièrement la partition du jour
DELETE FROM dwh.fact_orders WHERE dt = '{{ ds }}';

INSERT INTO dwh.fact_orders (
    order_id, dt, order_ts, seller_id, product_id, customer_id, quantity,
    unit_price, total_amount, commission_amount, status, is_valid,
    payment_method, shipping_country
)
SELECT
    order_id,
    dt,
    order_ts,
    seller_id,
    product_id,
    customer_id,
    quantity,
    unit_price,
    total_amount,
    CASE WHEN status IN ('cancelled', 'refunded') THEN 0
         ELSE ROUND(total_amount * {{ params.commission_rate }}, 2) END,
    status,
    status NOT IN ('cancelled', 'refunded'),
    payment_method,
    shipping_country
FROM staging.orders
WHERE dt = '{{ ds }}';

COMMIT;
