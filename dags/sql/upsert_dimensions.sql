-- Upsert idempotent des dimensions à partir du staging (rejouable à volonté).
-- Écrase aussi les membres inférés créés par le pipeline des commandes.
BEGIN;

INSERT INTO dwh.dim_seller AS d (seller_id, seller_name, seller_type, country, city, joined_date,
                                 main_category, is_active, is_inferred, updated_at)
SELECT seller_id, name, seller_type, country, city, joined_date, main_category, is_active, FALSE, now()
FROM staging.sellers
ON CONFLICT (seller_id) DO UPDATE SET
    seller_name   = EXCLUDED.seller_name,
    seller_type   = EXCLUDED.seller_type,
    country       = EXCLUDED.country,
    city          = EXCLUDED.city,
    joined_date   = EXCLUDED.joined_date,
    main_category = EXCLUDED.main_category,
    is_active     = EXCLUDED.is_active,
    is_inferred   = FALSE,
    updated_at    = now();

INSERT INTO dwh.dim_product AS d (product_id, product_name, category, seller_id, list_price,
                                  created_at, is_inferred, updated_at)
SELECT product_id, name, category, seller_id, price, created_at, FALSE, now()
FROM staging.products
ON CONFLICT (product_id) DO UPDATE SET
    product_name = EXCLUDED.product_name,
    category     = EXCLUDED.category,
    seller_id    = EXCLUDED.seller_id,
    list_price   = EXCLUDED.list_price,
    created_at   = EXCLUDED.created_at,
    is_inferred  = FALSE,
    updated_at   = now();

COMMIT;
