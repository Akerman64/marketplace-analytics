-- =============================================================================
-- MarketPlace Analytics — Data Warehouse (PostgreSQL 18)
-- Modèle dimensionnel en étoile (Kimball) réparti sur 3 schémas :
--   staging   : données brutes typées, telles que reçues de l'API
--   dwh       : modèle dimensionnel propre (dimensions + faits)
--   analytics : agrégations pré-calculées pour les dashboards Metabase
-- Exécuté automatiquement au premier démarrage du conteneur postgres-dwh.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS analytics;

-- -----------------------------------------------------------------------------
-- STAGING
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.orders (
    dt               DATE           NOT NULL,      -- partition logique ({{ ds }})
    order_id         TEXT,
    order_ts         TIMESTAMP,
    customer_id      INTEGER,
    seller_id        INTEGER,
    product_id       INTEGER,
    quantity         INTEGER,
    unit_price       NUMERIC(12,2),
    total_amount     NUMERIC(12,2),
    status           TEXT,
    payment_method   TEXT,
    shipping_country TEXT,
    loaded_at        TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_stg_orders_dt ON staging.orders (dt);

CREATE TABLE IF NOT EXISTS staging.sellers (
    seller_id     INTEGER,
    name          TEXT,
    seller_type   TEXT,
    country       TEXT,
    city          TEXT,
    joined_date   DATE,
    main_category TEXT,
    is_active     BOOLEAN,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id  INTEGER,
    name        TEXT,
    category    TEXT,
    seller_id   INTEGER,
    price       NUMERIC(12,2),
    created_at  DATE,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- DWH — DIMENSIONS
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    dt             DATE PRIMARY KEY,
    year           SMALLINT NOT NULL,
    quarter        SMALLINT NOT NULL,
    month          SMALLINT NOT NULL,
    month_name     TEXT     NOT NULL,
    iso_week       SMALLINT NOT NULL,
    day            SMALLINT NOT NULL,
    day_of_week    SMALLINT NOT NULL,      -- 1 = lundi … 7 = dimanche (ISO)
    day_name       TEXT     NOT NULL,
    is_weekend     BOOLEAN  NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_seller (
    seller_id      INTEGER PRIMARY KEY,
    seller_name    TEXT    NOT NULL,
    seller_type    TEXT,
    country        TEXT,
    city           TEXT,
    joined_date    DATE,
    main_category  TEXT,
    is_active      BOOLEAN,
    is_inferred    BOOLEAN NOT NULL DEFAULT FALSE,  -- membre inféré (vendeur vu dans une commande avant le refresh des dimensions)
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_id     INTEGER PRIMARY KEY,
    product_name   TEXT    NOT NULL,
    category       TEXT    NOT NULL,
    seller_id      INTEGER,
    list_price     NUMERIC(12,2),
    created_at     DATE,
    is_inferred    BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dim_product_category ON dwh.dim_product (category);

-- -----------------------------------------------------------------------------
-- DWH — FAITS (grain : une ligne = une commande)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_id           TEXT          PRIMARY KEY,
    dt                 DATE          NOT NULL REFERENCES dwh.dim_date (dt),
    order_ts           TIMESTAMP     NOT NULL,
    seller_id          INTEGER       NOT NULL REFERENCES dwh.dim_seller (seller_id),
    product_id         INTEGER       NOT NULL REFERENCES dwh.dim_product (product_id),
    customer_id        INTEGER       NOT NULL,
    quantity           INTEGER       NOT NULL CHECK (quantity > 0),
    unit_price         NUMERIC(12,2) NOT NULL,
    total_amount       NUMERIC(12,2) NOT NULL,
    commission_amount  NUMERIC(12,2) NOT NULL,
    status             TEXT          NOT NULL,
    is_valid           BOOLEAN       NOT NULL,      -- FALSE si annulée ou remboursée (exclue du CA)
    payment_method     TEXT,
    shipping_country   TEXT,
    loaded_at          TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fact_orders_dt      ON dwh.fact_orders (dt);
CREATE INDEX IF NOT EXISTS idx_fact_orders_seller  ON dwh.fact_orders (seller_id, dt);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product ON dwh.fact_orders (product_id);

-- Calendrier pré-rempli 2024 → 2027 (étendu à la volée par le pipeline si besoin)
INSERT INTO dwh.dim_date
SELECT d::date,
       EXTRACT(YEAR    FROM d)::smallint,
       EXTRACT(QUARTER FROM d)::smallint,
       EXTRACT(MONTH   FROM d)::smallint,
       (ARRAY['janvier','février','mars','avril','mai','juin','juillet','août',
              'septembre','octobre','novembre','décembre'])[EXTRACT(MONTH FROM d)],
       EXTRACT(WEEK    FROM d)::smallint,
       EXTRACT(DAY     FROM d)::smallint,
       EXTRACT(ISODOW  FROM d)::smallint,
       (ARRAY['lundi','mardi','mercredi','jeudi','vendredi','samedi','dimanche'])[EXTRACT(ISODOW FROM d)],
       EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series('2024-01-01'::date, '2027-12-31'::date, interval '1 day') AS d
ON CONFLICT (dt) DO NOTHING;

-- -----------------------------------------------------------------------------
-- ANALYTICS — agrégations pour les dashboards
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.daily_summary (
    dt                  DATE PRIMARY KEY,
    total_orders        INTEGER       NOT NULL,
    valid_orders        INTEGER       NOT NULL,
    cancelled_orders    INTEGER       NOT NULL,
    total_revenue       NUMERIC(14,2) NOT NULL,     -- CA (commandes valides)
    total_commission    NUMERIC(14,2) NOT NULL,     -- 12 % du CA
    avg_basket          NUMERIC(10,2),
    active_sellers      INTEGER       NOT NULL,
    top_seller_id       INTEGER,
    top_seller_revenue  NUMERIC(14,2),
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics.seller_daily (
    dt          DATE    NOT NULL,
    seller_id   INTEGER NOT NULL,
    orders      INTEGER NOT NULL,
    revenue     NUMERIC(14,2) NOT NULL,
    commission  NUMERIC(14,2) NOT NULL,
    PRIMARY KEY (dt, seller_id)
);

CREATE TABLE IF NOT EXISTS analytics.category_daily (
    dt          DATE NOT NULL,
    category    TEXT NOT NULL,
    orders      INTEGER NOT NULL,
    revenue     NUMERIC(14,2) NOT NULL,
    commission  NUMERIC(14,2) NOT NULL,
    PRIMARY KEY (dt, category)
);

CREATE TABLE IF NOT EXISTS analytics.anomalies (
    anomaly_id    BIGSERIAL PRIMARY KEY,
    dt            DATE        NOT NULL,
    anomaly_type  TEXT        NOT NULL,         -- revenue_drop | suspicious_price
    entity        TEXT,                         -- ex : order_id / product_id concerné
    observed      NUMERIC(14,2),
    expected      NUMERIC(14,2),
    ratio         NUMERIC(8,3),
    details       TEXT,
    detected_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_anomalies_dt ON analytics.anomalies (dt, anomaly_type);

CREATE TABLE IF NOT EXISTS analytics.dq_alerts (
    alert_id    BIGSERIAL PRIMARY KEY,
    dag_id      TEXT        NOT NULL,
    run_id      TEXT        NOT NULL,
    dt          DATE        NOT NULL,
    failures    JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
