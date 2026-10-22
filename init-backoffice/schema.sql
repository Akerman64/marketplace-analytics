-- =============================================================================
-- Base transactionnelle (OLTP) du backoffice Maelys — MySQL 8.4
-- C'est la base « du site » : elle enregistre chaque vente au moment où elle a
-- lieu. L'équipe data n'y accède jamais directement : elle passe par l'API.
-- Exécuté automatiquement au premier démarrage du conteneur mysql-backoffice.
-- =============================================================================

CREATE TABLE IF NOT EXISTS sellers (
    seller_id      INT          PRIMARY KEY,
    name           VARCHAR(120) NOT NULL,
    seller_type    VARCHAR(20)  NOT NULL,          -- artisan | dnvb | revendeur
    country        CHAR(2)      NOT NULL,
    city           VARCHAR(60)  NOT NULL,
    joined_date    DATE         NOT NULL,
    main_category  VARCHAR(60)  NOT NULL,
    is_active      BOOLEAN      NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    product_id  INT           PRIMARY KEY,
    name        VARCHAR(120)  NOT NULL,
    category    VARCHAR(60)   NOT NULL,
    seller_id   INT           NOT NULL,
    price       DECIMAL(10,2) NOT NULL,
    created_at  DATE          NOT NULL,
    CONSTRAINT fk_products_seller FOREIGN KEY (seller_id) REFERENCES sellers (seller_id),
    INDEX idx_products_category (category)
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id  INT          PRIMARY KEY,
    first_name   VARCHAR(60)  NOT NULL,
    last_name    VARCHAR(60)  NOT NULL,
    email        VARCHAR(120) NOT NULL,
    country      CHAR(2)      NOT NULL,
    signup_date  DATE         NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id          VARCHAR(30)   PRIMARY KEY,
    order_date        DATE          NOT NULL,
    order_ts          DATETIME      NOT NULL,
    customer_id       INT           NOT NULL,
    seller_id         INT           NOT NULL,
    product_id        INT           NOT NULL,
    quantity          INT           NOT NULL,
    unit_price        DECIMAL(10,2) NOT NULL,
    total_amount      DECIMAL(12,2) NOT NULL,
    status            VARCHAR(20)   NOT NULL,      -- paid | shipped | delivered | cancelled | refunded
    payment_method    VARCHAR(20)   NOT NULL,
    shipping_country  CHAR(2)       NOT NULL,
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
    CONSTRAINT fk_orders_seller   FOREIGN KEY (seller_id)   REFERENCES sellers (seller_id),
    CONSTRAINT fk_orders_product  FOREIGN KEY (product_id)  REFERENCES products (product_id),
    INDEX idx_orders_date (order_date)
);

-- Journal des journées d'activité déjà enregistrées par le simulateur du site
CREATE TABLE IF NOT EXISTS recorded_days (
    order_date   DATE      PRIMARY KEY,
    nb_orders    INT       NOT NULL DEFAULT 0,
    recorded_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
