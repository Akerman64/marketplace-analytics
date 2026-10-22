"""Accès à la base MySQL du backoffice (base transactionnelle du site)."""

import os
import time
from datetime import date

import pymysql

import generator

ORDER_COLUMNS = ["order_id", "order_date", "order_ts", "customer_id", "seller_id", "product_id",
                 "quantity", "unit_price", "total_amount", "status", "payment_method", "shipping_country"]


def connect(retries: int = 1) -> pymysql.connections.Connection:
    for attempt in range(retries):
        try:
            return pymysql.connect(
                host=os.environ.get("MYSQL_HOST", "mysql-backoffice"),
                port=int(os.environ.get("MYSQL_PORT", "3306")),
                user=os.environ["MYSQL_USER"],
                password=os.environ["MYSQL_PASSWORD"],
                database=os.environ.get("MYSQL_DATABASE", "backoffice"),
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=False,
            )
        except pymysql.err.OperationalError:
            if attempt == retries - 1:
                raise
            time.sleep(3)


def insert_many(cur, table: str, columns: list, rows: list, batch: int = 5000) -> None:
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
    for i in range(0, len(rows), batch):
        cur.executemany(sql, rows[i:i + batch])


def record_day(conn, day: date) -> int:
    """Simulateur du site : enregistre UNE fois les ventes d'une journée dans la base.

    Le verrou est la ligne de recorded_days : si deux appels arrivent en même
    temps, le second attend la fin du premier puis relit ses commandes.
    """
    with conn.cursor() as cur:
        cur.execute("INSERT IGNORE INTO recorded_days (order_date) VALUES (%s)", (day,))
        if cur.rowcount == 1:
            orders = generator.orders_for(day)
            rows = [(o["order_id"], day, o["order_ts"].replace("T", " "), o["customer_id"], o["seller_id"],
                     o["product_id"], o["quantity"], o["unit_price"], o["total_amount"], o["status"],
                     o["payment_method"], o["shipping_country"]) for o in orders]
            insert_many(cur, "orders", ORDER_COLUMNS, rows)
            cur.execute("UPDATE recorded_days SET nb_orders = %s WHERE order_date = %s", (len(rows), day))
            conn.commit()
            return len(rows)
    conn.commit()
    return 0


def read_orders(conn, day: date) -> list:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT order_id, order_ts, customer_id, seller_id, product_id, quantity, unit_price,
                      total_amount, status, payment_method, shipping_country
               FROM orders WHERE order_date = %s ORDER BY order_id""", (day,))
        rows = cur.fetchall()
    for r in rows:
        r["order_ts"] = r["order_ts"].isoformat()
        r["unit_price"] = float(r["unit_price"])
        r["total_amount"] = float(r["total_amount"])
    return rows
