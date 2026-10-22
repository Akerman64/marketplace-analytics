"""Remplissage initial de la base MySQL du backoffice.

Lancé à chaque démarrage par le service backoffice-seed, il ne fait que ce qui
manque (rejouable) :
  1. le catalogue : 2 400 vendeurs, 180 000 produits, 1 200 000 clients ;
  2. l'activité du site sur les SEED_DAYS derniers jours (60 par défaut).
Les autres journées sont enregistrées à la première demande de l'API.
"""

import os
import time
from datetime import date, timedelta

import db
import generator


def count(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS n FROM {table}")
        return cur.fetchone()["n"]


def seed_catalog(conn) -> None:
    with conn.cursor() as cur:
        if count(conn, "sellers") == 0:
            rows = [(s["seller_id"], s["name"], s["seller_type"], s["country"], s["city"],
                     s["joined_date"], s["main_category"], s["is_active"]) for s in generator.sellers()]
            db.insert_many(cur, "sellers", ["seller_id", "name", "seller_type", "country", "city",
                                            "joined_date", "main_category", "is_active"], rows)
            conn.commit()
            print(f"sellers   : {len(rows):>9,} lignes", flush=True)

        if count(conn, "products") == 0:
            rows = [(p["product_id"], p["name"], p["category"], p["seller_id"], p["price"], p["created_at"])
                    for p in generator.products()]
            db.insert_many(cur, "products", ["product_id", "name", "category", "seller_id", "price",
                                             "created_at"], rows)
            conn.commit()
            print(f"products  : {len(rows):>9,} lignes", flush=True)

        done = count(conn, "customers")
        if done < generator.N_CUSTOMERS:
            cols = ["customer_id", "first_name", "last_name", "email", "country", "signup_date"]
            for start in range(done + 1, generator.N_CUSTOMERS + 1, 50_000):
                end = min(start + 50_000, generator.N_CUSTOMERS + 1)
                rows = [tuple(generator.customer(cid)[c] for c in cols) for cid in range(start, end)]
                db.insert_many(cur, "customers", cols, rows)
                conn.commit()
            print(f"customers : {generator.N_CUSTOMERS:>9,} lignes", flush=True)


def seed_activity(conn, days: int) -> None:
    today = date.today()
    added = 0
    for offset in range(days, 0, -1):
        added += db.record_day(conn, today - timedelta(days=offset))
    print(f"orders    : {added:>9,} commandes ajoutées ({days} derniers jours)", flush=True)


def main() -> None:
    t0 = time.time()
    conn = db.connect(retries=40)
    seed_catalog(conn)
    seed_activity(conn, int(os.environ.get("SEED_DAYS", "60")))
    conn.close()
    print(f"Base du backoffice prête en {time.time() - t0:.0f} s", flush=True)


if __name__ == "__main__":
    main()
