#!/usr/bin/env python3
"""Configuration automatique de Metabase via son API REST.

    python3 metabase/setup_metabase.py

1. Crée le compte admin (premier login), la source de données PostgreSQL DWH
   (host postgres-dwh, port interne 5432 — nom Docker, pas localhost) et la
   base MySQL du backoffice en seconde source (pour parcourir ses tables)
2. Crée les questions SQL natives et les 5 dashboards :
     - Executive Summary          (MVP, US-01 / US-02)
     - Top Sellers                (MVP, US-02)
     - Commissions                (bonus Should, US-03)
     - Catégories                 (bonus Should, US-04)
     - Anomalies & Qualité        (bonus Could, US-05)
Rejouable : les dashboards existants de la collection sont archivés puis recréés.
"""

import os
import sys
import time

import requests

MB = os.environ.get("METABASE_URL", "http://localhost:3000")
ADMIN_EMAIL = os.environ.get("METABASE_ADMIN_EMAIL", "admin@maelys.local")
ADMIN_PASSWORD = os.environ.get("METABASE_ADMIN_PASSWORD", "Admin2026!")
DWH = {
    "host": "postgres-dwh", "port": 5432, "dbname": "dwh",
    "user": os.environ.get("DWH_USER", "dwh_user"),
    "password": os.environ.get("DWH_PASSWORD", "dwh_password_2026"),
    "ssl": False,
}
BACKOFFICE = {
    "host": "mysql-backoffice", "port": 3306, "dbname": os.environ.get("BACKOFFICE_DB", "backoffice"),
    "user": os.environ.get("BACKOFFICE_USER", "backoffice_user"),
    "password": os.environ.get("BACKOFFICE_PASSWORD", "backoffice_password_2026"),
    "ssl": False, "additional-options": "allowPublicKeyRetrieval=true",
}
COLLECTION_NAME = "MarketPlace Analytics"

LAST_DAY = "(SELECT MAX(dt) FROM analytics.daily_summary)"


def eur(*cols):
    return {"column_settings": {f'["name","{c}"]': {"number_style": "currency", "currency": "EUR",
                                                    "currency_style": "symbol"} for c in cols}}


# ------------------------------------------------------------------ questions
CARDS = {
    # ---------------- Executive Summary
    "ca_jour": dict(name="CA du jour (vs veille)", display="smartscalar",
                    sql="SELECT dt AS \"Jour\", total_revenue AS \"CA\" FROM analytics.daily_summary ORDER BY dt",
                    viz=eur("CA")),
    "commandes_jour": dict(name="Commandes du jour", display="smartscalar",
                           sql="SELECT dt AS \"Jour\", valid_orders AS \"Commandes\" FROM analytics.daily_summary ORDER BY dt"),
    "panier_jour": dict(name="Panier moyen du jour", display="smartscalar",
                        sql="SELECT dt AS \"Jour\", avg_basket AS \"Panier moyen\" FROM analytics.daily_summary ORDER BY dt",
                        viz=eur("Panier moyen")),
    "vendeurs_actifs_jour": dict(name="Vendeurs actifs du jour", display="smartscalar",
                                 sql="SELECT dt AS \"Jour\", active_sellers AS \"Vendeurs actifs\" FROM analytics.daily_summary ORDER BY dt"),
    "ca_30j": dict(name="CA des 30 derniers jours", display="line",
                   sql=f"""SELECT dt AS "Jour", total_revenue AS "CA"
FROM analytics.daily_summary
WHERE dt > {LAST_DAY} - 30
ORDER BY dt""",
                   viz={"graph.dimensions": ["Jour"], "graph.metrics": ["CA"], "graph.show_values": False,
                        "graph.y_axis.title_text": "CA (€)", **eur("CA")}),
    "top5_jour": dict(name="Top 5 vendeurs du jour", display="row",
                      sql=f"""SELECT s.seller_name AS "Vendeur", sd.revenue AS "CA"
FROM analytics.seller_daily sd
JOIN dwh.dim_seller s USING (seller_id)
WHERE sd.dt = {LAST_DAY}
ORDER BY sd.revenue DESC
LIMIT 5""",
                      viz={"graph.dimensions": ["Vendeur"], "graph.metrics": ["CA"], "graph.show_values": True, **eur("CA")}),

    # ---------------- Top Sellers
    "top10_mois": dict(name="Top 10 vendeurs — mois en cours", display="row",
                       sql=f"""SELECT s.seller_name AS "Vendeur", SUM(sd.revenue) AS "CA", SUM(sd.orders) AS "Commandes"
FROM analytics.seller_daily sd
JOIN dwh.dim_seller s USING (seller_id)
WHERE sd.dt BETWEEN date_trunc('month', {LAST_DAY})::date AND {LAST_DAY}
GROUP BY s.seller_name
ORDER BY "CA" DESC
LIMIT 10""",
                       viz={"graph.dimensions": ["Vendeur"], "graph.metrics": ["CA"], "graph.show_values": True, **eur("CA")}),
    "top3_evolution": dict(name="Évolution du CA des top 3 vendeurs (30 jours)", display="line",
                           sql=f"""WITH top3 AS (
    SELECT seller_id FROM analytics.seller_daily
    WHERE dt > {LAST_DAY} - 30
    GROUP BY seller_id ORDER BY SUM(revenue) DESC LIMIT 3
)
SELECT sd.dt AS "Jour", s.seller_name AS "Vendeur", sd.revenue AS "CA"
FROM analytics.seller_daily sd
JOIN top3 USING (seller_id)
JOIN dwh.dim_seller s USING (seller_id)
WHERE sd.dt > {LAST_DAY} - 30
ORDER BY 1""",
                           viz={"graph.dimensions": ["Jour", "Vendeur"], "graph.metrics": ["CA"], **eur("CA")}),
    "nb_inactifs": dict(name="Vendeurs inactifs", display="scalar",
                        sql=f"""WITH last_sale AS (SELECT seller_id, MAX(dt) AS last_dt FROM analytics.seller_daily GROUP BY seller_id)
SELECT COUNT(*) AS "Vendeurs inactifs"
FROM dwh.dim_seller s LEFT JOIN last_sale l USING (seller_id)
WHERE NOT s.is_inferred AND (l.last_dt IS NULL OR l.last_dt < {LAST_DAY} - 7)"""),
    "liste_inactifs": dict(name="Liste des vendeurs inactifs (> 7 jours sans vente)", display="table",
                           sql=f"""WITH last_sale AS (SELECT seller_id, MAX(dt) AS last_dt FROM analytics.seller_daily GROUP BY seller_id)
SELECT s.seller_id AS "ID", s.seller_name AS "Vendeur", s.seller_type AS "Type", s.country AS "Pays",
       s.main_category AS "Catégorie principale", l.last_dt AS "Dernière vente",
       {LAST_DAY} - l.last_dt AS "Jours sans vente",
       CASE WHEN l.last_dt IS NULL THEN 'Aucune vente chargée'
            WHEN NOT s.is_active THEN 'Compte désactivé'
            ELSE 'À relancer' END AS "Statut"
FROM dwh.dim_seller s LEFT JOIN last_sale l USING (seller_id)
WHERE NOT s.is_inferred AND (l.last_dt IS NULL OR l.last_dt < {LAST_DAY} - 7)
ORDER BY "Jours sans vente" ASC NULLS LAST"""),

    # ---------------- Commissions
    "commission_jour": dict(name="Commission du jour (vs veille)", display="smartscalar",
                            sql="SELECT dt AS \"Jour\", total_commission AS \"Commission\" FROM analytics.daily_summary ORDER BY dt",
                            viz=eur("Commission")),
    "commission_mois": dict(name="Commissions — mois en cours", display="scalar",
                            sql=f"""SELECT SUM(total_commission) AS "Commissions"
FROM analytics.daily_summary
WHERE dt BETWEEN date_trunc('month', {LAST_DAY})::date AND {LAST_DAY}""",
                            viz=eur("Commissions")),
    "commission_30j": dict(name="Commissions par jour (30 jours)", display="bar",
                           sql=f"""SELECT dt AS "Jour", total_commission AS "Commission"
FROM analytics.daily_summary WHERE dt > {LAST_DAY} - 30 ORDER BY dt""",
                           viz={"graph.dimensions": ["Jour"], "graph.metrics": ["Commission"], **eur("Commission")}),
    "commission_recap": dict(name="Récapitulatif quotidien des commissions", display="table",
                             sql=f"""SELECT dt AS "Jour", valid_orders AS "Commandes valides", cancelled_orders AS "Annulées / remboursées",
       total_revenue AS "CA", total_commission AS "Commission (12 %)",
       ROUND(100 * total_commission / NULLIF(total_revenue, 0), 2) AS "Taux effectif (%)"
FROM analytics.daily_summary
WHERE dt > {LAST_DAY} - 31
ORDER BY dt DESC""",
                             viz=eur("CA", "Commission (12 %)")),

    # ---------------- Catégories
    "categorie_pie": dict(name="Répartition du CA par catégorie (30 jours)", display="pie",
                          sql=f"""SELECT category AS "Catégorie", SUM(revenue) AS "CA"
FROM analytics.category_daily WHERE dt > {LAST_DAY} - 30
GROUP BY category ORDER BY "CA" DESC""",
                          viz={"pie.dimension": "Catégorie", "pie.metric": "CA", **eur("CA")}),
    "categorie_semaine": dict(name="CA hebdomadaire par catégorie", display="bar",
                              sql="""SELECT date_trunc('week', dt)::date AS "Semaine", category AS "Catégorie", SUM(revenue) AS "CA"
FROM analytics.category_daily
WHERE dt >= '2026-09-01'
GROUP BY 1, 2 ORDER BY 1""",
                              viz={"graph.dimensions": ["Semaine", "Catégorie"], "graph.metrics": ["CA"],
                                   "stackable.stack_type": "stacked", **eur("CA")}),
    "categorie_table": dict(name="Performance par catégorie (30 jours)", display="table",
                            sql=f"""SELECT category AS "Catégorie", SUM(orders) AS "Commandes", SUM(revenue) AS "CA",
       ROUND(100 * SUM(revenue) / SUM(SUM(revenue)) OVER (), 1) AS "Part du CA (%)",
       ROUND(SUM(revenue) / SUM(orders), 2) AS "Panier moyen", SUM(commission) AS "Commission"
FROM analytics.category_daily WHERE dt > {LAST_DAY} - 30
GROUP BY category ORDER BY "CA" DESC""",
                            viz=eur("CA", "Panier moyen", "Commission")),

    # ---------------- Anomalies & qualité
    "ca_vs_moyenne": dict(name="CA journalier vs moyenne mobile 7 jours et seuil d'alerte", display="line",
                          sql="""SELECT dt AS "Jour", total_revenue AS "CA",
       ROUND(AVG(total_revenue) OVER w, 2) AS "Moyenne 7j",
       ROUND(0.7 * AVG(total_revenue) OVER w, 2) AS "Seuil d'alerte (70 %)"
FROM analytics.daily_summary
WHERE dt >= '2026-09-01'
WINDOW w AS (ORDER BY dt RANGE BETWEEN INTERVAL '7 days' PRECEDING AND INTERVAL '1 day' PRECEDING)
ORDER BY dt""",
                          viz={"graph.dimensions": ["Jour"], "graph.metrics": ["CA", "Moyenne 7j", "Seuil d'alerte (70 %)"],
                               **eur("CA", "Moyenne 7j", "Seuil d'alerte (70 %)")}),
    "anomalies_ca": dict(name="Chutes de CA détectées (< 70 % de la moyenne 7j)", display="table",
                         sql="""SELECT dt AS "Jour", observed AS "CA observé", expected AS "Moyenne 7j",
       ROUND(100 * ratio, 1) AS "Ratio (%)", details AS "Détail"
FROM analytics.anomalies WHERE anomaly_type = 'revenue_drop' ORDER BY dt DESC""",
                         viz=eur("CA observé", "Moyenne 7j")),
    "prix_suspects": dict(name="Prix suspects (×10 ou ÷10 vs prix catalogue)", display="table",
                          sql="""SELECT dt AS "Jour", entity AS "Commande", observed AS "Prix vendu", expected AS "Prix catalogue",
       ratio AS "Ratio", details AS "Détail"
FROM analytics.anomalies WHERE anomaly_type = 'suspicious_price' ORDER BY dt DESC, ratio DESC""",
                          viz=eur("Prix vendu", "Prix catalogue")),
    "dq_alerts": dict(name="Alertes qualité de données (DAG d'ingestion)", display="table",
                      sql="""SELECT created_at AS "Date alerte", dt AS "Partition", run_id AS "Run", failures::text AS "Règles en échec"
FROM analytics.dq_alerts ORDER BY created_at DESC"""),
}

# (titre, description, [(clé carte, col, row, largeur, hauteur)]) — grille de 24 colonnes
DASHBOARDS = [
    ("1 · Executive Summary", "KPIs clés du dernier jour chargé — CEO (US-01, US-02)", [
        ("ca_jour", 0, 0, 6, 4), ("commandes_jour", 6, 0, 6, 4), ("panier_jour", 12, 0, 6, 4),
        ("vendeurs_actifs_jour", 18, 0, 6, 4),
        ("ca_30j", 0, 4, 15, 8), ("top5_jour", 15, 4, 9, 8),
    ]),
    ("2 · Top Sellers", "Performance des vendeurs — CEO (US-02)", [
        ("top10_mois", 0, 0, 12, 9), ("top3_evolution", 12, 0, 12, 9),
        ("nb_inactifs", 0, 9, 6, 8), ("liste_inactifs", 6, 9, 18, 8),
    ]),
    ("3 · Commissions", "Facturation des commissions (12 %) — Finance (US-03)", [
        ("commission_jour", 0, 0, 8, 4), ("commission_mois", 8, 0, 8, 4),
        ("commission_30j", 16, 0, 8, 8), ("commission_recap", 0, 4, 16, 10),
    ]),
    ("4 · Catégories", "Répartition du CA par catégorie — Responsable catalogue (US-04)", [
        ("categorie_pie", 0, 0, 10, 9), ("categorie_table", 10, 0, 14, 9),
        ("categorie_semaine", 0, 9, 24, 8),
    ]),
    ("5 · Anomalies & Qualité", "Détection des chutes de CA, prix suspects et alertes DQ (US-05)", [
        ("ca_vs_moyenne", 0, 0, 24, 8), ("anomalies_ca", 0, 8, 12, 7), ("dq_alerts", 12, 8, 12, 7),
        ("prix_suspects", 0, 15, 24, 7),
    ]),
]


class Metabase:
    def __init__(self, url):
        self.url = url.rstrip("/")
        self.s = requests.Session()

    def call(self, method, path, **kw):
        r = self.s.request(method, f"{self.url}/api{path}", timeout=180, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path} → {r.status_code} : {r.text[:400]}")
        return r.json() if r.text else None

    def wait_ready(self):
        for _ in range(60):
            try:
                if self.s.get(f"{self.url}/api/health", timeout=5).json().get("status") == "ok":
                    return
            except Exception:
                pass
            time.sleep(5)
        sys.exit("Metabase ne répond pas")

    def setup_or_login(self):
        props = self.call("GET", "/session/properties")
        if not props.get("has-user-setup"):
            self.call("POST", "/setup", json={
                "token": props["setup-token"],
                "user": {"first_name": "Admin", "last_name": "Maelys", "email": ADMIN_EMAIL,
                         "password": ADMIN_PASSWORD, "site_name": "Maelys Marketplace"},
                "prefs": {"site_name": "Maelys Marketplace", "site_locale": "fr", "allow_tracking": False},
            })
            print(f"✅ Compte admin créé : {ADMIN_EMAIL}")
        session = self.call("POST", "/session", json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
        self.s.headers["X-Metabase-Session"] = session["id"]

    def database_id(self):
        dbs = self.call("GET", "/database")
        dbs = dbs.get("data", dbs) if isinstance(dbs, dict) else dbs
        for db in dbs:
            if db.get("engine") == "postgres" and db.get("details", {}).get("dbname") == "dwh":
                return db["id"]
        db = self.call("POST", "/database", json={"engine": "postgres", "name": "DWH Maelys",
                                                  "details": DWH, "is_full_sync": True})
        print(f"✅ Source de données DWH ajoutée (id {db['id']})")
        return db["id"]

    def ensure_backoffice_db(self):
        """Base MySQL du site, en lecture : pour parcourir ses tables depuis Metabase."""
        dbs = self.call("GET", "/database")
        dbs = dbs.get("data", dbs) if isinstance(dbs, dict) else dbs
        if any(db.get("engine") == "mysql" for db in dbs):
            return
        db = self.call("POST", "/database", json={"engine": "mysql", "name": "Backoffice Maelys (MySQL)",
                                                  "details": BACKOFFICE, "is_full_sync": True})
        print(f"✅ Source de données Backoffice MySQL ajoutée (id {db['id']})")

    def collection_id(self):
        for c in self.call("GET", "/collection"):
            if c.get("name") == COLLECTION_NAME and not c.get("archived"):
                return c["id"]
        return self.call("POST", "/collection", json={"name": COLLECTION_NAME, "color": "#509EE3"})["id"]

    def clear_collection(self, coll_id):
        items = self.call("GET", f"/collection/{coll_id}/items")
        items = items.get("data", items) if isinstance(items, dict) else items
        for it in items:
            if it["model"] == "dashboard":
                self.call("PUT", f"/dashboard/{it['id']}", json={"archived": True})
            elif it["model"] == "card":
                self.call("PUT", f"/card/{it['id']}", json={"archived": True})


def main():
    mb = Metabase(MB)
    mb.wait_ready()
    mb.setup_or_login()
    db_id = mb.database_id()
    mb.ensure_backoffice_db()
    coll_id = mb.collection_id()
    mb.clear_collection(coll_id)

    card_ids = {}
    for key, c in CARDS.items():
        card = mb.call("POST", "/card", json={
            "name": c["name"],
            "display": c["display"],
            "collection_id": coll_id,
            "dataset_query": {"type": "native", "database": db_id,
                              "native": {"query": c["sql"], "template-tags": {}}},
            "visualization_settings": c.get("viz", {}),
        })
        card_ids[key] = card["id"]
    print(f"✅ {len(card_ids)} questions SQL créées")

    mb.call("PUT", "/setting/enable-public-sharing", json={"value": True})
    links = {}
    for title, description, layout in DASHBOARDS:
        dash = mb.call("POST", "/dashboard", json={"name": title, "description": description,
                                                   "collection_id": coll_id})
        dashcards = [{"id": -(i + 1), "card_id": card_ids[k], "col": col, "row": row,
                      "size_x": w, "size_y": h, "parameter_mappings": [], "visualization_settings": {}}
                     for i, (k, col, row, w, h) in enumerate(layout)]
        mb.call("PUT", f"/dashboard/{dash['id']}", json={"dashcards": dashcards})
        uuid = mb.call("POST", f"/dashboard/{dash['id']}/public_link")["uuid"]
        links[title] = (f"{MB}/dashboard/{dash['id']}", f"{MB}/public/dashboard/{uuid}")
        print(f"✅ Dashboard « {title} » ({len(layout)} visualisations)")

    print("\nDashboards :")
    for title, (url, public) in links.items():
        print(f"  {title:<28} {url}   (public : {public})")


if __name__ == "__main__":
    main()
