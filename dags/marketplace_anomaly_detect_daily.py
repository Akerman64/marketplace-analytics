"""Détection d'anomalies commerciales (bonus).

- revenue_drop     : CA du jour < 70 % de la moyenne mobile des 7 jours précédents (US-05)
- suspicious_price : prix de vente ≥ 10× ou ≤ 1/10 du prix catalogue du produit

Les anomalies sont écrites (idempotent : DELETE + INSERT par dt) dans
analytics.anomalies puis remontées par la tâche d'alerte.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag, task

from marketplace.common import DEFAULT_ARGS, DWH_CONN_ID


@dag(
    dag_id="marketplace_anomaly_detect_daily",
    description="Détection des chutes de CA et des prix suspects",
    schedule="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=3,
    default_args=DEFAULT_ARGS,
    params={"revenue_drop_threshold": 0.7, "price_ratio_threshold": 10},
    tags=["marketplace", "anomalies", "bonus"],
    doc_md=__doc__,
)
def marketplace_anomaly_detect_daily():

    wait_for_ingest = ExternalTaskSensor(
        task_id="wait_for_orders_ingest",
        external_dag_id="marketplace_orders_ingest_daily",
        external_task_id="transform_fact_orders",
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        skipped_states=["skipped"],
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 3600,
    )

    @task
    def detect_revenue_drop(ds: str | None = None, params=None) -> dict:
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        row = hook.get_first(
            """
            WITH daily AS (
                SELECT dt, SUM(total_amount) AS revenue
                FROM dwh.fact_orders
                WHERE is_valid AND dt BETWEEN %(ds)s::date - 7 AND %(ds)s::date
                GROUP BY dt
            )
            SELECT (SELECT revenue FROM daily WHERE dt = %(ds)s::date),
                   (SELECT AVG(revenue) FROM daily WHERE dt < %(ds)s::date),
                   (SELECT COUNT(*) FROM daily WHERE dt < %(ds)s::date)
            """,
            parameters={"ds": ds},
        )
        revenue, avg_7d, n_days = row
        hook.run("DELETE FROM analytics.anomalies WHERE dt = %s AND anomaly_type = 'revenue_drop'",
                 parameters=(ds,))
        if revenue is None or avg_7d is None or n_days < 3:
            print(f"{ds} : historique insuffisant ({n_days} jours) — pas d'évaluation")
            return {"evaluated": False}

        ratio = float(revenue) / float(avg_7d)
        threshold = float(params["revenue_drop_threshold"])
        is_anomaly = ratio < threshold
        if is_anomaly:
            hook.run(
                """INSERT INTO analytics.anomalies (dt, anomaly_type, entity, observed, expected, ratio, details)
                   VALUES (%s, 'revenue_drop', 'marketplace', %s, %s, %s, %s)""",
                parameters=(ds, revenue, round(avg_7d, 2), round(ratio, 3),
                            f"CA à {ratio:.0%} de la moyenne 7j (seuil {threshold:.0%})"),
            )
        print(f"{ds} : CA {revenue:,.2f} € vs moyenne 7j {avg_7d:,.2f} € → ratio {ratio:.1%}"
              + ("  🚨 ANOMALIE" if is_anomaly else "  ✅ normal"))
        return {"evaluated": True, "anomaly": is_anomaly, "ratio": round(ratio, 3)}

    @task
    def detect_suspicious_prices(ds: str | None = None, params=None) -> int:
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        hook.run(
            """
            DELETE FROM analytics.anomalies WHERE dt = %(ds)s AND anomaly_type = 'suspicious_price';
            INSERT INTO analytics.anomalies (dt, anomaly_type, entity, observed, expected, ratio, details)
            SELECT f.dt, 'suspicious_price', f.order_id, f.unit_price, p.list_price,
                   ROUND(f.unit_price / p.list_price, 3),
                   'Produit ' || p.product_id || ' (' || p.category || ') vendu par le vendeur ' || f.seller_id
            FROM dwh.fact_orders f
            JOIN dwh.dim_product p USING (product_id)
            WHERE f.dt = %(ds)s AND p.list_price > 0
              AND (f.unit_price >= p.list_price * %(r)s OR f.unit_price <= p.list_price / %(r)s);
            """,
            parameters={"ds": ds, "r": float(params["price_ratio_threshold"])},
        )
        n = hook.get_first(
            "SELECT COUNT(*) FROM analytics.anomalies WHERE dt = %s AND anomaly_type = 'suspicious_price'",
            parameters=(ds,))[0]
        print(f"{ds} : {n} prix suspect(s)")
        return n

    @task
    def raise_alerts(revenue: dict, n_prices: int, ds: str | None = None) -> None:
        if revenue.get("anomaly"):
            print(f"🚨 ALERTE CA — {ds} : CA à {revenue['ratio']:.0%} de la moyenne 7 jours")
        if n_prices:
            print(f"⚠️  {n_prices} commande(s) à prix suspect le {ds} — voir analytics.anomalies")
        if not revenue.get("anomaly") and not n_prices:
            print(f"✅ Aucune anomalie le {ds}")

    revenue = detect_revenue_drop()
    prices = detect_suspicious_prices()
    wait_for_ingest >> [revenue, prices]
    raise_alerts(revenue, prices)


marketplace_anomaly_detect_daily()
