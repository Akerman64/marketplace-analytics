"""Pipeline principal : API → MinIO (raw) → staging.orders → DQ → dwh.fact_orders.

    extract_orders_to_raw → load_staging_orders → data_quality_staging → dq_branch
        ├─ DQ OK → transform_fact_orders (DELETE + INSERT idempotent) → cleanup_staging
        └─ DQ KO → alert_dq_failure (au lieu du load DWH)
"""

from __future__ import annotations

import json
from datetime import datetime

from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from marketplace.common import (
    API_CONN_ID, DEFAULT_ARGS, DWH_CONN_ID, FACT_ORDERS_ASSET, S3_CONN_ID,
    copy_records, read_raw_json, upload_raw_json,
)
from marketplace.hooks import MarketplaceAPIHook
from marketplace.operators import DataQualityOperator

ORDER_COLUMNS = [
    "dt", "order_id", "order_ts", "customer_id", "seller_id", "product_id", "quantity",
    "unit_price", "total_amount", "status", "payment_method", "shipping_country",
]


@dag(
    dag_id="marketplace_orders_ingest_daily",
    description="Ingestion quotidienne des commandes : API → MinIO → staging → DWH",
    schedule="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=3,
    default_args=DEFAULT_ARGS,
    params={"commission_rate": 0.12, "staging_retention_days": 35},
    template_searchpath=["/opt/airflow/dags/sql"],
    tags=["marketplace", "ingestion", "dwh"],
    doc_md=__doc__,
)
def marketplace_orders_ingest_daily():

    @task
    def extract_orders_to_raw(ds: str | None = None) -> dict:
        """Appelle l'API via le Custom Hook et archive le JSON brut dans MinIO."""
        orders = MarketplaceAPIHook(API_CONN_ID).get_orders(ds)
        key = upload_raw_json(S3Hook(aws_conn_id=S3_CONN_ID), "orders", ds, orders)
        return {"key": key, "count": len(orders)}

    @task
    def load_staging_orders(raw: dict, ds: str | None = None) -> int:
        """Relit le raw MinIO et recharge la partition staging du jour (DELETE + COPY)."""
        orders = read_raw_json(S3Hook(aws_conn_id=S3_CONN_ID), raw["key"])
        for o in orders:
            o["dt"] = ds
        n = copy_records(
            PostgresHook(postgres_conn_id=DWH_CONN_ID), "staging.orders", ORDER_COLUMNS, orders,
            delete_sql="DELETE FROM staging.orders WHERE dt = %s", delete_params=(ds,),
        )
        print(f"staging.orders : {n} lignes chargées pour {ds}")
        return n

    data_quality = DataQualityOperator(
        task_id="data_quality_staging",
        conn_id=DWH_CONN_ID,
        table="staging.orders",
        partition_filter="dt = '{{ ds }}'",
        rules=[
            {"type": "not_empty"},
            {"type": "not_null", "columns": ["order_id", "order_ts", "seller_id", "product_id",
                                             "quantity", "unit_price", "total_amount"]},
            {"type": "no_future_date", "column": "order_ts"},
        ],
    )

    @task.branch
    def dq_branch(dq_result: dict) -> str:
        return "transform_fact_orders" if dq_result["passed"] else "alert_dq_failure"

    transform = SQLExecuteQueryOperator(
        task_id="transform_fact_orders",
        conn_id=DWH_CONN_ID,
        sql="transform_fact_orders.sql",
        autocommit=True,              # le fichier gère lui-même BEGIN / COMMIT
        outlets=[FACT_ORDERS_ASSET],  # déclenche marketplace_analytics_aggregate_daily
    )

    @task
    def alert_dq_failure(dq_result: dict, ds: str | None = None, dag_run=None) -> None:
        failures = [r for r in dq_result["results"] if not r["passed"]]
        PostgresHook(postgres_conn_id=DWH_CONN_ID).run(
            "INSERT INTO analytics.dq_alerts (dag_id, run_id, dt, failures) VALUES (%s, %s, %s, %s)",
            parameters=(dag_run.dag_id, dag_run.run_id, ds, json.dumps(failures)),
        )
        raise AirflowFailException(
            f"🚨 Data quality KO pour {ds} — chargement DWH annulé : {failures}")

    cleanup_staging = SQLExecuteQueryOperator(
        task_id="cleanup_staging",
        conn_id=DWH_CONN_ID,
        sql="DELETE FROM staging.orders WHERE dt < DATE '{{ ds }}' - {{ params.staging_retention_days }};",
    )

    raw = extract_orders_to_raw()
    loaded = load_staging_orders(raw)
    loaded >> data_quality
    branch = dq_branch(data_quality.output)
    branch >> [transform, alert_dq_failure(data_quality.output)]
    transform >> cleanup_staging


marketplace_orders_ingest_daily()
