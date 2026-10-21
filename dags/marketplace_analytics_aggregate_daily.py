"""Construction des tables analytics (daily_summary, seller_daily, category_daily).

Asset-scheduled : se déclenche automatiquement à chaque mise à jour de
dwh.fact_orders par marketplace_orders_ingest_daily.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag

from marketplace.common import ANALYTICS_ASSET, DEFAULT_ARGS, DWH_CONN_ID, FACT_ORDERS_ASSET


@dag(
    dag_id="marketplace_analytics_aggregate_daily",
    description="Agrégations analytics pour Metabase (déclenché par l'asset fact_orders)",
    schedule=[FACT_ORDERS_ASSET],
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    template_searchpath=["/opt/airflow/dags/sql"],
    tags=["marketplace", "analytics"],
    doc_md=__doc__,
)
def marketplace_analytics_aggregate_daily():
    SQLExecuteQueryOperator(
        task_id="build_analytics_tables",
        conn_id=DWH_CONN_ID,
        sql="build_analytics.sql",
        autocommit=True,
        outlets=[ANALYTICS_ASSET],
    )


marketplace_analytics_aggregate_daily()
