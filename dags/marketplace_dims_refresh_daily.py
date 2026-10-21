"""Refresh quotidien des dimensions vendeurs et produits.

    extract_sellers_to_raw → load_staging_sellers ┐
                                                   ├→ upsert_dimensions
    extract_products_to_raw → load_staging_products ┘
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from marketplace.common import (
    API_CONN_ID, DEFAULT_ARGS, DIMENSIONS_ASSET, DWH_CONN_ID, S3_CONN_ID,
    copy_records, read_raw_json, upload_raw_json,
)
from marketplace.hooks import MarketplaceAPIHook

SELLER_COLUMNS = ["seller_id", "name", "seller_type", "country", "city", "joined_date",
                  "main_category", "is_active"]
PRODUCT_COLUMNS = ["product_id", "name", "category", "seller_id", "price", "created_at"]


@dag(
    dag_id="marketplace_dims_refresh_daily",
    description="Refresh des dimensions dim_seller et dim_product depuis l'API",
    schedule="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    template_searchpath=["/opt/airflow/dags/sql"],
    tags=["marketplace", "dimensions", "dwh"],
    doc_md=__doc__,
)
def marketplace_dims_refresh_daily():

    @task
    def extract_sellers_to_raw(ds: str | None = None) -> str:
        sellers = MarketplaceAPIHook(API_CONN_ID).get_sellers()
        return upload_raw_json(S3Hook(aws_conn_id=S3_CONN_ID), "sellers", ds, sellers)

    @task
    def extract_products_to_raw(ds: str | None = None) -> str:
        products = MarketplaceAPIHook(API_CONN_ID, timeout=120).get_products()
        return upload_raw_json(S3Hook(aws_conn_id=S3_CONN_ID), "products", ds, products)

    @task
    def load_staging_sellers(key: str) -> int:
        rows = read_raw_json(S3Hook(aws_conn_id=S3_CONN_ID), key)
        return copy_records(PostgresHook(postgres_conn_id=DWH_CONN_ID), "staging.sellers",
                            SELLER_COLUMNS, rows, delete_sql="TRUNCATE staging.sellers")

    @task
    def load_staging_products(key: str) -> int:
        rows = read_raw_json(S3Hook(aws_conn_id=S3_CONN_ID), key)
        return copy_records(PostgresHook(postgres_conn_id=DWH_CONN_ID), "staging.products",
                            PRODUCT_COLUMNS, rows, delete_sql="TRUNCATE staging.products")

    upsert = SQLExecuteQueryOperator(
        task_id="upsert_dimensions",
        conn_id=DWH_CONN_ID,
        sql="upsert_dimensions.sql",
        autocommit=True,
        outlets=[DIMENSIONS_ASSET],
    )

    [load_staging_sellers(extract_sellers_to_raw()),
     load_staging_products(extract_products_to_raw())] >> upsert


marketplace_dims_refresh_daily()
