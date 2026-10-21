"""Constantes et helpers partagés par les DAGs MarketPlace."""

from __future__ import annotations

import csv
import io
import json
from datetime import timedelta

from airflow.sdk import Asset

DWH_CONN_ID = "postgres_dwh"
S3_CONN_ID = "minio_s3"
API_CONN_ID = "marketplace_api"
DATA_LAKE_BUCKET = "data-lake"

# Assets : relient le pipeline d'ingestion au DAG d'agrégation (asset-scheduling)
FACT_ORDERS_ASSET = Asset("postgres://postgres-dwh:5432/dwh/dwh/fact_orders")
DIMENSIONS_ASSET = Asset("postgres://postgres-dwh:5432/dwh/dwh/dim_product")
ANALYTICS_ASSET = Asset("postgres://postgres-dwh:5432/dwh/analytics/daily_summary")

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def raw_key(entity: str, ds: str) -> str:
    """Partition Hive-style du raw layer : raw/<entité>/dt=YYYY-MM-DD/data.json"""
    return f"raw/{entity}/dt={ds}/data.json"


def upload_raw_json(s3_hook, entity: str, ds: str, records: list[dict]) -> str:
    key = raw_key(entity, ds)
    s3_hook.load_string(
        json.dumps(records, ensure_ascii=False),
        key=key,
        bucket_name=DATA_LAKE_BUCKET,
        replace=True,
    )
    return key


def read_raw_json(s3_hook, key: str) -> list[dict]:
    return json.loads(s3_hook.read_key(key=key, bucket_name=DATA_LAKE_BUCKET))


def copy_records(pg_hook, table: str, columns: list[str], records: list[dict],
                 delete_sql: str | None = None, delete_params: tuple | None = None) -> int:
    """Chargement en masse (COPY) dans une seule transaction, précédé d'un DELETE optionnel."""
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    for rec in records:
        writer.writerow(["" if rec.get(c) is None else rec.get(c) for c in columns])
    buffer.seek(0)

    conn = pg_hook.get_conn()
    try:
        with conn.cursor() as cur:
            if delete_sql:
                cur.execute(delete_sql, delete_params)
            cur.copy_expert(
                f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '')",
                buffer,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return len(records)
