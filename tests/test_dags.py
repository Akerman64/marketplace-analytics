"""Tests de structure des DAGs (exigences §12.2 du cahier des charges)."""

import os
import re

from conftest import DAGS_FOLDER

EXPECTED_DAGS = {
    "marketplace_dims_refresh_daily",
    "marketplace_orders_ingest_daily",
    "marketplace_analytics_aggregate_daily",
    "marketplace_anomaly_detect_daily",
}
INGEST = "marketplace_orders_ingest_daily"


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"Erreurs d'import : {dagbag.import_errors}"


def test_expected_dags_are_present(dagbag):
    assert EXPECTED_DAGS <= set(dagbag.dag_ids)


def test_all_dags_catchup_false(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.catchup is False, f"{dag_id} doit avoir catchup=False"


def test_all_dags_have_retries_and_tags(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"{dag_id} sans tags"
        for task in dag.tasks:
            assert task.retries >= 1, f"{dag_id}.{task.task_id} sans retry"


def test_marketplace_ingest_has_idempotent_transform(dagbag):
    """La transformation staging → dwh doit faire DELETE puis INSERT sur la partition {{ ds }}."""
    dag = dagbag.get_dag(INGEST)
    transform = dag.get_task("transform_fact_orders")
    sql = transform.sql
    if sql.endswith(".sql"):
        with open(os.path.join(DAGS_FOLDER, "sql", sql)) as f:
            sql = f.read()
    normalized = re.sub(r"\s+", " ", sql)

    delete_pos = normalized.find("DELETE FROM dwh.fact_orders WHERE dt = '{{ ds }}'")
    insert_pos = normalized.find("INSERT INTO dwh.fact_orders")
    assert delete_pos != -1, "DELETE par partition {{ ds }} manquant"
    assert insert_pos > delete_pos, "INSERT doit suivre le DELETE"
    assert "WHERE dt = '{{ ds }}'" in normalized[insert_pos:], "INSERT non filtré sur {{ ds }}"
    assert "BEGIN;" in normalized and "COMMIT;" in normalized, "DELETE + INSERT doivent être dans une transaction"
    assert not re.search(r"'20\d\d-\d\d-\d\d'", normalized), "Date en dur dans le SQL"


def test_ingest_task_chain(dagbag):
    dag = dagbag.get_dag(INGEST)
    t = dag.get_task
    assert t("load_staging_orders").task_id in t("extract_orders_to_raw").downstream_task_ids
    assert "data_quality_staging" in t("load_staging_orders").downstream_task_ids
    assert {"transform_fact_orders", "alert_dq_failure"} <= t("dq_branch").downstream_task_ids
    assert "cleanup_staging" in t("transform_fact_orders").downstream_task_ids


def test_analytics_dag_is_asset_scheduled(dagbag):
    ingest = dagbag.get_dag(INGEST)
    analytics = dagbag.get_dag("marketplace_analytics_aggregate_daily")
    outlets = {getattr(a, "uri", None) for a in ingest.get_task("transform_fact_orders").outlets}
    assert "postgres://postgres-dwh:5432/dwh/dwh/fact_orders" in outlets
    assert "fact_orders" in str(analytics.timetable.summary) or "Asset" in type(analytics.timetable).__name__


def test_no_hardcoded_api_token():
    for root in (DAGS_FOLDER, os.path.join(os.path.dirname(DAGS_FOLDER), "plugins")):
        for dirpath, _, files in os.walk(root):
            for name in files:
                if name.endswith((".py", ".sql")):
                    with open(os.path.join(dirpath, name)) as f:
                        assert "formation-token-2026" not in f.read(), f"Token en dur dans {name}"
