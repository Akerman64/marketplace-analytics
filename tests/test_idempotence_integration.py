"""Test d'intégration : la transformation rejouée 3 fois donne le même résultat en base.

Exécute le vrai fichier SQL (rendu pour une date déjà présente en staging)
contre le Data Warehouse. Ignoré si postgres-dwh n'est pas joignable.
"""

import os

import pytest

from conftest import DAGS_FOLDER

psycopg2 = pytest.importorskip("psycopg2")


@pytest.fixture(scope="module")
def dwh():
    try:
        conn = psycopg2.connect(host="postgres-dwh", port=5432, dbname="dwh",
                                user=os.environ.get("DWH_USER", "dwh_user"),
                                password=os.environ.get("DWH_PASSWORD", "dwh_password_2026"),
                                connect_timeout=3)
    except psycopg2.OperationalError:
        pytest.skip("postgres-dwh injoignable")
    conn.autocommit = True
    yield conn
    conn.close()


def _render(ds: str) -> str:
    with open(os.path.join(DAGS_FOLDER, "sql", "transform_fact_orders.sql")) as f:
        return f.read().replace("{{ ds }}", ds).replace("{{ params.commission_rate }}", "0.12")


def _snapshot(cur, ds):
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT order_id), SUM(total_amount), SUM(commission_amount) "
                "FROM dwh.fact_orders WHERE dt = %s", (ds,))
    return cur.fetchone()


def test_transform_is_idempotent(dwh):
    with dwh.cursor() as cur:
        # Dernière partition déjà validée par la DQ (présente en staging ET dans le DWH)
        cur.execute("SELECT MAX(dt)::text FROM staging.orders s "
                    "WHERE EXISTS (SELECT 1 FROM dwh.fact_orders f WHERE f.dt = s.dt)")
        ds = cur.fetchone()[0]
        if ds is None:
            pytest.skip("staging.orders vide : lancer d'abord le DAG d'ingestion")

        snapshots = []
        for _ in range(3):
            cur.execute(_render(ds))
            snapshots.append(_snapshot(cur, ds))

        cur.execute("SELECT COUNT(*) FROM staging.orders WHERE dt = %s", (ds,))
        staging_count = cur.fetchone()[0]

    assert snapshots[0] == snapshots[1] == snapshots[2]
    count, distinct, _, _ = snapshots[0]
    assert count == distinct == staging_count > 0
