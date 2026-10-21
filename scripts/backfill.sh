#!/usr/bin/env bash
# Backfill manuel (US-06) : rejoue le pipeline sur une plage de dates passées.
#   ./scripts/backfill.sh 2026-10-01 2026-10-07          # 7 jours d'historique
# Le DAG d'agrégation analytics se redéclenche tout seul (asset fact_orders) ;
# la détection d'anomalies est rejouée ensuite sur la même plage.
set -euo pipefail
cd "$(dirname "$0")/.."
FROM="${1:?date de début YYYY-MM-DD}"
TO="${2:?date de fin YYYY-MM-DD}"

airflow() { docker compose exec -T airflow-worker airflow "$@" 2>&1; }

wait_runs() {
  local dag="$1"
  until [ "$(docker compose exec -T postgres-airflow psql -U airflow -d airflow -tA -c \
      "SELECT COUNT(*) FROM dag_run WHERE dag_id='${dag}' AND state IN ('queued','running');")" = "0" ]; do
    sleep 10
  done
}

echo "▶ Backfill marketplace_orders_ingest_daily du ${FROM} au ${TO}"
airflow backfill create --dag-id marketplace_orders_ingest_daily --from-date "${FROM}" --to-date "${TO}" --max-active-runs 4 \
  | { grep -c "created backfill dag run" || true; } | xargs -I{} echo "  {} runs créés"
wait_runs marketplace_orders_ingest_daily

echo "▶ Backfill marketplace_anomaly_detect_daily du ${FROM} au ${TO}"
airflow backfill create --dag-id marketplace_anomaly_detect_daily --from-date "${FROM}" --to-date "${TO}" --max-active-runs 4 > /dev/null
wait_runs marketplace_anomaly_detect_daily

docker compose exec -T postgres-dwh psql -U dwh_user -d dwh -c \
  "SELECT dt, COUNT(*) AS commandes, SUM(total_amount) AS ca FROM dwh.fact_orders
   WHERE dt BETWEEN '${FROM}' AND '${TO}' GROUP BY dt ORDER BY dt;"
