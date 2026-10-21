#!/usr/bin/env bash
# Premier lancement complet, de zéro à des dashboards remplis (~10 min) :
#   ./scripts/bootstrap.sh
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] || cp .env.example .env

echo "▶ 1/5 Démarrage de la stack"
docker compose up -d --build
until [ "$(docker compose ps --format '{{.Service}} {{.Status}}' | grep -c '(healthy)')" -ge 10 ]; do sleep 5; done

echo "▶ 2/5 Activation des DAGs"
for d in marketplace_dims_refresh_daily marketplace_orders_ingest_daily \
         marketplace_analytics_aggregate_daily marketplace_anomaly_detect_daily; do
  docker compose exec -T airflow-worker airflow dags unpause "$d" > /dev/null 2>&1
done

echo "▶ 3/5 Chargement des dimensions (2 400 vendeurs, 180 000 produits)"
docker compose exec -T airflow-worker airflow dags test marketplace_dims_refresh_daily "$(date -v-1d +%F 2>/dev/null || date -d yesterday +%F)" > /dev/null 2>&1

echo "▶ 4/5 Backfill de l'historique (50 jours)"
./scripts/backfill.sh "$(date -v-50d +%F 2>/dev/null || date -d '50 days ago' +%F)" \
                      "$(date -v-2d +%F 2>/dev/null || date -d '2 days ago' +%F)" > /dev/null

echo "▶ 5/5 Configuration de Metabase (compte admin, source DWH, 5 dashboards)"
python3 metabase/setup_metabase.py

echo "✅ Prêt — Airflow http://localhost:8080 · Metabase http://localhost:3000 · Grafana http://localhost:3001"
