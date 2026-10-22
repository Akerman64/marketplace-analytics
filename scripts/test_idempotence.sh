#!/usr/bin/env bash
# Test manuel d'idempotence (§12.3 du cahier des charges) : rejouer le même run
# deux fois doit laisser exactement le même nombre de lignes dans dwh.fact_orders.
#   ./scripts/test_idempotence.sh [YYYY-MM-DD]
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"   # CLI de Docker Desktop (macOS)
DS="${1:-2026-04-07}"

count() {
  docker compose exec -T postgres-dwh psql -U dwh_user -d dwh -tA \
    -c "SELECT COUNT(*) FROM dwh.fact_orders WHERE dt='${DS}';"
}

for run in 1 2; do
  echo "▶ Run ${run} : airflow dags test marketplace_orders_ingest_daily ${DS}"
  docker compose exec -T airflow-worker airflow dags test marketplace_orders_ingest_daily "${DS}" > /dev/null 2>&1
  n=$(count)
  echo "  → dwh.fact_orders WHERE dt='${DS}' : ${n} lignes"
  eval "N${run}=${n}"
done

if [ "${N1}" = "${N2}" ] && [ "${N1}" -gt 0 ]; then
  echo "✅ Idempotent : ${N1} = ${N2}"
else
  echo "❌ NON idempotent : ${N1} ≠ ${N2}" >&2
  exit 1
fi
