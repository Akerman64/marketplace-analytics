#!/usr/bin/env bash
# Prépare la soutenance (~2 min) : démarre Docker et la stack, attend que chaque
# application réponde, vérifie les données, puis ouvre un onglet par application.
#   ./scripts/demo.sh             # ouvre les onglets dans le navigateur
#   ./scripts/demo.sh --no-open   # vérifie tout sans ouvrir le navigateur
set -euo pipefail
cd "$(dirname "$0")/.."
OPEN=1
[ "${1:-}" = "--no-open" ] && OPEN=0
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"   # CLI de Docker Desktop (macOS)

if ! docker info > /dev/null 2>&1; then
  echo "▶ Démarrage de Docker Desktop…"
  open -a Docker
  until docker info > /dev/null 2>&1; do sleep 3; done
fi

echo "▶ Démarrage de la stack"
docker compose up -d > /dev/null 2>&1

API_PORT=$(docker compose port api-marketplace 5000 | cut -d: -f2)

wait_http() {
  printf "  %-11s" "$1"
  until curl -sf -o /dev/null "$2"; do sleep 3; done
  echo "prêt"
}
echo "▶ Attente des applications"
wait_http Airflow    http://localhost:8080/api/v2/version
wait_http API        "http://localhost:${API_PORT}/health"
wait_http Metabase   http://localhost:3000/api/health
wait_http Grafana    http://localhost:3001/api/health
wait_http Prometheus http://localhost:9090/-/ready

echo "▶ Données du Data Warehouse"
docker compose exec -T postgres-dwh psql -U dwh_user -d dwh -tA -c \
  "SELECT '  ' || COUNT(*) || ' commandes sur ' || COUNT(DISTINCT dt) || ' jours, dernier jour chargé : ' || MAX(dt) FROM dwh.fact_orders;"

# Adresse du dashboard « Executive Summary » (son id dépend de l'instance Metabase)
MB_DASH=$(python3 - <<'EOF'
import json, os, urllib.request
base = "http://localhost:3000/api"
try:
    login = urllib.request.Request(f"{base}/session", method="POST",
        data=json.dumps({"username": os.environ.get("METABASE_ADMIN_EMAIL", "admin@maelys.local"),
                         "password": os.environ.get("METABASE_ADMIN_PASSWORD", "Admin2026!")}).encode(),
        headers={"Content-Type": "application/json"})
    token = json.load(urllib.request.urlopen(login, timeout=10))["id"]
    req = urllib.request.Request(f"{base}/dashboard/", headers={"X-Metabase-Session": token})
    ids = [d["id"] for d in json.load(urllib.request.urlopen(req, timeout=10))
           if d["name"].startswith("1 ·") and not d.get("archived")]
    print(f"http://localhost:3000/dashboard/{ids[0]}" if ids else "http://localhost:3000")
except Exception:
    print("http://localhost:3000")
EOF
)

URLS=(
  "https://github.com/Akerman64/marketplace-analytics"
  "http://localhost:8080/dags/marketplace_orders_ingest_daily"
  "${MB_DASH}"
  "http://localhost:3001/d/marketplace-pipeline"
  "http://localhost:9090/targets"
)

cat <<EOF
  
✅ Tout est prêt
  GitHub (README)   ${URLS[0]}
  Airflow           ${URLS[1]}      airflow / airflow
  Metabase          ${URLS[2]}      admin@maelys.local / Admin2026!
  Grafana           ${URLS[3]}      admin / admin
  Prometheus        ${URLS[4]}
  API Marketplace   http://localhost:${API_PORT}                token formation-token-2026
  Data Warehouse    psql -h localhost -p 5433 -U dwh_user -d dwh   (dwh_password_2026)
  Base du site      docker compose exec mysql-backoffice mysql -ubackoffice_user -p --default-character-set=utf8mb4 backoffice   (backoffice_password_2026)
EOF

if [ "$OPEN" = 1 ]; then
  for u in "${URLS[@]}"; do open "$u" 2>/dev/null || xdg-open "$u" > /dev/null 2>&1 || true; done
  echo "  → 5 onglets ouverts : connectez-vous à Airflow, Metabase et Grafana avant de commencer."
fi
