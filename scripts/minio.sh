#!/usr/bin/env bash
# Explorer le data lake MinIO (la console web n'existe plus dans la version gratuite).
#   ./scripts/minio.sh                  # liste les journées archivées
#   ./scripts/minio.sh 2026-10-20       # affiche le début du fichier JSON de cette journée
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"   # CLI de Docker Desktop (macOS)
set -a; source .env; set +a                                            # identifiants MinIO

if [ -z "${1:-}" ]; then
  CMD='mc ls local/data-lake/raw/orders/ | tail -8; echo; mc du --depth 2 local/data-lake/raw/'
else
  CMD="mc cat local/data-lake/raw/orders/dt=$1/data.json | head -c 700; echo; echo '…'"
fi

docker compose run --rm --no-deps --entrypoint /bin/sh minio-init -c \
  "mc alias set local http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD > /dev/null; $CMD" 2>/dev/null
