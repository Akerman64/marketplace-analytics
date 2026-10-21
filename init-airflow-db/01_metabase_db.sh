#!/bin/bash
# Base applicative de Metabase (dashboards, questions, comptes) hébergée sur le
# serveur PostgreSQL des métadonnées : plus robuste que H2 face à un arrêt brutal.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    CREATE USER metabase WITH PASSWORD '${METABASE_DB_PASSWORD}';
    CREATE DATABASE metabase OWNER metabase;
SQL
