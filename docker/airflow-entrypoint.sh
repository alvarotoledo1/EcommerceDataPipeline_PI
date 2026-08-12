#!/usr/bin/env bash
# Arranque de Airflow en un solo contenedor.
#
# Con SQLite y SequentialExecutor alcanza: el DAG es lineal y corre a mano. Un entorno
# real usaría Postgres y CeleryExecutor, pero eso son tres servicios más para no ganar
# nada en este pipeline.
set -euo pipefail

echo "==> Migrando la base de metadatos"
airflow db migrate

echo "==> Asegurando el usuario administrador"
airflow users create \
    --username "${AIRFLOW_ADMIN_USER:-admin}" \
    --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
    --firstname Olist \
    --lastname Pipeline \
    --role Admin \
    --email admin@example.com 2>/dev/null || echo "    (ya existía)"

echo "==> Scheduler en segundo plano"
airflow scheduler &

echo "==> Webserver en http://localhost:8080"
exec airflow webserver
