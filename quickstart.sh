#!/usr/bin/env bash
#
# Levanta el proyecto completo desde cero con un solo comando.
#
#   ./quickstart.sh
#
# Construye las imágenes, levanta MinIO y Airflow, y ejecuta el pipeline entero:
# Bronze -> Silver (PySpark) -> Gold (dbt) -> validación de las tres capas.
#
# Lo único que hace falta tener instalado es Docker.

set -euo pipefail

cd "$(dirname "$0")"

AZUL='\033[1;36m'; VERDE='\033[1;32m'; ROJO='\033[1;31m'; GRIS='\033[0;90m'; FIN='\033[0m'

paso()  { printf "\n${AZUL}==> %s${FIN}\n" "$1"; }
ok()    { printf "${VERDE}    %s${FIN}\n" "$1"; }
nota()  { printf "${GRIS}    %s${FIN}\n" "$1"; }
morir() { printf "\n${ROJO}ERROR: %s${FIN}\n\n" "$1" >&2; exit 1; }

ARCHIVOS_REQUERIDOS=(
    "data/bronze/olist_orders_dataset.csv"
    "data/bronze/olist_order_items_dataset.csv"
)

# --- 1. Requisitos --------------------------------------------------------

paso "Comprobando requisitos"

command -v docker >/dev/null 2>&1 || morir "Docker no está instalado. https://docs.docker.com/get-docker/"
docker info >/dev/null 2>&1 || morir "Docker está instalado pero el demonio no responde. ¿Está abierto Docker Desktop?"
docker compose version >/dev/null 2>&1 || morir "Falta Docker Compose v2 (viene con Docker Desktop)."
ok "Docker $(docker version --format '{{.Server.Version}}') respondiendo"

faltantes=()
for archivo in "${ARCHIVOS_REQUERIDOS[@]}"; do
    [ -f "$archivo" ] || faltantes+=("$archivo")
done

if [ ${#faltantes[@]} -gt 0 ]; then
    printf "\n${ROJO}ERROR: faltan los archivos del dataset:${FIN}\n" >&2
    for archivo in "${faltantes[@]}"; do printf "  - %s\n" "$archivo" >&2; done
    printf "\nDescargalos de Kaggle y ponelos en data/bronze/:\n" >&2
    printf "  https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce\n\n" >&2
    exit 1
fi
ok "Los dos CSV del dataset están en data/bronze/"

# --- 2. Imágenes ----------------------------------------------------------

paso "Construyendo las imágenes (la primera vez tarda ~10 min: descarga PySpark y el SDK de AWS)"
docker compose build
ok "olist-pipeline, olist-dbt y olist-airflow listas"

# --- 3. Servicios ---------------------------------------------------------

paso "Levantando MinIO y Airflow"
docker compose up -d minio airflow

printf "    esperando a MinIO"
for _ in $(seq 1 60); do
    estado=$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q minio)" 2>/dev/null || echo "")
    [ "$estado" = "healthy" ] && break
    printf "."
    sleep 2
done
printf "\n"
[ "${estado:-}" = "healthy" ] || morir "MinIO no llegó a estar disponible. Mirá 'docker compose logs minio'."
ok "MinIO disponible en http://localhost:9001"

# --- 4. Pipeline ----------------------------------------------------------

paso "Bronze -> Silver (PySpark): tipado, validaciones y transformación"
docker compose run --rm spark python -m jobs.run_pipeline

paso "Silver -> Gold (dbt): modelo analítico y tests"
docker compose run --rm dbt dbt build

paso "Validando el flujo Medallion completo"
docker compose run --rm spark python -m jobs.validate_medallion

# --- 5. Airflow (opcional, ya está arrancando) ---------------------------

paso "Esperando a Airflow"
printf "    "
for _ in $(seq 1 60); do
    estado_af=$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q airflow)" 2>/dev/null || echo "")
    [ "$estado_af" = "healthy" ] && break
    printf "."
    sleep 3
done
printf "\n"
if [ "${estado_af:-}" = "healthy" ]; then
    ok "Airflow disponible en http://localhost:8080"
else
    nota "Airflow todavía está arrancando. El pipeline ya corrió igual; probá en un minuto."
fi

# --- 6. Resumen -----------------------------------------------------------

printf "\n${VERDE}%s${FIN}\n" "======================================================================"
printf "${VERDE}  LISTO — el pipeline corrió de punta a punta${FIN}\n"
printf "${VERDE}%s${FIN}\n\n" "======================================================================"

printf "  El resultado final está en:\n"
printf "    output/daily_product_sales.parquet    (formato del pipeline)\n"
printf "    output/daily_product_sales.csv        (para abrir con cualquier herramienta)\n\n"

printf "  Los reportes de calidad, en output/reports/*.json\n\n"

printf "  Interfaces:\n"
printf "    MinIO    http://localhost:9001   %s / %s\n" "${MINIO_ROOT_USER:-minioadmin}" "${MINIO_ROOT_PASSWORD:-minioadmin}"
printf "             buckets bronze / silver / gold\n"
printf "    Airflow  http://localhost:8080   %s / %s\n" "${AIRFLOW_ADMIN_USER:-admin}" "${AIRFLOW_ADMIN_PASSWORD:-admin}"
printf "             DAG 'olist_medallion' (despausalo y dale al play para verlo correr)\n\n"

printf "  Otros comandos:\n"
printf "    docker compose run --rm spark pytest              los 36 tests\n"
printf "    docker compose run --rm dbt dbt show --select daily_product_sales --limit 10\n"
printf "    docker compose down                              apagar\n\n"
