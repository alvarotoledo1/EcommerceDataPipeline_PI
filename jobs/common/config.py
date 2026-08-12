"""Rutas, backend de almacenamiento y constantes del proyecto.

Único lugar donde se define **dónde** viven las capas Medallion. Los jobs nunca
construyen una ruta a mano: piden `uri_bronze(...)` o `uri_silver(...)` y este módulo
decide si eso apunta al disco local o a un bucket de MinIO.

Esa indirección es la que permitió incorporar MinIO sin tocar ni un job.

Variables de entorno relevantes:

    OLIST_PROJECT_ROOT   raíz del proyecto (dentro del contenedor, /app)
    OLIST_STORAGE        "local" (disco) o "s3" (MinIO). Por defecto "local".
    MINIO_ENDPOINT       http://minio:9000
    MINIO_ROOT_USER      credencial de acceso
    MINIO_ROOT_PASSWORD  credencial secreta
"""

from __future__ import annotations

import os
from pathlib import Path

# --- Raíz del proyecto ----------------------------------------------------

_DEFAULT_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = Path(os.getenv("OLIST_PROJECT_ROOT", _DEFAULT_ROOT))

# --- Capas Medallion en disco local ---------------------------------------
# En modo "s3" siguen existiendo: Bronze local es el origen de la ingesta, y los
# reportes de calidad se escriben siempre en el filesystem.

DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

OUTPUT_DIR = PROJECT_ROOT / "output"
REPORTS_DIR = OUTPUT_DIR / "reports"
LOGS_DIR = OUTPUT_DIR / "logs"

# --- Nombres de recursos --------------------------------------------------

ORDERS_CSV_NAME = "olist_orders_dataset.csv"
ORDER_ITEMS_CSV_NAME = "olist_order_items_dataset.csv"

# Archivos fuente que el pipeline sube a Bronze. El dataset tiene nueve CSV, pero
# el flujo actual solo usa estos dos.
ARCHIVOS_BRONZE = (ORDERS_CSV_NAME, ORDER_ITEMS_CSV_NAME)

ORDERS_CSV = BRONZE_DIR / ORDERS_CSV_NAME
ORDER_ITEMS_CSV = BRONZE_DIR / ORDER_ITEMS_CSV_NAME

TABLA_ORDERS = "orders"
TABLA_ORDER_ITEMS = "order_items"
TABLA_ORDER_PRODUCT_SALES = "order_product_sales"

# Gold lo produce dbt, que materializa cada modelo como un único archivo Parquet
# en lugar de un directorio con fragmentos.
GOLD_DAILY_PRODUCT_SALES = "daily_product_sales.parquet"

# Rutas locales de Silver, usadas por los tests y por el modo "local".
SILVER_ORDERS = SILVER_DIR / TABLA_ORDERS
SILVER_ORDER_ITEMS = SILVER_DIR / TABLA_ORDER_ITEMS
SILVER_ORDER_PRODUCT_SALES = SILVER_DIR / TABLA_ORDER_PRODUCT_SALES

# --- Backend de almacenamiento --------------------------------------------

BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"
BUCKET_GOLD = "gold"
BUCKETS = (BUCKET_BRONZE, BUCKET_SILVER, BUCKET_GOLD)

S3_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
S3_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
S3_REGION = os.getenv("MINIO_REGION", "us-east-1")


def usa_s3() -> bool:
    """True si las capas viven en MinIO en lugar del disco local.

    Se lee en cada llamada y no una sola vez al importar, para que los tests puedan
    cambiar de backend sin recargar el módulo.
    """
    return os.getenv("OLIST_STORAGE", "local").lower() == "s3"


def _uri(bucket: str, directorio_local: Path, recurso: str) -> str:
    if usa_s3():
        return f"s3a://{bucket}/{recurso}"
    return (directorio_local / recurso).as_posix()


def uri_bronze(recurso: str) -> str:
    """Ubicación de un archivo fuente en la capa Bronze."""
    return _uri(BUCKET_BRONZE, BRONZE_DIR, recurso)


def uri_silver(tabla: str) -> str:
    """Ubicación de una tabla de la capa Silver."""
    return _uri(BUCKET_SILVER, SILVER_DIR, tabla)


def uri_gold(tabla: str) -> str:
    """Ubicación de una tabla de la capa Gold."""
    return _uri(BUCKET_GOLD, GOLD_DIR, tabla)


def descripcion_backend() -> str:
    """Texto para los logs, para que quede claro contra qué está corriendo el job."""
    return f"MinIO ({S3_ENDPOINT})" if usa_s3() else f"disco local ({DATA_DIR})"


# --- Constantes del dataset -----------------------------------------------
# Ventana temporal real de las compras, según la exploración (notebooks/).
# Se usa para detectar fechas fuera de rango como advertencia, no como error.

DATASET_START = "2016-09-01"
DATASET_END = "2018-10-31"

# Estados que Gold considera venta concretada. Definido acá para que quede
# documentado en un solo lugar, pero aplicado exclusivamente en dbt.
ESTADOS_VENTA_VALIDA = ("delivered",)


def as_spark_path(path: Path) -> str:
    """Convierte un Path local a la forma que espera Spark.

    En Windows `str(Path)` produce barras invertidas, que Spark interpreta como
    escapes. Con `as_posix()` la ruta funciona igual en Windows y en Linux.
    """
    return path.as_posix()
