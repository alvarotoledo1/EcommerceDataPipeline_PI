"""DAG del pipeline Medallion de Olist.

Airflow **solo orquesta**: no ejecuta Spark ni dbt dentro de su propio proceso. Cada
tarea levanta un contenedor con la imagen que corresponde y muere al terminar. Eso
mantiene la separación de responsabilidades del proyecto y evita que la imagen de
Airflow tenga que cargar con Java, PySpark y dbt.

    ingest_bronze -> validate_bronze -> transform_silver -> validate_silver
                  -> dbt_build_gold -> dbt_test -> validate_gold

Las imágenes `olist-pipeline:dev` y `olist-dbt:dev` tienen que existir antes de
disparar el DAG (`docker compose build`).
"""

from __future__ import annotations

import os
import re
import socket
from datetime import datetime, timedelta
from pathlib import PurePosixPath

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

MONTAJE_DAGS = "/opt/airflow/dags"

# Docker Desktop expone los discos del host dentro de su VM bajo este prefijo.
PREFIJO_DOCKER_DESKTOP = "/run/desktop/mnt/host"


def _normalizar_ruta_del_demonio(origen: str) -> str:
    """Traduce una ruta de montaje a la forma que entiende el demonio de Docker.

    En Windows, `docker inspect` devuelve el origen como `D:\\proyecto\\airflow\\dags`,
    pero el demonio corre dentro de la VM de Docker Desktop y ahí ese mismo directorio
    se ve como `/run/desktop/mnt/host/d/proyecto/airflow/dags`. Si se le pasa la ruta
    de Windows, el bind mount falla con "mount path must be absolute".

    En Linux y macOS la ruta ya viene en formato POSIX y se devuelve tal cual.
    """
    if re.fullmatch(r"[A-Za-z]:[\\/].*", origen):
        unidad = origen[0].lower()
        resto = origen[2:].replace("\\", "/").lstrip("/")
        return f"{PREFIJO_DOCKER_DESKTOP}/{unidad}/{resto}"

    return origen.replace("\\", "/")


def _detectar_ruta_del_host() -> str:
    """Descubre la ruta del proyecto **en el host** inspeccionando este contenedor.

    El demonio de Docker corre fuera de Airflow y resuelve los bind mounts contra su
    propio sistema de archivos, así que no sirve una ruta de adentro del contenedor.

    En vez de pedirle al usuario que averigüe esa ruta, se la deduce: Compose ya montó
    `./airflow/dags` acá, y el demonio sabe con qué origen lo hizo.
    """
    import docker

    contenedor = docker.from_env().containers.get(socket.gethostname())

    for montaje in contenedor.attrs.get("Mounts", []):
        if montaje.get("Destination") == MONTAJE_DAGS:
            ruta = _normalizar_ruta_del_demonio(montaje["Source"])
            # .../proyecto/airflow/dags -> .../proyecto
            return str(PurePosixPath(ruta).parent.parent)

    raise RuntimeError(
        f"No se pudo deducir la ruta del proyecto en el host: no hay ningún montaje "
        f"en {MONTAJE_DAGS}. Definí HOST_PROJECT_PATH en el .env (ver .env.example)."
    )


# La variable de entorno gana si está definida; si no, se deduce sola.
HOST_PROJECT_PATH = os.environ.get("HOST_PROJECT_PATH") or _detectar_ruta_del_host()

# Red de Compose donde vive MinIO. Sin esto, los contenedores que lanza Airflow no
# resuelven el nombre `minio`.
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "ecommercedatapipeline_pi_default")

IMAGEN_SPARK = os.environ.get("IMAGEN_SPARK", "olist-pipeline:dev")
IMAGEN_DBT = os.environ.get("IMAGEN_DBT", "olist-dbt:dev")

CREDENCIALES_MINIO = {
    "MINIO_ROOT_USER": os.environ.get("MINIO_ROOT_USER", "minioadmin"),
    "MINIO_ROOT_PASSWORD": os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
}

ENTORNO_SPARK = {
    "OLIST_PROJECT_ROOT": "/app",
    "OLIST_STORAGE": "s3",
    "MINIO_ENDPOINT": "http://minio:9000",
    **CREDENCIALES_MINIO,
}

ENTORNO_DBT = {
    "DBT_PROFILES_DIR": "/app/dbt",
    # DuckDB quiere el endpoint sin esquema, a diferencia de Spark.
    "MINIO_ENDPOINT_HOST": "minio:9000",
    **CREDENCIALES_MINIO,
}

OPCIONES_DOCKER = dict(
    docker_url="unix://var/run/docker.sock",
    network_mode=DOCKER_NETWORK,
    auto_remove="success",
    mount_tmp_dir=False,
    mounts=[Mount(source=HOST_PROJECT_PATH, target="/app", type="bind")],
)


def tarea_spark(task_id: str, comando: str, **kwargs) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=IMAGEN_SPARK,
        command=comando,
        working_dir="/app",
        environment=ENTORNO_SPARK,
        **OPCIONES_DOCKER,
        **kwargs,
    )


def tarea_dbt(task_id: str, comando: str, **kwargs) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=IMAGEN_DBT,
        command=comando,
        working_dir="/app/dbt",
        environment=ENTORNO_DBT,
        **OPCIONES_DOCKER,
        **kwargs,
    )


with DAG(
    dag_id="olist_medallion",
    description="Bronze -> Silver (PySpark) -> Gold (dbt) sobre MinIO",
    # Sin schedule: el pipeline se dispara a mano. El dataset es histórico y cerrado,
    # no hay datos nuevos que justifiquen una corrida periódica.
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "olist-data-pipeline",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["olist", "medallion", "pyspark", "dbt"],
    doc_md=__doc__,
) as dag:

    # --- Bronze ----------------------------------------------------------
    ingest_bronze = tarea_spark(
        "ingest_bronze",
        "python -m jobs.ingest_bronze",
    )
    ingest_bronze.doc_md = (
        "Sube los CSV originales al bucket `bronze` sin transformarlos."
    )

    validate_bronze = tarea_spark(
        "validate_bronze",
        "python -m jobs.validate_bronze",
    )
    validate_bronze.doc_md = (
        "Comprueba que los archivos existan, sean legibles y traigan las columnas "
        "esperadas, antes de gastar tiempo transformando."
    )

    # --- Silver ----------------------------------------------------------
    transform_silver = tarea_spark(
        "transform_silver",
        # La ingesta ya corrió como tarea propia.
        "python -m jobs.run_pipeline --sin-ingesta",
    )
    transform_silver.doc_md = (
        "Tipa, valida y transforma. Genera `orders`, `order_items` y "
        "`order_product_sales` en Parquet. Una validación crítica fallida corta acá."
    )

    validate_silver = tarea_spark(
        "validate_silver",
        "python -m jobs.validate_medallion --hasta silver",
    )
    validate_silver.doc_md = (
        "Reconcilia Bronze contra Silver: ninguna fila perdida, ninguna unidad "
        "duplicada por la agregación."
    )

    # --- Gold ------------------------------------------------------------
    dbt_build_gold = tarea_dbt("dbt_build_gold", "dbt run")
    dbt_build_gold.doc_md = (
        "Aplica la regla de negocio (qué estados son venta) y materializa "
        "`daily_product_sales` como Parquet en el bucket `gold`."
    )

    dbt_test = tarea_dbt("dbt_test", "dbt test")
    dbt_test.doc_md = "Tests de dbt: no nulos, granularidad y reconciliación con Silver."

    validate_gold = tarea_spark(
        "validate_gold",
        "python -m jobs.validate_medallion --hasta gold",
    )
    validate_gold.doc_md = (
        "Comprobación final de punta a punta: los totales de Gold tienen que coincidir "
        "con los de Silver restringido a los estados de venta válida."
    )

    (
        ingest_bronze
        >> validate_bronze
        >> transform_silver
        >> validate_silver
        >> dbt_build_gold
        >> dbt_test
        >> validate_gold
    )
