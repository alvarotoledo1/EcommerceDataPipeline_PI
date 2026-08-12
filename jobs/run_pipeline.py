"""Punto de ejecución único del pipeline Bronze -> Silver.

Corre la ingesta y los tres jobs de transformación en el orden correcto, sobre una
única sesión de Spark, y consolida todas las validaciones en un reporte.

El orden no es negociable: `order_product_sales` se construye leyendo las tablas Silver
que producen los dos jobs anteriores.

    ingest_bronze  (CSV originales -> Bronze)
        |
        +-- bronze_to_silver_orders ------> silver/orders
        +-- bronze_to_silver_order_items -> silver/order_items
                    |
                    +-- build_order_product_sales -> silver/order_product_sales

Uso:
    docker compose run --rm spark python -m jobs.run_pipeline

Termina con código 1 si falla alguna validación crítica, para que un orquestador
pueda detectarlo.
"""

from __future__ import annotations

import argparse
import sys
import time

from jobs import (
    bronze_to_silver_order_items,
    bronze_to_silver_orders,
    build_order_product_sales,
    ingest_bronze,
)
from jobs.common import config, quality, spark as spark_utils
from jobs.common.logging_setup import get_logger
from jobs.common.quality import DataQualityError

logger = get_logger("pipeline.silver")

PASOS = [
    ("orders", bronze_to_silver_orders.run),
    ("order_items", bronze_to_silver_order_items.run),
    ("order_product_sales", build_order_product_sales.run),
]

REPORTE = config.REPORTS_DIR / "silver_quality.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sin-ingesta",
        action="store_true",
        help=(
            "No subir los CSV a Bronze. Lo usa el DAG de Airflow, donde la ingesta es "
            "una tarea aparte y repetirla acá sería subir 30 MB dos veces."
        ),
    )
    args = parser.parse_args(argv)

    logger.info("=" * 78)
    logger.info("Pipeline Bronze -> Silver")
    logger.info("Almacenamiento: %s", config.descripcion_backend())
    logger.info("=" * 78)

    # La ingesta va antes de crear la sesión de Spark: no la necesita, y si los
    # archivos fuente faltan conviene enterarse sin haber levantado la JVM.
    if args.sin_ingesta:
        logger.info("Ingesta omitida (--sin-ingesta)")
    else:
        ingest_bronze.run()

    spark = spark_utils.get_spark("olist_silver_pipeline")
    spark.sparkContext.setLogLevel("WARN")

    resultados = []
    inicio = time.time()
    estado = "OK"
    salida = 0

    try:
        for numero, (nombre, ejecutar) in enumerate(PASOS, start=1):
            logger.info("=" * 78)
            logger.info("Paso %d/%d — %s", numero, len(PASOS), nombre)
            logger.info("=" * 78)
            resultados.extend(ejecutar(spark))

    except DataQualityError as error:
        # Falla esperable: los datos no pasaron los controles. Se reporta y se corta.
        estado = "FALLA_CALIDAD"
        salida = 1
        logger.error("=" * 78)
        logger.error("PIPELINE DETENIDO POR CALIDAD DE DATOS")
        logger.error("%s", error)

    finally:
        duracion = time.time() - inicio
        destino = quality.escribir_reporte(
            resultados, REPORTE, estado=estado, duracion_segundos=duracion
        )
        spark.stop()

    logger.info("=" * 78)
    if salida == 0:
        logger.info("Pipeline Bronze -> Silver completado en %.1f s", duracion)
        logger.info("Tablas Silver generadas:")
        for tabla in (
            config.TABLA_ORDERS,
            config.TABLA_ORDER_ITEMS,
            config.TABLA_ORDER_PRODUCT_SALES,
        ):
            logger.info("  - %s", config.uri_silver(tabla))

    hallazgos = [r for r in resultados if not r.ok]
    logger.info(
        "Validaciones: %d ejecutadas, %d con hallazgos", len(resultados), len(hallazgos)
    )
    logger.info("Reporte de calidad: %s", destino)

    return salida


if __name__ == "__main__":
    sys.exit(main())
