"""Validación de la capa Bronze.

Comprueba que los archivos fuente estén donde deben y sean legibles, antes de gastar
tiempo transformando. Es deliberadamente superficial: Bronze conserva los datos como
vienen, así que acá no se juzga el contenido, solo que el archivo exista, tenga las
columnas esperadas y no esté vacío.

    docker compose run --rm spark python -m jobs.validate_bronze
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession

from jobs.common import config, quality, schemas, spark as spark_utils
from jobs.common.logging_setup import get_logger
from jobs.common.quality import CheckResult, DataQualityError, Severidad

logger = get_logger("bronze.validate")

ARCHIVOS = (
    (config.ORDERS_CSV_NAME, schemas.ORDERS_BRONZE),
    (config.ORDER_ITEMS_CSV_NAME, schemas.ORDER_ITEMS_BRONZE),
)


def validar(spark: SparkSession) -> list[CheckResult]:
    resultados: list[CheckResult] = []

    for nombre, esquema in ARCHIVOS:
        uri = config.uri_bronze(nombre)
        esperadas = [campo.name for campo in esquema.fields]

        try:
            # Sin esquema, para ver qué columnas trae realmente el archivo.
            encabezado = spark.read.option("header", True).csv(uri).limit(0).columns
            filas = spark_utils.read_bronze_csv(spark, uri, esquema).count()
            legible, error = True, ""
        except Exception as exc:  # noqa: BLE001 - cualquier fallo de lectura cuenta
            encabezado, filas, legible = [], 0, False
            error = str(exc).splitlines()[0]

        resultados.append(
            CheckResult(
                nombre=f"{nombre}_legible",
                tabla=nombre,
                severidad=Severidad.CRITICO,
                ok=legible,
                mensaje=f"archivo legible en {uri}" if legible else f"no se pudo leer: {error}",
            )
        )

        if not legible:
            continue

        faltantes = [c for c in esperadas if c not in encabezado]
        resultados.append(
            CheckResult(
                nombre=f"{nombre}_columnas_esperadas",
                tabla=nombre,
                severidad=Severidad.CRITICO,
                ok=not faltantes,
                filas_afectadas=len(faltantes),
                mensaje=(
                    f"las {len(esperadas)} columnas esperadas están presentes"
                    if not faltantes
                    else f"faltan columnas: {', '.join(faltantes)}"
                ),
                detalles={"encontradas": encabezado} if faltantes else {},
            )
        )

        resultados.append(
            CheckResult(
                nombre=f"{nombre}_no_vacio",
                tabla=nombre,
                severidad=Severidad.CRITICO,
                ok=filas > 0,
                filas_afectadas=filas,
                mensaje=f"{filas:,} filas" if filas else "el archivo está vacío",
            )
        )

    return resultados


def main() -> int:
    logger.info("Validando Bronze en %s", config.descripcion_backend())

    spark = spark_utils.get_spark("olist_validate_bronze")
    spark.sparkContext.setLogLevel("WARN")

    salida, estado = 0, "OK"
    resultados: list[CheckResult] = []

    try:
        resultados = quality.evaluar(validar(spark), logger, contexto="Bronze")
    except DataQualityError as error:
        salida, estado = 1, "FALLA_CALIDAD"
        logger.error("BRONZE NO VÁLIDO: %s", error)
    finally:
        quality.escribir_reporte(
            resultados, config.REPORTS_DIR / "bronze_quality.json", estado=estado
        )
        spark.stop()

    return salida


if __name__ == "__main__":
    sys.exit(main())
