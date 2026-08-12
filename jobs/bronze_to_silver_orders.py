"""Bronze -> Silver: tabla `orders`.

Tipa las columnas temporales y deriva `purchase_date`, que es la fecha con la que Gold
va a construir las métricas diarias.

No filtra pedidos ni estados: Silver conserva los 99.441 registros, incluidos los 775
que no tienen ítems asociados. Qué representa una venta válida se define en Gold.

Ejecución individual:
    docker compose run --rm spark python -m jobs.bronze_to_silver_orders
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.common import config, quality, schemas, spark as spark_utils
from jobs.common.logging_setup import get_logger
from jobs.common.quality import CheckResult, Severidad

logger = get_logger("silver.orders")

TABLA = "orders"


def transform(bronze: DataFrame) -> DataFrame:
    """Convierte las columnas de texto de Bronze a sus tipos definitivos.

    Los nulos se conservan tal cual. En este dataset son coherentes con el estado del
    pedido — uno cancelado nunca tiene fecha de entrega — así que imputarlos sería
    inventar información.

    Se usa `try_to_timestamp` y no `to_timestamp`: Spark 4 corre en modo ANSI, donde
    una fecha malformada hace fallar el job con una excepción de la JVM. Dejándola en
    nulo, el problema lo reporta la validación de calidad con el conteo de filas
    afectadas.
    """
    df = bronze

    for columna in schemas.ORDERS_TIMESTAMP_COLUMNS:
        df = df.withColumn(
            columna,
            F.try_to_timestamp(F.col(columna), F.lit(schemas.TIMESTAMP_FORMAT)),
        )

    df = df.withColumn("purchase_date", F.to_date(F.col("order_purchase_timestamp")))

    # Orden explícito: primero las claves y los atributos de negocio, después las fechas.
    return df.select(
        "order_id",
        "customer_id",
        "order_status",
        "purchase_date",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    )


def validate(silver: DataFrame) -> list[CheckResult]:
    """Controles sobre la tabla ya tipada, antes de escribirla."""
    return [
        # `order_id` es la clave primaria: sin esto nada aguas abajo tiene sentido.
        quality.check_no_nulos(silver, "order_id", TABLA),
        quality.check_unicidad(silver, ["order_id"], TABLA),
        quality.check_no_nulos(silver, "order_status", TABLA),
        # Si el parseo de la fecha de compra fallara, `purchase_date` quedaría nula y
        # las métricas diarias de Gold perderían filas sin aviso.
        quality.check_no_nulos(silver, "purchase_date", TABLA),
        quality.check_rango_de_fechas(
            silver,
            "order_purchase_timestamp",
            config.DATASET_START,
            config.DATASET_END,
            TABLA,
            severidad=Severidad.ADVERTENCIA,
        ),
    ]


def run(spark: SparkSession) -> list[CheckResult]:
    origen = config.uri_bronze(config.ORDERS_CSV_NAME)
    destino = config.uri_silver(config.TABLA_ORDERS)

    logger.info("Leyendo Bronze: %s", origen)
    bronze = spark_utils.read_bronze_csv(spark, origen, schemas.ORDERS_BRONZE)

    silver = transform(bronze).cache()

    # Validar antes de escribir: si algo crítico falla, Silver no se toca.
    resultados = quality.evaluar(validate(silver), logger, contexto=TABLA)

    logger.info("Escribiendo Silver: %s", destino)
    spark_utils.write_silver(silver, destino)
    logger.info("orders -> %s filas", f"{silver.count():,}")

    silver.unpersist()
    return resultados


def main() -> None:
    spark = spark_utils.get_spark("bronze_to_silver_orders")
    spark.sparkContext.setLogLevel("WARN")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
