"""Bronze -> Silver: tabla `order_items`.

Tipa las columnas numéricas y temporales. Mantiene la granularidad original de Bronze:
una fila por unidad vendida. La agregación a nivel producto ocurre en
`build_order_product_sales`.

Ejecución individual:
    docker compose run --rm spark python -m jobs.bronze_to_silver_order_items
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from jobs.common import config, quality, schemas, spark as spark_utils
from jobs.common.logging_setup import get_logger
from jobs.common.quality import CheckResult, Severidad

logger = get_logger("silver.order_items")

TABLA = "order_items"


def transform(bronze: DataFrame) -> DataFrame:
    """Convierte las columnas de texto de Bronze a sus tipos definitivos.

    Se usa `try_cast` y no `cast`: Spark 4 corre en modo ANSI, donde un valor
    malformado hace fallar el job con una excepción de la JVM. Convirtiendo a nulo en
    su lugar, el problema lo detecta y lo reporta la validación de calidad, que dice
    cuántas filas están mal en vez de morir en la primera.
    """
    return bronze.select(
        F.col("order_id"),
        F.col("order_item_id").try_cast(IntegerType()).alias("order_item_id"),
        F.col("product_id"),
        F.col("seller_id"),
        F.try_to_timestamp(
            F.col("shipping_limit_date"), F.lit(schemas.TIMESTAMP_FORMAT)
        ).alias("shipping_limit_date"),
        F.col("price").try_cast(schemas.MONEY).alias("price"),
        F.col("freight_value").try_cast(schemas.MONEY).alias("freight_value"),
    )


def validate(silver: DataFrame) -> list[CheckResult]:
    """Controles sobre la tabla ya tipada, antes de escribirla."""
    return [
        quality.check_no_nulos(silver, "order_id", TABLA),
        quality.check_no_nulos(silver, "product_id", TABLA),
        # Si `order_item_id` no fuera un entero, el try_cast lo deja nulo.
        quality.check_no_nulos(silver, "order_item_id", TABLA),
        # Clave natural del detalle.
        quality.check_unicidad(silver, ["order_id", "order_item_id"], TABLA),
        # Si el casteo a decimal fallara, el precio quedaría nulo y el revenue se
        # calcularía sobre menos filas de las que corresponde.
        quality.check_no_nulos(silver, "price", TABLA),
        quality.check_no_negativo(silver, "price", TABLA),
        quality.check_no_negativo(silver, "freight_value", TABLA),
        # El control que sostiene toda la lógica de cantidad: si el precio variara
        # entre las filas de un mismo producto dentro de un pedido, `price` no sería
        # un precio unitario y la agregación de `order_product_sales` sería incorrecta.
        quality.check_valor_consistente_por_grupo(
            silver, ["order_id", "product_id"], "price", TABLA
        ),
        # Anomalía conocida: la fecha límite de envío llega hasta 2020, fuera del
        # período del dataset. No invalida nada y no participa de las métricas.
        quality.check_rango_de_fechas(
            silver,
            "shipping_limit_date",
            config.DATASET_START,
            config.DATASET_END,
            TABLA,
            severidad=Severidad.ADVERTENCIA,
        ),
    ]


def run(spark: SparkSession) -> list[CheckResult]:
    origen = config.uri_bronze(config.ORDER_ITEMS_CSV_NAME)
    destino = config.uri_silver(config.TABLA_ORDER_ITEMS)

    logger.info("Leyendo Bronze: %s", origen)
    bronze = spark_utils.read_bronze_csv(spark, origen, schemas.ORDER_ITEMS_BRONZE)

    silver = transform(bronze).cache()

    resultados = quality.evaluar(validate(silver), logger, contexto=TABLA)

    logger.info("Escribiendo Silver: %s", destino)
    spark_utils.write_silver(silver, destino)
    logger.info("order_items -> %s filas", f"{silver.count():,}")

    silver.unpersist()
    return resultados


def main() -> None:
    spark = spark_utils.get_spark("bronze_to_silver_order_items")
    spark.sparkContext.setLogLevel("WARN")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
