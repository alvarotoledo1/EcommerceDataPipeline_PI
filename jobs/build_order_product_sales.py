"""Silver: tabla `order_product_sales`.

La transformación central del pipeline. Cambia la granularidad de `order_items`, que
viene a nivel unidad, y la lleva a un registro por `(order_id, product_id)`.

El dataset **no tiene columna de cantidad**: si un pedido lleva 3 unidades del mismo
producto, hay 3 filas idénticas numeradas 1, 2 y 3 en `order_item_id`. Por eso
`quantity` se calcula contando filas y `price` es el precio unitario, no el total de la
línea. Verificado en la exploración: de 102.425 combinaciones `(order_id, product_id)`,
7.088 tienen más de una fila y en todas ellas el precio se repite idéntico.

Se parte de la tabla Silver `order_items`, no de Bronze: la agregación necesita las
columnas ya tipadas.

Ejecución individual:
    docker compose run --rm spark python -m jobs.build_order_product_sales
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.common import config, quality, spark as spark_utils
from jobs.common.logging_setup import get_logger
from jobs.common.quality import CheckResult, Severidad

logger = get_logger("silver.sales")

TABLA = "order_product_sales"


def transform(order_items: DataFrame, orders: DataFrame) -> DataFrame:
    """Agrega los ítems a nivel producto y les suma el contexto del pedido."""
    agregado = order_items.groupBy("order_id", "product_id").agg(
        # Una fila = una unidad vendida.
        F.count(F.lit(1)).cast("int").alias("quantity"),
        # El precio se repite en todas las filas del grupo, así que `max` devuelve el
        # precio unitario. Que esa repetición sea consistente lo verifica la validación
        # `price_consistente_por_order_id_product_id` en el job de order_items.
        F.max("price").alias("unit_price"),
        F.sum("price").alias("item_revenue"),
        F.sum("freight_value").alias("freight_total"),
    )

    contexto_pedido = orders.select("order_id", "purchase_date", "order_status")

    # LEFT y no INNER: si algún ítem apuntara a un pedido inexistente, con INNER
    # desaparecería en silencio. Con LEFT queda con contexto nulo y la validación
    # crítica de integridad referencial lo detecta.
    return agregado.join(contexto_pedido, on="order_id", how="left").select(
        "order_id",
        "product_id",
        "purchase_date",
        "order_status",
        "quantity",
        "unit_price",
        "item_revenue",
        "freight_total",
    )


def validate(
    sales: DataFrame, order_items: DataFrame, orders: DataFrame
) -> list[CheckResult]:
    """Controles sobre el dataset agregado, antes de escribirlo."""
    unidades = sales.agg(F.sum("quantity").alias("total")).collect()[0]["total"]

    return [
        # Granularidad declarada: un registro por pedido y producto.
        quality.check_unicidad(sales, ["order_id", "product_id"], TABLA),
        quality.check_mayor_que(sales, "quantity", 0, TABLA),
        quality.check_no_negativo(sales, "item_revenue", TABLA),
        quality.check_no_negativo(sales, "freight_total", TABLA),
        # Todo ítem tiene que corresponder a un pedido existente.
        quality.check_integridad_referencial(
            sales, orders, "order_id", TABLA, "orders"
        ),
        # Si el LEFT JOIN no encontró el pedido, estas columnas quedan nulas.
        quality.check_no_nulos(sales, "purchase_date", TABLA),
        quality.check_no_nulos(sales, "order_status", TABLA),
        # Reconciliación: la suma de cantidades tiene que dar exactamente la cantidad
        # de filas de order_items. Es el control que detecta una agregación que perdió
        # o duplicó unidades.
        quality.check_conteo_esperado(
            actual=int(unidades),
            esperado=order_items.count(),
            nombre="unidades_reconciliadas_contra_order_items",
            tabla=TABLA,
        ),
        # Informativo: los pedidos sin ítems son legítimos (cancelados, no disponibles)
        # y no aparecen acá por diseño. Se registra para que el número quede visible.
        quality.check_claves_sin_correspondencia(
            orders, sales, "order_id", "orders", TABLA, severidad=Severidad.ADVERTENCIA
        ),
    ]


def run(spark: SparkSession) -> list[CheckResult]:
    destino = config.uri_silver(config.TABLA_ORDER_PRODUCT_SALES)

    logger.info("Leyendo Silver: order_items y orders")
    order_items = spark_utils.read_silver(
        spark, config.uri_silver(config.TABLA_ORDER_ITEMS)
    )
    orders = spark_utils.read_silver(spark, config.uri_silver(config.TABLA_ORDERS))

    sales = transform(order_items, orders).cache()

    resultados = quality.evaluar(
        validate(sales, order_items, orders), logger, contexto=TABLA
    )

    logger.info("Escribiendo Silver: %s", destino)
    spark_utils.write_silver(sales, destino)
    logger.info("order_product_sales -> %s filas", f"{sales.count():,}")

    sales.unpersist()
    return resultados


def main() -> None:
    spark = spark_utils.get_spark("build_order_product_sales")
    spark.sparkContext.setLogLevel("WARN")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
