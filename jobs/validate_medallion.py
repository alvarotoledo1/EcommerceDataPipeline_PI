"""Validación del flujo Medallion completo: Bronze -> Silver -> Gold.

Cada capa ya se valida por dentro, pero eso no alcanza. Un JOIN que duplica filas o un
filtro que se come registros deja cada capa internamente consistente y el total final
mal. Este script compara las capas **entre sí**, que es donde ese tipo de error se ve.

Se ejecuta después de `run_pipeline` y de `dbt build`:

    docker compose run --rm spark python -m jobs.validate_medallion

Termina con código 1 si alguna reconciliación falla.
"""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from jobs.common import config, quality, schemas, spark as spark_utils
from jobs.common.logging_setup import get_logger
from jobs.common.quality import CheckResult, DataQualityError, Severidad

logger = get_logger("medallion.validate")


def _contar_csv(spark: SparkSession, uri: str, schema) -> int:
    return spark.read.option("header", True).schema(schema).csv(uri).count()


def recolectar_metricas(spark: SparkSession, incluir_gold: bool = True) -> dict:
    """Lee las capas y devuelve los números que hay que reconciliar.

    Con `incluir_gold=False` se valida solo Bronze -> Silver, que es lo que puede
    comprobarse antes de que dbt haya corrido.
    """
    bronze_orders = _contar_csv(
        spark, config.uri_bronze(config.ORDERS_CSV_NAME), schemas.ORDERS_BRONZE
    )
    bronze_items = _contar_csv(
        spark, config.uri_bronze(config.ORDER_ITEMS_CSV_NAME), schemas.ORDER_ITEMS_BRONZE
    )

    silver_orders = spark_utils.read_silver(spark, config.uri_silver(config.TABLA_ORDERS))
    silver_items = spark_utils.read_silver(
        spark, config.uri_silver(config.TABLA_ORDER_ITEMS)
    )
    silver_sales = spark_utils.read_silver(
        spark, config.uri_silver(config.TABLA_ORDER_PRODUCT_SALES)
    ).cache()

    # Silver restringido a los mismos estados que Gold considera venta válida.
    ventas_validas = silver_sales.filter(
        F.col("order_status").isin(list(config.ESTADOS_VENTA_VALIDA))
    )
    agregado_silver = ventas_validas.agg(
        F.sum("quantity").alias("unidades"), F.sum("item_revenue").alias("facturacion")
    ).collect()[0]

    metricas = {
        "bronze_orders_filas": bronze_orders,
        "bronze_order_items_filas": bronze_items,
        "silver_orders_filas": silver_orders.count(),
        "silver_order_items_filas": silver_items.count(),
        "silver_sales_filas": silver_sales.count(),
        "silver_sales_claves_unicas": silver_sales.select(
            "order_id", "product_id"
        ).distinct().count(),
        "silver_unidades_totales": int(
            silver_sales.agg(F.sum("quantity")).collect()[0][0]
        ),
        "silver_ventas_validas_filas": ventas_validas.count(),
        "silver_ventas_validas_unidades": int(agregado_silver["unidades"]),
        "silver_ventas_validas_facturacion": Decimal(agregado_silver["facturacion"]),
    }

    if incluir_gold:
        gold = spark.read.parquet(
            config.uri_gold(config.GOLD_DAILY_PRODUCT_SALES)
        ).cache()
        agregado_gold = gold.agg(
            F.sum("quantity").alias("unidades"),
            F.sum("total_revenue").alias("facturacion"),
        ).collect()[0]

        metricas.update(
            {
                "gold_filas": gold.count(),
                "gold_claves_unicas": gold.select("purchase_date", "product_id")
                .distinct()
                .count(),
                "gold_unidades": int(agregado_gold["unidades"]),
                "gold_facturacion": Decimal(agregado_gold["facturacion"]),
                "gold_productos": gold.select("product_id").distinct().count(),
                "gold_dias": gold.select("purchase_date").distinct().count(),
            }
        )
        gold.unpersist()

    silver_sales.unpersist()
    return metricas


def validar(m: dict, incluir_gold: bool = True) -> list[CheckResult]:
    """Reconciliaciones entre capas."""
    checks = [
        # Bronze -> Silver: el tipado no puede perder ni agregar filas.
        quality.check_conteo_esperado(
            m["silver_orders_filas"],
            m["bronze_orders_filas"],
            "bronze_a_silver_orders_sin_perdida",
            "orders",
        ),
        quality.check_conteo_esperado(
            m["silver_order_items_filas"],
            m["bronze_order_items_filas"],
            "bronze_a_silver_order_items_sin_perdida",
            "order_items",
        ),
        # Silver: la agregación conserva todas las unidades vendidas.
        quality.check_conteo_esperado(
            m["silver_unidades_totales"],
            m["silver_order_items_filas"],
            "silver_unidades_igual_a_filas_de_items",
            "order_product_sales",
        ),
        # Silver: granularidad declarada.
        quality.check_conteo_esperado(
            m["silver_sales_filas"],
            m["silver_sales_claves_unicas"],
            "silver_granularidad_order_id_product_id",
            "order_product_sales",
        ),
    ]

    if not incluir_gold:
        return checks

    checks += [
        # Gold: granularidad declarada.
        quality.check_conteo_esperado(
            m["gold_filas"],
            m["gold_claves_unicas"],
            "gold_granularidad_purchase_date_product_id",
            "daily_product_sales",
        ),
        # Silver -> Gold: la agregación por fecha no puede alterar los totales.
        quality.check_conteo_esperado(
            m["gold_unidades"],
            m["silver_ventas_validas_unidades"],
            "gold_unidades_reconcilia_con_silver",
            "daily_product_sales",
        ),
        CheckResult(
            nombre="gold_facturacion_reconcilia_con_silver",
            tabla="daily_product_sales",
            severidad=Severidad.CRITICO,
            ok=abs(m["gold_facturacion"] - m["silver_ventas_validas_facturacion"])
            <= Decimal("0.01"),
            mensaje=(
                f"Gold {m['gold_facturacion']:,} contra "
                f"Silver {m['silver_ventas_validas_facturacion']:,}"
            ),
        ),
        # Gold tiene que ser un subconjunto: filtra estados, nunca agrega filas.
        CheckResult(
            nombre="gold_no_supera_a_silver",
            tabla="daily_product_sales",
            severidad=Severidad.CRITICO,
            ok=m["gold_unidades"] <= m["silver_unidades_totales"],
            mensaje=(
                f"{m['gold_unidades']:,} unidades en Gold sobre "
                f"{m['silver_unidades_totales']:,} en Silver"
            ),
        ),
    ]

    return checks


def _tabla_resumen(m: dict, incluir_gold: bool = True) -> list[str]:
    """Resumen legible del recorrido de los datos por las capas."""
    lineas = [
        "",
        "  CAPA     TABLA                      REGISTROS      DETALLE",
        "  " + "-" * 76,
        f"  BRONZE   olist_orders_dataset.csv   {m['bronze_orders_filas']:>9,}      un registro por pedido",
        f"  BRONZE   olist_order_items          {m['bronze_order_items_filas']:>9,}      un registro por UNIDAD vendida",
        "  " + "-" * 76,
        f"  SILVER   orders                     {m['silver_orders_filas']:>9,}      tipada, todos los estados",
        f"  SILVER   order_items                {m['silver_order_items_filas']:>9,}      tipada, granularidad de unidad",
        f"  SILVER   order_product_sales        {m['silver_sales_filas']:>9,}      un registro por pedido y producto",
        "  " + "-" * 76,
    ]

    if not incluir_gold:
        lineas += [
            "",
            f"  Unidades vendidas en Silver: {m['silver_unidades_totales']:,}",
            "",
        ]
        return lineas

    descartadas = m["silver_unidades_totales"] - m["gold_unidades"]
    porcentaje = descartadas / m["silver_unidades_totales"] * 100

    lineas += [
        f"  GOLD     daily_product_sales        {m['gold_filas']:>9,}      un registro por fecha y producto",
        "  " + "-" * 76,
        "",
        f"  Unidades vendidas    Silver: {m['silver_unidades_totales']:>10,}   Gold: {m['gold_unidades']:>10,}",
        f"  Facturación (BRL)    Silver: {m['silver_ventas_validas_facturacion']:>13,}   Gold: {m['gold_facturacion']:>13,}",
        "",
        f"  Gold cubre {m['gold_dias']:,} días y {m['gold_productos']:,} productos distintos.",
        f"  Se descartaron {descartadas:,} unidades ({porcentaje:.2f} %) por estado de pedido",
        f"  distinto de {', '.join(config.ESTADOS_VENTA_VALIDA)}.",
        "",
    ]
    return lineas


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--hasta",
        choices=("silver", "gold"),
        default="gold",
        help=(
            "Hasta qué capa validar. 'silver' reconcilia solo Bronze -> Silver y sirve "
            "como control intermedio, antes de que dbt haya generado Gold."
        ),
    )
    args = parser.parse_args(argv)
    incluir_gold = args.hasta == "gold"

    logger.info("=" * 78)
    logger.info("Validación del flujo Medallion (hasta %s)", args.hasta.upper())
    logger.info("Almacenamiento: %s", config.descripcion_backend())
    logger.info("=" * 78)

    spark = spark_utils.get_spark("olist_validate_medallion")
    spark.sparkContext.setLogLevel("WARN")

    salida = 0
    estado = "OK"
    resultados: list[CheckResult] = []
    reporte = (
        "medallion_validation.json" if incluir_gold else "silver_reconciliation.json"
    )

    try:
        metricas = recolectar_metricas(spark, incluir_gold=incluir_gold)

        for linea in _tabla_resumen(metricas, incluir_gold=incluir_gold):
            logger.info(linea)

        resultados = quality.evaluar(
            validar(metricas, incluir_gold=incluir_gold), logger, contexto="Medallion"
        )

    except DataQualityError as error:
        estado = "FALLA_RECONCILIACION"
        salida = 1
        logger.error("=" * 78)
        logger.error("LAS CAPAS NO RECONCILIAN")
        logger.error("%s", error)

    finally:
        destino = quality.escribir_reporte(
            resultados, config.REPORTS_DIR / reporte, estado=estado
        )
        spark.stop()

    logger.info("=" * 78)
    logger.info(
        "Flujo Medallion %s. Reporte: %s",
        "validado" if salida == 0 else "CON ERRORES",
        destino,
    )
    return salida


if __name__ == "__main__":
    sys.exit(main())
