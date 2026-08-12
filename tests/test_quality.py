"""Tests de las validaciones de calidad.

Lo importante acá no es que los checks pasen con datos buenos, sino que **fallen con
datos malos**. Una validación que nunca falla es peor que no tenerla: da confianza sin
respaldo.
"""

from __future__ import annotations

import logging

import pytest

from jobs import bronze_to_silver_order_items as job_items
from jobs.common import quality
from jobs.common.quality import DataQualityError, Severidad
from jobs.common.schemas import ORDER_ITEMS_BRONZE

logger = logging.getLogger("tests.quality")


def _items(spark, filas):
    """Construye una tabla order_items ya tipada, pasando por la transformación real."""
    return job_items.transform(spark.createDataFrame(filas, schema=ORDER_ITEMS_BRONZE))


def _item(order_id, item_id, product_id, price="100.00", freight="10.00", fecha="2018-01-01 00:00:00"):
    return (order_id, item_id, product_id, "s1", fecha, price, freight)


# --- Checks individuales --------------------------------------------------


def test_unicidad_detecta_clave_duplicada(spark):
    df = _items(spark, [_item("o1", "1", "p1"), _item("o1", "1", "p1")])

    r = quality.check_unicidad(df, ["order_id", "order_item_id"], "order_items")

    assert not r.ok
    assert r.filas_afectadas == 1
    assert r.severidad is Severidad.CRITICO


def test_unicidad_pasa_con_clave_correcta(spark):
    df = _items(spark, [_item("o1", "1", "p1"), _item("o1", "2", "p1")])

    assert quality.check_unicidad(df, ["order_id", "order_item_id"], "order_items").ok


def test_no_negativo_detecta_precio_negativo(spark):
    df = _items(spark, [_item("o1", "1", "p1", price="-5.00")])

    r = quality.check_no_negativo(df, "price", "order_items")

    assert not r.ok
    assert r.filas_afectadas == 1


def test_no_nulos_detecta_precio_no_parseable(spark):
    """Un precio que no castea a decimal queda nulo, y el check lo reporta.

    Spark 4 usa modo ANSI: con `cast` normal esto reventaría el job con una excepción
    de la JVM. La transformación usa `try_cast` justamente para que el problema llegue
    a la capa de calidad y no a un stack trace.
    """
    df = _items(spark, [_item("o1", "1", "p1", price="no-es-un-numero")])

    r = quality.check_no_nulos(df, "price", "order_items")

    assert not r.ok
    assert r.filas_afectadas == 1


def test_fecha_malformada_queda_nula_sin_romper_el_job(spark):
    df = _items(spark, [_item("o1", "1", "p1", fecha="fecha-invalida")])

    fila = df.collect()[0]

    assert fila["shipping_limit_date"] is None


def test_precio_inconsistente_dentro_del_grupo_falla(spark):
    """Si el precio variara entre filas del mismo producto, no sería unitario."""
    df = _items(
        spark,
        [
            _item("o1", "1", "p1", price="100.00"),
            _item("o1", "2", "p1", price="120.00"),
        ],
    )

    r = quality.check_valor_consistente_por_grupo(
        df, ["order_id", "product_id"], "price", "order_items"
    )

    assert not r.ok
    assert r.filas_afectadas == 1


def test_precio_consistente_dentro_del_grupo_pasa(spark):
    df = _items(
        spark,
        [
            _item("o1", "1", "p1", price="100.00"),
            _item("o1", "2", "p1", price="100.00"),
            _item("o1", "1", "p2", price="55.00"),
        ],
    )

    assert quality.check_valor_consistente_por_grupo(
        df, ["order_id", "product_id"], "price", "order_items"
    ).ok


def test_integridad_referencial_detecta_huerfanos(spark):
    items = _items(spark, [_item("o1", "1", "p1"), _item("o_inexistente", "1", "p1")])
    orders = spark.createDataFrame([("o1",)], "order_id string")

    r = quality.check_integridad_referencial(
        items, orders, "order_id", "order_items", "orders"
    )

    assert not r.ok
    assert r.filas_afectadas == 1


def test_rango_de_fechas_reporta_como_advertencia(spark):
    """La anomalía de shipping_limit_date hasta 2020 no debe ser crítica."""
    df = _items(spark, [_item("o1", "1", "p1", fecha="2020-04-09 22:35:08")])

    r = quality.check_rango_de_fechas(
        df, "shipping_limit_date", "2016-09-01", "2018-10-31", "order_items"
    )

    assert not r.ok
    assert r.severidad is Severidad.ADVERTENCIA
    assert r.detalles["maximo"].startswith("2020-04-09")


def test_conteo_esperado_detecta_diferencia():
    r = quality.check_conteo_esperado(100, 112, "unidades", "order_product_sales")

    assert not r.ok
    assert r.filas_afectadas == 12


# --- Semántica de severidad -----------------------------------------------


def test_una_critica_fallida_corta_la_ejecucion(spark):
    df = _items(spark, [_item("o1", "1", "p1", price="-1.00")])

    with pytest.raises(DataQualityError, match="price_no_negativo"):
        quality.evaluar(
            [quality.check_no_negativo(df, "price", "order_items")],
            logger,
            contexto="test",
        )


def test_una_advertencia_fallida_no_corta_la_ejecucion(spark):
    df = _items(spark, [_item("o1", "1", "p1", fecha="2020-04-09 22:35:08")])

    resultados = quality.evaluar(
        [
            quality.check_rango_de_fechas(
                df, "shipping_limit_date", "2016-09-01", "2018-10-31", "order_items"
            )
        ],
        logger,
        contexto="test",
    )

    assert len(resultados) == 1
    assert not resultados[0].ok


# --- Suite completa de un job ---------------------------------------------


def test_suite_de_order_items_pasa_con_datos_validos(spark):
    df = _items(
        spark,
        [
            _item("o1", "1", "p1", price="100.00"),
            _item("o1", "2", "p1", price="100.00"),
            _item("o2", "1", "p2", price="49.90"),
        ],
    )

    resultados = job_items.validate(df)

    assert all(r.ok for r in resultados), [r.mensaje for r in resultados if not r.ok]


def test_suite_de_order_items_falla_con_clave_duplicada(spark):
    df = _items(spark, [_item("o1", "1", "p1"), _item("o1", "1", "p1")])

    fallidas = [r for r in job_items.validate(df) if not r.ok]

    assert any(r.nombre.startswith("unicidad") for r in fallidas)
    assert any(r.severidad is Severidad.CRITICO for r in fallidas)
