"""Tests de la lógica de transformación Bronze -> Silver.

Trabajan sobre DataFrames pequeños construidos a mano, sin tocar los archivos reales.
El objetivo es verificar el comportamiento, no el volumen: que las fechas se tipen, que
los nulos sobrevivan y sobre todo que la cantidad se calcule contando filas.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from jobs import bronze_to_silver_order_items as job_items
from jobs import bronze_to_silver_orders as job_orders
from jobs import build_order_product_sales as job_sales
from jobs.common import schemas


# --- Helpers --------------------------------------------------------------


def _orders_bronze(spark, filas):
    return spark.createDataFrame(filas, schema=schemas.ORDERS_BRONZE)


def _items_bronze(spark, filas):
    return spark.createDataFrame(filas, schema=schemas.ORDER_ITEMS_BRONZE)


def _por_clave(df, *claves):
    """Devuelve {(order_id, product_id): fila} para consultar el resultado."""
    return {tuple(f[c] for c in claves): f for f in df.collect()}


# --- orders ---------------------------------------------------------------


def test_orders_tipa_timestamps_y_deriva_purchase_date(spark):
    bronze = _orders_bronze(
        spark,
        [
            (
                "o1",
                "c1",
                "delivered",
                "2017-10-02 10:56:33",
                "2017-10-02 11:07:15",
                "2017-10-04 19:55:00",
                "2017-10-10 21:25:13",
                "2017-10-18 00:00:00",
            )
        ],
    )

    fila = job_orders.transform(bronze).collect()[0]

    assert fila["order_purchase_timestamp"] == datetime(2017, 10, 2, 10, 56, 33)
    assert fila["purchase_date"] == date(2017, 10, 2)
    assert fila["order_estimated_delivery_date"] == datetime(2017, 10, 18, 0, 0, 0)


def test_orders_conserva_los_nulos_de_fecha(spark):
    """Un pedido cancelado no tiene fecha de entrega: el nulo no se imputa."""
    bronze = _orders_bronze(
        spark,
        [("o1", "c1", "canceled", "2017-10-02 10:56:33", None, None, None, "2017-10-18 00:00:00")],
    )

    fila = job_orders.transform(bronze).collect()[0]

    assert fila["order_approved_at"] is None
    assert fila["order_delivered_carrier_date"] is None
    assert fila["order_delivered_customer_date"] is None
    # La fecha de compra, en cambio, siempre está.
    assert fila["purchase_date"] == date(2017, 10, 2)


def test_orders_conserva_todos_los_estados(spark):
    """Silver no filtra por estado: eso se define en Gold."""
    bronze = _orders_bronze(
        spark,
        [
            ("o1", "c1", "delivered", "2017-10-02 10:00:00", None, None, None, None),
            ("o2", "c2", "canceled", "2017-10-03 10:00:00", None, None, None, None),
            ("o3", "c3", "unavailable", "2017-10-04 10:00:00", None, None, None, None),
        ],
    )

    estados = {f["order_status"] for f in job_orders.transform(bronze).collect()}

    assert estados == {"delivered", "canceled", "unavailable"}


# --- order_items ----------------------------------------------------------


def test_order_items_tipa_numeros_y_timestamp(spark):
    bronze = _items_bronze(
        spark, [("o1", "1", "p1", "s1", "2017-09-19 09:45:35", "58.90", "13.29")]
    )

    fila = job_items.transform(bronze).collect()[0]

    assert fila["order_item_id"] == 1
    assert fila["shipping_limit_date"] == datetime(2017, 9, 19, 9, 45, 35)
    assert fila["price"] == Decimal("58.90")
    assert fila["freight_value"] == Decimal("13.29")


def test_order_items_conserva_shipping_limit_fuera_de_rango(spark):
    """La anomalía conocida de fechas hasta 2020 no elimina filas."""
    bronze = _items_bronze(
        spark, [("o1", "1", "p1", "s1", "2020-04-09 22:35:08", "10.00", "1.00")]
    )

    resultado = job_items.transform(bronze).collect()

    assert len(resultado) == 1
    assert resultado[0]["shipping_limit_date"] == datetime(2020, 4, 9, 22, 35, 8)


def test_order_items_mantiene_granularidad_de_unidad(spark):
    """Tres unidades del mismo producto siguen siendo tres filas en esta capa."""
    bronze = _items_bronze(
        spark,
        [
            ("o1", "1", "p1", "s1", "2018-01-01 00:00:00", "100.00", "10.00"),
            ("o1", "2", "p1", "s1", "2018-01-01 00:00:00", "100.00", "10.00"),
            ("o1", "3", "p1", "s1", "2018-01-01 00:00:00", "100.00", "10.00"),
        ],
    )

    assert job_items.transform(bronze).count() == 3


# --- order_product_sales --------------------------------------------------


def _escenario_ventas(spark):
    """Dos pedidos: uno con 3 unidades del mismo producto, otro con 2 productos."""
    items = job_items.transform(
        _items_bronze(
            spark,
            [
                # o1: 3 unidades del producto p1, mismo precio unitario.
                ("o1", "1", "p1", "s1", "2018-01-01 00:00:00", "100.00", "10.12"),
                ("o1", "2", "p1", "s1", "2018-01-01 00:00:00", "100.00", "10.12"),
                ("o1", "3", "p1", "s1", "2018-01-01 00:00:00", "100.00", "10.12"),
                # o2: dos productos distintos, una unidad cada uno.
                ("o2", "1", "p1", "s1", "2018-02-01 00:00:00", "100.00", "5.00"),
                ("o2", "2", "p2", "s2", "2018-02-01 00:00:00", "49.90", "7.50"),
            ],
        )
    )
    orders = job_orders.transform(
        _orders_bronze(
            spark,
            [
                ("o1", "c1", "delivered", "2018-01-15 08:00:00", None, None, None, None),
                ("o2", "c2", "shipped", "2018-02-20 09:30:00", None, None, None, None),
                # o3 no tiene ítems: no debe aparecer en el resultado.
                ("o3", "c3", "unavailable", "2018-03-01 10:00:00", None, None, None, None),
            ],
        )
    )
    return job_sales.transform(items, orders)


def test_sales_calcula_cantidad_contando_filas(spark):
    """El hallazgo central: 3 filas del mismo producto son 3 unidades."""
    fila = _por_clave(_escenario_ventas(spark), "order_id", "product_id")[("o1", "p1")]

    assert fila["quantity"] == 3
    assert fila["unit_price"] == Decimal("100.00")
    assert fila["item_revenue"] == Decimal("300.00")
    assert fila["freight_total"] == Decimal("30.36")


def test_sales_separa_productos_distintos_del_mismo_pedido(spark):
    resultado = _por_clave(_escenario_ventas(spark), "order_id", "product_id")

    assert resultado[("o2", "p1")]["quantity"] == 1
    assert resultado[("o2", "p1")]["item_revenue"] == Decimal("100.00")
    assert resultado[("o2", "p2")]["quantity"] == 1
    assert resultado[("o2", "p2")]["item_revenue"] == Decimal("49.90")


def test_sales_incorpora_contexto_del_pedido(spark):
    resultado = _por_clave(_escenario_ventas(spark), "order_id", "product_id")

    assert resultado[("o1", "p1")]["purchase_date"] == date(2018, 1, 15)
    assert resultado[("o1", "p1")]["order_status"] == "delivered"
    assert resultado[("o2", "p2")]["order_status"] == "shipped"


def test_sales_no_incluye_pedidos_sin_items(spark):
    """Los 775 pedidos sin ítems quedan en Silver.orders pero no acá."""
    pedidos = {f["order_id"] for f in _escenario_ventas(spark).collect()}

    assert pedidos == {"o1", "o2"}


def test_sales_granularidad_es_una_fila_por_pedido_y_producto(spark):
    resultado = _escenario_ventas(spark)
    claves = resultado.select("order_id", "product_id").distinct()

    assert resultado.count() == claves.count() == 3


def test_sales_revenue_equivale_a_precio_unitario_por_cantidad(spark):
    for fila in _escenario_ventas(spark).collect():
        assert fila["item_revenue"] == fila["unit_price"] * fila["quantity"]
