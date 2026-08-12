"""Esquemas de lectura de la capa Bronze.

Bronze se lee con **todas las columnas como texto**, a propósito. Dos razones:

1. Bronze debe conservar los datos tal como vienen. Si dejáramos que Spark infiriera
   tipos, un valor mal formado se convertiría en null sin que nadie se entere, y la
   capa dejaría de ser fiel a la fuente.
2. `inferSchema` obliga a Spark a leer el archivo completo una vez más solo para
   adivinar. Con el esquema explícito la lectura es una sola pasada y el resultado no
   depende del contenido del archivo.

El tipado real ocurre en los jobs de Silver, de forma explícita y visible.
"""

from __future__ import annotations

from pyspark.sql.types import DecimalType, StringType, StructField, StructType

# Los importes se manejan como decimal y no como float: en punto flotante,
# sumar miles de precios arrastra error de redondeo y los totales de Gold
# dejarían de cuadrar contra la fuente.
MONEY = DecimalType(10, 2)


def _todas_texto(*columnas: str) -> StructType:
    return StructType([StructField(c, StringType(), nullable=True) for c in columnas])


# --- Bronze ---------------------------------------------------------------

ORDERS_BRONZE = _todas_texto(
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
)

ORDER_ITEMS_BRONZE = _todas_texto(
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value",
)

# --- Constantes de tipado -------------------------------------------------

# Formato en el que vienen todas las fechas del dataset, incluidas las que
# representan solo un día (llegan como "2017-10-18 00:00:00").
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

ORDERS_TIMESTAMP_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
