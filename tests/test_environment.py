"""Verifica que el entorno de ejecución esté correctamente configurado.

No prueba lógica de negocio: solo que Spark arranque sobre la JVM, que las rutas de las
capas Medallion se resuelvan y que los archivos de Bronze estén donde el pipeline los
espera. Si algo de esto falla, ningún job va a funcionar.
"""

from __future__ import annotations

from jobs.common import config

# La fixture `spark` vive en tests/conftest.py y se comparte con el resto de los tests.


def test_pyspark_instalado():
    import pyspark

    assert pyspark.__version__.startswith("4."), pyspark.__version__


def test_spark_arranca(spark):
    """Si la JVM o Java no estuvieran disponibles, esto falla al crear la sesión."""
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "letra"])
    assert df.count() == 2


def test_capas_medallion_existen():
    assert config.BRONZE_DIR.is_dir(), config.BRONZE_DIR
    assert config.SILVER_DIR.is_dir(), config.SILVER_DIR
    assert config.GOLD_DIR.is_dir(), config.GOLD_DIR


def test_archivos_bronze_presentes():
    assert config.ORDERS_CSV.is_file(), config.ORDERS_CSV
    assert config.ORDER_ITEMS_CSV.is_file(), config.ORDER_ITEMS_CSV


def test_spark_lee_bronze(spark):
    """Confirma que el volumen está montado y que Bronze tiene el dataset esperado."""
    orders = spark.read.csv(config.as_spark_path(config.ORDERS_CSV), header=True)
    order_items = spark.read.csv(config.as_spark_path(config.ORDER_ITEMS_CSV), header=True)

    assert orders.count() == 99_441
    assert order_items.count() == 112_650
