"""Sesión de Spark y helpers de lectura/escritura de las capas.

Los jobs reciben y devuelven URIs (`str`), no rutas: quién decide si eso es un archivo
local o un objeto en MinIO es `jobs/common/config.py`.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from jobs.common import config


def _configurar_s3a(builder):
    """Configura el conector S3A para hablar con MinIO.

    MinIO es compatible con S3 pero no es S3, y tres opciones lo hacen explícito:

    - `path.style.access`: S3 usa `bucket.host/objeto`, MinIO usa `host/bucket/objeto`.
      Sin esto, Spark intenta resolver un subdominio que no existe.
    - `connection.ssl.enabled=false`: en local MinIO habla HTTP.
    - `endpoint.region`: el SDK v2 de AWS exige una región aunque MinIO la ignore.
    """
    opciones = {
        "spark.hadoop.fs.s3a.endpoint": config.S3_ENDPOINT,
        "spark.hadoop.fs.s3a.endpoint.region": config.S3_REGION,
        "spark.hadoop.fs.s3a.access.key": config.S3_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": config.S3_SECRET_KEY,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
    }
    for clave, valor in opciones.items():
        builder = builder.config(clave, valor)
    return builder


def get_spark(app_name: str) -> SparkSession:
    """Crea (o reutiliza) la sesión de Spark del pipeline."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        # Zona horaria fija: sin esto, `to_date` sobre un timestamp daría un día
        # distinto según la zona del contenedor donde corra el pipeline.
        .config("spark.sql.session.timeZone", "UTC")
        # La UI de Spark no aporta nada en ejecución por lotes y ocupa un puerto.
        .config("spark.ui.enabled", "false")
    )

    if config.usa_s3():
        builder = _configurar_s3a(builder)

    return builder.getOrCreate()


def read_bronze_csv(spark: SparkSession, uri: str, schema: StructType) -> DataFrame:
    """Lee un CSV de Bronze con esquema explícito (ver jobs/common/schemas.py)."""
    return spark.read.option("header", True).schema(schema).csv(uri)


def write_silver(df: DataFrame, uri: str) -> None:
    """Escribe una tabla Silver en Parquet, sobrescribiendo la versión anterior.

    `coalesce(1)` porque a esta escala (cientos de miles de filas) un único archivo
    es más simple de inspeccionar y de consumir desde dbt que decenas de fragmentos.
    """
    df.coalesce(1).write.mode("overwrite").parquet(uri)


def read_silver(spark: SparkSession, uri: str) -> DataFrame:
    """Lee una tabla Silver ya generada."""
    return spark.read.parquet(uri)
