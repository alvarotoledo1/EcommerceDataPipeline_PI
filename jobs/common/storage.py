"""Acceso a MinIO por fuera de Spark.

Spark lee y escribe por `s3a://`, pero hay dos cosas que no puede hacer: crear buckets
y subir un archivo byte a byte. Lo segundo importa: si Bronze se cargara con Spark, el
CSV original se convertiría en un directorio de fragmentos reescritos por Spark y la
capa dejaría de conservar la fuente tal como vino.
"""

from __future__ import annotations

from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from jobs.common import config


def cliente_s3():
    """Cliente boto3 apuntando a MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=config.S3_ENDPOINT,
        aws_access_key_id=config.S3_ACCESS_KEY,
        aws_secret_access_key=config.S3_SECRET_KEY,
        region_name=config.S3_REGION,
        # MinIO no soporta el direccionamiento por subdominio de bucket.
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def asegurar_buckets(cliente=None) -> list[str]:
    """Crea los buckets de las tres capas si no existen. Devuelve los creados."""
    cliente = cliente or cliente_s3()
    creados = []

    for bucket in config.BUCKETS:
        try:
            cliente.head_bucket(Bucket=bucket)
        except ClientError:
            cliente.create_bucket(Bucket=bucket)
            creados.append(bucket)

    return creados


def subir_archivo(local: Path, bucket: str, clave: str, cliente=None) -> int:
    """Sube un archivo tal cual y devuelve su tamaño en bytes."""
    cliente = cliente or cliente_s3()
    cliente.upload_file(str(local), bucket, clave)
    return local.stat().st_size


def contar_objetos(bucket: str, prefijo: str = "", cliente=None) -> int:
    """Cuenta los objetos de un bucket bajo un prefijo."""
    cliente = cliente or cliente_s3()
    paginador = cliente.get_paginator("list_objects_v2")
    return sum(
        pagina.get("KeyCount", 0)
        for pagina in paginador.paginate(Bucket=bucket, Prefix=prefijo)
    )
