"""Ingesta a la capa Bronze.

Sube los CSV originales a MinIO **sin transformarlos**. Es deliberado que esto no use
Spark: Spark reescribiría el archivo como un directorio de fragmentos y Bronze dejaría
de ser una copia fiel de la fuente. Un `upload_file` conserva los bytes exactos.

En modo local no hace nada: los CSV ya están en `data/bronze/`.

Ejecución:
    docker compose run --rm spark python -m jobs.ingest_bronze
"""

from __future__ import annotations

import sys

from jobs.common import config, storage
from jobs.common.logging_setup import get_logger

logger = get_logger("bronze.ingest")


def run() -> int:
    """Sube los archivos fuente a Bronze. Devuelve la cantidad de archivos subidos."""
    if not config.usa_s3():
        logger.info(
            "Almacenamiento local: los CSV ya están en %s, no hay nada que ingestar",
            config.BRONZE_DIR,
        )
        return 0

    cliente = storage.cliente_s3()

    creados = storage.asegurar_buckets(cliente)
    if creados:
        logger.info("Buckets creados: %s", ", ".join(creados))
    else:
        logger.info("Buckets ya existentes: %s", ", ".join(config.BUCKETS))

    subidos = 0
    for nombre in config.ARCHIVOS_BRONZE:
        origen = config.BRONZE_DIR / nombre
        if not origen.is_file():
            raise FileNotFoundError(
                f"Falta el archivo fuente {origen}. "
                "Descargá el dataset de Kaggle y colocalo en data/bronze/."
            )

        tamanio = storage.subir_archivo(origen, config.BUCKET_BRONZE, nombre, cliente)
        logger.info(
            "Subido a s3://%s/%s (%.1f MB)",
            config.BUCKET_BRONZE,
            nombre,
            tamanio / 1024 / 1024,
        )
        subidos += 1

    logger.info("Bronze listo: %d archivos en el bucket '%s'", subidos, config.BUCKET_BRONZE)
    return subidos


def main() -> int:
    run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
