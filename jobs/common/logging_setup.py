"""Logger común a todos los jobs.

Spark es muy ruidoso por defecto. Tener un logger propio hace que los mensajes del
pipeline se distingan del log interno de Spark, algo que importa sobre todo cuando en la
Etapa 3 las validaciones empiecen a reportar por acá.
"""

from __future__ import annotations

import logging
import sys

_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)-22s | %(message)s"
_DATE_FORMAT = "%H:%M:%S"


def get_logger(name: str) -> logging.Logger:
    """Devuelve un logger que escribe a stdout, sin duplicar handlers."""
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FORMAT))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        # Sin esto los mensajes salen dos veces cuando Spark configura el root logger.
        logger.propagate = False

    return logger
