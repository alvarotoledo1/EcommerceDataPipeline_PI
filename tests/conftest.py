"""Fixtures compartidas por los tests."""

from __future__ import annotations

import os

import pytest

# Los tests corren siempre contra el disco local: son unitarios y no deben depender de
# que MinIO esté levantado. Se define antes de que cualquier test importe
# `jobs.common.config`, que lee esta variable.
os.environ["OLIST_STORAGE"] = "local"


@pytest.fixture(scope="session")
def spark():
    """Sesión de Spark única para toda la corrida de tests.

    Levantar la JVM cuesta varios segundos, así que se comparte entre módulos.
    Se fuerza `local[2]` para que los tests no ocupen todos los cores.
    """
    os.environ.setdefault("SPARK_MASTER", "local[2]")

    from jobs.common import spark as spark_utils

    session = spark_utils.get_spark("olist-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
