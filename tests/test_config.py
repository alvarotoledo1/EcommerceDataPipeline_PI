"""Tests del direccionamiento de las capas.

`config` es la pieza que permite cambiar de disco local a MinIO sin tocar ningún job.
Si esta lógica se rompe, los jobs escriben en el lugar equivocado en silencio.
"""

from __future__ import annotations

import pytest

from jobs.common import config


@pytest.fixture
def backend_s3(monkeypatch):
    monkeypatch.setenv("OLIST_STORAGE", "s3")


@pytest.fixture
def backend_local(monkeypatch):
    monkeypatch.setenv("OLIST_STORAGE", "local")


def test_por_defecto_usa_disco_local(monkeypatch):
    monkeypatch.delenv("OLIST_STORAGE", raising=False)

    assert not config.usa_s3()


def test_modo_s3_devuelve_uris_s3a(backend_s3):
    assert config.usa_s3()
    assert config.uri_bronze("olist_orders_dataset.csv") == (
        "s3a://bronze/olist_orders_dataset.csv"
    )
    assert config.uri_silver("orders") == "s3a://silver/orders"
    assert config.uri_gold("daily_product_sales") == "s3a://gold/daily_product_sales"


def test_modo_local_devuelve_rutas_del_filesystem(backend_local):
    assert not config.usa_s3()

    uri = config.uri_silver("orders")

    assert uri.endswith("/data/silver/orders")
    assert "s3a://" not in uri
    # Barras normales incluso en Windows: Spark no interpreta las invertidas.
    assert "\\" not in uri


def test_el_backend_se_lee_en_cada_llamada(monkeypatch):
    """Sin esto, cambiar de backend exigiría reimportar el módulo."""
    monkeypatch.setenv("OLIST_STORAGE", "local")
    local = config.uri_silver("orders")

    monkeypatch.setenv("OLIST_STORAGE", "s3")
    remoto = config.uri_silver("orders")

    assert local != remoto


def test_los_tres_buckets_estan_declarados():
    assert config.BUCKETS == ("bronze", "silver", "gold")
