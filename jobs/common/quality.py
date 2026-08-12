"""Validaciones de calidad de datos.

Dos niveles de severidad, con semántica distinta:

- **CRITICO**: el dato está mal y publicarlo haría daño aguas abajo. Corta la ejecución
  antes de escribir, para que Silver nunca contenga algo que no pasó los controles.
- **ADVERTENCIA**: una anomalía conocida y documentada que no invalida el dataset. Se
  registra y la ejecución sigue.

La distinción importa: si todo fuera crítico el pipeline no correría nunca por
problemas conocidos, y si todo fuera advertencia nadie las miraría.

Las funciones `check_*` son genéricas y reutilizables. Las suites concretas de cada
tabla viven en el job correspondiente, junto a la transformación que validan.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class Severidad(str, Enum):
    CRITICO = "CRITICO"
    ADVERTENCIA = "ADVERTENCIA"


class DataQualityError(Exception):
    """Se levanta cuando falla al menos una validación crítica."""


@dataclass
class CheckResult:
    """Resultado de una validación individual."""

    nombre: str
    tabla: str
    severidad: Severidad
    ok: bool
    mensaje: str
    filas_afectadas: int = 0
    detalles: dict = field(default_factory=dict)

    def como_dict(self) -> dict:
        return {
            "check": self.nombre,
            "tabla": self.tabla,
            "severidad": self.severidad.value,
            "resultado": "OK" if self.ok else "FALLA",
            "filas_afectadas": self.filas_afectadas,
            "mensaje": self.mensaje,
            **({"detalles": self.detalles} if self.detalles else {}),
        }


# --- Validaciones genéricas -----------------------------------------------


def check_no_nulos(
    df: DataFrame,
    columna: str,
    tabla: str,
    severidad: Severidad = Severidad.CRITICO,
) -> CheckResult:
    afectadas = df.filter(F.col(columna).isNull()).count()
    return CheckResult(
        nombre=f"{columna}_no_nulo",
        tabla=tabla,
        severidad=severidad,
        ok=afectadas == 0,
        filas_afectadas=afectadas,
        mensaje=(
            f"{columna} sin nulos"
            if afectadas == 0
            else f"{afectadas:,} filas con {columna} nulo"
        ),
    )


def check_unicidad(
    df: DataFrame,
    columnas: Sequence[str],
    tabla: str,
    severidad: Severidad = Severidad.CRITICO,
) -> CheckResult:
    clave = ", ".join(columnas)
    total = df.count()
    distintos = df.select(*columnas).distinct().count()
    duplicadas = total - distintos

    return CheckResult(
        nombre=f"unicidad_{'_'.join(columnas)}",
        tabla=tabla,
        severidad=severidad,
        ok=duplicadas == 0,
        filas_afectadas=duplicadas,
        mensaje=(
            f"({clave}) es único en {total:,} filas"
            if duplicadas == 0
            else f"({clave}) se repite: {duplicadas:,} filas duplicadas sobre {total:,}"
        ),
    )


def check_no_negativo(
    df: DataFrame,
    columna: str,
    tabla: str,
    severidad: Severidad = Severidad.CRITICO,
) -> CheckResult:
    afectadas = df.filter(F.col(columna) < 0).count()
    return CheckResult(
        nombre=f"{columna}_no_negativo",
        tabla=tabla,
        severidad=severidad,
        ok=afectadas == 0,
        filas_afectadas=afectadas,
        mensaje=(
            f"{columna} >= 0 en todas las filas"
            if afectadas == 0
            else f"{afectadas:,} filas con {columna} negativo"
        ),
    )


def check_mayor_que(
    df: DataFrame,
    columna: str,
    minimo: float,
    tabla: str,
    severidad: Severidad = Severidad.CRITICO,
) -> CheckResult:
    afectadas = df.filter(F.col(columna).isNull() | (F.col(columna) <= minimo)).count()
    return CheckResult(
        nombre=f"{columna}_mayor_que_{minimo}",
        tabla=tabla,
        severidad=severidad,
        ok=afectadas == 0,
        filas_afectadas=afectadas,
        mensaje=(
            f"{columna} > {minimo} en todas las filas"
            if afectadas == 0
            else f"{afectadas:,} filas con {columna} <= {minimo} o nulo"
        ),
    )


def check_integridad_referencial(
    hijo: DataFrame,
    padre: DataFrame,
    columna: str,
    tabla: str,
    tabla_referida: str,
    severidad: Severidad = Severidad.CRITICO,
) -> CheckResult:
    """Verifica que toda clave del hijo exista en el padre."""
    huerfanas = hijo.join(
        padre.select(columna).distinct(), on=columna, how="left_anti"
    ).count()

    return CheckResult(
        nombre=f"integridad_{columna}_contra_{tabla_referida}",
        tabla=tabla,
        severidad=severidad,
        ok=huerfanas == 0,
        filas_afectadas=huerfanas,
        mensaje=(
            f"todo {columna} existe en {tabla_referida}"
            if huerfanas == 0
            else f"{huerfanas:,} filas con {columna} inexistente en {tabla_referida}"
        ),
    )


def check_valor_consistente_por_grupo(
    df: DataFrame,
    grupo: Sequence[str],
    columna: str,
    tabla: str,
    severidad: Severidad = Severidad.CRITICO,
) -> CheckResult:
    """Verifica que `columna` tome un único valor dentro de cada grupo.

    Es el control que sostiene toda la lógica de cantidad: si el precio variara entre
    las filas de un mismo (order_id, product_id), no sería un precio unitario y
    `item_revenue = unit_price * quantity` dejaría de ser cierto.
    """
    clave = ", ".join(grupo)
    inconsistentes = (
        df.groupBy(*grupo)
        .agg(F.countDistinct(columna).alias("_valores"))
        .filter(F.col("_valores") > 1)
        .count()
    )

    return CheckResult(
        nombre=f"{columna}_consistente_por_{'_'.join(grupo)}",
        tabla=tabla,
        severidad=severidad,
        ok=inconsistentes == 0,
        filas_afectadas=inconsistentes,
        mensaje=(
            f"{columna} es constante dentro de cada ({clave})"
            if inconsistentes == 0
            else f"{inconsistentes:,} grupos ({clave}) con más de un {columna}"
        ),
    )


def check_rango_de_fechas(
    df: DataFrame,
    columna: str,
    desde: str,
    hasta: str,
    tabla: str,
    severidad: Severidad = Severidad.ADVERTENCIA,
) -> CheckResult:
    """Cuenta las fechas fuera del período esperado del dataset."""
    fecha = F.col(columna).cast("date")
    fuera_de_rango = df.filter(
        fecha.isNotNull()
        & ((fecha < F.lit(desde).cast("date")) | (fecha > F.lit(hasta).cast("date")))
    )
    afectadas = fuera_de_rango.count()

    detalles = {}
    if afectadas:
        extremos = fuera_de_rango.agg(
            F.min(columna).alias("min"), F.max(columna).alias("max")
        ).collect()[0]
        detalles = {"minimo": str(extremos["min"]), "maximo": str(extremos["max"])}

    return CheckResult(
        nombre=f"{columna}_en_rango",
        tabla=tabla,
        severidad=severidad,
        ok=afectadas == 0,
        filas_afectadas=afectadas,
        mensaje=(
            f"{columna} dentro de [{desde}, {hasta}]"
            if afectadas == 0
            else f"{afectadas:,} filas con {columna} fuera de [{desde}, {hasta}]"
        ),
        detalles=detalles,
    )


def check_claves_sin_correspondencia(
    izquierda: DataFrame,
    derecha: DataFrame,
    columna: str,
    tabla: str,
    tabla_derecha: str,
    severidad: Severidad = Severidad.ADVERTENCIA,
) -> CheckResult:
    """Cuenta claves de `izquierda` que no aparecen en `derecha`.

    A diferencia de la integridad referencial, acá la ausencia puede ser legítima: los
    pedidos sin ítems existen y son correctos. Sirve para dejarlo registrado, no para
    frenar la ejecución.
    """
    sin_correspondencia = (
        izquierda.select(columna)
        .distinct()
        .join(derecha.select(columna).distinct(), on=columna, how="left_anti")
        .count()
    )

    return CheckResult(
        nombre=f"{columna}_con_correspondencia_en_{tabla_derecha}",
        tabla=tabla,
        severidad=severidad,
        ok=sin_correspondencia == 0,
        filas_afectadas=sin_correspondencia,
        mensaje=(
            f"todo {columna} aparece en {tabla_derecha}"
            if sin_correspondencia == 0
            else f"{sin_correspondencia:,} valores de {columna} sin correspondencia en {tabla_derecha}"
        ),
    )


def check_conteo_esperado(
    actual: int,
    esperado: int,
    nombre: str,
    tabla: str,
    severidad: Severidad = Severidad.CRITICO,
) -> CheckResult:
    """Reconciliación entre dos conteos que deben coincidir."""
    diferencia = actual - esperado
    return CheckResult(
        nombre=nombre,
        tabla=tabla,
        severidad=severidad,
        ok=diferencia == 0,
        filas_afectadas=abs(diferencia),
        mensaje=(
            f"{actual:,} coincide con lo esperado"
            if diferencia == 0
            else f"{actual:,} contra {esperado:,} esperados (diferencia: {diferencia:+,})"
        ),
    )


# --- Ejecución y reporte --------------------------------------------------

_SIMBOLO = {
    (True, Severidad.CRITICO): "OK   ",
    (True, Severidad.ADVERTENCIA): "OK   ",
    (False, Severidad.CRITICO): "FALLA",
    (False, Severidad.ADVERTENCIA): "AVISO",
}


def evaluar(resultados: list[CheckResult], logger, contexto: str) -> list[CheckResult]:
    """Registra los resultados y corta la ejecución si falló algo crítico."""
    logger.info("Validaciones de calidad — %s", contexto)

    for r in resultados:
        linea = "  [%s] %-46s %s" % (
            _SIMBOLO[(r.ok, r.severidad)],
            r.nombre,
            r.mensaje,
        )
        if r.ok:
            logger.info(linea)
        elif r.severidad is Severidad.ADVERTENCIA:
            logger.warning(linea)
        else:
            logger.error(linea)

    criticas = [r for r in resultados if not r.ok and r.severidad is Severidad.CRITICO]
    avisos = [r for r in resultados if not r.ok and r.severidad is Severidad.ADVERTENCIA]

    logger.info(
        "  Resumen: %d validaciones, %d críticas fallidas, %d advertencias",
        len(resultados),
        len(criticas),
        len(avisos),
    )

    if criticas:
        detalle = "; ".join(f"{r.tabla}.{r.nombre}: {r.mensaje}" for r in criticas)
        raise DataQualityError(
            f"{len(criticas)} validación(es) crítica(s) fallaron en {contexto} -> {detalle}"
        )

    return resultados


def escribir_reporte(
    resultados: list[CheckResult],
    destino: Path,
    *,
    estado: str,
    duracion_segundos: float | None = None,
) -> Path:
    """Guarda el resultado de todas las validaciones de una corrida.

    En JSON y no en texto porque en la Etapa 8 Airflow va a querer leerlo, y porque
    permite comparar corridas entre sí.
    """
    destino.parent.mkdir(parents=True, exist_ok=True)

    criticas = [r for r in resultados if not r.ok and r.severidad is Severidad.CRITICO]
    avisos = [r for r in resultados if not r.ok and r.severidad is Severidad.ADVERTENCIA]

    reporte = {
        "ejecutado_en": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "estado": estado,
        "duracion_segundos": round(duracion_segundos, 1) if duracion_segundos else None,
        "resumen": {
            "total": len(resultados),
            "ok": sum(1 for r in resultados if r.ok),
            "criticas_fallidas": len(criticas),
            "advertencias": len(avisos),
        },
        "validaciones": [r.como_dict() for r in resultados],
    }

    destino.write_text(
        json.dumps(reporte, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return destino
