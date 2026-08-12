# Roadmap de implementación

Arquitectura Medallion **Bronze → Silver → Gold**, construida de forma incremental.
Cada etapa se validó antes de pasar a la siguiente.

## Separación de responsabilidades

| Componente | Responsabilidad |
|---|---|
| Bronze | Datos originales, tal como vienen de la fuente |
| PySpark | Procesamiento Bronze → Silver: tipado, validación, transformación |
| Silver | Datos limpios, tipados y estructurados |
| dbt | Modelado Silver → Gold, lógica de negocio |
| Gold | Modelo analítico listo para consumo |
| MinIO | Almacenamiento de las tres capas (S3-compatible) |
| Airflow | Orquestación del pipeline |
| Docker | Entorno reproducible |

## Estado

| # | Etapa | Estado | Verificación |
|---|---|---|---|
| 0 | Exploración del dataset | Completada | 38 celdas ejecutadas sobre los datos reales |
| 1 | Estructura del proyecto y entorno | Completada | 5 tests de entorno |
| 2 | Bronze → Silver con PySpark | Completada | 3 tablas Parquet, invariantes de agregación |
| 3 | Validaciones de calidad | Completada | 23 validaciones, 14 tests unitarios |
| 4 | Punto de ejecución único | Completada | `run_pipeline` en 13 s, reporte JSON |
| 5 | MinIO como almacenamiento | Completada | Bronze byte a byte, Silver en buckets |
| 6 | Capa Gold con dbt | Completada | 26 tests dbt en verde |
| 7 | Validación del flujo completo | Completada | 8 reconciliaciones entre capas |
| 8 | Orquestación con Airflow | Completada | DAG de 7 tareas, corrida exitosa |
| 9 | Consolidación con Docker Compose | Completada | Levantado desde cero tras `down -v` |
| 10 | Documentación y cierre | Completada | README, decisiones y anomalías |

## Detalle de cada etapa

### Etapa 1 — Estructura del proyecto

Separar código, datos, configuración, tests y documentación. Las tres capas Medallion
existen como carpetas desde el inicio, junto con los espacios para dbt, Airflow y Docker.

Se descubrió acá que la máquina no tenía Java ni un Python compatible con PySpark, lo
que llevó a containerizar todo el runtime.

### Etapa 2 — Bronze → Silver con PySpark

Tres jobs que leen, tipan y transforman:

- `orders` — tipada, con `purchase_date` derivada de `order_purchase_timestamp`
- `order_items` — tipada, con los importes como `decimal` y no `double`
- `order_product_sales` — un registro por `(order_id, product_id)` con `quantity`,
  `unit_price`, `item_revenue`, `freight_total`, `purchase_date` y `order_status`

Silver conserva **todos** los estados de pedido y **todos** los pedidos, incluidos los
775 que no tienen ítems. La definición de venta válida es responsabilidad de Gold.

### Etapa 3 — Calidad de datos

Validaciones dentro del procesamiento, separadas en dos niveles:

- **Críticas** (detienen la ejecución antes de escribir): unicidad de claves,
  integridad referencial, `price` y `freight_value` ≥ 0, `quantity` > 0 tras la
  agregación, y consistencia del precio unitario dentro de cada `(order_id, product_id)`.
- **Advertencias** (solo se registran): anomalías conocidas como `shipping_limit_date`
  fuera del período del dataset, o los pedidos sin ítems.

Acá se descubrió que Spark 4 usa modo ANSI: un `cast` sobre un valor malformado lanza
excepción en lugar de devolver nulo. Se cambió a `try_cast` para que el problema lo
reporte la capa de calidad y no un stack trace de la JVM.

### Etapa 4 — Ejecución única

`jobs/run_pipeline.py` corre la ingesta y los tres jobs en orden sobre una sola sesión
de Spark, consolida las 23 validaciones en `output/reports/silver_quality.json` y
termina con código 1 si alguna crítica falla.

### Etapa 5 — MinIO

MinIO con Docker como almacenamiento de las tres capas. La clave del diseño fue que
ningún job construye rutas a mano: piden `config.uri_silver("orders")` y ese módulo
decide si eso es una carpeta o un bucket. Incorporar MinIO no requirió tocar ni un job.

### Etapa 6 — Gold con dbt

Modelado analítico sobre Silver con `dbt-duckdb`, que lee los Parquet directamente desde
MinIO. El modelo `daily_product_sales` agrega por fecha y producto, aplicando el filtro
de estados parametrizado en `dbt_project.yml`.

### Etapa 7 — Validación end-to-end

`jobs/validate_medallion.py` compara las capas entre sí: Bronze contra Silver, Silver
contra Gold. Detecta los errores que dejan cada capa internamente coherente y el total
final mal.

### Etapa 8 — Airflow

DAG de siete tareas usando `DockerOperator`: Airflow solo orquesta, cada tarea corre en
un contenedor con la imagen que le corresponde.

### Etapa 9 — Docker Compose

Cuatro servicios (`minio`, `spark`, `dbt`, `airflow`) y `run.ps1` con los comandos de
operación. Se eliminó la última configuración manual frágil haciendo que el DAG deduzca
solo la ruta del proyecto en el host.

### Etapa 10 — Documentación

README completo, [decisiones técnicas](decisiones_tecnicas.md) con las alternativas
descartadas, y [anomalías conocidas](anomalias_conocidas.md) del dataset.

## Qué quedaría para una versión siguiente

- **Particionar Silver y Gold por fecha.** Hoy se escribe con `coalesce(1)` porque el
  volumen entra cómodo en un archivo. Con datos crecientes habría que particionar y
  procesar de forma incremental en vez de reescribir todo.
- **Postgres y CeleryExecutor en Airflow.** SQLite y `SequentialExecutor` alcanzan para
  un DAG lineal disparado a mano, no para varias corridas concurrentes.
- **Incorporar los otros siete archivos del dataset.** `customers` permitiría medir
  recompra (hoy imposible: `customer_id` es único por pedido), `products` daría la
  categoría para analizar por rubro, y `reviews` cruzaría satisfacción con ventas.
- **Historizar los reportes de calidad.** Hoy cada corrida sobrescribe el JSON; guardar
  la serie permitiría ver si un problema empeora.
