# Arquitectura

## Vista general

```
   ┌─────────────────────────── Docker Compose ───────────────────────────┐
   │                                                                       │
   │   ┌──────────┐         lanza contenedores        ┌───────────────┐   │
   │   │ Airflow  │ ─────────────────────────────────►│ olist-pipeline│   │
   │   │  (DAG)   │                                   │   (PySpark)   │   │
   │   └──────────┘ ─────────────────────────────────►│  olist-dbt    │   │
   │        │                                         └───────┬───────┘   │
   │        │                                                 │           │
   │        │              ┌──────────────────┐               │           │
   │        └─────────────►│      MinIO       │◄──────────────┘           │
   │                       │  bronze/ silver/ │                           │
   │                       │      gold/       │                           │
   │                       └──────────────────┘                           │
   └───────────────────────────────────────────────────────────────────────┘
```

Airflow no procesa datos: lanza contenedores y espera su código de salida. Todo el
trabajo ocurre en `olist-pipeline` (PySpark) y `olist-dbt`, que leen y escriben en MinIO.

## Las tres capas

### Bronze — la fuente

Los CSV originales, sin modificar. Se suben con boto3 byte a byte, no con Spark: si
Spark los reescribiera, dejarían de ser una copia fiel.

```
s3://bronze/olist_orders_dataset.csv        17.241 KB
s3://bronze/olist_order_items_dataset.csv   15.077 KB
```

**Regla:** nada que se lea de acá puede haber sido transformado por el pipeline.

### Silver — los datos utilizables

Tipados, validados y con la granularidad resuelta. **Sin lógica de negocio.**

```
s3://silver/orders/                   99.441 filas
s3://silver/order_items/             112.650 filas
s3://silver/order_product_sales/     102.425 filas
```

**Regla:** Silver no descarta nada por criterio de negocio. Conserva los ocho estados de
pedido y los 775 pedidos sin ítems. Lo único que puede frenar una escritura es una
validación crítica de integridad.

El cambio de granularidad de `order_items` a `order_product_sales` es la transformación
central: el dataset no tiene columna de cantidad, así que `quantity` se obtiene contando
filas por `(order_id, product_id)`.

### Gold — el modelo analítico

```
s3://gold/daily_product_sales.parquet   92.587 filas
```

**Regla:** acá y solo acá viven las decisiones de negocio. Hoy son dos: qué estados
cuentan como venta (`delivered`) y a qué nivel se agrega (fecha × producto).

## Flujo de una corrida

```
1. ingest_bronze      CSV local ──────────────► s3://bronze/          boto3
2. validate_bronze    ¿existe, es legible, tiene las columnas?        PySpark
3. transform_silver   s3://bronze/ ───────────► s3://silver/          PySpark
                      · tipado con try_cast
                      · 23 validaciones antes de escribir
4. validate_silver    ¿Bronze y Silver reconcilian?                   PySpark
5. dbt_build_gold     s3://silver/ ───────────► s3://gold/            dbt + DuckDB
                      · filtro por estados de venta válida
6. dbt_test           26 tests sobre fuentes y modelo                 dbt
7. validate_gold      ¿Silver y Gold reconcilian al centavo?          PySpark
```

Cada paso puede correrse solo. El orden lo garantiza el DAG, o `run_pipeline` para los
pasos de PySpark.

## Decisiones estructurales

### Las rutas están centralizadas

Ningún job construye una ruta. Piden `config.uri_silver("orders")` y
`jobs/common/config.py` decide si eso es `data/silver/orders` o `s3a://silver/orders`
según `OLIST_STORAGE`.

Esa indirección es la razón por la que incorporar MinIO no requirió tocar ningún job, y
la que permite que los tests corran sin levantar MinIO.

### Una imagen por responsabilidad

| Imagen | Contiene | Para |
|---|---|---|
| `olist-pipeline:dev` | Java 17, Python 3.12, PySpark, boto3 | Procesar |
| `olist-dbt:dev` | Python 3.12, dbt-duckdb | Modelar |
| `olist-airflow:dev` | Airflow + provider de Docker | Orquestar |

Airflow no tiene Java ni PySpark porque no ejecuta nada: lanza las otras dos.

### La validación ocurre antes de escribir

Los checks corren sobre el DataFrame en memoria, antes del `write`. Si una crítica falla,
la capa no se toca y el proceso termina con código 1. Silver nunca contiene datos que no
pasaron los controles.

### Severidades separadas

Una **crítica** significa "esto está mal y no se puede publicar". Una **advertencia**
significa "esto es raro pero conocido y documentado". Sin esa distinción, o el pipeline
no correría nunca por anomalías legítimas, o nadie miraría los avisos.

## Dónde tocar para cambiar cada cosa

| Quiero cambiar... | Toco... |
|---|---|
| Qué estados cuentan como venta | `dbt/dbt_project.yml`, variable `estados_venta_valida` |
| Dónde viven las capas | `jobs/common/config.py` |
| Una validación de Silver | La función `validate()` del job correspondiente |
| Un check reutilizable | `jobs/common/quality.py` |
| El modelo analítico | `dbt/models/gold/` |
| El orden de las tareas | `airflow/dags/olist_medallion.py` |
| Una versión de dependencia | `requirements.txt` o `requirements-dbt.txt` |
