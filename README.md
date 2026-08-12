# CoderHouse Data Engineering - Entrega Final

**Alumno: Alvaro Julian Toledo**

En el siguiente proyecto se desarrolla un **pipeline de datos end-to-end** utilizando el dataset público de comercio electrónico de **Olist**.

El trabajo parte de los datos originales, estudia su estructura y realiza las transformaciones necesarias para convertirlos en información **limpia, consistente, validada y preparada para análisis**.

Para organizar el procesamiento se utiliza una arquitectura **Medallion**, compuesta por las capas **Bronze, Silver y Gold**. El pipeline combina **PySpark** para el procesamiento, **dbt** para el modelado analítico, **MinIO** para el almacenamiento, **Airflow** para la orquestación y **Docker** para mantener un entorno reproducible.

El resultado final es un dataset de **ventas diarias por producto**, preparado para ser utilizado posteriormente en análisis, reportes o herramientas de visualización.

---

## 1. Introducción

En un proyecto de ingeniería de datos, los archivos de origen normalmente no se encuentran listos para ser analizados.

Pueden existir datos distribuidos entre diferentes fuentes, columnas con tipos incorrectos, valores nulos, distintas granularidades o reglas de negocio que todavía no fueron aplicadas.

En este proyecto se parte de datos transaccionales reales de Olist y se construye un flujo que permite recorrer todo el proceso:

```text
Datos originales
      │
      ▼
   Bronze
      │
      ▼
   Silver
      │
      ▼
    Gold
      │
      ▼
Dataset preparado para análisis
```

Cada etapa tiene una responsabilidad específica y cuenta con controles que permiten verificar que los datos mantengan su consistencia durante el recorrido.

---

## 2. Objetivo

### Objetivo general

El objetivo del proyecto es **comprender la estructura de los datos originales de Olist y construir un pipeline que realice las transformaciones necesarias para convertirlos en un dataset preparado para análisis**.

Los datos de origen se encuentran distribuidos en diferentes archivos y poseen distintas granularidades. Antes de poder analizarlos es necesario integrarlos, convertir sus tipos, validar su calidad y reorganizar su estructura.

### Preparación de los datos

A lo largo del pipeline se realizan las siguientes tareas:

1. **Conservar los datos originales** sin modificaciones.
2. **Validar los archivos de entrada** antes de comenzar las transformaciones.
3. **Convertir los campos al tipo de dato adecuado**, especialmente fechas e importes.
4. **Integrar la información de pedidos e ítems** mediante `order_id`.
5. **Calcular la cantidad de unidades vendidas**, ya que el dataset no posee una columna de cantidad.
6. **Consolidar los ítems por pedido y producto**.
7. **Aplicar la regla de negocio que determina qué pedidos representan ventas válidas**.
8. **Agregar los datos por fecha y producto**.
9. **Validar los resultados entre las distintas capas** para detectar pérdidas, duplicaciones o diferencias inesperadas.

### Dataset final

El resultado del pipeline es el modelo:

```text
daily_product_sales
```

Su granularidad es:

```text
purchase_date + product_id
```

Esto significa que **cada fila representa las ventas de un producto en una fecha determinada**.

El dataset final contiene:

| Campo           | Descripción                   |
| --------------- | ----------------------------- |
| `purchase_date` | Fecha de compra               |
| `product_id`    | Identificador del producto    |
| `quantity`      | Unidades vendidas             |
| `total_revenue` | Facturación total             |
| `total_freight` | Costo total de envío          |
| `orders_count`  | Cantidad de pedidos distintos |

Para este modelo se consideran únicamente los pedidos con estado `delivered`.

De esta forma, los archivos transaccionales originales se convierten en un **dataset analítico estructurado y validado**, preparado para análisis posteriores.

---

## 3. Dataset y alcance

El proyecto utiliza el **Brazilian E-Commerce Public Dataset by Olist**, disponible públicamente en Kaggle.

[Ver dataset en Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

Olist contiene información de aproximadamente 100.000 pedidos realizados en Brasil entre 2016 y 2018.

El dataset completo está formado por varias fuentes relacionadas que permiten analizar pedidos, clientes, productos, vendedores, pagos, reseñas y ubicación geográfica.

### Estructura general del dataset

La siguiente imagen muestra las principales relaciones entre las fuentes disponibles:

![Relaciones entre los datasets de Olist](docs/images/olist_dataset_relationships.png)

### Alcance del proyecto

Aunque el dataset completo contiene varias tablas, el pipeline utiliza únicamente las dos fuentes necesarias para construir el modelo de ventas:

```text
olist_orders_dataset.csv
olist_order_items_dataset.csv
```

`olist_orders_dataset.csv` aporta principalmente:

* identificador del pedido;
* fecha de compra;
* estado del pedido.

`olist_order_items_dataset.csv` aporta:

* producto;
* precio;
* costo de envío;
* ítems incluidos en cada pedido.

Ambos archivos se relacionan mediante:

```text
order_id
```

Estas dos fuentes son suficientes para obtener ventas por fecha y producto.

Las demás tablas quedan fuera del alcance actual porque no son necesarias para el objetivo definido. Podrían incorporarse en futuras extensiones para analizar, por ejemplo:

* clientes y recompra;
* categorías de productos;
* medios de pago;
* vendedores;
* reseñas;
* distribución geográfica.

El alcance se concentra así en construir un **pipeline completo y reproducible sobre el núcleo transaccional de las ventas**.

---

## 4. Fuentes utilizadas

### `olist_orders_dataset.csv`

Contiene **99.441 pedidos**.

Su granularidad es:

```text
1 registro = 1 pedido
```

Las principales columnas son:

| Columna                         | Descripción                       |
| ------------------------------- | --------------------------------- |
| `order_id`                      | Identificador único del pedido    |
| `customer_id`                   | Identificador asociado al pedido  |
| `order_status`                  | Estado del pedido                 |
| `order_purchase_timestamp`      | Fecha y hora de compra            |
| `order_approved_at`             | Fecha de aprobación               |
| `order_delivered_carrier_date`  | Fecha de entrega al transportista |
| `order_delivered_customer_date` | Fecha de entrega al cliente       |
| `order_estimated_delivery_date` | Fecha estimada de entrega         |

---

### `olist_order_items_dataset.csv`

Contiene **112.650 registros**.

Su granularidad es:

```text
1 registro = 1 ítem del pedido
```

Las principales columnas son:

| Columna               | Descripción                         |
| --------------------- | ----------------------------------- |
| `order_id`            | Pedido asociado                     |
| `order_item_id`       | Posición del ítem dentro del pedido |
| `product_id`          | Producto                            |
| `seller_id`           | Vendedor                            |
| `shipping_limit_date` | Fecha límite de envío               |
| `price`               | Precio                              |
| `freight_value`       | Costo de envío                      |

La relación entre ambas fuentes es:

```text
orders
  1
  │
  │ order_id
  │
  N
order_items
```

Un pedido puede contener uno o varios ítems.

---

## 5. Exploración inicial

Antes de construir el pipeline se realizó una exploración del dataset con **pandas**.

El objetivo de esta etapa fue comprender la estructura de las fuentes antes de definir las transformaciones.

Los principales resultados fueron:

| Métrica                      | Resultado |
| ---------------------------- | --------: |
| Pedidos                      |    99.441 |
| Registros de `order_items`   |   112.650 |
| Pedidos con al menos un ítem |    98.666 |
| Pedidos sin ítems            |       775 |
| Productos                    |    32.951 |
| Vendedores                   |     3.095 |
| Estados de pedido            |         8 |

También se detectó una característica fundamental del dataset:

> **No existe una columna explícita de cantidad.**

Cuando un pedido contiene varias unidades del mismo producto, existen varias filas con la misma combinación:

```text
order_id + product_id
```

Por lo tanto, la cantidad debe obtenerse contando registros:

```text
quantity = count(*)
```

La exploración completa se encuentra en:

[`notebooks/01_exploracion_olist.ipynb`](notebooks/01_exploracion_olist.ipynb)

Las particularidades detectadas se documentan también en:

[`docs/anomalias_conocidas.md`](docs/anomalias_conocidas.md)

---

## 6. Arquitectura del pipeline

El proyecto utiliza una arquitectura **Medallion**.

```text
                   PySpark                         dbt

┌──────────────┐              ┌──────────────┐              ┌──────────────┐
│    BRONZE    │ ───────────► │    SILVER    │ ───────────► │     GOLD     │
│              │              │              │              │              │
│ Datos        │              │ Datos        │              │ Modelo       │
│ originales   │              │ preparados   │              │ analítico    │
└──────────────┘              └──────────────┘              └──────────────┘
       │                             │                             │
       └────────────────────────── MinIO ──────────────────────────┘

                           Airflow
                              │
                         Orquestación

                           Docker
                              │
                    Entorno reproducible
```

Cada capa cumple una función diferente:

| Capa       | Función                                                  |
| ---------- | -------------------------------------------------------- |
| **Bronze** | Conservar la información original                        |
| **Silver** | Limpiar, tipar, integrar y validar                       |
| **Gold**   | Aplicar reglas de negocio y preparar el modelo analítico |

Esta separación permite mantener claramente diferenciados:

* los datos de origen;
* las transformaciones técnicas;
* la lógica de negocio.

---

# 7. Capa Bronze

## ¿Qué representa?

Bronze es el punto de entrada del pipeline.

Su objetivo es conservar los archivos utilizados **tal como fueron recibidos**, sin aplicar transformaciones.

Los archivos de origen se encuentran inicialmente en:

```text
data/bronze/
```

El pipeline toma únicamente:

```text
olist_orders_dataset.csv
olist_order_items_dataset.csv
```

y los carga en MinIO:

```text
s3://bronze/
```

---

## ¿Cómo se realiza la ingesta?

La ingesta se ejecuta mediante:

```text
jobs/ingest_bronze.py
```

utilizando **boto3**.

Los archivos se suben directamente a MinIO sin pasar por Spark.

Esto permite preservar los CSV originales sin modificar su contenido.

---

## Validación de Bronze

Antes de comenzar las transformaciones se verifica para cada archivo que:

* pueda leerse;
* contenga las columnas esperadas;
* contenga registros.

En esta etapa existen:

**6 validaciones críticas.**

Si alguna falla, el pipeline se detiene antes de continuar hacia Silver.

---

# 8. Capa Silver

## ¿Qué representa?

Silver contiene los datos ya **preparados técnicamente**.

En esta capa se realizan tareas de:

* conversión de tipos;
* tratamiento de fechas;
* agregaciones;
* integración entre tablas;
* validación de calidad;
* cambio de granularidad.

Las transformaciones se realizan con **PySpark**.

Los resultados se almacenan como **Parquet** en:

```text
s3://silver/
```

---

## Datasets de Silver

Se generan tres datasets:

| Dataset               | Registros | Granularidad         |
| --------------------- | --------: | -------------------- |
| `orders`              |    99.441 | un pedido            |
| `order_items`         |   112.650 | un ítem              |
| `order_product_sales` |   102.425 | un pedido + producto |

---

## `orders`

Se construye mediante:

```text
jobs/bronze_to_silver_orders.py
```

Las columnas temporales originales se convierten a `timestamp`.

Además, se crea:

```text
purchase_date
```

a partir de:

```text
order_purchase_timestamp
```

Esto permite trabajar posteriormente con una fecha de compra directamente utilizable para agregaciones diarias.

---

## `order_items`

Se construye mediante:

```text
jobs/bronze_to_silver_order_items.py
```

Las principales conversiones son:

```text
order_item_id        → integer
shipping_limit_date  → timestamp
price                → decimal(10,2)
freight_value        → decimal(10,2)
```

Los importes monetarios se manejan como valores decimales para conservar la precisión durante las agregaciones.

---

## `order_product_sales`

Es la transformación central de Silver.

Se construye mediante:

```text
jobs/build_order_product_sales.py
```

Primero se agrupan los ítems por:

```text
order_id + product_id
```

Luego se calculan:

```text
quantity      = count(*)
unit_price    = max(price)
item_revenue  = sum(price)
freight_total = sum(freight_value)
```

De esta manera, si un pedido contiene varias unidades del mismo producto, esas filas pasan a representar un único registro con su cantidad correspondiente.

Después se incorpora desde `orders`:

```text
purchase_date
order_status
```

mediante un:

```text
LEFT JOIN
```

El `LEFT JOIN` permite conservar cualquier ítem que no encuentre correspondencia en `orders`, de manera que el problema pueda ser detectado por las validaciones en lugar de eliminarse silenciosamente.

---

## Validaciones de Silver

En Silver se ejecutan:

**23 validaciones.**

Distribuidas de la siguiente manera:

| Dataset               | Validaciones |
| --------------------- | -----------: |
| `orders`              |            5 |
| `order_items`         |            9 |
| `order_product_sales` |            9 |
| **Total**             |       **23** |

De ellas:

* **20 son críticas**;
* **3 son advertencias**.

Se verifican, entre otras condiciones:

* valores nulos;
* claves únicas;
* precios negativos;
* fletes negativos;
* cantidades válidas;
* granularidad;
* consistencia de precios;
* integridad entre tablas;
* reconciliación de unidades.

Las validaciones críticas se ejecutan antes de escribir los resultados.

---

# 9. Capa Gold

## ¿Qué representa?

Gold contiene los datos preparados para su consumo analítico.

En esta capa ya no se realizan transformaciones destinadas únicamente a limpiar o tipar información.

Su función es aplicar la **lógica de negocio** y construir el modelo final.

Gold se desarrolla mediante **dbt**.

---

## Modelos dbt

El proyecto contiene dos modelos.

### `stg_order_product_sales`

Se materializa como:

```text
view
```

Funciona como capa de preparación sobre los datos Silver.

---

### `daily_product_sales`

Es el modelo analítico final.

Se materializa como un archivo Parquet externo:

```text
s3://gold/daily_product_sales.parquet
```

También se genera una copia en:

```text
output/
```

---

## Regla de negocio

Silver conserva los ocho estados presentes en Olist.

La decisión sobre qué pedidos representan ventas válidas se toma únicamente en Gold.

Actualmente:

```yaml
estados_venta_valida:
  - delivered
```

Por lo tanto:

> **El modelo final considera únicamente los pedidos con estado `delivered`.**

Mantener esta regla en Gold permite modificar posteriormente la definición de venta sin reconstruir Bronze y Silver.

---

## Estructura final

La granularidad de `daily_product_sales` es:

```text
purchase_date + product_id
```

Sus columnas son:

| Campo           | Descripción                   |
| --------------- | ----------------------------- |
| `purchase_date` | Fecha de compra               |
| `product_id`    | Producto                      |
| `quantity`      | Unidades vendidas             |
| `total_revenue` | Facturación total             |
| `total_freight` | Costo total de envío          |
| `orders_count`  | Cantidad de pedidos distintos |

---

# 10. Resultado obtenido

El modelo final contiene:

| Métrica       |         Resultado |
| ------------- | ----------------: |
| Registros     |            92.587 |
| Unidades      |           110.197 |
| Productos     |            32.216 |
| Días          |               612 |
| Facturación   | BRL 13.221.498,11 |
| Flete         |  BRL 2.198.275,64 |
| Primera fecha |        2016-09-15 |
| Última fecha  |        2018-08-29 |

El principal resultado del proyecto es:

```text
output/daily_product_sales.parquet
```

También se genera una versión CSV:

```text
output/daily_product_sales.csv
```

para facilitar su inspección con otras herramientas.

---

## Evolución entre capas

| Capa                         | Registros | Unidades |       Facturación |
| ---------------------------- | --------: | -------: | ----------------: |
| Bronze `order_items`         |   112.650 |  112.650 | BRL 13.591.643,70 |
| Silver `order_product_sales` |   102.425 |  112.650 | BRL 13.591.643,70 |
| Gold `daily_product_sales`   |    92.587 |  110.197 | BRL 13.221.498,11 |

Entre Silver y Gold se excluyen:

**2.453 unidades**, equivalentes al **2,18 %** de las unidades de Silver.

La diferencia corresponde a pedidos cuyo estado no es `delivered`.

---

# 11. Calidad y reconciliación entre capas

Además de las validaciones realizadas durante el procesamiento, se comparan los resultados entre las diferentes capas.

La lógica se encuentra en:

```text
jobs/validate_medallion.py
```

Se realizan:

```text
4 validaciones Bronze ↔ Silver
4 validaciones Silver ↔ Gold
```

En total:

**8 reconciliaciones entre capas.**

Estas verificaciones permiten detectar situaciones como:

* filas perdidas;
* registros duplicados por un join;
* diferencias inesperadas de unidades;
* diferencias de facturación;
* cambios incorrectos de granularidad.

---

## Validaciones PySpark totales

| Grupo                      | Cantidad |
| -------------------------- | -------: |
| Validaciones Bronze        |        6 |
| Validaciones Silver        |       23 |
| Reconciliaciones Medallion |        8 |
| **Total**                  |   **37** |

Estas validaciones se ejecutan sobre los datos reales del pipeline.

---

# 12. Testing automatizado

Además de validar los datos procesados, el proyecto incorpora pruebas automatizadas sobre el código.

## pytest

Existen:

**36 tests con pytest.**

Los principales grupos son:

| Archivo                   | Qué verifica                               |
| ------------------------- | ------------------------------------------ |
| `test_environment.py`     | funcionamiento del entorno Spark           |
| `test_config.py`          | configuración y almacenamiento             |
| `test_transformations.py` | lógica de transformación                   |
| `test_quality.py`         | funcionamiento de los controles de calidad |

Los tests utilizan:

```text
OLIST_STORAGE=local
```

Esto permite ejecutarlos sin depender de MinIO.

---

## dbt tests

El proyecto contiene:

**24 tests de dbt.**

Entre los controles realizados se encuentran:

* valores no nulos;
* unicidad;
* granularidad;
* métricas positivas;
* reconciliación de facturación.

De esta forma, las transformaciones y el modelo analítico cuentan con controles independientes.

---

# 13. Orquestación con Airflow

Airflow coordina las diferentes etapas del pipeline.

El DAG principal se denomina:

```text
olist_medallion
```

y contiene siete tareas:

```text
ingest_bronze
      │
      ▼
validate_bronze
      │
      ▼
transform_silver
      │
      ▼
validate_silver
      │
      ▼
dbt_build_gold
      │
      ▼
dbt_test
      │
      ▼
validate_gold
```

Cada tarea tiene una función específica:

| Tarea              | Función                         |
| ------------------ | ------------------------------- |
| `ingest_bronze`    | cargar los CSV en Bronze        |
| `validate_bronze`  | validar los archivos originales |
| `transform_silver` | construir Silver                |
| `validate_silver`  | reconciliar Bronze y Silver     |
| `dbt_build_gold`   | construir Gold                  |
| `dbt_test`         | ejecutar los tests de dbt       |
| `validate_gold`    | reconciliar Silver y Gold       |

---

## Separación de responsabilidades

Airflow **solo se encarga de la orquestación**.

Las tareas utilizan `DockerOperator` para ejecutar los procesos en sus respectivos contenedores.

```text
Airflow → coordina
Spark   → procesa
dbt     → modela
MinIO   → almacena
```

De esta manera, Airflow no necesita incorporar Java, Spark o dbt dentro de su propio entorno.

---

## Ejecución

El DAG utiliza:

```text
schedule = None
```

porque Olist es un dataset histórico y cerrado.

El pipeline se ejecuta manualmente cuando se desea realizar una nueva corrida.

---

# 14. Infraestructura

Todo el entorno se ejecuta mediante **Docker**.

El proyecto define cuatro servicios principales:

| Servicio  | Tipo        | Función                 |
| --------- | ----------- | ----------------------- |
| `minio`   | persistente | almacenamiento de datos |
| `airflow` | persistente | orquestación            |
| `spark`   | temporal    | procesamiento PySpark   |
| `dbt`     | temporal    | modelado Gold           |

---

## MinIO

MinIO proporciona almacenamiento compatible con S3 dentro del entorno local.

Se utilizan tres buckets:

```text
bronze
silver
gold
```

La organización es:

```text
MinIO
│
├── bronze/
│   └── CSV originales
│
├── silver/
│   ├── orders
│   ├── order_items
│   └── order_product_sales
│
└── gold/
    └── daily_product_sales.parquet
```

PySpark accede a MinIO mediante `s3a://`, mientras que dbt utiliza DuckDB para trabajar con los archivos almacenados mediante `s3://`.

---

## Contenedores persistentes y temporales

MinIO y Airflow permanecen activos como servicios.

Spark y dbt se utilizan únicamente durante la ejecución de una tarea.

Se crean mediante:

```text
docker compose run --rm
```

y se eliminan al finalizar.

Por ese motivo, es normal que Spark y dbt no permanezcan visibles como contenedores activos en Docker Desktop.

---

# 15. Flujo completo del pipeline

```text
olist_orders_dataset.csv
olist_order_items_dataset.csv
            │
            ▼
      INGESTA - boto3
            │
            ▼
        ┌────────┐
        │ BRONZE │
        │  CSV   │
        └───┬────┘
            │
         PySpark
            │
            ▼
        ┌────────┐
        │ SILVER │
        └───┬────┘
            │
            ├── orders
            ├── order_items
            └── order_product_sales
            │
           dbt
            │
            ▼
        ┌────────┐
        │  GOLD  │
        └───┬────┘
            │
            ▼
 daily_product_sales
            │
            ▼
       Validaciones
            │
            ▼
output/daily_product_sales.parquet
```

Durante todo el recorrido:

* **MinIO** almacena los datos;
* **Airflow** coordina las etapas;
* **Docker** proporciona los entornos de ejecución.

---

# 16. Principales decisiones técnicas

## Arquitectura Medallion

La división Bronze, Silver y Gold permite separar claramente:

* fuente original;
* preparación técnica;
* lógica de negocio.

Cada capa puede evolucionar sin mezclar responsabilidades.

---

## PySpark para las transformaciones

PySpark se utiliza para:

* tipado;
* procesamiento de fechas;
* agregaciones;
* joins;
* validaciones;
* cambios de granularidad.

El procesamiento queda así separado del modelado analítico.

---

## dbt para Gold

dbt se utiliza para transformar Silver en modelos preparados para análisis.

Permite mantener la lógica de negocio en SQL y aporta:

* modelos;
* dependencias;
* tests;
* parametrización.

---

## MinIO como almacenamiento

MinIO permite utilizar una interfaz compatible con S3 dentro de un entorno local.

De esta manera, Bronze, Silver y Gold se almacenan con una organización similar a la utilizada en un data lake.

---

## Parquet en Silver y Gold

Las capas procesadas utilizan **Parquet** porque:

* conserva los tipos;
* utiliza almacenamiento columnar;
* permite compresión;
* resulta adecuado para cargas analíticas.

Actualmente:

| Capa   | Tamaño aproximado |
| ------ | ----------------: |
| Bronze |           31,6 MB |
| Silver |           22,5 MB |
| Gold   |            3,5 MB |

---

## Tipos decimales

Los importes se convierten a:

```text
decimal(10,2)
```

Esto evita errores de precisión propios del punto flotante durante las agregaciones monetarias.

---

## Cantidad mediante `count(*)`

Olist no posee una columna de cantidad.

Por lo tanto:

```text
quantity = count(*)
```

para cada combinación:

```text
order_id + product_id
```

---

## `LEFT JOIN`

La información agregada de productos se combina con `orders` mediante:

```text
LEFT JOIN
```

Esto permite mantener visibles posibles registros sin correspondencia y detectarlos posteriormente con controles de calidad.

---

## Reglas de negocio en Gold

La definición:

```text
delivered = venta válida
```

se aplica únicamente en Gold.

Silver conserva todos los estados disponibles.

Esto permite modificar posteriormente la regla sin volver a procesar las capas anteriores.

---

## Validar antes de escribir

Las validaciones críticas se ejecutan antes de persistir los datos.

Si una validación falla, la capa correspondiente no se actualiza con información incorrecta.

---

## Bronze mediante boto3

Los CSV originales se cargan mediante boto3 en lugar de Spark.

Esto permite conservar los archivos originales sin que Spark los reescriba o divida en fragmentos.

---

# 17. Puesta en marcha

## Requisitos

La máquina necesita:

```text
Docker
Docker Compose
```

No es necesario instalar localmente:

* Java;
* Spark;
* PySpark;
* dbt;
* Airflow;
* MinIO.

---

## Descargar los datos

Descargar desde Kaggle:

```text
olist_orders_dataset.csv
olist_order_items_dataset.csv
```

y colocarlos en:

```text
data/bronze/
```

---

## Windows

Ejecutar:

```powershell
.\run.ps1 quickstart
```

---

## Linux, macOS, Git Bash o WSL

Ejecutar:

```bash
./quickstart.sh
```

El script de Bash también verifica que Docker esté disponible, que existan los archivos de entrada y espera a que MinIO y Airflow se encuentren saludables.

---

# 18. Interfaces disponibles

## Airflow

```text
http://localhost:8080
```

Credenciales:

```text
Usuario: admin
Contraseña: admin
```

---

## MinIO

```text
http://localhost:9001
```

Credenciales:

```text
Usuario: minioadmin
Contraseña: minioadmin
```

La API de MinIO se encuentra disponible en:

```text
http://localhost:9000
```

---

# 19. Resultado generado

El entregable analítico principal es:

```text
output/daily_product_sales.parquet
```

También se genera:

```text
output/daily_product_sales.csv
```

Los reportes de calidad disponibles se almacenan en:

```text
output/reports/
```

---

# 20. Comandos útiles

En Windows se encuentra disponible `run.ps1`.

### Consultar estado

```powershell
.\run.ps1 status
```

### Construir imágenes

```powershell
.\run.ps1 build
```

### Levantar servicios

```powershell
.\run.ps1 up
```

### Ejecutar Bronze → Silver

```powershell
.\run.ps1 pipeline
```

### Construir Gold

```powershell
.\run.ps1 gold
```

### Ejecutar validaciones

```powershell
.\run.ps1 validate
```

### Ejecutar todo el flujo

```powershell
.\run.ps1 all
```

### Ejecutar tests

```powershell
.\run.ps1 test
```

### Disparar el DAG

```powershell
.\run.ps1 dag
```

### Consultar logs

```powershell
.\run.ps1 logs
```

### Abrir una terminal Spark

```powershell
.\run.ps1 shell
```

### Detener servicios

```powershell
.\run.ps1 down
```

### Reiniciar completamente el entorno

```powershell
.\run.ps1 reset
```

---

# 21. Estructura del repositorio

```text
olist-data-pipeline/
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── jobs/
│   ├── common/
│   │   ├── config.py
│   │   ├── spark.py
│   │   ├── schemas.py
│   │   ├── quality.py
│   │   ├── storage.py
│   │   └── logging_setup.py
│   │
│   ├── ingest_bronze.py
│   ├── validate_bronze.py
│   ├── bronze_to_silver_orders.py
│   ├── bronze_to_silver_order_items.py
│   ├── build_order_product_sales.py
│   ├── validate_medallion.py
│   └── run_pipeline.py
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   └── gold/
│   └── tests/
│
├── airflow/
│   └── dags/
│       └── olist_medallion.py
│
├── tests/
├── docker/
│
├── docs/
│   └── images/
│       └── olist_dataset_relationships.png
│
├── notebooks/
│   └── 01_exploracion_olist.ipynb
│
├── output/
│   ├── daily_product_sales.parquet
│   ├── daily_product_sales.csv
│   └── reports/
│
├── docker-compose.yml
├── quickstart.sh
├── run.ps1
├── requirements.txt
├── requirements-dbt.txt
└── .env.example
```

---

# 22. Documentación adicional

El repositorio incluye documentación complementaria para profundizar en distintos aspectos del proyecto:

* [`docs/arquitectura.md`](docs/arquitectura.md) — detalle de la arquitectura y sus capas.
* [`docs/decisiones_tecnicas.md`](docs/decisiones_tecnicas.md) — decisiones de diseño y fundamentos.
* [`docs/anomalias_conocidas.md`](docs/anomalias_conocidas.md) — particularidades detectadas en los datos y su tratamiento.
* [`docs/roadmap.md`](docs/roadmap.md) — evolución de las etapas del proyecto.
* [`notebooks/01_exploracion_olist.ipynb`](notebooks/01_exploracion_olist.ipynb) — exploración inicial del dataset.
