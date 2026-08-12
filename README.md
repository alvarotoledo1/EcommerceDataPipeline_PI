# CoderHouse Data Engineering - Entrega Final

**Alumno: Alvaro Julian Toledo**

En el siguiente proyecto se construye un **pipeline de datos end-to-end** sobre el dataset público de comercio electrónico de **Olist**, que registra alrededor de 100.000 pedidos realizados en Brasil entre 2016 y 2018.

El punto de partida son datos transaccionales originales: archivos separados, con distintas granularidades, campos sin tipar y sin ninguna regla de negocio aplicada. El pipeline los recorre progresivamente hasta convertirlos en un dataset consistente, validado y preparado para análisis.

El procesamiento se organiza en una arquitectura **Medallion** de tres capas —Bronze, Silver y Gold—, donde cada una tiene una responsabilidad definida. **PySpark** se encarga del procesamiento, **dbt** del modelado analítico, **MinIO** del almacenamiento, **Airflow** de la orquestación y **Docker** de mantener un entorno reproducible.

El resultado es `daily_product_sales`, un dataset de ventas diarias por producto listo para ser consumido por herramientas de análisis o visualización.

---

## Contenido

| # | Sección |
| - | ------- |
| 1 | [Objetivo](#1-objetivo) |
| 2 | [Cómo ejecutar el proyecto](#2-cómo-ejecutar-el-proyecto) |
| 3 | [Dataset y alcance](#3-dataset-y-alcance) |
| 4 | [Exploración inicial](#4-exploración-inicial) |
| 5 | [Arquitectura del pipeline](#5-arquitectura-del-pipeline) |
| 6 | [Capa Bronze](#6-capa-bronze) |
| 7 | [Capa Silver](#7-capa-silver) |
| 8 | [Capa Gold](#8-capa-gold) |
| 9 | [Resultado obtenido](#9-resultado-obtenido) |
| 10 | [Calidad y testing](#10-calidad-y-testing) |
| 11 | [Orquestación e infraestructura](#11-orquestación-e-infraestructura) |
| 12 | [Decisiones técnicas](#12-decisiones-técnicas) |
| 13 | [Estructura del repositorio](#13-estructura-del-repositorio) |
| 14 | [Comandos auxiliares](#14-comandos-auxiliares) |

---

## 1. Objetivo

El objetivo del proyecto es **comprender la estructura de los datos originales de Olist y realizar las transformaciones necesarias para convertirlos en un dataset limpio, consistente, validado y preparado para análisis**.

Las fuentes originales no permiten un análisis directo. Están repartidas en archivos distintos que hay que integrar, sus fechas e importes llegan como texto, la información de ventas tiene una granularidad que no coincide con la que se necesita para analizarla, no existe una columna explícita de cantidad y los pedidos incluyen ocho estados diferentes, no todos los cuales representan una venta concretada.

El pipeline resuelve cada uno de esos puntos por capas hasta producir un único modelo:

```text
daily_product_sales
```

con granularidad:

```text
purchase_date + product_id
```

Esto significa que **cada fila del resultado representa las ventas de un producto en una fecha determinada**.

| Campo | Descripción |
| ----- | ----------- |
| `purchase_date` | Fecha de compra |
| `product_id` | Identificador del producto |
| `quantity` | Unidades vendidas |
| `total_revenue` | Facturación total |
| `total_freight` | Costo total de envío |
| `orders_count` | Cantidad de pedidos distintos |

---

## 2. Cómo ejecutar el proyecto

El único requisito es tener **Docker Desktop** instalado. Java, Spark, dbt, Airflow y MinIO se ejecutan dentro de contenedores.

### 1. Clonar el repositorio

```bash
git clone <url-del-repositorio>
cd olist-data-pipeline
```

### 2. Verificar los datos de origen

Los dos archivos que utiliza el pipeline vienen incluidos en el repositorio:

```text
data/bronze/olist_orders_dataset.csv
data/bronze/olist_order_items_dataset.csv
```

No hace falta descargar nada. El dataset completo, con sus demás fuentes, está disponible en [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

### 3. Levantar el entorno

```bash
docker compose up -d
```

Este comando construye las imágenes necesarias y deja el entorno listo. La primera ejecución tarda varios minutos porque descarga PySpark y el SDK de AWS; las siguientes arrancan en segundos.

MinIO y Airflow quedan corriendo como servicios. Las imágenes de Spark y dbt quedan disponibles para que Airflow cree contenedores de tarea cuando las necesite.

Airflow requiere alrededor de 40 segundos para migrar su base de metadatos. Está disponible cuando `docker compose ps` lo muestra como `healthy`.

### 4. Ejecutar el pipeline desde Airflow

Abrir `http://localhost:8080` e iniciar sesión con `admin` / `admin`.

En la lista de DAGs aparece `olist_medallion`, inicialmente pausado. Activarlo con el interruptor de la izquierda y presionar el botón **▶ (Trigger DAG)**.

En la vista **Graph** se ven las siete tareas ejecutándose en orden:

| Tarea | Qué hace |
| ----- | -------- |
| `ingest_bronze` | Sube los CSV al bucket `bronze` sin modificarlos |
| `validate_bronze` | Comprueba que los archivos sean legibles, completos y no estén vacíos |
| `transform_silver` | Construye las tres tablas Silver con PySpark, aplicando 23 validaciones |
| `validate_silver` | Verifica que no se hayan perdido ni duplicado filas entre Bronze y Silver |
| `dbt_build_gold` | Aplica la regla de negocio y construye el modelo analítico |
| `dbt_test` | Ejecuta los tests de dbt sobre el modelo y sus fuentes |
| `validate_gold` | Comprueba que unidades y facturación de Gold coincidan con Silver |

Las dependencias son lineales: si una tarea falla, queda en rojo y las siguientes no se ejecutan. Entrando a **Logs** puede verse qué validación falló y cuántas filas resultaron afectadas.

En una máquina de desarrollo, una corrida completa suele finalizar en pocos minutos una vez construidas las imágenes.

### 5. Comprobar las capas en MinIO

Abrir `http://localhost:9001` e iniciar sesión con `minioadmin` / `minioadmin`.

En **Object Browser** deberían existir tres buckets, creados automáticamente durante la ejecución:

| Bucket | Contenido |
| ------ | --------- |
| `bronze` | Los dos CSV originales, sin modificar |
| `silver` | `orders/`, `order_items/` y `order_product_sales/`, cada uno con su Parquet |
| `gold` | `daily_product_sales.parquet` |

Recorrer los buckets permite verificar el trayecto completo de los datos a través de las tres capas.

### 6. Revisar el resultado

El pipeline escribe el modelo analítico en la carpeta `output/`:

```text
output/
├── daily_product_sales.parquet     # resultado principal
├── daily_product_sales.csv         # mismo contenido, en formato legible
└── reports/                        # detalle de las validaciones ejecutadas
```

El CSV puede abrirse directamente:

```text
purchase_date,product_id,quantity,total_revenue,total_freight,orders_count
2016-09-15,5a6b04657a4c5ee34285d1e4619a96b4,3,134.97,8.49,1
2016-10-03,107177bf61755f05c604fe57e02467d6,1,119.90,13.56,1
```

### Si algo falla

| Síntoma | Causa |
| ------- | ----- |
| `Cannot connect to the Docker daemon` | Docker Desktop no está en ejecución |
| `http://localhost:8080` no responde | Airflow todavía está migrando su base de metadatos |
| Aparece `pull access denied` al construir | Docker intenta descargar las imágenes del proyecto antes de construirlas localmente. Es un aviso esperable; la construcción continúa |
| Se quiere empezar desde cero | `docker compose down -v` elimina los volúmenes sin tocar los datos de origen |

---

## 3. Dataset y alcance

Olist es una empresa brasileña que conecta comerciantes con los principales marketplaces del país. Su dataset público reúne información de pedidos, clientes, productos, vendedores, pagos, reseñas y geolocalización, repartida en varias fuentes relacionadas.

![Relaciones entre los datasets de Olist](docs/images/olist_dataset_relationships.png)

### Fuentes utilizadas

El pipeline trabaja con dos de esas fuentes, que son las que contienen la información necesaria para construir ventas por fecha y producto.

`olist_orders_dataset.csv` contiene **99.441 pedidos**, con un registro por pedido. Aporta el identificador del pedido, la fecha de compra y el estado.

| Columna | Descripción |
| ------- | ----------- |
| `order_id` | Identificador único del pedido |
| `customer_id` | Identificador asociado al pedido |
| `order_status` | Estado del pedido |
| `order_purchase_timestamp` | Fecha y hora de compra |
| `order_approved_at` | Fecha de aprobación |
| `order_delivered_carrier_date` | Fecha de entrega al transportista |
| `order_delivered_customer_date` | Fecha de entrega al cliente |
| `order_estimated_delivery_date` | Fecha estimada de entrega |

`olist_order_items_dataset.csv` contiene **112.650 registros**, con un registro por ítem. Aporta el producto, el precio, el costo de envío y la relación con el pedido.

| Columna | Descripción |
| ------- | ----------- |
| `order_id` | Pedido asociado |
| `order_item_id` | Posición del ítem dentro del pedido |
| `product_id` | Producto |
| `seller_id` | Vendedor |
| `shipping_limit_date` | Fecha límite de envío |
| `price` | Precio |
| `freight_value` | Costo de envío |

Ambas tablas se relacionan mediante `order_id`, en una relación de uno a muchos: un pedido puede contener varios ítems.

### Delimitación del alcance

Trabajar con dos fuentes es una decisión de alcance, no una limitación del diseño. El objetivo definido —ventas diarias por producto— se resuelve completamente con la información de pedidos e ítems, y acotarlo permitió profundizar en el pipeline en lugar de en la cantidad de tablas.

Las fuentes restantes permitirían extender el análisis más adelante hacia clientes y recompra, categorías de producto, medios de pago o distribución geográfica. La arquitectura está pensada para que incorporarlas signifique agregar tareas y modelos, no rediseñar el flujo.

---

## 4. Exploración inicial

Antes de escribir el pipeline se exploró el dataset con pandas, para conocer su estructura y detectar particularidades que condicionaran el diseño.

| Métrica | Resultado |
| ------- | --------: |
| Pedidos | 99.441 |
| Registros de `order_items` | 112.650 |
| Pedidos con al menos un ítem | 98.666 |
| Pedidos sin ítems | 775 |
| Productos | 32.951 |
| Vendedores | 3.095 |
| Estados de pedido | 8 |

### El hallazgo que define la transformación central

> **No existe una columna de cantidad.**

Cada fila de `order_items` representa **una unidad vendida**. Cuando un pedido incluye varias unidades del mismo producto, aparecen varias filas con la misma combinación `order_id + product_id`.

Por lo tanto, la cantidad se obtiene contando registros:

```text
quantity = count(*)   por   order_id + product_id
```

La exploración también permitió confirmar que `price` funciona como **precio unitario** y no como importe de la línea. De las 102.425 combinaciones distintas de `order_id + product_id`, 7.088 aparecen más de una vez, y en todas ellas el precio se repite idéntico entre filas. Si `price` fuera el total de la línea, ese valor variaría con la cantidad.

Esta verificación es la que sostiene que `item_revenue = unit_price × quantity`, y por eso el pipeline la controla en cada ejecución.

El análisis completo está en [`notebooks/01_exploracion_olist.ipynb`](notebooks/01_exploracion_olist.ipynb), y las particularidades detectadas en [`docs/anomalias_conocidas.md`](docs/anomalias_conocidas.md).

---

## 5. Arquitectura del pipeline

```text
                   PySpark                         dbt

┌──────────────┐              ┌──────────────┐              ┌──────────────┐
│    BRONZE    │ ───────────► │    SILVER    │ ───────────► │     GOLD     │
│   Datos      │              │   Datos      │              │   Modelo     │
│  originales  │              │  preparados  │              │  analítico   │
└──────────────┘              └──────────────┘              └──────────────┘
       │                             │                             │
       └────────────────────────── MinIO ──────────────────────────┘

                              Airflow
                              Docker
```

Cada capa cumple una función distinta:

**Bronze** conserva la información original tal como fue recibida.

**Silver** la prepara técnicamente: convierte tipos, integra las tablas, resuelve la granularidad y valida el resultado. No aplica ninguna regla de negocio.

**Gold** aplica la lógica de negocio y produce el modelo listo para consumo.

### Por qué una arquitectura por capas

El volumen actual del dataset es moderado, y estas transformaciones podrían resolverse con herramientas más simples. La arquitectura por capas no responde a una necesidad de procesamiento, sino a una de organización.

Separar las responsabilidades permite que cada parte del pipeline evolucione de forma independiente. Incorporar una nueva fuente afecta a Silver, pero no obliga a tocar Gold. Cambiar la definición de qué constituye una venta afecta a Gold, pero no requiere reprocesar Bronze ni Silver. Aumentar el volumen de datos afecta al motor de procesamiento, pero no a la estructura del flujo.

Esa separación es lo que hace que el pipeline pueda crecer sin rediseñarse. Las secciones siguientes explican, para cada capa, qué problema concreto resuelve la tecnología elegida.

---

## 6. Capa Bronze

**Recibe** los dos CSV desde `data/bronze/`.
**Almacena** los archivos en `s3://bronze/` sin modificarlos.
**Genera** una copia exacta de la fuente dentro del almacenamiento del pipeline.

La ingesta se realiza mediante [`jobs/ingest_bronze.py`](jobs/ingest_bronze.py), utilizando **boto3**.

### Por qué boto3 y no Spark

Bronze debe conservar una copia fiel del origen. Si los archivos se cargaran con Spark, el motor los leería y volvería a escribirlos, convirtiendo cada CSV en un directorio de fragmentos cuyo contenido ya no es idéntico al original.

Subirlos con boto3 preserva los bytes exactos. Esto es lo que permite que la capa sirva como punto de referencia: ante cualquier duda sobre un dato en Silver o Gold, siempre se puede volver al archivo tal como entró al sistema.

### Controles previos

Antes de comenzar las transformaciones, [`jobs/validate_bronze.py`](jobs/validate_bronze.py) comprueba para cada archivo que pueda leerse, que contenga las columnas esperadas y que no esté vacío. Son 6 validaciones críticas: si alguna falla, el pipeline se detiene sin avanzar hacia Silver.

---

## 7. Capa Silver

**Recibe** los archivos de Bronze.
**Transforma** tipos y fechas, integra las tablas y resuelve la granularidad.
**Genera** tres datasets en formato Parquet dentro de `s3://silver/`.

| Dataset | Registros | Granularidad | Job |
| ------- | --------: | ------------ | --- |
| `orders` | 99.441 | un pedido | `bronze_to_silver_orders.py` |
| `order_items` | 112.650 | un ítem | `bronze_to_silver_order_items.py` |
| `order_product_sales` | 102.425 | un pedido y producto | `build_order_product_sales.py` |

### `orders`

Las columnas temporales se convierten a `timestamp` y se deriva `purchase_date` a partir de `order_purchase_timestamp`, que es la fecha con la que Gold construye las métricas diarias.

Los valores nulos de las fechas se conservan. Son coherentes con el estado del pedido —uno cancelado nunca tiene fecha de entrega— y completarlos sería introducir información que no existe.

### `order_items`

Se tipan las columnas numéricas y temporales, manteniendo la granularidad original de una fila por unidad vendida:

```text
order_item_id        → integer
shipping_limit_date  → timestamp
price                → decimal(10,2)
freight_value        → decimal(10,2)
```

Los importes se manejan como `decimal` y no como punto flotante: al sumar cientos de miles de valores monetarios, el redondeo del punto flotante introduce diferencias que impedirían comparar totales entre capas.

### `order_product_sales`

Es la transformación central de la capa. Consolida los ítems agrupando por `order_id + product_id`:

```text
quantity      = count(*)
unit_price    = max(price)
item_revenue  = sum(price)
freight_total = sum(freight_value)
```

`quantity` cuenta filas porque cada fila es una unidad. `unit_price` toma el máximo del grupo, que equivale a cualquiera de sus valores porque el precio es constante dentro del grupo, condición que el pipeline verifica explícitamente. `item_revenue` suma los precios, lo que equivale a multiplicar el precio unitario por la cantidad.

Después se incorporan `purchase_date` y `order_status` desde `orders` mediante un **`LEFT JOIN`**. La elección no es indiferente: con un `INNER JOIN`, un ítem que apuntara a un pedido inexistente desaparecería sin dejar rastro. Con `LEFT JOIN` queda en el resultado con su contexto en nulo, y la validación de integridad lo detecta.

Silver no descarta pedidos por criterios de negocio. La tabla `orders` conserva los ocho estados y los 775 pedidos sin ítems. `order_product_sales`, al construirse desde `order_items`, contiene únicamente los pedidos que poseen ítems.

### Por qué PySpark

En el volumen actual, estas transformaciones también podrían resolverse con pandas o con SQL. PySpark se utiliza para concentrar el procesamiento estructural en una capa con un motor pensado para ese trabajo: aplicar esquemas explícitos, tipar de forma controlada, resolver joins y agregaciones, y escribir directamente en Parquet sobre almacenamiento compatible con S3.

El valor está en la continuidad: la lógica de transformación puede mantenerse si el volumen aumenta y la ejecución pasa posteriormente a un entorno Spark con mayores recursos, sin tener que reescribirla con otra herramienta.

---

## 8. Capa Gold

**Recibe** los datasets de Silver.
**Aplica** la lógica de negocio.
**Genera** el modelo analítico final en `s3://gold/` y una copia en `output/`.

El modelado se realiza con **dbt**, que contiene dos modelos:

| Modelo | Materialización | Función |
| ------ | --------------- | ------- |
| `stg_order_product_sales` | `view` | Capa de preparación sobre Silver |
| `daily_product_sales` | `external` (Parquet) | Modelo analítico final |

### La regla de negocio

Silver conserva los ocho estados de pedido. La decisión sobre cuáles representan una venta concretada se toma únicamente en Gold, y está parametrizada en `dbt_project.yml`:

```yaml
estados_venta_valida:
  - delivered
```

Esta separación tiene una consecuencia práctica: modificar la definición de venta —por ejemplo, incorporar los pedidos en tránsito— significa editar esa lista y reconstruir Gold. Bronze y Silver no se tocan.

### Estructura final

Granularidad: `purchase_date + product_id`.

| Campo | Tipo | Descripción |
| ----- | ---- | ----------- |
| `purchase_date` | `date` | Fecha de compra |
| `product_id` | `string` | Producto |
| `quantity` | `bigint` | Unidades vendidas |
| `total_revenue` | `decimal` | Facturación total |
| `total_freight` | `decimal` | Costo total de envío |
| `orders_count` | `bigint` | Cantidad de pedidos distintos |

### Por qué dbt teniendo PySpark

PySpark y dbt resuelven problemas distintos. PySpark prepara los datos; dbt los modela.

La lógica de negocio se expresa mejor en SQL, y dbt aporta alrededor de ese SQL lo que un script suelto no tiene: dependencias explícitas entre modelos, documentación junto a la definición, tests declarativos y parametrización de las reglas.

La ventaja arquitectónica es que una regla de negocio puede cambiar sin que se modifique el procesamiento estructural. A medida que aparezcan más modelos analíticos, dbt resuelve el orden entre ellos sin que haya que coordinarlo manualmente.

---

## 9. Resultado obtenido

| Métrica | Resultado |
| ------- | --------: |
| Registros | 92.587 |
| Unidades | 110.197 |
| Productos | 32.216 |
| Días | 612 |
| Facturación | BRL 13.221.498,11 |
| Flete | BRL 2.198.275,64 |
| Primera fecha | 2016-09-15 |
| Última fecha | 2018-08-29 |

### Evolución entre capas

| Capa | Registros | Unidades | Facturación |
| ---- | --------: | -------: | ----------: |
| Bronze `order_items` | 112.650 | 112.650 | BRL 13.591.643,70 |
| Silver `order_product_sales` | 102.425 | 112.650 | BRL 13.591.643,70 |
| Gold `daily_product_sales` | 92.587 | 110.197 | BRL 13.221.498,11 |

Entre Bronze y Silver los registros bajan porque las filas repetidas del mismo producto se consolidan en una sola, pero **no se pierde ninguna unidad**: la suma de `quantity` sigue siendo 112.650.

Entre Silver y Gold sí se reducen las unidades, en 2.453 (un 2,18 %), que corresponden a los pedidos cuyo estado no es `delivered`.

---

## 10. Calidad y testing

El proyecto distingue dos tipos de comprobación que responden a preguntas diferentes. Las **validaciones** verifican que una ejecución concreta haya producido datos consistentes. Los **tests** verifican que el código y el modelo se comporten como declaran, con datasets controlados.

### 10.1 Validaciones de datos

Se aplican 37 controles distribuidos en tres momentos, cada uno protegiendo contra un tipo de problema distinto.

| Momento | Controles | Protege contra |
| ------- | --------: | -------------- |
| Sobre Bronze | 6 | Archivos ilegibles, vacíos o con el esquema cambiado |
| Durante Silver | 23 | Claves duplicadas, campos obligatorios nulos, importes negativos, pérdida de unidades, problemas de integridad e inconsistencias de precio |
| Entre capas | 8 | Pérdidas, duplicaciones y diferencias de unidades o facturación entre una capa y la siguiente |

**Críticas y advertencias.** Una validación crítica detiene la ejecución antes de escribir, de modo que la capa nunca llega a contener datos que no pasaron el control. Una advertencia deja registro del problema y permite continuar.

La distinción existe porque el dataset tiene particularidades conocidas que no invalidan el resultado. En la ejecución actual las validaciones críticas pasan y quedan dos advertencias: cuatro registros con `shipping_limit_date` posterior al período del dataset, y los 775 pedidos que nunca tuvieron ítems.

**Dos controles destacados de Silver** protegen las suposiciones sobre las que se apoya todo el modelo. La *consistencia del precio unitario* verifica que dentro de cada `order_id + product_id` el precio sea siempre el mismo; si variara, `price` no sería unitario y el cálculo de facturación dejaría de ser válido. La *reconciliación de unidades* verifica que la suma de `quantity` después de agrupar sea exactamente igual a la cantidad de filas de `order_items`.

Los resultados de cada ejecución quedan en `output/reports/` en formato JSON, con el detalle de cada control, su severidad y las filas afectadas.

### 10.2 pytest

Los 36 tests se ejecutan contra almacenamiento local, sin necesidad de que MinIO esté levantado.

**`test_environment.py`** comprueba que el entorno Spark esté disponible y que las fuentes puedan leerse correctamente.

**`test_config.py`** comprueba que las rutas de las capas cambien correctamente entre almacenamiento local y MinIO. Si esta pieza fallara, los jobs escribirían en la ubicación equivocada sin producir ningún error.

**`test_transformations.py`** verifica el comportamiento de las transformaciones sobre datasets de pocas filas: que las fechas se conviertan correctamente, que los nulos se conserven, que los importes queden como decimales exactos, que tres filas del mismo producto produzcan `quantity = 3`, que dos productos distintos del mismo pedido queden separados y que se cumpla siempre `item_revenue = unit_price × quantity`.

**`test_quality.py`** introduce deliberadamente datos incorrectos para comprobar que los controles los detecten: una clave duplicada, un precio negativo, un precio no numérico, una fecha inválida, un precio inconsistente dentro del mismo `order_id + product_id` y un `order_id` que no existe en `orders`. Los tests confirman también que una validación crítica interrumpa la ejecución y que una advertencia no lo haga.

### 10.3 dbt tests

Los 24 tests se dividen en tres grupos.

**Sobre las fuentes de Silver (13)** funcionan como contrato entre PySpark y dbt: verifican unicidad y ausencia de nulos en las columnas que el modelado consume, de manera que un cambio en Silver se detecte antes de construir Gold.

**Sobre los modelos (8)** verifican que ninguna columna del resultado quede nula.

**Tests singulares (3)**, escritos como consultas SQL propias:

| Test | Qué comprueba |
| ---- | ------------- |
| Granularidad | Que no existan duplicados para `purchase_date + product_id` |
| Métricas válidas | Que no haya cantidades menores a 1 ni importes negativos |
| Reconciliación con Silver | Que unidades y facturación coincidan con Silver aplicando la misma definición de venta válida |

### 10.4 Por qué la reconciliación es importante

Un dataset puede no tener nulos, respetar su granularidad y cumplir con todos sus tipos, y aun así tener totales incorrectos. Un join que duplica filas o un filtro que descarta de más producen un resultado con la forma esperada, donde todos los demás controles pasan.

Esa clase de error solo se detecta comparando totales entre capas. Por eso la reconciliación exige que la facturación de Gold coincida al centavo con la de Silver filtrada por los mismos estados, y por eso los importes se manejan como `decimal`.

---

## 11. Orquestación e infraestructura

| Componente | Responsabilidad |
| ---------- | --------------- |
| MinIO | Almacenamiento de las tres capas |
| Airflow | Orquestación del flujo |
| Spark | Procesamiento |
| dbt | Modelado |
| Docker | Entorno de ejecución |

MinIO y Airflow son servicios persistentes. Spark y dbt funcionan como runtimes de tarea: se instancian para ejecutar un trabajo y se eliminan al terminar.

### Airflow

Airflow no procesa datos. Su responsabilidad es definir el orden de ejecución, gestionar las dependencias, controlar los estados, registrar los logs de cada tarea, manejar los reintentos e impedir que una etapa se ejecute si la anterior no terminó bien.

Cada tarea del DAG `olist_medallion` se ejecuta mediante `DockerOperator`, que instancia un contenedor con la imagen correspondiente, espera su código de salida y lo elimina. Gracias a eso, la imagen de Airflow no necesita incluir Java, Spark ni dbt.

El DAG se ejecuta con `schedule = None`. El dataset de Olist es histórico y cerrado, de modo que no hay datos nuevos que justifiquen corridas periódicas y la ejecución se dispara manualmente.

El valor se aprecia sobre todo pensando en el crecimiento: hoy son siete tareas en secuencia, y si más adelante se incorporan fuentes, transformaciones o modelos analíticos, se agregan como nuevas tareas con sus dependencias declaradas, sin construir un mecanismo de coordinación propio.

### MinIO

MinIO separa el **almacenamiento** del **procesamiento**. Las tres capas viven en una capa de almacenamiento independiente, a la que PySpark accede mediante `s3a://` y dbt mediante `s3://`.

```text
MinIO
├── bronze/   CSV originales
├── silver/   orders · order_items · order_product_sales
└── gold/     daily_product_sales.parquet
```

Esta separación permite que procesamiento, modelado y orquestación interactúen a través de una capa de almacenamiento común, sin depender directamente entre sí. Localmente se trabaja con la misma organización que tendría un data lake basado en object storage, y si el proyecto creciera hacia almacenamiento en la nube compatible con S3, el diseño conceptual de las capas se mantendría, aunque la migración requeriría ajustes de configuración y credenciales.

### Docker

El pipeline necesita cuatro runtimes distintos: Java con PySpark, dbt, Airflow y MinIO. Containerizarlos resuelve dos cosas: que el entorno de ejecución sea idéntico en cualquier máquina, y que esos runtimes no interfieran entre sí ni con el sistema donde se desarrolla.

El proyecto mantiene tres imágenes separadas por responsabilidad —`olist-pipeline:dev`, `olist-dbt:dev` y `olist-airflow:dev`— en lugar de una sola con todo. dbt no necesita Java, y Airflow no necesita ninguno de los dos. Separarlas evita reconstruir todo cuando cambia la dependencia de un solo componente.

### Parquet y DuckDB

Silver y Gold se almacenan en **Parquet** porque conserva los tipos, utiliza almacenamiento columnar y permite compresión. Evita tener que volver a castear en cada lectura, reduce el tamaño de las capas procesadas frente al CSV y, si el volumen creciera, permite leer únicamente las columnas necesarias.

dbt utiliza **DuckDB** como motor de consulta para leer esos Parquet y escribir el modelo final. La elección evita incorporar una base de datos o un metastore adicional únicamente para ejecutar el modelado. DuckDB no almacena los datos: estos siguen viviendo en MinIO.

Los detalles de implementación de cada componente están en [`docs/arquitectura.md`](docs/arquitectura.md).

---

## 12. Decisiones técnicas

| Decisión | Qué problema resuelve |
| -------- | --------------------- |
| Arquitectura Medallion | Mantiene separados el origen, la preparación técnica y la lógica de negocio |
| PySpark para el procesamiento | Concentra las transformaciones estructurales en un motor que admite mayor volumen sin cambiar la lógica |
| dbt para el modelado | Aporta dependencias, documentación y tests sobre la lógica de negocio expresada en SQL |
| MinIO como almacenamiento | Desacopla el almacenamiento del procesamiento |
| Airflow para la orquestación | Controla orden, estados, logs y reintentos, y permite sumar tareas al flujo |
| Docker por componente | Garantiza reproducibilidad y evita que los runtimes compartan dependencias |
| Parquet en Silver y Gold | Conserva tipos, comprime y permite lectura columnar |
| DuckDB como motor de dbt | Permite modelar sobre los Parquet sin incorporar una base de datos ni un metastore |
| `decimal` en los importes | Evita el error de redondeo del punto flotante al sumar valores monetarios |
| `quantity = count(*)` | El dataset no tiene columna de cantidad: cada fila es una unidad vendida |
| `LEFT JOIN` contra `orders` | Mantiene visible cualquier ítem sin pedido asociado para que lo detecten las validaciones |
| Regla de negocio en Gold | Permite redefinir qué es una venta sin reprocesar Bronze ni Silver |
| Validar antes de escribir | Impide que una capa quede publicada con datos que no pasaron los controles |
| Ingesta con boto3 | Conserva los archivos originales byte a byte |
| Conversiones tolerantes | Un valor malformado queda nulo y lo reporta la capa de calidad, en lugar de interrumpir el proceso con un error del motor |

El detalle de cada decisión, incluidas las alternativas descartadas, está en [`docs/decisiones_tecnicas.md`](docs/decisiones_tecnicas.md).

---

## 13. Estructura del repositorio

```text
olist-data-pipeline/
│
├── data/bronze/                     # CSV de origen
│
├── jobs/                            # Procesamiento con PySpark
│   ├── common/                      # Configuración, sesión Spark, esquemas,
│   │                                #   validaciones y acceso a almacenamiento
│   ├── ingest_bronze.py             # CSV → bucket bronze
│   ├── validate_bronze.py
│   ├── bronze_to_silver_orders.py
│   ├── bronze_to_silver_order_items.py
│   ├── build_order_product_sales.py # Transformación central
│   ├── validate_medallion.py        # Reconciliación entre capas
│   └── run_pipeline.py              # Ejecuta Bronze → Silver completo
│
├── dbt/                             # Modelado Silver → Gold
│   ├── dbt_project.yml              # Contiene la regla de negocio
│   ├── models/staging/ y gold/
│   └── tests/                       # Tests singulares
│
├── airflow/dags/olist_medallion.py  # DAG de siete tareas
│
├── tests/                           # Tests de pytest
├── docker/                          # Dockerfiles de los tres runtimes
├── docs/                            # Documentación complementaria
├── notebooks/                       # Exploración inicial
│
├── output/                          # Resultado del pipeline
│   ├── daily_product_sales.parquet
│   ├── daily_product_sales.csv
│   └── reports/
│
├── docker-compose.yml
├── quickstart.sh · run.ps1          # Ejecución alternativa sin Airflow
└── requirements.txt · requirements-dbt.txt
```

---

## 14. Comandos auxiliares

El camino recomendado para ejecutar el pipeline es el DAG de Airflow. Los comandos de esta sección están pensados para desarrollo y diagnóstico.

Ejecutar las etapas directamente, sin pasar por Airflow, muestra el log en la terminal:

```bash
docker compose run --rm spark python -m jobs.run_pipeline        # Bronze → Silver
docker compose run --rm dbt   dbt build                          # Silver → Gold
docker compose run --rm spark python -m jobs.validate_medallion  # Reconciliación
```

Los scripts `quickstart.sh` y `run.ps1 quickstart` encadenan esas etapas incluyendo la construcción de imágenes.

| Comando | Función |
| ------- | ------- |
| `docker compose run --rm spark pytest` | Ejecuta los tests de pytest |
| `docker compose run --rm dbt dbt show --select daily_product_sales --limit 10` | Muestra las primeras filas del modelo final |
| `docker compose logs -f airflow` | Sigue los logs de Airflow |
| `docker compose ps` | Estado de los servicios |
| `docker compose down` | Detiene el entorno conservando los datos |
| `docker compose down -v` | Detiene el entorno y elimina los volúmenes |

En Windows, `run.ps1` agrupa estos comandos con nombres más cortos: `build`, `up`, `all`, `test`, `dag`, `status`, `logs`, `shell`, `down` y `reset`.

---

## Documentación complementaria

* [`docs/arquitectura.md`](docs/arquitectura.md) — detalle de las capas y de la implementación de cada componente.
* [`docs/decisiones_tecnicas.md`](docs/decisiones_tecnicas.md) — fundamento de cada decisión y alternativas descartadas.
* [`docs/anomalias_conocidas.md`](docs/anomalias_conocidas.md) — particularidades del dataset y cómo las trata el pipeline.
* [`docs/roadmap.md`](docs/roadmap.md) — evolución de las etapas del proyecto.
* [`notebooks/01_exploracion_olist.ipynb`](notebooks/01_exploracion_olist.ipynb) — exploración inicial del dataset.
