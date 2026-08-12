# Decisiones técnicas

Las elecciones que no eran obvias, con el porqué y la alternativa que se descartó.

## Entorno

### Todo el runtime corre en contenedores

**Decisión:** ni Java, ni PySpark, ni dbt se instalan en la máquina del desarrollador.

**Por qué:** Spark 4 necesita Java 17 y Python ≤ 3.12; la máquina de desarrollo tenía
Python 3.14 y ningún JDK. En vez de instalar dos runtimes y arrastrar un conflicto de
versiones, el entorno vive en imágenes Docker y el proyecto se monta como volumen. Se
edita en el host y se ejecuta adentro, sin reconstruir la imagen en cada cambio.

**Excepción:** el notebook de exploración corre en el host con pandas. No necesita
Spark y obligarlo a pasar por un contenedor solo complicaría abrirlo.

### Imágenes separadas para Spark, dbt y Airflow

**Decisión:** tres imágenes en lugar de una.

**Por qué:** dbt no necesita Java ni PySpark, y Airflow no necesita ninguno de los dos
porque solo lanza contenedores. Una imagen única pesaría varios GB y cualquier cambio
en una dependencia obligaría a reconstruir todo. La separación también refleja la de
responsabilidades del proyecto.

### Versiones del conector S3A fijadas a mano

**Decisión:** `hadoop-aws:3.4.1` y `awssdk:bundle:2.24.6`, descargados al construir la
imagen.

**Por qué:** no son versiones libres. `hadoop-aws` tiene que coincidir con el Hadoop
que trae PySpark 4.0.4 (3.4.1), y el SDK con el que declara ese `hadoop-aws` (se
verifica en el `pom` de `hadoop-project`). Una combinación distinta no falla al
construir: falla en runtime con `NoSuchMethodError`, que es mucho más difícil de
diagnosticar.

## Procesamiento

### Bronze se lee con todas las columnas como texto

**Decisión:** esquemas explícitos de solo `string`, y el tipado real en Silver.

**Por qué:** dos razones. Bronze debe conservar los datos tal como vienen, y si Spark
infiriera tipos, un valor mal formado se volvería nulo sin que nadie se entere. Además
`inferSchema` obliga a leer el archivo entero una vez más solo para adivinar.

### `try_cast` en lugar de `cast`

**Decisión:** todas las conversiones de tipo usan las variantes tolerantes
(`try_cast`, `try_to_timestamp`).

**Por qué:** Spark 4 corre en modo ANSI por defecto, donde un valor malformado hace
fallar el job con una excepción de la JVM. Con la variante tolerante el valor queda
nulo y el problema lo detecta la validación de calidad, que informa **cuántas** filas
están mal en vez de morir en la primera. Un stack trace de Java no es un reporte de
calidad de datos.

Se descubrió porque un test que inyectaba un precio no numérico reventaba en lugar de
fallar la validación.

### Los importes son `decimal`, no `double`

**Decisión:** `DecimalType(10, 2)` para `price` y `freight_value`.

**Por qué:** en punto flotante, sumar cientos de miles de precios arrastra error de
redondeo. Con decimal, la facturación de Gold cuadra al centavo contra la de Silver, y
eso es justamente lo que verifica el test de reconciliación.

### La cantidad se calcula contando filas

**Decisión:** `quantity = count(*)` agrupando por `(order_id, product_id)`.

**Por qué:** el dataset no tiene columna de cantidad. Cada fila de `order_items` es una
unidad vendida. Verificado en la exploración: de 102.425 combinaciones, 7.088 aparecen
más de una vez, y en todas ellas el precio es idéntico entre filas — o sea que `price`
es unitario. Ver [anomalias_conocidas.md](anomalias_conocidas.md).

### `LEFT JOIN` contra `orders`, no `INNER`

**Decisión:** al incorporar el contexto del pedido se usa `LEFT`.

**Por qué:** con `INNER`, un ítem que apuntara a un pedido inexistente desaparecería en
silencio. Con `LEFT` queda con contexto nulo y la validación crítica de integridad
referencial lo detecta. Hoy los dos dan el mismo resultado porque no hay huérfanos;
la diferencia importa el día que los haya.

### `coalesce(1)` al escribir Silver

**Decisión:** un único archivo Parquet por tabla.

**Por qué:** a esta escala (cientos de miles de filas) un archivo es más simple de
inspeccionar y de consumir desde dbt que decenas de fragmentos. Con volúmenes mayores
habría que particionar por fecha y sacar el `coalesce`.

## Calidad

### Dos niveles de severidad

**Decisión:** validaciones críticas que cortan la ejecución, y advertencias que solo se
registran.

**Por qué:** si todo fuera crítico el pipeline no correría nunca, porque el dataset
tiene anomalías conocidas y legítimas. Si todo fuera advertencia nadie las miraría. La
distinción es lo que hace que un rojo signifique algo.

### Se valida antes de escribir

**Decisión:** los checks corren sobre el DataFrame transformado y en memoria, antes del
`write`.

**Por qué:** así Silver nunca llega a contener datos que no pasaron los controles. Si se
validara después, habría que borrar lo escrito o convivir con una capa corrupta.

## Almacenamiento

### MinIO en vez de disco, pero con las dos opciones vivas

**Decisión:** `jobs/common/config.py` decide si una capa es una carpeta local o un
bucket, según la variable `OLIST_STORAGE`.

**Por qué:** los tests unitarios no deberían necesitar que MinIO esté levantado. Y como
ningún job construye rutas a mano, incorporar MinIO no requirió tocar ni un job: solo
ese módulo.

### La ingesta a Bronze no usa Spark

**Decisión:** los CSV se suben con boto3, byte a byte.

**Por qué:** si Bronze se cargara con Spark, el CSV original se convertiría en un
directorio de fragmentos reescritos y la capa dejaría de conservar la fuente. Se
verificó que los tamaños en el bucket coinciden exactamente con los de los archivos
originales.

## Modelado

### dbt con adaptador DuckDB

**Decisión:** `dbt-duckdb` para el modelado Silver → Gold.

**Por qué:** DuckDB lee los Parquet directamente desde MinIO por S3 y escribe Gold como
Parquet, sin necesitar un motor de base de datos aparte ni un metastore. Es el camino
más corto entre "hay Parquet en un bucket" y "hay modelos dbt con tests".

**Alternativa descartada:** `dbt-spark` en modo `session`. Habría mantenido un solo
motor, pero exige registrar las tablas Silver en un catálogo (Hive o Derby) antes de
poder referenciarlas como `source`, y ese metastore hay que persistirlo entre
corridas. Mucha infraestructura para ejecutar unas consultas de agregación.

**Consecuencia a tener en cuenta:** DuckDB es el motor de consulta, no el almacén. Los
datos siguen viviendo en MinIO; el archivo `.duckdb` solo guarda el catálogo y se
regenera. Si el volumen creciera hasta no entrar en memoria, habría que reconsiderarlo.

### La definición de "venta válida" vive en Gold

**Decisión:** Silver conserva los ocho estados de pedido; el filtro por `delivered` está
en `dbt_project.yml` como la variable `estados_venta_valida`.

**Por qué:** qué cuenta como venta es una pregunta de negocio, no técnica. Ponerla en
Silver obligaría a reprocesar todo cada vez que cambie. Así, incluir los `shipped` es
editar una lista y volver a correr dbt, sin tocar PySpark.

### Tests singulares en lugar de `dbt_utils`

**Decisión:** los tests de granularidad, rango y reconciliación son SQL propio en
`dbt/tests/`.

**Por qué:** evita una dependencia externa y una descarga de paquetes en cada entorno,
y el SQL explícito documenta mejor qué se está verificando. Para tests genéricos
repetidos en muchos modelos, `dbt_utils` valdría la pena.

## Orquestación

### Airflow lanza contenedores en lugar de ejecutar el trabajo

**Decisión:** `DockerOperator` para todas las tareas.

**Por qué:** mantiene a Airflow en su única responsabilidad, que es orquestar. La
alternativa — instalar Java, PySpark y dbt dentro de la imagen de Airflow y usar
`BashOperator` — daría una imagen enorme y mezclaría las capas del proyecto.

**Costo:** hay que montarle el socket de Docker y correr el contenedor como root, y el
DAG necesita saber la ruta del proyecto **en el host** (variable `HOST_PROJECT_PATH`),
porque el demonio resuelve los bind mounts contra su propio sistema de archivos. En
Windows con Docker Desktop esa ruta es `/run/desktop/mnt/host/<letra>/...`, no la del
explorador de archivos.

### SQLite y `SequentialExecutor`

**Decisión:** un solo contenedor de Airflow, sin Postgres ni Celery.

**Por qué:** el DAG es lineal y se dispara a mano. Postgres, Redis y workers serían tres
servicios más para no ganar nada. En producción la elección sería la contraria.

### El DAG no tiene `schedule`

**Decisión:** `schedule=None`.

**Por qué:** el dataset es histórico y cerrado, no hay datos nuevos que justifiquen una
corrida periódica. Programarlo daría corridas que reprocesan siempre lo mismo.
