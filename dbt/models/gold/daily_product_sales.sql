-- Modelo analítico principal: ventas diarias por producto.
--
-- Acá se aplica la única regla de negocio del proyecto: qué estados de pedido
-- representan una venta concretada. Está parametrizada en `dbt_project.yml` como
-- `estados_venta_valida` para que cambiarla sea editar una lista, no reescribir SQL.
--
-- Granularidad: un registro por (purchase_date, product_id).

-- Además de escribir en el bucket `gold`, deja una copia en `output/`. Es el
-- entregable del proyecto: el archivo que alguien abre para ver el resultado sin
-- tener que levantar MinIO. En Parquet (el formato del pipeline) y en CSV (para
-- poder mirarlo con cualquier herramienta).

{{
    config(
        materialized="external",
        location="s3://gold/daily_product_sales.parquet",
        format="parquet",
        post_hook=[
            "COPY (SELECT * FROM {{ this }} ORDER BY purchase_date, product_id) "
            "TO '{{ var('directorio_salida') }}/daily_product_sales.parquet' (FORMAT PARQUET)",
            "COPY (SELECT * FROM {{ this }} ORDER BY purchase_date, product_id) "
            "TO '{{ var('directorio_salida') }}/daily_product_sales.csv' (FORMAT CSV, HEADER)",
        ],
    )
}}

with ventas_validas as (

    select *
    from {{ ref('stg_order_product_sales') }}
    where order_status in ('{{ var("estados_venta_valida") | join("', '") }}')

)

select
    purchase_date,
    product_id,

    -- Unidades vendidas. `quantity` ya viene resuelto desde Silver contando las
    -- filas de order_items, porque el dataset no trae una columna de cantidad.
    --
    -- El cast a BIGINT no es cosmético: DuckDB suma enteros en HUGEINT, que Parquet
    -- no soporta, así que al escribir lo degradaría a `double`. Un conteo de unidades
    -- guardado como float es un tipo incorrecto y, con volúmenes grandes, impreciso.
    cast(sum(quantity) as bigint) as quantity,

    -- Facturación del producto en el día, sin flete.
    sum(item_revenue) as total_revenue,

    -- Métricas de apoyo, más allá del mínimo pedido.
    sum(freight_total) as total_freight,
    count(distinct order_id) as orders_count

from ventas_validas
group by purchase_date, product_id
