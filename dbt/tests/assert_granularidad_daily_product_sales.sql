-- La granularidad declarada del modelo es un registro por (purchase_date, product_id).
--
-- Un test de `unique` sobre una sola columna no alcanza acá: ninguna de las dos es
-- única por separado, solo la combinación. Si este test devuelve filas, el GROUP BY
-- del modelo cambió o alguien agregó una dimensión sin actualizar la documentación.

select
    purchase_date,
    product_id,
    count(*) as registros

from {{ ref('daily_product_sales') }}

group by purchase_date, product_id
having count(*) > 1
