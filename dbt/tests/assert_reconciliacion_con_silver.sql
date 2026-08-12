-- Reconciliación entre capas: Gold no puede inventar ni perder facturación.
--
-- La suma de `total_revenue` en Gold tiene que coincidir exactamente con la suma de
-- `item_revenue` en Silver, restringida a los mismos estados de pedido. Es el test
-- que detecta un JOIN que duplica filas o un filtro que se comió datos, que son los
-- dos errores que más silenciosamente arruinan un modelo analítico.

with gold as (

    select
        sum(quantity) as unidades,
        sum(total_revenue) as facturacion
    from {{ ref('daily_product_sales') }}

),

silver as (

    select
        sum(quantity) as unidades,
        sum(item_revenue) as facturacion
    from {{ ref('stg_order_product_sales') }}
    where order_status in ('{{ var("estados_venta_valida") | join("', '") }}')

)

select
    gold.unidades as unidades_gold,
    silver.unidades as unidades_silver,
    gold.facturacion as facturacion_gold,
    silver.facturacion as facturacion_silver

from gold
cross join silver

where gold.unidades <> silver.unidades
   or abs(gold.facturacion - silver.facturacion) > 0.01
