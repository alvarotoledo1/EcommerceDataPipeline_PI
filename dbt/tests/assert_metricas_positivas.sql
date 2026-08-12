-- Las métricas de venta no pueden ser cero ni negativas.
--
-- `quantity` sale de contar filas, así que un valor menor a 1 significaría que el
-- GROUP BY produjo un grupo vacío, algo imposible salvo que la lógica esté rota.
-- `total_revenue` en negativo indicaría precios corruptos que las validaciones de
-- Silver deberían haber frenado antes.

select
    purchase_date,
    product_id,
    quantity,
    total_revenue,
    total_freight

from {{ ref('daily_product_sales') }}

where quantity < 1
   or total_revenue < 0
   or total_freight < 0
