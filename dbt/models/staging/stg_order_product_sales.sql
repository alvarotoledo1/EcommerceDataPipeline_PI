-- Capa de staging sobre Silver.
--
-- No aplica reglas de negocio ni filtra: solo le da nombre y forma estable a lo que
-- viene de Silver, para que los modelos de Gold no dependan de la ruta física de los
-- Parquet en MinIO. Si mañana Silver cambia de ubicación, se toca únicamente el
-- `source` y nada más.

select
    order_id,
    product_id,
    purchase_date,
    order_status,
    quantity,
    unit_price,
    item_revenue,
    freight_total

from {{ source('silver', 'order_product_sales') }}
