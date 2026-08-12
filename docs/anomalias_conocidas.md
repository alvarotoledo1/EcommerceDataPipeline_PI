# Anomalías y particularidades conocidas del dataset

Hallazgos de la exploración (`notebooks/01_exploracion_olist.ipynb`) que condicionan el
diseño del pipeline. Todos verificados sobre los datos reales.

## 1. `order_items` no tiene columna de cantidad

**Qué pasa:** cada fila representa **una unidad vendida**. Si un pedido lleva 3 unidades
del mismo producto, hay 3 filas con el mismo `order_id` y `product_id`, numeradas 1, 2 y
3 en `order_item_id`.

**Evidencia:** de 102.425 combinaciones `(order_id, product_id)`, 7.088 (6,92 %)
aparecen en más de una fila. En los 7.088 casos el precio es idéntico entre las filas del
grupo, lo que confirma que `price` es el **precio unitario** y no el importe de la línea.

**Decisión:** `quantity = count(*)` agrupando por `(order_id, product_id)`;
`item_revenue = sum(price)`, que equivale a `unit_price * quantity`.

**Por qué importa:** el 90 % de los pedidos tiene un solo ítem, así que tratar cada fila
como un producto distinto da resultados correctos en la mayoría de los casos y el error
pasa desapercibido. Se rompe justamente en los pedidos grandes, que son los que más
pesan en la facturación.

## 2. `customer_id` no identifica a la persona

**Qué pasa:** `orders` tiene 99.441 filas y 99.441 valores distintos de `customer_id`,
uno por pedido. Identifica la relación cliente-pedido, no al comprador.

**Decisión:** no usar `customer_id` para contar clientes ni medir recompra. Eso requiere
`customer_unique_id`, en `olist_customers_dataset.csv`, que hoy no forma parte del flujo.

## 3. 775 pedidos sin ítems asociados

**Qué pasa:** 775 `order_id` de `orders` no tienen ninguna fila en `order_items`. Se
concentran en `unavailable` (603) y `canceled` (164); el resto son `created` (5),
`invoiced` (2) y `shipped` (1). En sentido inverso no hay huérfanos: todos los ítems
corresponden a un pedido existente.

**Decisión:** se conservan en la tabla Silver `orders`. No aparecen en
`order_product_sales` porque ese dataset parte del detalle de ítems, lo cual es
esperado y no un error.

## 4. Nulos en las fechas de `orders`

**Qué pasa:** `order_approved_at` (160), `order_delivered_carrier_date` (1.783) y
`order_delivered_customer_date` (2.965, un 2,98 %) tienen nulos. Son coherentes con el
estado del pedido: uno cancelado nunca tiene fecha de entrega.

**Decisión:** se conservan como nulos. No se imputan, porque completarlos sería inventar
información.

## 5. `shipping_limit_date` fuera del período del dataset

**Qué pasa:** la fecha límite de envío llega hasta 2020-04-09, más de un año después del
último pedido registrado (2018-10-17).

**Decisión:** los registros **se conservan**. Se reporta como **advertencia**, no como
error crítico: es una inconsistencia en unos pocos registros de una columna que no
participa de las métricas.

## 6. Distribución de `price` muy asimétrica

**Qué pasa:** mediana 74,99, percentil 99 en 890 y máximo 6.735. Los valores altos son
productos reales, no errores de carga. No hay precios negativos ni nulos, el mínimo es
0,85.

**Decisión:** no se filtra nada. Se valida `price >= 0` como control crítico. En Gold,
cualquier promedio conviene acompañarlo con la mediana.

## 7. Extremos del período poco representativos

**Qué pasa:** el rango de compras va del 2016-09-04 al 2018-10-17, pero septiembre de
2016 tiene 4 pedidos, diciembre de 2016 tiene 1, y del otro lado septiembre de 2018
tiene 16 y octubre 4. El volumen real arranca en 2017 y se corta en agosto de 2018.

**Decisión:** Silver conserva todo el rango. Si en Gold se construyen series temporales,
recortar a la ventana con volumen real para que los extremos no parezcan una caída.

## 8. `order_status` fuertemente desbalanceado

**Qué pasa:** `delivered` concentra el 97,02 % (96.478 pedidos). El resto: `shipped`
(1.107), `canceled` (625), `unavailable` (609), `invoiced` (314), `processing` (301),
`created` (5) y `approved` (2).

**Decisión:** Silver conserva todos los estados. El filtro por venta válida se define en
Gold, inicialmente `delivered`.
