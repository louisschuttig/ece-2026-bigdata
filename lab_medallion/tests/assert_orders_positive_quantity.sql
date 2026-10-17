-- Exercise 2: every order has a strictly positive quantity
select order_id, quantity
from {{ ref('stg_orders') }}
where quantity is null or quantity <= 0
