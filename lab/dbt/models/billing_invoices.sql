-- One invoice line per order, joined to the customer it is addressed to.
select o.order_id, o.customer_id, c.name, c.email, o.amount_eur::numeric(10, 2) as amount_eur,
       o.ordered_on::date as ordered_on
from {{ source('raw', 'orders') }} o
join {{ ref('crm_customers') }} c using (customer_id)
