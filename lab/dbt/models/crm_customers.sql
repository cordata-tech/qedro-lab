-- The customer master: one row per customer, as the CRM domain keeps it.
select customer_id, name, email, postcode, city, joined_on::date as joined_on
from {{ source('raw', 'customers') }}
