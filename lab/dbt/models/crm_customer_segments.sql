-- Each customer's spend band, for marketing to target. Added on day 6 of the
-- fortnight (lab/scenario.yaml): before then the DAG leaves it out of the run.
select c.customer_id, c.email,
       case when coalesce(sum(i.amount_eur), 0) >= 100 then 'high' else 'standard' end
           as spend_band
from {{ ref('crm_customers') }} c
left join {{ ref('billing_invoices') }} i using (customer_id)
group by c.customer_id, c.email
