-- Each customer's latest consent per channel.
select customer_id, channel, granted::boolean as granted, recorded_on::date as recorded_on
from (
    select *, row_number() over (
        partition by customer_id, channel order by recorded_on desc, event_id desc
    ) as latest
    from {{ source('raw', 'consent_events') }}
) ranked
where latest = 1
