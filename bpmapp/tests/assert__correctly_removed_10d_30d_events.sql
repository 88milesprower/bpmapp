-- number of events with rounded duration of 10d or 30d is less than 3% of all events

select *
from (
    select event_duration_days, 100 * count(*) / sum(count(*)) over() as pct
    from {{ ref("eventlog__remove_10d_30d") }}
    group by event_duration_days
) pcts
where (event_duration_days = 10 OR event_duration_days = 30) and pct >= 3