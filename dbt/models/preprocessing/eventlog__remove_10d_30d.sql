with final as (
    select
        *,
        extract(days from event_duration) as event_duration_days,
        extract(hours from event_duration) as event_duration_hours
    from {{ ref("eventlog__remove_duplicates")}}
    where case_id not in (
        select distinct case_id
        from {{ ref("eventlog__remove_duplicates")}}
        where
            (extract(days from event_duration) in (10, 30)) and
            (extract(hours from (event_duration - '10 days'::interval)) = 0 or extract (hours from (event_duration - '30 days'::interval)) = 0)
    )
-- test: number of events with rounded duration of 10d or 30d is less than 3% of all events
)

select * from final