with eventlog as (
    select
        _time as created_at_time,
        case_id,
        event_id,
        resource_id,
        to_timestamp(_time) as created_at
    from {{ source('public', 'eventlog') }}
),
final as (
    select * from eventlog
)

select * from final