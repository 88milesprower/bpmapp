with eventlog as (
    select * from {{ source('bpmsrc', 'eventlog') }}
),
final as (
    select * from eventlog
)

select * from final