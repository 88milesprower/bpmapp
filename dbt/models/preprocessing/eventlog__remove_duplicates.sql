with leads as (
    select
        *,
        lead(event_id) over w as next_event_id,
        lead(created_at) over w as next_created_at
    from {{ ref('src_eventlog' )}}
    window w as (partition by case_id order by created_at)
), eliminate_leads as (
    select *
    from leads
    where event_id != next_event_id OR next_event_id is NULL
),
final as (
    select
        l.created_at,
        l.case_id,
        l.event_id,
        l.resource_id,
        l.next_event_id,
        l.next_created_at,
        (l.next_created_at - l.created_at) as event_duration,
        c.case_start,
        c.case_end,
        c.start_time,
        c.end_time,
        c.city,
        c.loan_sum,
        c.started_at,
        c.closed_at
    from eliminate_leads as l
    left join {{ ref('src_cases') }} c on l.case_id = c.case_id
)

select * from final