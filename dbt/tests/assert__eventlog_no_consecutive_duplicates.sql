select
    *
from (
    select
        lead(event_id) over(partition by case_id order by created_at) as next_event_id,
        event_id
    from {{ ref('eventlog__remove_duplicates' )}}
) leads
where event_id = next_event_id