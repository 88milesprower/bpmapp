with casted_cases as (
    select
        case_id,
        case_start,
        case_end,
        start_time,
        end_time,
        to_timestamp(start_time) as started_at,
        to_timestamp(end_time) as closed_at,
        city,
        loan_sum
    from {{ source('public', 'cases') }}
),
final as (
    select * from casted_cases
)

select * from final