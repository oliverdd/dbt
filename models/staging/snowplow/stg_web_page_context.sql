with staged_events as (
    select * from  {{ ref('stg_events') }}
),

context as (

    select 
        event_id as root_id, 
        c.value:data:id::string as id,
        collector_tstamp
    from staged_events,
    lateral flatten (input => contexts:data) c
)

select * from context    