with raw_events as (
    
    select * from raw.snowplow.events
),

add_row_number as (
    select
        raw_events.*,
        row_number() over (partition by event_id 
            order by collector_tstamp desc) as most_recent_event
    from raw_events
),

filter_duplicates as (
    select * from add_row_number
    where most_recent_event = 1
)

select * from filter_duplicates