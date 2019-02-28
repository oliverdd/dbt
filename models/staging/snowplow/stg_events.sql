with source as (
    select * from raw.snowplow.events
)

select * from source