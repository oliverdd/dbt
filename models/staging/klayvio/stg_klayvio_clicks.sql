with source as (
    select * from raw.perfect_keto_klaviyo.click
),

renamed as (
    
    select
        datetime::timestamp as date_time,
        event_properties:"Campaign Name"::string as campaign_name,
        person:email::string as email,
        *
    from source
)

select * from renamed    