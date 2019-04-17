with sessions as (

    select * from {{ ref('snowplow_sessions') }}
    
),

snowplow_gclids as (

    select * from {{ ref('snowplow_sessions__gclids') }}
    
),

mapped as (

    select
    
        sessions.*,
        snowplow_gclids.criteria_id,
        snowplow_gclids.ad_group_id,
        snowplow_gclids.adwords_campaign
        
    from sessions
    left join snowplow_gclids using (user_snowplow_domain_id, session_index)
    
)

select * from mapped