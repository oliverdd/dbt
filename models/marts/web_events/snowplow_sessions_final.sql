{{
    config(
        materialized='table'
    )
}}

with sessions as (
    select * from  {{ ref('snowplow_sessions') }}
),

shopify_customers as (
    select * from {{ ref('customers') }}
),

gclids as (
    select * from {{ ref('snowplow_sessions_gclid') }}
),

join_adwords as (

    select
    
        sessions.*,
        date_trunc('day', cast(session_start as date)) as session_day,
        shopify_customers.customer_created_at,
        shopify_customers.customer_age_days,
        shopify_customers.customer_type,
        shopify_customers.lifetime_revenue,
        shopify_customers.lifetime_completed_orders,

--start to categorize source, medium, and campaign from snowplow fields

        case
            when first_page_url_query like '%gclid%' then 'cpc'
            when referer_url_host = 'com.google.android.googlequicksearchbox'
                then 'search'
            when marketing_medium is not null
                or marketing_source is not null
                or marketing_campaign is not null
                then lower(marketing_medium)
            when referer_medium is not null then lower(referer_medium)
            when referer_medium = 'unknown' then 'referral'
            else 'direct'
        end as attribution_channel,

        case
            when first_page_url_query like '%gclid%' then 'google'
            when referer_url_host = 'com.google.android.googlequicksearchbox'
                then 'Android'
            when marketing_medium is not null
                or marketing_source is not null
                or marketing_campaign is not null
                then lower(marketing_source)
            when referer_source is not null then lower(referer_source)
        end as attribution_source,

        case
            when gclids.adwords_campaign is not null
                then lower(gclids.adwords_campaign)
            when marketing_medium is not null
                or marketing_source is not null
                or marketing_campaign is not null
                then lower(marketing_campaign)
            when referer_url_host is not null then lower(referer_url_host)
        end as attribution_campaign,
        
        gclids.criteria_id,
        gclids.ad_group_id
        
    from sessions
    left join shopify_customers on sessions.inferred_user_id = shopify_customers.email
    left join gclids 
        on sessions.user_snowplow_domain_id = gclids.user_snowplow_domain_id
        and  sessions.session_index = gclids.session_index

),

add_ad_performance as (
    
    select 

        *,

        case
            when attribution_source like '%facebook%' 
                and attribution_channel like '%paid%'
                then 
                    {{dbt_utils.surrogate_key(
                        'session_day',
                        'marketing_content',
                        'marketing_term',
                        'marketing_campaign'
                    )}}
                    
            when attribution_source like '%google%' 
                and criteria_id is not null
                then 
                    {{dbt_utils.surrogate_key(
                        'session_day',
                        'criteria_id',
                        'ad_group_id',
                        'attribution_campaign'
                    )}}
            else null
        end as ad_performance_id
        
    from join_adwords
)

select * from add_ad_performance