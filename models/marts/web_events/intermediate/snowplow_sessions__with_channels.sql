{{
    config(
        materialized='table'
    )
}}


with sessions as (
    
    select * from  {{ ref('snowplow_sessions__mapped') }}
    
),

with_channels as (
    
    select 
        
        *,
        case
            when first_page_url_query like '%gclid%' 
                or lower(marketing_medium) in ('paidsearch', 'cpc', 'shopping')
                then 'paid search'
            when referer_url_host = 'com.google.android.googlequicksearchbox'
                and lower(marketing_source) != 'facebook' then 'search'
            when marketing_medium in ('ads', 'paid_social') 
                and lower(marketing_source) = 'pinterest' then 'pinterest ads'
            when marketing_medium in ('paid', 'paid_social')
                and lower(marketing_source) = 'facebook' then 'facebook ads'
            when lower(marketing_medium) = 'social' then 'social'
            when marketing_medium is not null
                or marketing_source is not null
                or marketing_campaign is not null
                then lower(marketing_medium)
            when referer_medium is not null then lower(referer_medium)
            when referer_medium = 'unknown' then 'referral'
            else 'direct'
        end as channel,

        case
            when first_page_url_query like '%gclid%' then 'google'
            when referer_url_host = 'com.google.android.googlequicksearchbox'
                then 'Android'
            when marketing_medium is not null
                or marketing_source is not null
                or marketing_campaign is not null
                then lower(marketing_source)
            when referer_source is not null then lower(referer_source)
        end as platform,

        case
            when adwords_campaign is not null
                then lower(adwords_campaign)
            when marketing_medium is not null
                or marketing_source is not null
                or marketing_campaign is not null
                then lower(marketing_campaign)
            when referer_url_host is not null then lower(referer_url_host)
        end as campaign
        
    from sessions
    
)

select * from with_channels