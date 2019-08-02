{{
    config(
        materialized='table'
    )
}}


with sessions as (
    
    select * from {{ ref('snowplow_sessions__mapped') }}
    
),

criteria as (

    select 
        campaign_id::string as campaign_id,
        campaign_name::string as campaign_name
    from analytics.adwords_criteria_performance
    
),

joined as (
    select *,
        case when adwords_campaign is null and marketing_campaign = campaign_id then campaign_name end as adwords_campaign2
    from sessions s 
    left join criteria c on s.marketing_campaign = c.campaign_id 

),

with_channels as (
    
    select 
        
        *,
        case
            when lower(marketing_medium) = 'affiliate' then 'affiliate'
            when lower(marketing_medium) = 'influencers' then 'influencers'
            when first_page_url_query like '%gclid%' 
                or lower(marketing_medium) in ('paidsearch', 'cpc', 'shopping')
                then 'paid search'
            when referer_url_host = 'com.google.android.googlequicksearchbox'
                and lower(marketing_source) != 'facebook' then 'search'
            when marketing_medium in ('ads', 'paid_social') 
                and lower(marketing_source) = 'pinterest' then 'pinterest ads'
            when (marketing_medium in ('paid', 'paid_social') and lower(marketing_source) = 'facebook') 
                or (lower(marketing_source) = 'geistm' and marketing_campaign ilike 'fb%') then 'facebook ads'
            when lower(marketing_medium) = 'social' then 'social'
            when lower(marketing_source) = 'criteo' then 'criteo'
            when marketing_medium is not null
                or marketing_source is not null
                or marketing_campaign is not null
                then lower(marketing_medium)
            when referer_medium is not null then lower(referer_medium)
            when referer_medium = 'unknown' then 'referral'
            else 'direct'
        end as channel,

        case 
            when (marketing_medium = 'shopping' or adwords_campaign ilike '%shopping%' or marketing_campaign ilike '%shopping%'
                or adwords_campaign2 ilike '%shopping%' or adwords_campaign ilike '%pla%' or adwords_campaign2 ilike '%pla%' 
                or marketing_campaign ilike '%pla%') and first_page_url_query like '%gclid%' then 'google-shopping'
            when (adwords_campaign ilike '%nonbranded%' or adwords_campaign2 ilike '%nonbranded%' 
                or marketing_campaign ilike '%nonbranded%') and first_page_url_query like '%gclid%' then 'google-non-branded'
            when (adwords_campaign ilike '%branded%' or adwords_campaign2 ilike '%branded%' 
                or marketing_campaign ilike '%branded%') and first_page_url_query like '%gclid%' then 'google-branded'
            when (adwords_campaign ilike '%discover%' or adwords_campaign2 ilike '%discover%' 
                or marketing_campaign ilike '%discover%') and first_page_url_query like '%gclid%' then 'google-discovery'
            when (marketing_medium = 'video' or adwords_campaign ilike '%video%' or adwords_campaign2 ilike '%video%' 
                or marketing_campaign ilike '%video%') and first_page_url_query like '%gclid%' then 'youtube'
            when first_page_url_query like '%gclid%' then 'google'
            
            when (marketing_medium = 'shopping' or marketing_campaign ilike '%shopping%' or marketing_campaign ilike '%pla%') 
                and marketing_source = 'bing' then 'bing-shopping'
            when marketing_campaign ilike '%nonbranded%' and marketing_source = 'bing' then 'bing-non-branded'
            when (marketing_campaign ilike '%branded%' or marketing_campaign ilike '%brand%') 
                and marketing_source = 'bing' then 'bing-branded'
            
            when marketing_medium in ('ads', 'paid_social') 
                and lower(marketing_source) = 'pinterest' then 'pinterest ads'
            
            when (marketing_medium in ('paid', 'paid_social') and lower(marketing_source) = 'facebook') 
                or (lower(marketing_source) = 'geistm' and marketing_campaign ilike 'fb%') then 'facebook ads'
            
            when marketing_medium is not null
                or marketing_source is not null
                or marketing_campaign is not null
                then lower(marketing_source)
            when referer_source is not null then lower(referer_source)
        end as platform,

        case
            when adwords_campaign is not null
                then lower(adwords_campaign)
            when adwords_campaign is null and marketing_campaign = campaign_id
                then adwords_campaign2
            when marketing_medium is not null
                or marketing_source is not null
                or marketing_campaign is not null
                then lower(marketing_campaign)
            when referer_url_host is not null then lower(referer_url_host)
        end as campaign
        
    from joined

)

select * from with_channels