with sessions as (
    
    select * from  {{ ref('snowplow_sessions__mapped') }}
    
),

with_channels as (
    
    select 
        
        *,
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
            when adwords_campaign is not null
                then lower(adwords_campaign)
            when marketing_medium is not null
                or marketing_source is not null
                or marketing_campaign is not null
                then lower(marketing_campaign)
            when referer_url_host is not null then lower(referer_url_host)
        end as attribution_campaign
        
    from sessions
    
)

select * from with_channels