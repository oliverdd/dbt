with sessions as (

    select * from {{ref('snowplow_sessions')}}

),

clicks as (

    select * from {{ref('adwords_click_performance')}}

),

criteria as (

    select * from {{ref('adwords_criteria_performance')}}

),

ads as (

    select distinct
        criteria.criteria_id,
        criteria.ad_group_id,
        criteria.campaign_name as adwords_campaign,
        clicks.gclid
    from criteria
    left outer join clicks
    on criteria.ad_group_id = clicks.ad_group_id
    and criteria.criteria_id = clicks.criteria_id
    and criteria.date_day = clicks.date_day

),

filter_gclids as (

    select
        user_snowplow_domain_id,
        session_index,
        split(split(first_page_url_query, 'gclid=')[1], '&')[0]::string as gcl_id   
    from sessions
    where lower(first_page_url_query) like '%gclid%'

),

final as (

    select
        filter_gclids.user_snowplow_domain_id,
        filter_gclids.session_index,
        ads.criteria_id,
        ads.ad_group_id,
        ads.adwords_campaign
    from filter_gclids
    left outer join ads
        on filter_gclids.gcl_id = ads.gclid

)

select * from final