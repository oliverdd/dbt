with fb_keyword_performance as (

    select * from {{ref('fb_ad_insights_xf')}}

),

fb_keyword_performance_agg as (

    select
    
        date_day as campaign_date,
        adset_id as ad_group_id,
        adset_name as ad_group_name,
        ad_name,
        campaign_id,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        campaign_name as fb_campaign_name,
        'facebook' as platform,
        sum(clicks) as clicks,
        sum(impressions) as impressions,
        sum(spend) as spend
        

    from fb_keyword_performance
    {{dbt_utils.group_by(12)}}

)

select * from fb_keyword_performance_agg


