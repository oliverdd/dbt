with criteria_performance as (

    select * from {{ ref('adwords_criteria_performance') }}

),

final as (

    select
    
        date_day as campaign_date,
        criteria_id,
        ad_group_name,
        ad_group_id,
        ad_group_state,
        'google' as utm_source,
        'cpc' as utm_medium,
        lower(campaign_name) as campaign_name,
        campaign_id,
        campaign_state,
        customer_id,
        clicks,
        impressions,
        spend,
        'google' as platform

    from criteria_performance
)

select * from final