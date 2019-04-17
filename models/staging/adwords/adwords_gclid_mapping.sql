with clicks as (

    select * from {{ref('adwords_click_performance')}}

),

criteria as (

    select * from {{ref('adwords_criteria_performance')}}

),

joined as (

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

)

select * from joined