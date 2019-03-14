{{
    config(
        materialized='table'
    )
}}

with unioned as (

    {{ dbt_utils.union_tables(
        tables=[ref('adwords_ad_performance_adapter'), 
        ref('fb_ad_performance_adapter')]
    ) }}

),

id_creation as (

    select
        case 
            when platform = 'facebook' then
                {{dbt_utils.surrogate_key (
                    'campaign_date',
                    'ad_name',
                    'adset_name',
                    'fb_campaign_name'
                )}}
            when platform = 'adwords' then
                {{dbt_utils.surrogate_key (
                    'campaign_date',
                    'criteria_id',
                    'ad_group_id',
                    'adwords_campaign_name'
                )}}
            else null
        end as id,
        *

    from unioned

)

select * from id_creation

/*
remove_duplicates as (
    
    select 
    
        id,
        max(_dbt_source_table) as ad_data_source,
        max(campaign_date) as date_day,
        max(utm_medium) as utm_medium,
        max(utm_source) as utm_source,
        max(utm_campaign) as utm_campaign,
        max(utm_term) as utm_term,
        max(platform) as platform,
        max(utm_content) as utm_content,
        max(criteria_id) as criteria_id,
        max(ad_group_id) as ad_group_id,
        max(ad_group_name) as ad_group_name,
        sum(clicks) as clicks,
        sum(spend) as spend,
        sum(impressions) as impressions

    from id_creation
    group by 1

)

select * from remove_duplicates
*/