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
                    'ad_group_id',
                    'ad_group_name',
                    'utm_source',
                    'utm_medium',
                    'campaign_name'
                )}}
            when platform = 'google' then
                {{dbt_utils.surrogate_key (
                    'campaign_date',
                    'criteria_id',
                    'ad_group_id',
                    'campaign_name'
                )}}
            else null
        end as id,
        *

    from unioned

),

marketing_channels as (
    
    select
    
        *,
        case

            when lower(campaign_name) ilike '%display%' then 'display'
            when lower(campaign_name) ilike '%shopping%'
                or lower(campaign_name) ilike '%cpc%' then 'paid search' 
            when utm_medium is null then 'direct'
            when lower(utm_medium) in ('retargeting', 'social', 'influencers') then utm_medium
            when lower(utm_medium) in ('paid', 'paid_social') then 'facebook ads'
            when lower(utm_medium) in ('paidsearch', 'cpc', 'shopping') 
                then 'paid search'
            else null
        end as channel
        
    from id_creation
    
)

select * from marketing_channels