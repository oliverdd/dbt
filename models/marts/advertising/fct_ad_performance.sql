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
                    'campaign'
                )}}
            when platform = 'google' then
                {{dbt_utils.surrogate_key (
                    'campaign_date',
                    'criteria_id',
                    'ad_group_id',
                    'campaign'
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
            when lower(campaign) ilike '%display%' then 'display'
            when lower(campaign) ilike '%shopping%'
                or lower(campaign) ilike '%cpc%' then 'paid search'
            when lower(utm_medium) in ('retargeting', 'social', 'influencers') 
                then utm_medium
            when lower(utm_campaign) in ('fb_retargeted', 'ig_retargeted') 
                then 'retargeting'
            when lower(utm_campaign) ilike '%amazon%' then 'facebook amazon'
            when utm_medium is null and platform = 'facebook'
                or lower(utm_medium) in ('paid', 'paid_social', 'prospecting')
                then 'facebook ads'
            when lower(utm_medium) in ('paidsearch', 'cpc', 'shopping') 
                then 'paid search'
            else null
        end as channel
        
    from id_creation
    
)

select * from marketing_channels