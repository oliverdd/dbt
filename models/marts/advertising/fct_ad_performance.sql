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
            when platform = 'google' then
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