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

         {{dbt_utils.surrogate_key (
             'campaign_date',
             'url_host',
             'url_path',
             'utm_medium',
             'utm_source',
             'utm_campaign',
             'utm_term',
             'utm_content',
             'criteria_id',
             'ad_group_id'
             
         )}} as id,
        *

    from unioned

)

select * from id_creation