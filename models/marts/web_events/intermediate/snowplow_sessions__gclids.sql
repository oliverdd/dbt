with sessions as (

    select * from {{ref('snowplow_sessions')}}

),

ads as (
    
    select * from {{ ref('adwords_gclid_mapping') }}
    
),

filter_gclids as (

    select
    
        user_snowplow_domain_id,
        session_index,
        split(split(first_page_url_query, 'gclid=')[1], '&')[0]::string as gcl_id
        
    from sessions
    where lower(first_page_url_query) like '%gclid%'

),

gclids_joined as (

    select
        
        {{ dbt_utils.surrogate_key(
            'user_snowplow_domain_id', 
            'session_index'
        ) }} as unique_id,
        filter_gclids.user_snowplow_domain_id,
        filter_gclids.session_index,
        ads.criteria_id,
        ads.ad_group_id,
        ads.adwords_campaign
        
    from filter_gclids
    left join ads
        on filter_gclids.gcl_id = ads.gclid

)

select * from gclids_joined