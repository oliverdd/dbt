{{
    config(
        materialized='table'
    )
}}

with sessions as (
    select * from  {{ ref('snowplow_sessions__mapped') }}
),

shopify_customers as (
    
    select * from {{ ref('customers') }}
    
),

sessions_joined as (

    select
    
        sessions.*,
        date_trunc('day', cast(session_start as date)) as session_day,
        shopify_customers.customer_id,
        shopify_customers.customer_created_at,
        shopify_customers.customer_age_days,
        shopify_customers.customer_type,
        shopify_customers.lifetime_revenue

--start to categorize source, medium, and campaign from snowplow fields

    from sessions
    left join shopify_customers 
        on sessions.inferred_user_id = shopify_customers.email
        and sessions.session_start <= shopify_customers.first_order_date
        
),

apply_rank as (

    select

        *,

        row_number() over (partition by customer_id order by session_start)
            as attribution_session_number,

        count(*) over (partition by customer_id) as attribution_total_sessions

    from sessions_joined

),

attribution_calculations as (

    select

        *,

        case
            when attribution_total_sessions = 1 then 1.0
            when attribution_total_sessions = 2 then 0.5
            when attribution_session_number = 1 then 0.4
            when attribution_session_number = attribution_total_sessions then 0.4
            else 0.2 / (attribution_total_sessions - 2)
        end as forty_twenty_forty_attribution_points,

        case
            when attribution_session_number = 1 then 1.0
            else 0.0
        end as first_click_attribution_points,

        case
            when attribution_session_number = attribution_total_sessions then 1.0
            else 0.0
        end as last_click_attribution_points,

        1.0 / attribution_total_sessions as linear_attribution_points

    from apply_rank

),

all_sessions as (
    
    select 
    
        sessions.*,
        attribution_calculations.customer_id,
        attribution_calculations.lifetime_revenue,
        attribution_calculations.forty_twenty_forty_attribution_points,
        attribution_calculations.first_click_attribution_points,
        attribution_calculations.last_click_attribution_points,
        attribution_calculations.linear_attribution_points
        
    from sessions 
    left join attribution_calculations using (session_id)

)

select * from all_sessions
