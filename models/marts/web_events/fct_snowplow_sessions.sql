{{
    config(
        materialized='table'
    )
}}

with sessions as (
    
    select * from  {{ ref('snowplow_sessions__with_channels') }}

),

orders as (
    
    select * from {{ ref('fct_orders') }}
    where is_completed = 1
    
),

sessions_joined as (

    select
    
        sessions.*,
        date_trunc('day', cast(session_start as date)) as session_day,
        orders.email,
        orders.customer_id,
        orders.first_order_date,
        orders.order_id,
        orders.completed_order_number,
        orders.created_at as order_date

    from sessions
    inner join orders 
        on sessions.inferred_user_id = orders.customer_id
        
    where sessions.session_start <= orders.created_at
        and (orders.previous_completed_order_date is null
        or sessions.session_start > orders.previous_completed_order_date)    
        
),

apply_rank as (

    select

        *,

        row_number() over (partition by order_id order by session_start)
            as attribution_session_number,

        count(*) over (partition by order_id) as attribution_total_sessions

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
        attribution_calculations.email,
        attribution_calculations.customer_id,
        attribution_calculations.order_id,
        attribution_calculations.completed_order_number,
        attribution_calculations.order_date,
        attribution_calculations.forty_twenty_forty_attribution_points,
        attribution_calculations.first_click_attribution_points,
        attribution_calculations.last_click_attribution_points,
        attribution_calculations.linear_attribution_points
        
    from sessions 
    left join attribution_calculations using (session_id)

)

select * from all_sessions
