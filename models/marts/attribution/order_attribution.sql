{{
    config(
        materialized='table'
    )
}}

with sessions as (

    select

        session_id,
        ad_performance_id,
        inferred_user_id,
        session_start,
        session_index,
        device_type,
        attribution_channel,
        attribution_source,
        attribution_campaign

    from {{ref('fct_snowplow_sessions')}}

),

orders as (

    select * from {{ ref('fct_orders') }}
    where customer_id is not null

),

sessions_joined as (

    select

        orders.order_id,
        orders.total_price,
        orders.created_at,
        sessions.session_id,
        sessions.session_start,
        sessions.session_index,
        sessions.device_type,
        sessions.inferred_user_id,
        sessions.ad_performance_id,
        sessions.attribution_channel,
        sessions.attribution_source,
        sessions.attribution_campaign,
        timestampdiff('hour', created_at, session_start) as hours_before_order

    from orders
    left join sessions
        on orders.email = sessions.inferred_user_id
    where sessions.session_start <= orders.created_at

),

apply_rank as (

    select

        *,

        case when inferred_user_id is not null
                then row_number() over (partition by order_id order by session_start)
            else null
        end as order_session_number,

        case when inferred_user_id is not null
                then count(*) over (partition by order_id)
            else null
        end as order_total_sessions,

        lag(session_start) over (partition by order_id order by session_start)
            as previous_session_start

    from sessions_joined

),

attribution_calculations as (

    select

        *,
        {{ dbt_utils.surrogate_key('order_id','order_session_number') }}
            as attribution_id,

        case
            when order_total_sessions = 1 then 1.0
            when order_total_sessions = 2 then 0.5
            when order_session_number = 1 then 0.4
            when order_session_number = order_total_sessions then 0.4
            else 0.2 / (order_total_sessions - 2)
        end as forty_twenty_forty_points,

        case
            when order_session_number = 1 then 1.0
            else 0.0
        end as first_click_points,

        case
            when order_session_number = order_total_sessions then 1.0
            else 0.0
        end as last_click_points,

        1.0 / order_total_sessions as linear_points,

        timestampdiff('hour', session_start,previous_session_start)
            as hours_from_previous_session

    from apply_rank

),

revenue as (

    select

        *,

        total_price * forty_twenty_forty_points
            as forty_twenty_forty_revenue,
        total_price * first_click_points
            as first_click_revenue,
        total_price * last_click_points
            as last_click_revenue,
        total_price * linear_points
            as linear_revenue

    from attribution_calculations

)

select * from revenue