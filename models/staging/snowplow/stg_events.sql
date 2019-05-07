with 

orders as (    
    select * from {{ ref('stg_shopify_orders') }}
),

add_users as (
    select
    
        {{ dbt_utils.star(from=ref('events_base'), except=["USER_ID", "APP_ID"]) }},
        
        events.app_id,
        coalesce(orders.customer_id, events.user_id) as user_id

    from {{ref('events_base')}} as events
    left join orders
        on events.user_id = orders.cart_token

)

select * from add_users