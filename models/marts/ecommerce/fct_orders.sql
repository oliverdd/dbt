{{
    config(
        materialized='table'
    )
}}

with 
orders as (
    select * from {{ ref('orders_xf') }}
),

transactions as (
    select 
        *,
        row_number() over (partition by order_id order by 
            created_at desc) as most_recent_transaction
    from {{ ref('stg_shopify_transactions') }}
),

recent_transactions as (
    select * from transactions
    where most_recent_transaction = 1
),

joined as (
    
    select 
        orders.*,
        recent_transactions.created_at as paid_at,
        recent_transactions.gateway as payment_method
    from orders
    left join recent_transactions
        using (order_id)
)

select * from joined