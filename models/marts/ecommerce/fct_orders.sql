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
    select distinct
        
        order_id,
         
        last_value(created_at) over (partition by order_id order by 
            created_at) as paid_at,
            
        last_value(gateway) over (partition by order_id order by 
            created_at) as payment_method
            
    from {{ ref('stg_shopify_transactions') }}
),

joined as (
    
    select 
        orders.*,
        transactions.paid_at,
        transactions.payment_method
    from orders
    left join transactions
        using (order_id)
)

select * from joined