with orders as (

    select 
        o.order_id,
        o.created_at,
        product_sku as sku,
        variant_title,
        quantity,
        coalesce(discount_allocations,0) as discount_allocations,
        price,
        price*quantity as gross_item_revenue
    from {{ ref('stg_shopify_orders') }} o 
    join {{ ref('stg_shopify_order_items') }} oi on o.order_id = oi.order_id
    where gift_card = false
    
),

refund_items as (

    select
        refund_processed_at::date as date,
        variant_title,
        sku,
        order_item_price,
        order_item_subtotal,
        quantity,
        order_item_subtotal*-1 as gross_returns,
        discount_amount,
        tax_amount as tax_amount
    from {{ ref('stg_shopify_refund_items') }}

),

refund_calc as (

    select 
        date_trunc(month,date) as month,
        variant_title,
        sku,
        sum(quantity) as units_returned,
        sum(gross_returns) as returns 
    from refund_items 
    group by 1,2,3

),

orders_calc as (

    select 
        date_trunc(month,created_at) as month,
        variant_title,
        sku,
        sum(quantity) as total_units,
        sum(gross_item_revenue) as gross_item_revenue,
        sum(discount_allocations) as discounts
    from orders 
    group by 1,2,3
    order by 4 asc  

),

final as (

    select 
        o.month,
        o.variant_title,
        o.sku,
        o.gross_item_revenue,
        o.gross_item_revenue-o.discounts+zeroifnull(r.returns) as net_item_revenue,
        o.total_units-zeroifnull(r.units_returned) as total_units
    from orders_calc o
    left join refund_calc r on o.month = r.month and o.variant_title = r.variant_title and o.sku = r.sku

)

select * from final 
order by 1 asc, 2 asc