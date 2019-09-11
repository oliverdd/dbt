with gift_cards as (

    select *
    from {{ ref('stg_shopify_order_items') }}
    where gift_card = true 

),

orders as (

    select 
        o.*,
        case when app_id = 1150484 then 'Wholesale'
            when app_id = 580111 then 'Online Store'
            when app_id = 294517 then 'Recharge'
            else 'Other' end as channel    
    from {{ ref('orders_xf') }} o
    where 
        not exists (
        
            select * from gift_cards g where o.order_id = g.order_id
        
        )
    
),

refund_items as (

    select * from {{ ref('stg_shopify_refund_items') }}

),

order_adjustments as (

    select * from {{ ref('stg_shopify_refund_order_adjustments') }}

),

refund_calc as (

    select
        refund_processed_at::date as date,
        order_id,
        order_item_id,
        product_title,
        order_item_price,
        order_item_subtotal*-1 as gross_returns,
        quantity,
        discount_amount,
        tax_amount as tax_amount
    from refund_items 

),

adjustments_calc as (

    select 
        refund_processed_at::date as date,
        order_id,
        case when kind = 'shipping_refund' then 0 else order_adjustments end as gross_returns,
        case when kind = 'shipping_refund' then order_adjustments else 0 end as shipping_amount
    from order_adjustments

),

refund_joined as (

    select 
        date,
        order_id,
        order_item_id,
        product_title,
        order_item_price,
        quantity,
        gross_returns,
        discount_amount,
        tax_amount,
        0 as shipping_amount
    from refund_calc
    union all 
    select 
        date,
        order_id,
        null as order_item_id,
        null as product_title,
        null as order_item_price,
        0 as quantity,
        gross_returns,
        0 as discount_amount,
        0 as tax_amount,
        shipping_amount
    from adjustments_calc

),

refund_channel1 as (

    select 
        r.*,
        o.channel
    from refund_joined r 
    left join orders o on o.order_id = r.order_id  
    
),

refund_channel2 as (

    select
        date_trunc(week,date) as week,
        channel,
        sum(gross_returns) as gross_returns,
        sum(discount_amount) as discount_returned,
        sum(gross_returns)+sum(discount_amount) as net_returns,
        sum(tax_amount) as tax_returned,
        sum(shipping_amount) as shipping_returned,
        sum(gross_returns)+sum(discount_amount)+sum(tax_amount)+sum(shipping_amount) as total_returns
    from refund_channel1
    group by 1,2

),

refund_final as (

    select
        date_trunc(week,date) as week,
        sum(gross_returns) as gross_returns,
        sum(discount_amount) as discount_returned,
        sum(gross_returns)+sum(discount_amount) as net_returns,
        sum(tax_amount) as tax_returned,
        sum(shipping_amount) as shipping_returned,
        sum(gross_returns)+sum(discount_amount)+sum(tax_amount)+sum(shipping_amount) as total_returns
    from refund_joined
    group by 1
    
),

orders_calc as (

    select 
        date_trunc(week,created_at) as week,
        count(distinct order_id) as orders,
        sum(total_line_items_price) as order_gross,
        sum(total_discounts) as total_discounts,
        sum(total_tax) as order_tax_net,
        sum(total_shipping_cost) as order_shipping_gross,
        count(case when tags ilike '%subscription%' then 1 else null end) as subscription_orders, 
        count(case when tags not ilike '%subscription%' then 1 else null end) as one_time_orders, 
        count(distinct customer_id) as unique_customers, 
        count(case when new_vs_repeat = 'new' then 1 else null end) as first_time_customers, 
        count(case when new_vs_repeat = 'repeat' then 1 else null end) as repeat_customers 
    from orders 
    group by 1
    order by 1 desc 
    
),

quantity as (

    select 
        date_trunc(week,o.created_at) as week,
        sum(quantity) as total_units
    from analytics.stg_shopify_orders o 
    join analytics.stg_shopify_order_items oi on o.order_id = oi.order_id
    group by 1

),

channels as (

    select 
        date_trunc(week,created_at) as order_week,
        channel,
        sum(total_line_items_price) as order_gross,
        sum(total_discounts) as total_discounts,
        sum(total_shipping_cost) as order_shipping_gross,
        sum(total_tax) as order_tax_gross
    from orders 
    group by 1,2
    order by 1 desc, 6 desc
    
),

channels2 as (

    select 
        c.order_week,
        c.channel,
        order_gross,
        total_discounts,
        zeroifnull(gross_returns) as gross_returns,
        order_shipping_gross,
        zeroifnull(shipping_returned) as shipping_returned,
        order_tax_gross,
        zeroifnull(tax_returned) as tax_returned,
        order_gross-total_discounts+zeroifnull(gross_returns)+order_shipping_gross+zeroifnull(shipping_returned) as total_net
    from channels c 
    left join refund_channel2 r on c.order_week = r.week and c.channel = r.channel

),

channels3 as (

    select 
        order_week,
        case when channel = 'Online Store' then total_net end as "Online Store Rev",
        case when channel = 'Recharge' then total_net end as "Recharge Rev",
        case when channel = 'Wholesale' then total_net end as "Wholesale Rev"
    from channels2
    order by 1 desc
    
),

final as (

    select 
        o.week,
        o.orders,
        q.total_units, 
        o.order_gross,
        o.total_discounts,
        r.net_returns as total_refunds,
        o.order_gross-o.total_discounts+r.net_returns as total_net,
        o.order_shipping_gross+r.shipping_returned as order_shipping_net,
        o.order_tax_net+r.tax_returned as order_tax_net,
        o.subscription_orders,
        o.one_time_orders,
        o.unique_customers,
        o.first_time_customers,
        o.repeat_customers
    from orders_calc o 
    left join refund_final r on o.week = r.week
    left join quantity q on o.week = q.week

)

select 
    f.*,
    sum(ch."Online Store Rev") as "Online Store Rev",
    sum(ch."Recharge Rev") as "Recharge Rev",
    sum(ch."Wholesale Rev") as "Wholesale Rev"
from final f
left join channels3 ch on f.week = ch.order_week
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
order by 1 asc