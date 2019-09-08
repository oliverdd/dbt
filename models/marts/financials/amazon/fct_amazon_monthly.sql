with order_items as (  

    select 
        id,
        created_at,
        buyeremail,
        buyername,
        item_price,
        item_tax,
        discount,
        discount_tax,
        numberofitems,
        quantityordered,
        product_sales,
        order_total,
        orderstatus,
        is_completed,
        isbusinessorder,
        isgift
    from {{ ref('stg_amazon_mws') }} m 
    left join {{ ref('stg_amazon_mws_order_items') }} i on m.id = i.order_id

), orders as (

    select *
    from {{ ref('amazon_orders_calculations') }}

), calc as (

    select 
        date_trunc(month,oi.created_at) as month,
        sum(quantityordered) as total_units,
        sum(product_sales) as order_gross,
        sum(discount) as total_discounts,
        sum(product_sales)-sum(discount) as total_net,
        sum(item_tax+discount_tax) as order_tax_net,
        count(distinct oi.id) as one_time_orders,
        count(distinct oi.buyeremail) as unique_customers,
        count(case when new_vs_repeat = 'new' then 1 else null end) as first_time_customers, 
        count(case when new_vs_repeat = 'repeat' then 1 else null end) as repeat_customers
    from order_items oi 
    left join orders o on oi.id = o.id 
    where oi.orderstatus != 'Canceled'
    group by 1
    order by 1 asc 

) 

select * from calc 