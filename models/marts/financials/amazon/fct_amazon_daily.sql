with prices as (

    select
        sku,
        round(avg(item_price),2) as item_price
    from {{ ref('stg_amazon_mws') }} m 
    left join {{ ref('stg_amazon_mws_order_items') }} i on m.id = i.order_id
    where is_completed = 1
    group by 1

), order_items as (  

    select 
        m.id,
        m.created_at,
        m.buyeremail,
        m.buyername,
        i.sku,
        coalesce(i.item_price, p.item_price) as item_price,
        i.item_tax,
        i.discount,
        i.discount_tax,
        i.numberofitems,
        i.quantityordered,
        case when orderstatus = 'Pending' then coalesce(i.item_price, p.item_price)*quantityordered 
            else i.product_sales end as product_sales,
        m.order_total,
        m.orderstatus,
        m.is_completed,
        m.isbusinessorder,
        i.isgift
    from {{ ref('stg_amazon_mws') }} m 
    left join {{ ref('stg_amazon_mws_order_items') }} i on m.id = i.order_id
    left join prices p on i.sku = p.sku

), orders as (

    select *
    from {{ ref('amazon_orders_calculations') }}

), calc as (

    select 
        date_trunc(day,oi.created_at) as date,
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