with order_items as (  

    select 
        id,
        created_at,
        buyeremail,
        buyername,
        sku,
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
    
)

select 
    date_trunc(day,created_at) as date,
    sku,  
    sum(product_sales) as gross_item_revenue,
    sum(discount) as total_discounts,
    sum(product_sales)-sum(discount) as net_item_revenue,
    sum(quantityordered) as total_units
from order_items 
where orderstatus != 'Canceled'
group by 1,2
order by 1 asc 