with source as (

    select * from raw.perfect_keto_shopify.orders
    
),

flattened as (

    select
        refunds,
        f.value:id as refund_id,
        id as order_id,
        f1.value:id as order_item_id,
        f1.value:line_item:title::string as product_title,
        f1.value:line_item:variant_title::string as variant_title,
        f1.value:line_item:sku::string as sku,
        f1.value:line_item:price::number(38,6) as order_item_price,
        f1.value:quantity::number(38,6) as quantity,
        f1.value:subtotal::number(38,6) as order_item_subtotal,
        f2.value:amount::number(38,6) as discount_amount,
        f1.value:total_tax::number(38,6) as tax_amount,
        convert_timezone('UTC','America/Los_Angeles',f.value:processed_at::timestampntz) as refund_processed_at
    from source,
    lateral flatten (input => refunds) f,
    lateral flatten (input => f.value:refund_line_items) f1,
    lateral flatten (input => f1.value:line_item:discount_allocations, outer => true) f2

),

final as (
  
    select
        refund_id,
        order_id,
        order_item_id,
        product_title,
        variant_title,
        sku,
        refund_processed_at,
        max(order_item_price) as order_item_price,
        max(order_item_subtotal) as order_item_subtotal,
        max(quantity) as quantity,
        sum(discount_amount) as discount_amount,
        max(tax_amount*-1) as tax_amount
    from flattened
    group by 1,2,3,4,5,6,7
  
)

select * from final