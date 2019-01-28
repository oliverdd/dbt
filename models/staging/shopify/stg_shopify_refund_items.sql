with source as (

    select * from raw.perfect_keto_shopify.orders
    
),

flattened as (

    select

            f.value:id as refund_id,
            id as order_id,
            sum(f1.value:subtotal::number(38,6) * -1) 
                over (partition by refund_id) as refund_amount,
            f1.value:id as order_item_id,
            f1.value:subtotal::number(38,6)*-1 as order_item_subtotal,
            f1.value:total_tax::number(38,6) as tax_amount,
            f.value:processed_at::timestampntz as refund_processed_at

         from source,
            lateral flatten (input => refunds) f,
            lateral flatten (input => f.value:refund_line_items) f1
)

select * from flattened