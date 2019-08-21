with source as (

    select * from raw.perfect_keto_shopify.orders
    
),

flattened as (

    select
        id as order_id,
        f.value:id as refund_id,
        f2.value:amount::number(38,6) as order_adjustments,
        f2.value:kind::string as kind,
        convert_timezone('UTC','America/Los_Angeles',f.value:processed_at::timestampntz) as refund_processed_at
    from source,
    lateral flatten (input => refunds) f,
    lateral flatten (input => f.value:order_adjustments) f2
)

select * from flattened