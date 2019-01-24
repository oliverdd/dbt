{% set frame_clause = 'over (partition by refund_id order by refund_processed_at
    rows between unbounded preceding and unbounded following)' %}


with source as (

    select * from raw.perfect_keto_shopify.orders
    
),

flattened as (

    select
    
        f.value:id as refund_id,
        id as order_id,
        sum(f1.value:subtotal::number(38,6) * -1) 
            over (partition by refund_id) as refund_amount,
        f1.value:total_tax::number(38,6) as tax_amount,
        f.value:processed_at::timestampntz as refund_processed_at

    from source,
        lateral flatten (input => refunds) f,
        lateral flatten (input => f.value:refund_line_items) f1
        
),

final as (
    
    select distinct
        
        order_id,
        refund_id,
        first_value(refund_amount) {{ frame_clause }} as refund_amount,
        first_value(tax_amount) {{ frame_clause }} as tax_amount,
        first_value(refund_processed_at) {{ frame_clause }} refund_processed_at
         
    from flattened
)

select * from final