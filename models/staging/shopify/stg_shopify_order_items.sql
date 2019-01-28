with orders as (
    
    select * from raw.perfect_keto_shopify.orders
    
),

flattened_items as (
    
    select
    
        f.value:id::varchar as order_item_id,
        id as order_id,
        order_number,
        nullif(lower(email),'') as email,
        financial_status,
        f.value:product_id::varchar as product_id,
        f.value:sku::varchar as product_sku,
        f.value:title::varchar as product_title,
        f.value:name::varchar as product_name,
        f.value:quantity as quantity,
        f.value:pre_tax_price::number(38,6) as pre_tax_price,
        f.value:total_discount::number(38,6) as total_discount,
        f.value:price::number(38,6) as price,
        f.value:fulfillment_quantity as fulfillment_quantity,
        f.value:fulfillment_service::varchar as fulfillment_service,
        f.value:fulfillment_status::varchar as fulfillment_status,
        f.value:gift_card::boolean as gift_card,
        f.value:grams as grams,
        f.value:product_exists::boolean as product_exists,
        f.value:requires_shipping::boolean as requires_shipping,
        f.value:taxable::boolean as taxable,
        f.value:variant_id::varchar as variant_id,
        f.value:variant_inventory_management::varchar as variant_inventory_managment,
        f.value:variant_title::varchar as variant_title,
        f.value:vendor::varchar as vendor
        
    from orders,
    lateral flatten (input => line_items) f
        
)

select * from flattened_items