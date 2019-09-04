with orders as (

    select * from raw.perfect_keto_amazon_mws.orders

),

flattened_items as (

    select
        item.value:OrderItemId::string as order_item_id,
        amazonorderid as order_id,
        convert_timezone('UTC','America/Los_Angeles',purchasedate::timestamp_ntz) as order_created_at,
        item.value:SellerSKU::string as sku,
        item.value:ASIN::string as asin,
        item.value:Title::string as title,
        item.value:ItemPrice:Amount::float as product_sales,
        (item.value:ItemPrice:Amount::float)/nullif(item.value:QuantityOrdered::int,0) as item_price,
        item.value:ItemTax:Amount::float as item_tax,
        item.value:PromotionDiscount:Amount::float as discount,
        item.value:PromotionDiscountTax:Amount::float as discount_tax,
        item.value:ProductInfo:NumberOfItems::int as numberofitems,
        item.value:QuantityOrdered::int as quantityordered,
        item.value:QuantityShipped::int as quantityshipped,
        item.value:IsGift::string as isgift,
        item.value:IsTransparency::string as istransparency
    from orders,
    lateral flatten (input => orderitems) item

)

select * from flattened_items