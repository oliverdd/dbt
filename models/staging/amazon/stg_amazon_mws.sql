with source as (
    select
        convert_timezone('America/Los_Angeles', purchasedate)::date as day,
        item.value:SellerSKU::string as sku,
        item.value:ASIN::string as asin,
        item.value:ItemPrice:Amount::float as price,
        item.value:ProductInfo:NumberOfItems::int as numberofitems,
        item.value:QuantityOrdered::int as quantityordered,
        orderstatus,
        isbusinessorder
    from raw.perfect_keto_amazon_mws.orders,
    lateral flatten (input => orderitems) item
)

select * from source